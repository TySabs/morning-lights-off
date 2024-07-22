use chrono::{DateTime, Local, Utc};
use dotenv::dotenv;
use reqwest::Error as ReqwestError;
use serde::Deserialize;
use std::env;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::time::{sleep, Duration};
use tokio_postgres::Client;
use tokio_postgres::NoTls;

#[derive(Deserialize)]
struct SunriseSunsetResponse {
    results: Results,
}

#[derive(Deserialize)]
struct Results {
    sunrise: String,
    // sunset: String,
    // solar_noon: String,
    // day_length: String,
    // civil_twilight_begin: String,
    // civil_twilight_end: String,
    // nautical_twilight_begin: String,
    // nautical_twilight_end: String,
    // astronomical_twilight_begin: String,
    // astronomical_twilight_end: String,
}

struct WizLight {
    host_id: String,
    name: String,
}

#[derive(Error, Debug)]
enum SunriseError {
    #[error("HTTP request error")]
    ReqwestError(#[from] ReqwestError),
    #[error("DateTime parse error")]
    ChronoParseError(#[from] chrono::ParseError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let db_host = env::var("DB_HOST").expect("DB_HOST not set");
    let db_user = env::var("DB_USER").expect("DB_USER not set");
    let db_password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
    let db_name: String = env::var("DB_NAME").expect("DB_NAME not set");

    let conn_str = format!(
        "host={} user={} password={} dbname={}",
        db_host, db_user, db_password, db_name
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // Spawn the connection to run in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let wiz_lights = fetch_wiz_lights(&client).await?;
    let sunrise_utc = fetch_sunrise_time().await?;
    let sunrise_local = sunrise_utc.with_timezone(&Local);

    // Calculate the target time (30 minutes before sunrise)
    let target_time = sunrise_local - chrono::Duration::minutes(30);

    // Calculate the duration to sleep
    let duration_to_sleep = target_time - Local::now();
    if duration_to_sleep.num_seconds() > 0 {
        let message = format!(
            "Sunrise local is {}. Sleeping for {} seconds until {} before turning off morning lights.",
            sunrise_local,
            duration_to_sleep.num_seconds(),
            target_time
        );
        println!("{}", message);
        log_light_event(&client, "Info", &message, "All").await?;
        sleep(Duration::from_secs(duration_to_sleep.num_seconds() as u64)).await;
    } else {
        let message = format!(
            "It is already close enough to sunrise. Sunrise local today is {}. Turning lights off immediately.",
            sunrise_local.format("%Y-%m-%d %H:%M:%S")
        );
        println!("{}", message);
        log_light_event(&client, "Info", &message, "All").await?;
    }

    // Turn the lights off
    let payload_off = r#"{"method":"setPilot","params":{"state":false}}"#;
    for light in &wiz_lights {
        match send_udp_packet(&light.host_id, payload_off).await {
            Ok(_) => {
                let severity: &str = "Info";
                let message: String =
                    format!("Light {} at {} turned off!", light.name, light.host_id);
                println!("SUCCESS: {}", message);
                log_light_event(&client, severity, &message, &light.name).await?;
            }
            Err(e) => {
                let severity: &str = "Error";
                let message = format!(
                    "Failed to turn off light {} at {}: {}",
                    light.name, light.host_id, e
                );
                println!("ERROR: {}", message);
                log_light_event(&client, severity, &message, &light.name).await?;
            }
        }
    }

    Ok(())
}

async fn fetch_wiz_lights(client: &Client) -> Result<Vec<WizLight>, Box<dyn std::error::Error>> {
    let rows = client
        .query("SELECT host_id, name FROM machine", &[])
        .await?;

    let network_id: String = env::var("NETWORK_ID").expect("NETWORK_ID not set");

    let mut wiz_lights = Vec::new();
    for row in rows {
        let host_id: String = row.get("host_id");
        let name: String = row.get("name");
        wiz_lights.push(WizLight {
            host_id: format!("{}.{}:38899", network_id, host_id),
            name,
        });
    }

    Ok(wiz_lights)
}

async fn fetch_sunrise_time() -> Result<DateTime<Utc>, SunriseError> {
    let lat: f64 = env::var("LAT")
        .expect("LAT not set")
        .parse()
        .expect("Invalid latitude value");
    let lng: f64 = env::var("LNG")
        .expect("LNG not set")
        .parse()
        .expect("Invalid longitude value");

    let url = format!(
        "https://api.sunrise-sunset.org/json?lat={}&lng={}&formatted=0",
        lat, lng
    );

    let resp = reqwest::get(&url)
        .await?
        .json::<SunriseSunsetResponse>()
        .await?;
    let sunrise_utc = resp.results.sunrise.parse::<DateTime<Utc>>()?;
    Ok(sunrise_utc)
}

async fn send_udp_packet(addr: &str, payload: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let addr: SocketAddr = addr.parse()?;
    socket.send_to(payload.as_bytes(), &addr).await?;
    Ok(())
}

async fn log_light_event(
    client: &Client,
    severity: &str,
    message: &str,
    machine: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_type = "Morning";

    client
        .execute(
            "INSERT INTO log (severity, message, machine, event_type) VALUES ($1, $2, $3, $4)",
            &[&severity, &message, &machine, &event_type],
        )
        .await?;

    Ok(())
}
