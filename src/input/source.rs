use crate::{Result, input::event::extract_sse_json_frames};
use anyhow::Context;
use futures_util::StreamExt;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::Path,
    time::Duration,
};

pub const WIKIMEDIA_RECENT_CHANGE_STREAM: &str =
    "https://stream.wikimedia.org/v2/stream/recentchange";

#[derive(Debug, Clone, Default)]
pub struct LiveSourceResult {
    pub events: Vec<String>,
    pub network_resets: u64,
}

pub fn read_replay_file(path: &Path, limit: usize) -> Result<Vec<String>> {
    let file =
        File::open(path).with_context(|| format!("opening replay file {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut events = Vec::with_capacity(limit);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        events.push(trimmed.to_string());
        if events.len() >= limit {
            break;
        }
    }

    Ok(events)
}

pub fn write_replay_file(path: &Path, events: &[String]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    for event in events {
        writeln!(file, "{event}")?;
    }
    Ok(())
}

pub async fn fetch_live_events(limit: usize, watchdog_timeout: Duration) -> Result<Vec<String>> {
    Ok(fetch_live_events_with_reconnect(limit, watchdog_timeout, 3)
        .await?
        .events)
}

pub async fn fetch_live_events_with_reconnect(
    limit: usize,
    watchdog_timeout: Duration,
    max_network_resets: u64,
) -> Result<LiveSourceResult> {
    let mut events = Vec::with_capacity(limit);
    let mut network_resets = 0;

    while events.len() < limit {
        match fetch_live_events_once(limit - events.len(), watchdog_timeout).await {
            Ok(mut batch) => {
                if batch.is_empty() {
                    break;
                }
                events.append(&mut batch);
                if events.len() >= limit {
                    break;
                }
            }
            Err(error) if error.to_string().contains("watchdog timeout") => {
                network_resets += 1;
                if network_resets > max_network_resets {
                    return Err(error).with_context(|| {
                        format!("network reset limit exceeded after {max_network_resets} retries")
                    });
                }
                continue;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(LiveSourceResult {
        events,
        network_resets,
    })
}

async fn fetch_live_events_once(limit: usize, watchdog_timeout: Duration) -> Result<Vec<String>> {
    let client = reqwest::Client::builder()
        .user_agent("rts2601-wiki-monitor/0.1")
        .build()
        .context("building HTTP client")?;
    let response = client
        .get(WIKIMEDIA_RECENT_CHANGE_STREAM)
        .send()
        .await
        .context("connecting to Wikimedia recentchange stream")?
        .error_for_status()
        .context("Wikimedia recentchange stream returned an error status")?;

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut events = Vec::with_capacity(limit);

    while events.len() < limit {
        let chunk = tokio::time::timeout(watchdog_timeout, stream.next()).await;
        match chunk {
            Ok(Some(Ok(bytes))) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(frame_end) = buffer.find("\n\n") {
                    let frame = buffer[..frame_end].to_string();
                    buffer.drain(..frame_end + 2);
                    for event in extract_sse_json_frames(&(frame + "\n\n")) {
                        events.push(event.to_string());
                        if events.len() >= limit {
                            break;
                        }
                    }
                    if events.len() >= limit {
                        break;
                    }
                }
            }
            Ok(Some(Err(error))) => return Err(error).context("reading Wikimedia stream chunk"),
            Ok(None) => break,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "watchdog timeout: no data received for {} seconds",
                    watchdog_timeout.as_secs()
                ));
            }
        }
    }

    Ok(events)
}

pub async fn simulate_stalled_live_source_resets(
    watchdog_timeout: Duration,
    max_network_resets: u64,
) -> LiveSourceResult {
    let mut network_resets = 0;
    while network_resets < max_network_resets {
        let stalled_input = std::future::pending::<()>();
        if tokio::time::timeout(watchdog_timeout, stalled_input)
            .await
            .is_err()
        {
            network_resets += 1;
        }
    }

    LiveSourceResult {
        events: Vec::new(),
        network_resets,
    }
}

pub fn synthetic_recent_changes() -> Vec<String> {
    vec![
        r#"{"meta":{"domain":"en.wikipedia.org","dt":"2026-05-05T12:00:00Z"},"user":"Alice","bot":false,"server_name":"en.wikipedia.org","wiki":"enwiki","type":"edit","title":"Real-time system","id":1}"#.to_string(),
        r#"{"meta":{"domain":"commons.wikimedia.org","dt":"2026-05-05T12:00:00Z"},"user":"ImageBot","bot":true,"server_name":"commons.wikimedia.org","wiki":"commonswiki","type":"edit","title":"File:Telemetry.png","id":2}"#.to_string(),
        r#"{"meta":{"domain":"wikidata.org","dt":"2026-05-05T12:00:01Z"},"user":"Bala","bot":false,"server_name":"wikidata.org","wiki":"wikidatawiki","type":"edit","title":"Q42","id":3}"#.to_string(),
        r#"{"meta":{"domain":"en.wikipedia.org","dt":"2026-05-05T12:00:02Z"},"user":"Charlie","bot":false,"server_name":"en.wikipedia.org","wiki":"enwiki","type":"new","title":"Scheduling drift","id":4}"#.to_string(),
        r#"{"meta":{"domain":"en.wikipedia.org","dt":"2026-05-05T12:00:03Z"},"user":"MaintenanceBot","bot":true,"server_name":"en.wikipedia.org","wiki":"enwiki","type":"edit","title":"Template cleanup","id":5}"#.to_string(),
        r#"{"meta":{"domain":"de.wikipedia.org","dt":"2026-05-05T12:00:04Z"},"user":"Dana","bot":false,"server_name":"de.wikipedia.org","wiki":"dewiki","type":"edit","title":"Echtzeitsystem","id":6}"#.to_string(),
    ]
}
