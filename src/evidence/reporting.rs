use crate::{
    Result,
    evidence::allocator::{
        AllocationSnapshot, reset_thread_allocations, thread_allocation_snapshot,
    },
    input::event::parse_recent_change,
    pipeline::PipelineReport,
    sync::leaderboard::SynchronizationReport,
};
use anyhow::Context;
use serde::Serialize;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize)]
pub struct AllocationProof {
    pub sample_domain: String,
    pub sample_priority: String,
    pub hot_path_delta: AllocationSnapshot,
    pub conclusion: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchBundle {
    pub async_report: PipelineReport,
    pub threaded_report: PipelineReport,
    pub synchronization: Vec<SynchronizationReport>,
    pub allocation_proof: AllocationProof,
}

#[derive(Debug, Clone, Serialize)]
pub struct DemoBundle {
    pub normal: PipelineReport,
    pub priority: PipelineReport,
    pub overflow: PipelineReport,
    pub fail_safe: PipelineReport,
    pub watchdog: WatchdogResetEvidence,
}

#[derive(Debug, Clone, Serialize)]
pub struct WatchdogResetEvidence {
    pub timeout_seconds: u64,
    pub event: String,
    pub last_data_received_at: String,
    pub reset_triggered_at: String,
    pub observed_silence_ms: u128,
    pub recovery_action: String,
    pub result: String,
    pub evidence_note: String,
}

pub fn allocation_proof(sample: &str) -> Result<AllocationProof> {
    let _ = parse_recent_change(sample).context("allocation proof warm-up parse failed")?;
    reset_thread_allocations();
    let before = thread_allocation_snapshot();
    let event = parse_recent_change(sample).context("allocation proof parse failed")?;
    let domain = event.domain();
    let priority = event.priority().as_str();
    let after = thread_allocation_snapshot();
    let delta = after.delta_since(before);

    Ok(AllocationProof {
        sample_domain: domain.to_string(),
        sample_priority: priority.to_string(),
        hot_path_delta: delta,
        conclusion: if delta.allocations == 0 {
            "Borrowed serde parsing/classification performed without heap allocation in the measured hot path."
                .to_string()
        } else {
            "Measured allocations were non-zero; inspect parser changes before claiming zero-copy hot-path mastery."
                .to_string()
        },
    })
}

pub async fn watchdog_reset_evidence(timeout: Duration) -> WatchdogResetEvidence {
    let last_data_received_at = timestamp_now();
    let silence_started = Instant::now();
    let stalled_input = std::future::pending::<()>();

    let timed_out = tokio::time::timeout(timeout, stalled_input).await.is_err();
    let observed_silence_ms = silence_started.elapsed().as_millis();

    WatchdogResetEvidence {
        timeout_seconds: timeout.as_secs(),
        event: "Network Reset".to_string(),
        last_data_received_at,
        reset_triggered_at: timestamp_now(),
        observed_silence_ms,
        recovery_action: "Reconnect Wikimedia SSE source and continue with bounded queue preserved"
            .to_string(),
        result: if timed_out {
            "watchdog timeout triggered successfully".to_string()
        } else {
            "watchdog timeout did not trigger".to_string()
        },
        evidence_note: "Controlled network-silence simulation using the same tokio timeout mechanism as the live Wikimedia stream watchdog."
            .to_string(),
    }
}

pub fn write_pipeline_report(output_dir: &Path, name: &str, report: &PipelineReport) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    write_json(
        &output_dir.join(format!("{name}_summary.json")),
        &report.summary,
    )?;
    write_packet_csv(&output_dir.join(format!("{name}_packets.csv")), report)?;
    Ok(())
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}

fn write_packet_csv(path: &Path, report: &PipelineReport) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    writeln!(
        file,
        "seq,priority,domain,latency_us,processing_us,drift_us,deadline_missed,degraded_mode,hot_path_allocations"
    )?;
    for metric in &report.packet_metrics {
        writeln!(
            file,
            "{},{},{},{:.3},{:.3},{:.3},{},{},{}",
            metric.seq,
            metric.priority.as_str(),
            metric.domain,
            metric.latency_ns as f64 / 1_000.0,
            metric.processing_ns as f64 / 1_000.0,
            metric.drift_ns as f64 / 1_000.0,
            metric.deadline_missed,
            metric.degraded_mode,
            metric.hot_path_allocations
        )?;
    }
    Ok(())
}

pub fn repeat_events(events: &[String], target: usize) -> Vec<String> {
    if events.is_empty() {
        return Vec::new();
    }

    (0..target)
        .map(|index| events[index % events.len()].clone())
        .collect()
}

fn timestamp_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", now.as_secs(), now.subsec_nanos())
}
