use rts_wiki_monitor::{
    pipeline::{
        Architecture, PipelineConfig, PipelineMode, ProcessingDelayProfile, run_async_pipeline,
        run_threaded_pipeline,
    },
    reporting::{repeat_events, watchdog_reset_evidence},
    source::{simulate_stalled_live_source_resets, synthetic_recent_changes},
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

fn synthetic_config(
    architecture: Architecture,
    name: &str,
    events: usize,
    queue_capacity: usize,
    processing_delay: Duration,
) -> PipelineConfig {
    let source = repeat_events(&synthetic_recent_changes(), events);
    let mut config = PipelineConfig::replay(
        architecture,
        PathBuf::from("data/replay/sample_recentchange.ndjson"),
        events,
    );
    config.mode = PipelineMode::Synthetic {
        name: name.to_string(),
        events: source,
    };
    config.queue_capacity = queue_capacity;
    config.processing_delay = ProcessingDelayProfile::Fixed(processing_delay);
    config.deadline_miss_limit = 2;
    config
}

#[tokio::test]
async fn async_replay_processes_deterministically() {
    let config = synthetic_config(
        Architecture::Async,
        "async-integration",
        24,
        64,
        Duration::from_micros(100),
    );
    let report = run_async_pipeline(config).await.unwrap();

    assert_eq!(report.summary.packets_seen, 24);
    assert_eq!(report.summary.packets_processed, 24);
    assert!(!report.summary.top_domains.is_empty());
}

#[test]
fn threaded_pipeline_logs_overflow_when_queue_is_saturated() {
    let config = synthetic_config(
        Architecture::Threaded,
        "threaded-overflow",
        80,
        2,
        Duration::from_millis(3),
    );
    let report = run_threaded_pipeline(config).unwrap();

    assert!(report.summary.overflow_count > 0);
    assert!(report.summary.packets_seen >= report.summary.packets_processed);
}

#[tokio::test]
async fn fail_safe_drops_low_priority_bot_edits_under_timing_violations() {
    let mut config = synthetic_config(
        Architecture::Async,
        "fail-safe",
        80,
        12,
        Duration::from_millis(3),
    );
    config.jitter_threshold = Duration::from_millis(2);
    config.deadline_miss_limit = 1;

    let report = run_async_pipeline(config).await.unwrap();

    assert!(report.summary.degraded_transitions > 0);
    assert!(report.summary.degraded_bot_drops > 0);
}

#[test]
fn threaded_leaderboard_remains_consistent_under_concurrency() {
    let config = synthetic_config(
        Architecture::Threaded,
        "leaderboard",
        36,
        16,
        Duration::from_micros(50),
    );
    let report = run_threaded_pipeline(config).unwrap();
    let total_leaderboard_count: u64 = report
        .summary
        .top_domains
        .iter()
        .map(|(_, count)| *count)
        .sum();

    assert!(total_leaderboard_count > 0);
    assert!(report.summary.top_domains.len() <= 3);
}

#[tokio::test]
async fn watchdog_evidence_uses_real_timeout_without_slowing_tests() {
    let timeout = Duration::from_millis(50);
    let started = Instant::now();
    let evidence = watchdog_reset_evidence(timeout).await;

    assert_eq!(evidence.event, "Network Reset");
    assert_eq!(evidence.result, "watchdog timeout triggered successfully");
    assert!(evidence.observed_silence_ms >= 45);
    assert!(started.elapsed() >= timeout);
}

#[tokio::test]
async fn priority_pressure_proves_humans_have_lower_drift() {
    let mut events = Vec::new();
    for id in 0..40 {
        events.push(format!(
            r#"{{"user":"Bot","bot":true,"server_name":"en.wikipedia.org","title":"bot{id}"}}"#
        ));
    }
    for id in 40..52 {
        events.push(format!(
            r#"{{"user":"Human","bot":false,"server_name":"en.wikipedia.org","title":"human{id}"}}"#
        ));
    }

    let mut config = PipelineConfig::replay(
        Architecture::Async,
        PathBuf::from("data/replay/sample_recentchange.ndjson"),
        events.len(),
    );
    config.mode = PipelineMode::Synthetic {
        name: "priority-pressure-test".to_string(),
        events,
    };
    config.queue_capacity = 80;
    config.worker_start_delay = Duration::from_millis(10);
    config.processing_delay = ProcessingDelayProfile::Fixed(Duration::from_micros(200));

    let report = run_async_pipeline(config).await.unwrap();

    assert!(report.summary.human_drift.p99_us < report.summary.bot_drift.p99_us);
}

#[tokio::test]
async fn phased_fail_safe_enters_and_recovers_in_pipeline() {
    let mut events = vec![
        r#"{"user":"OverloadHuman","bot":false,"server_name":"en.wikipedia.org","title":"overload"}"#
            .to_string(),
    ];
    for id in 1..10 {
        events.push(format!(
            r#"{{"user":"Bot","bot":true,"server_name":"commons.wikimedia.org","title":"bot{id}"}}"#
        ));
    }
    for id in 10..40 {
        events.push(format!(
            r#"{{"user":"Human","bot":false,"server_name":"wikidata.org","title":"stable{id}"}}"#
        ));
    }

    let mut config = PipelineConfig::replay(
        Architecture::Async,
        PathBuf::from("data/replay/sample_recentchange.ndjson"),
        events.len(),
    );
    config.mode = PipelineMode::Synthetic {
        name: "phased-fail-safe-test".to_string(),
        events,
    };
    config.queue_capacity = 80;
    config.ingest_pause = Duration::from_micros(500);
    config.processing_delay = ProcessingDelayProfile::Phased {
        overload_packets: 10,
        overload_delay: Duration::from_millis(3),
        recovery_delay: Duration::from_micros(100),
    };
    config.fail_safe_window = 8;
    config.deadline_miss_limit = 1;

    let report = run_async_pipeline(config).await.unwrap();

    assert!(report.summary.degraded_transitions > 0);
    assert!(report.summary.recovered_transitions > 0);
    assert!(report.summary.degraded_bot_drops > 0);
}

#[tokio::test]
async fn stalled_live_source_counts_watchdog_retries() {
    let result = simulate_stalled_live_source_resets(Duration::from_millis(20), 2).await;

    assert_eq!(result.network_resets, 2);
    assert!(result.events.is_empty());
}
