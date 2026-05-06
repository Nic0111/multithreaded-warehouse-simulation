use crate::{
    Result,
    evidence::{
        allocator::{AllocationSnapshot, thread_allocation_snapshot},
        metrics::{MetricsCollector, MetricsSummary, PacketMetric, signed_duration_ns},
    },
    input::{event::parse_recent_change, source},
    realtime::{
        failsafe::{FailSafeController, FailSafeState, FailSafeTransition},
        priority::Priority,
        queue::{BoundedDropOldestChannel, QueueItem},
    },
    sync::leaderboard::DomainLeaderboard,
};
use anyhow::Context;
use serde::Serialize;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Architecture {
    Async,
    Threaded,
}

impl Architecture {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Async => "async",
            Self::Threaded => "threaded",
        }
    }
}

#[derive(Debug, Clone)]
pub enum PipelineMode {
    Replay { input: PathBuf },
    Live,
    Synthetic { name: String, events: Vec<String> },
}

impl PipelineMode {
    pub fn label(&self) -> String {
        match self {
            Self::Replay { input } => format!("replay:{}", input.display()),
            Self::Live => "live".to_string(),
            Self::Synthetic { name, .. } => format!("synthetic:{name}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ProcessingDelayProfile {
    Fixed(Duration),
    Phased {
        overload_packets: u64,
        overload_delay: Duration,
        recovery_delay: Duration,
    },
}

impl ProcessingDelayProfile {
    pub fn delay_for(&self, seq: u64) -> Duration {
        match self {
            Self::Fixed(delay) => *delay,
            Self::Phased {
                overload_packets,
                overload_delay,
                recovery_delay,
            } => {
                if seq < *overload_packets {
                    *overload_delay
                } else {
                    *recovery_delay
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub architecture: Architecture,
    pub mode: PipelineMode,
    pub queue_capacity: usize,
    pub limit: usize,
    pub expected_period: Duration,
    pub micro_deadline: Duration,
    pub jitter_threshold: Duration,
    pub fail_safe_window: usize,
    pub deadline_miss_limit: usize,
    pub processing_delay: ProcessingDelayProfile,
    pub ingest_pause: Duration,
    pub worker_start_delay: Duration,
    pub watchdog_timeout: Duration,
    pub max_network_resets: u64,
}

impl PipelineConfig {
    pub fn replay(architecture: Architecture, input: PathBuf, limit: usize) -> Self {
        Self {
            architecture,
            mode: PipelineMode::Replay { input },
            queue_capacity: 256,
            limit,
            expected_period: Duration::from_micros(500),
            micro_deadline: Duration::from_millis(2),
            jitter_threshold: Duration::from_millis(2),
            fail_safe_window: 32,
            deadline_miss_limit: 4,
            processing_delay: ProcessingDelayProfile::Fixed(Duration::ZERO),
            ingest_pause: Duration::ZERO,
            worker_start_delay: Duration::ZERO,
            watchdog_timeout: Duration::from_secs(10),
            max_network_resets: 3,
        }
    }

    pub fn live(architecture: Architecture, limit: usize) -> Self {
        let mut config = Self::replay(architecture, PathBuf::new(), limit);
        config.mode = PipelineMode::Live;
        config.ingest_pause = Duration::from_millis(1);
        config
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineReport {
    pub summary: MetricsSummary,
    pub packet_metrics: Vec<PacketMetric>,
}

#[derive(Debug, Clone)]
pub struct RawPacket {
    pub seq: u64,
    pub payload: Arc<str>,
    pub ingested_at: Instant,
    pub expected_ready_at: Instant,
    pub priority: Priority,
}

impl QueueItem for RawPacket {
    fn seq(&self) -> u64 {
        self.seq
    }
}

#[derive(Debug, Default)]
pub struct PriorityScheduler {
    human: std::collections::VecDeque<RawPacket>,
    bot: std::collections::VecDeque<RawPacket>,
}

impl PriorityScheduler {
    pub fn push(&mut self, packet: RawPacket) {
        match packet.priority {
            Priority::Human => self.human.push_back(packet),
            Priority::Bot => self.bot.push_back(packet),
        }
    }

    pub fn pop_next(&mut self) -> Option<RawPacket> {
        self.human.pop_front().or_else(|| self.bot.pop_front())
    }
}

#[derive(Debug, Clone, Default)]
pub struct LoadedEvents {
    pub events: Vec<String>,
    pub network_resets: u64,
}

pub async fn load_events(config: &PipelineConfig) -> Result<LoadedEvents> {
    match &config.mode {
        PipelineMode::Replay { input } => Ok(LoadedEvents {
            events: source::read_replay_file(input, config.limit)
                .with_context(|| format!("failed reading replay file {}", input.display()))?,
            network_resets: 0,
        }),
        PipelineMode::Live => {
            let live = source::fetch_live_events_with_reconnect(
                config.limit,
                config.watchdog_timeout,
                config.max_network_resets,
            )
            .await?;
            Ok(LoadedEvents {
                events: live.events,
                network_resets: live.network_resets,
            })
        }
        PipelineMode::Synthetic { events, .. } => Ok(LoadedEvents {
            events: events.iter().take(config.limit).cloned().collect(),
            network_resets: 0,
        }),
    }
}

pub fn load_events_blocking(config: &PipelineConfig) -> Result<LoadedEvents> {
    match &config.mode {
        PipelineMode::Replay { input } => Ok(LoadedEvents {
            events: source::read_replay_file(input, config.limit)
                .with_context(|| format!("failed reading replay file {}", input.display()))?,
            network_resets: 0,
        }),
        PipelineMode::Live => {
            let runtime =
                tokio::runtime::Runtime::new().context("failed creating live source runtime")?;
            let live = runtime.block_on(source::fetch_live_events_with_reconnect(
                config.limit,
                config.watchdog_timeout,
                config.max_network_resets,
            ))?;
            Ok(LoadedEvents {
                events: live.events,
                network_resets: live.network_resets,
            })
        }
        PipelineMode::Synthetic { events, .. } => Ok(LoadedEvents {
            events: events.iter().take(config.limit).cloned().collect(),
            network_resets: 0,
        }),
    }
}

pub fn enqueue_event(
    queue: &mut BoundedDropOldestChannel<RawPacket>,
    metrics: &mut MetricsCollector,
    payload: String,
    seq: u64,
) -> Result<()> {
    let priority = parse_recent_change(&payload)
        .with_context(|| format!("failed parsing event before enqueue seq={seq}"))?
        .priority();
    let ready_at = Instant::now();
    let packet = RawPacket {
        seq,
        payload: Arc::<str>::from(payload),
        ingested_at: ready_at,
        expected_ready_at: ready_at,
        priority,
    };
    queue.send_drop_oldest(packet, now_ns());
    metrics.record_seen(queue.len());
    Ok(())
}

pub fn process_packet(
    packet: RawPacket,
    leaderboard: &DomainLeaderboard,
    fail_safe: &mut FailSafeController,
    metrics: &mut MetricsCollector,
    config: &PipelineConfig,
) -> Result<()> {
    if fail_safe.is_degraded() && packet.priority == Priority::Bot {
        metrics.record_degraded_drop();
        return Ok(());
    }

    let actual_start = Instant::now();
    let hot_before = thread_allocation_snapshot();
    let event = parse_recent_change(&packet.payload)
        .with_context(|| format!("failed processing packet seq={}", packet.seq))?;
    let priority = event.priority();
    let domain = event.domain();
    let hot_after = thread_allocation_snapshot();
    let hot_delta: AllocationSnapshot = hot_after.delta_since(hot_before);

    simulate_work(config.processing_delay.delay_for(packet.seq));
    leaderboard.update(domain);

    let finished = Instant::now();
    let processing_ns = finished.duration_since(actual_start).as_nanos();
    let latency_ns = finished.duration_since(packet.ingested_at).as_nanos();
    let deadline_missed = processing_ns > config.micro_deadline.as_nanos();

    match fail_safe.observe(processing_ns, deadline_missed) {
        FailSafeTransition::EnteredDegraded => metrics.degraded_transitions += 1,
        FailSafeTransition::RecoveredNormal => metrics.recovered_transitions += 1,
        FailSafeTransition::None => {}
    }

    metrics.record_packet(PacketMetric {
        seq: packet.seq,
        priority,
        domain: domain.to_string(),
        latency_ns,
        processing_ns,
        drift_ns: signed_duration_ns(actual_start, packet.expected_ready_at),
        deadline_missed,
        degraded_mode: fail_safe.state() == FailSafeState::Degraded,
        hot_path_allocations: hot_delta.allocations,
    });

    Ok(())
}

pub fn drain_queue_to_scheduler(
    queue: &mut BoundedDropOldestChannel<RawPacket>,
    scheduler: &mut PriorityScheduler,
) {
    while let Some(packet) = queue.try_recv() {
        scheduler.push(packet);
    }
}

pub fn finalize_report(
    config: &PipelineConfig,
    metrics: MetricsCollector,
    overflow_events: Vec<crate::realtime::queue::OverflowEvent>,
    leaderboard: &DomainLeaderboard,
) -> PipelineReport {
    let packet_metrics = metrics.packet_metrics.clone();
    let summary = metrics.summary(
        config.architecture.as_str(),
        &config.mode.label(),
        overflow_events,
        leaderboard.top_n(3),
    );
    PipelineReport {
        summary,
        packet_metrics,
    }
}

pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

pub fn simulate_work(duration: Duration) {
    if duration.is_zero() {
        return;
    }

    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::event::parse_recent_change;

    #[test]
    fn scheduler_prioritizes_humans() {
        let bot_payload = r#"{"bot":true,"server_name":"en.wikipedia.org"}"#.to_string();
        let human_payload = r#"{"bot":false,"server_name":"en.wikipedia.org"}"#.to_string();
        let mut scheduler = PriorityScheduler::default();
        let now = Instant::now();
        scheduler.push(RawPacket {
            seq: 1,
            priority: parse_recent_change(&bot_payload).unwrap().priority(),
            payload: Arc::from(bot_payload),
            ingested_at: now,
            expected_ready_at: now,
        });
        scheduler.push(RawPacket {
            seq: 2,
            priority: parse_recent_change(&human_payload).unwrap().priority(),
            payload: Arc::from(human_payload),
            ingested_at: now,
            expected_ready_at: now,
        });

        assert_eq!(scheduler.pop_next().unwrap().seq, 2);
        assert_eq!(scheduler.pop_next().unwrap().seq, 1);
    }
}
