use crate::{realtime::priority::Priority, realtime::queue::OverflowEvent};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PacketMetric {
    pub seq: u64,
    pub priority: Priority,
    pub domain: String,
    pub latency_ns: u128,
    pub processing_ns: u128,
    pub drift_ns: i128,
    pub deadline_missed: bool,
    pub degraded_mode: bool,
    pub hot_path_allocations: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PercentileSummary {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSummary {
    pub arch: String,
    pub mode: String,
    pub packets_seen: u64,
    pub packets_processed: u64,
    pub degraded_bot_drops: u64,
    pub overflow_count: u64,
    pub deadline_misses: u64,
    pub network_resets: u64,
    pub degraded_transitions: u64,
    pub recovered_transitions: u64,
    pub latency: PercentileSummary,
    pub processing: PercentileSummary,
    pub human_drift: PercentileSummary,
    pub bot_drift: PercentileSummary,
    pub max_queue_depth: usize,
    pub top_domains: Vec<(String, u64)>,
    pub overflow_events: Vec<OverflowEvent>,
}

#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub packets_seen: u64,
    pub packet_metrics: Vec<PacketMetric>,
    pub degraded_bot_drops: u64,
    pub network_resets: u64,
    pub degraded_transitions: u64,
    pub recovered_transitions: u64,
    pub max_queue_depth: usize,
}

impl MetricsCollector {
    pub fn record_seen(&mut self, queue_depth: usize) {
        self.packets_seen += 1;
        self.max_queue_depth = self.max_queue_depth.max(queue_depth);
    }

    pub fn record_packet(&mut self, metric: PacketMetric) {
        self.packet_metrics.push(metric);
    }

    pub fn record_degraded_drop(&mut self) {
        self.degraded_bot_drops += 1;
    }

    pub fn record_network_reset(&mut self) {
        self.network_resets += 1;
    }

    pub fn summary(
        &self,
        arch: &str,
        mode: &str,
        overflow_events: Vec<OverflowEvent>,
        top_domains: Vec<(String, u64)>,
    ) -> MetricsSummary {
        let deadline_misses = self
            .packet_metrics
            .iter()
            .filter(|metric| metric.deadline_missed)
            .count() as u64;
        let latency_values = self
            .packet_metrics
            .iter()
            .map(|metric| metric.latency_ns)
            .collect::<Vec<_>>();
        let processing_values = self
            .packet_metrics
            .iter()
            .map(|metric| metric.processing_ns)
            .collect::<Vec<_>>();
        let human_drifts = self
            .packet_metrics
            .iter()
            .filter(|metric| metric.priority == Priority::Human)
            .map(|metric| metric.drift_ns.unsigned_abs())
            .collect::<Vec<_>>();
        let bot_drifts = self
            .packet_metrics
            .iter()
            .filter(|metric| metric.priority == Priority::Bot)
            .map(|metric| metric.drift_ns.unsigned_abs())
            .collect::<Vec<_>>();

        MetricsSummary {
            arch: arch.to_string(),
            mode: mode.to_string(),
            packets_seen: self.packets_seen,
            packets_processed: self.packet_metrics.len() as u64,
            degraded_bot_drops: self.degraded_bot_drops,
            overflow_count: overflow_events.len() as u64,
            deadline_misses,
            network_resets: self.network_resets,
            degraded_transitions: self.degraded_transitions,
            recovered_transitions: self.recovered_transitions,
            latency: percentiles_us(&latency_values),
            processing: percentiles_us(&processing_values),
            human_drift: percentiles_us(&human_drifts),
            bot_drift: percentiles_us(&bot_drifts),
            max_queue_depth: self.max_queue_depth,
            top_domains,
            overflow_events,
        }
    }
}

pub fn percentiles_us(values: &[u128]) -> PercentileSummary {
    if values.is_empty() {
        return PercentileSummary {
            p50_us: 0.0,
            p90_us: 0.0,
            p99_us: 0.0,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    PercentileSummary {
        p50_us: percentile_value_us(&sorted, 0.50),
        p90_us: percentile_value_us(&sorted, 0.90),
        p99_us: percentile_value_us(&sorted, 0.99),
    }
}

fn percentile_value_us(sorted: &[u128], percentile: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)] as f64 / 1_000.0
}

pub fn signed_duration_ns(actual: std::time::Instant, expected: std::time::Instant) -> i128 {
    if actual >= expected {
        actual.duration_since(expected).as_nanos() as i128
    } else {
        -(expected.duration_since(actual).as_nanos() as i128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_tail_percentiles() {
        let summary = percentiles_us(&[1_000, 2_000, 3_000, 10_000]);
        assert_eq!(summary.p50_us, 3.0);
        assert_eq!(summary.p90_us, 10.0);
        assert_eq!(summary.p99_us, 10.0);
    }
}
