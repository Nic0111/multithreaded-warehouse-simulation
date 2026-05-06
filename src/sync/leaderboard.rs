use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Instant,
};

#[derive(Debug, Default)]
pub struct DomainLeaderboard {
    counts: Mutex<HashMap<String, u64>>,
}

impl DomainLeaderboard {
    pub fn update(&self, domain: &str) {
        let mut counts = self.counts.lock().expect("leaderboard lock poisoned");
        *counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    pub fn top_n(&self, n: usize) -> Vec<(String, u64)> {
        let mut counts = self
            .counts
            .lock()
            .expect("leaderboard lock poisoned")
            .iter()
            .map(|(domain, count)| (domain.clone(), *count))
            .collect::<Vec<_>>();
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        counts.truncate(n);
        counts
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SynchronizationReport {
    pub strategy: String,
    pub threads: usize,
    pub updates: usize,
    pub total_ms: f64,
    pub throughput_updates_per_sec: f64,
    pub p99_wait_us: f64,
}

pub fn compare_synchronization(
    threads: usize,
    updates_per_thread: usize,
) -> Vec<SynchronizationReport> {
    vec![
        benchmark_mutex(threads, updates_per_thread),
        benchmark_rwlock(threads, updates_per_thread),
        benchmark_atomic(threads, updates_per_thread),
    ]
}

fn benchmark_mutex(threads: usize, updates_per_thread: usize) -> SynchronizationReport {
    let counts = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    run_lock_benchmark("Mutex", threads, updates_per_thread, move |domain| {
        let started = Instant::now();
        let mut counts = counts.lock().expect("mutex poisoned");
        let wait = started.elapsed().as_nanos();
        *counts.entry(domain.to_string()).or_insert(0) += 1;
        wait
    })
}

fn benchmark_rwlock(threads: usize, updates_per_thread: usize) -> SynchronizationReport {
    let counts = Arc::new(RwLock::new(HashMap::<String, u64>::new()));
    run_lock_benchmark("RwLock", threads, updates_per_thread, move |domain| {
        let started = Instant::now();
        let mut counts = counts.write().expect("rwlock poisoned");
        let wait = started.elapsed().as_nanos();
        *counts.entry(domain.to_string()).or_insert(0) += 1;
        wait
    })
}

fn benchmark_atomic(threads: usize, updates_per_thread: usize) -> SynchronizationReport {
    let domains = Arc::new([
        ("en.wikipedia.org", AtomicU64::new(0)),
        ("commons.wikimedia.org", AtomicU64::new(0)),
        ("wikidata.org", AtomicU64::new(0)),
    ]);
    run_lock_benchmark("Atomic", threads, updates_per_thread, move |domain| {
        let started = Instant::now();
        let slot = match domain {
            "commons.wikimedia.org" => &domains[1].1,
            "wikidata.org" => &domains[2].1,
            _ => &domains[0].1,
        };
        let wait = started.elapsed().as_nanos();
        slot.fetch_add(1, Ordering::Relaxed);
        wait
    })
}

fn run_lock_benchmark<F>(
    strategy: &str,
    threads: usize,
    updates_per_thread: usize,
    updater: F,
) -> SynchronizationReport
where
    F: Fn(&str) -> u128 + Send + Sync + 'static,
{
    let updater = Arc::new(updater);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(threads);

    for thread_id in 0..threads {
        let updater = Arc::clone(&updater);
        handles.push(thread::spawn(move || {
            let domains = ["en.wikipedia.org", "commons.wikimedia.org", "wikidata.org"];
            let mut waits = Vec::with_capacity(updates_per_thread);
            for update in 0..updates_per_thread {
                let domain = domains[(thread_id + update) % domains.len()];
                waits.push(updater(domain));
            }
            waits
        }));
    }

    let mut waits = Vec::with_capacity(threads * updates_per_thread);
    for handle in handles {
        waits.extend(handle.join().expect("synchronization worker panicked"));
    }

    let total = started.elapsed();
    waits.sort_unstable();
    let p99_wait_us = if waits.is_empty() {
        0.0
    } else {
        let index = ((waits.len() as f64 - 1.0) * 0.99).ceil() as usize;
        waits[index.min(waits.len() - 1)] as f64 / 1_000.0
    };
    let updates = threads * updates_per_thread;

    SynchronizationReport {
        strategy: strategy.to_string(),
        threads,
        updates,
        total_ms: total.as_secs_f64() * 1_000.0,
        throughput_updates_per_sec: updates as f64 / total.as_secs_f64().max(f64::EPSILON),
        p99_wait_us,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaderboard_returns_top_three() {
        let leaderboard = DomainLeaderboard::default();
        leaderboard.update("en.wikipedia.org");
        leaderboard.update("wikidata.org");
        leaderboard.update("en.wikipedia.org");
        leaderboard.update("commons.wikimedia.org");

        assert_eq!(leaderboard.top_n(3)[0], ("en.wikipedia.org".to_string(), 2));
    }
}
