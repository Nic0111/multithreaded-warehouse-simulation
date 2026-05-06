use crate::{
    Result,
    evidence::metrics::MetricsCollector,
    pipeline::common::{
        PipelineConfig, PipelineReport, PriorityScheduler, drain_queue_to_scheduler, enqueue_event,
        finalize_report, load_events_blocking, process_packet,
    },
    realtime::{failsafe::FailSafeController, queue::BoundedDropOldestChannel},
    sync::leaderboard::DomainLeaderboard,
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

pub fn run_threaded_pipeline(config: PipelineConfig) -> Result<PipelineReport> {
    let loaded = load_events_blocking(&config)?;
    let events = loaded.events;
    let queue = Arc::new((
        Mutex::new(BoundedDropOldestChannel::new(config.queue_capacity)),
        Condvar::new(),
    ));
    let done = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Mutex::new(MetricsCollector {
        network_resets: loaded.network_resets,
        ..Default::default()
    }));
    let leaderboard = Arc::new(DomainLeaderboard::default());
    let fail_safe = Arc::new(Mutex::new(FailSafeController::new(
        config.jitter_threshold,
        config.fail_safe_window,
        config.deadline_miss_limit,
    )));

    let producer_config = config.clone();
    let producer_queue = Arc::clone(&queue);
    let producer_done = Arc::clone(&done);
    let producer_metrics = Arc::clone(&metrics);

    let producer = thread::spawn(move || -> Result<()> {
        for (seq, payload) in events.into_iter().enumerate() {
            let (queue_lock, notify) = &*producer_queue;
            {
                let mut queue = queue_lock.lock().expect("queue mutex poisoned");
                let mut metrics = producer_metrics.lock().expect("metrics mutex poisoned");
                enqueue_event(&mut queue, &mut metrics, payload, seq as u64)?;
            }
            notify.notify_one();
            if !producer_config.ingest_pause.is_zero() {
                thread::sleep(producer_config.ingest_pause);
            }
        }
        producer_done.store(true, Ordering::Release);
        producer_queue.1.notify_all();
        Ok(())
    });

    let worker_config = config.clone();
    let worker_queue = Arc::clone(&queue);
    let worker_done = Arc::clone(&done);
    let worker_metrics = Arc::clone(&metrics);
    let worker_leaderboard = Arc::clone(&leaderboard);
    let worker_fail_safe = Arc::clone(&fail_safe);

    let worker = thread::spawn(move || -> Result<()> {
        if !worker_config.worker_start_delay.is_zero() {
            thread::sleep(worker_config.worker_start_delay);
        }
        let mut scheduler = PriorityScheduler::default();
        loop {
            {
                let (queue_lock, notify) = &*worker_queue;
                let mut queue = queue_lock.lock().expect("queue mutex poisoned");
                while queue.is_empty() && !worker_done.load(Ordering::Acquire) {
                    queue = notify
                        .wait(queue)
                        .expect("queue mutex poisoned while waiting");
                }
                drain_queue_to_scheduler(&mut queue, &mut scheduler);
            }

            if let Some(packet) = scheduler.pop_next() {
                let mut metrics = worker_metrics.lock().expect("metrics mutex poisoned");
                let mut fail_safe = worker_fail_safe.lock().expect("fail-safe mutex poisoned");
                process_packet(
                    packet,
                    &worker_leaderboard,
                    &mut fail_safe,
                    &mut metrics,
                    &worker_config,
                )?;
                continue;
            }

            if worker_done.load(Ordering::Acquire) {
                break;
            }
        }
        Ok(())
    });

    producer
        .join()
        .map_err(|_| anyhow::anyhow!("threaded producer panicked"))??;
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("threaded worker panicked"))??;

    let overflow_events = {
        let (queue_lock, _) = &*queue;
        queue_lock
            .lock()
            .expect("queue mutex poisoned")
            .take_overflow_events()
    };
    let metrics = Arc::try_unwrap(metrics)
        .map_err(|_| anyhow::anyhow!("threaded metrics still shared"))?
        .into_inner()
        .expect("metrics mutex poisoned");
    let leaderboard = Arc::try_unwrap(leaderboard)
        .map_err(|_| anyhow::anyhow!("threaded leaderboard still shared"))?;

    Ok(finalize_report(
        &config,
        metrics,
        overflow_events,
        &leaderboard,
    ))
}
