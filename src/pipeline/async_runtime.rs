use crate::{
    Result,
    evidence::metrics::MetricsCollector,
    pipeline::common::{
        PipelineConfig, PipelineReport, PriorityScheduler, drain_queue_to_scheduler, enqueue_event,
        finalize_report, load_events, process_packet,
    },
    realtime::{failsafe::FailSafeController, queue::BoundedDropOldestChannel},
    sync::leaderboard::DomainLeaderboard,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, Notify};

pub async fn run_async_pipeline(config: PipelineConfig) -> Result<PipelineReport> {
    let loaded = load_events(&config).await?;
    let events = loaded.events;
    let queue = Arc::new(Mutex::new(BoundedDropOldestChannel::new(
        config.queue_capacity,
    )));
    let notify = Arc::new(Notify::new());
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
    let producer_notify = Arc::clone(&notify);
    let producer_done = Arc::clone(&done);
    let producer_metrics = Arc::clone(&metrics);

    let producer = tokio::spawn(async move {
        for (seq, payload) in events.into_iter().enumerate() {
            {
                let mut queue = producer_queue.lock().await;
                let mut metrics = producer_metrics.lock().await;
                enqueue_event(&mut queue, &mut metrics, payload, seq as u64)?;
            }
            producer_notify.notify_one();
            if !producer_config.ingest_pause.is_zero() {
                tokio::time::sleep(producer_config.ingest_pause).await;
            }
        }
        producer_done.store(true, Ordering::Release);
        producer_notify.notify_waiters();
        Ok::<(), anyhow::Error>(())
    });

    let worker_config = config.clone();
    let worker_queue = Arc::clone(&queue);
    let worker_notify = Arc::clone(&notify);
    let worker_done = Arc::clone(&done);
    let worker_metrics = Arc::clone(&metrics);
    let worker_leaderboard = Arc::clone(&leaderboard);
    let worker_fail_safe = Arc::clone(&fail_safe);

    let worker = tokio::spawn(async move {
        if !worker_config.worker_start_delay.is_zero() {
            tokio::time::sleep(worker_config.worker_start_delay).await;
        }
        let mut scheduler = PriorityScheduler::default();
        loop {
            {
                let mut queue = worker_queue.lock().await;
                drain_queue_to_scheduler(&mut queue, &mut scheduler);
            }

            if let Some(packet) = scheduler.pop_next() {
                let mut metrics = worker_metrics.lock().await;
                let mut fail_safe = worker_fail_safe.lock().await;
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
                let queue = worker_queue.lock().await;
                if queue.is_empty() {
                    break;
                }
            }

            worker_notify.notified().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    producer.await??;
    worker.await??;

    let mut queue = queue.lock().await;
    let overflow_events = queue.take_overflow_events();
    drop(queue);

    let metrics = Arc::try_unwrap(metrics)
        .map_err(|_| anyhow::anyhow!("async metrics still shared"))?
        .into_inner();
    let leaderboard = Arc::try_unwrap(leaderboard)
        .map_err(|_| anyhow::anyhow!("async leaderboard still shared"))?;

    Ok(finalize_report(
        &config,
        metrics,
        overflow_events,
        &leaderboard,
    ))
}
