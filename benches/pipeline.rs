use criterion::{Criterion, criterion_group, criterion_main};
use rts_wiki_monitor::{
    event::parse_recent_change,
    pipeline::{
        Architecture, PipelineConfig, PipelineMode, ProcessingDelayProfile, run_async_pipeline,
        run_threaded_pipeline,
    },
    reporting::repeat_events,
    source::synthetic_recent_changes,
};
use std::{path::PathBuf, time::Duration};

fn config(architecture: Architecture, events: usize) -> PipelineConfig {
    let source = repeat_events(&synthetic_recent_changes(), events);
    let mut config = PipelineConfig::replay(
        architecture,
        PathBuf::from("data/replay/sample_recentchange.ndjson"),
        events,
    );
    config.mode = PipelineMode::Synthetic {
        name: "criterion".to_string(),
        events: source,
    };
    config.queue_capacity = events.max(64);
    config.processing_delay = ProcessingDelayProfile::Fixed(Duration::from_micros(150));
    config
}

fn criterion_benchmark(c: &mut Criterion) {
    let sample = synthetic_recent_changes().remove(0);

    c.bench_function("zero_copy_parse_hot_path", |b| {
        b.iter(|| {
            let event = parse_recent_change(std::hint::black_box(&sample)).unwrap();
            std::hint::black_box((event.domain(), event.priority()));
        })
    });

    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("async_pipeline_p99_120_events", |b| {
        b.iter(|| {
            let report = runtime
                .block_on(run_async_pipeline(config(Architecture::Async, 120)))
                .unwrap();
            std::hint::black_box(report.summary.latency.p99_us);
        })
    });

    c.bench_function("threaded_pipeline_p99_120_events", |b| {
        b.iter(|| {
            let report = run_threaded_pipeline(config(Architecture::Threaded, 120)).unwrap();
            std::hint::black_box(report.summary.latency.p99_us);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
