use crate::{
    Result,
    evidence::reporting::{
        BenchBundle, DemoBundle, allocation_proof, repeat_events, watchdog_reset_evidence,
        write_json, write_pipeline_report,
    },
    input::source::{fetch_live_events, synthetic_recent_changes, write_replay_file},
    pipeline::{
        Architecture, PipelineConfig, PipelineMode, ProcessingDelayProfile, run_async_pipeline,
        run_threaded_pipeline,
    },
    sync::leaderboard::compare_synchronization,
};
use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use std::{path::PathBuf, time::Duration};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "RTS2601 Wikimedia real-time monitoring engine"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Capture live Wikimedia SSE events into an NDJSON replay fixture.
    Capture {
        #[arg(long, default_value = "data/replay/captured_recentchange.ndjson")]
        output: PathBuf,
        #[arg(long, default_value_t = 200)]
        limit: usize,
        #[arg(long, default_value_t = 10)]
        watchdog_seconds: u64,
    },
    /// Run one architecture against live or replay input.
    Run {
        #[arg(long, value_enum)]
        arch: ArchArg,
        #[arg(long, value_enum, default_value_t = ModeArg::Replay)]
        mode: ModeArg,
        #[arg(long, default_value = "data/replay/sample_recentchange.ndjson")]
        input: PathBuf,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, default_value_t = 256)]
        queue_capacity: usize,
        #[arg(long, default_value_t = 0)]
        processing_delay_us: u64,
        #[arg(long, default_value = "output/run")]
        output_dir: PathBuf,
    },
    /// Generate evidence reports for architecture, synchronization, and allocation comparisons.
    Bench {
        #[arg(long, default_value = "output/bench")]
        output_dir: PathBuf,
        #[arg(long, default_value_t = 300)]
        events: usize,
    },
    /// Run scripted evidence scenarios for presentation/demo.
    Demo {
        #[arg(long, default_value = "output/demo")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value_t = DemoScenario::All)]
        scenario: DemoScenario,
        #[arg(long, value_enum, default_value_t = DemoArch::Mixed)]
        arch: DemoArch,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ArchArg {
    Async,
    Threaded,
}

impl From<ArchArg> for Architecture {
    fn from(value: ArchArg) -> Self {
        match value {
            ArchArg::Async => Self::Async,
            ArchArg::Threaded => Self::Threaded,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ModeArg {
    Replay,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DemoScenario {
    All,
    Normal,
    Priority,
    Overflow,
    FailSafe,
    Watchdog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DemoArch {
    Mixed,
    Async,
    Threaded,
    Both,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.command {
            Commands::Capture {
                output,
                limit,
                watchdog_seconds,
            } => {
                let events =
                    fetch_live_events(limit, Duration::from_secs(watchdog_seconds)).await?;
                write_replay_file(&output, &events)?;
                println!("captured {} events to {}", events.len(), output.display());
            }
            Commands::Run {
                arch,
                mode,
                input,
                limit,
                queue_capacity,
                processing_delay_us,
                output_dir,
            } => {
                let architecture = Architecture::from(arch);
                let mut config = match mode {
                    ModeArg::Replay => PipelineConfig::replay(architecture, input, limit),
                    ModeArg::Live => PipelineConfig::live(architecture, limit),
                };
                config.queue_capacity = queue_capacity;
                config.processing_delay =
                    ProcessingDelayProfile::Fixed(Duration::from_micros(processing_delay_us));

                let report = run_for_architecture(config).await?;
                let name = report.summary.arch.clone();
                write_pipeline_report(&output_dir, &name, &report)?;
                println!(
                    "{} processed {} packets, p99 latency {:.3}us, deadline misses {}",
                    report.summary.arch,
                    report.summary.packets_processed,
                    report.summary.latency.p99_us,
                    report.summary.deadline_misses
                );
                println!("evidence written to {}", output_dir.display());
            }
            Commands::Bench { output_dir, events } => {
                let source = repeat_events(&synthetic_recent_changes(), events);
                let mut async_config = PipelineConfig::replay(
                    Architecture::Async,
                    PathBuf::from("data/replay/sample_recentchange.ndjson"),
                    events,
                );
                async_config.mode = PipelineMode::Synthetic {
                    name: "criterion-comparable-spike".to_string(),
                    events: source.clone(),
                };
                async_config.queue_capacity = events.max(64);
                async_config.processing_delay =
                    ProcessingDelayProfile::Fixed(Duration::from_micros(250));

                let mut threaded_config = async_config.clone();
                threaded_config.architecture = Architecture::Threaded;

                let async_report = run_async_pipeline(async_config).await?;
                let threaded_report = run_threaded_pipeline(threaded_config)?;
                let synchronization = compare_synchronization(8, 10_000);
                let proof_sample = source
                    .first()
                    .context("synthetic source unexpectedly empty for allocation proof")?;
                let proof = allocation_proof(proof_sample)?;

                let bundle = BenchBundle {
                    async_report,
                    threaded_report,
                    synchronization,
                    allocation_proof: proof,
                };
                write_json(&output_dir.join("benchmark_bundle.json"), &bundle)?;
                write_pipeline_report(&output_dir, "async_bench", &bundle.async_report)?;
                write_pipeline_report(&output_dir, "threaded_bench", &bundle.threaded_report)?;
                println!("benchmark evidence written to {}", output_dir.display());
            }
            Commands::Demo {
                output_dir,
                scenario,
                arch,
            } => {
                run_demo_command(output_dir, scenario, arch).await?;
            }
        }

        Ok(())
    }
}

async fn run_demo_command(
    output_dir: PathBuf,
    scenario: DemoScenario,
    arch: DemoArch,
) -> Result<()> {
    if scenario == DemoScenario::Watchdog && matches!(arch, DemoArch::Async | DemoArch::Threaded) {
        anyhow::bail!(
            "watchdog demo is architecture-neutral; use `--scenario watchdog` without `--arch async|threaded`"
        );
    }

    if scenario == DemoScenario::All && arch == DemoArch::Mixed {
        let bundle = run_default_demo_bundle().await?;
        write_json(&output_dir.join("demo_bundle.json"), &bundle)?;
        write_json(&output_dir.join("watchdog_summary.json"), &bundle.watchdog)?;
        write_pipeline_report(&output_dir, "normal", &bundle.normal)?;
        write_pipeline_report(&output_dir, "priority", &bundle.priority)?;
        write_pipeline_report(&output_dir, "overflow", &bundle.overflow)?;
        write_pipeline_report(&output_dir, "fail_safe", &bundle.fail_safe)?;
        println!("demo evidence written to {}", output_dir.display());
        return Ok(());
    }

    match scenario {
        DemoScenario::All => {
            for scenario in [
                DemoScenario::Normal,
                DemoScenario::Priority,
                DemoScenario::Overflow,
                DemoScenario::FailSafe,
            ] {
                run_demo_pipeline_scenario(&output_dir, scenario, arch).await?;
            }
            write_watchdog_evidence(&output_dir).await?;
        }
        DemoScenario::Watchdog => {
            write_watchdog_evidence(&output_dir).await?;
        }
        scenario => {
            run_demo_pipeline_scenario(&output_dir, scenario, arch).await?;
        }
    }

    println!("demo evidence written to {}", output_dir.display());
    Ok(())
}

async fn run_default_demo_bundle() -> Result<DemoBundle> {
    let normal_report = run_demo_pipeline(DemoScenario::Normal, Architecture::Async).await?;
    let priority_report = run_demo_pipeline(DemoScenario::Priority, Architecture::Async).await?;
    let overflow_report = run_demo_pipeline(DemoScenario::Overflow, Architecture::Threaded).await?;
    let fail_safe_report = run_demo_pipeline(DemoScenario::FailSafe, Architecture::Async).await?;
    let watchdog = watchdog_reset_evidence(Duration::from_secs(10)).await;

    Ok(DemoBundle {
        normal: normal_report,
        priority: priority_report,
        overflow: overflow_report,
        fail_safe: fail_safe_report,
        watchdog,
    })
}

async fn run_demo_pipeline_scenario(
    output_dir: &std::path::Path,
    scenario: DemoScenario,
    arch: DemoArch,
) -> Result<()> {
    match arch {
        DemoArch::Mixed => {
            let architecture = default_demo_architecture(scenario)?;
            let report = run_demo_pipeline(scenario, architecture).await?;
            write_pipeline_report(output_dir, scenario.report_name(), &report)?;
        }
        DemoArch::Async => {
            let report = run_demo_pipeline(scenario, Architecture::Async).await?;
            write_pipeline_report(output_dir, scenario.report_name(), &report)?;
        }
        DemoArch::Threaded => {
            let report = run_demo_pipeline(scenario, Architecture::Threaded).await?;
            write_pipeline_report(output_dir, scenario.report_name(), &report)?;
        }
        DemoArch::Both => {
            let async_report = run_demo_pipeline(scenario, Architecture::Async).await?;
            let threaded_report = run_demo_pipeline(scenario, Architecture::Threaded).await?;
            write_pipeline_report(
                output_dir,
                &format!("{}_async", scenario.report_name()),
                &async_report,
            )?;
            write_pipeline_report(
                output_dir,
                &format!("{}_threaded", scenario.report_name()),
                &threaded_report,
            )?;
        }
    }

    Ok(())
}

async fn run_demo_pipeline(
    scenario: DemoScenario,
    architecture: Architecture,
) -> Result<crate::pipeline::PipelineReport> {
    let config = build_demo_config(scenario, architecture)?;
    run_for_architecture(config).await
}

async fn write_watchdog_evidence(output_dir: &std::path::Path) -> Result<()> {
    let watchdog = watchdog_reset_evidence(Duration::from_secs(10)).await;
    write_json(&output_dir.join("watchdog_summary.json"), &watchdog)?;
    Ok(())
}

fn default_demo_architecture(scenario: DemoScenario) -> Result<Architecture> {
    match scenario {
        DemoScenario::Normal | DemoScenario::Priority | DemoScenario::FailSafe => {
            Ok(Architecture::Async)
        }
        DemoScenario::Overflow => Ok(Architecture::Threaded),
        DemoScenario::All | DemoScenario::Watchdog => {
            anyhow::bail!("scenario {scenario:?} does not have a pipeline architecture")
        }
    }
}

fn build_demo_config(scenario: DemoScenario, architecture: Architecture) -> Result<PipelineConfig> {
    match scenario {
        DemoScenario::Normal => Ok(normal_demo_config(architecture)),
        DemoScenario::Priority => Ok(priority_demo_config(architecture)),
        DemoScenario::Overflow => Ok(overflow_demo_config(architecture)),
        DemoScenario::FailSafe => Ok(fail_safe_demo_config(architecture)),
        DemoScenario::All | DemoScenario::Watchdog => {
            anyhow::bail!("scenario {scenario:?} is not a pipeline scenario")
        }
    }
}

async fn run_for_architecture(config: PipelineConfig) -> Result<crate::pipeline::PipelineReport> {
    match config.architecture {
        Architecture::Async => run_async_pipeline(config).await,
        Architecture::Threaded => run_threaded_pipeline(config),
    }
}

impl DemoScenario {
    fn report_name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Priority => "priority",
            Self::Overflow => "overflow",
            Self::FailSafe => "fail_safe",
            Self::All | Self::Watchdog => "watchdog",
        }
    }
}

fn normal_demo_config(architecture: Architecture) -> PipelineConfig {
    let base_events = synthetic_recent_changes();
    demo_config(
        "normal-load",
        architecture,
        repeat_events(&base_events, 60),
        128,
        150,
        0,
    )
}

fn priority_demo_config(architecture: Architecture) -> PipelineConfig {
    let mut priority = demo_config(
        "priority-pressure",
        architecture,
        priority_pressure_events(),
        128,
        250,
        0,
    );
    priority.worker_start_delay = Duration::from_millis(20);
    priority.jitter_threshold = Duration::from_millis(100);
    priority.deadline_miss_limit = 10_000;
    priority
}

fn overflow_demo_config(architecture: Architecture) -> PipelineConfig {
    let base_events = synthetic_recent_changes();
    let mut overflow = demo_config(
        "overflow-spike",
        architecture,
        repeat_events(&base_events, 120),
        4,
        3_000,
        0,
    );
    overflow.worker_start_delay = Duration::from_millis(20);
    overflow
}

fn fail_safe_demo_config(architecture: Architecture) -> PipelineConfig {
    let mut fail_safe = demo_config(
        "fail-safe-recovery",
        architecture,
        fail_safe_recovery_events(),
        128,
        0,
        0,
    );
    fail_safe.processing_delay = ProcessingDelayProfile::Phased {
        overload_packets: 12,
        overload_delay: Duration::from_millis(3),
        recovery_delay: Duration::from_micros(100),
    };
    fail_safe.ingest_pause = Duration::from_micros(500);
    fail_safe.worker_start_delay = Duration::ZERO;
    fail_safe.fail_safe_window = 8;
    fail_safe.deadline_miss_limit = 1;
    fail_safe
}

fn demo_config(
    name: &str,
    architecture: Architecture,
    events: Vec<String>,
    queue_capacity: usize,
    processing_delay_us: u64,
    ingest_pause_us: u64,
) -> PipelineConfig {
    let limit = events.len();
    let mut config = PipelineConfig::replay(
        architecture,
        PathBuf::from("data/replay/sample_recentchange.ndjson"),
        limit,
    );
    config.mode = PipelineMode::Synthetic {
        name: name.to_string(),
        events,
    };
    config.queue_capacity = queue_capacity;
    config.processing_delay =
        ProcessingDelayProfile::Fixed(Duration::from_micros(processing_delay_us));
    config.ingest_pause = Duration::from_micros(ingest_pause_us);
    config.jitter_threshold = Duration::from_millis(2);
    config.deadline_miss_limit = 2;
    config
}

fn priority_pressure_events() -> Vec<String> {
    let mut events = Vec::new();
    for id in 0..60 {
        events.push(json_event(
            id,
            "BacklogBot",
            true,
            "en.wikipedia.org",
            &format!("Queued bot edit {id}"),
        ));
    }
    for id in 60..80 {
        events.push(json_event(
            id,
            "HumanEditor",
            false,
            "en.wikipedia.org",
            &format!("Urgent human edit {id}"),
        ));
    }
    events
}

fn fail_safe_recovery_events() -> Vec<String> {
    let mut events = vec![json_event(
        0,
        "OverloadHuman",
        false,
        "en.wikipedia.org",
        "Initial overloaded human edit",
    )];
    for id in 1..12 {
        events.push(json_event(
            id,
            "LoadBot",
            true,
            "commons.wikimedia.org",
            &format!("Low-priority overload bot {id}"),
        ));
    }
    for id in 12..80 {
        events.push(json_event(
            id,
            "RecoveryHuman",
            false,
            "wikidata.org",
            &format!("Stable recovery human edit {id}"),
        ));
    }
    events
}

fn json_event(id: u64, user: &str, bot: bool, domain: &str, title: &str) -> String {
    format!(
        r#"{{"meta":{{"domain":"{domain}","dt":"2026-05-05T12:00:00Z"}},"user":"{user}","bot":{bot},"server_name":"{domain}","wiki":"syntheticwiki","type":"edit","title":"{title}","id":{id}}}"#
    )
}
