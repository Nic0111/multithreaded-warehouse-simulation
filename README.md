# RTS2601 Wikimedia Real-Time Monitoring Engine

This project implements the RTS2601 assignment brief as an evidence-driven Rust simulation. It compares an async Tokio pipeline against a `std::thread` pipeline while ingesting Wikimedia recent-change events, prioritising human edits, enforcing a 2ms processing deadline, measuring scheduling drift, and collecting benchmark artefacts for the report.

### Async Architecture Commands

```powershell
cargo run -- demo --scenario normal --arch async --output-dir output/presentation/async/normal
cargo run -- demo --scenario priority --arch async --output-dir output/presentation/async/priority
cargo run -- demo --scenario overflow --arch async --output-dir output/presentation/async/overflow
cargo run -- demo --scenario fail-safe --arch async --output-dir output/presentation/async/fail_safe
cargo run -- run --arch async --mode replay --input data/replay/captured_recentchange.ndjson --limit 500 --output-dir output/presentation/async/replay_500
```

These commands prove the Tokio async pipeline can run normal replay and controlled scenario configurations.

### Threaded Architecture Commands

```powershell
cargo run -- demo --scenario normal --arch threaded --output-dir output/presentation/threaded/normal
cargo run -- demo --scenario priority --arch threaded --output-dir output/presentation/threaded/priority
cargo run -- demo --scenario overflow --arch threaded --output-dir output/presentation/threaded/overflow
cargo run -- demo --scenario fail-safe --arch threaded --output-dir output/presentation/threaded/fail_safe
cargo run -- run --arch threaded --mode replay --input data/replay/captured_recentchange.ndjson --limit 500 --output-dir output/presentation/threaded/replay_500
```

### Architecture-Neutral Commands

```powershell
cargo test
cargo run -- capture --output data/replay/captured_recentchange.ndjson --limit 500
cargo run -- demo --scenario watchdog --output-dir output/presentation/neutral/watchdog
```

### Benchmark Commands

```powershell
cargo run -- bench --output-dir output/presentation/benchmark --events 1000
cargo bench
```

### Quick Smoke Test of Architecture

```powershell
cargo run -- run --arch async --mode replay --input data/replay/sample_recentchange.ndjson --limit 12 --output-dir output/presentation/async/sample
cargo run -- run --arch threaded --mode replay --input data/replay/sample_recentchange.ndjson --limit 12 --output-dir output/presentation/threaded/sample
```


### Output Folder Policy

```text
output/presentation/
  async/
    normal/
    priority/
    overflow/
    fail_safe/
    sample/
    replay_500/
  threaded/
    normal/
    priority/
    overflow/
    fail_safe/
    sample/
    replay_500/
  neutral/
    watchdog/
  benchmark/
```


## Distinction Presentation Runbook

Use this section as the live presentation script. Part 1 proves the system from commands and output files. Part 2 proves that the source code implements the real-time system concepts shown in Part 1.

### Part 1: Input/Output Presentation

1. **Introduce the system purpose**

   Open `README.md`, line 3.

   Say: I built this project as a soft real-time Wikimedia monitoring engine in Rust. The input is treated like a real-time event stream, similar to sensor data in a monitoring system. Each event passes through ingestion, bounded buffering, priority scheduling, deadline measurement, safety monitoring, and evidence generation. The important point is that the system does not only process data; it measures whether the timing behaviour is acceptable.

   Expected outcome: the lecturer understands that the project is an end-to-end real-time monitoring pipeline.

2. **Run the test suite**

   ```powershell
   cargo test
   ```

   Open `tests/pipeline_integration.rs`, lines 37 to 199.

   Say: I start by running the tests because real-time safety features should be predictable under controlled conditions. These tests verify parsing, bounded queue overflow, priority scheduling, fail-safe state transitions, watchdog timeout evidence, live-source reset counting, and leaderboard correctness. Passing tests prove that the real-time control logic is not only demonstrated manually, but also checked automatically.

   Expected outcome: all tests pass.

3. **Run all async architecture scenarios**

   ```powershell
   cargo run -- demo --scenario normal --arch async --output-dir output/presentation/async/normal
   cargo run -- demo --scenario priority --arch async --output-dir output/presentation/async/priority
   cargo run -- demo --scenario overflow --arch async --output-dir output/presentation/async/overflow
   cargo run -- demo --scenario fail-safe --arch async --output-dir output/presentation/async/fail_safe
   ```

   Open `src/cli.rs`, lines 270 to 349.

   Say: These four commands run the same controlled scenarios using the Tokio async architecture. The async architecture is event-driven, so it is suitable for streaming and timeout-based work because tasks can wait without blocking an operating system thread. I run normal load, priority pressure, overflow, and fail-safe recovery separately so each output folder has one clear purpose.

   Expected outcome: `output/presentation/async` contains `normal`, `priority`, `overflow`, and `fail_safe` scenario folders.

4. **Show async normal output**

   Open `output/presentation/async/normal/normal_summary.json`.

   Say: This is the async baseline. I expect 60 packets seen and 60 packets processed. The real-time concept here is deadline compliance under stable load. I check that `overflow_count` is 0, `deadline_misses` is 0, and `degraded_transitions` is 0. This means the system meets the 2ms micro-deadline before I introduce overload.

   Expected outcome: 60 packets processed, no overflow, no deadline misses, and no degraded mode.

5. **Show async priority output**

   Open `output/presentation/async/priority/priority_summary.json` and `output/presentation/async/priority/priority_packets.csv`.

   Say: This scenario creates a bot-heavy backlog and then injects human edits. The real-time concept is priority scheduling with scheduling drift. I compare `human_drift.p99_us` against `bot_drift.p99_us`; the expected result is that human p99 drift is lower than bot p99 drift. The packet CSV shows packet-level evidence, where each row represents one processed packet with priority, latency, processing time, drift, deadline result, degraded mode, and hot-path allocation count.

   Expected outcome: 80 packets processed and `human_drift.p99_us < bot_drift.p99_us`.

6. **Show async overflow output**

   Open `output/presentation/async/overflow/overflow_summary.json`.

   Say: This is overflow using the async architecture. The real-time concept is bounded backpressure. Under a spike, the system must avoid unbounded memory growth, so the custom channel drops the oldest packet and records the overflow reason. I use this output to show that async can run the same overload scenario, even if I later use the threaded output as the main overflow example.

   Expected outcome: `overflow_count` is greater than 0 and overflow events include `bounded-channel-overflow-drop-oldest`.

7. **Show async fail-safe output**

   Open `output/presentation/async/fail_safe/fail_safe_summary.json`.

   Say: This output proves fail-safe behaviour in the async pipeline. The real-time concept is graceful degradation. When processing delay becomes unstable, the system enters degraded mode and drops low-priority bot edits first. After stable timing windows, it recovers. I check that `degraded_transitions` is greater than 0, `recovered_transitions` is greater than 0, and `degraded_bot_drops` is greater than 0.

   Expected outcome: degraded mode activates, bot work is dropped first, and recovery occurs.

8. **Run all threaded architecture scenarios**

   ```powershell
   cargo run -- demo --scenario normal --arch threaded --output-dir output/presentation/threaded/normal
   cargo run -- demo --scenario priority --arch threaded --output-dir output/presentation/threaded/priority
   cargo run -- demo --scenario overflow --arch threaded --output-dir output/presentation/threaded/overflow
   cargo run -- demo --scenario fail-safe --arch threaded --output-dir output/presentation/threaded/fail_safe
   ```

   Open `src/pipeline/threaded.rs`, lines 19 to 103.

   Say: These commands run the same four scenario configurations using standard operating system threads. The threaded architecture is easier to map to a classic real-time pipeline because producer and worker stages are separated by threads, mutexes, and condition variables. The functional behaviour should match the async architecture because both share the same pipeline contract, but the timing measurements may differ.

   Expected outcome: `output/presentation/threaded` contains `normal`, `priority`, `overflow`, and `fail_safe` scenario folders.

9. **Show threaded normal output**

   Open `output/presentation/threaded/normal/normal_summary.json`.

   Say: This is the threaded baseline. It proves that the thread-based version can also process stable load with the same evidence format. I compare this with the async normal summary to show that both architectures meet the same functional requirements: packets are processed, deadlines are measured, and overflow and fail-safe status are recorded.

   Expected outcome: 60 packets processed with no unexpected overflow or fail-safe activation.

10. **Show threaded priority output**

    Open `output/presentation/threaded/priority/priority_summary.json`.

    Say: This proves that priority scheduling is not tied only to Tokio. The threaded architecture uses the same human-before-bot scheduler, so the expected real-time behaviour is the same: human edits should have lower p99 scheduling drift than bot edits during backlog pressure. The difference between architectures is the concurrency mechanism, not the scheduling requirement.

    Expected outcome: `human_drift.p99_us < bot_drift.p99_us`.

11. **Show threaded overflow output**

    Open `output/presentation/threaded/overflow/overflow_summary.json` and `output/presentation/threaded/overflow/overflow_packets.csv`.

    Say: I use the threaded overflow output as a clear example of bounded backpressure. The queue capacity is intentionally small, so the system receives more packets than it can hold. Instead of allowing unbounded queue growth, it drops the oldest packets and records each overflow. This is a real-time safety choice because predictable bounded memory is safer than unpredictable latency and memory growth.

    Expected outcome: overflow count greater than 0, dropped sequence numbers are recorded, and the reason is `bounded-channel-overflow-drop-oldest`.

12. **Show threaded fail-safe output**

    Open `output/presentation/threaded/fail_safe/fail_safe_summary.json`.

    Say: This output shows that the fail-safe scenario can also run using the threaded architecture. The same safety state machine is used, so the expected result is degraded transition, recovery transition, and bot drops. This demonstrates that the fail-safe design is part of the shared pipeline, not a feature of only one architecture.

    Expected outcome: degraded mode activates and later recovers.

13. **Run architecture-neutral watchdog evidence**

    ```powershell
    cargo run -- demo --scenario watchdog --output-dir output/presentation/neutral/watchdog
    ```

    Open `output/presentation/neutral/watchdog/watchdog_summary.json`.

    Say: The watchdog scenario is architecture-neutral because it supervises input silence rather than packet processing architecture. It deliberately waits for the configured 10 second timeout. The output should show `event` as `Network Reset`, `timeout_seconds` as 10, and `observed_silence_ms` close to 10000. The real-time safety concept is fault detection and recovery after stalled input.

    Expected outcome: a real timeout is recorded with `Network Reset` and approximately 10000ms silence.

14. **Capture live Wikimedia data**

    ```powershell
    cargo run -- capture --output data/replay/captured_recentchange.ndjson --limit 500
    ```

    Open `data/replay/captured_recentchange.ndjson`.

    Say: This command captures 500 real events from the Wikimedia Server-Sent Events stream. I use controlled synthetic scenarios for repeatable timing proof, but this captured replay file proves the system can also work with real Wikimedia recent-change data. Running this command with the same output path recreates the file.

    Expected outcome: `captured_recentchange.ndjson` contains 500 live-captured Wikimedia events.

15. **Replay the same captured data through both architectures**

    ```powershell
    cargo run -- run --arch async --mode replay --input data/replay/captured_recentchange.ndjson --limit 500 --output-dir output/presentation/async/replay_500
    cargo run -- run --arch threaded --mode replay --input data/replay/captured_recentchange.ndjson --limit 500 --output-dir output/presentation/threaded/replay_500
    ```

    Open `output/presentation/async/replay_500/async_summary.json` and `output/presentation/threaded/replay_500/threaded_summary.json`.

    Say: This is the fairest architecture comparison because both pipelines process the exact same 500 captured events. The real-time concept is controlled comparison: I change the concurrency architecture while keeping the workload fixed. Both outputs should process 500 packets, and the discussion focuses on p50, p90, p99 latency, drift, deadline misses, and overflow behaviour.

    Expected outcome: both summaries show 500 processed packets and comparable metric fields.

16. **Run 1000-event benchmark evidence**

    ```powershell
    cargo run -- bench --output-dir output/presentation/benchmark --events 1000
    ```

    Open `output/presentation/benchmark/benchmark_bundle.json`, `output/presentation/benchmark/async_bench_summary.json`, and `output/presentation/benchmark/threaded_bench_summary.json`.

    Say: This command generates statistical evidence for the report. The bundle contains an async pipeline benchmark, a threaded pipeline benchmark, synchronization benchmark results, and hot-path allocation proof. The real-time focus is tail latency and jitter because p99 behaviour is more important than average behaviour when discussing deadline risk.

    Expected outcome: benchmark bundle exists, async and threaded benchmark summaries exist, synchronization results exist, and allocation proof shows zero hot-path allocations.

17. **Run Criterion benchmark report**

    ```powershell
    cargo bench
    ```

    Open `target/criterion/report/index.html`.

    Say: Criterion is the formal benchmark report. It supports the report appendix and screenshots because it measures selected functions repeatedly using a standard Rust benchmark framework. The benchmark code uses `std::hint::black_box`, so it avoids the deprecated `criterion::black_box` warning and prevents the compiler from removing measured work.

    Expected outcome: Criterion HTML report exists under `target/criterion/report/index.html`.

### Part 2: Code-Related Presentation

1. **Entry point and async runtime**

   Open `src/main.rs`, lines 4 to 6.

   Say: This is the program entry point. I use `#[tokio::main]` because the system needs asynchronous execution for live stream ingestion, timeout handling, and the Tokio pipeline. The entry point calls the CLI dispatcher, so every command shown in Part 1 goes through the same executable path.

2. **Library module map**

   Open `src/lib.rs`, lines 1 to 19.

   Say: The project is organised into real-time subsystems. `input` handles live, replay, and synthetic data. `realtime` contains bounded buffering, priority, and fail-safe control. `pipeline` contains the shared pipeline plus async and threaded implementations. `evidence` records metrics, reports, and allocation proof. `sync` contains the leaderboard synchronization benchmark. This structure maps code organization directly to the real-time concepts.

3. **CLI modes and scenario selection**

   Open `src/cli.rs`, lines 30 to 71, lines 97 to 115, and lines 207 to 349.

   Say: This file connects commands to implementation. `capture`, `run`, `bench`, and `demo` are separated because the assignment needs live input, replay input, controlled scenarios, and benchmark evidence. The `DemoScenario` and `DemoArch` enums allow each scenario to run as async, threaded, both, or mixed. This is why the presentation can show the same scenario under both architectures without duplicating logic.

4. **Input module overview**

   Open `src/input/mod.rs`, line 1.

   Say: This module groups the real-time input layer. It is intentionally separated from the pipeline so the same processing logic can be driven by live Wikimedia events, replay files, or synthetic events.

5. **Zero-copy event parsing**

   Open `src/input/event.rs`, lines 5 to 29 and lines 52 to 68.

   Say: `RecentChangeBorrowed<'a>` borrows string fields from the JSON input instead of allocating owned strings in the parser. The real-time concept is reducing jitter in the hot path. Allocations can introduce unpredictable latency, so the parser is designed to classify packets and extract fields with borrowed data where possible.

6. **Live, replay, synthetic source, and watchdog reconnect**

   Open `src/input/source.rs`, lines 11 to 18, lines 20 to 49, and lines 58 to 143.

   Say: This file implements the input sources. Replay gives deterministic experiments, synthetic data gives controlled load patterns, and the live source reads Wikimedia SSE. The live source uses a watchdog timeout around network reads. If the stream is silent too long, it records a network reset and reconnects until the configured maximum reset count is reached. This is a real-time safety feature because stalled input must be detected.

7. **Realtime module overview**

   Open `src/realtime/mod.rs`, line 1.

   Say: This module groups the control mechanisms that make the project a real-time system rather than a simple data processor: bounded queueing, priority classification, and fail-safe control.

8. **Bounded drop-oldest channel**

   Open `src/realtime/queue.rs`, lines 13 to 19 and lines 43 to 73.

   Say: This is the bounded backpressure mechanism. The channel has fixed capacity, `send_drop_oldest` inserts new packets, and when full it removes the oldest packet and records an overflow event. This design avoids unbounded memory growth, which is important because uncontrolled queues can destroy latency guarantees in real-time systems.

9. **Priority classes**

   Open `src/realtime/priority.rs`, lines 4 to 19.

   Say: This file defines human edits as high priority and bot edits as lower priority. The real-time concept is priority-based scheduling. Under overload, the system preserves more important work first, which is why the priority-pressure scenario compares human drift against bot drift.

10. **Fail-safe state machine**

    Open `src/realtime/failsafe.rs`, lines 19 to 36 and lines 52 to 98.

    Say: This file implements degraded mode and recovery. The controller watches deadline misses and processing jitter over a rolling window. If timing becomes unstable, it enters degraded mode. After stable windows, it recovers. This is a safety feature because the system changes behaviour under overload instead of failing silently.

11. **Pipeline module overview**

    Open `src/pipeline/mod.rs`, lines 1 to 4.

    Say: This module exposes the shared pipeline contract and the two architecture implementations. The key design decision is that async and threaded pipelines share the same packet model, metrics, scheduler, and fail-safe logic. That makes the comparison fair.

12. **Pipeline configuration and shared contract**

    Open `src/pipeline/common.rs`, lines 55 to 127 and lines 132 to 148.

    Say: This is the shared timing contract. It defines queue capacity, processing delay, 2ms micro-deadline, jitter threshold, fail-safe window, watchdog timeout, and max network resets. Because both architectures use this same configuration, the outputs are comparable. The report structure stores both summary metrics and packet-level metrics.

13. **Priority scheduler**

    Open `src/pipeline/common.rs`, lines 152 to 167.

    Say: The scheduler maintains separate human and bot queues. `pop_next` always takes from the human queue first. This is the code-level reason the priority scenario should show lower p99 drift for human edits than bot edits.

14. **Enqueue timing and bounded channel use**

    Open `src/pipeline/common.rs`, lines 229 to 248.

    Say: `enqueue_event` parses enough information to classify the packet, records ingestion time, sets expected ready time, and sends the packet into the bounded channel. These timestamps are the basis of latency and drift measurement, so the real-time metrics begin at the ingestion boundary.

15. **Processing hot path**

    Open `src/pipeline/common.rs`, lines 251 to 299.

    Say: This is the main processing hot path. It drops bot packets first during degraded mode, parses the event, updates the leaderboard, measures processing time, checks the 2ms deadline, observes fail-safe transitions, calculates latency and drift, and records hot-path allocations. This single function connects the major real-time concepts: priority, deadline compliance, fail-safe behaviour, and observability.

16. **Async Tokio pipeline**

    Open `src/pipeline/async_runtime.rs`, lines 17 to 31, lines 42 to 57, and lines 67 to 101.

    Say: This is the async architecture. It uses Tokio tasks, `tokio::sync::Mutex`, `Notify`, and atomic flags. The producer task ingests events and notifies the worker task. The worker drains the bounded channel into the priority scheduler and processes packets. This design fits event-driven stream processing because waiting does not need to block an operating system thread.

17. **Threaded pipeline**

    Open `src/pipeline/threaded.rs`, lines 19 to 36, lines 43 to 59, and lines 68 to 103.

    Say: This is the threaded architecture. It uses `std::thread`, `Mutex`, `Condvar`, and atomic flags. The producer and worker are operating system threads, which is closer to a traditional staged real-time pipeline. It implements the same shared contract as the async version, so any behaviour difference should come from concurrency overhead and scheduling, not from different feature logic.

18. **Evidence module overview**

    Open `src/evidence/mod.rs`, line 1.

    Say: This module groups the evidence layer. In a real-time assignment, evidence is important because I need to prove latency, drift, deadline misses, overflow, watchdog reset, fail-safe recovery, synchronization cost, and allocation behaviour from generated files.

19. **Metrics and percentiles**

    Open `src/evidence/metrics.rs`, lines 5 to 43 and lines 80 to 127.

    Say: This file defines packet metrics and summary metrics. It records packet counts, deadline misses, network resets, degraded transitions, bot drops, latency percentiles, processing percentiles, human and bot drift, queue depth, top domains, and overflow events. Percentiles such as p90 and p99 are important because tail latency matters more than average latency in real-time systems.

20. **Report writing and packet-level CSV**

    Open `src/evidence/reporting.rs`, lines 106 to 147.

    Say: This code writes each summary JSON and packet-level CSV. The CSV columns show sequence, priority, domain, latency, processing time, drift, deadline miss, degraded mode, and hot-path allocations. This is why I do not need to print every packet to the terminal; the CSV is structured packet-level evidence that can be opened, verified, and graphed.

21. **Watchdog evidence writer**

    Open `src/evidence/reporting.rs`, lines 80 to 103.

    Say: This code creates the controlled watchdog evidence. It starts a stalled input future and waits with `tokio::time::timeout`. When the timeout fires, it writes a `Network Reset` event with the observed silence time and recovery action. This is clearer for presentation than only describing watchdog logic.

22. **Hot-path allocation proof**

    Open `src/evidence/allocator.rs`, lines 18 to 36 and lines 70 to 82, then `src/evidence/reporting.rs`, lines 56 to 78.

    Say: The global allocator counts allocations and deallocations, including per-thread counters. The allocation proof warms up the parser, resets the thread counter, runs the measured parse and classification path, and checks the delta. The real-time concept is reducing allocation-related jitter, and the benchmark bundle reports whether the measured hot path performed zero allocations.

23. **Synchronization module overview**

    Open `src/sync/mod.rs`, line 1.

    Say: This module contains the shared-resource part of the system. Shared resources can cause lock wait time, contention, and priority inversion risk, so the project includes a leaderboard and synchronization benchmark instead of ignoring shared-state behaviour.

24. **Leaderboard and synchronization strategies**

    Open `src/sync/leaderboard.rs`, lines 13 to 34 and lines 47 to 148.

    Say: The leaderboard records the top Wikipedia domains and benchmark variants compare `Mutex`, `RwLock`, and atomic counters. This is connected to real-time systems because synchronization overhead can affect p99 latency. The benchmark measures throughput, wait time, update latency, and priority inversion risk notes.

25. **Criterion benchmark code**

    Open `benches/pipeline.rs`, lines 29 to 57.

    Say: Criterion benchmarks the zero-copy parser and both pipeline architectures. The benchmark uses `std::hint::black_box` to stop the compiler from optimizing away the measured work. This supports measurement discipline and gives the formal HTML report under `target/criterion/report/index.html`.

26. **Integration tests as safety proof**

    Open `tests/pipeline_integration.rs`, lines 37 to 199.

    Say: The integration tests prove the behaviours that matter for distinction-level real-time evidence: deterministic replay, overflow logging, watchdog timeout, priority-pressure drift, fail-safe recovery, and live-source timeout reset counting. These tests are important because safety mechanisms should be repeatable, not only shown once during presentation.

### Closing Script

Say: To conclude, this is a simulated soft real-time system, not a hard real-time operating system. The distinction-level contribution is that each real-time concept is both implemented in Rust and evidenced through generated outputs: async and threaded architectures, bounded backpressure, priority scheduling, deadline measurement, scheduling drift, watchdog reset, fail-safe degradation and recovery, synchronization benchmarking, zero-copy allocation proof, captured live-data replay, and Criterion benchmarking.

## Assignment Coverage

- Dual architectures: `run --arch async` and `run --arch threaded`.
- Live and replay input: `capture` records live Wikimedia SSE data; replay files make benchmarks deterministic.
- Backpressure: a custom bounded drop-oldest channel drops the oldest packet and records high-precision overflow evidence.
- Zero-copy: `RecentChangeBorrowed<'a>` parses borrowed JSON fields with serde lifetimes.
- Priority scheduling: humans are served before bot edits; the priority-pressure demo proves human p99 drift is lower than bot p99 drift.
- Micro-deadlines: each packet records processing time and 2ms deadline compliance.
- Shared resource: top-3 domain leaderboard is updated concurrently and benchmarked with `Mutex`, `RwLock`, and atomics.
- Watchdog and fail-safe: live input has a watchdog timeout; the demo also performs a controlled 10 second stalled-input watchdog reset; degraded mode drops low-priority bot work until timing stabilises.
- Evidence: JSON summaries and packet CSVs are written under `output/`.

## Evidence Files

Run the presentation commands before the presentation. The generated `output/presentation/async`, `output/presentation/threaded`, and `output/presentation/neutral` folders show normal load, priority pressure, overflow, fail-safe recovery, and watchdog evidence in an architecture-first layout. The watchdog reset is written to `output/presentation/neutral/watchdog/watchdog_summary.json`; it uses the same timeout mechanism as the live Wikimedia reader and deliberately waits for a controlled 10 second network-silence timeout. Packet-level CSVs can be graphed in Excel or Python for p50/p90/p99 latency and scheduling drift.

Run `cargo run -- bench --output-dir output/presentation/benchmark --events 1000` for report tables. The generated benchmark bundle contains async-vs-threaded summaries, synchronization results, and allocation proof. Run `cargo bench` for full Criterion reports under `target/criterion/report/index.html`.

## Data Volume Policy

- Keep `data/replay/sample_recentchange.ndjson` at 12 events for quick smoke tests.
- Use `output/presentation` for controlled presentation evidence: 60 normal events, 80 priority-pressure events, 120 overflow events, 80 fail-safe recovery events, and one 10 second watchdog timeout.
- Use 1000 synthetic benchmark events for report percentile discussion.
- Use `data/replay/captured_recentchange.ndjson` with 500 live Wikipedia events for authenticity replay evidence.
