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


## Data Volume Policy
- 12 Events: Quick smoke tests.
- 60 Events: Normal load 
- 80 Events: Priority-pressure 
- 120 Events: Overflow 
- 80 Events: Fail-safe recovery
- 1 Timeout: 10 second watchdog
- 500 live Wikipedia Events: Authenticity replay evidence.
- 1000 Synthetic Benchmark Events: Report percentile discussion.

