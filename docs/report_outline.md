# RTS2601 Report Outline

Keep the final report between 3000 and 4000 words.

## Abstract

Summarise the Wikimedia recent-change stream as the sensor source, the two Rust concurrency architectures, the 2ms micro-deadline, and the strongest measured result.

## Introduction

Explain why timeliness, predictable latency, bounded buffering, and fail-safe behaviour matter in real-time monitoring. State the objective: compare async/await and OS-threaded Rust pipelines under replayed and live event loads.

## Related Work

Cover Rust ownership and data-race safety, async runtimes, OS-threaded concurrency, real-time scheduling, backpressure, priority scheduling, jitter, and tail-latency percentiles. Use APA references consistently.

## System Design

Include diagrams for:

- Live/replay ingestion to the bounded drop-oldest channel.
- Drop-oldest backpressure.
- Human-vs-bot priority scheduler.
- Shared leaderboard and synchronization strategy.
- Watchdog reset.
- Fail-safe degraded-mode state machine.

Justify why controlled synthetic runs are used for deterministic benchmark evidence while `data/replay/captured_recentchange.ndjson` provides 500 live Wikimedia events for scenario authenticity.

## Results and Discussion

Use the generated JSON/CSV evidence:

- Async vs threaded p50/p90/p99 latency.
- 1000-event synthetic benchmark evidence from `output/bench/benchmark_bundle.json`.
- 500-event captured replay evidence from `output/replay_async_500` and `output/replay_threaded_500`.
- Human vs bot scheduling drift, using `output/demo/priority_summary.json` as the primary proof that human p99 drift is lower.
- Deadline misses against the 2ms micro-deadline.
- Overflow event count and packet loss behaviour.
- Mutex vs RwLock vs Atomic synchronization results.
- Allocation proof for the borrowed serde hot path.
- Watchdog reset evidence from `output/demo/watchdog_summary.json`, which records a controlled 10 second stalled-input timeout using the same timeout mechanism as live stream reads.
- Fail-safe entry and recovery observations, including both degraded and recovered transitions.

Discuss limitations honestly: Windows is not a hard real-time OS, live network jitter is non-deterministic, and the system is a simulated real-time system.

## Conclusion and Future Work

Conclude which architecture performed better for p99 tail latency in your measurements. Suggest improvements such as WSL/Valgrind allocation validation, more worker threads, lock-free queues, richer dashboarding, and longer live captures.
