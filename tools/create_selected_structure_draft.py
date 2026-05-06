from pathlib import Path

from docx import Document
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor


OUT = Path("docs/RTS2601_Assignment_Draft_Selected_Structure.docx")


def shade_cell(cell, fill):
    tcpr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), fill)
    tcpr.append(shd)


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc = Document()
    sec = doc.sections[0]
    sec.top_margin = Inches(0.75)
    sec.bottom_margin = Inches(0.75)
    sec.left_margin = Inches(0.85)
    sec.right_margin = Inches(0.85)

    for name, size in [
        ("Normal", 10.5),
        ("Title", 20),
        ("Heading 1", 15.5),
        ("Heading 2", 12.5),
        ("Heading 3", 11.2),
    ]:
        style = doc.styles[name]
        style.font.name = "Arial"
        style.font.size = Pt(size)

    header = sec.header.paragraphs[0]
    header.text = "RTS2601 Individual Assignment Draft"
    header.runs[0].font.name = "Arial"
    header.runs[0].font.size = Pt(9)
    header.runs[0].font.color.rgb = RGBColor(90, 90, 90)

    footer = sec.footer.paragraphs[0]
    footer.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    footer.text = "Nicholas Yee Hoe Yin | TP069057"
    footer.runs[0].font.name = "Arial"
    footer.runs[0].font.size = Pt(9)
    footer.runs[0].font.color.rgb = RGBColor(90, 90, 90)

    def p(text):
        para = doc.add_paragraph()
        para.paragraph_format.space_after = Pt(6)
        para.paragraph_format.line_spacing = 1.08
        para.add_run(text)
        return para

    def h(text, level=1):
        return doc.add_paragraph(text, style=f"Heading {level}")

    def bullet(text):
        para = doc.add_paragraph(style="List Bullet")
        para.paragraph_format.space_after = Pt(3)
        para.add_run(text)

    def table(headers, rows):
        t = doc.add_table(rows=1, cols=len(headers))
        t.alignment = WD_TABLE_ALIGNMENT.CENTER
        t.style = "Table Grid"
        for i, head in enumerate(headers):
            cell = t.rows[0].cells[i]
            cell.text = head
            shade_cell(cell, "EAF2F8")
            for para in cell.paragraphs:
                for run in para.runs:
                    run.bold = True
                    run.font.name = "Arial"
                    run.font.size = Pt(9)
        for row_data in rows:
            row = t.add_row().cells
            for i, val in enumerate(row_data):
                row[i].text = val
                row[i].vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.TOP
                for para in row[i].paragraphs:
                    for run in para.runs:
                        run.font.name = "Arial"
                        run.font.size = Pt(9)
        doc.add_paragraph()

    cover = doc.add_paragraph(style="Title")
    cover.alignment = WD_ALIGN_PARAGRAPH.CENTER
    cover.add_run("RTS2601 Wikimedia Real-Time Monitoring Engine").bold = True
    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sub.add_run("Assignment Draft Using Selected Structure\nNicholas Yee Hoe Yin | TP069057")
    doc.add_page_break()

    h("Abstract")
    p("This assignment presents a Rust-based soft real-time monitoring engine for Wikimedia recent-change events. The system treats the Wikimedia recent-change stream as a continuous event source and implements a complete real-time processing pipeline with live, replay, and synthetic input modes. It compares two concurrency architectures, a Tokio asynchronous pipeline and a standard thread-based pipeline, while keeping the same queueing, scheduling, processing, and reporting contract for fair comparison.")
    p("The implementation demonstrates key real-time system concepts: bounded buffering, human-before-bot priority scheduling, deadline measurement, scheduling drift analysis, watchdog timeout recovery, fail-safe degraded mode, synchronization benchmarking, and zero-allocation hot-path parsing. Evidence is generated as JSON summaries and CSV packet logs. Current demo evidence shows stable normal-load processing, lower human p99 drift than bot p99 drift under priority pressure, bounded drop-oldest overflow behaviour, fail-safe entry and recovery, and a real 10 second watchdog Network Reset. The system is therefore a soft real-time simulation with measurable safety behaviour rather than a hard real-time OS implementation.")

    h("1.0 Introduction")
    h("1.1 Background of Real-Time Monitoring Systems", 2)
    p("Real-time monitoring systems process external events while also considering timing constraints. Unlike ordinary batch systems, their usefulness depends on how quickly and predictably they react to incoming data. A monitoring system may continue operating after occasional delays, but late processing can reduce the value of the information being displayed or analysed. This project therefore adopts a soft real-time model, where timing targets are measured and discussed rather than guaranteed by a real-time operating system.")
    p("The project focuses on practical real-time concepts: bounded queues prevent uncontrolled memory growth, priority scheduling gives important work earlier service, deadlines provide timing targets, watchdogs detect stalled input, fail-safe logic protects the system during overload, and metrics provide evidence of timing behaviour. These concepts are implemented in Rust to benefit from memory safety and explicit concurrency control.")
    h("1.2 Wikimedia Recent-Change Stream as a Real-Time Data Source", 2)
    p("The Wikimedia recent-change stream continuously publishes edit events from Wikimedia projects through Server-Sent Events. Each event contains fields such as domain, user, bot flag, wiki, event type, title, and timestamp metadata. This makes it suitable as a real-time input source because the system must react to a continuous event flow instead of a fixed static dataset.")
    p("In this project, Wikimedia events are treated like monitoring signals. Human edits and bot edits are classified into different priorities, domains are used to maintain a live leaderboard, and event payloads are processed through two architectures. Live capture provides authenticity, replay files provide reproducibility, and synthetic data provides controlled overload scenarios for presentation and benchmarking.")
    h("1.3 Problem Statement", 2)
    p("A basic stream reader is not sufficient to demonstrate real-time system design. The assignment requires a system that can ingest live or replayed data, process it concurrently, handle overload, apply scheduling decisions, protect itself during timing violations, and produce evidence that these mechanisms work. Without bounded buffering, an event spike can cause memory growth and unpredictable latency. Without priority scheduling, high-value human edits may wait behind low-priority bot traffic. Without watchdog and fail-safe mechanisms, stalled input or processing instability may go undetected.")
    p("The problem addressed by this project is therefore to design and implement a Rust monitoring engine that demonstrates real-time concepts through measurable behaviour, not only through theoretical discussion.")
    h("1.4 Aim and Objectives", 2)
    p("The aim is to build an evidence-driven Rust soft real-time monitoring engine for Wikimedia recent-change events. The objectives are:")
    for item in [
        "Implement live Wikimedia SSE capture, deterministic replay, and controlled synthetic input modes.",
        "Implement and compare Tokio asynchronous and standard thread-based processing architectures.",
        "Use a bounded drop-oldest channel to control overload and record overflow evidence.",
        "Prioritise human edits over bot edits and measure scheduling drift separately for both classes.",
        "Measure latency, processing time, deadline misses, queue depth, p50, p90, and p99 timing behaviour.",
        "Implement watchdog Network Reset evidence and fail-safe degraded-mode recovery.",
        "Benchmark shared-resource synchronization and prove zero allocation in the measured hot path.",
    ]:
        bullet(item)
    h("1.5 Scope and Assumptions", 2)
    p("The system is a simulated soft real-time system running on Windows, not a hard real-time deployment. The 2ms micro-deadline is used as a measurable processing target, but the operating system scheduler can still introduce timing variation. Controlled synthetic scenarios are used to isolate real-time behaviours, while captured Wikimedia replay data is used to demonstrate realistic input compatibility. The code prioritises clarity, repeatability, and assignment evidence over production deployment concerns.")

    h("2.0 Related Work")
    for title, body in [
        ("2.1 Soft Real-Time Systems and Timing Constraints", "Soft real-time systems allow occasional deadline misses, but still require low latency and predictable behaviour. Monitoring and analytics systems commonly fall into this category because late information is less useful but not necessarily catastrophic. This project uses a 2ms micro-deadline and percentile-based latency reporting to evaluate timing behaviour. p99 latency is especially important because rare slow packets can reveal overload or synchronization issues that average latency hides."),
        ("2.2 Event-Driven Stream Processing", "Event-driven stream processing systems consume a continuous sequence of events and react as events arrive. The Wikimedia recent-change stream is event-driven because each edit is delivered as an independent message. The implementation separates ingestion, buffering, scheduling, processing, shared-state update, and evidence generation. This modular design makes it easier to reason about where timing delays can occur and how they are measured."),
        ("2.3 Rust for Safe Concurrent Systems", "Rust is suitable for concurrent real-time simulations because it enforces memory safety without a garbage collector. The ownership model prevents data races unless synchronization is explicitly used. In this project, Arc provides shared ownership, Mutex and RwLock protect shared state, AtomicBool coordinates worker completion, Condvar supports blocking threaded wakeups, and Tokio primitives support asynchronous coordination. Rust lifetimes are also used in zero-copy JSON parsing to reduce hot-path allocation."),
        ("2.4 Async Tokio vs Thread-Based Concurrency", "Tokio async concurrency is useful for I/O-heavy workloads because tasks can wait for input without occupying an operating system thread for each blocked operation. Thread-based concurrency uses OS threads and blocking synchronization, which is simpler to reason about but can have different scheduling and wakeup behaviour. The project implements both approaches with the same pipeline contract, allowing the results to compare concurrency architecture rather than unrelated processing logic."),
        ("2.5 Priority Scheduling, Backpressure, Watchdogs, and Fail-Safe Design", "Priority scheduling is used when some work should be processed before other work under contention. Backpressure prevents a system from accepting unlimited work when processing cannot keep up. Watchdogs detect fault conditions such as stalled input by using timeouts. Fail-safe design allows the system to continue in a reduced mode when normal timing cannot be maintained. These concepts are combined in the implementation: human edits are prioritised, a bounded channel drops oldest packets under overflow, the live source reconnects after watchdog timeout, and degraded mode drops bot work first."),
    ]:
        h(title, 2)
        p(body)

    h("3.0 System Design")
    sections = [
        ("3.1 Overall Architecture", "The system architecture follows the sequence: input source, bounded channel, priority scheduler, processing worker, leaderboard update, metrics collection, and evidence writing. The input source provides JSON event payloads from live SSE, replay NDJSON, or synthetic fixtures. Each payload is parsed to classify its priority, then wrapped into a RawPacket containing sequence number, payload, ingestion timestamp, expected ready timestamp, and priority.\n\nThe bounded channel limits queue capacity. The worker drains packets into the priority scheduler, which separates human and bot events. The processor measures timing, updates the leaderboard, checks the deadline, updates fail-safe state, records allocation counts, and stores packet-level metrics. Finally, summaries and CSV files are written under the output directory."),
        ("3.2 Input Sources: Live, Replay, and Synthetic Data", "The input layer is implemented in src/input/source.rs. Live input connects to the Wikimedia recent-change SSE endpoint. Replay input reads NDJSON files such as sample_recentchange.ndjson and captured_recentchange.ndjson. Synthetic input is generated for controlled demos and benchmarks. This three-source design is useful because each source has a different role: live mode proves real integration, replay mode provides repeatability, and synthetic mode isolates specific timing behaviours."),
        ("3.3 Shared Pipeline Contract", "Both the async and threaded architectures use the same PipelineConfig, RawPacket, BoundedDropOldestChannel, PriorityScheduler, process_packet function, MetricsCollector, and finalize_report function. This shared contract ensures that the architecture comparison is fair. The difference between async and threaded runs is the concurrency mechanism, not the scheduling or metrics logic."),
        ("3.4 Bounded Drop-Oldest Channel Design", "The bounded channel is implemented as BoundedDropOldestChannel in src/realtime/queue.rs. It uses a VecDeque internally and provides channel-style methods such as send_drop_oldest, try_recv, and take_overflow_events. When capacity is reached, the oldest packet is removed and an OverflowEvent is recorded. This supports bounded memory behaviour and gives visible evidence of overload."),
        ("3.5 Human-Before-Bot Priority Scheduling", "Human-before-bot scheduling is implemented by classifying events through the bot flag. The PriorityScheduler has separate queues for Human and Bot packets. pop_next always returns a human packet first if one is available, then returns a bot packet. This demonstrates fixed-priority scheduling and is evaluated using human and bot scheduling drift percentiles."),
        ("3.6 Watchdog and Fail-Safe State Machine", "The live source wraps stream reads in tokio::time::timeout. If no data arrives within the watchdog period, the system records a network reset and reconnects until the maximum reset count is reached. The controlled watchdog demo uses the same timeout mechanism with a pending input future, producing standalone Network Reset evidence after approximately 10 seconds.\n\nThe fail-safe controller observes processing jitter and deadline misses over a rolling window. If p99 processing time exceeds the jitter threshold or deadline misses exceed the configured limit, the controller enters degraded mode. In degraded mode, low-priority bot edits are dropped first. When timing becomes stable for enough windows, the controller recovers to normal mode."),
        ("3.7 Metrics and Evidence Generation", "Metrics are generated through MetricsCollector and written through the evidence reporting module. Each packet records latency, processing time, scheduling drift, deadline status, degraded-mode status, priority, domain, and hot-path allocation count. Each summary records packet totals, overflow count, deadline misses, network resets, degraded transitions, recovered transitions, p50/p90/p99 latency, p50/p90/p99 processing time, human and bot drift, maximum queue depth, top domains, and overflow events."),
    ]
    for title, body in sections:
        h(title, 2)
        for para in body.split("\n\n"):
            p(para)

    h("4.0 Implementation")
    impl_sections = [
        ("4.1 CLI Modes: Capture, Run, Demo, and Bench", "The CLI is implemented in src/cli.rs using clap. The capture command records live Wikimedia SSE events into an NDJSON replay file. The run command executes either the async or threaded architecture using live or replay mode. The demo command generates controlled presentation evidence for normal load, priority pressure, overflow, fail-safe recovery, and watchdog reset. The bench command generates larger benchmark evidence, synchronization results, and allocation proof."),
        ("4.2 Zero-Copy JSON Parsing", "Zero-copy parsing is implemented in src/input/event.rs using RecentChangeBorrowed with serde lifetimes. Fields such as user, server_name, wiki, event_type, title, and meta fields borrow from the raw JSON string. This avoids allocating new Strings during the parse/classify hot path. The parser is used both before enqueueing for priority classification and during processing for domain and priority extraction."),
        ("4.3 Tokio Async Pipeline Implementation", "The async pipeline is implemented in src/pipeline/async_runtime.rs. It uses tokio::spawn for producer and worker tasks, tokio::sync::Mutex for shared queue and metrics, Notify for wakeups, Arc for shared ownership, and AtomicBool to signal completion. The producer enqueues events and notifies the worker. The worker drains the bounded channel, applies priority scheduling, processes packets, and exits when all work is completed."),
        ("4.4 Threaded Pipeline Implementation", "The threaded pipeline is implemented in src/pipeline/threaded.rs. It uses std::thread, Mutex, Condvar, Arc, and AtomicBool. The condition variable allows the worker to block when the queue is empty and wake when the producer inserts new work. The threaded pipeline uses the same processing and metrics logic as the async pipeline, preserving comparability."),
        ("4.5 Deadline, Latency, and Drift Measurement", "Timing measurement is implemented in src/pipeline/common.rs and src/evidence/metrics.rs. Each packet stores ingested_at and expected_ready_at timestamps. Processing records actual start and finish time. Latency is finish minus ingestion time. Processing time is finish minus actual start. Deadline miss is true when processing time exceeds the 2ms micro-deadline. Scheduling drift is actual processing start minus expected ready time."),
        ("4.6 Leaderboard Shared Resource and Synchronization Strategies", "The leaderboard in src/sync/leaderboard.rs is the shared resource used to count top domains. The main DomainLeaderboard uses a Mutex-protected HashMap. The benchmark compares Mutex, RwLock, and Atomic strategies under concurrent updates. This satisfies the synchronization requirement and provides data for discussing throughput, p99 wait time, and contention risk."),
        ("4.7 Allocation Counting and Hot-Path Proof", "Allocation counting is implemented through a custom global allocator in src/evidence/allocator.rs. The measured hot path resets thread allocation counters, parses and classifies a sample event, then compares before and after allocation snapshots. The benchmark evidence currently reports allocations 0, deallocations 0, and live_bytes 0 for the measured hot path. This supports the zero-copy parsing claim while keeping the claim scoped to parse/classify work."),
    ]
    for title, body in impl_sections:
        h(title, 2)
        p(body)

    h("5.0 Testing and Experimental Methodology")
    test_sections = [
        ("5.1 Unit and Integration Test Coverage", "The latest cargo test run passed 16 tests. Unit tests cover borrowed parsing, SSE frame extraction, zero hot-path parse allocation, bounded queue drop-oldest behaviour, priority scheduling, fail-safe recovery, percentile calculation, and leaderboard correctness. Integration tests cover deterministic async replay, threaded overflow logging, fail-safe bot drops, watchdog timeout evidence, priority-pressure drift, phased fail-safe recovery, and stalled live-source reset counting."),
        ("5.2 Controlled Demo Scenarios", "The demo command creates controlled evidence under output/demo. Normal load uses 60 events to demonstrate stable baseline behaviour. Priority pressure uses 80 events to create a bot-heavy backlog and then inject human edits. Overflow spike uses 120 events with a queue capacity of 4. Fail-safe recovery uses 80 events with a phased delay profile. Watchdog evidence waits for a real 10 second stalled-input timeout."),
        ("5.3 1000-Event Benchmark Setup", "The benchmark command uses 1000 synthetic events for larger percentile discussion. Synthetic benchmark data is selected because it keeps the workload reproducible and controlled. The benchmark bundle includes async and threaded pipeline reports, synchronization benchmark results, and allocation proof. Criterion benchmarks are also available under target/criterion/report/index.html for formal benchmark presentation."),
        ("5.4 Captured 500-Event Wikimedia Replay Setup", "Captured replay uses 500 live Wikimedia events stored in data/replay/captured_recentchange.ndjson. The same file is then replayed through both architectures. This provides authenticity evidence and a fair architecture comparison because both architectures process the same captured input sequence."),
        ("5.5 Evaluation Metrics: p50, p90, p99, Drift, Deadline Misses, Overflow, and Recovery", "The evaluation uses packet counts, latency percentiles, processing percentiles, scheduling drift, deadline misses, overflow count, maximum queue depth, degraded transitions, recovered transitions, degraded bot drops, network resets, synchronization throughput, p99 lock wait time, and allocation counts. p50 represents typical behaviour, p90 shows high-percentile behaviour, and p99 highlights tail latency. Tail latency is important in real-time systems because rare slow events can violate timing expectations."),
    ]
    for title, body in test_sections:
        h(title, 2)
        p(body)

    h("6.0 Results and Discussion")
    result_sections = [
        ("6.1 Normal Load Baseline", "The normal-load demo processed 60 of 60 packets. overflow_count was 0, deadline_misses was 0, degraded_transitions was 0, and max_queue_depth was 1. The p99 latency was approximately 195.7us. This shows that under stable controlled load the system stays within the 2ms micro-deadline and does not trigger safety mechanisms unnecessarily."),
        ("6.2 Priority Scheduling and Human-vs-Bot Drift", "The priority-pressure demo processed 80 packets and created a bot-heavy backlog before human edits arrived. human_drift.p99_us was 27835.9 and bot_drift.p99_us was 44140.6. This proves that the human-before-bot scheduler reduces p99 drift for high-priority human edits compared with low-priority bot edits under pressure."),
        ("6.3 Bounded Backpressure Under Overflow", "The overflow demo ingested 120 packets with a queue capacity of 4. The output recorded overflow_count as 116 and max_queue_depth as 4. Overflow events included dropped sequence numbers and the reason bounded-channel-overflow-drop-oldest. This demonstrates a predictable overload policy that prevents unbounded memory growth."),
        ("6.4 Fail-Safe Degraded Mode and Recovery", "The fail-safe demo processed 69 of 80 packets. It recorded one deadline miss, one degraded transition, one recovered transition, and 11 degraded bot drops. This proves that the system can enter degraded mode, shed low-priority work, and later recover when timing becomes stable."),
        ("6.5 Watchdog Network Reset Evidence", "The watchdog summary recorded event as Network Reset, timeout_seconds as 10, observed_silence_ms around 10001, and result as watchdog timeout triggered successfully. This demonstrates fault detection through a real timeout mechanism. The same timeout concept is used in live stream reading, where stalled input causes a reconnect attempt."),
        ("6.6 Async vs Threaded Architecture Comparison", "The 1000-event benchmark processed all 1000 events in both architectures. The async benchmark p99 latency was around 14186.1us with max_queue_depth 17. The threaded benchmark p99 latency was around 243329.5us with max_queue_depth 213. In this configured benchmark, async produced lower tail latency and less queue growth. Captured 500-event replay also processed all events in both architectures, with async p99 latency around 67.3us and threaded p99 latency around 15413.7us. These results are workload-specific but provide fair evidence because the same inputs are used."),
        ("6.7 Synchronization Benchmark: Mutex, RwLock, and Atomic Counters", "The synchronization benchmark compares Mutex, RwLock, and Atomic strategies. The benchmark bundle reports throughput and p99 wait time. Atomic counters provide very low wait time for known fixed domains, while Mutex and RwLock support dynamic HashMap keys with more synchronization overhead. This demonstrates the real-time trade-off between flexibility and contention cost."),
        ("6.8 Zero Hot-Path Allocation Evidence", "The allocation proof reports hot_path_delta allocations 0, deallocations 0, and live_bytes 0. This indicates that borrowed serde parsing and classification avoid heap allocation in the measured hot path. This matters because heap allocation can introduce unpredictable timing jitter. The conclusion is scoped carefully: the hot parse/classify path is zero-allocation, while reporting and other non-hot-path operations may still allocate."),
    ]
    for title, body in result_sections:
        h(title, 2)
        p(body)

    table(
        ["Evidence", "Current Result", "Concept Proven"],
        [
            ("Normal load", "60 processed, 0 deadline misses, p99 195.7us", "Baseline deadline compliance"),
            ("Priority pressure", "Human p99 drift 27835.9us < bot p99 drift 44140.6us", "Priority scheduling"),
            ("Overflow", "116 overflow events, max queue depth 4", "Bounded backpressure"),
            ("Fail-safe", "1 degraded transition, 1 recovery, 11 bot drops", "Graceful degradation"),
            ("Watchdog", "Network Reset after about 10001ms", "Fault detection and recovery"),
            ("Benchmark", "Async p99 14186.1us; threaded p99 243329.5us", "Architecture comparison"),
            ("Allocation", "0 measured hot-path allocations", "Reduced allocation jitter"),
        ],
    )

    h("7.0 Conclusion and Future Work")
    h("7.1 Summary of Achievements", 2)
    p("This project achieved a complete Rust soft real-time monitoring engine for Wikimedia recent-change events. It implements live, replay, and synthetic input; async and threaded architectures; bounded drop-oldest backpressure; human-before-bot priority scheduling; 2ms deadline measurement; scheduling drift analysis; watchdog reset evidence; fail-safe degraded mode and recovery; synchronization benchmarking; zero-copy parsing; and allocation proof. The implementation is supported by tests, JSON summaries, CSV packet logs, benchmark bundles, captured replay evidence, and Criterion reports.")
    h("7.2 Real-Time System Limitations", 2)
    p("The main limitation is that the system runs on a general-purpose Windows environment, so timing cannot be guaranteed like a hard real-time OS. The 2ms micro-deadline is an experimental target rather than a certified deadline. Synthetic scenarios are controlled to isolate behaviour, but they cannot represent every live traffic pattern. Network conditions can also affect live Wikimedia capture. These limitations are acknowledged so that the project is presented accurately as a soft real-time simulation.")
    h("7.3 Future Improvements", 2)
    p("Future work could deploy the system on a real-time Linux kernel, add more scheduling policies such as earliest-deadline-first, implement adaptive queue capacities, add a live dashboard, and collect longer real Wikimedia replay traces. Synchronization could be improved using sharded counters or lock-free maps. The fail-safe policy could also be made adaptive by adjusting thresholds based on recent load.")

    h("8.0 References")
    for ref in [
        "Burns, A., & Wellings, A. (2009). Real-time systems and programming languages. Addison-Wesley.",
        "Buttazzo, G. C. (2011). Hard real-time computing systems: Predictable scheduling algorithms and applications. Springer.",
        "Klabnik, S., & Nichols, C. (2019). The Rust programming language. No Starch Press.",
        "Tokio Project. (n.d.). Tokio: An asynchronous Rust runtime. https://tokio.rs/",
        "Wikimedia Foundation. (n.d.). EventStreams. https://stream.wikimedia.org/",
        "Serde Project. (n.d.). Serde: Serialization framework for Rust. https://serde.rs/",
        "Criterion.rs. (n.d.). Criterion.rs benchmarking library. https://bheisler.github.io/criterion.rs/book/",
    ]:
        p(ref)

    for para in doc.paragraphs:
        for run in para.runs:
            run.font.name = "Arial"

    doc.save(OUT)
    print(OUT.resolve())


if __name__ == "__main__":
    main()
