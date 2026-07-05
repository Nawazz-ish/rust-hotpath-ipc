# rust-hotpath-ipc

An extracted hot-path IPC subsystem from a proprietary crypto trading platform. It moves order commands, market ticks, and execution reports between separate OS processes over zero-copy shared memory, on the critical path of a live trading engine, in sub-microsecond time. This repository is an anonymized slice of the production system: the message layout, the latency-measurement pipeline, and the real-time scheduling logic are intact; everything that touches persistence, accounting, or venue credentials has been deliberately left out (see Architecture). It is provided as a standalone, self-contained Rust crate.

## Architecture

The parent system is split along a hard boundary between a **hot path** and a **cold path**. This crate is the hot path.

The hot path carries anything a decision depends on within a trade cycle: an order going out, a tick coming in, a fill coming back. It must be fast and it must be predictable. The cold path carries everything else — persistence, audit trails, compliance checks, reconciliation, reporting — where correctness and durability matter and a few milliseconds do not.

The single most important design property here is what is *absent*. There is no database driver, no connection pool, no audit sink, no ORM, no HTTP server, nothing that can block, allocate under load, or take a lock held by another subsystem. That is not an accident of extraction; it is the architecture. The proof is mechanical: this crate compiles with zero database or web dependencies. If a `sqlx`, `postgres`, `axum`, or audit crate ever appeared in `Cargo.toml`, it would mean the hot/cold boundary had been violated, and the build is the thing that catches it. Anything durable happens off this path, downstream, on a cold-path consumer that reads the same shared-memory stream without ever sitting between a signal and an order.

The moving parts:

- **64-byte, cache-line-aligned POD messages.** `OrderCommand`, `MarketTick`, `ExecutionReport`, and `OrderBookSnapshot` are each exactly one cache line, `#[repr(C, align(64))]`, plain-old-data with no pointers or owned heap. They are copied bit-for-bit with no serialization step.
- **Zero-copy IPC over shared memory.** Messages are published into and received from a lock-free shared-memory ring buffer. The payload is written once, in place, and read in place by the subscriber. Nothing is serialized, framed, or copied through a kernel socket buffer on the way across.
- **RDTSC latency measurement with percentile aggregation.** Each message is timestamped with the CPU timestamp counter at send and at receive. On the receive thread the only work per message is one timestamp read and a push into a pre-allocated buffer; when a window fills, the buffer is handed to a separate reporter thread over a channel, and that thread does the sorting and printing. So the sort and the I/O never run on the pinned, real-time receive loop. Cycles are converted to nanoseconds using the crate's runtime-calibrated TSC frequency, not a hardcoded clock assumption. Results are reported as percentiles (min/p50/p99/p99.9/max), not an average, because the tail is what a trading path cares about.
- **CPU core pinning and real-time scheduling.** Publisher and subscriber threads pin themselves to specific cores and, on Linux, raise themselves to `SCHED_FIFO` real-time priority, so the OS scheduler does not migrate them or preempt them mid-cycle.

## Attribution

I want to be exact about what is mine and what is not, because it matters for reviewing this honestly.

**The shared-memory transport is [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) (Eclipse iceoryx / ekxide).** iceoryx2 provides the hard part of true zero-copy IPC: the lock-free shared-memory ring buffer, the publish/subscribe service discovery and lifecycle, the loan/send/receive sample API, and the safe-overflow semantics. I did not write a shared-memory allocator or a lock-free queue, and I would be suspicious of anyone who claimed to have hand-rolled one for an interview. iceoryx2 is doing the transport.

**What I designed on top of it:**

- The POD message layout and the fixed-point encoding of prices and quantities — the 64-byte cache-line discipline, the field packing, and the integer representation that keeps the wire format deterministic.
- The RDTSC latency pipeline — capturing the timestamp counter at the right points, keeping collection off the hot path, and aggregating into percentiles rather than averages.
- The CPU pinning and `SCHED_FIFO` real-time scheduling setup for the publisher and subscriber threads.
- The hot/cold architecture itself — the decision that this path carries no database, audit, or compliance coupling, and the layout that makes that boundary enforceable at compile time.

In short: iceoryx2 is the road; the message format, the timing instrumentation, the scheduling, and the separation of concerns are the vehicle I built to drive on it.

## Design decisions

Each of these is a trade-off, so here is the reasoning, not just the choice.

**64-byte cache-line alignment — to kill false sharing.** On x86-64 a cache line is 64 bytes. If two messages, or a message and an unrelated counter, share a line, a write to one invalidates the other in every core's cache, and you pay coherency traffic you never asked for. Sizing every message to exactly one aligned line means a producer writing message N never touches the line message N+1 lives on. The cost is padding — some messages carry unused bytes — and that is a cost worth paying to keep the cores from fighting over cache lines.

**Fixed-point integers, not floats — for determinism.** Prices and sizes are encoded as scaled integers. Floating point is not associative, rounds differently depending on operation order, and can produce a value that differs by a bit between two machines or two compilers. On a path where two processes must agree on exactly what price an order was placed at, "close enough" is a bug. Integers compare and hash exactly, serialize trivially into the POD layout, and behave the same everywhere. The scale factor is fixed and known, so the arithmetic is plain integer arithmetic.

**Zero-copy — because serialization does not belong on the hot path.** Serializing a struct to bytes, pushing it through a socket, and deserializing on the other side costs CPU, costs allocations, and costs cache. Because the messages are POD and laid out in shared memory, "sending" is writing the struct into a loaned slot and publishing a pointer to it; "receiving" is reading the struct in place. The bytes are authored once and never transformed. There is no encode/decode step to show up in a flame graph.

**RDTSC — for cycle-resolution timing without a syscall.** `clock_gettime` is a syscall (or a vDSO call) and carries overhead comparable to the intervals being measured, which corrupts the measurement. `RDTSC` reads the CPU's timestamp counter in a handful of cycles, entirely in userspace, at single-cycle resolution — roughly a third of a nanosecond at 3 GHz, finer than the latencies of interest. On modern parts the counter is invariant (constant rate regardless of frequency scaling), which makes it usable as a stopwatch. The cycle-to-nanosecond factor is measured at startup against the wall clock (see `tsc_calibration`) rather than assumed, and refreshed periodically. Where the architecture lacks the instruction, the code falls back to a system-time source so it still builds and runs.

**Core pinning and `SCHED_FIFO` — to make latency predictable, not just low.** A trading hot path cares about the tail more than the mean. The enemies of the tail are the scheduler migrating a thread to a cold core and an unrelated process preempting it mid-cycle. Pinning each thread to a dedicated core keeps its working set warm in that core's cache; `SCHED_FIFO` at high priority tells the kernel not to preempt it for ordinary work. Together they trade some of the machine's general-purpose fairness for a tighter, more predictable latency distribution — exactly the trade a trading system wants to make.

## Running it

**Prerequisites:**

- Linux. The scheduling and pinning paths are written for Linux; `SCHED_FIFO` in particular is Linux-only. Raising real-time priority needs the appropriate privilege (run under a user with `CAP_SYS_NICE` or the relevant `rlimit`, or via `sudo` for the demo).
- A C/C++ toolchain — `clang` (or `gcc`), `cmake`, and `make` — because iceoryx2 builds a native component at compile time.
- A recent stable Rust toolchain.

**Run the demo:**

```
make demo
```

This starts a subscriber and a publisher as separate processes over the shared-memory service, streams a burst of messages between them, and prints a latency percentile table gathered from the RDTSC deltas once the run completes.

Each line is one 100,000-message window: a single publisher and a single subscriber pinned to separate cores, with the reporter pinned to a third so sorting and printing never touch the receive loop.

**Measured output** (AWS `c7i.xlarge`, Intel Xeon Platinum 8488C, invariant TSC, Ubuntu, 4 vCPU; publisher core 2, subscriber core 3, reporter core 0, both hot threads under `SCHED_FIFO`):

```
n=  100000  Min=    320 ns  P50=    857 ns  P99=   1272 ns  P99.9=   3856 ns  Max=  13176 ns  loss=1.08%
n=  100000  Min=    330 ns  P50=    850 ns  P99=   1259 ns  P99.9=   5555 ns  Max=  12371 ns  loss=1.11%
n=  100000  Min=    327 ns  P50=    855 ns  P99=   1254 ns  P99.9=   9192 ns  Max=  12585 ns  loss=0.02%
n=  100000  Min=    302 ns  P50=    852 ns  P99=   1263 ns  P99.9=   3813 ns  Max=  12243 ns  loss=0.92%
n=  100000  Min=    311 ns  P50=    850 ns  P99=   1249 ns  P99.9=   6450 ns  Max=  15817 ns  loss=0.02%
```

Steady state on that box lands around **P50 ~850 ns, P99 ~1.25 µs** for end-to-end order delivery between two processes, with sub-0.1% loss once warmed up. (The very first window after startup shows a large `Max` and high loss — the subscriber is still attaching while the publisher is already at full rate — so the numbers above are steady-state windows, not the cold-start one.)

These will not reproduce exactly on other hardware: absolute latency depends on the CPU, whether the timestamp counter is invariant, how cores are isolated, thermal and frequency state, and machine load. The **method** is the portable part and it is the point: timing with `RDTSC` rather than a syscall, converting cycles to nanoseconds with a runtime-calibrated frequency, keeping the sort and I/O off the pinned receive thread, and reporting percentiles rather than an average so the tail is visible. A mean latency figure on a hot path hides exactly the behavior a trading engineer needs to see; the P99 and P99.9 rows are the ones that matter.

The loss figure is intentional and worth reading: the publisher runs flat-out (tens of millions of messages per second) into a bounded ring with safe-overflow enabled, so under a hard imbalance the ring drops rather than blocks the producer — a deliberate choice for a market-data-style path where the freshest message matters more than delivering every stale one. Sizing the ring, batching the consumer, or rate-matching the producer all move that number; it is a knob, not a defect.

## What I'd do next

This is a focused slice, and there are clear next steps I deliberately left out to keep it honest and small:

- **Batching.** Publishing and receiving in small batches would amortize the per-message loan/send overhead and lift throughput, at the cost of a little latency on the first message of a batch. The right batch size is workload-dependent and worth measuring rather than guessing.
- **MPMC.** The demo is single-publisher, single-subscriber. Real deployments want multiple producers (several strategies emitting orders) and multiple consumers (execution plus a cold-path recorder). That means moving from the current topology to a multi-producer, multi-consumer arrangement and being careful about ordering and fairness guarantees under contention.
- **Huge pages.** Backing the shared-memory segment with 2 MB huge pages would cut TLB pressure and the associated tail-latency jitter for large ring buffers. It needs system configuration and a fallback path, so it belongs behind a flag rather than on by default.
