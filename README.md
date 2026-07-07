# rust-hotpath-ipc

An extracted hot-path IPC subsystem from a proprietary crypto trading platform. It runs a full tick-to-trade loop across separate OS processes — a **matching engine** with a real limit order book, a **strategy** that reads ticks and emits orders, and an **execution** stage that books fills and P&L — all wired over **zero-copy shared-memory IPC** and instrumented with **RDTSC nanosecond latency measurement**. This repository is an anonymized slice of the production system: the message layout, the matching engine, the latency-measurement pipeline, and the real-time scheduling are intact; everything that touches persistence, accounting, or venue credentials has been deliberately left out (see [Architecture](#architecture)). It is a standalone, self-contained Rust crate.

## Quick start

Linux, a recent stable Rust toolchain, and a C/C++ toolchain (`clang`/`gcc`, `cmake`, `make` — iceoryx2 builds a native component). Real-time scheduling and CPU pinning need privilege, so the run targets use `sudo`.

```sh
cargo test              # unit tests — the matcher's guarantees are proven here
sudo make demo-execution  # watch orders sweep the book and PARTIALLY FILL, with live P&L
sudo make demo-latency    # the three tick-to-trade latency windows (decision / order / fill)
sudo make studio          # visual strategy builder + live pipeline on http://localhost:8080
```

The three `make demo-*` targets run the same `exchange → strategy → execution` pipeline, pinned to cores 1/2/3 with latency reporters on core 0; let each run ~15 seconds then `Ctrl-C`. Tunables are env vars (`TICK_US`, `THRESHOLD`, `MAX_POSITION`, `ORDER_UNITS`, `PASSIVE=1`, `WAIT_MODE=waitset`) — e.g. `sudo make demo-execution ORDER_UNITS=5`. `make demo` runs the raw transport microbenchmark on its own. Viewing `studio` from a laptop when the server is remote: forward the port with `ssh -L 8080:localhost:8080 …`, then open `localhost:8080`.

## Architecture

The parent system is split along a hard boundary between a **hot path** and a **cold path**. This crate is the hot path.

The hot path carries anything a decision depends on within a trade cycle: an order going out, a tick coming in, a fill coming back. It must be fast and it must be predictable. The cold path carries everything else — persistence, audit trails, compliance checks, reconciliation, reporting — where correctness and durability matter and a few milliseconds do not.

The single most important design property here is what is *absent*. There is no database driver, no connection pool, no audit sink, no ORM, no HTTP server, nothing that can block, allocate under load, or take a lock held by another subsystem. That is not an accident of extraction; it is the architecture. The proof is mechanical: this crate compiles with zero database or web dependencies. If a `sqlx`, `postgres`, `axum`, or audit crate ever appeared in `Cargo.toml`, it would mean the hot/cold boundary had been violated, and the build is the thing that catches it. Anything durable happens off this path, downstream, on a cold-path consumer that reads the same shared-memory stream without ever sitting between a signal and an order.

The moving parts:

- **64-byte, cache-line-aligned POD messages.** `OrderCommand`, `MarketTick`, `ExecutionReport`, and `OrderBookSnapshot` are each exactly one cache line, `#[repr(C, align(64))]`, plain-old-data with no pointers or owned heap. They are copied bit-for-bit with no serialization step.
- **Zero-copy IPC over shared memory.** Messages are published into and received from a lock-free shared-memory ring buffer. The payload is written once, in place, and read in place by the subscriber. Nothing is serialized, framed, or copied through a kernel socket buffer on the way across.
- **RDTSC latency measurement with percentile aggregation.** Each message is timestamped with the CPU timestamp counter at send and at receive. On the receive thread the only work per message is one timestamp read and a push into a pre-allocated buffer; when a window fills, the buffer is handed to a separate reporter thread over a channel, and that thread does the sorting and printing. So the sort and the I/O never run on the pinned, real-time receive loop. Cycles are converted to nanoseconds using the crate's runtime-calibrated TSC frequency, not a hardcoded clock assumption. Results are reported as percentiles (min/p50/p99/p99.9/max), not an average, because the tail is what a trading path cares about.
- **CPU core pinning and real-time scheduling.** Hot threads pin themselves to specific cores and, on Linux, raise themselves to `SCHED_FIFO` real-time priority, so the OS scheduler does not migrate them or preempt them mid-cycle.
- **A real matching engine, not a loopback fill.** Orders match against a limit order book with price-time priority, partial fills, and queue position — see [The matching engine](#the-matching-engine).

## Repository layout

The library (`src/`) is transport-agnostic logic; the runnable processes (`src/bin/`) are where that logic is wired onto the shared-memory bus. The strategy, the bytecode VM, and the latency recorder have no dependency on the IPC layer at all — only the process binaries touch iceoryx2. That split is deliberate: the decision logic is a pure function of the price stream, and the transport is swappable underneath it.

```
src/
  hot_path.rs        the 64-byte POD message contract + rdtsc()  (the wire types iceoryx2 carries)
  order_book.rs      the limit order book + matcher (price-time priority, partial fills, cancels)
  strategy.rs        composite trend / momentum / mean-reversion strategy
  compiler.rs        a drawn strategy graph  ->  a flat bytecode program
  bytecode.rs        a stack VM that interprets that program, once per tick
  latency_window.rs  the off-hot-path latency recorder (lock-free push, off-core aggregation)
  tsc_calibration.rs cycle <-> nanosecond calibration
  bin/
    exchange.rs      pipeline stage 1: order-book matching engine + market source } the trading
    strategy.rs      pipeline stage 2: signal -> risk -> OrderCommand              } system —
    execution.rs     pipeline stage 3: consumes fills, tracks position + P&L       } run all three
    control-server.rs serves the visual builder and launches the pipeline
    disasm.rs        tool: print the bytecode a strategy graph compiles to
    bench-publisher.rs / bench-subscriber.rs   a microbenchmark of raw iceoryx2 transport latency
webui/               the drag-and-drop strategy builder (a thin UI over the real engine)
```

The iceoryx2 calls all live in these binaries. The core sequence, from `exchange.rs`: `publisher.loan_uninit()` borrows an uninitialized slot **that physically lives in the shared-memory segment**, `write_payload(tick)` writes the 64-byte message straight into that slot, and `send()` publishes by handing off the pointer — no copy. The consumer's `receive()` returns a reference to those same bytes, read in place. That `loan → write → send` / `receive` cycle, over POD messages that are safe to interpret byte-for-byte in another process, *is* the zero-copy path.

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

## The matching engine

The `exchange` stage is a real **limit order book** — two price-sorted sides (`BTreeMap` of price levels), each a FIFO queue, so matching honours **price-time priority**. It lives in `src/order_book.rs` as a pure, unit-tested module with no IPC dependency; the binary wires it onto the bus and drives it.

- A **marketable** order crosses the spread and fills against resting liquidity, walking several price levels for size — so a large order **partially fills** at worsening prices (real slippage). `sudo make demo-execution ORDER_UNITS=5` shows this: one order id filling in pieces at different prices.
- A **passive** limit order (`PASSIVE=1`) rests and only fills once the queue ahead of it clears — so **queue position** is a real thing you can watch.
- Because a venue's price is whatever its book says, the exchange is also the **market-data source**: seeded synthetic participants post and cancel around a drifting fair value, and the top of book becomes the `MarketTick` stream the strategy trades on (plus an `OrderBookSnapshot` feed for depth). The ticks the strategy sees and the fills it gets come from the *same* book, so `tick-to-fill` is a genuine round-trip through the matcher, not a loopback.

The matcher's guarantees — price-time priority both sides, partial fills across levels, queue position, cancels — are covered by unit tests in `order_book.rs` (`cargo test`).

## Transport benchmark

Separately from the pipeline, `make demo` measures the *raw* iceoryx2 transport: one publisher and one subscriber pinned to separate cores exchanging 100,000-message windows, RDTSC-timed, reporter pinned to a third core so sorting never touches the receive loop. On the c7i.xlarge it lands around **P50 ~850 ns, P99 ~1.25 µs** for end-to-end delivery between two processes:

```
n=100000  Min=320 ns  P50=857 ns  P99=1272 ns  P99.9=3856 ns  Max=13176 ns  loss=1.08%
n=100000  Min=327 ns  P50=855 ns  P99=1254 ns  P99.9=9192 ns  Max=12585 ns  loss=0.02%
```

Numbers won't reproduce exactly elsewhere — the **method** is the portable part: RDTSC instead of a syscall, runtime-calibrated cycles→ns, sort/IO off the pinned receive thread, percentiles not averages (the tail is what a trading path cares about). The `loss` is intentional: the publisher runs flat-out into a bounded ring with safe-overflow, so under imbalance it drops the oldest rather than block the producer — the right call for market data, and a tunable knob.

**What these numbers are — and are not.** The box is a `c7i.xlarge`, an AWS Nitro *VM*, not bare metal. These are **in-process compute + intra-host shared-memory IPC latency** — not wire-to-wire exchange latency; there is no NIC and no kernel-bypass. RDTSC is trustworthy on the VM because the TSC is invariant (`constant_tsc`/`nonstop_tsc`) and Nitro runs `rdtsc` in the guest without a hypervisor trap. Because the hot loops are pinned userspace with no syscalls in steady state, the hypervisor isn't in the critical path — so the **median** is representative of the same microarchitecture on bare metal. The **tail** is where not owning the host shows (no `isolcpus`/`nohz_full`, shared uncore, possible vCPU steal → occasional multi-µs P99.9 outliers). The p50 is the claim; the p99.9 is honest about the environment.

## Tick-to-trade latency

The transport benchmark measures the pipe. The more interesting number is how fast a **strategy** executes through the pipeline. `sudo make demo-latency` runs `exchange → strategy → execution` and reports three windows:

- **decision-only** — the `Strategy::on_price()` call in isolation: three EMAs, a momentum reading, a rolling-window z-score, the weighted blend, and the threshold decision. Pure signal math, no IPC.
- **tick-to-order** — from the origin tick's timestamp to the moment the strategy emits the order: decision plus one shared-memory hop.
- **tick-to-fill** — from the origin tick to the fill: the full round-trip out to the matching engine and back.

**Measured** (AWS `c7i.xlarge`, Xeon 8488C, invariant TSC; exchange core 1, strategy core 2, execution core 3, latency reporters core 0; the strategy under `SCHED_FIFO`; marketable orders):

```
LAT decision-only  n=2000  min= 63  p50=  69  p99= 146  p999= 240  ns
LAT tick-to-order  n= 251  min=717  p50= 880  p99=1205  p999=1645  ns
LAT tick-to-fill   n= 253  min=75200 p50=76623 p99=93107 p999=130074 ns
```

Reading the breakdown:

- **The strategy decides in ~69 ns** at p50 — the whole composite signal, per tick. This is the number I stand behind: it is my code, on the hot path, and it does not change when the venue does.
- **tick-to-order ≈ 880 ns**; subtract the ~69 ns of decision and the remainder (~810 ns) is one Iceoryx2 shared-memory hop — which matches the transport benchmark's ~850 ns/P50 above, an independent confirmation of both numbers.
- **tick-to-fill ≈ 77 µs**, and this is where the matching engine shows up honestly. Under the old loopback fill this was ~1.75 µs (just the strategy→execution hop). Now the order round-trips to a real order book: it waits for the exchange's match loop to drain it, crosses the spread against resting liquidity, and a fill comes back. Most of the 77 µs is *not* transport — it is the order sitting until the exchange's next match/publish round. That is the true cost of going to a venue rather than filling in-process, and it is the right thing to see.

In **passive** mode (`PASSIVE=1`, the strategy posts resting limit orders at the touch) tick-to-fill is different in kind, not just degree: it becomes **queue-wait** — the order rests and only fills once flow reaches its price and the queue ahead of it clears — so the p50 is hundreds of µs and the tail runs into milliseconds. That is what making a market actually looks like, and the same window measuring two very different things (taking vs. making) is the point.

So the split that matters holds: the *decision* is ~69 ns of my code, the *order hop* is ~810 ns of framework transport, and the *fill* is dominated by the venue — exactly the separation an execution engineer reasons about.

**Not perturbing the measurement.** The per-sample cost on the hot path is a timestamp read, a subtract, one float multiply, and a push into a pre-allocated buffer; all sorting and printing happen on a reporter thread pinned to a separate core. The decision-only window is short enough (tens of ns) that the timestamp read itself is a material fraction, so it uses a serializing read (`rdtscp`/`lfence`, so the CPU can't reorder work out of the window) and subtracts a read overhead calibrated at startup. The larger windows are hundreds of ns to µs, where the ~6 ns read is noise, so they use a plain read and no correction. Cross-stage deltas subtract timestamps taken on different cores, which is valid here because this CPU's TSC is invariant and synchronized across cores.

The visual builder (`make studio`, then open `:8080`) shows these three windows and the decision-vs-transport breakdown live while a strategy you draw runs.

## Waiting for the next tick: busy-poll vs. blocking

The pipeline is event-driven at the granularity of a tick — every market tick is an event that drives one decision. *How* the strategy waits for that event is the classic hot-path tradeoff, and the strategy supports both (`WAIT_MODE=poll|waitset`):

- **poll (default)** — the receive loop busy-spins on `receive()`, never sleeping. The instant a tick lands in shared memory the next loop iteration sees it. Lowest latency; costs a dedicated core.
- **waitset** — the feed publishes an iceoryx2 event notification after each tick, and the strategy blocks on the matching listener (`blocking_wait_one`) until signalled, then drains. The core is free between ticks; the price is wake-up latency (a kernel context switch and scheduler dispatch to go from blocked to running).

Measured on the c7i (`tick-to-order` p50, and the strategy process's CPU):

| mode | tick-to-order p50 | p99 | strategy CPU |
|---|---:|---:|---:|
| poll (busy-spin) | ~1.08 µs | ~1.4 µs | ~99% (one core) |
| waitset (blocking) | ~8.0 µs | ~19 µs | ~3% |

So blocking frees the core but adds ~7 µs of wake-up latency — about 7× the busy-poll number. That is exactly why a trading system busy-polls the critical path (spend a core to eliminate wake-up latency) and reserves blocking waits for cold-path consumers (audit, metrics, recording) where a few microseconds do not matter and CPU efficiency does. The default here is `poll`; `WAIT_MODE=waitset` demonstrates the other end of the tradeoff.

## Graph-defined strategies compiled to bytecode

The strategy above is hand-written Rust. The visual builder lets you *draw* a strategy from nodes (indicators, conditions, logic gates, buy/sell signals), and that graph is not just tuning parameters — it is **compiled to a flat bytecode program and interpreted per tick**. Change the wiring and the emitted program changes, so the drawn graph genuinely drives execution.

The compiler (`src/compiler.rs`) topologically lowers the node graph, emitting operands before the operators that consume them; the VM (`src/bytecode.rs`) is a stack machine over `f64` with the terminal `BUY`/`SELL` opcodes popping a truthy condition. Indicators are stateful ops — each carries an index into a state vector that persists across ticks — so they work on a live tick stream rather than a precomputed series. A drawn graph disassembles like this (opcodes are in the same space as the parent platform's VM — `BUY = 0x30`, `GT = 0x50`):

```
  0  0x61  MOMENTUM  lookback=16 slot=0
  1  0x40  PUSH_CONST 0.15
  2  0x50  GT
  3  0x30  BUY
  4  0x62  REVERSION window=64 slot=1
  5  0x40  PUSH_CONST -0.15
  6  0x51  LT
  7  0x31  SELL
```

The obvious question is whether interpreting bytecode per tick is too slow for a hot path. Measured on the same c7i, the interpreter's per-tick cost (`vm-decision`) sits right next to the hand-written strategy's:

```
LAT vm-decision   n=2000  min=61  p50=74  p99=112  p999=138  ns   (bytecode interpreter, 8 ops)
LAT decision-only n=2000  min=61  p50=67  p99=161  p999=215  ns   (hand-written Rust)
```

**~74 ns to interpret the compiled strategy versus ~67 ns for native code** — within a few nanoseconds at the median, and *tighter* at the tail. A straight-line program over a fixed stack with pre-allocated indicator state and no per-tick allocation costs almost nothing to interpret, so the flexibility of graph-defined strategies is essentially free on this path. The interpreter dispatches on a typed opcode enum for speed; `to_bytes()` produces the equivalent flat byte program for wire transport or inspection.

The obvious follow-up — is the interpreter really doing the work, or is the optimizer eliding it? — is answered by the cost scaling with program size (same c7i, `vm-decision` p50):

| program | ops | p50 |
|---|---:|---:|
| momentum → condition → buy | 4 | 43 ns |
| momentum → buy, reversion → sell | 8 | 74 ns |
| (momentum AND reversion) → buy, reversion → sell | 10 | 95 ns |

Latency grows roughly linearly at ~5–6 ns per op (~15–18 cycles), exactly what a match-dispatch plus a couple of arithmetic ops per instruction should cost. If the VM were being optimized away the three would be indistinguishable; instead the cost tracks the work, which is the evidence that the interpreter is genuinely executing the compiled graph.

A note on faithful compilation: the compiler rejects ambiguous graphs (e.g. two indicators wired straight into one condition) with a clear error rather than silently dropping an input, so the disassembly always matches the graph you drew. Use `cargo run --bin disasm -- graph.json` to see the compiled bytecode for any graph.

## What I'd do next

This is a focused slice, and there are clear next steps I deliberately left out to keep it honest and small:

- **Batching.** Publishing and receiving in small batches would amortize the per-message loan/send overhead and lift throughput, at the cost of a little latency on the first message of a batch. The right batch size is workload-dependent and worth measuring rather than guessing.
- **MPMC.** The demo is single-publisher, single-subscriber. Real deployments want multiple producers (several strategies emitting orders) and multiple consumers (execution plus a cold-path recorder). That means moving from the current topology to a multi-producer, multi-consumer arrangement and being careful about ordering and fairness guarantees under contention.
- **Huge pages.** Backing the shared-memory segment with 2 MB huge pages would cut TLB pressure and the associated tail-latency jitter for large ring buffers. It needs system configuration and a fallback path, so it belongs behind a flag rather than on by default.
