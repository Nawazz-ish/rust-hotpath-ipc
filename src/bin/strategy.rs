//! Strategy engine: stage 2 of the trading pipeline.
//!
//! Subscribes to `MarketTick`s from the feed, runs the composite strategy
//! (trend + momentum + mean-reversion, see `crate::strategy`) on each price, and
//! when the blended score crosses the threshold publishes an `OrderCommand` to
//! the order service for the execution stage to fill.
//!
//! This is the classic hot-path shape: consume market data on one shared-memory
//! service, emit orders on another, with the decision logic in between and no
//! database or network on the path.
//!
//! Run with:  CPU_CORE=2 cargo run --release --bin strategy

use core_affinity::CoreId;
use iceoryx2::prelude::*;
use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
};

use rust_hotpath_ipc::bytecode::{self, Vm};
use rust_hotpath_ipc::compiler;
use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::latency_window::{calibrate_rdtsc_floor, LatencyReporter};
use rust_hotpath_ipc::strategy::{Decision, Side, Strategy, StrategyConfig};
use rust_hotpath_ipc::tsc_calibration::fast_cycles_to_ns;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpu_id: usize = env::var("CPU_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    if core_affinity::set_for_current(CoreId { id: cpu_id }) {
        println!("strategy pinned to CPU core {}", cpu_id);
    }
    #[cfg(target_os = "linux")]
    unsafe {
        let param = libc::sched_param { sched_priority: 90 };
        let _ = libc::sched_setscheduler(0, libc::SCHED_FIFO, &param);
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))?;
    }

    // Wait mode: how the hot loop waits for the next tick.
    //   poll    (default) — busy-poll `receive()`, spinning; lowest latency, burns a core.
    //   waitset           — block on an iceoryx2 event listener until the feed
    //                       notifies, then drain; frees the core, adds wake-up latency.
    let wait_mode = env::var("WAIT_MODE").unwrap_or_else(|_| "poll".into());
    let use_waitset = wait_mode == "waitset";

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    // Input: market ticks from the feed.
    let market = node
        .service_builder(&MARKET_SERVICE.try_into()?)
        .publish_subscribe::<MarketTick>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let ticks = market.subscriber_builder().create()?;

    // Event listener paired with the tick service — used only in waitset mode to
    // block until the feed signals a new tick.
    let market_event = node
        .service_builder(&MARKET_EVENT.try_into()?)
        .event()
        .open_or_create()?;
    let listener = market_event.listener_builder().create()?;

    // Output: orders to the execution stage.
    let orders_svc = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let orders = orders_svc.publisher_builder().create()?;

    // The strategy is fully env-tunable so the same binary can be driven by the
    // visual builder (which passes signal weights and thresholds) without a
    // rebuild. Any unset knob keeps its default.
    let mut cfg = StrategyConfig::default();
    let envf = |k: &str| env::var(k).ok().and_then(|v| v.parse::<f64>().ok());
    let envu = |k: &str| env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    if let Some(t) = envf("THRESHOLD") {
        cfg.threshold = t;
    }
    if let Some(w) = envf("WEIGHT_TREND") {
        cfg.weight_trend = w;
    }
    if let Some(w) = envf("WEIGHT_MOMENTUM") {
        cfg.weight_momentum = w;
    }
    if let Some(w) = envf("WEIGHT_REVERSION") {
        cfg.weight_reversion = w;
    }
    if let Some(p) = envu("FAST_EMA") {
        cfg.fast_ema_period = p;
    }
    if let Some(p) = envu("SLOW_EMA") {
        cfg.slow_ema_period = p;
    }
    let mut strat = Strategy::new(cfg);

    // Risk limit: the strategy will not send an order that would take its net
    // position beyond +/- MAX_POSITION units. This is the risk check on the hot
    // path — cheap, pre-trade, and it keeps the book bounded regardless of how
    // strong the signal is. Real systems layer notional and loss limits here too.
    let max_position: i64 = env::var("MAX_POSITION")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    // Latency windows, aggregated off the hot path on a reporter thread pinned
    // to REPORTER_CORE (a core no stage's hot loop owns). This stage owns two:
    //   - decision-only: the Strategy::on_price() call in isolation (per tick)
    //   - tick-to-order: origin tick timestamp -> order emitted
    let reporter_core: usize = env::var("REPORTER_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let reporter = LatencyReporter::new(reporter_core);
    let mut win_decision = reporter.window("decision-only");
    let mut win_tick_to_order = reporter.window("tick-to-order");
    // Timed window for the bytecode VM path, when a graph-defined strategy is
    // loaded (directly comparable to decision-only, the hand-written strategy).
    let mut win_vm = reporter.window("vm-decision");
    // Irreducible cost of the serialized read, subtracted from decision-only
    // (whose interval is short enough that the read overhead is material).
    let rdtsc_floor = calibrate_rdtsc_floor();

    // If STRATEGY_JSON points at a graph the visual builder produced, compile it
    // to bytecode and run THAT per tick — the drawn graph actually drives
    // execution. A parse or compile failure is reported (not silently ignored),
    // and the strategy falls back to the hand-written composite.
    let mut vm: Option<Vm> = match env::var("STRATEGY_JSON").ok() {
        Some(path) => {
            match std::fs::read_to_string(&path) {
                Ok(json) => match compiler::parse(&json)
                    .and_then(|g| compiler::compile(&g).map_err(|e| e.to_string()))
                {
                    Ok(prog) => {
                        let vm = Vm::new(prog);
                        if vm.is_empty() {
                            println!("  strategy graph compiled to an empty program; using fixed strategy");
                            None
                        } else {
                            Some(vm)
                        }
                    }
                    Err(e) => {
                        println!("  strategy graph failed to compile ({e}); using fixed strategy");
                        None
                    }
                },
                Err(e) => {
                    println!("  could not read STRATEGY_JSON ({e}); using fixed strategy");
                    None
                }
            }
        }
        None => None,
    };

    println!("strategy: consuming MarketTick, emitting OrderCommand");
    println!("  in:  {}", MARKET_SERVICE);
    println!("  out: {}", ORDER_SERVICE);
    match &vm {
        Some(v) => {
            println!("  mode: bytecode VM ({} ops)", v.program().len());
            print!("{}", v.disassemble());
        }
        None => println!("  mode: fixed composite strategy"),
    }
    println!(
        "  wait_mode: {}  threshold: {:+.3}  max_position: +/-{}  reporter_core: {}  rdtsc_floor: {} cyc",
        if use_waitset { "waitset (blocking)" } else { "poll (busy-spin)" },
        cfg.threshold,
        max_position,
        reporter_core,
        rdtsc_floor
    );

    let mut ticks_seen = 0u64;
    let mut orders_sent = 0u64;
    let mut order_id = 0u64;
    let mut position: i64 = 0; // net units the strategy intends to hold
    let mut suppressed = 0u64; // orders blocked by the position limit

    while running.load(Ordering::Relaxed) {
        let sample = match ticks.receive()? {
            Some(s) => s,
            None => {
                if use_waitset {
                    // Block in the kernel until the feed notifies a new tick.
                    // This frees the core between ticks at the cost of wake-up
                    // latency (the poll path never sleeps).
                    let _ = listener.blocking_wait_one();
                    continue;
                }
                std::hint::spin_loop();
                continue;
            }
        };
        let tick: MarketTick = *sample;
        ticks_seen += 1;

        // T0: the origin tick's timestamp — the clock starts here.
        let t0 = tick.timestamp_ns;

        // Fixed-point (1e8) wire price -> f64 for the strategy math.
        let price = tick.price as f64 / 100_000_000.0;

        // Periodic heartbeat so the demo shows the strategy is alive and what
        // the signals look like even during quiet stretches.
        if ticks_seen.is_multiple_of(50_000) {
            let (trend, mom, rev) = strat.signals();
            println!(
                "  .. {} ticks, px={:.2}  signals[trend={:+.2} mom={:+.2} rev={:+.2}]  orders={}",
                ticks_seen, price, trend, mom, rev, orders_sent
            );
        }

        // Decision, timed in isolation. Serialized reads (rdtscp) bracket the
        // call so the CPU can't reorder work out of the window; subtract the
        // calibrated read floor since the interval is short enough that the read
        // overhead is material. The bytecode-VM path and the fixed-strategy path
        // feed distinct windows (vm-decision vs decision-only) so their per-tick
        // cost is directly comparable.
        let trade: Option<(Side, f64)> = match &mut vm {
            Some(vm) => {
                let d0 = rdtsc_serialized();
                let d = vm.run(price);
                let d1 = rdtsc_serialized();
                win_vm.push(fast_cycles_to_ns(
                    d1.saturating_sub(d0).saturating_sub(rdtsc_floor),
                ));
                bytecode::to_side(d)
            }
            None => {
                let d0 = rdtsc_serialized();
                let decision = strat.on_price(price);
                let d1 = rdtsc_serialized();
                win_decision.push(fast_cycles_to_ns(
                    d1.saturating_sub(d0).saturating_sub(rdtsc_floor),
                ));
                match decision {
                    Decision::Trade { side, score } => Some((side, score)),
                    Decision::Hold => None,
                }
            }
        };

        if let Some((side, score)) = trade {
            // Pre-trade risk check: would this order breach the position limit?
            // A buy at +max (or a sell at -max) is suppressed; orders that
            // reduce or flip toward flat are always allowed.
            let delta: i64 = match side {
                Side::Buy => 1,
                Side::Sell => -1,
            };
            let new_position = position + delta;
            if new_position > max_position || new_position < -max_position {
                suppressed += 1;
                continue;
            }
            position = new_position;

            order_id += 1;
            // T1: the moment the strategy commits to sending this order.
            let t1 = rdtsc();
            let cmd = OrderCommand {
                timestamp_ns: t1,
                order_id,
                price_ticks: tick.price, // marketable: submit at last price
                quantity: 100_000_000,   // 1.0 unit
                origin_ts: t0,           // carry T0 through for tick-to-fill downstream
                symbol_id: tick.symbol_id,
                user_id: 1,
                side: match side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
                order_type: 0, // market
                action: 0,     // new
                flags: 0,
                exchange_id: tick.exchange_id,
                priority: 0,
                padding: [0; 12],
            };
            let out = orders.loan_uninit()?;
            let out = out.write_payload(cmd);
            out.send()?;
            orders_sent += 1;

            // tick-to-order window: origin tick (T0) -> order emitted (T1).
            win_tick_to_order.push(fast_cycles_to_ns(t1.saturating_sub(t0)));

            println!(
                "order #{:>5}  {:<4}  px={:>10.2}  score={:+.3}  pos={:>+3}  (tick {})",
                order_id,
                match side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                },
                price,
                score,
                position,
                ticks_seen
            );
        }
    }

    println!(
        "strategy stopped: {} ticks in, {} orders out ({:.2}% conversion), {} suppressed by position limit",
        ticks_seen,
        orders_sent,
        if ticks_seen > 0 {
            orders_sent as f64 / ticks_seen as f64 * 100.0
        } else {
            0.0
        },
        suppressed
    );
    Ok(())
}
