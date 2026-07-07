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

use iceoryx2::prelude::*;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rust_hotpath_ipc::bytecode::{self, Vm};
use rust_hotpath_ipc::compiler;
use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::latency_window::{calibrate_rdtsc_floor, LatencyReporter};
use rust_hotpath_ipc::runtime::{env_or, pin_and_prioritize};
use rust_hotpath_ipc::strategy::{Decision, Side, Strategy, StrategyConfig};
use rust_hotpath_ipc::tsc_calibration::fast_cycles_to_ns;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Keep iceoryx2's own logging to errors only (its info/warn chatter would
    // otherwise clutter the pipeline output).
    set_log_level(LogLevel::Error);

    // Busy-spin consumer on the hot path: pin it and raise it to real-time
    // priority so the kernel does not preempt it mid-decision.
    pin_and_prioritize(env_or("CPU_CORE", 2), "strategy");

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

    // Order style. Default sends marketable orders (cross the spread, fill now) so
    // the latency windows match earlier runs. PASSIVE=1 posts limit orders at the
    // near touch (bid to buy / ask to sell), which rest in the book and only fill
    // once the queue ahead of them clears — so the exchange's queue-position and
    // maker-fill behaviour is exercised end to end.
    let passive = env::var("PASSIVE").map(|v| v == "1").unwrap_or(false);

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

    // Event notifier paired with the order service. When the exchange runs in
    // waitset mode it blocks on the matching listener instead of busy-polling, so
    // we signal here after each send. In poll mode the exchange ignores this and
    // the notify is a cheap no-op-ish call.
    let order_event = node
        .service_builder(&ORDER_EVENT.try_into()?)
        .event()
        .open_or_create()?;
    let order_notifier = order_event.notifier_builder().create()?;

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
    let max_position: i64 = env_or("MAX_POSITION", 3);

    // Order size in whole units. The default 1.0 fills whole against a single
    // maker; sizing up past a typical resting level makes the order sweep several
    // price levels, so the exchange returns partial fills — worth setting when you
    // want to see multi-level execution rather than clean single fills.
    let order_units: f64 = env_or("ORDER_UNITS", 1.0);
    let order_qty: u64 = (order_units * 100_000_000.0) as u64;
    // Position moves by the order's size, not by one. Round up so a fractional
    // size still counts toward the limit (conservative — never under-counts
    // exposure). This is what keeps the strategy's position book in the same
    // units the execution stage accounts, so the limit means what it says.
    let position_delta: i64 = (order_units.ceil() as i64).max(1);

    // Latency windows, aggregated off the hot path on a reporter thread pinned
    // to REPORTER_CORE (a core no stage's hot loop owns). This stage owns two:
    //   - decision-only: the Strategy::on_price() call in isolation (per tick)
    //   - tick-to-order: origin tick timestamp -> order emitted
    let reporter_core: usize = env_or("REPORTER_CORE", 0);
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
        "  wait_mode: {}  order_style: {}  order_units: {}  threshold: {:+.3}  max_position: +/-{}  reporter_core: {}  rdtsc_floor: {} cyc",
        if use_waitset { "waitset (blocking)" } else { "poll (busy-spin)" },
        if passive { "passive (rest at touch)" } else { "marketable" },
        order_units,
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
            // The order moves the book by its full size (position_delta units), not
            // by one, so the limit is enforced in the same units execution accounts.
            // A buy that would push past +max (or a sell past -max) is suppressed.
            let delta: i64 = match side {
                Side::Buy => position_delta,
                Side::Sell => -position_delta,
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
            // Marketable (default): submit at the last price, order_type=market.
            // Passive: rest at the near touch (bid to buy / ask to sell) as a limit.
            let (order_price, order_type) = if passive {
                let touch = match side {
                    Side::Buy => tick.bid,
                    Side::Sell => tick.ask,
                };
                // Fall back to last price if the touch isn't populated yet.
                (if touch != 0 { touch } else { tick.price }, 1u8)
            } else {
                (tick.price, 0u8)
            };
            let cmd = OrderCommand {
                timestamp_ns: t1,
                order_id,
                price_ticks: order_price,
                quantity: order_qty,
                origin_ts: t0, // carry T0 through for tick-to-fill downstream
                symbol_id: tick.symbol_id,
                user_id: 1,
                side: match side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
                order_type,
                action: 0, // new
                flags: 0,
                exchange_id: tick.exchange_id,
                priority: 0,
                padding: [0; 12],
            };
            let out = orders.loan_uninit()?;
            let out = out.write_payload(cmd);
            out.send()?;
            // Wake a waitset-mode exchange; ignored (and cheap) in poll mode.
            let _ = order_notifier.notify();
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
