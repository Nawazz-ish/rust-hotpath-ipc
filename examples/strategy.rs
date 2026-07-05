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
//! Run with:  CPU_CORE=2 cargo run --release --example strategy

use core_affinity::CoreId;
use iceoryx2::prelude::*;
use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
};

use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::strategy::{Decision, Side, Strategy, StrategyConfig};

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

    // Threshold is env-tunable so the same binary works across feeds of
    // different volatility without a rebuild.
    let mut cfg = StrategyConfig::default();
    if let Some(t) = env::var("THRESHOLD").ok().and_then(|v| v.parse().ok()) {
        cfg.threshold = t;
    }
    let mut strat = Strategy::new(cfg);

    println!("strategy: consuming MarketTick, emitting OrderCommand");
    println!("  in:  {}", MARKET_SERVICE);
    println!("  out: {}", ORDER_SERVICE);
    println!("  threshold: {:+.3}", cfg.threshold);

    let mut ticks_seen = 0u64;
    let mut orders_sent = 0u64;
    let mut order_id = 0u64;

    while running.load(Ordering::Relaxed) {
        let sample = match ticks.receive()? {
            Some(s) => s,
            None => {
                std::hint::spin_loop();
                continue;
            }
        };
        let tick: MarketTick = *sample;
        ticks_seen += 1;

        // Fixed-point (1e8) wire price -> f64 for the strategy math.
        let price = tick.price as f64 / 100_000_000.0;

        // Periodic heartbeat so the demo shows the strategy is alive and what
        // the signals look like even during quiet stretches.
        if ticks_seen % 50_000 == 0 {
            let (trend, mom, rev) = strat.signals();
            println!(
                "  .. {} ticks, px={:.2}  signals[trend={:+.2} mom={:+.2} rev={:+.2}]  orders={}",
                ticks_seen, price, trend, mom, rev, orders_sent
            );
        }

        if let Decision::Trade { side, score } = strat.on_price(price) {
            order_id += 1;
            let cmd = OrderCommand {
                timestamp_ns: rdtsc(),
                order_id,
                price_ticks: tick.price, // marketable: submit at last price
                quantity: 100_000_000,   // 1.0 unit
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
                padding: [0; 20],
            };
            let out = orders.loan_uninit()?;
            let out = out.write_payload(cmd);
            out.send()?;
            orders_sent += 1;

            println!(
                "order #{:>5}  {:<4}  px={:>10.2}  score={:+.3}  (tick {})",
                order_id,
                match side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                },
                price,
                score,
                ticks_seen
            );
        }
    }

    println!(
        "strategy stopped: {} ticks in, {} orders out ({:.2}% conversion)",
        ticks_seen,
        orders_sent,
        if ticks_seen > 0 {
            orders_sent as f64 / ticks_seen as f64 * 100.0
        } else {
            0.0
        }
    );
    Ok(())
}
