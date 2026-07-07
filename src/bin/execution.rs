//! Execution / P&L stage: the last stage of the trading pipeline.
//!
//! In the old loopback design this stage *filled* orders itself. Now the
//! matching lives in the exchange, so this stage inverts: it consumes the
//! exchange's `ExecutionReport`s (fills, including partials) and keeps the
//! book of record — running position and mark-to-market P&L — so the demo still
//! shows the strategy making or losing money.
//!
//! Two things the report alone does not carry, so we reconstruct them by
//! correlating each report back to its order on `order_id`:
//!
//! - the order's **side** (buy/sell) — needed to sign the position change;
//! - the order's **origin timestamp T0** — needed for the tick-to-fill window.
//!
//! We keep a small `order_id -> (origin_ts, side)` map fed from the order stream.
//! (Widening `ExecutionReport` to carry these would break its 64-byte cache line,
//! which the whole transport rests on, so we correlate instead.)
//!
//! Run with:  CPU_CORE=3 cargo run --release --bin execution

use iceoryx2::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::latency_window::LatencyReporter;
use rust_hotpath_ipc::runtime::{env_or, pin_only};
use rust_hotpath_ipc::tsc_calibration::fast_cycles_to_ns;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Keep iceoryx2's own logging to errors only so the fill/P&L output is clean.
    set_log_level(LogLevel::Error);

    // Pinned but not real-time: it accounts fills off the tightest loop and stays
    // preemptible so it never blocks the reporter.
    let cpu_id: usize = env_or("CPU_CORE", 3);
    pin_only(cpu_id);
    println!("execution pinned to CPU core {cpu_id}");

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))?;
    }

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    // Primary input: fills from the exchange.
    let exec_svc = node
        .service_builder(&EXECUTION_SERVICE.try_into()?)
        .publish_subscribe::<ExecutionReport>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let reports = exec_svc.subscriber_builder().create()?;

    // Secondary input: the order stream, read only to learn each order's origin
    // timestamp and side so we can correlate fills back to them.
    let orders_svc = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let orders = orders_svc.subscriber_builder().create()?;

    // tick-to-fill latency window, aggregated off the hot path on a reporter
    // thread pinned to REPORTER_CORE. T0 comes from the correlated order's
    // origin_ts; T2 is the report's fill timestamp — so it measures the full
    // pipeline through a real matcher.
    let reporter_core: usize = env_or("REPORTER_CORE", 0);
    let reporter = LatencyReporter::new(reporter_core);
    let mut win_tick_to_fill = reporter.window("tick-to-fill");

    println!("execution: consuming ExecutionReport, tracking position + P&L");
    println!("  fills in: {}", EXECUTION_SERVICE);
    println!(
        "  orders in: {} (for origin_ts / side correlation)",
        ORDER_SERVICE
    );
    println!("  reporter_core: {}", reporter_core);

    // order_id -> (origin_ts T0, side) for every order we have seen. Pre-sized so
    // steady-state inserts rarely reallocate.
    let mut order_ctx: HashMap<u64, (u64, u8)> = HashMap::with_capacity(4096);

    // Position accounting in f64 (derived from fixed-point 1e8 wire prices).
    let mut position = 0.0_f64; // net units held (+long / -short)
    let mut cash = 0.0_f64; // realized cash flow: sells add, buys subtract
    let mut last_price = 0.0_f64;
    let mut fills = 0u64;

    while running.load(Ordering::Relaxed) {
        // Drain any pending orders first so a fill always finds its context.
        while let Some(sample) = orders.receive()? {
            let cmd: OrderCommand = *sample;
            order_ctx.insert(cmd.order_id, (cmd.origin_ts, cmd.side));
        }

        let sample = match reports.receive()? {
            Some(s) => s,
            None => {
                std::hint::spin_loop();
                continue;
            }
        };
        let report: ExecutionReport = *sample;

        // status: 0=acked, 1=partial, 2=filled, 3=cancelled/reject. Only partials
        // and terminal fills move the book.
        if report.status != 1 && report.status != 2 {
            continue;
        }

        let (origin_ts, side) = match order_ctx.get(&report.order_id) {
            Some(&ctx) => ctx,
            None => {
                // A fill for an order we never saw the command for (e.g. we
                // attached late). Skip its P&L rather than guess the side.
                continue;
            }
        };

        let price = report.executed_price as f64 / 100_000_000.0;
        let qty = report.executed_quantity as f64 / 100_000_000.0;
        let fee = report.commission as f64 / 100_000_000.0;
        last_price = price;

        // side: 0 = buy, 1 = sell
        if side == 0 {
            position += qty;
            cash -= price * qty + fee;
        } else {
            position -= qty;
            cash += price * qty - fee;
        }
        fills += 1;

        // tick-to-fill only on a *terminal* fill (status=2), so a multi-partial
        // order contributes one sample, not one per partial.
        if report.status == 2 {
            let t2 = report.timestamp_ns;
            if origin_ts != 0 && t2 > origin_ts {
                win_tick_to_fill.push(fast_cycles_to_ns(t2.saturating_sub(origin_ts)));
            }
            // Order is done; drop its context to keep the map bounded.
            order_ctx.remove(&report.order_id);
        }

        // Mark-to-market equity: realized cash + value of the open position.
        let equity = cash + position * price;
        let side_s = if side == 0 { "BUY " } else { "SELL" };
        let partial = if report.status == 1 { " (partial)" } else { "" };
        println!(
            "fill #{:>5}  {}  px={:>10.2}  qty={:>6.2}  pos={:>+7.2}  equity={:>+11.2}{}",
            report.order_id, side_s, price, qty, position, equity, partial
        );
    }

    let final_equity = cash + position * last_price;
    println!("---------------------------------------------------------------");
    println!(
        "execution stopped: {} fills | final position {:+.2} | mark-to-market P&L {:+.2}",
        fills, position, final_equity
    );
    Ok(())
}
