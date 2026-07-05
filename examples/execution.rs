//! Execution engine: stage 3 of the trading pipeline.
//!
//! Subscribes to `OrderCommand`s from the strategy, simulates an immediate fill
//! at the order price (a real engine would route to a venue here), publishes an
//! `ExecutionReport`, and maintains a running position and mark-to-market P&L so
//! the demo shows the strategy actually making or losing money on the synthetic
//! series.
//!
//! Run with:  CPU_CORE=3 cargo run --release --example execution

use core_affinity::CoreId;
use iceoryx2::prelude::*;
use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
};

use rust_hotpath_ipc::hot_path::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpu_id: usize = env::var("CPU_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    if core_affinity::set_for_current(CoreId { id: cpu_id }) {
        println!("execution pinned to CPU core {}", cpu_id);
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))?;
    }

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    // Input: orders from the strategy.
    let orders_svc = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let orders = orders_svc.subscriber_builder().create()?;

    // Output: execution reports (a cold-path recorder would consume these).
    let exec_svc = node
        .service_builder(&EXECUTION_SERVICE.try_into()?)
        .publish_subscribe::<ExecutionReport>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let reports = exec_svc.publisher_builder().create()?;

    println!("execution: filling OrderCommand, publishing ExecutionReport");
    println!("  in:  {}", ORDER_SERVICE);
    println!("  out: {}", EXECUTION_SERVICE);

    // Position accounting in f64 (derived from fixed-point 1e8 wire prices).
    let mut position = 0.0_f64; // net units held (+long / -short)
    let mut cash = 0.0_f64; // realized cash flow: sells add, buys subtract
    let mut last_price = 0.0_f64;
    let mut fills = 0u64;
    // Simple linear fee/slippage model: cost in bps of notional per fill.
    let fee_bps = 1.0_f64;

    while running.load(Ordering::Relaxed) {
        let sample = match orders.receive()? {
            Some(s) => s,
            None => {
                std::hint::spin_loop();
                continue;
            }
        };
        let cmd: OrderCommand = *sample;

        let price = cmd.price_ticks as f64 / 100_000_000.0;
        let qty = cmd.quantity as f64 / 100_000_000.0;
        last_price = price;
        let fee = price * qty * (fee_bps / 10_000.0);

        // side: 0 = buy, 1 = sell
        if cmd.side == 0 {
            position += qty;
            cash -= price * qty + fee;
        } else {
            position -= qty;
            cash += price * qty - fee;
        }
        fills += 1;

        // Publish an execution report (immediate full fill).
        let report = ExecutionReport {
            timestamp_ns: rdtsc(),
            order_id: cmd.order_id,
            exchange_order_id: cmd.order_id,
            executed_price: cmd.price_ticks,
            executed_quantity: cmd.quantity,
            remaining_quantity: 0,
            commission: (fee * 100_000_000.0) as i64,
            status: 2, // filled
            reject_reason: 0,
            padding: [0; 6],
        };
        let out = reports.loan_uninit()?;
        let out = out.write_payload(report);
        out.send()?;

        // Mark-to-market equity: realized cash + value of the open position.
        let equity = cash + position * price;
        let side = if cmd.side == 0 { "BUY " } else { "SELL" };
        println!(
            "fill #{:>5}  {}  px={:>10.2}  pos={:>+7.2}  equity={:>+11.2}",
            cmd.order_id, side, price, position, equity
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
