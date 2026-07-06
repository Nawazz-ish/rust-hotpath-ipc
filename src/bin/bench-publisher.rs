//! Hot-path order publisher benchmark.
//!
//! Pins to a CPU core, sets real-time priority (Linux), and publishes
//! `OrderCommand` messages over Iceoryx2 on the order service, measuring
//! per-publish latency in TSC cycles.
//!
//! Run with:  CPU_CORE=2 cargo run --release --bin bench-publisher
//! (reliable core pinning + real-time priority may require sudo on Linux)

use iceoryx2::prelude::*;
use std::{thread, time::Duration};

use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::runtime::{env_or, pin_and_prioritize};
use rust_hotpath_ipc::tsc_calibration::fast_cycles_to_ns;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("hot-path order publisher");
    println!("========================");

    pin_and_prioritize(env_or("CPU_CORE", 2), "publisher");

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let service = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(0)
        .open_or_create()?;
    let publisher = service.publisher_builder().create()?;

    println!("service: {}", ORDER_SERVICE);
    println!("waiting 2s for a subscriber to connect...");
    thread::sleep(Duration::from_secs(2));
    println!("publishing at maximum speed (Ctrl-C to stop)\n");

    let mut seq = 0u64;
    let mut total_publish_cycles = 0u64;
    let mut publish_count = 0u64;

    loop {
        seq += 1;

        // Scalar writes into a 64-byte cache-line-aligned POD — no allocation.
        let msg = OrderCommand {
            timestamp_ns: rdtsc(),
            order_id: seq,
            price_ticks: 5_000_000 + (seq % 1000) as i64,
            quantity: 100 + (seq % 50),
            origin_ts: 0, // transport benchmark: no origin tick to carry
            symbol_id: symbols::BTC_USDT,
            user_id: 1,
            side: (seq % 2) as u8,
            order_type: 1, // limit
            action: 0,     // new
            flags: 0,
            exchange_id: exchanges::BINANCE,
            priority: 0,
            padding: [0; 12],
        };

        // Hot path: loan a slot, write the payload, send. Time it in cycles.
        let publish_start = rdtsc();
        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(msg);
        sample.send()?;
        let publish_end = rdtsc();

        total_publish_cycles += publish_end.wrapping_sub(publish_start);
        publish_count += 1;

        if seq.is_multiple_of(100_000) {
            let avg_cycles = total_publish_cycles / publish_count;
            let avg_ns = fast_cycles_to_ns(avg_cycles);
            println!(
                "published {} messages: avg publish {} cycles ({} ns, calibrated)",
                seq, avg_cycles, avg_ns
            );
            total_publish_cycles = 0;
            publish_count = 0;
        }

        std::hint::spin_loop();
    }
}
