//! Hot-path latency benchmark subscriber.
//!
//! Standalone binary that opens the shared iceoryx2 order-command service,
//! receives `OrderCommand` samples zero-copy, and measures end-to-end wire
//! latency by comparing the publisher-side rdtsc timestamp against a fresh
//! local read.
//!
//! Run alongside the publisher example:
//!   CPU_CORE=3 cargo run --release --example subscriber

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use iceoryx2::prelude::*;

use rust_hotpath_ipc::hot_path::*;

/// Number of samples consumed before measurements begin. Warming the caches,
/// branch predictors, and the iceoryx2 receive path keeps the reported
/// distribution free of first-touch outliers.
const WARMUP_RECEIVES: u64 = 10_000;

/// How many measured samples accumulate before we emit a distribution report
/// and reset the latency buffer.
const REPORT_INTERVAL: usize = 100_000;

/// Approximate rdtsc ticks per nanosecond on the benchmark host. The raw
/// timestamps are cycle counts, so we divide the delta to recover nanoseconds.
const TICKS_PER_NS: u64 = 3;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Pin to a dedicated core so the hot loop is never migrated and shares no
    // L1/L2 with the publisher. Defaults to core 3, overridable via CPU_CORE.
    let core_id: usize = env::var("CPU_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    if core_affinity::set_for_current(core_affinity::CoreId { id: core_id }) {
        tracing::info!("pinned subscriber to CPU core {}", core_id);
    } else {
        tracing::warn!("failed to pin to CPU core {}", core_id);
    }

    // Real-time scheduling on Linux so the kernel does not preempt us mid-loop.
    // Requires CAP_SYS_NICE (or root); best-effort, non-fatal otherwise.
    #[cfg(target_os = "linux")]
    unsafe {
        let param = libc::sched_param { sched_priority: 99 };
        if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) == 0 {
            tracing::info!("engaged SCHED_FIFO priority 99");
        } else {
            tracing::warn!("failed to set SCHED_FIFO (need CAP_SYS_NICE?)");
        }
    }

    // Ctrl-C handler: flip the flag and let the loop drain out cleanly.
    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || {
            running.store(false, Ordering::SeqCst);
        })?;
    }

    // Open the exact same publish-subscribe service the publisher creates.
    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let service = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(0)
        .open_or_create()?;

    let subscriber = service.subscriber_builder().create()?;

    tracing::info!(
        "subscriber listening on '{}' for OrderCommand ({} B)",
        ORDER_SERVICE,
        std::mem::size_of::<OrderCommand>()
    );

    // ---- Warmup: burn through the first samples without measuring. ----
    tracing::info!("warming up ({} receives)...", WARMUP_RECEIVES);
    let mut warmed: u64 = 0;
    while warmed < WARMUP_RECEIVES && running.load(Ordering::Relaxed) {
        if subscriber.receive()?.is_some() {
            warmed += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    tracing::info!("warmup complete, entering hot loop");

    // ---- Hot loop measurement state. ----
    // Pre-allocated so the measured path never hits the allocator.
    let mut latencies: Vec<u64> = Vec::with_capacity(REPORT_INTERVAL);

    // Message-loss tracking via monotonic order_id. `expected_id` is the id we
    // anticipate on the next sample; any positive gap is counted as lost.
    let mut expected_id: u64 = 0;
    let mut have_prev: bool = false;
    let mut lost_in_window: u64 = 0;
    let mut received_in_window: u64 = 0;

    while running.load(Ordering::Relaxed) {
        // Zero-copy receive: the sample points straight into shared memory.
        let sample = match subscriber.receive()? {
            Some(sample) => sample,
            None => {
                std::hint::spin_loop();
                continue;
            }
        };

        // Dereference is a zero-copy read of the POD payload.
        let msg: OrderCommand = *sample;

        // End-to-end latency: local timestamp minus the publisher's send-time
        // timestamp, converted from rdtsc ticks to nanoseconds. Guard against
        // clock skew / wraparound producing a nonsensical negative delta.
        let now = rdtsc();
        let latency_ns = now.saturating_sub(msg.timestamp_ns) / TICKS_PER_NS;
        latencies.push(latency_ns);
        received_in_window += 1;

        // Loss detection: order_id is strictly monotonic on the publisher, so
        // any forward gap between the expected id and the observed id is the
        // count of samples that overflowed the queue before we drained them.
        if have_prev {
            if msg.order_id > expected_id {
                lost_in_window += msg.order_id - expected_id;
            }
        } else {
            have_prev = true;
        }
        expected_id = msg.order_id.wrapping_add(1);

        // Every REPORT_INTERVAL measured samples: summarize and reset.
        if latencies.len() >= REPORT_INTERVAL {
            report(&mut latencies, received_in_window, lost_in_window);
            latencies.clear();
            lost_in_window = 0;
            received_in_window = 0;
        }
    }

    tracing::info!("shutting down");
    Ok(())
}

/// Sort the latency buffer, compute the standard percentiles, and print a
/// one-line summary together with the message-loss rate for the window.
fn report(latencies: &mut [u64], received: u64, lost: u64) {
    if latencies.is_empty() {
        return;
    }

    latencies.sort_unstable();

    let n = latencies.len();
    let min = latencies[0];
    let max = latencies[n - 1];
    let p50 = latencies[percentile_index(n, 50.0)];
    let p99 = latencies[percentile_index(n, 99.0)];
    let p999 = latencies[percentile_index(n, 99.9)];

    // Loss is measured against the total samples the sender emitted over the
    // window (the ones we saw plus the ones we detected as gaps).
    let total = received + lost;
    let loss_pct = if total > 0 {
        (lost as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    println!(
        "n={:>8}  Min={:>7} ns  P50={:>7} ns  P99={:>7} ns  P99.9={:>7} ns  Max={:>7} ns  loss={:.4}% ({} lost)",
        n, min, p50, p99, p999, max, loss_pct, lost
    );
}

/// Index into a sorted slice of length `n` for the given percentile in [0,100].
#[inline]
fn percentile_index(n: usize, pct: f64) -> usize {
    if n == 0 {
        return 0;
    }
    let rank = (pct / 100.0 * n as f64).ceil() as usize;
    rank.saturating_sub(1).min(n - 1)
}
