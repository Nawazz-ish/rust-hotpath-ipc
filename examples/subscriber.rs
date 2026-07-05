//! Hot-path latency benchmark subscriber.
//!
//! Opens the shared iceoryx2 order-command service, receives `OrderCommand`
//! samples zero-copy, and measures end-to-end wire latency by comparing the
//! publisher-side rdtsc timestamp against a fresh local read.
//!
//! Two design points the numbers depend on:
//!   * cycles are converted to nanoseconds with the crate's runtime TSC
//!     calibration, not a hardcoded clock assumption;
//!   * sorting and printing happen on a separate reporter thread, so the
//!     receive loop never sorts or does I/O — the measurement stays off the
//!     hot path.
//!
//! Run alongside the publisher example:
//!   CPU_CORE=3 cargo run --release --example subscriber

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;

use iceoryx2::prelude::*;

use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::tsc_calibration::fast_cycles_to_ns;

/// Samples consumed before measurement begins — warms caches, branch
/// predictors, and the iceoryx2 receive path so first-touch outliers don't
/// pollute the distribution.
const WARMUP_RECEIVES: u64 = 10_000;

/// Measured samples per reporting window.
const REPORT_INTERVAL: usize = 100_000;

/// One window of latencies plus its loss accounting, handed to the reporter.
struct Window {
    latencies: Vec<u64>,
    received: u64,
    lost: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let core_id: usize = env::var("CPU_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    if core_affinity::set_for_current(core_affinity::CoreId { id: core_id }) {
        tracing::info!("pinned subscriber to CPU core {}", core_id);
    } else {
        tracing::warn!("failed to pin to CPU core {}", core_id);
    }

    // Real-time scheduling on Linux so the kernel does not preempt mid-loop.
    #[cfg(target_os = "linux")]
    unsafe {
        let param = libc::sched_param { sched_priority: 98 };
        if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) == 0 {
            tracing::info!("engaged SCHED_FIFO priority 98");
        } else {
            tracing::warn!("failed to set SCHED_FIFO (need CAP_SYS_NICE?)");
        }
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))?;
    }

    // Reporter thread: owns all sorting and printing so the receive loop never
    // does either. The hot loop hands it a full window over a channel and
    // immediately gets back to receiving.
    let (tx, rx): (Sender<Window>, Receiver<Window>) = mpsc::channel();
    let reporter = thread::spawn(move || {
        while let Ok(mut window) = rx.recv() {
            report(&mut window);
        }
    });

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

    tracing::info!("warming up ({} receives)...", WARMUP_RECEIVES);
    let mut warmed = 0u64;
    while warmed < WARMUP_RECEIVES && running.load(Ordering::Relaxed) {
        if subscriber.receive()?.is_some() {
            warmed += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    tracing::info!("warmup complete, entering hot loop");

    // Pre-allocated so the measured path never touches the allocator.
    let mut latencies: Vec<u64> = Vec::with_capacity(REPORT_INTERVAL);
    let mut expected_id = 0u64;
    let mut have_prev = false;
    let mut lost_in_window = 0u64;
    let mut received_in_window = 0u64;

    while running.load(Ordering::Relaxed) {
        // Zero-copy receive: the sample points straight into shared memory.
        let sample = match subscriber.receive()? {
            Some(sample) => sample,
            None => {
                std::hint::spin_loop();
                continue;
            }
        };
        let msg: OrderCommand = *sample;

        // End-to-end latency in cycles, converted to ns via the calibrated
        // TSC frequency (lock-free fast path). Guard against skew/wraparound.
        let now = rdtsc();
        let latency_ns = fast_cycles_to_ns(now.saturating_sub(msg.timestamp_ns));
        latencies.push(latency_ns);
        received_in_window += 1;

        // Loss detection: order_id is strictly monotonic on the publisher, so a
        // forward gap counts samples that overflowed the queue before we drained.
        if have_prev {
            if msg.order_id > expected_id {
                lost_in_window += msg.order_id - expected_id;
            }
        } else {
            have_prev = true;
        }
        expected_id = msg.order_id.wrapping_add(1);

        // Window full: hand it to the reporter and swap in a fresh buffer. The
        // only work on the hot thread is the channel send (a pointer move).
        if latencies.len() >= REPORT_INTERVAL {
            let full = std::mem::replace(&mut latencies, Vec::with_capacity(REPORT_INTERVAL));
            let _ = tx.send(Window {
                latencies: full,
                received: received_in_window,
                lost: lost_in_window,
            });
            lost_in_window = 0;
            received_in_window = 0;
        }
    }

    drop(tx);
    let _ = reporter.join();
    tracing::info!("shutting down");
    Ok(())
}

/// Sort a window, compute the standard percentiles, and print a one-line
/// summary with the message-loss rate. Runs on the reporter thread only.
fn report(window: &mut Window) {
    let latencies = &mut window.latencies;
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

    let total = window.received + window.lost;
    let loss_pct = if total > 0 {
        (window.lost as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    println!(
        "n={:>8}  Min={:>7} ns  P50={:>7} ns  P99={:>7} ns  P99.9={:>7} ns  Max={:>7} ns  loss={:.4}% ({} lost)",
        n, min, p50, p99, p999, max, loss_pct, window.lost
    );
}

/// Nearest-rank index into a sorted slice of length `n` for a percentile in [0,100].
#[inline]
fn percentile_index(n: usize, pct: f64) -> usize {
    if n == 0 {
        return 0;
    }
    let rank = (pct / 100.0 * n as f64).ceil() as usize;
    rank.saturating_sub(1).min(n - 1)
}
