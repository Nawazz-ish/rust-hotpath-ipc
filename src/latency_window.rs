//! Off-hot-path latency-window recorder.
//!
//! A stage on the hot path (strategy, execution) records one latency sample per
//! event and must not pay for sorting or I/O to do it. This recorder gives each
//! stage a cheap [`LatencyWindow::push`] — an integer compare and a `Vec` push
//! into a pre-allocated buffer — and moves all the expensive work (sorting,
//! percentiles, printing) onto a **reporter thread pinned to a separate core**.
//!
//! This is the same shape the benchmark subscriber uses, generalized so the
//! pipeline stages can share it. The reporter runs at normal priority on its own
//! core so it is never starved by a `SCHED_FIFO` hot loop that busy-spins and
//! never yields — a failure mode this project hit once and fixed by pinning the
//! reporter off the hot core.

use crate::hot_path::{rdtsc, rdtsc_serialized};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Samples to consume before recording begins — lets caches, branch predictors,
/// and the IPC path warm so first-touch outliers don't pollute the distribution.
///
/// Kept small on purpose: order-triggered windows (tick-to-order, tick-to-fill)
/// see far fewer samples than per-tick windows, so a large warmup would keep them
/// permanently in warmup and they'd never report. A couple hundred is enough to
/// skip cold-start on the fast windows.
const WARMUP_SAMPLES: u64 = 200;

/// Emit a report once a window reaches this many samples...
const FLUSH_SAMPLES: usize = 2_000;
/// ...or this long since the last flush, whichever comes first. Keeps the live
/// UI updating even for slow windows (e.g. orders, which are rarer than ticks).
const FLUSH_INTERVAL: Duration = Duration::from_millis(1_500);

/// One flushed window handed to the reporter thread.
struct Batch {
    label: &'static str,
    samples: Vec<u64>,
}

/// Calibrate the irreducible cost of a serialized timestamp read.
///
/// We read [`rdtsc_serialized`] back to back and take the **minimum** delta over
/// many trials — the min is the floor with no interference (no context switch,
/// no contention). That floor is subtracted from the decision-only window, whose
/// interval (tens of ns) is small enough that the read overhead is a material
/// fraction. It is *not* subtracted from the larger windows, where it is noise
/// and correcting would add error.
pub fn calibrate_rdtsc_floor() -> u64 {
    let mut floor = u64::MAX;
    // A few warm-up reads first, then the measured trials.
    for _ in 0..1_000 {
        let _ = rdtsc_serialized();
    }
    for _ in 0..10_000 {
        let a = rdtsc_serialized();
        let b = rdtsc_serialized();
        let d = b.saturating_sub(a);
        if d < floor {
            floor = d;
        }
    }
    if floor == u64::MAX {
        0
    } else {
        floor
    }
}

/// A single named latency window. Cheap to push to; flushes off-core.
pub struct LatencyWindow {
    label: &'static str,
    buf: Vec<u64>,
    seen: u64,
    last_flush: Instant,
    tx: Sender<Batch>,
}

impl LatencyWindow {
    /// Record one sample (already in nanoseconds). Hot-path cost: a compare, a
    /// `Vec::push`, and — only when a batch fills — a pointer swap plus a channel
    /// send. No sorting, no allocation, no I/O here.
    #[inline(always)]
    pub fn push(&mut self, ns: u64) {
        self.seen += 1;
        if self.seen <= WARMUP_SAMPLES {
            return;
        }
        self.buf.push(ns);
        if self.buf.len() >= FLUSH_SAMPLES || self.last_flush.elapsed() >= FLUSH_INTERVAL {
            self.flush();
        }
    }

    #[inline]
    fn flush(&mut self) {
        if self.buf.is_empty() {
            self.last_flush = Instant::now();
            return;
        }
        // Swap in a fresh pre-allocated buffer; hand the full one to the reporter.
        let full = std::mem::replace(&mut self.buf, Vec::with_capacity(FLUSH_SAMPLES));
        let _ = self.tx.send(Batch {
            label: self.label,
            samples: full,
        });
        self.last_flush = Instant::now();
    }
}

/// Owns the reporter thread and hands out [`LatencyWindow`]s that feed it.
///
/// All windows share one reporter thread (and one channel), so a stage that
/// tracks several windows (e.g. the strategy tracks decision-only *and*
/// tick-to-order) still uses exactly one off-core thread.
///
/// The reporter is a daemon: it lives until every sender (this handle plus all
/// windows it handed out) is dropped, at which point the channel closes and the
/// thread exits on its own. We deliberately do NOT `join()` in `Drop` — a window
/// may outlive the reporter handle, so joining here could block; the thread is
/// cheap and the process reaps it on exit.
pub struct LatencyReporter {
    tx: Sender<Batch>,
    // Kept so a caller *may* join explicitly via `shutdown()`; not joined on Drop.
    handle: Option<JoinHandle<()>>,
}

impl LatencyReporter {
    /// Spawn the reporter pinned to `reporter_core` at normal priority. It must
    /// be a core no hot loop owns, or a `SCHED_FIFO` busy-spin will starve it.
    pub fn new(reporter_core: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Batch>();
        let handle = thread::spawn(move || {
            let _ = core_affinity::set_for_current(core_affinity::CoreId { id: reporter_core });
            while let Ok(mut batch) = rx.recv() {
                report(batch.label, &mut batch.samples);
            }
        });
        Self {
            tx,
            handle: Some(handle),
        }
    }

    /// Create a window that feeds this reporter.
    pub fn window(&self, label: &'static str) -> LatencyWindow {
        LatencyWindow {
            label,
            buf: Vec::with_capacity(FLUSH_SAMPLES),
            seen: 0,
            last_flush: Instant::now(),
            tx: self.tx.clone(),
        }
    }

    /// Consume the reporter, close its channel, and wait for the thread to
    /// drain and exit. Call this only after all windows are dropped, or it will
    /// block until they are.
    pub fn shutdown(mut self) {
        // Replace the sender with a fresh dead one so the original drops now.
        let (dead_tx, _) = mpsc::channel();
        self.tx = dead_tx;
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Sort a window and print its percentile line. Runs on the reporter thread.
///
/// The `LAT` prefix is a stable, greppable, machine-parseable marker the control
/// server relays verbatim and the web UI parses.
fn report(label: &str, samples: &mut [u64]) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let n = samples.len();
    let min = samples[0];
    let p50 = samples[percentile_index(n, 50.0)];
    let p99 = samples[percentile_index(n, 99.0)];
    let p999 = samples[percentile_index(n, 99.9)];
    println!(
        "LAT {:<13} n={:<7} min={:<5} p50={:<5} p99={:<6} p999={:<6} ns",
        label, n, min, p50, p99, p999
    );
}

/// Nearest-rank index into a sorted slice of length `n` for a percentile in
/// `[0, 100]`. Single definition, shared by every latency consumer.
#[inline]
pub fn percentile_index(n: usize, pct: f64) -> usize {
    if n == 0 {
        return 0;
    }
    let rank = (pct / 100.0 * n as f64).ceil() as usize;
    rank.saturating_sub(1).min(n - 1)
}

/// Convenience: time a closure with plain `rdtsc` (for larger windows).
#[inline(always)]
pub fn time_ns<F, R>(f: F) -> (R, u64)
where
    F: FnOnce() -> R,
{
    let a = rdtsc();
    let r = f();
    let b = rdtsc();
    (r, b.saturating_sub(a))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_index_bounds() {
        assert_eq!(percentile_index(0, 50.0), 0);
        assert_eq!(percentile_index(100, 50.0), 49);
        assert_eq!(percentile_index(100, 99.0), 98);
        assert_eq!(percentile_index(1, 99.9), 0);
    }

    #[test]
    fn rdtsc_floor_is_small_and_positive() {
        let floor = calibrate_rdtsc_floor();
        // Should be a handful of ns worth of cycles — not zero, not huge.
        // (We only assert it doesn't explode; exact value is hardware-specific.)
        assert!(floor < 100_000, "rdtsc floor implausibly large: {floor}");
    }

    #[test]
    fn window_records_after_warmup() {
        let reporter = LatencyReporter::new(0);
        let mut w = reporter.window("test");
        // Below warmup: nothing buffered.
        for _ in 0..WARMUP_SAMPLES {
            w.push(100);
        }
        assert_eq!(w.buf.len(), 0);
        // After warmup: samples accumulate.
        w.push(100);
        assert_eq!(w.buf.len(), 1);
    }
}
