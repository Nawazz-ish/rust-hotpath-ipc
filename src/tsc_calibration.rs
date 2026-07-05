// TSC (Time Stamp Counter) calibration.
// Accurate conversion between CPU cycles and nanoseconds, plus correlation to
// the Unix epoch so timestamps taken in different processes can be compared.

use crate::hot_path::rdtsc;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Calibration data mapping cycles <-> nanoseconds and TSC <-> Unix time.
#[derive(Debug, Clone)]
pub struct TscCalibration {
    pub cycles_per_ns: f64,
    pub ns_per_cycle: f64,
    pub epoch_tsc: u64,
    pub epoch_unix_ns: u64,
    pub last_calibration: Instant,
    pub cpu_frequency_hz: u64,
}

impl TscCalibration {
    pub fn calibrate() -> Self {
        Self::calibrate_with_duration(Duration::from_millis(100))
    }

    /// Longer measurement windows give a more accurate frequency estimate.
    pub fn calibrate_with_duration(measurement_duration: Duration) -> Self {
        let start_instant = Instant::now();
        let start_tsc = rdtsc();

        std::thread::sleep(measurement_duration);

        let end_instant = Instant::now();
        let end_tsc = rdtsc();

        let elapsed_ns = (end_instant - start_instant).as_nanos() as f64;
        let elapsed_cycles = end_tsc.saturating_sub(start_tsc) as f64;

        let cycles_per_ns = elapsed_cycles / elapsed_ns;
        let ns_per_cycle = elapsed_ns / elapsed_cycles;

        let epoch_instant = SystemTime::now();
        let epoch_tsc = rdtsc();
        let epoch_unix_ns = epoch_instant
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let cpu_frequency_hz = (cycles_per_ns * 1_000_000_000.0) as u64;

        Self {
            cycles_per_ns,
            ns_per_cycle,
            epoch_tsc,
            epoch_unix_ns,
            last_calibration: Instant::now(),
            cpu_frequency_hz,
        }
    }

    #[inline(always)]
    pub fn cycles_to_ns(&self, cycles: u64) -> u64 {
        (cycles as f64 * self.ns_per_cycle) as u64
    }

    #[inline(always)]
    pub fn ns_to_cycles(&self, ns: u64) -> u64 {
        (ns as f64 * self.cycles_per_ns) as u64
    }

    #[inline(always)]
    pub fn tsc_to_unix_ns(&self, tsc: u64) -> u64 {
        let elapsed_cycles = tsc.wrapping_sub(self.epoch_tsc);
        let elapsed_ns = self.cycles_to_ns(elapsed_cycles);
        self.epoch_unix_ns.wrapping_add(elapsed_ns)
    }

    #[inline(always)]
    pub fn unix_ns_to_tsc(&self, unix_ns: u64) -> u64 {
        let elapsed_ns = unix_ns.wrapping_sub(self.epoch_unix_ns);
        let elapsed_cycles = self.ns_to_cycles(elapsed_ns);
        self.epoch_tsc.wrapping_add(elapsed_cycles)
    }

    pub fn needs_refresh(&self) -> bool {
        self.last_calibration.elapsed() > Duration::from_secs(60)
    }
}

/// Process-wide calibration, lazily initialized on first use.
pub static TSC_CALIBRATION: Lazy<RwLock<TscCalibration>> =
    Lazy::new(|| RwLock::new(TscCalibration::calibrate()));

/// Cached `cycles_per_ns` bits for a lock-free fast path.
static CYCLES_PER_NS_CACHED: Lazy<AtomicU64> = Lazy::new(|| {
    let cal = TSC_CALIBRATION.read();
    AtomicU64::new(cal.cycles_per_ns.to_bits())
});

/// Lock-free cycles->ns conversion using the cached frequency.
#[inline(always)]
pub fn fast_cycles_to_ns(cycles: u64) -> u64 {
    let bits = CYCLES_PER_NS_CACHED.load(Ordering::Relaxed);
    let cycles_per_ns = f64::from_bits(bits);
    let ns_per_cycle = 1.0 / cycles_per_ns;
    (cycles as f64 * ns_per_cycle) as u64
}

pub fn fast_tsc_to_unix_ns(tsc: u64) -> u64 {
    let cal = TSC_CALIBRATION.read();
    cal.tsc_to_unix_ns(tsc)
}

/// Recompute calibration; call periodically to correct for frequency drift.
pub fn refresh_calibration() {
    let new_cal = TscCalibration::calibrate();
    CYCLES_PER_NS_CACHED.store(new_cal.cycles_per_ns.to_bits(), Ordering::Relaxed);
    *TSC_CALIBRATION.write() = new_cal;
}

/// Background thread that refreshes calibration every 60s.
pub fn start_auto_refresh() {
    std::thread::spawn(|| loop {
        std::thread::sleep(Duration::from_secs(60));
        refresh_calibration();
        tracing::debug!("TSC calibration refreshed");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calibration_produces_positive_values() {
        let cal = TscCalibration::calibrate();
        assert!(cal.cpu_frequency_hz > 0);
        assert!(cal.cycles_per_ns > 0.0);
        assert!(cal.ns_per_cycle > 0.0);
    }

    #[test]
    fn round_trip_conversion_is_close() {
        let cal = TscCalibration::calibrate();
        let original_ns = 1_000_000_000u64;
        let cycles = cal.ns_to_cycles(original_ns);
        let back = cal.cycles_to_ns(cycles);
        let diff = back.abs_diff(original_ns);
        assert!(diff < original_ns / 100);
    }
}
