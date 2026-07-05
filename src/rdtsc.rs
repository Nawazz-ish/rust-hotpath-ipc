// RDTSC-based ultra-low-latency measurement.
// CPU cycle-accurate timestamps with runtime-calibrated nanosecond conversion.

use crate::hot_path::rdtsc as rdtsc_raw;
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// CPU frequency in GHz, calibrated once at first use.
static CPU_FREQ_GHZ: Lazy<f64> = Lazy::new(calibrate_cpu_frequency);

/// Process-wide latency recorder.
pub static LATENCY_RECORDER: Lazy<Arc<RdtscLatencyRecorder>> =
    Lazy::new(|| Arc::new(RdtscLatencyRecorder::new(1_000_000)));

/// Component identifiers for per-stage latency attribution.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentId {
    OrderValidation = 1,
    MarketDataLookup = 2,
    RiskCheck = 3,
    IceoryxEvent = 4,
    ExchangeSubmission = 5,
    PositionUpdate = 6,
    AlgorithmSlice = 7,
    HotPath = 8,
    Total = 100,
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Entry = 0,
    Exit = 1,
}

/// Cache-line-aligned latency record.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct LatencyRecord {
    pub timestamp_cycles: u64,
    pub timestamp_ns: u64,
    pub component_id: ComponentId,
    pub operation: OperationType,
    pub order_id: [u8; 16],
    pub latency_cycles: u64,
    pub thread_id: u32,
    pub cpu_core: u32,
}

impl Default for LatencyRecord {
    fn default() -> Self {
        Self {
            timestamp_cycles: 0,
            timestamp_ns: 0,
            component_id: ComponentId::Total,
            operation: OperationType::Entry,
            order_id: [0; 16],
            latency_cycles: 0,
            thread_id: 0,
            cpu_core: 0,
        }
    }
}

/// Thread-safe latency recorder backed by a bounded ring buffer. Writers never
/// block: if the buffer is full or the lock is contended, the record is dropped
/// and counted rather than stalling the hot path.
pub struct RdtscLatencyRecorder {
    ring_buffer: Arc<Mutex<VecDeque<LatencyRecord>>>,
    capacity: usize,
    records_written: AtomicU64,
    records_dropped: AtomicU64,
    tsc_offset: AtomicU64,
}

impl RdtscLatencyRecorder {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            records_written: AtomicU64::new(0),
            records_dropped: AtomicU64::new(0),
            tsc_offset: AtomicU64::new(0),
        }
    }

    /// Current CPU timestamp (delegates to the crate's single timestamp source).
    #[inline(always)]
    pub fn timestamp() -> u64 {
        rdtsc_raw()
    }

    /// Record entry into a component; returns the entry timestamp for the
    /// matching exit call.
    #[inline(always)]
    pub fn record_entry(&self, order_id: &[u8; 16], component: ComponentId) -> u64 {
        let cycles = Self::timestamp();
        let record = LatencyRecord {
            timestamp_cycles: cycles,
            timestamp_ns: self.cycles_to_ns(cycles),
            component_id: component,
            operation: OperationType::Entry,
            order_id: *order_id,
            latency_cycles: 0,
            thread_id: 0,
            cpu_core: get_current_cpu(),
        };
        self.try_push(record);
        cycles
    }

    /// Record exit from a component and compute its latency in cycles.
    #[inline(always)]
    pub fn record_exit(&self, order_id: &[u8; 16], component: ComponentId, entry_cycles: u64) {
        let exit_cycles = Self::timestamp();
        let latency = exit_cycles.saturating_sub(entry_cycles);
        let record = LatencyRecord {
            timestamp_cycles: exit_cycles,
            timestamp_ns: self.cycles_to_ns(exit_cycles),
            component_id: component,
            operation: OperationType::Exit,
            order_id: *order_id,
            latency_cycles: latency,
            thread_id: 0,
            cpu_core: get_current_cpu(),
        };
        self.try_push(record);
    }

    #[inline(always)]
    fn try_push(&self, record: LatencyRecord) {
        if let Ok(mut buffer) = self.ring_buffer.try_lock() {
            if buffer.len() >= self.capacity {
                buffer.pop_front();
                self.records_dropped.fetch_add(1, Ordering::Relaxed);
            }
            buffer.push_back(record);
            self.records_written.fetch_add(1, Ordering::Relaxed);
        } else {
            self.records_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub fn cycles_to_ns(&self, cycles: u64) -> u64 {
        let offset = self.tsc_offset.load(Ordering::Relaxed);
        let adjusted = cycles.saturating_sub(offset);
        ((adjusted as f64) / *CPU_FREQ_GHZ) as u64
    }

    /// Drain up to `max_count` records for offline processing (metrics thread).
    pub fn drain_records(&self, max_count: usize) -> Vec<LatencyRecord> {
        let mut records = Vec::with_capacity(max_count);
        if let Ok(mut buffer) = self.ring_buffer.try_lock() {
            for _ in 0..max_count {
                match buffer.pop_front() {
                    Some(record) => records.push(record),
                    None => break,
                }
            }
        }
        records
    }

    /// `(written, dropped)` counters.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.records_written.load(Ordering::Relaxed),
            self.records_dropped.load(Ordering::Relaxed),
        )
    }
}

/// Calibrate CPU frequency by measuring TSC advance over a known wall-clock window.
fn calibrate_cpu_frequency() -> f64 {
    use std::time::{Duration, Instant};

    for _ in 0..100 {
        let _ = RdtscLatencyRecorder::timestamp();
    }

    let duration = Duration::from_millis(100);
    let start_time = Instant::now();
    let start_cycles = RdtscLatencyRecorder::timestamp();
    std::thread::sleep(duration);
    let end_cycles = RdtscLatencyRecorder::timestamp();
    let elapsed = start_time.elapsed();

    let cycles_elapsed = end_cycles.saturating_sub(start_cycles);
    let nanos_elapsed = elapsed.as_nanos() as f64;
    let freq_ghz = (cycles_elapsed as f64) / nanos_elapsed;

    tracing::debug!(freq_ghz, "CPU frequency calibrated");
    freq_ghz
}

/// Current CPU core number (Linux `getcpu`; 0 elsewhere).
#[inline(always)]
fn get_current_cpu() -> u32 {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut cpu: u32 = 0;
            libc::syscall(
                libc::SYS_getcpu,
                &mut cpu as *mut u32,
                std::ptr::null_mut::<u32>(),
                std::ptr::null_mut::<u32>(),
            );
            cpu
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// RAII guard: records entry on construction, exit on drop.
pub struct LatencyGuard {
    order_id: [u8; 16],
    component: ComponentId,
    entry_cycles: u64,
}

impl LatencyGuard {
    pub fn new(order_id: [u8; 16], component: ComponentId) -> Self {
        let entry_cycles = LATENCY_RECORDER.record_entry(&order_id, component);
        Self {
            order_id,
            component,
            entry_cycles,
        }
    }
}

impl Drop for LatencyGuard {
    fn drop(&mut self) {
        LATENCY_RECORDER.record_exit(&self.order_id, self.component, self.entry_cycles);
    }
}

/// Scoped latency measurement around a block.
#[macro_export]
macro_rules! measure_latency {
    ($order_id:expr, $component:expr, $code:block) => {{
        let _guard = $crate::rdtsc::LatencyGuard::new($order_id, $component);
        $code
    }};
}

/// Free function for inline timestamps.
#[inline(always)]
pub fn rdtsc() -> u64 {
    RdtscLatencyRecorder::timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_monotonic() {
        let t1 = RdtscLatencyRecorder::timestamp();
        std::thread::sleep(std::time::Duration::from_micros(1));
        let t2 = RdtscLatencyRecorder::timestamp();
        assert!(t2 > t1);
    }

    #[test]
    fn records_entry_and_exit() {
        let recorder = RdtscLatencyRecorder::new(100);
        let order_id = [1u8; 16];
        let entry = recorder.record_entry(&order_id, ComponentId::OrderValidation);
        std::thread::sleep(std::time::Duration::from_micros(10));
        recorder.record_exit(&order_id, ComponentId::OrderValidation, entry);
        let records = recorder.drain_records(10);
        assert_eq!(records.len(), 2);
        assert!(records[1].latency_cycles > 0);
    }
}
