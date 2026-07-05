//! Higher-level async latency monitor for IPC operations.
//!
//! This sits above the RDTSC recorder in [`crate::rdtsc`]: where that module is
//! the zero-overhead hot-path timer, this one aggregates named operations off
//! the hot path and computes percentile statistics. Recording is fire-and-forget
//! (spawned onto Tokio) so it never blocks the caller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Percentile statistics for one named operation.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub median: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub count: u64,
    pub total: Duration,
}

/// A single measurement.
#[derive(Debug, Clone)]
pub struct LatencyMeasurement {
    pub operation: String,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub metadata: HashMap<String, String>,
}

/// Collects measurements per operation and derives percentile statistics.
pub struct LatencyMonitor {
    measurements: Arc<RwLock<HashMap<String, Vec<LatencyMeasurement>>>>,
    stats: Arc<RwLock<HashMap<String, LatencyStats>>>,
    max_measurements_per_operation: usize,
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyMonitor {
    pub fn new() -> Self {
        Self {
            measurements: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(HashMap::new())),
            max_measurements_per_operation: 10_000,
        }
    }

    /// Record a latency for `operation`. Non-blocking: the aggregation runs on a
    /// spawned task so the caller returns immediately.
    pub fn record_operation_latency(&self, operation: &str, duration: Duration) {
        self.record_operation_latency_with_metadata(operation, duration, HashMap::new());
    }

    /// Record a latency with attached metadata.
    pub fn record_operation_latency_with_metadata(
        &self,
        operation: &str,
        duration: Duration,
        metadata: HashMap<String, String>,
    ) {
        let now = Instant::now();
        let measurement = LatencyMeasurement {
            operation: operation.to_string(),
            start_time: now.checked_sub(duration).unwrap_or(now),
            end_time: now,
            duration,
            metadata,
        };

        let measurements = self.measurements.clone();
        let stats = self.stats.clone();
        let max_measurements = self.max_measurements_per_operation;

        tokio::spawn(async move {
            {
                let mut measurements = measurements.write().await;
                let op = measurements
                    .entry(measurement.operation.clone())
                    .or_default();
                op.push(measurement.clone());
                if op.len() > max_measurements {
                    op.remove(0);
                }
            }

            Self::recompute_stats(&measurements, &stats, &measurement.operation).await;

            if measurement.duration > Duration::from_micros(100) {
                warn!(
                    operation = %measurement.operation,
                    latency = ?measurement.duration,
                    "high latency"
                );
            } else if measurement.duration > Duration::from_micros(10) {
                debug!(
                    operation = %measurement.operation,
                    latency = ?measurement.duration,
                    "moderate latency"
                );
            }
        });
    }

    pub async fn get_stats(&self) -> HashMap<String, LatencyStats> {
        self.stats.read().await.clone()
    }

    pub async fn get_operation_stats(&self, operation: &str) -> Option<LatencyStats> {
        self.stats.read().await.get(operation).cloned()
    }

    pub async fn get_measurements(&self, operation: &str) -> Option<Vec<LatencyMeasurement>> {
        self.measurements.read().await.get(operation).cloned()
    }

    pub async fn clear(&self) {
        self.measurements.write().await.clear();
        self.stats.write().await.clear();
        info!("cleared latency measurements and statistics");
    }

    /// Recompute percentile statistics for one operation from its raw samples.
    async fn recompute_stats(
        measurements: &Arc<RwLock<HashMap<String, Vec<LatencyMeasurement>>>>,
        stats: &Arc<RwLock<HashMap<String, LatencyStats>>>,
        operation: &str,
    ) {
        let mut durations: Vec<Duration> = {
            let measurements = measurements.read().await;
            match measurements.get(operation) {
                Some(samples) if !samples.is_empty() => {
                    samples.iter().map(|m| m.duration).collect()
                }
                _ => return,
            }
        };

        durations.sort_unstable();
        let len = durations.len();
        let total: Duration = durations.iter().sum();
        // Nearest-rank percentile: ceil(p * n), 1-indexed, clamped — the same
        // definition the benchmark subscriber uses, so the two agree.
        let percentile = |p: f64| {
            let rank = (p * len as f64).ceil() as usize;
            durations[rank.saturating_sub(1).min(len - 1)]
        };

        let computed = LatencyStats {
            min: durations[0],
            max: durations[len - 1],
            mean: total / len as u32,
            median: percentile(0.50),
            p95: percentile(0.95),
            p99: percentile(0.99),
            p999: percentile(0.999),
            count: len as u64,
            total,
        };

        stats.write().await.insert(operation.to_string(), computed);
    }
}

/// RAII-style measurer: start a timer, finish to record.
pub struct LatencyMeasurer {
    operation: String,
    start_time: Instant,
    monitor: Arc<LatencyMonitor>,
    metadata: HashMap<String, String>,
}

impl LatencyMeasurer {
    pub fn new(operation: &str, monitor: Arc<LatencyMonitor>) -> Self {
        Self {
            operation: operation.to_string(),
            start_time: Instant::now(),
            monitor,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn finish(self) {
        let duration = self.start_time.elapsed();
        self.monitor.record_operation_latency_with_metadata(
            &self.operation,
            duration,
            self.metadata,
        );
    }
}

/// Expected latency thresholds for the IPC operations.
pub mod thresholds {
    use std::time::Duration;

    pub const PUBLISH_EXPECTED: Duration = Duration::from_nanos(500);
    pub const PUBLISH_WARNING: Duration = Duration::from_micros(1);
    pub const RECEIVE_EXPECTED: Duration = Duration::from_nanos(300);
    pub const RECEIVE_WARNING: Duration = Duration::from_micros(1);
    pub const REQUEST_RESPONSE_EXPECTED: Duration = Duration::from_micros(1);
    pub const EVENT_EXPECTED: Duration = Duration::from_nanos(200);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_and_aggregates() {
        let monitor = LatencyMonitor::new();
        monitor.record_operation_latency("publish", Duration::from_micros(100));
        monitor.record_operation_latency("publish", Duration::from_micros(200));
        monitor.record_operation_latency("publish", Duration::from_micros(150));
        tokio::time::sleep(Duration::from_millis(20)).await;

        let stats = monitor.get_operation_stats("publish").await;
        assert!(stats.is_some());
        let s = stats.unwrap();
        assert_eq!(s.count, 3);
        assert_eq!(s.min, Duration::from_micros(100));
        assert_eq!(s.max, Duration::from_micros(200));
    }
}
