//! # rust-hotpath-ipc
//!
//! A lock-free, zero-copy shared-memory hot path for a low-latency trading
//! system. This is an extracted subsystem: the message layer, latency
//! instrumentation, and CPU-pinned processing loop that carry orders and market
//! data between processes with no serialization, no syscalls on the steady
//! state, and no database coupling.
//!
//! ## Layers
//!
//! - [`hot_path`] — the 64-byte cache-line-aligned POD messages
//!   ([`hot_path::OrderCommand`], [`hot_path::MarketTick`],
//!   [`hot_path::ExecutionReport`]) plus [`hot_path::rdtsc`] and the service names.
//! - [`pod_types`] — a wider set of `bytemuck`-validated POD market-data and
//!   order types, including fixed-point encoding and TWAP/VWAP/Iceberg parameter
//!   packing.
//! - [`rdtsc`] — cycle-accurate RDTSC latency recording with a bounded,
//!   non-blocking ring buffer.
//! - [`tsc_calibration`] — cycle <-> nanosecond calibration and TSC <-> Unix-time
//!   correlation across processes.
//! - [`latency`] — an async, off-hot-path monitor that aggregates named
//!   operations into percentile statistics.
//! - [`hot_path_service`] — the CPU-pinned, real-time-scheduled processing loop.
//! - [`service_names`] — shared Iceoryx2 service-name constants.
//!
//! ## Transport attribution
//!
//! The shared-memory ring buffer and pub/sub transport are provided by
//! [Iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2). What lives in this
//! crate is the message layout, the RDTSC latency pipeline, the CPU pinning and
//! real-time scheduling, and the hot/cold separation that keeps the hot path
//! free of any database or audit dependency.

pub mod hot_path;
pub mod hot_path_service;
pub mod latency;
pub mod latency_window;
pub mod pod_types;
pub mod rdtsc;
pub mod service_names;
pub mod strategy;
pub mod tsc_calibration;

// Convenient top-level re-exports for the most-used items.
pub use hot_path::{
    rdtsc as rdtsc_now, ExecutionReport, MarketTick, OrderBookSnapshot, OrderCommand,
    EXECUTION_SERVICE, MARKET_SERVICE, ORDER_SERVICE,
};
pub use hot_path_service::HotPathOrderService;
