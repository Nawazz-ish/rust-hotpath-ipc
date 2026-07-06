//! # rust-hotpath-ipc
//!
//! A lock-free, zero-copy shared-memory hot path for a low-latency trading
//! system. This is an extracted subsystem: the message layer, the strategy
//! engine, and the latency instrumentation that carry orders and market data
//! between processes with no serialization, no syscalls on the steady state,
//! and no database coupling.
//!
//! ## Layers
//!
//! - [`hot_path`] — the 64-byte cache-line-aligned POD messages
//!   ([`hot_path::OrderCommand`], [`hot_path::MarketTick`],
//!   [`hot_path::ExecutionReport`]) plus [`hot_path::rdtsc`] and the service names.
//! - [`strategy`] — the composite trend/momentum/mean-reversion strategy that
//!   runs on each tick.
//! - [`compiler`] + [`bytecode`] — compile a drawn strategy graph into a flat
//!   bytecode program and interpret it on a stack VM, once per tick.
//! - [`tsc_calibration`] — cycle <-> nanosecond calibration and TSC <-> Unix-time
//!   correlation across processes.
//! - [`latency_window`] — the off-hot-path latency recorder: a lock-free push on
//!   the hot side, percentile aggregation on a reporter thread pinned off-core.
//!
//! ## Transport attribution
//!
//! The shared-memory ring buffer and pub/sub transport are provided by
//! [Iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2). What lives in this
//! crate is the message layout, the RDTSC latency pipeline, the CPU pinning and
//! real-time scheduling, and the hot/cold separation that keeps the hot path
//! free of any database or audit dependency.

pub mod bytecode;
pub mod compiler;
pub mod hot_path;
pub mod latency_window;
pub mod strategy;
pub mod tsc_calibration;

// Convenient top-level re-exports for the most-used items.
pub use hot_path::{
    rdtsc as rdtsc_now, ExecutionReport, MarketTick, OrderBookSnapshot, OrderCommand,
    EXECUTION_SERVICE, MARKET_SERVICE, ORDER_SERVICE,
};
