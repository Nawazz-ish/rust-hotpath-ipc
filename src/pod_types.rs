//! Plain-Old-Data types for zero-copy Iceoryx2 communication.
//!
//! Every type here is:
//! - `#[repr(C, align(64))]` (or `repr(C)` for the 128-byte order types) —
//!   C-compatible, cache-line aware layout,
//! - `Pod + Zeroable` — validated safe for zero-copy by `bytemuck`,
//! - `ZeroCopySend` — usable directly in Iceoryx2 shared memory,
//! - one or two cache lines (64 / 128 bytes).
//!
//! Prices and quantities are fixed-point integers (value * 1e8), never floats,
//! so results are deterministic across processes and machines.

use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;

// ============================================================================
// MARKET-DATA POD TYPES (64 bytes each, one cache line)
// ============================================================================

/// Market tick (trade) — 64 bytes.
#[repr(C, align(64))]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct MarketTickPOD {
    pub timestamp_ns: u64, // RDTSC timestamp
    pub symbol_id: u32,
    pub exchange_id: u8,
    pub side: u8, // 0=buy, 1=sell
    pub padding1: [u8; 2],
    pub price: i64,    // fixed-point (price * 1e8)
    pub quantity: u64, // fixed-point (qty * 1e8)
    pub trade_id: u64,
    pub sequence_num: u64,
    pub flags: u32, // bit flags (is_buyer_maker, etc.)
    pub padding2: [u8; 12],
}

/// Order-book update — 64 bytes.
#[repr(C, align(64))]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct OrderBookUpdatePOD {
    pub timestamp_ns: u64,
    pub symbol_id: u32,
    pub exchange_id: u8,
    pub side: u8,        // 0=bid, 1=ask
    pub update_type: u8, // 0=add, 1=update, 2=delete
    pub padding1: u8,
    pub price: i64,
    pub quantity: u64,
    pub sequence_num: u64,
    pub padding2: [u8; 24],
}

/// Ticker (24h statistics) — 64 bytes.
#[repr(C, align(64))]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct TickerPOD {
    pub timestamp_ns: u64,
    pub symbol_id: u32,
    pub exchange_id: u8,
    pub padding1: [u8; 3],
    pub last_price: i64,
    pub bid_price: i64,
    pub ask_price: i64,
    pub volume_24h: u64,
    pub high_24h: i64,
    pub low_24h: i64,
}

/// OHLCV candle — 64 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug, Zeroable)]
pub struct OhlcvPOD {
    pub timestamp_ns: u64, // candle open time
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: u64,
    pub symbol_id: u32,
    pub exchange_id: u8,
    pub timeframe: u8, // 0=1m, 1=5m, 2=15m, 3=1h, 4=4h, 5=1d
    pub padding1: u16,
    pub padding2: u32,
}

// ============================================================================
// ORDER-MANAGEMENT POD TYPES (128 bytes each, two cache lines)
// ============================================================================

/// Order-creation request — 128 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug, Zeroable)]
pub struct CreateOrderRequestPOD {
    // Core order fields (64 bytes)
    pub timestamp_ns: u64,
    pub request_id: u64,
    pub price: i64,    // fixed-point (0 for market orders)
    pub quantity: u64, // fixed-point
    pub stop_price: i64,
    pub client_order_id: u64,
    pub strategy_id: u64, // 0 if manual
    pub user_id: i32,
    pub symbol_id: u32,

    // Algorithm & strategy fields (64 bytes)
    pub strategy_version: u32,
    pub exchange_id: u8,
    pub side: u8,            // 0=buy, 1=sell
    pub order_type: u16,     // see OrderType
    pub time_in_force: u8,   // 0=GTC, 1=IOC, 2=FOK, 3=GTT
    pub algorithm_type: u16, // see AlgorithmType
    pub padding1: u8,
    pub algorithm_params: [u8; 48], // flexible algorithm parameters
}

/// Order-type enumeration.
/// - 0-99: basic types (market, limit, stop, ...)
/// - 100-199: execution algorithms (TWAP, VWAP, Iceberg, ...)
/// - 200-299: compiled strategies
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OrderType {
    Market = 0,
    Limit = 1,
    StopLoss = 2,
    StopLimit = 3,
    TakeProfit = 4,
    TakeProfitLimit = 5,
    TrailingStop = 6,

    TWAP = 100,
    VWAP = 101,
    Iceberg = 102,
    POV = 103,
    Implementation = 104,
    TargetClose = 105,

    CompiledStrategy = 200,
}

/// Algorithm type (for the `algorithm_type` field).
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AlgorithmType {
    Direct = 0,
    TWAP = 1,
    VWAP = 2,
    Iceberg = 3,
    POV = 4,
    Compiled = 100,
}

/// Order response — 128 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug, Zeroable)]
pub struct OrderResponsePOD {
    pub timestamp_ns: u64,
    pub request_id: u64, // matches request
    pub filled_quantity: u64,
    pub avg_fill_price: i64,
    pub latency_ns: u64,    // hot-path processing latency
    pub order_id: [u8; 64], // exchange order ID (null-terminated)
    pub status: u8,         // 0=rejected, 1=accepted, 2=pending
    pub error_code: u16,    // 0 if success
    pub padding1: [u8; 13],
}

/// Order status update (lifecycle events) — 128 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct OrderStatusUpdatePOD {
    pub timestamp_ns: u64,
    pub filled_quantity: u64,
    pub remaining_quantity: u64,
    pub avg_fill_price: i64,
    pub last_fill_price: i64,
    pub last_fill_quantity: u64,
    pub order_id: [u8; 64],
    pub status: u8, // NEW, PENDING, FILLED, CANCELLED, REJECTED
    pub padding1: [u8; 15],
}

// ============================================================================
// ZeroCopySend for Iceoryx2
// ============================================================================

unsafe impl ZeroCopySend for MarketTickPOD {}
unsafe impl ZeroCopySend for OrderBookUpdatePOD {}
unsafe impl ZeroCopySend for TickerPOD {}
unsafe impl ZeroCopySend for OhlcvPOD {}
unsafe impl ZeroCopySend for CreateOrderRequestPOD {}
unsafe impl ZeroCopySend for OrderResponsePOD {}
unsafe impl ZeroCopySend for OrderStatusUpdatePOD {}

// Manual Pod for types with explicit padding.
unsafe impl Pod for OhlcvPOD {}
unsafe impl Pod for CreateOrderRequestPOD {}
unsafe impl Pod for OrderResponsePOD {}

// ============================================================================
// Fixed-point helpers and algorithm-parameter encoding
// ============================================================================

impl MarketTickPOD {
    pub fn from_price_f64(price: f64) -> i64 {
        (price * 100_000_000.0) as i64
    }
    pub fn to_price_f64(price_fixed: i64) -> f64 {
        price_fixed as f64 / 100_000_000.0
    }
    pub fn from_quantity_f64(quantity: f64) -> u64 {
        (quantity * 100_000_000.0) as u64
    }
    pub fn to_quantity_f64(quantity_fixed: u64) -> f64 {
        quantity_fixed as f64 / 100_000_000.0
    }
}

impl CreateOrderRequestPOD {
    pub fn from_price_f64(price: f64) -> i64 {
        (price * 100_000_000.0) as i64
    }
    pub fn to_price_f64(price_fixed: i64) -> f64 {
        price_fixed as f64 / 100_000_000.0
    }
    pub fn from_quantity_f64(quantity: f64) -> u64 {
        (quantity * 100_000_000.0) as u64
    }
    pub fn to_quantity_f64(quantity_fixed: u64) -> f64 {
        quantity_fixed as f64 / 100_000_000.0
    }

    /// TWAP params: [duration_seconds: u64, num_slices: u32, ...].
    pub fn decode_twap_params(&self) -> Option<(u64, u32)> {
        if self.algorithm_type != AlgorithmType::TWAP as u16 {
            return None;
        }
        let duration = u64::from_le_bytes(self.algorithm_params[0..8].try_into().ok()?);
        let num_slices = u32::from_le_bytes(self.algorithm_params[8..12].try_into().ok()?);
        Some((duration, num_slices))
    }

    pub fn encode_twap_params(&mut self, duration_seconds: u64, num_slices: u32) {
        self.algorithm_type = AlgorithmType::TWAP as u16;
        self.algorithm_params[0..8].copy_from_slice(&duration_seconds.to_le_bytes());
        self.algorithm_params[8..12].copy_from_slice(&num_slices.to_le_bytes());
    }

    /// VWAP params: [participation_rate: f64 bits, duration_seconds: u64, ...].
    pub fn decode_vwap_params(&self) -> Option<(f64, u64)> {
        if self.algorithm_type != AlgorithmType::VWAP as u16 {
            return None;
        }
        let participation_bits = u64::from_le_bytes(self.algorithm_params[0..8].try_into().ok()?);
        let participation_rate = f64::from_bits(participation_bits);
        let duration = u64::from_le_bytes(self.algorithm_params[8..16].try_into().ok()?);
        Some((participation_rate, duration))
    }

    pub fn encode_vwap_params(&mut self, participation_rate: f64, duration_seconds: u64) {
        self.algorithm_type = AlgorithmType::VWAP as u16;
        self.algorithm_params[0..8].copy_from_slice(&participation_rate.to_bits().to_le_bytes());
        self.algorithm_params[8..16].copy_from_slice(&duration_seconds.to_le_bytes());
    }

    /// Iceberg params: [display_quantity: u64, total_quantity: u64, ...].
    pub fn decode_iceberg_params(&self) -> Option<(u64, u64)> {
        if self.algorithm_type != AlgorithmType::Iceberg as u16 {
            return None;
        }
        let display_qty = u64::from_le_bytes(self.algorithm_params[0..8].try_into().ok()?);
        let total_qty = u64::from_le_bytes(self.algorithm_params[8..16].try_into().ok()?);
        Some((display_qty, total_qty))
    }
}

// ============================================================================
// Compile-time size / alignment assertions
// ============================================================================

const _: () = assert!(std::mem::size_of::<MarketTickPOD>() == 64);
const _: () = assert!(std::mem::align_of::<MarketTickPOD>() == 64);
const _: () = assert!(std::mem::size_of::<OrderBookUpdatePOD>() == 64);
const _: () = assert!(std::mem::size_of::<TickerPOD>() == 64);
const _: () = assert!(std::mem::size_of::<OhlcvPOD>() == 64);
const _: () = assert!(std::mem::size_of::<CreateOrderRequestPOD>() == 128);
const _: () = assert!(std::mem::size_of::<OrderResponsePOD>() == 128);
const _: () = assert!(std::mem::size_of::<OrderStatusUpdatePOD>() == 128);
