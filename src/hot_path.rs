// Hot-path POD message types for ultra-low-latency IPC.
//
// These are the only types used on the Iceoryx2 hot path. Each is exactly
// one cache line (64 bytes), C-layout, and marked ZeroCopySend so it can be
// handed to another process through shared memory with no serialization.

use iceoryx2::prelude::*;

// ============================================================================
// POD MESSAGE TYPES — exactly cache-line aligned (64 bytes)
// ============================================================================

/// Order command — 64 bytes exactly.
#[repr(C, align(64))]
#[derive(Debug, Copy, Clone, Default)]
pub struct OrderCommand {
    pub timestamp_ns: u64, // 8  @0  - T1: RDTSC when the strategy emits this order
    pub order_id: u64,     // 8  @8
    pub price_ticks: i64,  // 8  @16 - price in minimum tick size (fixed point)
    pub quantity: u64,     // 8  @24 - fixed point
    // T0: the origin MarketTick's RDTSC timestamp, carried through so the
    // execution stage can measure tick-to-fill latency end to end. Placed at
    // offset 32 (8-aligned) so the struct stays exactly 64 bytes with no
    // implicit padding — it lives in what used to be spare padding, so it
    // costs nothing on the wire.
    pub origin_ts: u64,    // 8  @32
    pub symbol_id: u32,    // 4  @40 - pre-mapped symbol
    pub user_id: u16,      // 2  @44
    pub side: u8,          // 1  @46 - 0=buy, 1=sell
    pub order_type: u8,    // 1  @47 - 0=market, 1=limit
    pub action: u8,        // 1  @48 - 0=new, 1=cancel, 2=modify
    pub flags: u8,         // 1  @49
    pub exchange_id: u8,   // 1  @50 - pre-mapped exchange
    pub priority: u8,      // 1  @51 - execution priority
    pub padding: [u8; 12], // 12 @52 - pad to 64
}

/// Market-data tick — 64 bytes exactly.
#[repr(C, align(64))]
#[derive(Debug, Copy, Clone, Default)]
pub struct MarketTick {
    pub timestamp_ns: u64, // 8
    pub symbol_id: u32,    // 4
    pub exchange_id: u8,   // 1
    pub tick_type: u8,     // 1  - 0=trade, 1=bid, 2=ask
    pub padding1: [u8; 2], // 2  - alignment
    pub price: i64,        // 8
    pub quantity: u64,     // 8
    pub bid: i64,          // 8
    pub ask: i64,          // 8
    pub volume_24h: u64,   // 8
    pub sequence: u64,     // 8
}

/// Execution report — 64 bytes exactly.
#[repr(C, align(64))]
#[derive(Debug, Copy, Clone, Default)]
pub struct ExecutionReport {
    pub timestamp_ns: u64,       // 8
    pub order_id: u64,           // 8
    pub exchange_order_id: u64,  // 8
    pub executed_price: i64,     // 8
    pub executed_quantity: u64,  // 8
    pub remaining_quantity: u64, // 8
    pub commission: i64,         // 8
    pub status: u8,              // 1  - 0=new, 1=partial, 2=filled, 3=cancelled
    pub reject_reason: u8,       // 1
    pub padding: [u8; 6],        // 6  - pad to 64
}

/// Order-book snapshot — 64 bytes exactly.
#[repr(C, align(64))]
#[derive(Debug, Copy, Clone, Default)]
pub struct OrderBookSnapshot {
    pub timestamp_ns: u64, // 8
    pub symbol_id: u32,    // 4
    pub exchange_id: u8,   // 1
    pub depth: u8,         // 1  - number of levels
    pub padding1: [u8; 2], // 2  - alignment
    pub best_bid: i64,     // 8
    pub best_ask: i64,     // 8
    pub bid_size: u64,     // 8
    pub ask_size: u64,     // 8
    pub mid_price: i64,    // 8
    pub spread: i64,       // 8
}

// Zero-copy send is sound for these types: they are POD, fixed size, no
// pointers, no padding-dependent invariants.
unsafe impl ZeroCopySend for OrderCommand {}
unsafe impl ZeroCopySend for MarketTick {}
unsafe impl ZeroCopySend for ExecutionReport {}
unsafe impl ZeroCopySend for OrderBookSnapshot {}

// ============================================================================
// SERVICE NAMES — must match across every process on the bus
// ============================================================================

pub const ORDER_SERVICE: &str = "Trading/Orders/Commands";
pub const MARKET_SERVICE: &str = "Trading/Market/Ticks";
pub const EXECUTION_SERVICE: &str = "Trading/Orders/Executions";

/// Event (notification) service paired with `MARKET_SERVICE`. In WaitSet mode the
/// consumer notifies here after each publish so the strategy can block instead of
/// busy-poll; the tick data itself still travels over the pub/sub service.
pub const MARKET_EVENT: &str = "Trading/Market/TickEvent";

/// Top-of-book snapshots (best bid/ask + sizes), published by the exchange
/// alongside the tick stream. Cold-path consumers and the UI read depth here
/// without touching `MARKET_SERVICE`.
pub const BOOK_SERVICE: &str = "Trading/Market/Book";

/// Event (notification) service paired with `ORDER_SERVICE`. When the exchange
/// runs in ORDER_WAIT=waitset mode it blocks on this listener instead of
/// busy-polling `receive()`; the strategy notifies here after each order send.
/// Orders themselves still travel over the pub/sub `ORDER_SERVICE`.
pub const ORDER_EVENT: &str = "Trading/Orders/CommandEvent";

// ============================================================================
// UTILITIES
// ============================================================================

/// Read the CPU timestamp counter. On x86_64 this is a single `rdtsc`
/// instruction (a few cycles); elsewhere we fall back to the system clock.
///
/// This is the crate's single hardware timestamp source; other modules
/// delegate here rather than re-implementing the read.
#[inline(always)]
pub fn rdtsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

/// A *serializing* timestamp read, for timing very short intervals (tens of ns).
///
/// Plain `rdtsc` is not ordered against surrounding instructions, so the CPU can
/// hoist work into or out of the window you think you are timing. For the
/// decision-only measurement — where the interval is small enough that a few
/// reordered instructions matter — we fence first (`lfence` waits for prior
/// instructions to retire) and then read. This trades a little more read
/// overhead for a read that actually brackets the code between two calls; that
/// overhead is measured and subtracted (see `latency_window::calibrate_rdtsc_floor`).
///
/// Only worth using for short windows: for the hundreds-of-ns tick-to-order and
/// tick-to-fill windows, plain `rdtsc` is fine and cheaper.
#[inline(always)]
pub fn rdtsc_serialized() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_lfence();
        core::arch::x86_64::_rdtsc()
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        rdtsc()
    }
}

/// Pre-mapped symbol IDs — integer keys instead of string symbols so the hot
/// path never allocates or hashes.
pub mod symbols {
    pub const BTC_USDT: u32 = 1;
    pub const ETH_USDT: u32 = 2;
    pub const BTC_USD: u32 = 3;
    pub const ETH_USD: u32 = 4;
}

/// Pre-mapped exchange IDs.
pub mod exchanges {
    pub const BINANCE: u8 = 1;
    pub const OKX: u8 = 2;
    pub const DERIBIT: u8 = 3;
    pub const COINBASE: u8 = 4;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_types_are_one_cache_line() {
        assert_eq!(std::mem::size_of::<OrderCommand>(), 64);
        assert_eq!(std::mem::align_of::<OrderCommand>(), 64);
        assert_eq!(std::mem::size_of::<MarketTick>(), 64);
        assert_eq!(std::mem::size_of::<ExecutionReport>(), 64);
        assert_eq!(std::mem::size_of::<OrderBookSnapshot>(), 64);
    }

    #[test]
    fn origin_ts_is_eight_byte_aligned() {
        // origin_ts is a u64 read/written on the hot path; a misaligned offset
        // would force implicit padding (growing the struct past 64) or a slower
        // unaligned access. Guard the offset explicitly.
        assert_eq!(std::mem::offset_of!(OrderCommand, origin_ts) % 8, 0);
        assert_eq!(std::mem::offset_of!(OrderCommand, origin_ts), 32);
    }
}
