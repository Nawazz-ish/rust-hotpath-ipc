//! A limit order book with price-time priority — the pure matching engine.
//!
//! This is the venue's core, factored out of the exchange binary so it can be
//! unit-tested and reused without any IPC. It has zero dependency on the
//! transport or the wire types: it speaks in plain integers (`price_ticks`,
//! fixed-point `quantity`) and hands back [`Fill`]s that the binary turns into
//! `ExecutionReport`s.
//!
//! The book is two price-sorted sides, each a map of price levels, and each
//! level a FIFO queue — so matching honours **price-time priority**: best price
//! first across levels, arrival order within a level. A marketable order crosses
//! the spread and fills against resting liquidity (walking several levels for
//! size → partial fills); a passive limit order rests and gains a queue position
//! that only clears as the orders ahead of it fill or cancel.
//!
//! Data structures: `BTreeMap<price, PriceLevel>` per side (ordered iteration
//! from the best price inward) + a `HashMap<order_id, (side, price)>` so cancels
//! are O(1) to locate. The steady-state match/pop path does not allocate; a new
//! price level or a growing queue can. This is a venue simulation held to a
//! slightly looser standard than the strategy hot path, and the doc says so.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// Which side of the book an order sits on. Mirrors `OrderCommand.side`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    /// Decode the wire `side` byte (0 = buy, else sell).
    pub fn from_u8(v: u8) -> Self {
        if v == 0 {
            Side::Buy
        } else {
            Side::Sell
        }
    }
    fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// One resting order in a price level's FIFO queue. It carries only what
/// matching needs; the strategy's T0/side for tick-to-fill are correlated
/// downstream by `order_id` (the execution stage keeps that map), so they are
/// not duplicated here.
#[derive(Clone, Copy, Debug)]
struct RestingOrder {
    order_id: u64,
    remaining: u64,     // fixed-point qty still working
    is_synthetic: bool, // synthetic (participant) orders emit no ExecutionReport
}

/// FIFO queue at a single price. Time priority == queue order.
#[derive(Default)]
struct PriceLevel {
    total_size: u64, // sum of `remaining`, cached so snapshots are O(1)
    queue: VecDeque<RestingOrder>,
}

impl PriceLevel {
    fn new() -> Self {
        // Pre-size the queue so steady-state resting does not reallocate.
        Self {
            total_size: 0,
            queue: VecDeque::with_capacity(16),
        }
    }
}

/// A single match between an incoming (taker) order and a resting (maker) order.
/// The exchange binary turns each of these into an `ExecutionReport`; tests
/// collect them into a `Vec`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fill {
    pub taker_order_id: u64,
    pub maker_order_id: u64,
    pub price_ticks: i64,
    pub quantity: u64,
    pub taker_origin_ts: u64,
    /// 0 => this was the terminal fill for the taker.
    pub taker_remaining_after: u64,
    pub taker_is_synthetic: bool,
    pub maker_is_synthetic: bool,
}

/// Why an order did not (fully) execute — carried into `ExecutionReport.reject_reason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    /// A marketable order with nothing (more) to hit.
    NoLiquidity = 2,
}

/// The outcome of resting or rejecting the remainder of an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Fully filled by crossing; nothing left.
    Filled,
    /// Limit order rested; carries its queue position (size ahead of it) at insert.
    Rested { queue_ahead: u64 },
    /// Marketable remainder with no liquidity left.
    Rejected(Reject),
    /// A cancel resolved (order removed).
    Cancelled,
    /// A cancel for an unknown order.
    CancelRejected,
}

/// The limit order book: bids and asks as price-sorted maps of FIFO levels.
pub struct Book {
    bids: BTreeMap<i64, PriceLevel>, // best bid = greatest key
    asks: BTreeMap<i64, PriceLevel>, // best ask = least key
    // O(1) cancel: order_id -> (side, price) so we never scan the book to find it.
    locate: HashMap<u64, (Side, i64)>,
}

impl Default for Book {
    fn default() -> Self {
        Self::new()
    }
}

impl Book {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            locate: HashMap::new(),
        }
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }
    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }
    pub fn size_at(&self, side: Side, px: i64) -> u64 {
        let book = if side == Side::Buy {
            &self.bids
        } else {
            &self.asks
        };
        book.get(&px).map(|l| l.total_size).unwrap_or(0)
    }

    /// Number of distinct bid price levels — used for the snapshot's depth byte.
    pub fn bid_level_count(&self) -> usize {
        self.bids.len()
    }

    /// Whether an order id is currently resting in the book. Lets the synthetic
    /// participants prune ids that have since filled or cancelled without
    /// exposing the internal map.
    pub fn is_resting(&self, order_id: u64) -> bool {
        self.locate.contains_key(&order_id)
    }

    /// Submit a new order (marketable or limit). Crossing fills are pushed into
    /// `fills`; the return value describes what happened to the remainder.
    ///
    /// `is_limit` false = market order (cross what you can, reject the rest).
    /// `is_limit` true  = limit order (cross what price allows, rest the remainder).
    #[allow(clippy::too_many_arguments)]
    pub fn submit(
        &mut self,
        order_id: u64,
        side: Side,
        is_limit: bool,
        limit_px: i64,
        quantity: u64,
        origin_ts: u64,
        is_synthetic: bool,
        fills: &mut Vec<Fill>,
    ) -> Outcome {
        let mut remaining = quantity;

        // ---- CROSS: consume the opposite side from the best price inward. ----
        loop {
            if remaining == 0 {
                break;
            }
            let opp = side.opposite();
            let best_px = match self.best_of(opp) {
                Some(p) => p,
                None => break, // opposite side empty
            };
            // A limit order only crosses while the price is acceptable.
            if is_limit {
                let acceptable = match side {
                    Side::Buy => best_px <= limit_px,  // don't pay above my bid
                    Side::Sell => best_px >= limit_px, // don't sell below my ask
                };
                if !acceptable {
                    break;
                }
            }

            // Consume the FIFO queue at this level, front to back (time priority).
            // We collect the maker ids that fully fill and clear them from
            // `locate` after this borrow of the level ends (can't touch another
            // field of `self` while `level` is borrowed).
            let mut cleared_makers: Vec<u64> = Vec::new();
            let level = self.level_mut(opp, best_px);
            while remaining > 0 {
                let front = match level.queue.front_mut() {
                    Some(f) => f,
                    None => break,
                };
                let fill_qty = remaining.min(front.remaining);
                remaining -= fill_qty;
                front.remaining -= fill_qty;
                level.total_size -= fill_qty;

                let maker_id = front.order_id;
                let maker_syn = front.is_synthetic;
                let front_done = front.remaining == 0;

                fills.push(Fill {
                    taker_order_id: order_id,
                    maker_order_id: maker_id,
                    price_ticks: best_px,
                    quantity: fill_qty,
                    taker_origin_ts: origin_ts,
                    taker_remaining_after: remaining,
                    taker_is_synthetic: is_synthetic,
                    maker_is_synthetic: maker_syn,
                });

                if front_done {
                    // FIFO advance: the order behind moves up a queue position.
                    level.queue.pop_front();
                    cleared_makers.push(maker_id);
                }
            }
            let level_empty = level.queue.is_empty();

            // Borrow of `level` has ended; now update the sibling fields.
            for id in cleared_makers {
                self.locate.remove(&id);
            }
            if level_empty {
                self.remove_level(opp, best_px);
            }
        }

        // ---- REST or REJECT the remainder. ----
        if remaining == 0 {
            return Outcome::Filled;
        }
        if !is_limit {
            // A market order does not rest; its remainder is rejected (IOC-like).
            return Outcome::Rejected(Reject::NoLiquidity);
        }

        // Limit remainder rests on its own side. Queue position = size already
        // resting at that price when we arrive.
        let level = self.level_mut(side, limit_px);
        let queue_ahead = level.total_size;
        level.queue.push_back(RestingOrder {
            order_id,
            remaining,
            is_synthetic,
        });
        level.total_size += remaining;
        self.locate.insert(order_id, (side, limit_px));
        Outcome::Rested { queue_ahead }
    }

    /// Cancel a resting order by id. O(1) location, O(level) removal.
    pub fn cancel(&mut self, order_id: u64) -> Outcome {
        let (side, px) = match self.locate.remove(&order_id) {
            Some(v) => v,
            None => return Outcome::CancelRejected,
        };
        let level = self.level_mut(side, px);
        if let Some(pos) = level.queue.iter().position(|o| o.order_id == order_id) {
            let removed = level.queue.remove(pos).unwrap();
            level.total_size -= removed.remaining;
        }
        if level.queue.is_empty() {
            self.remove_level(side, px);
        }
        Outcome::Cancelled
    }

    // --- small helpers so the borrow checker stays happy across sides ---

    fn best_of(&self, side: Side) -> Option<i64> {
        match side {
            Side::Buy => self.best_bid(),
            Side::Sell => self.best_ask(),
        }
    }

    fn level_mut(&mut self, side: Side, px: i64) -> &mut PriceLevel {
        let book = if side == Side::Buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        book.entry(px).or_insert_with(PriceLevel::new)
    }

    fn remove_level(&mut self, side: Side, px: i64) {
        let book = if side == Side::Buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        book.remove(&px);
    }
}

// ============================================================================
// Tests — drive the pure Book directly, collecting fills into a Vec.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Convenience: rest a maker order (limit that does not cross) and return its id.
    fn rest(book: &mut Book, id: u64, side: Side, px: i64, qty: u64) {
        let mut fills = Vec::new();
        let out = book.submit(id, side, true, px, qty, 0, true, &mut fills);
        assert!(
            matches!(out, Outcome::Rested { .. }),
            "expected the maker to rest, got {out:?}"
        );
        assert!(fills.is_empty(), "a resting maker should not fill");
    }

    #[test]
    fn marketable_order_fills_against_resting_liquidity() {
        let mut book = Book::new();
        rest(&mut book, 1, Side::Sell, 100, 5); // ask 5 @100
        let mut fills = Vec::new();
        // market buy 3 -> fills 3 @100, ask left with 2.
        let out = book.submit(10, Side::Buy, false, 0, 3, 0, false, &mut fills);
        assert_eq!(out, Outcome::Filled);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price_ticks, 100);
        assert_eq!(fills[0].quantity, 3);
        assert_eq!(fills[0].taker_remaining_after, 0); // terminal fill
        assert_eq!(book.size_at(Side::Sell, 100), 2);
    }

    #[test]
    fn passive_order_rests_records_queue_position_and_advances() {
        let mut book = Book::new();
        rest(&mut book, 1, Side::Buy, 99, 4); // maker bid 4 @99
        let mut fills = Vec::new();
        // our limit buy 2 @99 does not cross (no ask); it rests BEHIND the maker.
        let out = book.submit(10, Side::Buy, true, 99, 2, 0, false, &mut fills);
        assert_eq!(out, Outcome::Rested { queue_ahead: 4 });
        assert!(fills.is_empty());
        assert_eq!(book.size_at(Side::Buy, 99), 6);

        // A crossing sell of 4 clears the maker ahead; our order advances to front.
        let mut f2 = Vec::new();
        let out2 = book.submit(20, Side::Sell, false, 0, 4, 0, true, &mut f2);
        assert_eq!(out2, Outcome::Filled);
        // The maker (id 1) filled, not us (id 10): time priority.
        assert!(f2.iter().all(|f| f.maker_order_id == 1));
        assert_eq!(book.size_at(Side::Buy, 99), 2); // only our 2 left, now at front
    }

    #[test]
    fn large_order_partial_fills_across_levels() {
        let mut book = Book::new();
        rest(&mut book, 1, Side::Sell, 100, 2);
        rest(&mut book, 2, Side::Sell, 101, 2);
        rest(&mut book, 3, Side::Sell, 102, 2);
        let mut fills = Vec::new();
        // market buy 5 walks 100,101, then 1 @102.
        let out = book.submit(10, Side::Buy, false, 0, 5, 0, false, &mut fills);
        assert_eq!(out, Outcome::Filled);
        assert_eq!(fills.len(), 3);
        assert_eq!((fills[0].price_ticks, fills[0].quantity), (100, 2));
        assert_eq!((fills[1].price_ticks, fills[1].quantity), (101, 2));
        assert_eq!((fills[2].price_ticks, fills[2].quantity), (102, 1));
        // remaining_after decrements 5 -> 3 -> 1 -> 0
        assert_eq!(fills[0].taker_remaining_after, 3);
        assert_eq!(fills[1].taker_remaining_after, 1);
        assert_eq!(fills[2].taker_remaining_after, 0);
        assert_eq!(book.best_ask(), Some(102));
        assert_eq!(book.size_at(Side::Sell, 102), 1);
        assert_eq!(book.size_at(Side::Sell, 100), 0);
    }

    #[test]
    fn cancel_removes_queue_size_and_advances_those_behind() {
        let mut book = Book::new();
        rest(&mut book, 1, Side::Buy, 99, 3); // A: 3 @99
        rest(&mut book, 2, Side::Buy, 99, 2); // B: 2 @99, behind A
        assert_eq!(book.size_at(Side::Buy, 99), 5);
        // cancel A
        assert_eq!(book.cancel(1), Outcome::Cancelled);
        assert_eq!(book.size_at(Side::Buy, 99), 2);
        // a crossing sell of 2 now fills B (proving A's slot is gone, B advanced).
        let mut fills = Vec::new();
        book.submit(20, Side::Sell, false, 0, 2, 0, true, &mut fills);
        assert!(fills.iter().all(|f| f.maker_order_id == 2));
        assert_eq!(book.size_at(Side::Buy, 99), 0);
    }

    #[test]
    fn price_time_priority_respected_both_sides() {
        // Ask side: X then Y at best price 100, Z at 101. A buy of 2 hits X,Y first.
        let mut book = Book::new();
        rest(&mut book, 101, Side::Sell, 100, 1); // X (arrives first)
        rest(&mut book, 102, Side::Sell, 100, 1); // Y (arrives second)
        rest(&mut book, 103, Side::Sell, 101, 1); // Z (worse price)
        let mut fills = Vec::new();
        book.submit(10, Side::Buy, false, 0, 2, 0, false, &mut fills);
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].maker_order_id, 101); // X first (time priority)
        assert_eq!(fills[1].maker_order_id, 102); // then Y
        assert_eq!(book.size_at(Side::Sell, 101), 1); // Z untouched

        // Bid side symmetry: highest bid fills first for an incoming sell.
        let mut book2 = Book::new();
        rest(&mut book2, 201, Side::Buy, 100, 1); // worse bid
        rest(&mut book2, 202, Side::Buy, 101, 1); // best bid
        let mut f2 = Vec::new();
        book2.submit(20, Side::Sell, false, 0, 1, 0, false, &mut f2);
        assert_eq!(f2.len(), 1);
        assert_eq!(f2[0].maker_order_id, 202); // best (highest) bid hit first
        assert_eq!(f2[0].price_ticks, 101);
    }

    #[test]
    fn market_order_rejects_when_no_liquidity() {
        let mut book = Book::new();
        let mut fills = Vec::new();
        let out = book.submit(10, Side::Buy, false, 0, 5, 0, false, &mut fills);
        assert_eq!(out, Outcome::Rejected(Reject::NoLiquidity));
        assert!(fills.is_empty());
    }

    #[test]
    fn origin_ts_threads_to_fill() {
        // The taker's origin_ts (T0) must ride onto its fills so tick-to-fill still
        // measures a real round-trip downstream.
        let mut book = Book::new();
        rest(&mut book, 1, Side::Sell, 100, 5);
        let mut fills = Vec::new();
        book.submit(10, Side::Buy, false, 0, 3, 987654321, false, &mut fills);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].taker_origin_ts, 987654321);
    }

    #[test]
    fn cancel_unknown_order_is_rejected() {
        let mut book = Book::new();
        assert_eq!(book.cancel(999), Outcome::CancelRejected);
    }
}
