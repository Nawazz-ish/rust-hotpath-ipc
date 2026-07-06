//! Mock exchange: a limit order book matching engine that is also the market's
//! source of truth.
//!
//! This process replaces the old loopback fill. It maintains a real order book —
//! two price-sorted sides with a first-in-first-out queue at each price, so
//! matching respects **price-time priority** — and it matches the strategy's
//! orders against resting liquidity: a marketable order crosses the spread and
//! fills (walking several price levels for size, i.e. partial fills), a passive
//! limit order rests and only fills once the queue ahead of it clears, and a
//! cancel pulls a resting order.
//!
//! The matcher itself is the pure [`rust_hotpath_ipc::order_book`] module (no IPC,
//! unit-tested standalone); this binary wires it onto the shared-memory bus and
//! drives it with synthetic liquidity.
//!
//! Because a real venue's price *is* whatever its book says, the exchange is also
//! the market-data source. A set of seeded synthetic participants post and cancel
//! orders around a slowly drifting fair value, keeping the book two-sided and
//! liquid; the top of that book becomes the `MarketTick` stream the strategy
//! trades against, and the book's best bid/ask are published as `OrderBookSnapshot`.
//! So the ticks the strategy sees and the fills it gets come from the *same* book.
//!
//! Run with:  CPU_CORE=1 SEED=42 TICK_US=40 cargo run --release --bin exchange

use iceoryx2::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rust_hotpath_ipc::hot_path::*;
use rust_hotpath_ipc::order_book::{Book, Fill, Side};
use rust_hotpath_ipc::runtime::{env_or, pin_only};

// ============================================================================
// Synthetic participants — a seeded market that keeps the book liquid
// ============================================================================

/// A tiny deterministic PRNG (xorshift64*) — no rng crate, reproducible from a
/// seed, same generator the old feed used so runs stay comparable.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Standard normal via Box-Muller.
    fn normal(&mut self) -> f64 {
        let u1 = self.unit().max(1e-12);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
    /// Integer in [0, n).
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

/// Fixed-point scale for prices/quantities on the wire (1e8), matching the rest
/// of the pipeline.
const SCALE: f64 = 100_000_000.0;

/// Synthetic order ids live in a reserved high range so they never collide with
/// the strategy's ids (which start at 1 and climb). The top bit marks synthetic.
const SYNTHETIC_BIT: u64 = 1 << 63;

/// Drives the book's synthetic liquidity: a mean-reverting fair value with the
/// same regime-switching flavour the old feed had, plus maker order flow around
/// it. Deterministic given a seed.
struct Participants {
    rng: Rng,
    fair_value: f64, // in price units (not ticks)
    vol: f64,
    drift: f64,
    regime_left: u64,
    next_syn_id: u64,
    // Ids of live synthetic orders so we can cancel some to churn the queue.
    live: Vec<u64>,
}

impl Participants {
    fn new(seed: u64, vol: f64) -> Self {
        Self {
            rng: Rng::new(seed),
            fair_value: 50_000.0,
            vol,
            drift: 0.0,
            regime_left: 0,
            next_syn_id: 1,
            live: Vec::with_capacity(256),
        }
    }

    fn new_syn_id(&mut self) -> u64 {
        let id = SYNTHETIC_BIT | self.next_syn_id;
        self.next_syn_id += 1;
        id
    }

    /// Advance the fair value one step (regime-switching random walk, mild pull
    /// toward a wide band), same shape as the old feed's price model.
    fn step_fair_value(&mut self) {
        if self.regime_left == 0 {
            let roll = self.rng.unit();
            self.drift = if roll < 0.35 {
                self.vol * (0.15 + 0.25 * self.rng.unit())
            } else if roll < 0.70 {
                -self.vol * (0.15 + 0.25 * self.rng.unit())
            } else {
                0.0
            };
            self.regime_left = 2_000 + self.rng.below(6_000);
        }
        self.regime_left -= 1;
        let pull = (50_000.0 - self.fair_value) * 0.0005;
        self.fair_value += self.drift + pull + self.vol * self.rng.normal();
        if self.fair_value < 1.0 {
            self.fair_value = 1.0;
        }
    }

    /// Convert a price in units to fixed-point ticks.
    fn to_ticks(px: f64) -> i64 {
        (px * SCALE) as i64
    }

    /// Run one round of participant activity against the book. Returns whether the
    /// top of book plausibly changed (so the caller republishes market data).
    fn act(&mut self, book: &mut Book, sink: &mut Vec<Fill>) -> bool {
        self.step_fair_value();

        // Post a fresh two-sided quote around fair value. Spread and offset are a
        // few ticks; sizes are 1..=4 units.
        let tick = 1.0; // 1.0 price unit granularity for the synthetic book
        let half_spread = tick * (1.0 + self.rng.below(3) as f64);
        let bid_px = Self::to_ticks(self.fair_value - half_spread);
        let ask_px = Self::to_ticks(self.fair_value + half_spread);
        let bid_sz = ((1 + self.rng.below(4)) as f64 * SCALE) as u64;
        let ask_sz = ((1 + self.rng.below(4)) as f64 * SCALE) as u64;

        let bid_id = self.new_syn_id();
        book.submit(bid_id, Side::Buy, true, bid_px, bid_sz, 0, true, sink);
        self.live.push(bid_id);
        let ask_id = self.new_syn_id();
        book.submit(ask_id, Side::Sell, true, ask_px, ask_sz, 0, true, sink);
        self.live.push(ask_id);

        // Occasionally cancel an older synthetic order so the queue churns and a
        // resting strategy order actually advances.
        if !self.live.is_empty() && self.rng.unit() < 0.5 {
            let idx = self.rng.below(self.live.len() as u64) as usize;
            let id = self.live.swap_remove(idx);
            book.cancel(id);
        }

        // Occasionally a synthetic taker crosses the spread — this is what
        // produces trade prints that move the top of book.
        if self.rng.unit() < 0.25 {
            let cross_id = self.new_syn_id();
            let sz = ((1 + self.rng.below(3)) as f64 * SCALE) as u64;
            let side = if self.rng.unit() < 0.5 {
                Side::Buy
            } else {
                Side::Sell
            };
            book.submit(cross_id, side, false, 0, sz, 0, true, sink);
        }

        // Prune ids that no longer rest (filled/cancelled) so `live` doesn't grow
        // unbounded; cheap membership check against the book's locate map.
        self.live.retain(|id| book.is_resting(*id));

        true
    }
}

// ============================================================================
// Main: wire the book + participants onto the shared-memory bus
// ============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Paced producer of market data (like the old feed), so it pins to its core
    // but does not take real-time priority — the strategy is the only busy-spin
    // RT loop, which keeps the four vCPUs from being oversubscribed.
    let cpu_id: usize = env_or("CPU_CORE", 1);
    pin_only(cpu_id);
    println!("exchange pinned to CPU core {cpu_id}");

    let seed: u64 = env_or("SEED", 42);
    let vol: f64 = env_or("VOL", 6.0);
    // Microseconds between participant rounds; 0 = full speed.
    let tick_us: u64 = env_or("TICK_US", 100);

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))?;
    }

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    // Input: orders from the strategy.
    let order_svc = node
        .service_builder(&ORDER_SERVICE.try_into()?)
        .publish_subscribe::<OrderCommand>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let orders = order_svc.subscriber_builder().create()?;

    // How the exchange waits for the next order:
    //   poll    (default) — busy-poll `receive()` continuously. This is the
    //                       default because it wins on both latency and
    //                       throughput: measured on the c7i, poll and waitset had
    //                       the same tick-to-fill (~81us, so latency is a wash),
    //                       but poll processed several times more orders per run.
    //   waitset           — block on an order-event listener with a timeout equal
    //                       to the participant cadence, waking the instant an order
    //                       is notified. Same latency as poll but ~7x less CPU
    //                       (100% -> ~15%) at the cost of throughput — the choice
    //                       when the exchange is not the latency-critical path and
    //                       you want the core back.
    //
    // The latency floor here (~78us) is the same in both modes because it is the
    // cross-core visibility of a sparsely-written cache line, not the poll cadence;
    // neither poll nor waitset changes when the shared-memory write becomes visible.
    let order_wait = env_or::<String>("ORDER_WAIT", "poll".into());
    let use_order_waitset = order_wait == "waitset";
    let order_event = node
        .service_builder(&ORDER_EVENT.try_into()?)
        .event()
        .open_or_create()?;
    let order_listener = order_event.listener_builder().create()?;

    // Output: market ticks (top of book) + the event notify for WaitSet consumers.
    let market_svc = node
        .service_builder(&MARKET_SERVICE.try_into()?)
        .publish_subscribe::<MarketTick>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let ticks = market_svc.publisher_builder().create()?;
    let event = node
        .service_builder(&MARKET_EVENT.try_into()?)
        .event()
        .open_or_create()?;
    let notifier = event.notifier_builder().create()?;

    // Output: order-book snapshots (finally using OrderBookSnapshot).
    let book_svc = node
        .service_builder(&BOOK_SERVICE.try_into()?)
        .publish_subscribe::<OrderBookSnapshot>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let snapshots = book_svc.publisher_builder().create()?;

    // Output: execution reports (fills, partials, rejects) for the strategy's orders.
    let exec_svc = node
        .service_builder(&EXECUTION_SERVICE.try_into()?)
        .publish_subscribe::<ExecutionReport>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let reports = exec_svc.publisher_builder().create()?;

    println!("exchange: matching orders against a live book, publishing market data");
    println!("  in:   {}", ORDER_SERVICE);
    println!(
        "  out:  {} (ticks) / {} (book) / {} (fills)",
        MARKET_SERVICE, BOOK_SERVICE, EXECUTION_SERVICE
    );
    println!(
        "  seed={seed} vol={vol} tick={tick_us}us  order_wait={}",
        if use_order_waitset {
            "waitset (blocking)"
        } else {
            "poll (busy-spin)"
        }
    );
    println!("waiting 2s for the strategy to attach...");
    thread::sleep(Duration::from_secs(2));

    let mut book = Book::new();
    let mut participants = Participants::new(seed, vol);
    let fee_bps = 1.0_f64;

    // Reusable scratch for fills so the loop doesn't allocate a Vec per round.
    let mut fills: Vec<Fill> = Vec::with_capacity(64);
    let mut seq = 0u64;
    let mut last_bid = 0i64;
    let mut last_ask = 0i64;

    // Prime the book with a few participant rounds so it is two-sided before the
    // strategy's first order arrives.
    for _ in 0..64 {
        fills.clear();
        participants.act(&mut book, &mut fills);
    }

    // A real matcher matches on order *arrival*, not on a timer. So the hot loop
    // busy-polls incoming orders and matches them immediately; the synthetic
    // participants (the "market", not the matching path) run on a separate,
    // paced cadence. This keeps an order's time-to-fill down to the match plus
    // one shared-memory hop, instead of making it wait for the next paced round.
    //
    // The cadence is checked with `Instant` only when no order is pending — a
    // vDSO read off the tightest match path, not a syscall on it. tick_us == 0
    // means run participants every iteration (full speed, no pacing).
    let participant_period = Duration::from_micros(tick_us);
    let mut last_participant = Instant::now();

    while running.load(Ordering::Relaxed) {
        // 1) HOT: match any pending order right now. This is the latency path.
        let mut got_order = false;
        while let Some(sample) = orders.receive()? {
            got_order = true;
            let cmd: OrderCommand = *sample;
            fills.clear();
            match_strategy_order(&mut book, &cmd, &mut fills);
            publish_fills(&reports, &cmd, &fills, fee_bps)?;
        }

        // 2) PACED: run one round of synthetic market activity only when the
        // cadence has elapsed — this is the "market", not the matching path, so
        // it must not gate how fast an order fills.
        let due = tick_us == 0 || last_participant.elapsed() >= participant_period;
        if due {
            last_participant = Instant::now();
            fills.clear();
            participants.act(&mut book, &mut fills);
            // If the strategy is resting passive orders and a synthetic taker hits
            // one, the strategy is the maker on that fill — report those.
            publish_strategy_maker_fills(&reports, &fills, fee_bps)?;
        } else if !got_order {
            // Nothing to match and no round due. In poll mode, spin. In waitset
            // mode, block on the order-event listener until either an order is
            // notified (targeted wake-up — fills the moment the strategy sends) or
            // the participant cadence elapses (so the synthetic market still runs).
            if use_order_waitset {
                let until_next = participant_period.saturating_sub(last_participant.elapsed());
                let timeout = if until_next.is_zero() {
                    Duration::from_micros(1)
                } else {
                    until_next
                };
                let _ = order_listener.timed_wait_one(timeout);
            } else {
                std::hint::spin_loop();
            }
        }

        // 3) Publish top-of-book if it moved (after either an order match or a
        // participant round).
        let bid = book.best_bid().unwrap_or(0);
        let ask = book.best_ask().unwrap_or(0);
        if bid != last_bid || ask != last_ask {
            seq += 1;
            let mid = if bid > 0 && ask > 0 {
                (bid + ask) / 2
            } else {
                bid.max(ask)
            };
            let tick = MarketTick {
                timestamp_ns: rdtsc(),
                symbol_id: symbols::BTC_USDT,
                exchange_id: exchanges::BINANCE,
                tick_type: 0,
                padding1: [0; 2],
                price: mid,
                quantity: 100_000_000,
                bid,
                ask,
                volume_24h: 0,
                sequence: seq,
            };
            let s = ticks.loan_uninit()?;
            s.write_payload(tick).send()?;
            let _ = notifier.notify();

            let snap = OrderBookSnapshot {
                timestamp_ns: tick.timestamp_ns,
                symbol_id: symbols::BTC_USDT,
                exchange_id: exchanges::BINANCE,
                depth: (book.bid_level_count().min(255)) as u8,
                padding1: [0; 2],
                best_bid: bid,
                best_ask: ask,
                bid_size: book.size_at(Side::Buy, bid),
                ask_size: book.size_at(Side::Sell, ask),
                mid_price: mid,
                spread: (ask - bid).max(0),
            };
            let bs = snapshots.loan_uninit()?;
            bs.write_payload(snap).send()?;

            last_bid = bid;
            last_ask = ask;

            if seq.is_multiple_of(50_000) {
                println!(
                    "exchange: {} book updates, bid={:.2} ask={:.2}",
                    seq,
                    bid as f64 / SCALE,
                    ask as f64 / SCALE
                );
            }
        }
    }

    println!("exchange stopped after {seq} book updates");
    Ok(())
}

/// Match one strategy order against the book. Interprets `OrderCommand` fields:
/// action 1 = cancel, order_type 0 = market / 1 = limit.
fn match_strategy_order(book: &mut Book, cmd: &OrderCommand, fills: &mut Vec<Fill>) {
    let side = Side::from_u8(cmd.side);
    if cmd.action == 1 {
        // cancel; the outcome is reported by the caller via publish_fills' path,
        // but a cancel produces no fills, so we handle its report here inline.
        book.cancel(cmd.order_id);
        return;
    }
    // action 0 (new) or 2 (modify=cancel+replace)
    if cmd.action == 2 {
        book.cancel(cmd.order_id);
    }
    let is_limit = cmd.order_type == 1;
    book.submit(
        cmd.order_id,
        side,
        is_limit,
        cmd.price_ticks,
        cmd.quantity,
        cmd.origin_ts,
        false, // strategy order
        fills,
    );
}

/// Publish an ExecutionReport per fill that belongs to the taker strategy order,
/// carrying origin_ts-correlation downstream (execution correlates by order_id).
fn publish_fills(
    reports: &iceoryx2::port::publisher::Publisher<ipc::Service, ExecutionReport, ()>,
    cmd: &OrderCommand,
    fills: &[Fill],
    fee_bps: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    for f in fills {
        if f.taker_is_synthetic {
            continue; // only the strategy's own taker fills are reported
        }
        let px = f.price_ticks as f64 / SCALE;
        let qty = f.quantity as f64 / SCALE;
        let fee = px * qty * (fee_bps / 10_000.0);
        let status = if f.taker_remaining_after == 0 { 2 } else { 1 };
        let report = ExecutionReport {
            timestamp_ns: rdtsc(),
            order_id: f.taker_order_id,
            exchange_order_id: f.taker_order_id,
            executed_price: f.price_ticks,
            executed_quantity: f.quantity,
            remaining_quantity: f.taker_remaining_after,
            commission: (fee * SCALE) as i64,
            status,
            reject_reason: 0,
            padding: [0; 6],
        };
        let out = reports.loan_uninit()?;
        out.write_payload(report).send()?;
    }
    let _ = cmd;
    Ok(())
}

/// When the strategy rests a passive order (PASSIVE mode) and a synthetic taker
/// later hits it, the strategy is the *maker* — report those fills too so its P&L
/// and tick-to-fill still land.
fn publish_strategy_maker_fills(
    reports: &iceoryx2::port::publisher::Publisher<ipc::Service, ExecutionReport, ()>,
    fills: &[Fill],
    fee_bps: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    for f in fills {
        if f.maker_is_synthetic {
            continue; // only strategy makers get a report
        }
        let px = f.price_ticks as f64 / SCALE;
        let qty = f.quantity as f64 / SCALE;
        let fee = px * qty * (fee_bps / 10_000.0);
        let report = ExecutionReport {
            timestamp_ns: rdtsc(),
            order_id: f.maker_order_id,
            exchange_order_id: f.maker_order_id,
            executed_price: f.price_ticks,
            executed_quantity: f.quantity,
            remaining_quantity: 0, // maker fill; remaining tracked by execution's map
            commission: (fee * SCALE) as i64,
            status: 2,
            reject_reason: 0,
            padding: [0; 6],
        };
        let out = reports.loan_uninit()?;
        out.write_payload(report).send()?;
    }
    Ok(())
}
