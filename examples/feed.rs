//! Market-data feed: stage 1 of the trading pipeline.
//!
//! Publishes a stream of `MarketTick`s on the market service. Prices follow a
//! seeded regime-switching model — alternating trending and choppy stretches
//! over a mild pull toward a fair value — so the series is reproducible run to
//! run and has real structure (trends and reversions) for the strategy to react
//! to. In a real deployment this process would wrap an exchange websocket
//! adapter; the rest of the pipeline is identical either way.
//!
//! Run with:  CPU_CORE=1 TICK_US=100 cargo run --release --example feed

use core_affinity::CoreId;
use iceoryx2::prelude::*;
use std::{env, thread, time::Duration};

use rust_hotpath_ipc::hot_path::*;

/// A tiny deterministic PRNG (xorshift64*) so we depend on no rng crate and the
/// series is reproducible from a seed.
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpu_id: usize = env::var("CPU_CORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if core_affinity::set_for_current(CoreId { id: cpu_id }) {
        println!("feed pinned to CPU core {}", cpu_id);
    }

    let seed: u64 = env::var("SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(42);
    // Microseconds between ticks; 0 = full speed. A realistic feed is paced, so
    // the default gives a readable ~10k ticks/sec.
    let tick_us: u64 = env::var("TICK_US")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let service = node
        .service_builder(&MARKET_SERVICE.try_into()?)
        .publish_subscribe::<MarketTick>()
        .enable_safe_overflow(true)
        .max_subscribers(8)
        .max_publishers(1)
        .history_size(16)
        .open_or_create()?;
    let publisher = service.publisher_builder().create()?;

    println!(
        "feed publishing MarketTick on '{}' (seed={}, tick={}us)",
        MARKET_SERVICE, seed, tick_us
    );
    println!("waiting 2s for the strategy to attach...");
    thread::sleep(Duration::from_secs(2));

    // Regime-switching price model in fixed-point (1e8 scale): the series
    // alternates between trending regimes (a persistent drift) and choppy
    // mean-reverting regimes, so there is real structure for the strategy to
    // trade rather than pure noise around a fixed level. VOL scales the shocks.
    let mut rng = Rng::new(seed);
    let mut price = 50_000.0_f64; // e.g. BTC-USD
    let vol: f64 = env::var("VOL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6.0);

    // Regime state: drift (price units per tick) and how long it lasts.
    let mut drift = 0.0_f64;
    let mut regime_left: u64 = 0;

    let mut seq = 0u64;
    loop {
        seq += 1;

        // Occasionally pick a new regime: a trend (up or down) or a flat/choppy
        // stretch. Trends have a steady drift; flat stretches drift ~0.
        if regime_left == 0 {
            let roll = rng.unit();
            drift = if roll < 0.35 {
                vol * (0.15 + 0.25 * rng.unit()) // up-trend
            } else if roll < 0.70 {
                -vol * (0.15 + 0.25 * rng.unit()) // down-trend
            } else {
                0.0 // choppy / flat
            };
            regime_left = 2_000 + (rng.next_u64() % 6_000); // 2k-8k ticks
        }
        regime_left -= 1;

        // dP = drift + vol * N(0,1), with a gentle pull back toward a wide band
        // so price stays in a sane range over long runs.
        let pull = (50_000.0 - price) * 0.0005;
        price += drift + pull + vol * rng.normal();
        if price < 1.0 {
            price = 1.0;
        }

        let tick = MarketTick {
            timestamp_ns: rdtsc(),
            symbol_id: symbols::BTC_USDT,
            exchange_id: exchanges::BINANCE,
            tick_type: 0, // trade
            padding1: [0; 2],
            price: (price * 100_000_000.0) as i64, // fixed-point 1e8
            quantity: 100_000_000,                 // 1.0 unit
            bid: ((price - 0.5) * 100_000_000.0) as i64,
            ask: ((price + 0.5) * 100_000_000.0) as i64,
            volume_24h: 0,
            sequence: seq,
        };

        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(tick);
        sample.send()?;

        if seq % 50_000 == 0 {
            println!("feed: {} ticks, price={:.2}", seq, price);
        }

        if tick_us > 0 {
            thread::sleep(Duration::from_micros(tick_us));
        } else {
            std::hint::spin_loop();
        }
    }
}
