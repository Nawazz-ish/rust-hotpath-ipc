// Ultra-low-latency order service on the Iceoryx2 hot path.
//
// Subscribes to order commands, processes each with no allocation and no
// database access, and publishes an execution report. The processing thread
// pins itself to a dedicated core and requests real-time (SCHED_FIFO)
// scheduling so it is not preempted by ordinary work.

use crate::hot_path::*;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

/// Hot-path order-management service.
pub struct HotPathOrderService {
    order_subscriber: Option<Subscriber<ipc::Service, OrderCommand, ()>>,
    execution_publisher: Option<Publisher<ipc::Service, ExecutionReport, ()>>,
    is_running: Arc<AtomicBool>,
    // Used for thread pinning on Linux; harmless elsewhere.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    cpu_core: usize,
}

impl HotPathOrderService {
    pub fn new(cpu_core: usize) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            order_subscriber: None,
            execution_publisher: None,
            is_running: Arc::new(AtomicBool::new(false)),
            cpu_core,
        })
    }

    /// Pin the thread, raise scheduling priority, and wire up the Iceoryx2
    /// subscriber/publisher pair.
    pub fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Initializing hot-path order service");

        // Pin this thread to a dedicated core so it owns an L1/L2 and is not
        // migrated across sockets.
        #[cfg(target_os = "linux")]
        {
            unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_ZERO(&mut set);
                libc::CPU_SET(self.cpu_core, &mut set);
                let result =
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                if result == 0 {
                    println!("Pinned to CPU core {}", self.cpu_core);
                } else {
                    println!(
                        "Failed to pin to CPU core {} (try with sudo)",
                        self.cpu_core
                    );
                }
            }
        }

        // Real-time priority so the OS scheduler will not preempt the hot loop.
        #[cfg(target_os = "linux")]
        {
            unsafe {
                let param = libc::sched_param {
                    sched_priority: 98, // below the market-data feed, above normal work
                };
                let result = libc::sched_setscheduler(0, libc::SCHED_FIFO, &param);
                if result == 0 {
                    println!("Real-time priority set (SCHED_FIFO, priority 98)");
                } else {
                    println!("Failed to set real-time priority (need root)");
                }
            }
        }

        let node = NodeBuilder::new().create::<ipc::Service>()?;

        // Subscribe to order commands (config must match the strategy side).
        let order_service = node
            .service_builder(&ORDER_SERVICE.try_into()?)
            .publish_subscribe::<OrderCommand>()
            .enable_safe_overflow(true)
            .max_subscribers(8)
            .max_publishers(16)
            .history_size(10)
            .open_or_create()?;
        self.order_subscriber = Some(order_service.subscriber_builder().create()?);

        // Publish execution reports.
        let execution_service = node
            .service_builder(&EXECUTION_SERVICE.try_into()?)
            .publish_subscribe::<ExecutionReport>()
            .enable_safe_overflow(true)
            .max_subscribers(32)
            .max_publishers(1)
            .history_size(50)
            .open_or_create()?;
        self.execution_publisher = Some(execution_service.publisher_builder().create()?);

        println!("Iceoryx2 order processing initialized");
        println!("  order service:     {}", ORDER_SERVICE);
        println!("  execution service: {}", EXECUTION_SERVICE);
        Ok(())
    }

    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.order_subscriber.is_none() || self.execution_publisher.is_none() {
            return Err("service not initialized: call initialize() first".into());
        }
        self.is_running.store(true, Ordering::Relaxed);
        println!("Starting hot-path order service (target: sub-100ns processing)");
        self.run_hot_path_loop()
    }

    pub fn stop(&self) {
        println!("Stopping order service");
        self.is_running.store(false, Ordering::Relaxed);
    }

    /// Main hot loop: poll for a command, process it, publish the report. Zero
    /// allocation per iteration.
    fn run_hot_path_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let subscriber = self
            .order_subscriber
            .as_ref()
            .ok_or("order subscriber not initialized")?;
        let publisher = self
            .execution_publisher
            .as_ref()
            .ok_or("execution publisher not initialized")?;

        let mut processed_orders = 0u64;
        let mut processing_cycles_total = 0u64;
        let start_time = Instant::now();

        println!("Hot-path order loop started");

        while self.is_running.load(Ordering::Relaxed) {
            let poll_start = rdtsc();

            if let Some(sample) = subscriber.receive()? {
                let order_command = sample.payload();
                let execution_report = self.process_order_hot_path(order_command);

                let publish_sample = publisher.loan_uninit()?;
                let publish_sample = publish_sample.write_payload(execution_report);
                publish_sample.send()?;

                let poll_end = rdtsc();
                processing_cycles_total += poll_end.wrapping_sub(poll_start);
                processed_orders += 1;

                if processed_orders % 10_000 == 0 {
                    let avg_cycles = processing_cycles_total / processed_orders;
                    let avg_ns = crate::tsc_calibration::fast_cycles_to_ns(avg_cycles);
                    let elapsed = start_time.elapsed();
                    let orders_per_sec = processed_orders as f64 / elapsed.as_secs_f64();

                    println!(
                        "after {} orders ({:.1}s): avg {} cycles ({} ns), {:.0} orders/s",
                        processed_orders,
                        elapsed.as_secs_f64(),
                        avg_cycles,
                        avg_ns,
                        orders_per_sec
                    );

                    processing_cycles_total = 0;
                    processed_orders = 0;
                }
            } else {
                std::hint::spin_loop();
            }
        }

        println!("Hot-path order loop stopped");
        Ok(())
    }

    /// Process one order command. In a full system this is where pre-computed
    /// risk limits, routing, and exchange submission live; here it simulates an
    /// immediate fill to keep the transport path measurable in isolation.
    fn process_order_hot_path(&self, order: &OrderCommand) -> ExecutionReport {
        let process_end = rdtsc();
        ExecutionReport {
            timestamp_ns: process_end,
            order_id: order.order_id,
            exchange_order_id: order.order_id,
            executed_price: order.price_ticks,
            executed_quantity: order.quantity,
            remaining_quantity: 0,
            commission: 0,
            status: 2, // filled
            reject_reason: 0,
            padding: [0; 6],
        }
    }
}

/// Install a Ctrl-C handler that flips the returned flag to `false`.
pub fn setup_signal_handlers() -> Arc<AtomicBool> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("received Ctrl-C, stopping");
        r.store(false, Ordering::SeqCst);
    })
    .expect("failed to set Ctrl-C handler");
    running
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_command_is_one_cache_line() {
        assert_eq!(std::mem::size_of::<OrderCommand>(), 64);
        assert_eq!(std::mem::align_of::<OrderCommand>(), 64);
    }

    #[test]
    fn execution_report_is_one_cache_line() {
        assert_eq!(std::mem::size_of::<ExecutionReport>(), 64);
        assert_eq!(std::mem::align_of::<ExecutionReport>(), 64);
    }

    #[test]
    fn service_creation_succeeds() {
        assert!(HotPathOrderService::new(3).is_ok());
    }
}
