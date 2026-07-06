//! Small runtime helpers shared by the process binaries: reading tunables from
//! the environment, and placing a thread on a dedicated core at real-time
//! priority.
//!
//! These are pulled out of the individual `main`s so the setup ceremony lives in
//! one audited place — in particular the one `unsafe` real-time-scheduling call,
//! which every hot-loop process needs and which is easy to get subtly wrong if
//! it is copy-pasted around.

use std::str::FromStr;

/// Read an environment variable, parse it, and fall back to `default` if it is
/// unset or unparseable. Replaces the `env::var(k).ok().and_then(..).unwrap_or(..)`
/// dance that otherwise repeats in every binary.
///
/// ```
/// # use rust_hotpath_ipc::runtime::env_or;
/// let core: usize = env_or("CPU_CORE", 1);
/// ```
pub fn env_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Default real-time priority for a pinned hot loop. High enough that ordinary
/// work will not preempt it, below the very top of the `SCHED_FIFO` band so it
/// does not contend with kernel threads.
pub const DEFAULT_RT_PRIORITY: i32 = 90;

/// Pin the current thread to `core` and, on Linux, raise it to `SCHED_FIFO`
/// real-time priority. `label` names the process in the log line so the pipeline
/// output reads `feed pinned to CPU core 1`, etc.
///
/// This is the single most important setup step for a predictable tail: pinning
/// keeps the thread's working set warm in one core's cache, and `SCHED_FIFO`
/// stops the scheduler from preempting it mid-cycle for ordinary work. Failure
/// to raise priority is not fatal (it just needs privilege) — we log and carry
/// on, since the demo is still useful without it.
///
/// The reporter/aggregation thread is deliberately *not* pinned or prioritized
/// through here — it does I/O and must stay preemptible on its own core; see
/// [`pin_only`].
pub fn pin_and_prioritize(core: usize, label: &str) {
    if core_affinity::set_for_current(core_affinity::CoreId { id: core }) {
        println!("{label} pinned to CPU core {core}");
    } else {
        println!("{label} failed to pin to CPU core {core} (try with sudo)");
    }
    set_realtime_priority(label);
}

/// Pin the current thread to `core` without changing its scheduling priority.
/// Used for the off-hot-path reporter thread, which must remain preemptible.
pub fn pin_only(core: usize) {
    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: core });
}

#[cfg(target_os = "linux")]
fn set_realtime_priority(label: &str) {
    // SAFETY: `sched_setscheduler` with a valid `sched_param` on the current
    // thread (pid 0) is sound; it either succeeds or returns an error we handle.
    unsafe {
        let param = libc::sched_param {
            sched_priority: DEFAULT_RT_PRIORITY,
        };
        if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) == 0 {
            println!("{label} engaged SCHED_FIFO priority {DEFAULT_RT_PRIORITY}");
        } else {
            println!("{label} failed to set SCHED_FIFO (need CAP_SYS_NICE / sudo?)");
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_realtime_priority(_label: &str) {
    // SCHED_FIFO is Linux-only; on other platforms pinning alone is all we do.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_or_falls_back_when_unset() {
        // A key we are confident is not set in the test environment.
        let v: usize = env_or("RUST_HOTPATH_DEFINITELY_UNSET_XYZ", 7);
        assert_eq!(v, 7);
    }

    #[test]
    fn env_or_parses_when_set() {
        std::env::set_var("RUST_HOTPATH_TEST_CORE", "5");
        let v: usize = env_or("RUST_HOTPATH_TEST_CORE", 1);
        assert_eq!(v, 5);
        std::env::remove_var("RUST_HOTPATH_TEST_CORE");
    }

    #[test]
    fn env_or_falls_back_on_garbage() {
        std::env::set_var("RUST_HOTPATH_TEST_GARBAGE", "not-a-number");
        let v: usize = env_or("RUST_HOTPATH_TEST_GARBAGE", 3);
        assert_eq!(v, 3);
        std::env::remove_var("RUST_HOTPATH_TEST_GARBAGE");
    }
}
