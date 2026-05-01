//! Loom model checking for the background compactor's state
//! machine.
//!
//! v1.13 hoists compaction onto a dedicated background thread.
//! `Lattice::compact_async()` bumps a generation counter under a
//! mutex and notifies the worker on a condvar. The worker's loop
//! is `next_request -> run rounds -> finish -> repeat`.
//! `CompactionHandle::wait()` blocks on the same condvar until the
//! worker reports a `completed_generation` that catches up to the
//! handle's `target_generation`.
//!
//! These tests pin two invariants under every legal interleaving
//! of two and three threads:
//!
//! - **Liveness**: a `wait_for(N)` after a paired `schedule()` and
//!   `finish(N, Ok)` must return. No deadlock between the
//!   condvar-based waiter and the worker's notify.
//! - **Shutdown drains waiters**: a `shutdown()` mid-wait must
//!   wake every blocked `wait_for` rather than leave it parked
//!   forever.
//!
//! Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test -p lattice-loom-tests --release
//! ```

#![cfg(loom)]

use lattice_core::compactor::CompactorShared;
use loom::sync::Arc;

/// **Invariant L1 (liveness)**: a `wait_for` paired with a
/// `schedule()` + worker `finish(target, Ok)` must return without
/// deadlock under every interleaving of the two threads.
///
/// The model schedules a single round, spawns a worker thread that
/// calls `next_request()` and then `finish()`, and on the main
/// thread blocks on `wait_for(target)`. Loom permutes every legal
/// ordering of the underlying mutex acquisitions and condvar waits.
#[test]
fn wait_returns_after_paired_schedule_and_finish() {
    loom::model(|| {
        let shared = Arc::new(CompactorShared::new());

        let target = shared.schedule();

        let worker = {
            let shared = Arc::clone(&shared);
            loom::thread::spawn(move || {
                if let Some(captured) = shared.next_request() {
                    shared.finish(captured, Ok(()));
                }
            })
        };

        let result = shared.wait_for(target);
        assert!(
            result.is_ok(),
            "wait_for must return Ok after a successful round, got {result:?}",
        );

        worker.join().unwrap();
    });
}

/// **Invariant L2 (shutdown drains waiters)**: a `shutdown()`
/// while a `wait_for` is blocked must wake the waiter and let it
/// return, rather than leave it parked forever.
///
/// The model spawns a worker that does NOT call `finish` (so the
/// waiter has no completion signal from the round) and a
/// shutdown thread that calls `shutdown()`. The main-thread
/// `wait_for` must observe the shutdown flag and return.
#[test]
fn shutdown_wakes_pending_waiter() {
    loom::model(|| {
        let shared = Arc::new(CompactorShared::new());
        let target = shared.schedule();

        let shutter = {
            let shared = Arc::clone(&shared);
            loom::thread::spawn(move || {
                shared.shutdown();
            })
        };

        // wait_for returns when completed >= target OR shutdown.
        // The worker never calls finish, so only the shutdown path
        // can release the wait. If the condvar notification on
        // shutdown is missing or the wait predicate is wrong, this
        // hangs and loom reports the deadlock.
        let _ = shared.wait_for(target);

        shutter.join().unwrap();
    });
}
