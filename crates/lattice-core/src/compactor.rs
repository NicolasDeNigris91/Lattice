//! Background compactor: non-blocking compaction state machine.
//!
//! `Lattice::compact()` ran the merge on the caller's thread until
//! v1.13. The whole-round wall-clock cost (snapshot, I/O, manifest
//! rewrite, orphan unlink) sat directly on top of any caller's
//! tail latency, and `flush()`'s implicit auto-compaction trigger
//! propagated the same cost to plain writers whenever a level
//! crossed its threshold.
//!
//! v1.13 hoists the round onto a dedicated background thread,
//! spawned at `Lattice::open` time and joined when the last
//! `Arc<Inner>` drops. `Lattice::compact_async()` bumps a
//! generation counter and notifies the worker; the returned
//! `CompactionHandle` carries that generation, and `wait()`
//! blocks on a condvar until the worker reports a `completed`
//! generation that has caught up.
//!
//! ## Coalescing
//!
//! Multiple concurrent `compact_async()` calls coalesce. The
//! worker captures the latest `requested` generation when it
//! wakes, runs as many rounds as the level layout requires (one
//! per pass over the LSM until every level is below the
//! threshold), then publishes that captured generation as
//! `completed`. Every caller whose handle's `target_generation`
//! is no greater than the captured value sees `wait()` return.
//!
//! ## Errors
//!
//! A failed round is sticky: `last_error` records it, every
//! pending `wait()` returns the cloned error, and the next
//! successful round clears the slot. The error type is `String`
//! so `Clone` works without forcing every `lattice_core::Error`
//! variant to carry that bound.
//!
//! ## Loom
//!
//! The state machine is exercised by the loom suite under
//! `lattice-loom-tests`; see `tests/loom_compactor.rs` for the
//! invariants pinned (no waiter sees `completed > requested`,
//! shutdown drains all pending waiters, no thread leak).

// The parent module is declared `pub` under `--cfg loom` so the
// `lattice-loom-tests` crate can drive the state machine, and
// `pub(crate)` otherwise. The `pub` items below are deliberately
// reachable from outside the crate under loom; the
// `unreachable_pub` lint cannot see that and would otherwise
// fire on every default build.
#![allow(unreachable_pub)]

#[cfg(loom)]
use loom::sync::atomic::AtomicU64;
#[cfg(loom)]
use loom::sync::{Condvar, Mutex};

#[cfg(not(loom))]
use parking_lot::{Condvar, Mutex};
#[cfg(not(loom))]
use std::sync::atomic::AtomicU64;

// `Arc` is *not* swapped under `--cfg loom`. The engine's `Inner`
// always holds a `std::sync::Arc<CompactorShared>`, and the loom
// suite wraps a fresh `CompactorShared` in `loom::sync::Arc`
// itself before spawning threads. Mixing the two would force
// every call site through a typedef; constructing the shared
// state without an outer `Arc` keeps the API one-size-fits-both.
use std::sync::Arc;

use crate::error::{Error, Result};

/// Shared state between every `Lattice` handle and the background
/// compactor thread. Wrapped in an `Arc` so the thread can hold a
/// `Weak` to it (and exit when the last `Arc<Inner>` drops).
#[derive(Debug)]
pub struct CompactorShared {
    /// Mutex-guarded counters and error slot. `parking_lot::Mutex`
    /// in production builds; `loom::sync::Mutex` under `--cfg loom`
    /// so the loom suite can shadow it.
    state: Mutex<CompactorState>,
    /// Wakes the worker when a new request lands and wakes every
    /// `wait()` caller when a round completes.
    cv: Condvar,
    /// Mirror of `state.requested_generation` for the lockless
    /// "is there work to do?" check the worker does on the hot
    /// path. Reads are `Acquire`; the writer holds `state` and
    /// publishes with `Release`.
    pub latest_request: AtomicU64,
}

#[derive(Debug, Default)]
struct CompactorState {
    /// Bumped by every `Lattice::compact_async` call.
    requested_generation: u64,
    /// Updated by the worker after each round; equal to whatever
    /// `requested_generation` was at the start of the round.
    completed_generation: u64,
    /// Sticky error from the most recent failed round. Cleared on
    /// the next successful round; cloned out by `wait()`.
    last_error: Option<String>,
    /// Set to `true` by `Inner::Drop` to ask the worker to exit.
    shutdown: bool,
}

impl CompactorShared {
    /// Construct an unwrapped instance. The caller wraps it in
    /// whichever `Arc` flavour they need (`std::sync::Arc` for the
    /// engine; `loom::sync::Arc` for the loom suite).
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CompactorState::default()),
            cv: Condvar::new(),
            latest_request: AtomicU64::new(0),
        }
    }

    /// Schedule a new compaction round. Bumps the request
    /// generation under the state lock, publishes the new value
    /// to `latest_request`, and wakes the worker.
    pub fn schedule(&self) -> u64 {
        let generation = {
            let mut state = lock(&self.state);
            state.requested_generation += 1;
            state.requested_generation
        };
        self.latest_request
            .store(generation, std::sync::atomic::Ordering::Release);
        self.cv.notify_all();
        generation
    }

    /// Block until `completed_generation >= target` OR the
    /// compactor is shutting down. Returns the most recent error
    /// from a failed round, if any. The error is left in place so
    /// other waiters at the same generation see it; the next
    /// successful round clears it.
    pub fn wait_for(&self, target: u64) -> Result<()> {
        let mut state = lock(&self.state);
        while !state.shutdown && state.completed_generation < target {
            #[cfg(not(loom))]
            self.cv.wait(&mut state);
            #[cfg(loom)]
            {
                state = self.cv.wait(state).unwrap();
            }
        }
        let err = state.last_error.clone();
        drop(state);
        if let Some(err) = err {
            return Err(Error::Compaction(err));
        }
        Ok(())
    }

    /// Worker entry point: wait for a request, return the captured
    /// generation. Returns `None` when the compactor is shutting
    /// down and the worker should exit.
    pub fn next_request(&self) -> Option<u64> {
        let mut state = lock(&self.state);
        while !state.shutdown && state.requested_generation == state.completed_generation {
            #[cfg(not(loom))]
            self.cv.wait(&mut state);
            #[cfg(loom)]
            {
                state = self.cv.wait(state).unwrap();
            }
        }
        if state.shutdown {
            return None;
        }
        Some(state.requested_generation)
    }

    /// Worker exit point: publish the captured generation as
    /// completed, store any error, wake every waiter. Drops the
    /// state guard before notifying so woken waiters do not
    /// immediately re-block on the lock we are about to release.
    pub fn finish(&self, target: u64, result: Result<()>) {
        {
            let mut state = lock(&self.state);
            if state.completed_generation < target {
                state.completed_generation = target;
            }
            match result {
                Ok(()) => state.last_error = None,
                Err(err) => state.last_error = Some(err.to_string()),
            }
        }
        self.cv.notify_all();
    }

    /// Ask the worker to exit at the next loop boundary. Wakes
    /// every waiter so an in-flight `wait()` does not block forever
    /// after the worker thread is gone. Same drop-before-notify
    /// pattern as `finish`.
    pub fn shutdown(&self) {
        {
            let mut state = lock(&self.state);
            state.shutdown = true;
        }
        self.cv.notify_all();
    }
}

#[cfg(not(loom))]
fn lock<T>(m: &Mutex<T>) -> parking_lot::MutexGuard<'_, T> {
    m.lock()
}

#[cfg(loom)]
fn lock<T>(m: &Mutex<T>) -> loom::sync::MutexGuard<'_, T> {
    m.lock().unwrap()
}

/// Handle to a scheduled compaction round.
///
/// Returned by [`crate::Lattice::compact_async`]. Carries the
/// generation number captured at scheduling time; `wait()` blocks
/// until the background worker reports a `completed_generation`
/// at least that high.
///
/// Cheap to drop without waiting; the round still runs in the
/// background and its result is observed by any subsequent
/// `wait()`-style call.
#[derive(Debug)]
pub struct CompactionHandle {
    pub(crate) shared: Arc<CompactorShared>,
    pub(crate) target_generation: u64,
}

impl CompactionHandle {
    /// Block until the scheduled round completes. Returns
    /// `Err(Error::Compaction(...))` if the round (or any later
    /// round whose error has not yet been cleared) failed.
    pub fn wait(self) -> Result<()> {
        self.shared.wait_for(self.target_generation)
    }

    /// Generation number captured when the round was scheduled.
    /// Exposed for diagnostics and tests; production callers do
    /// not need it.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.target_generation
    }
}
