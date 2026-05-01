//! Conflict-detection state for snapshot-isolated transactions.
//!
//! Lives in its own module so the cooperating pieces (a monotonic
//! `write_seq`, the `key -> last seq` map, and the in-flight
//! `snapshot_seq` multiset) sit behind a single API. The engine
//! delegates all conflict bookkeeping here; transactional and plain
//! writes both flow through `record_write` and `check_conflict`,
//! which keeps the (bump `write_seq`, insert `last_writes`) pair
//! atomic to any reader and lets `maybe_trim` honour every in-flight
//! snapshot without the engine having to choreograph three locks by
//! hand.
//!
//! The module deliberately uses `std::sync::Mutex` rather than
//! `parking_lot`, because [`loom`](https://docs.rs/loom) shadows the
//! `std::sync` primitives under `--cfg loom` to model-check every
//! legal interleaving of the spawned threads. The critical sections
//! are short (one `BTreeMap` operation) so the difference vs.
//! `parking_lot` is below the noise floor of the surrounding I/O.
//!
//! Run the loom suite with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test -p lattice-loom-tests --release
//! ```

// The module is declared `pub` under `--cfg loom` so the
// `lattice-loom-tests` crate can drive the tracker, and `pub(crate)`
// otherwise. The `pub` items below are deliberately reachable from
// outside the crate under loom; the `unreachable_pub` lint cannot
// see that and would otherwise fire on every default build.
#![allow(unreachable_pub)]

use std::collections::BTreeMap;

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::sync::atomic::AtomicU64;

#[cfg(not(loom))]
use std::sync::Mutex;
#[cfg(not(loom))]
use std::sync::atomic::AtomicU64;

use std::sync::atomic::Ordering;

/// Trim threshold: when `last_writes` exceeds this many entries,
/// `maybe_trim` runs and drops entries whose `seq` cannot trigger a
/// conflict for any in-flight or future transaction. The threshold
/// is sized to amortise the trim cost over many writes; setting it
/// too low forces frequent O(n) sweeps, too high lets the map grow
/// unbounded between trims.
pub const LAST_WRITES_TRIM_THRESHOLD: usize = 1024;

/// Snapshot-isolation conflict tracker.
///
/// Owns three pieces of state cooperating under a single API:
///
/// - `write_seq`: monotonic per-write counter, bumped once per
///   logical write. Transactions capture its value at start
///   (`snapshot_seq`) and use it to decide whether a later observed
///   `last_writes` entry counts as a conflict.
/// - `last_writes`: `key -> last seq` map. Updated atomically with
///   the `write_seq` bump so any reader holding the inner lock sees
///   a consistent (`write_seq`, `last_writes`) pair.
/// - `active_tx`: multiset of in-flight `snapshot_seq` values. Pins
///   `last_writes` entries against trimming for as long as some
///   transaction might still need them.
#[derive(Debug, Default)]
pub struct ConflictTracker {
    write_seq: AtomicU64,
    /// Holds the `last_writes` map. Writes (`record_write`) and
    /// reads (`check_conflict`, `maybe_trim`) both take this lock.
    /// Co-located with the `write_seq` bump so the pair is atomic
    /// to any reader.
    inner: Mutex<TrackerInner>,
    /// `snapshot_seq -> count` multiset of in-flight transactions.
    /// Acquired briefly at `begin_tx` and `end_tx`, plus once at
    /// `maybe_trim` to compute the trim cutoff.
    active_tx: Mutex<BTreeMap<u64, usize>>,
}

#[derive(Debug, Default)]
struct TrackerInner {
    last_writes: BTreeMap<Vec<u8>, u64>,
}

impl ConflictTracker {
    /// Construct an empty tracker. `write_seq` starts at zero, both
    /// maps empty.
    #[must_use]
    pub fn new() -> Self {
        Self {
            write_seq: AtomicU64::new(0),
            inner: Mutex::new(TrackerInner::default()),
            active_tx: Mutex::new(BTreeMap::new()),
        }
    }

    /// Borrow the underlying `write_seq` atomic. Exposed only for
    /// the loom suite, which needs to observe the counter at
    /// specific points relative to the tracker's internal lock.
    /// The production path goes through `begin_tx` and never reads
    /// `write_seq` directly.
    #[cfg(loom)]
    #[must_use]
    pub const fn write_seq(&self) -> &AtomicU64 {
        &self.write_seq
    }

    /// Register a new in-flight transaction. Captures the current
    /// `write_seq` as the transaction's `snapshot_seq` and bumps the
    /// `active_tx` multiset entry, atomically under the same lock.
    /// The returned value must be paired with a later `end_tx` call;
    /// the engine wraps this in a guard whose `Drop` deregisters.
    pub fn begin_tx(&self) -> u64 {
        let mut active = self.active_tx.lock().unwrap();
        let seq = self.write_seq.load(Ordering::Acquire);
        *active.entry(seq).or_insert(0) += 1;
        seq
    }

    /// Deregister a transaction whose `snapshot_seq` was previously
    /// returned by `begin_tx`. Decrements the multiset entry and
    /// removes it once the count reaches zero so `maybe_trim` can
    /// move the cutoff forward.
    pub fn end_tx(&self, snapshot_seq: u64) {
        let mut active = self.active_tx.lock().unwrap();
        if let Some(count) = active.get_mut(&snapshot_seq) {
            *count -= 1;
            if *count == 0 {
                active.remove(&snapshot_seq);
            }
        }
    }

    /// Atomically bump `write_seq` and insert `key -> new_seq` into
    /// `last_writes`. Returns the new sequence.
    ///
    /// The two operations run under the inner lock, so any other
    /// thread that takes the same lock sees both effects together
    /// (or neither). This is the invariant validated by the loom
    /// test `record_write_pair_is_visible_atomically`.
    pub fn record_write(&self, key: Vec<u8>) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let new_seq = self.write_seq.fetch_add(1, Ordering::AcqRel) + 1;
        inner.last_writes.insert(key, new_seq);
        new_seq
    }

    /// Return `true` when at least one key produced by `keys` has
    /// a recorded `last_seq` greater than `snapshot_seq`. Drives
    /// the transaction commit's conflict check. Takes an iterator
    /// of borrows so the caller can chain its read and write sets
    /// without allocating an intermediate `Vec`.
    pub fn check_conflict<'a, I>(&self, snapshot_seq: u64, keys: I) -> bool
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let inner = self.inner.lock().unwrap();
        for key in keys {
            if let Some(&last_seq) = inner.last_writes.get(key)
                && last_seq > snapshot_seq
            {
                return true;
            }
        }
        false
    }

    /// Drop entries from `last_writes` whose `seq` cannot trigger a
    /// conflict for any in-flight or future transaction. A no-op
    /// when the map is below `LAST_WRITES_TRIM_THRESHOLD`.
    ///
    /// Cutoff = the smallest `snapshot_seq` of any in-flight
    /// transaction, or the current `write_seq` when no transaction
    /// is in flight. Retains entries strictly greater than the
    /// cutoff because the conflict check fires when
    /// `entry.seq > tx.snapshot_seq` and any retained entry must
    /// therefore stay visible for at least the oldest transaction.
    pub fn maybe_trim(&self) {
        if self.inner.lock().unwrap().last_writes.len() <= LAST_WRITES_TRIM_THRESHOLD {
            return;
        }
        let cutoff = {
            let active = self.active_tx.lock().unwrap();
            active
                .keys()
                .next()
                .copied()
                .unwrap_or_else(|| self.write_seq.load(Ordering::Acquire))
        };
        let mut inner = self.inner.lock().unwrap();
        inner.last_writes.retain(|_, seq| *seq > cutoff);
    }

    /// Return the number of entries currently held in `last_writes`.
    /// Used by in-crate tests to verify trim behaviour; the
    /// production path never reads it.
    #[cfg(any(test, loom))]
    #[must_use]
    pub fn last_writes_len(&self) -> usize {
        self.inner.lock().unwrap().last_writes.len()
    }

    /// Run the trim pass unconditionally, ignoring the size
    /// threshold. Used by the loom suite so a two-thread test does
    /// not need to push thousands of writes through the tracker
    /// to trigger a trim; production never calls this.
    #[cfg(loom)]
    pub fn force_trim(&self) {
        let cutoff = {
            let active = self.active_tx.lock().unwrap();
            active
                .keys()
                .next()
                .copied()
                .unwrap_or_else(|| self.write_seq.load(Ordering::Acquire))
        };
        let mut inner = self.inner.lock().unwrap();
        inner.last_writes.retain(|_, seq| *seq > cutoff);
    }

    /// Return the recorded `last_seq` for `key`, or `None` if no
    /// entry exists. Used by the loom suite to verify trim safety.
    #[cfg(loom)]
    #[must_use]
    pub fn last_seq_for(&self, key: &[u8]) -> Option<u64> {
        self.inner.lock().unwrap().last_writes.get(key).copied()
    }
}
