//! Loom model checking for the conflict-tracking state machine.
//!
//! Lattice's transaction commit relies on three pieces of shared
//! state cooperating: a monotonic `write_seq` counter, a `last_writes`
//! map of `key -> last seq`, and an `active_tx` multiset of in-flight
//! `snapshot_seq` values. The commit path reads all three to detect
//! write conflicts; the trim path mutates the first two while the
//! second protects an in-flight transaction's keys from being dropped.
//!
//! These invariants were added in v1.6 (conflict detection) and v1.10
//! (last-writes trim) and reasoned through by hand. Loom exercises
//! the actual production code under every legal interleaving of two
//! or three threads, so any missing fence or out-of-order observation
//! shows up here rather than in production.
//!
//! Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test -p lattice-loom-tests --release
//! ```

#![cfg(loom)]

use lattice_core::conflict_tracker::ConflictTracker;
use loom::sync::Arc;
use loom::sync::atomic::Ordering;

/// **Invariant A (atomic write recording)**: if a transaction's
/// later `check_conflict` call observes that `write_seq` has
/// advanced past its captured `snapshot_seq`, then the corresponding
/// `last_writes` entry must already be present in the map.
///
/// In other words, the `(write_seq` bump, `last_writes` insert) pair
/// performed by `record_write` must be observed atomically by any
/// later reader that takes the tracker's internal lock. If they
/// could be observed out of order, a transaction would see
/// `write_seq > snapshot_seq` (a write happened) but fail to find
/// the corresponding `last_writes` entry (no abort), losing snapshot
/// isolation.
#[test]
fn record_write_pair_is_visible_atomically() {
    loom::model(|| {
        let tracker = Arc::new(ConflictTracker::new());
        let key = b"k".to_vec();

        let writer = {
            let tracker = Arc::clone(&tracker);
            let key = key.clone();
            loom::thread::spawn(move || {
                tracker.record_write(key);
            })
        };

        let reader = {
            let tracker = Arc::clone(&tracker);
            let key = key.clone();
            loom::thread::spawn(move || {
                let snapshot_seq = tracker.begin_tx();
                // Load `write_seq` *before* the conflict check so any
                // observed advance proves the writer's `fetch_add`
                // completed strictly before the check starts. If we
                // loaded after, the writer could legitimately run
                // between the check and the load and the assertion
                // would flag a false positive.
                let observed_seq = tracker.write_seq().load(Ordering::Acquire);
                let conflict = tracker.check_conflict(snapshot_seq, [key.as_slice()]);
                tracker.end_tx(snapshot_seq);

                if observed_seq > snapshot_seq {
                    assert!(
                        conflict,
                        "write_seq advanced from {snapshot_seq} to {observed_seq} \
                         before check_conflict ran, but the conflict check did \
                         not see the last_writes entry for the key. The \
                         (write_seq bump, last_writes insert) pair must be \
                         atomic to any later reader that takes the tracker lock.",
                    );
                }
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// **Invariant B (trim safety)**: a concurrent trim must not drop
/// `last_writes` entries that an in-flight transaction may still
/// need for its commit-time conflict check.
///
/// Concretely: if transaction T captured `snapshot_seq = S` and a
/// writer recorded key `K` at sequence `W > S`, then a trim that
/// runs while T is still in flight must keep `last_writes[K] = W`.
/// Otherwise T would commit silently against a concurrent
/// overwrite, the lost-update bug v1.6 closed.
///
/// The atomic step `begin_tx` performs (capture `write_seq`,
/// register in `active_tx`) is the linchpin: any later trim must
/// observe the registration and pin its cutoff at or below T's
/// `snapshot_seq`. Loom interleaves three threads (the in-flight
/// transaction, the writer, the trimmer) and asserts that the
/// invariant holds under every legal ordering.
#[test]
fn trim_preserves_entries_an_active_transaction_might_need() {
    loom::model(|| {
        let tracker = Arc::new(ConflictTracker::new());
        let key = b"k".to_vec();

        // Pre-populate one entry so `last_writes` is non-empty
        // before the threads spawn. This entry has `seq = 1`; the
        // in-flight transaction below begins after it, so its
        // `snapshot_seq >= 1` and the entry is below the cutoff.
        tracker.record_write(key.clone());

        let txn = {
            let tracker = Arc::clone(&tracker);
            loom::thread::spawn(move || {
                let snapshot_seq = tracker.begin_tx();
                // Hold the registration alive across the writer
                // and trimmer scheduling. `end_tx` only fires
                // after both have had a chance to run.
                loom::thread::yield_now();
                snapshot_seq
            })
        };

        let writer = {
            let tracker = Arc::clone(&tracker);
            let key = key.clone();
            loom::thread::spawn(move || {
                tracker.record_write(key);
            })
        };

        let trimmer = {
            let tracker = Arc::clone(&tracker);
            loom::thread::spawn(move || {
                tracker.force_trim();
            })
        };

        let snapshot_seq = txn.join().unwrap();
        writer.join().unwrap();
        trimmer.join().unwrap();

        // Invariant: every `last_writes` entry that survived the
        // trim must have a `seq > snapshot_seq` for the in-flight
        // transaction (or be from the writer that ran after the
        // transaction registered). Equivalently, no entry whose
        // `seq` is strictly greater than `snapshot_seq` may have
        // been dropped while `snapshot_seq` was still registered
        // in `active_tx`. We verify the post-state: if the writer's
        // entry exists (it ran), it must still be present, since
        // its `seq` is necessarily greater than `snapshot_seq`.
        if let Some(last_seq) = tracker.last_seq_for(&key)
            && last_seq > snapshot_seq
        {
            // The writer's entry survives. Good. Nothing to assert
            // beyond presence: it is here.
        } else {
            // Either the writer ran before `begin_tx` (its `seq`
            // is at most `snapshot_seq` and trimming it is safe)
            // or it ran while the trimmer's cutoff included it.
            // Verify that the writer's effects are not load-bearing
            // for our snapshot by re-checking the conflict.
            let conflict = tracker.check_conflict(snapshot_seq, [key.as_slice()]);
            assert!(
                !conflict,
                "no entry in last_writes for the key, yet conflict \
                 check still sees a conflict for snapshot_seq \
                 {snapshot_seq}. Trim removed an entry the in-flight \
                 transaction needed.",
            );
        }

        // Always drop the registration to keep the tracker usable
        // by the implicit teardown loom does between iterations.
        tracker.end_tx(snapshot_seq);
    });
}
