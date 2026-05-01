//! Property-based tests for the durability contract.
//!
//! Generates arbitrary sequences of `put` and `delete` operations, applies
//! them to a real Lattice instance, drops the instance, reopens, and
//! verifies that every key has the same final value as a `BTreeMap` that
//! received the same sequence in memory.
//!
//! The model is intentionally small: a tiny key alphabet forces overlap so
//! that overwrites and tombstones are exercised on every run.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use lattice_core::{Error, Lattice};
use proptest::prelude::*;
use tempfile::tempdir;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    /// Force a flush of the memtable into a new `SSTable`.
    Flush,
    /// Force a compaction that merges every `SSTable` into one.
    Compact,
}

/// A single staged operation inside a transaction closure.
#[derive(Debug, Clone)]
enum Stage {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// A step in a transactional op sequence: a plain put / delete,
/// or a transaction that ends with `Ok` / `Err`.
#[derive(Debug, Clone)]
enum TxStep {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    TxOk(Vec<Stage>),
    TxErr(Vec<Stage>),
}

fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Tiny alphabet forces overlap so that overwrites and tombstones
        // exercise the same keys on most runs.
        Just(b"a".to_vec()),
        Just(b"b".to_vec()),
        Just(b"c".to_vec()),
        Just(b"d".to_vec()),
        Just(b"".to_vec()),
        proptest::collection::vec(any::<u8>(), 1..8),
    ]
}

fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..32)
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (arb_key(), arb_value()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => arb_key().prop_map(Op::Delete),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

/// Replay an op sequence against a `Lattice` and a parallel
/// `BTreeMap` reference. Records every distinct key the sequence
/// touched so the caller can iterate over it for assertions.
fn replay(
    db: &Lattice,
    ops: &[Op],
    reference: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    all_keys: &mut BTreeSet<Vec<u8>>,
) {
    for op in ops {
        match op {
            Op::Put(k, v) => {
                db.put(k, v).unwrap();
                reference.insert(k.clone(), v.clone());
                all_keys.insert(k.clone());
            }
            Op::Delete(k) => {
                db.delete(k).unwrap();
                reference.remove(k);
                all_keys.insert(k.clone());
            }
            Op::Flush => {
                db.flush().unwrap();
            }
            Op::Compact => {
                db.compact().unwrap();
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn arbitrary_ops_match_btreemap_after_reopen(
        ops in proptest::collection::vec(arb_op(), 0..200),
    ) {
        let dir = tempdir().unwrap();
        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        // First session: apply every operation, then drop the Lattice.
        {
            let db = Lattice::open(dir.path()).unwrap();
            replay(&db, &ops, &mut reference, &mut all_keys);
        }

        // Second session: every key the test ever touched must match the
        // reference's view.
        let db = Lattice::open(dir.path()).unwrap();
        for key in &all_keys {
            prop_assert_eq!(
                db.get(key).unwrap(),
                reference.get(key).cloned(),
                "key {:?} diverged after reopen",
                key,
            );
        }
    }

    /// A snapshot taken at point `t` returns values as of `t`.
    /// Subsequent writes through the parent handle do not change
    /// what the snapshot sees. This is the read-isolation
    /// guarantee that v1.2's `Send + Sync + Clone` work made the
    /// load-bearing primitive of v1.4 transactions.
    ///
    /// The test splits a random op sequence at a random index,
    /// applies the prefix, takes a snapshot, applies the suffix,
    /// and asserts that the snapshot's view of every touched key
    /// matches a reference `BTreeMap` built only from the prefix.
    #[test]
    fn snapshot_isolates_reads_from_subsequent_writes(
        ops in proptest::collection::vec(arb_op(), 1..120),
        snapshot_at in any::<prop::sample::Index>(),
    ) {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        let split = snapshot_at.index(ops.len());
        let (prefix, suffix) = ops.split_at(split);

        let mut at_snapshot: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        replay(&db, prefix, &mut at_snapshot, &mut all_keys);

        let snap = db.snapshot();

        // Build a separate reference that grows past the snapshot
        // point. The post-snapshot writes must not be visible to
        // `snap.get`.
        let mut after_snapshot = at_snapshot.clone();
        replay(&db, suffix, &mut after_snapshot, &mut all_keys);

        for key in &all_keys {
            prop_assert_eq!(
                snap.get(key).unwrap(),
                at_snapshot.get(key).cloned(),
                "snapshot saw a post-snapshot mutation for key {:?}",
                key,
            );
        }
    }

    /// A transaction that returns `Ok` is equivalent to applying
    /// its staged writes through the regular `put` / `delete`
    /// path; a transaction that returns `Err` is a no-op. Pinned
    /// here under random op sequences interleaved with both
    /// outcomes, against a reference `BTreeMap` that mirrors the
    /// commit-or-rollback semantics. With no concurrent writers
    /// the conflict detector never fires, so every `Ok` commit
    /// must succeed.
    #[test]
    fn transaction_ok_commits_err_rolls_back(
        ops in proptest::collection::vec(
            prop_oneof![
                4 => (arb_key(), arb_value()).prop_map(|(k, v)| TxStep::Put(k, v)),
                2 => arb_key().prop_map(TxStep::Delete),
                3 => proptest::collection::vec(
                    prop_oneof![
                        1 => (arb_key(), arb_value()).prop_map(|(k, v)| Stage::Put(k, v)),
                        1 => arb_key().prop_map(Stage::Delete),
                    ],
                    0..6,
                ).prop_map(TxStep::TxOk),
                1 => proptest::collection::vec(
                    prop_oneof![
                        1 => (arb_key(), arb_value()).prop_map(|(k, v)| Stage::Put(k, v)),
                        1 => arb_key().prop_map(Stage::Delete),
                    ],
                    0..6,
                ).prop_map(TxStep::TxErr),
            ],
            0..40,
        ),
    ) {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        for step in &ops {
            match step {
                TxStep::Put(k, v) => {
                    db.put(k, v).unwrap();
                    reference.insert(k.clone(), v.clone());
                    all_keys.insert(k.clone());
                }
                TxStep::Delete(k) => {
                    db.delete(k).unwrap();
                    reference.remove(k);
                    all_keys.insert(k.clone());
                }
                TxStep::TxOk(stages) => {
                    let result = db.transaction(|tx| {
                        for stage in stages {
                            match stage {
                                Stage::Put(k, v) => tx.put(k, v),
                                Stage::Delete(k) => tx.delete(k),
                            }
                        }
                        Ok::<_, Error>(())
                    });
                    prop_assert!(
                        result.is_ok(),
                        "an Ok-returning transaction with no concurrent writers must commit, got {result:?}",
                    );
                    for stage in stages {
                        match stage {
                            Stage::Put(k, v) => {
                                reference.insert(k.clone(), v.clone());
                                all_keys.insert(k.clone());
                            }
                            Stage::Delete(k) => {
                                reference.remove(k);
                                all_keys.insert(k.clone());
                            }
                        }
                    }
                }
                TxStep::TxErr(stages) => {
                    let result: Result<(), Error> = db.transaction(|tx| {
                        for stage in stages {
                            match stage {
                                Stage::Put(k, v) => tx.put(k, v),
                                Stage::Delete(k) => tx.delete(k),
                            }
                            // Track the key so we can later assert the
                            // staged write was rolled back.
                            match stage {
                                Stage::Put(k, _) | Stage::Delete(k) => {
                                    let _ = k;
                                }
                            }
                        }
                        Err(Error::Io(io::Error::other("rollback")))
                    });
                    prop_assert!(
                        matches!(result, Err(Error::Io(_))),
                        "the closure's Err should bubble out, got {result:?}",
                    );
                    // Record the keys for the assertion sweep so a
                    // staged-then-rolled-back write that happened to
                    // match an existing reference value is still
                    // verified.
                    for stage in stages {
                        match stage {
                            Stage::Put(k, _) | Stage::Delete(k) => {
                                all_keys.insert(k.clone());
                            }
                        }
                    }
                }
            }
        }

        for key in &all_keys {
            prop_assert_eq!(
                db.get(key).unwrap(),
                reference.get(key).cloned(),
                "transactional sequence diverged for key {:?}",
                key,
            );
        }
    }

    /// `compact()` is a pure data-preserving rearrangement of the
    /// on-disk SSTables. After a forced compaction, every key the
    /// test ever touched must read back to the same value the
    /// reference holds, with no extra reopen needed.
    ///
    /// This is a tighter invariant than `arbitrary_ops_match_btreemap_after_reopen`,
    /// which compacts as one of many random ops and only checks
    /// after a reopen. Here we compact deterministically at the
    /// end and check WITHOUT closing the handle, so a regression
    /// in the in-memory level state (frozen memtable, level
    /// installation, manifest write) shows up too.
    #[test]
    fn compaction_preserves_last_writer_wins(
        ops in proptest::collection::vec(arb_op(), 0..150),
    ) {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        replay(&db, &ops, &mut reference, &mut all_keys);

        // Force everything through the WAL into an SSTable, then
        // run leveled compaction until convergence. After this
        // the database is at most one SSTable per level.
        db.flush().unwrap();
        db.compact().unwrap();

        for key in &all_keys {
            prop_assert_eq!(
                db.get(key).unwrap(),
                reference.get(key).cloned(),
                "compaction lost or corrupted key {:?}",
                key,
            );
        }
    }

    /// `scan_range` (v1.16) yields the same set of pairs as a
    /// post-filtered `scan`, in the same order. The test drives a
    /// random op history, picks two random keys from the touched
    /// alphabet for the bounds (inclusive-exclusive), and asserts
    /// the two APIs converge. This pins `scan_range` as a
    /// behavioural drop-in for callers that previously
    /// post-filtered a `scan` result.
    #[test]
    fn scan_range_matches_post_filtered_scan_under_random_history(
        ops in proptest::collection::vec(arb_op(), 0..150),
        lo in any::<u8>(),
        hi in any::<u8>(),
    ) {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        replay(&db, &ops, &mut reference, &mut all_keys);

        // Map random bytes onto the test's tiny key alphabet for
        // bounds: each lo/hi byte picks one of {a, b, c, d, e, f, g, h, i, j}.
        let alphabet: Vec<&[u8]> = vec![
            b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i", b"j",
        ];
        let start = alphabet[lo as usize % alphabet.len()];
        let end = alphabet[hi as usize % alphabet.len()];

        let from_range: Vec<(Vec<u8>, Vec<u8>)> = db
            .scan_range(Some(start), Some(end))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let from_filtered: Vec<(Vec<u8>, Vec<u8>)> = db
            .scan(None)
            .unwrap()
            .into_iter()
            .filter(|(k, _)| k.as_slice() >= start && k.as_slice() < end)
            .collect();

        prop_assert_eq!(from_range, from_filtered);
    }

    /// `scan_iter` is the streaming variant of `scan`. The two must
    /// be observationally equivalent under any history of puts,
    /// deletes, flushes, and compactions: same set of visible
    /// pairs, same key order, same tombstone filtering. The test
    /// drives a random op sequence and asserts both APIs return the
    /// identical `Vec<(key, value)>`. This pins the v1.12 streaming
    /// scan as a behavioural drop-in for callers that want lazy
    /// iteration without changing their result-handling code.
    #[test]
    fn scan_iter_matches_scan_under_random_history(
        ops in proptest::collection::vec(arb_op(), 0..150),
    ) {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        replay(&db, &ops, &mut reference, &mut all_keys);

        let from_scan = db.scan(None).unwrap();
        let from_iter: Vec<(Vec<u8>, Vec<u8>)> = db
            .scan_iter(None)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        prop_assert_eq!(&from_iter, &from_scan,
            "scan_iter must yield the same pairs as scan");

        // And the result must match the reference's view of live
        // keys (no extras, no missing entries, value matches).
        let from_reference: Vec<(Vec<u8>, Vec<u8>)> = reference
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        prop_assert_eq!(from_iter, from_reference,
            "scan_iter must agree with the BTreeMap reference");
    }
}
