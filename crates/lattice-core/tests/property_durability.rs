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

use lattice_core::Lattice;
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
            let mut db = Lattice::open(dir.path()).unwrap();
            for op in &ops {
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
}
