//! Transaction handle for snapshot-isolated reads and atomic
//! writes.
//!
//! INVARIANT: a transaction sees the database state as of the
//! moment it was started, regardless of writes performed by other
//! handles. Writes accumulated inside the transaction are applied
//! atomically at commit time, or discarded on rollback (closure
//! returning `Err`, panic, or `Drop` without commit).

use std::collections::{BTreeMap, BTreeSet};

use crate::error::Result;
use crate::snapshot::Snapshot;

/// A transaction in flight.
///
/// Created by [`crate::Lattice::transaction`] and passed to the
/// user closure. Reads observe the database snapshot taken at
/// transaction start, layered on top of the transaction's own
/// pending writes. The transaction tracks every key it reads
/// (`read_set`) and stages (`write_set`) so the commit path can
/// detect conflicts with concurrent writers.
#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) snapshot: Snapshot,
    /// Engine `write_seq` at the moment the transaction started.
    /// The commit aborts with [`crate::Error::TransactionConflict`]
    /// if any key in `read_set` or `write_set` has a recorded last
    /// write greater than this value at commit time.
    pub(crate) snapshot_seq: u64,
    /// Keys read inside the transaction. Conflict detection treats
    /// reads as part of the dependency footprint so a transaction
    /// that only read K but did not write to it still aborts when
    /// K was modified by another writer.
    pub(crate) read_set: BTreeSet<Vec<u8>>,
    /// Accumulated writes. `None` is a deletion (tombstone). The
    /// engine applies these in key order at commit time.
    pub(crate) write_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Lifetime of the parent `Lattice`; the commit path uses it
    /// indirectly via the engine that wraps the transaction call.
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl Transaction<'_> {
    pub(crate) const fn new(snapshot: Snapshot, snapshot_seq: u64) -> Self {
        Self {
            snapshot,
            snapshot_seq,
            read_set: BTreeSet::new(),
            write_set: BTreeMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Read the value of `key` as of the transaction's snapshot,
    /// layered on top of any writes the transaction has staged.
    /// In-transaction writes shadow snapshot values, so the caller
    /// always observes its own most recent staged write. The key
    /// is recorded in the read-set for conflict detection at
    /// commit time.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_set.insert(key.to_vec());
        if let Some(staged) = self.write_set.get(key) {
            return Ok(staged.clone());
        }
        self.snapshot.get(key)
    }

    /// Stage a put. Visible to subsequent `get` calls inside this
    /// transaction; not visible to other handles until commit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.write_set.insert(key.to_vec(), Some(value.to_vec()));
    }

    /// Stage a delete. Visible to subsequent `get` calls inside this
    /// transaction as `None`; not visible to other handles until
    /// commit.
    pub fn delete(&mut self, key: &[u8]) {
        self.write_set.insert(key.to_vec(), None);
    }
}
