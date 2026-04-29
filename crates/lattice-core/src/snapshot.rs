//! Read-only point-in-time view of a [`Lattice`](crate::Lattice).
//!
//! INVARIANT: a `Snapshot` returned by [`Lattice::snapshot`] sees the
//! exact set of `(key, value)` pairs that were live at the moment of
//! the call, regardless of any subsequent `put`, `delete`, `flush`, or
//! `compact` operations on the parent. The snapshot achieves this by
//! cloning the in-memory memtable and holding `Arc` references to the
//! `SSTable` readers from that moment.
//!
//! While a snapshot is alive, compaction may still run on the parent.
//! New `SSTable`s are created and the manifest is updated as usual.
//! The old `SSTable` files cannot always be deleted immediately on
//! Windows because the snapshot's `Arc<SSTableReader>` keeps the file
//! open; the lingering files are cleaned up by the orphan sweep on
//! the next [`Lattice::open`](crate::Lattice::open).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::Result;
use crate::memtable::{Lookup, MemTable};
use crate::sstable::{SSTableReader, SsLookup};

/// Read-only point-in-time view of a database.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) memtable: MemTable,
    pub(crate) sstables: Vec<Arc<SSTableReader>>,
}

impl Snapshot {
    /// Read the value of `key` as it existed when the snapshot was
    /// created.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.lookup(key) {
            Lookup::Found(value) => return Ok(Some(value.to_vec())),
            Lookup::Tombstoned => return Ok(None),
            Lookup::Absent => {}
        }
        for sst in self.sstables.iter().rev() {
            match sst.get(key)? {
                SsLookup::Found(value) => return Ok(Some(value)),
                SsLookup::Tombstoned => return Ok(None),
                SsLookup::Absent => {}
            }
        }
        Ok(None)
    }

    /// Iterate live key-value pairs as they existed when the snapshot
    /// was created. If `prefix` is `Some`, only keys starting with it
    /// are returned.
    pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut accumulator: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        for (key, value) in self.memtable.iter_all() {
            if prefix.is_some_and(|p| !key.starts_with(p)) {
                continue;
            }
            accumulator.insert(key.to_vec(), value.map(<[u8]>::to_vec));
        }

        for sst in self.sstables.iter().rev() {
            for (key, value) in sst.iter_all(prefix)? {
                accumulator.entry(key).or_insert(value);
            }
        }

        Ok(accumulator
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }
}
