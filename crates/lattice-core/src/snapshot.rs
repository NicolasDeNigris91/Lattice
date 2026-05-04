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
use crate::scan_iter::ScanIter;
use crate::sstable::{SSTableReader, SsLookup};

/// Read-only point-in-time view of a database.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) memtable: MemTable,
    /// `SSTable`s partitioned by LSM level at snapshot time. Same
    /// shape as `Lattice`'s live state: `levels[0]` is L0, `levels[1]`
    /// onward are non-overlapping within a level.
    pub(crate) levels: Vec<Vec<Arc<SSTableReader>>>,
}

impl Snapshot {
    /// Iterate every captured `SSTable`, newest first. Each level
    /// is walked end-to-start so the most recently installed table
    /// wins under last-writer-wins.
    fn ssts_newest_first(&self) -> impl Iterator<Item = &Arc<SSTableReader>> + '_ {
        self.levels.iter().flat_map(|level| level.iter().rev())
    }

    /// Read the value of `key` as it existed when the snapshot was
    /// created. Accepts any `AsRef<[u8]>` for the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.get_inner(key.as_ref())
    }

    fn get_inner(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.lookup(key) {
            Lookup::Found(value) => return Ok(Some(value.to_vec())),
            Lookup::Tombstoned => return Ok(None),
            Lookup::Absent => {}
        }
        for sst in self.ssts_newest_first() {
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

        for sst in self.ssts_newest_first() {
            for (key, value) in sst.iter_all(prefix)? {
                accumulator.entry(key).or_insert(value);
            }
        }

        Ok(accumulator
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    /// Streaming counterpart of [`Self::scan`]. Yields visible
    /// `(key, value)` pairs in ascending key order, optionally
    /// filtered by `prefix`. Like `scan`, the iterator is frozen
    /// at snapshot time: subsequent live mutations do not change
    /// the values it produces.
    ///
    /// Same merge engine as [`crate::Lattice::scan_iter`], so the
    /// memory cost is one frontier entry per tier, not the full
    /// pair set.
    #[must_use]
    pub fn scan_iter(&self, prefix: Option<&[u8]>) -> ScanIter {
        let sstables: Vec<Arc<SSTableReader>> = self.ssts_newest_first().cloned().collect();
        ScanIter::new(&self.memtable, None, sstables, prefix)
    }

    /// Range-bounded streaming scan over the snapshot's pinned
    /// state. Bounds are inclusive-exclusive (`[start, end)`),
    /// matching [`crate::Lattice::scan_range`]. `start = None`
    /// means "from the beginning of the keyspace"; `end = None`
    /// means "to the end".
    #[must_use]
    pub fn scan_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> ScanIter {
        let sstables: Vec<Arc<SSTableReader>> = self.ssts_newest_first().cloned().collect();
        ScanIter::with_bounds(&self.memtable, None, sstables, None, start, end)
    }

    /// Deterministic xxh3-64 fingerprint of the snapshot's
    /// visible `(key, value)` set in ascending key order. The
    /// hash is the temporal counterpart of
    /// [`crate::Lattice::checksum`]: pinned at snapshot time and
    /// invariant under any subsequent live mutation, so two
    /// snapshots taken at the same logical state produce the
    /// same value.
    pub fn checksum(&self) -> Result<u64> {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for entry in self.scan_iter(None) {
            let (key, value) = entry?;
            #[allow(clippy::cast_possible_truncation)]
            let key_len = key.len() as u64;
            #[allow(clippy::cast_possible_truncation)]
            let value_len = value.len() as u64;
            hasher.update(&key_len.to_le_bytes());
            hasher.update(&key);
            hasher.update(&value_len.to_le_bytes());
            hasher.update(&value);
        }
        Ok(hasher.digest())
    }

    /// Bytes the snapshot's pinned `SSTable`s currently occupy
    /// on disk. Queried through each reader's open file handle
    /// rather than its path so the answer is robust against the
    /// file being unlinked from the live tree by a concurrent
    /// compaction (the snapshot's `Arc<SSTableReader>` keeps the
    /// inode alive on POSIX; on Windows the unlink itself fails
    /// while the handle is open).
    ///
    /// Memtable bytes are not counted; the snapshot's memtable
    /// is in process memory, not on disk.
    #[must_use]
    pub fn byte_size_on_disk(&self) -> u64 {
        let mut total: u64 = 0;
        for level in &self.levels {
            for reader in level {
                total = total.saturating_add(reader.file_size_bytes());
            }
        }
        total
    }
}
