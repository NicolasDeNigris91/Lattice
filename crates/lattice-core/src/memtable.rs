//! In-memory ordered map of pending writes.
//!
//! INVARIANT: a key mapped to `None` is a tombstone, meaning the key was
//! deleted. The memtable preserves last-writer-wins by using a `BTreeMap`
//! and overwriting on insert.

use std::collections::BTreeMap;

/// Three-state result for a memtable lookup.
///
/// The distinction between [`Lookup::Tombstoned`] and [`Lookup::Absent`]
/// is load-bearing once `SSTable`s exist: a tombstone here must shadow
/// any older value below, while an absent entry means "ask the next
/// layer".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lookup<'a> {
    /// Live value found at this layer.
    Found(&'a [u8]),
    /// Key was deleted at this layer. Lower layers must not be consulted.
    Tombstoned,
    /// Key is unknown to this layer. Lower layers may know it.
    Absent,
}

/// Sorted in-memory map from key to optional value, where `None` is a
/// tombstone.
#[derive(Debug, Default)]
pub(crate) struct MemTable {
    inner: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Approximate live byte size, used to drive auto-flush.
    bytes: usize,
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.bytes = self
            .bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        // Old size is no longer there. Approximation; we count `key`
        // size only on the first insert and once per overwrite.
        if let Some(Some(prev_value)) = self.inner.insert(key, Some(value)) {
            self.bytes = self.bytes.saturating_sub(prev_value.len());
        }
    }

    pub(crate) fn delete(&mut self, key: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(key.len());
        if let Some(Some(prev_value)) = self.inner.insert(key, None) {
            self.bytes = self.bytes.saturating_sub(prev_value.len());
        }
    }

    pub(crate) fn lookup(&self, key: &[u8]) -> Lookup<'_> {
        match self.inner.get(key) {
            Some(Some(v)) => Lookup::Found(v.as_slice()),
            Some(None) => Lookup::Tombstoned,
            None => Lookup::Absent,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Approximate memtable footprint in bytes. Counts key plus value
    /// bytes, ignores `BTreeMap` overhead. Good enough for flush
    /// scheduling.
    pub(crate) const fn approx_size(&self) -> usize {
        self.bytes
    }

    /// Iterate every entry, including tombstones, in key order.
    pub(crate) fn iter_all(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.inner.iter().map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    /// Drain self into a sorted vector of `(key, optional value)` pairs,
    /// suitable for streaming into an `SSTableWriter`.
    pub(crate) fn drain(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.bytes = 0;
        std::mem::take(&mut self.inner).into_iter().collect()
    }
}
