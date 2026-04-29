//! In-memory ordered map of pending writes.
//!
//! INVARIANT: a key mapped to `None` is a tombstone, meaning the key was
//! deleted. The memtable preserves last-writer-wins by using a `BTreeMap`
//! and overwriting on insert.

use std::collections::BTreeMap;

/// Sorted in-memory map from key to optional value, where `None` is a
/// tombstone.
#[derive(Debug, Default)]
pub(crate) struct MemTable {
    inner: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, Some(value));
    }

    pub(crate) fn delete(&mut self, key: Vec<u8>) {
        self.inner.insert(key, None);
    }

    /// Return the live value for `key`, or `None` if the key is tombstoned
    /// or absent. Phase 2 will introduce a richer return type to let
    /// callers distinguish "deleted here" from "ask the next layer".
    pub(crate) fn lookup(&self, key: &[u8]) -> Option<&[u8]> {
        self.inner.get(key).and_then(|opt| opt.as_deref())
    }

    /// Iterate over live pairs, ordered by key. Tombstoned keys are
    /// skipped. If `prefix` is `Some`, only keys starting with it are
    /// emitted.
    pub(crate) fn iter_live<'a>(
        &'a self,
        prefix: Option<&'a [u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> {
        self.inner.iter().filter_map(move |(k, v)| {
            let value = v.as_deref()?;
            if prefix.is_some_and(|p| !k.starts_with(p)) {
                return None;
            }
            Some((k.as_slice(), value))
        })
    }
}
