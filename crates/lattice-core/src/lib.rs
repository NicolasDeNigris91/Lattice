//! Lattice, an LSM-tree key-value storage engine.
//!
//! This crate exposes a small embeddable key-value store backed by a write
//! ahead log, an in-memory memtable, sorted string tables, bloom filters,
//! tiered compaction, and snapshots.
//!
//! See the companion book at <https://lattice.nicolaspilegidenigris.dev>
//! for a chapter-by-chapter explanation of every component.

#![forbid(unsafe_code)]

mod error;
mod memtable;
mod wal;

use std::path::{Path, PathBuf};

use tracing::info;

pub use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::wal::{LogEntry, Wal};

/// An open Lattice database.
#[derive(Debug)]
pub struct Lattice {
    path: PathBuf,
    memtable: MemTable,
    wal: Wal,
}

impl Lattice {
    /// Open or create a database in the given directory.
    ///
    /// Creates the directory if absent. Replays the write-ahead log into
    /// the in-memory memtable, then opens the WAL for further appends.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let wal_path = path.join("wal.log");
        let (wal, entries) = Wal::open(&wal_path)?;

        let mut memtable = MemTable::new();
        let recovered = entries.len();
        for entry in entries {
            match entry {
                LogEntry::Put { key, value } => memtable.put(key, value),
                LogEntry::Delete { key } => memtable.delete(key),
            }
        }
        info!(recovered, path = %path.display(), "lattice opened");

        Ok(Self {
            path,
            memtable,
            wal,
        })
    }

    /// Insert or overwrite a value for `key`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = LogEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.wal.append(&entry)?;
        let LogEntry::Put { key, value } = entry else {
            unreachable!()
        };
        self.memtable.put(key, value);
        Ok(())
    }

    /// Read the current value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.memtable.lookup(key).map(<[u8]>::to_vec))
    }

    /// Iterate live key-value pairs in key order. If `prefix` is `Some`,
    /// only keys starting with it are returned.
    pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .memtable
            .iter_live(prefix)
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect())
    }

    /// Delete `key`. A subsequent `get` returns `None`.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let entry = LogEntry::Delete { key: key.to_vec() };
        self.wal.append(&entry)?;
        let LogEntry::Delete { key } = entry else {
            unreachable!()
        };
        self.memtable.delete(key);
        Ok(())
    }

    /// Path to the database directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}
