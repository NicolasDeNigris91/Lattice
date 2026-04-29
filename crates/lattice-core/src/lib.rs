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
mod sstable;
mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use tracing::info;

pub use crate::error::{Error, Result};
use crate::memtable::{Lookup, MemTable};
use crate::sstable::{SSTableReader, SSTableWriter, SsLookup};
use crate::wal::{LogEntry, Wal};

/// Default memtable size (in bytes) before an auto-flush is triggered.
const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// `SSTable` filename format. Six zero-padded digits, lexicographic order
/// matches sequence order up to one million tables, which is enough for
/// any realistic Phase 2 workload.
const SSTABLE_DIGITS: usize = 6;

/// An open Lattice database.
pub struct Lattice {
    path: PathBuf,
    memtable: MemTable,
    wal: Wal,
    sstables: Vec<SSTableReader>,
    next_seq: u64,
    flush_threshold_bytes: usize,
}

impl std::fmt::Debug for Lattice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lattice")
            .field("path", &self.path)
            .field("sstables", &self.sstables.len())
            .field("memtable_bytes", &self.memtable.approx_size())
            .field("next_seq", &self.next_seq)
            .field("flush_threshold_bytes", &self.flush_threshold_bytes)
            .finish_non_exhaustive()
    }
}

impl Lattice {
    /// Open or create a database in the given directory.
    ///
    /// Creates the directory if absent. Discovers existing `SSTable`s,
    /// replays the write-ahead log into a fresh memtable, then opens the
    /// WAL for further appends.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let sstables = discover_sstables(&path)?;
        let next_seq = sstables.last().map_or(1, |t| t.seq() + 1);

        let wal_path = path.join("wal.log");
        let (wal, entries) = Wal::open(&wal_path)?;
        let mut memtable = MemTable::new();
        for entry in entries {
            match entry {
                LogEntry::Put { key, value } => memtable.put(key, value),
                LogEntry::Delete { key } => memtable.delete(key),
            }
        }
        info!(
            sstables = sstables.len(),
            next_seq,
            path = %path.display(),
            "lattice opened"
        );

        Ok(Self {
            path,
            memtable,
            wal,
            sstables,
            next_seq,
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
        })
    }

    /// Override the auto-flush threshold. Mostly useful for tests.
    #[doc(hidden)]
    pub const fn set_flush_threshold(&mut self, bytes: usize) {
        self.flush_threshold_bytes = bytes;
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
        self.maybe_flush()?;
        Ok(())
    }

    /// Delete `key`. A subsequent `get` returns `None`.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let entry = LogEntry::Delete { key: key.to_vec() };
        self.wal.append(&entry)?;
        let LogEntry::Delete { key } = entry else {
            unreachable!()
        };
        self.memtable.delete(key);
        self.maybe_flush()?;
        Ok(())
    }

    /// Read the current value for `key`, or `None` if absent or deleted.
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

    /// Iterate live key-value pairs in key order. If `prefix` is `Some`,
    /// only keys starting with it are returned.
    pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Newest-source-wins merge across memtable and SSTables.
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

    /// Flush the current memtable to a new on-disk `SSTable`, then
    /// truncate the WAL. No-op if the memtable is empty.
    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let seq = self.next_seq;
        let final_path = sstable_path(&self.path, seq);
        let tmp_path = self.path.join(format!("{seq:0SSTABLE_DIGITS$}.sst.tmp"));

        // Write to a temp file so a crash mid-write leaves no
        // half-formed `.sst` for `discover_sstables` to pick up.
        let _ = fs::remove_file(&tmp_path);
        {
            let mut writer = SSTableWriter::create(&tmp_path)?;
            for (key, value) in self.memtable.drain() {
                writer.append(key, value)?;
            }
            writer.finish()?;
        }
        fs::rename(&tmp_path, &final_path)?;

        let reader = SSTableReader::open(&final_path, seq)?;
        self.sstables.push(reader);
        self.next_seq = self.next_seq.saturating_add(1);

        self.wal.truncate()?;
        info!(seq, path = %final_path.display(), "sstable flushed");
        Ok(())
    }

    /// Path to the database directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.memtable.approx_size() >= self.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }
}

fn sstable_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:0SSTABLE_DIGITS$}.sst"))
}

fn discover_sstables(dir: &Path) -> Result<Vec<SSTableReader>> {
    let mut entries: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "sst") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if let Ok(seq) = stem.parse::<u64>() {
            entries.push((seq, path));
        }
    }
    entries.sort_by_key(|(seq, _)| *seq);

    let mut readers = Vec::with_capacity(entries.len());
    for (seq, path) in entries {
        readers.push(SSTableReader::open(&path, seq)?);
    }
    Ok(readers)
}
