//! Lattice, an LSM-tree key-value storage engine.
//!
//! This crate exposes a small embeddable key-value store backed by a write
//! ahead log, an in-memory memtable, sorted string tables, bloom filters,
//! tiered compaction, and snapshots.
//!
//! See the companion book at <https://lattice.nicolaspilegidenigris.dev>
//! for a chapter-by-chapter explanation of every component.

#![forbid(unsafe_code)]

mod bloom;
mod compaction;
mod error;
mod manifest;
mod memtable;
mod snapshot;
mod sstable;
mod wal;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{info, warn};

pub use crate::error::{Error, Result};
use crate::manifest::Manifest;
use crate::memtable::{Lookup, MemTable};
pub use crate::snapshot::Snapshot;
use crate::sstable::{SSTableReader, SSTableWriter, SsLookup};
use crate::wal::{LogEntry, Wal};

/// Per-write knobs. Today this is only `durable`; future options
/// (e.g. write priority) will land here without breaking callers.
#[derive(Debug, Clone, Copy)]
pub struct WriteOptions {
    /// If `true` (the default), the call returns only after the WAL
    /// has been `fsync`ed to disk. If `false`, the call returns once
    /// the bytes have been queued; a background flusher amortises the
    /// `fsync` across a window of writes.
    pub durable: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { durable: true }
    }
}

/// Fluent builder for opening a [`Lattice`] with non-default
/// configuration. Reach it via [`Lattice::builder`].
#[derive(Debug, Clone)]
pub struct LatticeBuilder {
    path: PathBuf,
    flush_threshold_bytes: usize,
    compaction_threshold: usize,
    commit_window: std::time::Duration,
    commit_batch: usize,
}

impl LatticeBuilder {
    /// Set the in-memory size at which the memtable auto-flushes to a
    /// new on-disk `SSTable`. Default is 4 MiB.
    #[must_use]
    pub const fn flush_threshold_bytes(mut self, bytes: usize) -> Self {
        self.flush_threshold_bytes = bytes;
        self
    }

    /// Set the number of live `SSTable`s that triggers an
    /// auto-compaction. Default is 4. Use [`usize::MAX`] to disable.
    #[must_use]
    pub const fn compaction_threshold(mut self, tables: usize) -> Self {
        self.compaction_threshold = tables;
        self
    }

    /// Maximum time a non-durable write may sit in the WAL buffer
    /// before the engine flushes and `fsync`s it. Default is 5 ms.
    ///
    /// **Reserved API.** The time-driven flusher requires a
    /// background thread sharing the WAL with the writer thread,
    /// which is a concurrency story that lands with M2. In v1.1 the
    /// value is accepted and stored but only the batch threshold and
    /// explicit `flush_wal` calls trigger an `fsync`. Setting a
    /// large value here is safe today and will become meaningful
    /// once M2 ships.
    #[must_use]
    pub const fn commit_window(mut self, window: std::time::Duration) -> Self {
        self.commit_window = window;
        self
    }

    /// Maximum number of pending non-durable writes before the
    /// engine flushes and `fsync`s. Default is 64. Use
    /// [`usize::MAX`] to disable batch-driven flushing.
    #[must_use]
    pub const fn commit_batch(mut self, batch: usize) -> Self {
        self.commit_batch = batch;
        self
    }

    /// Open or create the database at the configured path.
    pub fn open(self) -> Result<Lattice> {
        Lattice::open_with(self)
    }
}

/// Default memtable size (in bytes) before an auto-flush is triggered.
const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Default number of live `SSTable`s before an auto-compaction is
/// triggered. Tunable via [`LatticeBuilder::compaction_threshold`].
const DEFAULT_COMPACTION_THRESHOLD: usize = 4;

/// Default group commit window. Non-durable writes accumulate in the
/// WAL `BufWriter` for at most this long before the engine syncs.
const DEFAULT_COMMIT_WINDOW: std::time::Duration = std::time::Duration::from_millis(5);

/// Default group commit batch size. Non-durable writes are syncd
/// once this many records are queued, regardless of the window.
const DEFAULT_COMMIT_BATCH: usize = 64;

/// `SSTable` filename format. Six zero-padded digits, lexicographic order
/// matches sequence order up to one million tables, which is enough for
/// any realistic Phase 4 workload.
const SSTABLE_DIGITS: usize = 6;

/// An open Lattice database.
pub struct Lattice {
    path: PathBuf,
    memtable: MemTable,
    wal: Wal,
    sstables: Vec<Arc<SSTableReader>>,
    next_seq: u64,
    flush_threshold_bytes: usize,
    compaction_threshold: usize,
    #[allow(dead_code)] // Used by group commit timer; field reserved.
    commit_window: std::time::Duration,
    #[allow(dead_code)] // Used by group commit batch threshold; field reserved.
    commit_batch: usize,
    /// Number of non-durable WAL records queued since the last sync.
    pending_writes: usize,
}

impl Drop for Lattice {
    /// Graceful close: flush any pending non-durable WAL bytes so a
    /// well-behaved program does not lose acknowledged writes. Errors
    /// are logged because Drop cannot return them; callers that care
    /// about hard failure surfaces should call [`Lattice::flush_wal`]
    /// explicitly before dropping the handle.
    fn drop(&mut self) {
        if self.pending_writes > 0
            && let Err(err) = self.flush_wal()
        {
            warn!(
                ?err,
                "lattice drop: flush_wal failed; non-durable writes may be lost"
            );
        }
    }
}

impl std::fmt::Debug for Lattice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lattice")
            .field("path", &self.path)
            .field("sstables", &self.sstables.len())
            .field("memtable_bytes", &self.memtable.approx_size())
            .field("next_seq", &self.next_seq)
            .field("flush_threshold_bytes", &self.flush_threshold_bytes)
            .field("compaction_threshold", &self.compaction_threshold)
            .finish_non_exhaustive()
    }
}

impl Lattice {
    /// Start a fluent builder for opening a database at `path`. Use
    /// the returned [`LatticeBuilder`] to override defaults, then
    /// finish with [`LatticeBuilder::open`].
    pub fn builder(path: impl AsRef<Path>) -> LatticeBuilder {
        LatticeBuilder {
            path: path.as_ref().to_path_buf(),
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            commit_window: DEFAULT_COMMIT_WINDOW,
            commit_batch: DEFAULT_COMMIT_BATCH,
        }
    }

    /// Open or create a database at `path` with all defaults.
    /// Equivalent to `Lattice::builder(path).open()`.
    ///
    /// Creates the directory if absent. Loads the manifest (or
    /// bootstraps one), opens the listed `SSTable`s, deletes any orphan
    /// `*.sst` files left over from a crash mid-compaction, then
    /// replays the write-ahead log.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::builder(path).open()
    }

    fn open_with(builder: LatticeBuilder) -> Result<Self> {
        let LatticeBuilder {
            path,
            flush_threshold_bytes,
            compaction_threshold,
            commit_window,
            commit_batch,
        } = builder;
        fs::create_dir_all(&path)?;

        let manifest = match Manifest::load(&path)? {
            Some(m) => m,
            None => bootstrap_manifest(&path)?,
        };

        let live: BTreeSet<u64> = manifest.table_seqs.iter().copied().collect();
        delete_orphans(&path, &live)?;

        let mut sstables = Vec::with_capacity(live.len());
        for seq in &manifest.table_seqs {
            sstables.push(Arc::new(SSTableReader::open(
                &sstable_path(&path, *seq),
                *seq,
            )?));
        }

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
            next_seq = manifest.next_seq,
            path = %path.display(),
            "lattice opened"
        );

        Ok(Self {
            path,
            memtable,
            wal,
            sstables,
            next_seq: manifest.next_seq,
            flush_threshold_bytes,
            compaction_threshold,
            commit_window,
            commit_batch,
            pending_writes: 0,
        })
    }

    /// Insert or overwrite a value for `key` with explicit
    /// per-write options. See [`WriteOptions`] for the trade-off
    /// between durability and throughput.
    pub fn put_with(&mut self, key: &[u8], value: &[u8], opts: WriteOptions) -> Result<()> {
        let entry = LogEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.append_entry(&entry, opts)?;
        let LogEntry::Put { key, value } = entry else {
            unreachable!()
        };
        self.memtable.put(key, value);
        self.maybe_flush()?;
        Ok(())
    }

    /// Force a `fsync` of any pending non-durable WAL appends.
    /// Returns once the bytes are on stable storage. A no-op when
    /// nothing is pending.
    pub fn flush_wal(&mut self) -> Result<()> {
        if self.pending_writes == 0 {
            return Ok(());
        }
        self.wal.sync_pending()?;
        self.pending_writes = 0;
        Ok(())
    }

    /// Insert or overwrite a value for `key`. Equivalent to
    /// `put_with(key, value, WriteOptions::default())`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with(key, value, WriteOptions::default())
    }

    fn append_entry(&mut self, entry: &LogEntry, opts: WriteOptions) -> Result<()> {
        // Both paths buffer the record first. The durable case then
        // forces an immediate sync (which also drains any prior
        // pending non-durable writes), the non-durable case lets the
        // batch threshold decide.
        self.wal.append_pending(entry)?;
        if opts.durable {
            self.wal.sync_pending()?;
            self.pending_writes = 0;
        } else {
            self.pending_writes = self.pending_writes.saturating_add(1);
            if self.pending_writes >= self.commit_batch {
                self.wal.sync_pending()?;
                self.pending_writes = 0;
            }
        }
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

        let _ = fs::remove_file(&tmp_path);
        let entries = self.memtable.drain();
        {
            let mut writer = SSTableWriter::create(&tmp_path, entries.len())?;
            for (key, value) in entries {
                writer.append(key, value)?;
            }
            writer.finish()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.path)?;

        let reader = Arc::new(SSTableReader::open(&final_path, seq)?);
        self.sstables.push(reader);
        self.next_seq = self.next_seq.saturating_add(1);

        self.persist_manifest()?;
        self.wal.truncate()?;
        // Memtable contents are now durable in the SSTable. Anything
        // we were tracking for group commit on the WAL is captured
        // either way, so the pending counter resets.
        self.pending_writes = 0;
        info!(seq, path = %final_path.display(), "sstable flushed");

        self.maybe_compact()?;
        Ok(())
    }

    /// Run a compaction across every live `SSTable`, replacing them
    /// with a single merged table that drops tombstones. No-op if there
    /// are fewer than two tables.
    pub fn compact(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        let new_seq = self.next_seq;
        let final_path = sstable_path(&self.path, new_seq);
        let tmp_path = self
            .path
            .join(format!("{new_seq:0SSTABLE_DIGITS$}.sst.tmp"));
        let _ = fs::remove_file(&tmp_path);

        let readers: Vec<&SSTableReader> = self.sstables.iter().map(Arc::as_ref).collect();
        compaction::compact_all(&readers, &tmp_path)?;
        drop(readers);
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.path)?;
        let new_reader = Arc::new(SSTableReader::open(&final_path, new_seq)?);

        let old_seqs: Vec<u64> = self.sstables.iter().map(|r| r.seq()).collect();
        // Replace readers in memory before persisting the manifest, so
        // that a panic between rename and save still leaves the engine
        // in-memory state consistent with the on-disk new file. Any
        // outstanding `Snapshot` keeps the old `Arc<SSTableReader>`s
        // alive; that is intentional.
        self.sstables.clear();
        self.sstables.push(new_reader);
        self.next_seq = self.next_seq.saturating_add(1);
        self.persist_manifest()?;

        for seq in old_seqs {
            let path = sstable_path(&self.path, seq);
            if let Err(err) = fs::remove_file(&path) {
                warn!(
                    ?err,
                    path = %path.display(),
                    "could not delete old sstable (likely held by a live Snapshot on Windows; cleaned up on next open)"
                );
            }
        }
        info!(new_seq, "compaction complete");
        Ok(())
    }

    /// Open a read-only point-in-time view of the database.
    ///
    /// The snapshot sees the exact set of live keys at the time of the
    /// call. Subsequent `put`, `delete`, `flush`, and `compact` calls
    /// on the parent do not change what the snapshot sees.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            memtable: self.memtable.clone(),
            sstables: self.sstables.clone(),
        }
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

    fn maybe_compact(&mut self) -> Result<()> {
        if self.sstables.len() >= self.compaction_threshold {
            self.compact()?;
        }
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let manifest = Manifest {
            version: 1,
            next_seq: self.next_seq,
            table_seqs: self.sstables.iter().map(|r| r.seq()).collect(),
        };
        manifest.save(&self.path)
    }
}

fn sstable_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:0SSTABLE_DIGITS$}.sst"))
}

/// `fsync` the directory entry. Required on POSIX for a `rename` to be
/// durable across power loss; on Windows the rename atomicity already
/// covers the dirent and opening a directory as a file is not
/// supported, so this is a no-op.
#[cfg(unix)]
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps, clippy::missing_const_for_fn)]
pub(crate) fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

/// Bootstrap a manifest by scanning the directory for existing
/// `*.sst` files. Used the first time the engine opens a directory
/// that pre-dates the manifest, or that was created by an older
/// (pre-v0.4) version of Lattice.
fn bootstrap_manifest(dir: &Path) -> Result<Manifest> {
    let seqs = scan_sst_seqs(dir)?;
    let next_seq = seqs.last().copied().map_or(1, |s| s + 1);
    let manifest = Manifest {
        version: 1,
        next_seq,
        table_seqs: seqs,
    };
    manifest.save(dir)?;
    Ok(manifest)
}

fn delete_orphans(dir: &Path, live: &BTreeSet<u64>) -> Result<()> {
    let on_disk = scan_sst_seqs(dir)?;
    for seq in on_disk {
        if !live.contains(&seq) {
            let path = sstable_path(dir, seq);
            if let Err(err) = fs::remove_file(&path) {
                warn!(?err, path = %path.display(), "could not delete orphan sstable");
            }
        }
    }
    // Also clean any leftover .sst.tmp files.
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().is_some_and(|e| e == "tmp")
            && p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".sst.tmp"))
        {
            let _ = fs::remove_file(&p);
        }
    }
    Ok(())
}

fn scan_sst_seqs(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "sst") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if let Ok(seq) = stem.parse::<u64>() {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}
