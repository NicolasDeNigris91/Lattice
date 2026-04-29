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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
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
    /// the bytes have been queued; the engine amortises the `fsync`
    /// across a window of writes (see [`LatticeBuilder::commit_batch`]
    /// and [`LatticeBuilder::commit_window`]).
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
    commit_window: Duration,
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
    /// background thread that lands with M2.3. In v1.2 the value is
    /// accepted and stored but only the batch threshold and explicit
    /// [`Lattice::flush_wal`] calls trigger an `fsync`. Setting a
    /// large value here is safe and will become meaningful when the
    /// timer thread arrives.
    #[must_use]
    pub const fn commit_window(mut self, window: Duration) -> Self {
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
const DEFAULT_COMMIT_WINDOW: Duration = Duration::from_millis(5);

/// Default group commit batch size. Non-durable writes are syncd
/// once this many records are queued, regardless of the window.
const DEFAULT_COMMIT_BATCH: usize = 64;

/// `SSTable` filename format. Six zero-padded digits, lexicographic order
/// matches sequence order up to one million tables, which is enough for
/// any realistic Phase 4 workload.
const SSTABLE_DIGITS: usize = 6;

/// Immutable view of the LSM state. Clones are cheap (`Arc` field
/// bumps); reads pin a snapshot for the duration of their work.
pub(crate) struct State {
    /// Memtable currently being persisted to an `SSTable`, or `None`
    /// when no flush is in flight. Reads check this after the active
    /// memtable so read-your-writes is preserved across a flush.
    pub(crate) frozen: Option<Arc<MemTable>>,
    /// `SSTable`s partitioned by LSM level. `levels[0]` is L0
    /// (allowed to overlap by key range, written by flush);
    /// `levels[1]` is L1 onward (non-overlapping inside a level,
    /// written by leveled compaction). Phase 2 of M3 carries the
    /// shape; the leveled algorithm itself lands in phase 3.
    pub(crate) levels: Vec<Vec<Arc<SSTableReader>>>,
    pub(crate) next_seq: u64,
}

impl State {
    /// Iterate every live `SSTable`, newest first. L0 is walked
    /// end-to-start (last push = newest); L1 onward is walked in
    /// natural order because each level is internally
    /// non-overlapping. Used by `get` and `scan` to produce
    /// last-writer-wins semantics.
    pub(crate) fn all_sstables_newest_first(
        &self,
    ) -> impl Iterator<Item = &Arc<SSTableReader>> + '_ {
        let l0 = self
            .levels
            .first()
            .into_iter()
            .flat_map(|l0| l0.iter().rev());
        let lower = self.levels.iter().skip(1).flat_map(|level| level.iter());
        l0.chain(lower)
    }

    /// Total number of live `SSTable`s across every level. Used by
    /// the auto-compaction trigger and by Debug.
    pub(crate) fn total_sstables(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }
}

struct Inner {
    path: PathBuf,
    /// Active memtable, mutated on every put and delete.
    active: RwLock<MemTable>,
    /// LSM state (frozen memtable, sstables, next seq).
    state: RwLock<Arc<State>>,
    /// Append-only log; one writer at a time.
    wal: Mutex<Wal>,
    /// Pending non-durable WAL records since the last sync.
    pending_writes: AtomicUsize,
    /// Serialises flush and compact so two concurrent puts cannot
    /// race on `next_seq` or on the manifest write.
    mutation_lock: Mutex<()>,
    flush_threshold_bytes: usize,
    compaction_threshold: usize,
    commit_batch: usize,
    /// Set to `true` to ask the background flusher to stop. The
    /// thread also exits if `Weak::upgrade` returns `None`.
    flusher_stop: AtomicBool,
    /// Join handle for the background flusher; taken on Drop. Stored
    /// behind a `Mutex<Option<_>>` so it can be installed after the
    /// `Arc<Inner>` exists.
    flusher_join: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    /// Last-handle close. Stop the background flusher, then flush
    /// any pending non-durable WAL bytes so well-behaved programs
    /// do not lose acknowledged writes. Errors are logged because
    /// Drop cannot return them.
    fn drop(&mut self) {
        self.flusher_stop.store(true, Ordering::Release);
        if let Some(join) = self.flusher_join.get_mut().take() {
            join.thread().unpark();
            let _ = join.join();
        }
        if self.pending_writes.load(Ordering::Acquire) > 0
            && let Err(err) = self.wal.get_mut().sync_pending()
        {
            warn!(
                ?err,
                "lattice drop: flush_wal failed; non-durable writes may be lost"
            );
        }
    }
}

/// Background flusher loop body. Lives as a free function so it can
/// hold a `Weak<Inner>` and exit cleanly when the last `Arc<Inner>`
/// goes away (the upgrade returns `None`). The `Weak` is taken by
/// value because the closure passed to `thread::spawn` must own its
/// captures.
#[allow(clippy::needless_pass_by_value)]
fn flusher_loop(weak: Weak<Inner>, window: Duration) {
    let mut last_sync = Instant::now();
    loop {
        let elapsed = last_sync.elapsed();
        let to_sleep = window.saturating_sub(elapsed).max(Duration::from_millis(1));
        thread::park_timeout(to_sleep);

        let Some(inner) = weak.upgrade() else {
            break;
        };
        if inner.flusher_stop.load(Ordering::Acquire) {
            break;
        }

        let elapsed = last_sync.elapsed();
        if elapsed >= window && inner.pending_writes.load(Ordering::Acquire) > 0 {
            {
                let mut wal = inner.wal.lock();
                match wal.sync_pending() {
                    Ok(()) => {
                        inner.pending_writes.store(0, Ordering::Release);
                    }
                    Err(err) => {
                        warn!(?err, "background flusher: sync_pending failed");
                    }
                }
            }
            // Reset on success and on failure to back off; failures
            // already logged.
            last_sync = Instant::now();
        }
        // Drop the upgraded Arc here so Inner can be dropped if its
        // last clone goes away while we sleep next iteration.
        drop(inner);
    }
}

/// An open Lattice database.
///
/// Cheap to [`Clone`] (one `Arc` increment) and `Send + Sync`, so
/// multiple threads can hold a handle and read concurrently. Writes
/// (put, delete) serialise behind a single WAL mutex; reads run in
/// parallel.
pub struct Lattice {
    inner: Arc<Inner>,
}

impl Clone for Lattice {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for Lattice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.read();
        f.debug_struct("Lattice")
            .field("path", &self.inner.path)
            .field("sstables", &state.total_sstables())
            .field("levels", &state.levels.len())
            .field("memtable_bytes", &self.inner.active.read().approx_size())
            .field("next_seq", &state.next_seq)
            .field("flush_threshold_bytes", &self.inner.flush_threshold_bytes)
            .field("compaction_threshold", &self.inner.compaction_threshold)
            .finish_non_exhaustive()
    }
}

impl Lattice {
    /// Start a fluent builder for opening a database at `path`.
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

        let flat = manifest.flat_table_seqs();
        let live: BTreeSet<u64> = flat.iter().copied().collect();
        delete_orphans(&path, &live)?;

        // Reconstruct the per-level reader vectors from the manifest.
        // Empty levels at the tail are kept so Debug and the
        // compaction policy can see them.
        let mut levels: Vec<Vec<Arc<SSTableReader>>> = Vec::with_capacity(manifest.levels.len());
        for level_seqs in &manifest.levels {
            let mut readers = Vec::with_capacity(level_seqs.len());
            for seq in level_seqs {
                readers.push(Arc::new(SSTableReader::open(
                    &sstable_path(&path, *seq),
                    *seq,
                )?));
            }
            levels.push(readers);
        }

        let wal_path = path.join("wal.log");
        let (wal, entries) = Wal::open(&wal_path)?;
        let mut active = MemTable::new();
        for entry in entries {
            match entry {
                LogEntry::Put { key, value } => active.put(key, value),
                LogEntry::Delete { key } => active.delete(key),
            }
        }
        let total_sstables: usize = levels.iter().map(Vec::len).sum();
        info!(
            sstables = total_sstables,
            levels = levels.len(),
            next_seq = manifest.next_seq,
            path = %path.display(),
            "lattice opened"
        );

        let state = Arc::new(State {
            frozen: None,
            levels,
            next_seq: manifest.next_seq,
        });

        let inner = Arc::new(Inner {
            path,
            active: RwLock::new(active),
            state: RwLock::new(state),
            wal: Mutex::new(wal),
            pending_writes: AtomicUsize::new(0),
            mutation_lock: Mutex::new(()),
            flush_threshold_bytes,
            compaction_threshold,
            commit_batch,
            flusher_stop: AtomicBool::new(false),
            flusher_join: Mutex::new(None),
        });

        // Spawn the background flusher. It holds a `Weak<Inner>` so
        // it does not keep the engine alive on its own, and exits on
        // either `flusher_stop = true` or the last strong `Arc`
        // dropping.
        let weak = Arc::downgrade(&inner);
        let join = thread::Builder::new()
            .name("lattice-flusher".into())
            .spawn(move || flusher_loop(weak, commit_window))
            .expect("spawn lattice-flusher thread");
        *inner.flusher_join.lock() = Some(join);

        Ok(Self { inner })
    }

    /// Insert or overwrite a value for `key` with explicit per-write
    /// options. See [`WriteOptions`] for the durability trade-off.
    pub fn put_with(&self, key: &[u8], value: &[u8], opts: WriteOptions) -> Result<()> {
        let entry = LogEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.append_entry(&entry, opts)?;
        let LogEntry::Put { key, value } = entry else {
            unreachable!()
        };
        self.inner.active.write().put(key, value);
        self.maybe_flush()?;
        Ok(())
    }

    /// Insert or overwrite a value for `key`. Equivalent to
    /// `put_with(key, value, WriteOptions::default())`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with(key, value, WriteOptions::default())
    }

    /// Delete `key`. A subsequent `get` returns `None`. Always
    /// durable on return; non-durable deletes are not yet exposed.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let entry = LogEntry::Delete { key: key.to_vec() };
        self.append_entry(&entry, WriteOptions::default())?;
        let LogEntry::Delete { key } = entry else {
            unreachable!()
        };
        self.inner.active.write().delete(key);
        self.maybe_flush()?;
        Ok(())
    }

    /// Force a `fsync` of any pending non-durable WAL appends.
    /// Returns once the bytes are on stable storage. A no-op when
    /// nothing is pending.
    pub fn flush_wal(&self) -> Result<()> {
        if self.inner.pending_writes.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        self.inner.wal.lock().sync_pending()?;
        self.inner.pending_writes.store(0, Ordering::Release);
        Ok(())
    }

    fn append_entry(&self, entry: &LogEntry, opts: WriteOptions) -> Result<()> {
        // The whole append-and-maybe-sync runs under the WAL mutex
        // so the pending counter and the BufWriter stay consistent
        // with each other. The lock is released as soon as the
        // bytes are committed; the atomic reset that follows is
        // observable from any thread without it.
        let did_sync = {
            let mut wal = self.inner.wal.lock();
            wal.append_pending(entry)?;
            if opts.durable {
                wal.sync_pending()?;
                true
            } else {
                let prior = self.inner.pending_writes.fetch_add(1, Ordering::AcqRel);
                if prior + 1 >= self.inner.commit_batch {
                    wal.sync_pending()?;
                    true
                } else {
                    false
                }
            }
        };
        if did_sync {
            self.inner.pending_writes.store(0, Ordering::Release);
        }
        Ok(())
    }

    /// Read the current value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.active.read().lookup(key) {
            Lookup::Found(value) => return Ok(Some(value.to_vec())),
            Lookup::Tombstoned => return Ok(None),
            Lookup::Absent => {}
        }
        let state = self.inner.state.read().clone();
        if let Some(frozen) = &state.frozen {
            match frozen.lookup(key) {
                Lookup::Found(value) => return Ok(Some(value.to_vec())),
                Lookup::Tombstoned => return Ok(None),
                Lookup::Absent => {}
            }
        }
        for sst in state.all_sstables_newest_first() {
            match sst.get(key)? {
                SsLookup::Found(value) => return Ok(Some(value)),
                SsLookup::Tombstoned => return Ok(None),
                SsLookup::Absent => {}
            }
        }
        Ok(None)
    }

    /// Iterate live key-value pairs in key order. If `prefix` is
    /// `Some`, only keys starting with it are returned.
    pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut accumulator: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        for (key, value) in self.inner.active.read().iter_all() {
            if prefix.is_some_and(|p| !key.starts_with(p)) {
                continue;
            }
            accumulator.insert(key.to_vec(), value.map(<[u8]>::to_vec));
        }

        let state = self.inner.state.read().clone();
        if let Some(frozen) = &state.frozen {
            for (key, value) in frozen.iter_all() {
                if prefix.is_some_and(|p| !key.starts_with(p)) {
                    continue;
                }
                accumulator
                    .entry(key.to_vec())
                    .or_insert_with(|| value.map(<[u8]>::to_vec));
            }
        }

        for sst in state.all_sstables_newest_first() {
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
    pub fn flush(&self) -> Result<()> {
        let mutation_guard = self.inner.mutation_lock.lock();

        // Atomic move: drain `active` into an `Arc<MemTable>` and
        // install it as `state.frozen`. Reads during the SSTable
        // build see active=empty plus frozen=Some, so they still
        // observe the data they previously wrote.
        let (frozen_arc, seq) = {
            let mut active = self.inner.active.write();
            let mut state_g = self.inner.state.write();

            if active.is_empty() {
                return Ok(());
            }
            let seq = state_g.next_seq;
            let drained = Arc::new(std::mem::replace(&mut *active, MemTable::new()));
            let new_state = Arc::new(State {
                frozen: Some(Arc::clone(&drained)),
                levels: state_g.levels.clone(),
                next_seq: state_g.next_seq,
            });
            *state_g = new_state;
            drop(active);
            drop(state_g);
            (drained, seq)
        };

        // Build the SSTable from the frozen memtable.
        let final_path = sstable_path(&self.inner.path, seq);
        let tmp_path = self
            .inner
            .path
            .join(format!("{seq:0SSTABLE_DIGITS$}.sst.tmp"));
        let _ = fs::remove_file(&tmp_path);
        {
            let mut writer = SSTableWriter::create(&tmp_path, frozen_arc.len())?;
            for (key, value) in frozen_arc.iter_all() {
                writer.append(key.to_vec(), value.map(<[u8]>::to_vec))?;
            }
            writer.finish()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.inner.path)?;
        let reader = Arc::new(SSTableReader::open(&final_path, seq)?);

        // Install: clear `frozen`, append the new reader to L0,
        // bump seq. New tables from a memtable flush always land in
        // L0 because their key range may overlap any existing L0
        // table (last-writer-wins resolves it).
        {
            let mut state_g = self.inner.state.write();
            let mut new_levels = state_g.levels.clone();
            if new_levels.is_empty() {
                new_levels.push(Vec::new());
            }
            new_levels[0].push(reader);
            *state_g = Arc::new(State {
                frozen: None,
                levels: new_levels,
                next_seq: seq + 1,
            });
        }

        self.persist_manifest()?;

        // Truncate the WAL, since the memtable contents are now
        // durable in the SSTable. `pending_writes` resets because the
        // WAL is now empty.
        self.inner.wal.lock().truncate()?;
        self.inner.pending_writes.store(0, Ordering::Release);

        info!(seq, path = %final_path.display(), "sstable flushed");

        drop(mutation_guard);

        self.maybe_compact()
    }

    /// Run a compaction across every live `SSTable`, replacing them
    /// with a single merged table that drops tombstones. No-op if
    /// there are fewer than two tables.
    ///
    /// Phase 2 of M3 keeps the legacy "merge everything to one
    /// table" algorithm; the leveled algorithm lands in phase 3.
    /// The merged result goes back into L0 for now.
    pub fn compact(&self) -> Result<()> {
        let _mutation_guard = self.inner.mutation_lock.lock();

        // Snapshot every live reader across every level under a
        // brief read. Order is oldest first (L1 onward in level
        // order, then L0 in insertion order) because `compact_all`
        // is a last-writer-wins merge: later inputs override
        // earlier ones, and "later" here means "newer in the LSM".
        let (old_readers, new_seq) = {
            let state = self.inner.state.read();
            if state.total_sstables() < 2 {
                return Ok(());
            }
            let mut flat: Vec<Arc<SSTableReader>> = Vec::with_capacity(state.total_sstables());
            for level in state.levels.iter().skip(1) {
                flat.extend(level.iter().cloned());
            }
            if let Some(l0) = state.levels.first() {
                flat.extend(l0.iter().cloned());
            }
            (flat, state.next_seq)
        };

        // I/O outside the state lock so reads keep flowing.
        let final_path = sstable_path(&self.inner.path, new_seq);
        let tmp_path = self
            .inner
            .path
            .join(format!("{new_seq:0SSTABLE_DIGITS$}.sst.tmp"));
        let _ = fs::remove_file(&tmp_path);
        let readers: Vec<&SSTableReader> = old_readers.iter().map(Arc::as_ref).collect();
        compaction::compact_all(&readers, &tmp_path)?;
        drop(readers);
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.inner.path)?;
        let new_reader = Arc::new(SSTableReader::open(&final_path, new_seq)?);

        let old_seqs: Vec<u64> = old_readers.iter().map(|r| r.seq()).collect();

        // Install: replace every level with a single L0 entry, bump
        // next_seq, leave `frozen` alone (a flush in flight is
        // independent of compaction).
        {
            let mut state_g = self.inner.state.write();
            *state_g = Arc::new(State {
                frozen: state_g.frozen.clone(),
                levels: vec![vec![new_reader]],
                next_seq: new_seq + 1,
            });
        }
        // Drop our local clones of the old `Arc<SSTableReader>`s so
        // that on POSIX the file removals below actually unlink
        // (snapshots that still hold them keep the inode alive).
        drop(old_readers);

        self.persist_manifest()?;

        for seq in old_seqs {
            let p = sstable_path(&self.inner.path, seq);
            if let Err(err) = fs::remove_file(&p) {
                warn!(
                    ?err,
                    path = %p.display(),
                    "could not delete old sstable (likely held by a live Snapshot on Windows; cleaned up on next open)"
                );
            }
        }
        info!(new_seq, "compaction complete");
        Ok(())
    }

    /// Open a read-only point-in-time view of the database.
    ///
    /// The snapshot sees the exact set of live keys at the time of
    /// the call. Subsequent `put`, `delete`, `flush`, and `compact`
    /// calls on the parent do not change what the snapshot sees.
    pub fn snapshot(&self) -> Snapshot {
        let state_arc = self.inner.state.read().clone();
        let active_clone = self.inner.active.read().clone();

        // Merge `frozen` into the snapshot's memtable: keys in
        // `active` win because they are newer; keys present only in
        // `frozen` come from before the in-flight flush. The merged
        // result feeds the existing `Snapshot` API unchanged.
        let merged = if let Some(frozen) = &state_arc.frozen {
            let mut merged = active_clone;
            for (key, value) in frozen.iter_all() {
                if matches!(merged.lookup(key), Lookup::Absent) {
                    match value {
                        Some(v) => merged.put(key.to_vec(), v.to_vec()),
                        None => merged.delete(key.to_vec()),
                    }
                }
            }
            merged
        } else {
            active_clone
        };

        Snapshot {
            memtable: merged,
            levels: state_arc.levels.clone(),
        }
    }

    /// Path to the database directory.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn maybe_flush(&self) -> Result<()> {
        let size = self.inner.active.read().approx_size();
        if size >= self.inner.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn maybe_compact(&self) -> Result<()> {
        let count = self.inner.state.read().total_sstables();
        if count >= self.inner.compaction_threshold {
            self.compact()?;
        }
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let state = self.inner.state.read().clone();
        let manifest_levels: Vec<Vec<u64>> = state
            .levels
            .iter()
            .map(|level| level.iter().map(|r| r.seq()).collect())
            .collect();
        let manifest = Manifest {
            version: crate::manifest::MANIFEST_VERSION,
            next_seq: state.next_seq,
            levels: manifest_levels,
        };
        manifest.save(&self.inner.path)
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
        version: crate::manifest::MANIFEST_VERSION,
        next_seq,
        levels: vec![seqs],
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
