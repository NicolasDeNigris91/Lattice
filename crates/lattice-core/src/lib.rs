//! Lattice, an LSM-tree key-value storage engine.
//!
//! This crate exposes a small embeddable key-value store backed by a write
//! ahead log, an in-memory memtable, sorted string tables, bloom filters,
//! tiered compaction, and snapshots.
//!
//! See the companion book at <https://lattice.nicolaspilegidenigris.dev>
//! for a chapter-by-chapter explanation of every component.

#![forbid(unsafe_code)]

#[cfg(feature = "tokio")]
mod async_api;
mod bloom;
mod compaction;
mod compactor;
// `conflict_tracker` is internal to the engine, but the `loom` test
// crate needs cross-crate access to drive its model checks. The
// `cfg(loom)` build flips the visibility to `pub`; default builds
// keep the symbol crate-private so `cargo public-api` does not
// register it as part of the public surface.
#[cfg(loom)]
pub mod conflict_tracker;
#[cfg(not(loom))]
pub(crate) mod conflict_tracker;
mod error;
mod manifest;
mod memtable;
mod metrics_compat;
mod scan_iter;
mod snapshot;
mod sstable;
mod transaction;
mod wal;

#[cfg(feature = "tokio")]
pub use crate::async_api::AsyncLattice;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tracing::{info, instrument, warn};

pub use crate::compactor::CompactionHandle;
use crate::compactor::CompactorShared;
use crate::conflict_tracker::ConflictTracker;
pub use crate::error::{Error, Result};
use crate::manifest::Manifest;
use crate::memtable::{Lookup, MemTable};
pub use crate::scan_iter::ScanIter;
pub use crate::snapshot::Snapshot;
use crate::sstable::{SSTableReader, SSTableWriter, SsLookup};
pub use crate::transaction::Transaction;
use crate::wal::{LogEntry, Wal};

// The conflict-detection state (`write_seq`, `last_writes`,
// `active_tx`) and its trim threshold live behind
// [`ConflictTracker`]. The engine delegates every bump, lookup,
// and trim through that module so the loom suite under
// `lattice-loom-tests` exercises the same code path as production.

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

/// Default number of `SSTable`s in a single level before an
/// auto-compaction is triggered for that level. Tunable via
/// [`LatticeBuilder::compaction_threshold`].
const DEFAULT_COMPACTION_THRESHOLD: usize = 4;

/// Maximum LSM level depth. The leveled algorithm stops cascading
/// once it reaches this depth; in practice the dataset would have
/// to be petabytes for this to fire on the default fan-out, but
/// the guard prevents unbounded recursion in pathological tests.
const MAX_LEVELS: usize = 7;

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
    /// Iterate every live `SSTable`, newest first. Each level is
    /// walked end-to-start so the most recently installed table in
    /// the level wins under last-writer-wins. L0 is the freshest
    /// level overall, then L1, then L2, etc. The size-tiered
    /// algorithm in v1.3 still produces overlapping tables within
    /// L1+, so the reverse walk inside every level is load-bearing.
    pub(crate) fn all_sstables_newest_first(
        &self,
    ) -> impl Iterator<Item = &Arc<SSTableReader>> + '_ {
        self.levels.iter().flat_map(|level| level.iter().rev())
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
    /// Snapshot-isolation conflict tracker. Owns the monotonic
    /// `write_seq` counter, the `key -> last seq` map, and the
    /// in-flight `snapshot_seq` multiset. The engine delegates
    /// every bump, conflict check, and trim through the tracker
    /// so the loom model checks under `lattice-loom-tests`
    /// exercise the same code path as production.
    tracker: ConflictTracker,
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
    /// Shared state for the non-blocking compaction worker added in
    /// v1.13. `Lattice::compact_async` schedules a round here; the
    /// worker holds a `Weak<Inner>` and exits when the last strong
    /// `Arc` drops or when `shutdown` is set in `Drop`.
    compactor: Arc<CompactorShared>,
    /// Join handle for the background compactor; taken on Drop.
    compactor_join: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    /// Last-handle close. Stop the background flusher and the
    /// background compactor, then flush any pending non-durable
    /// WAL bytes so well-behaved programs do not lose
    /// acknowledged writes. Errors are logged because Drop cannot
    /// return them.
    fn drop(&mut self) {
        self.flusher_stop.store(true, Ordering::Release);
        if let Some(join) = self.flusher_join.get_mut().take() {
            join.thread().unpark();
            let _ = join.join();
        }
        // Tell the compactor to exit at its next loop boundary,
        // wake it from any in-flight wait, and join.
        self.compactor.shutdown();
        if let Some(join) = self.compactor_join.get_mut().take() {
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

/// Background compactor loop body. Holds `CompactorShared` so it
/// can wait on the condvar without pinning `Inner` alive; the
/// `Weak<Inner>` is upgraded only while a round is in flight.
///
/// Exits when:
/// - `CompactorShared::shutdown()` is called (by `Inner::Drop`).
/// - The last `Arc<Inner>` drops between rounds, so the upgrade
///   inside the loop returns `None`.
#[allow(clippy::needless_pass_by_value)]
fn compactor_loop(weak: Weak<Inner>, shared: Arc<CompactorShared>) {
    while let Some(target) = shared.next_request() {
        let Some(inner) = weak.upgrade() else { break };
        // Wrap the inner in a temporary Lattice so we can reuse the
        // existing public-facing compaction loop. The wrapper bumps
        // the strong count for the duration of the round; we drop
        // it before going back to wait so `Inner::Drop` can fire.
        let lattice = Lattice {
            inner: Arc::clone(&inner),
        };
        let result = lattice.run_pending_compactions();
        shared.finish(target, result);
        drop(lattice);
        drop(inner);
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
///
/// # Example
///
/// ```
/// use lattice_core::Lattice;
///
/// let dir = tempfile::tempdir()?;
/// let db = Lattice::open(dir.path())?;
/// db.put(b"hello", b"world")?;
/// assert_eq!(db.get(b"hello")?.as_deref(), Some(b"world".as_slice()));
///
/// // Cloning is cheap; both handles see the same database.
/// let db_for_reader = db.clone();
/// std::thread::spawn(move || {
///     let _ = db_for_reader.get(b"hello");
/// })
/// .join()
/// .unwrap();
/// # Ok::<_, Box<dyn std::error::Error>>(())
/// ```
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
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    ///
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// db.put(b"k", b"v")?;
    /// drop(db);
    ///
    /// // Reopen against the same path: every durable write replays
    /// // from the WAL.
    /// let db = Lattice::open(dir.path())?;
    /// assert_eq!(db.get(b"k")?.as_deref(), Some(b"v".as_slice()));
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[instrument(level = "info", skip_all, fields(path = %path.as_ref().display()))]
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
            tracker: ConflictTracker::new(),
            mutation_lock: Mutex::new(()),
            flush_threshold_bytes,
            compaction_threshold,
            commit_batch,
            flusher_stop: AtomicBool::new(false),
            flusher_join: Mutex::new(None),
            compactor: Arc::new(CompactorShared::new()),
            compactor_join: Mutex::new(None),
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

        // Spawn the background compactor. The worker holds the
        // compactor's shared state directly (so it can wait on the
        // condvar without keeping `Inner` alive) plus a `Weak<Inner>`
        // it upgrades only while a round is actually in flight. Exits
        // on `compactor.shutdown()` (set by `Inner::Drop`) or when
        // the last strong `Arc<Inner>` goes away.
        let weak = Arc::downgrade(&inner);
        let compactor_state = Arc::clone(&inner.compactor);
        let join = thread::Builder::new()
            .name("lattice-compactor".into())
            .spawn(move || compactor_loop(weak, compactor_state))
            .expect("spawn lattice-compactor thread");
        *inner.compactor_join.lock() = Some(join);

        Ok(Self { inner })
    }

    /// Insert or overwrite a value for `key` with explicit per-write
    /// options. See [`WriteOptions`] for the durability trade-off.
    #[instrument(
        level = "debug",
        skip(self, value),
        fields(key_len = key.len(), value_len = value.len(), durable = opts.durable),
    )]
    pub fn put_with(&self, key: &[u8], value: &[u8], opts: WriteOptions) -> Result<()> {
        let started = Instant::now();
        let entry = LogEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let needs_flush = {
            let mut wal = self.inner.wal.lock();
            self.apply_entry_locked(&mut wal, &entry, opts)?
        };
        if needs_flush {
            self.flush()?;
        }
        self.maybe_trim_last_writes();
        metrics_compat::record_put(started.elapsed());
        Ok(())
    }

    /// Insert or overwrite a value for `key`. Equivalent to
    /// `put_with(key, value, WriteOptions::default())`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with(key, value, WriteOptions::default())
    }

    /// Delete `key`. A subsequent `get` returns `None`. Always
    /// durable on return; non-durable deletes are not yet exposed.
    #[instrument(level = "debug", skip(self), fields(key_len = key.len()))]
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let started = Instant::now();
        let entry = LogEntry::Delete { key: key.to_vec() };
        let needs_flush = {
            let mut wal = self.inner.wal.lock();
            self.apply_entry_locked(&mut wal, &entry, WriteOptions::default())?
        };
        if needs_flush {
            self.flush()?;
        }
        self.maybe_trim_last_writes();
        metrics_compat::record_delete(started.elapsed());
        Ok(())
    }

    /// Force a `fsync` of any pending non-durable WAL appends.
    /// Returns once the bytes are on stable storage. A no-op when
    /// nothing is pending.
    #[instrument(level = "debug", skip(self))]
    pub fn flush_wal(&self) -> Result<()> {
        if self.inner.pending_writes.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        self.inner.wal.lock().sync_pending()?;
        self.inner.pending_writes.store(0, Ordering::Release);
        Ok(())
    }

    /// Apply a single WAL entry under a pre-locked WAL guard.
    /// Returns `true` if the active memtable now exceeds the flush
    /// threshold (the caller must call `flush()` AFTER releasing
    /// the WAL guard, since `flush()` re-acquires the WAL guard
    /// internally for the truncate).
    ///
    /// This helper exists so that transaction commit can take the
    /// WAL guard once and atomically check `last_writes` against the
    /// transaction's snapshot before applying every staged write.
    /// The bump of `write_seq` and the update of `last_writes`
    /// happen here, both under the WAL mutex, so any concurrent
    /// transaction commit that holds the same guard sees a
    /// consistent (`write_seq`, `last_writes`) pair.
    fn apply_entry_locked(
        &self,
        wal: &mut Wal,
        entry: &LogEntry,
        opts: WriteOptions,
    ) -> Result<bool> {
        wal.append_pending(entry)?;
        let did_sync = if opts.durable {
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
        };
        if did_sync {
            self.inner.pending_writes.store(0, Ordering::Release);
        }

        // Track the per-key write sequence used by transaction
        // conflict detection. `record_write` bumps `write_seq` and
        // inserts into `last_writes` under a single tracker-internal
        // lock, so the pair is observed atomically by any reader
        // (including a concurrent transaction commit's check).
        let key_for_tracking = match entry {
            LogEntry::Put { key, .. } | LogEntry::Delete { key } => key.clone(),
        };
        self.inner.tracker.record_write(key_for_tracking);

        // Apply to the memtable.
        let needs_flush = {
            let mut active = self.inner.active.write();
            match entry {
                LogEntry::Put { key, value } => active.put(key.clone(), value.clone()),
                LogEntry::Delete { key } => active.delete(key.clone()),
            }
            active.approx_size() >= self.inner.flush_threshold_bytes
        };
        Ok(needs_flush)
    }

    /// Read the current value for `key`, or `None` if absent or deleted.
    #[instrument(level = "trace", skip(self), fields(key_len = key.len()))]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let started = Instant::now();
        let value = self.get_inner(key)?;
        metrics_compat::record_get(started.elapsed(), value.is_some());
        Ok(value)
    }

    fn get_inner(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
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
    /// `Some`, only keys starting with it are returned. Returns the
    /// full result as a `Vec`; callers that prefer to walk the
    /// keyspace one entry at a time should use [`Self::scan_iter`].
    #[instrument(level = "debug", skip(self), fields(prefix_len = prefix.map_or(0, <[u8]>::len)))]
    pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_iter(prefix).collect()
    }

    /// Streaming variant of [`Self::scan`]. Returns an iterator that
    /// merges the active memtable, the frozen memtable, and every
    /// `SSTable` on disk through a k-way heap merge, yielding live
    /// `(key, value)` pairs in strictly increasing key order. The
    /// engine holds only the merge frontier (one entry per source)
    /// plus one decoded block per `SSTable` source, so memory is
    /// independent of the total number of keys in the database.
    ///
    /// `prefix` filters the keyspace at every source, so a tightly
    /// scoped scan reads only the blocks whose keys can match.
    ///
    /// Errors from block reads or parse failures surface as
    /// `Some(Err(...))` items; callers can choose to abort or
    /// continue.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    ///
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// db.put(b"a", b"1")?;
    /// db.put(b"b", b"2")?;
    /// db.put(b"c", b"3")?;
    ///
    /// let mut keys = Vec::new();
    /// for entry in db.scan_iter(None) {
    ///     let (k, _) = entry?;
    ///     keys.push(k);
    /// }
    /// assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[instrument(level = "debug", skip(self), fields(prefix_len = prefix.map_or(0, <[u8]>::len)))]
    pub fn scan_iter(&self, prefix: Option<&[u8]>) -> ScanIter {
        let active_guard = self.inner.active.read();
        let state = self.inner.state.read().clone();
        let sstables: Vec<Arc<SSTableReader>> =
            state.all_sstables_newest_first().cloned().collect();
        ScanIter::new(&active_guard, state.frozen.as_deref(), sstables, prefix)
    }

    /// Flush the current memtable to a new on-disk `SSTable`, then
    /// truncate the WAL. No-op if the memtable is empty.
    #[instrument(level = "info", skip(self))]
    pub fn flush(&self) -> Result<()> {
        let started = Instant::now();
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
        metrics_compat::record_flush(started.elapsed());

        drop(mutation_guard);

        self.maybe_compact()
    }

    /// Run leveled compaction until no level holds more than one
    /// `SSTable`. Synchronous wrapper around [`Self::compact_async`]
    /// that blocks the caller until the background worker reports
    /// completion.
    ///
    /// Each round picks the shallowest level above the per-level
    /// threshold (or any level above two tables, when the caller
    /// forces a full compaction), merges every table in that level
    /// into a single output, and pushes the output to the next
    /// level down. After `compact()` the database has at most one
    /// `SSTable` per non-empty level.
    ///
    /// Internally the algorithm only rewrites one level's bytes per
    /// round, so write amplification scales with the number of
    /// levels (~`log_T(N)`) rather than the total dataset size.
    #[instrument(level = "info", skip(self))]
    pub fn compact(&self) -> Result<()> {
        self.compact_async().wait()
    }

    /// Schedule a leveled-compaction round to run on the background
    /// compactor thread and return immediately. The returned
    /// [`CompactionHandle`] can be `wait()`-ed on to observe the
    /// outcome, or dropped (the round still runs and any error
    /// surfaces on the next compact call).
    ///
    /// Multiple concurrent calls coalesce: the worker captures the
    /// latest scheduled generation when it wakes and runs as many
    /// rounds as the level layout requires before publishing the
    /// captured generation as completed. Every caller whose
    /// handle's generation is no greater than the captured value
    /// sees `wait()` return.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// let handle = db.compact_async();
    /// // ... do other work; the round is in flight ...
    /// handle.wait()?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[instrument(level = "info", skip(self))]
    pub fn compact_async(&self) -> CompactionHandle {
        let target = self.inner.compactor.schedule();
        CompactionHandle {
            shared: Arc::clone(&self.inner.compactor),
            target_generation: target,
        }
    }

    /// Body of the compaction loop run by the background worker.
    /// Same algorithm as the pre-v1.13 synchronous `compact()`:
    /// pick the shallowest level with at least two tables and
    /// compact it down, repeat until every level holds at most one
    /// table.
    fn run_pending_compactions(&self) -> Result<()> {
        loop {
            let target = {
                let state = self.inner.state.read();
                state
                    .levels
                    .iter()
                    .position(|level| level.len() >= 2)
                    .filter(|&idx| idx + 1 < MAX_LEVELS)
            };
            match target {
                Some(idx) => self.compact_level(idx)?,
                None => break,
            }
        }
        Ok(())
    }

    /// Compact every `SSTable` in `level_idx` into a single table
    /// pushed to `level_idx + 1`. Internal helper used by both the
    /// auto-compaction trigger and the user-facing `compact()`.
    fn compact_level(&self, level_idx: usize) -> Result<()> {
        let started = Instant::now();
        let _mutation_guard = self.inner.mutation_lock.lock();

        // Snapshot the source level. Sources are taken in insertion
        // order (oldest first) because `compact_all` is a
        // last-writer-wins merge that needs newer inputs later.
        // We also check whether the target level (level_idx + 1) is
        // the bottom of the LSM right now: if no deeper level has
        // any tables, tombstones in the merged output have nothing
        // older to shadow and are safe to drop.
        let (sources, new_seq, drop_tombstones) = {
            let state = self.inner.state.read();
            let Some(level) = state.levels.get(level_idx) else {
                return Ok(());
            };
            if level.len() < 2 {
                return Ok(());
            }
            let target_level = level_idx + 1;
            // Tombstones in the source can shadow data physically
            // resident in the target level (we do not yet merge with
            // target-level tables) or in any deeper level. Drop them
            // only when no level at or below target holds any
            // tables; otherwise keep them so reads still see the
            // delete.
            let drop_tombstones = state.levels.iter().skip(target_level).all(Vec::is_empty);
            (level.clone(), state.next_seq, drop_tombstones)
        };

        // I/O outside any state lock.
        let final_path = sstable_path(&self.inner.path, new_seq);
        let tmp_path = self
            .inner
            .path
            .join(format!("{new_seq:0SSTABLE_DIGITS$}.sst.tmp"));
        let _ = fs::remove_file(&tmp_path);
        let readers: Vec<&SSTableReader> = sources.iter().map(Arc::as_ref).collect();
        compaction::compact_all(&readers, &tmp_path, drop_tombstones)?;
        drop(readers);
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.inner.path)?;
        let new_reader = Arc::new(SSTableReader::open(&final_path, new_seq)?);

        let old_seqs: Vec<u64> = sources.iter().map(|r| r.seq()).collect();

        // Install: empty the source level, push the merged output to
        // the next level down (creating that level if needed).
        // `frozen` and other levels are unchanged.
        {
            let mut state_g = self.inner.state.write();
            let mut new_levels = state_g.levels.clone();
            if level_idx >= new_levels.len() {
                return Ok(());
            }
            new_levels[level_idx] = Vec::new();
            while new_levels.len() <= level_idx + 1 {
                new_levels.push(Vec::new());
            }
            new_levels[level_idx + 1].push(new_reader);
            *state_g = Arc::new(State {
                frozen: state_g.frozen.clone(),
                levels: new_levels,
                next_seq: new_seq + 1,
            });
        }
        // Drop our local clones so the file removals below can
        // unlink on POSIX (snapshots still holding them keep the
        // inode alive, which is fine).
        drop(sources);

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
        info!(
            from_level = level_idx,
            to_level = level_idx + 1,
            new_seq,
            "level compacted"
        );
        metrics_compat::record_compaction(started.elapsed());
        Ok(())
    }

    /// Open a read-only point-in-time view of the database.
    ///
    /// The snapshot sees the exact set of live keys at the time of
    /// the call. Subsequent `put`, `delete`, `flush`, and `compact`
    /// calls on the parent do not change what the snapshot sees.
    #[instrument(level = "debug", skip(self))]
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

    /// Run a closure inside a snapshot-isolated transaction. Reads
    /// inside the closure see the database as of the transaction
    /// start, layered on top of the transaction's own staged
    /// writes. On `Ok` return, every staged write is applied
    /// atomically after a conflict check; on `Err` (or panic) the
    /// staged writes are discarded.
    ///
    /// The commit aborts with [`Error::TransactionConflict`] if any
    /// key in the transaction's read-set or write-set was modified
    /// by another writer after the transaction's snapshot was
    /// taken. The standard recovery is to retry the closure against
    /// a fresh snapshot.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::{Error, Lattice};
    ///
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    ///
    /// db.transaction(|tx| {
    ///     // Reads see the database as of transaction start.
    ///     if tx.get(b"counter")?.is_none() {
    ///         tx.put(b"counter", b"0");
    ///     }
    ///     tx.put(b"last_seen", b"alice");
    ///     Ok::<_, Error>(())
    /// })?;
    ///
    /// assert_eq!(db.get(b"counter")?.as_deref(), Some(b"0".as_slice()));
    /// assert_eq!(db.get(b"last_seen")?.as_deref(), Some(b"alice".as_slice()));
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[instrument(level = "info", skip_all)]
    pub fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<R>,
    {
        let started = Instant::now();
        let snapshot = self.snapshot();
        // The guard holds the smallest atomic step: register
        // `snapshot_seq` in the active-tx multiset under
        // `active_tx.lock()` and capture the same `write_seq` that
        // was active at that lock. A concurrent trim observes this
        // registration before it computes its cutoff, so any
        // entry whose `seq` could still trigger our conflict check
        // is preserved in `last_writes`.
        let active_guard = self.register_active_tx();
        let snapshot_seq = active_guard.snapshot_seq;
        let mut tx = Transaction::new(snapshot, snapshot_seq);
        let outcome = f(&mut tx)?;

        // Atomic check-then-apply under the WAL mutex. Holding it
        // through both phases prevents any plain put or delete from
        // advancing `write_seq` or `last_writes` between our check
        // and our apply, so a concurrent overwrite cannot slip past
        // the conflict guard.
        let needs_flush = {
            let mut wal = self.inner.wal.lock();

            // Conflict check. The tracker walks every key in the
            // transaction's read and write sets in one pass,
            // returning early when one has a `last_seq >
            // snapshot_seq`. The borrow chain avoids an
            // intermediate `Vec` allocation per commit.
            let conflict_keys = tx
                .read_set
                .iter()
                .chain(tx.write_set.keys())
                .map(Vec::as_slice);
            if self
                .inner
                .tracker
                .check_conflict(tx.snapshot_seq, conflict_keys)
            {
                metrics_compat::record_transaction_conflict();
                return Err(Error::TransactionConflict);
            }

            // Apply phase.
            let mut needs_flush = false;
            for (key, value) in &tx.write_set {
                let entry = value.as_ref().map_or_else(
                    || LogEntry::Delete { key: key.clone() },
                    |v| LogEntry::Put {
                        key: key.clone(),
                        value: v.clone(),
                    },
                );
                needs_flush |=
                    self.apply_entry_locked(&mut wal, &entry, WriteOptions::default())?;
            }
            needs_flush
        };
        if needs_flush {
            self.flush()?;
        }
        // Drop the active-tx registration BEFORE the trim runs, so
        // the trim's cutoff is not pinned by this transaction's
        // (now-completed) snapshot. The guard's Drop deregisters
        // under `active_tx.lock()`.
        drop(active_guard);
        self.maybe_trim_last_writes();
        metrics_compat::record_transaction_commit(started.elapsed());
        Ok(outcome)
    }

    fn maybe_compact(&self) -> Result<()> {
        // Auto-compaction picks the shallowest level above the
        // per-level threshold and runs one round inline on the
        // writer's thread. Cascading happens gradually: each
        // subsequent flush re-checks and runs the next round if
        // the cascade overflowed. This keeps the writer's
        // worst-case latency bounded by a single round.
        //
        // Pre-v1.13 the user-facing `compact()` did the same; v1.13
        // adds [`Self::compact_async`] for callers that want
        // non-blocking compaction. Auto-compaction stays inline so
        // a fresh `Lattice::open` followed by `flush` leaves the
        // database in a deterministically settled state, which the
        // existing integration tests (and many user reopens) rely
        // on. Callers running heavy write workloads can disable
        // implicit auto-compaction (a v2.x feature) and drive
        // `compact_async` from their own scheduler.
        let target = {
            let state = self.inner.state.read();
            state
                .levels
                .iter()
                .position(|level| level.len() >= self.inner.compaction_threshold)
                .filter(|&idx| idx + 1 < MAX_LEVELS)
        };
        if let Some(idx) = target {
            self.compact_level(idx)?;
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

    /// Register a new in-flight transaction by delegating to the
    /// conflict tracker. The tracker captures the current
    /// `write_seq` as the transaction's `snapshot_seq` and bumps
    /// the active multiset entry under a single lock; the returned
    /// guard decrements on drop. Must run before the transaction's
    /// snapshot is taken so any concurrent trim observes this
    /// registration and preserves entries the commit might need.
    fn register_active_tx(&self) -> ActiveTxGuard<'_> {
        let snapshot_seq = self.inner.tracker.begin_tx();
        ActiveTxGuard {
            tracker: &self.inner.tracker,
            snapshot_seq,
        }
    }

    /// Delegates to the tracker's `maybe_trim`. See
    /// [`ConflictTracker::maybe_trim`] for the cutoff rule.
    fn maybe_trim_last_writes(&self) {
        self.inner.tracker.maybe_trim();
    }
}

/// Drops a transaction's `snapshot_seq` from the tracker's active
/// multiset on scope exit, including panic unwinding. Holding a
/// borrow of the tracker (rather than a callback closure) keeps the
/// guard `Send + Sync` and zero-allocation.
struct ActiveTxGuard<'a> {
    tracker: &'a ConflictTracker,
    snapshot_seq: u64,
}

impl Drop for ActiveTxGuard<'_> {
    fn drop(&mut self) {
        self.tracker.end_tx(self.snapshot_seq);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::{Error, Lattice};
    use crate::conflict_tracker::LAST_WRITES_TRIM_THRESHOLD;
    use tempfile::tempdir;

    #[test]
    fn last_writes_does_not_grow_unbounded_without_active_transactions() {
        // The conflict-detection map (`Inner::last_writes`) records
        // the latest `write_seq` per key. With no active transaction,
        // no entry is load-bearing for a future commit (any future
        // transaction starts at a snapshot_seq >= the current
        // `write_seq`, so every existing entry's `seq <= snapshot_seq`,
        // i.e. cannot trigger a conflict).
        //
        // Without trimming, the map grows by one entry per unique
        // key forever. v1.10 introduces a size-triggered trim. After
        // 2 * threshold non-transactional writes, the map MUST be
        // bounded by the threshold; the precise count depends on the
        // trim cadence, but unbounded growth (= 2 * threshold
        // entries) is the failure we are pinning out.
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();

        for i in 0..(2 * LAST_WRITES_TRIM_THRESHOLD) {
            let key = format!("k{i:08}");
            db.put(key.as_bytes(), b"v").unwrap();
        }

        let len = db.inner.tracker.last_writes_len();
        assert!(
            len <= LAST_WRITES_TRIM_THRESHOLD,
            "last_writes must be bounded by the trim threshold; got {len}, expected <= {LAST_WRITES_TRIM_THRESHOLD}",
        );
    }

    #[test]
    fn trim_does_not_drop_entries_an_active_transaction_still_needs() {
        // Soundness invariant: while a transaction is in flight,
        // any entry whose `seq` is greater than that transaction's
        // `snapshot_seq` must survive a trim, because the
        // transaction's commit may yet need to detect a conflict
        // against it. The thread spawning + barrier-free
        // synchronisation here pins this end-to-end: T1 starts and
        // reads `K`, T2 then does enough non-transactional writes
        // to push `last_writes` well past the trim threshold (and
        // also overwrites `K`), and T1 commits. The commit MUST
        // abort with `Error::TransactionConflict`; if the trim ate
        // the entry for `K`, the conflict goes undetected and T1
        // commits silently over T2's overwrite, which is the
        // lost-update bug v1.6 closed.
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"v0").unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let t2_done = Arc::new(AtomicBool::new(false));

        let t1 = {
            let db = db.clone();
            let started = Arc::clone(&started);
            let t2_done = Arc::clone(&t2_done);
            thread::spawn(move || {
                db.transaction(|tx| {
                    assert_eq!(tx.get(b"k").unwrap(), Some(b"v0".to_vec()));
                    started.store(true, Ordering::Release);
                    while !t2_done.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    tx.put(b"k", b"v_t1");
                    Ok::<_, Error>(())
                })
            })
        };

        // Wait for T1 to be inside its transaction with `k` in its
        // read-set, then do enough non-transactional writes to
        // push `last_writes` past the trim threshold AND overwrite
        // `k`. The trim that fires during these writes must
        // observe T1's registration in `active_tx` and refrain
        // from dropping the entry for `k` (whose `seq` is now
        // greater than T1's `snapshot_seq`).
        while !started.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }
        for i in 0..(2 * LAST_WRITES_TRIM_THRESHOLD) {
            let key = format!("noise{i:08}");
            db.put(key.as_bytes(), b"x").unwrap();
        }
        db.put(b"k", b"v_t2").unwrap();
        t2_done.store(true, Ordering::Release);

        let r1 = t1.join().unwrap();
        assert!(
            matches!(r1, Err(Error::TransactionConflict)),
            "T1 commit must abort with TransactionConflict, got {r1:?}",
        );
        assert_eq!(db.get(b"k").unwrap(), Some(b"v_t2".to_vec()));
    }
}
