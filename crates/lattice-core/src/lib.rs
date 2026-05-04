//! Lattice: an embeddable LSM-tree key-value storage engine.
//!
//! Lattice is a small, single-process, ordered byte-key
//! byte-value store built from scratch in safe Rust. It is
//! designed for learning and as a portfolio piece, but it is
//! tested as if it were a production system: 90+ unit and
//! integration tests, a four-pillar property fence
//! (replay-on-reopen, snapshot isolation, compaction
//! equivalence, transactional rollback), three cargo-fuzz
//! targets against the on-disk decoders, and a `loom` model
//! checker that drives the conflict tracker and the background
//! compactor under every legal interleaving.
//!
//! ## Quick start
//!
//! ```
//! use lattice_core::Lattice;
//!
//! let dir = tempfile::tempdir()?;
//! let db = Lattice::open(dir.path())?;
//!
//! db.put(b"alpha", b"first")?;
//! db.put(b"bravo", b"second")?;
//! assert_eq!(db.get(b"alpha")?.as_deref(), Some(b"first".as_slice()));
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! See the [`examples`](https://github.com/NicolasDeNigris91/Lattice/tree/main/crates/lattice-core/examples)
//! directory for runnable programs covering the basic key-value
//! API, snapshot-isolated transactions, point-in-time
//! snapshots, the streaming scan iterator, and non-blocking
//! background compaction.
//!
//! ## Architecture
//!
//! At the storage layer Lattice composes:
//!
//! - A **write-ahead log** (`wal`) that every mutation hits
//!   first, with optional per-write durability via
//!   [`WriteOptions`].
//! - An in-memory **memtable** (`memtable`) — a `BTreeMap`
//!   behind a `parking_lot::RwLock`.
//! - On-disk **sorted string tables** (`sstable`) with a sparse
//!   index, LZ4-compressed blocks, and a per-table bloom filter.
//! - **Inventory and fingerprinting** ([`Lattice::byte_size_on_disk`],
//!   [`Lattice::checksum`]) for capacity dashboards and
//!   cross-host divergence detection. The fingerprint is an
//!   xxh3-64 hash over the visible key/value set in ascending
//!   key order; it is invariant under `flush` and `compact`,
//!   so two replicas on the same logical state agree
//!   regardless of how each got there.
//! - **Strict-leveled compaction** (`compaction`) that picks
//!   the shallowest level above its threshold and rewrites it
//!   together with the overlapping subset of the next level
//!   down. Non-overlapping tables in the target level are kept
//!   in place, so each compaction pays only for the bytes whose
//!   key range actually changed. Runs synchronously
//!   ([`Lattice::compact`]) or on a dedicated background thread
//!   ([`Lattice::compact_async`]).
//!
//! On top of that storage layer it exposes:
//!
//! - **Snapshots** (`snapshot`) — read-only point-in-time views
//!   that pin the memtables and `SSTable` readers they need.
//! - **Transactions** (`transaction`) — snapshot-isolated
//!   read-modify-write closures with per-key conflict detection
//!   via an internal `ConflictTracker` module (loom-tested).
//! - A **streaming scan iterator** ([`Lattice::scan_iter`]) that
//!   yields visible `(key, value)` pairs through a `BinaryHeap`
//!   k-way merge over every LSM tier.
//!
//! ## Public API surface
//!
//! - [`Lattice`] / [`LatticeBuilder`] — opening, configuring,
//!   reading, writing, scanning.
//! - [`Snapshot`] — point-in-time read view.
//! - [`Transaction`] — snapshot-isolated commit closure.
//! - [`ScanIter`] — streaming scan iterator.
//! - [`CompactionHandle`] — handle returned by `compact_async`.
//! - [`WriteOptions`] — per-write durability knob.
//! - [`Error`] / [`Result`] — error type.
//! - [`AsyncLattice`] — `tokio` feature-gated wrapper that
//!   shifts blocking work onto `spawn_blocking`.
//!
//! Everything else is `pub(crate)` and not part of the
//! [SemVer](https://semver.org/) contract. The [public-API diff
//! CI job][pubapi] flags any change to this surface so it stays
//! intentional.
//!
//! [pubapi]: https://github.com/NicolasDeNigris91/Lattice/blob/main/.github/workflows/public-api.yml
//!
//! ## Companion book
//!
//! Every component above has its own chapter in the [companion
//! book](https://lattice.nicolaspilegidenigris.dev). The book
//! is the long-form design rationale; this crate documentation
//! is the API reference.
//!
//! ## Cargo features
//!
//! - `tokio` — pulls in [`AsyncLattice`], a thin wrapper that
//!   runs the synchronous engine on tokio's blocking pool. Use
//!   it from inside an async function when you do not want the
//!   blocking I/O to stall the executor.
//! - `metrics` — records counters and histograms via the
//!   [`metrics`](https://docs.rs/metrics) crate facade. The
//!   facade is a no-op until the host process installs a
//!   recorder; the runtime cost is one atomic per recorded
//!   sample when an exporter is wired up.
//!
//! Both features are off by default.

#![forbid(unsafe_code)]

#[cfg(feature = "tokio")]
mod async_api;
mod bloom;
mod compaction;
// `compactor` is internal to the engine; the loom test crate
// needs cross-crate access to drive its model checks. Same trick
// as `conflict_tracker`: flip the visibility to `pub` only under
// `--cfg loom` so default builds keep the symbol crate-private
// and `cargo public-api` does not register it.
#[cfg(loom)]
pub mod compactor;
#[cfg(not(loom))]
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

/// Operational snapshot of an open [`Lattice`] handle, returned
/// by [`Lattice::stats`]. All fields are owned and sized for a
/// metrics scrape; reading the snapshot does not affect the
/// engine.
///
/// The values are point-in-time and may change between calls.
/// Callers wiring this into a metrics exporter should treat the
/// numbers as the most recent observation, not a live counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    /// Approximate byte footprint of the active memtable. Sized
    /// against [`LatticeBuilder::flush_threshold_bytes`]; a flush
    /// fires once this crosses the threshold.
    pub memtable_bytes: usize,
    /// Approximate byte footprint of the frozen memtable
    /// (mid-flush). Zero when no flush is in progress.
    pub frozen_memtable_bytes: usize,
    /// Per-level `SSTable` counts. Index is the level number;
    /// element is the number of tables in that level. Empty
    /// trailing levels are omitted.
    pub level_sstables: Vec<usize>,
    /// Next `SSTable` sequence number the engine will assign on
    /// its next flush or compaction. Monotonic across the
    /// lifetime of the database directory.
    pub next_seq: u64,
    /// Non-durable WAL records buffered since the last
    /// `fsync`. Bounded by `LatticeBuilder::commit_batch`.
    pub pending_writes: usize,
}

impl Stats {
    /// Total `SSTable` count across every level. Sums
    /// [`Self::level_sstables`].
    #[must_use]
    pub fn total_sstables(&self) -> usize {
        self.level_sstables.iter().sum()
    }

    /// Number of non-empty levels.
    #[must_use]
    pub fn level_count(&self) -> usize {
        self.level_sstables.iter().filter(|&&n| n > 0).count()
    }
}

/// Effective runtime configuration of an open [`Lattice`] handle,
/// returned by [`Lattice::config`]. The fields mirror the
/// [`LatticeBuilder`] knobs after defaults have been applied.
///
/// Useful for an operator who wants to verify the engine is
/// running with the configuration they think it is, and for tests
/// that want to assert a builder value actually stuck. Cheap to
/// construct: every field is read directly off `Inner` without a
/// lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// In-memory size at which the memtable auto-flushes. See
    /// [`LatticeBuilder::flush_threshold_bytes`].
    pub flush_threshold_bytes: usize,
    /// Number of live `SSTable`s in a single level that triggers
    /// auto-compaction. See [`LatticeBuilder::compaction_threshold`].
    pub compaction_threshold: usize,
    /// Level depth at which an auto-flush blocks the writer until
    /// the latest scheduled compaction completes. See
    /// [`LatticeBuilder::compaction_high_water_mark`].
    pub compaction_high_water_mark: usize,
    /// Maximum time a non-durable write may sit in the WAL buffer
    /// before the background flusher syncs it. See
    /// [`LatticeBuilder::commit_window`].
    pub commit_window: Duration,
    /// Number of buffered non-durable writes that triggers an
    /// inline `fsync`. See [`LatticeBuilder::commit_batch`].
    pub commit_batch: usize,
}

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
    compaction_high_water_mark: usize,
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

    /// Set the level depth at which an auto-flush blocks the
    /// writer until the latest scheduled compaction completes.
    /// Default is `4 * compaction_threshold`. Below the
    /// high-water mark, auto-compaction is fire-and-forget on
    /// the background compactor thread; once any level reaches
    /// the mark, the next flush waits on the in-flight
    /// compaction generation before returning.
    ///
    /// Lower values smooth tail latency at the cost of
    /// throughput (writers stall sooner); higher values let
    /// short bursts run flat-out while bounding worst-case
    /// level depth. Use [`usize::MAX`] to disable backpressure
    /// entirely (writers never wait, level depth is unbounded).
    #[must_use]
    pub const fn compaction_high_water_mark(mut self, mark: usize) -> Self {
        self.compaction_high_water_mark = mark;
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

/// Default level depth at which an auto-flush blocks the writer
/// until the latest scheduled compaction completes. v1.19's
/// async auto-compaction would otherwise let level depth grow
/// unbounded under a runaway producer; the high-water mark is
/// the throughput-vs-tail-latency knob. Default is `4 *
/// DEFAULT_COMPACTION_THRESHOLD = 16` so a healthy compactor
/// has room to keep up with normal bursts before any writer
/// stalls. Tunable via
/// [`LatticeBuilder::compaction_high_water_mark`].
const DEFAULT_COMPACTION_HIGH_WATER_MARK: usize = 4 * DEFAULT_COMPACTION_THRESHOLD;

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
    /// Level depth at which an auto-flush blocks the writer until
    /// the latest scheduled compaction completes (v1.19
    /// backpressure). Default is `4 * compaction_threshold`.
    compaction_high_water_mark: usize,
    commit_batch: usize,
    /// Mirror of the builder's `commit_window` value so
    /// [`Lattice::config`] can return it. The flusher thread
    /// captures the same value at spawn time; the field stored
    /// here is read-only and exists for introspection only.
    commit_window: Duration,
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
            compaction_high_water_mark: DEFAULT_COMPACTION_HIGH_WATER_MARK,
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
            compaction_high_water_mark,
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
            compaction_high_water_mark,
            commit_batch,
            commit_window,
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
    ///
    /// Accepts any `AsRef<[u8]>` for both key and value, so byte
    /// slices, `Vec<u8>`, byte arrays, and `&str` (via the
    /// `str::as_bytes` deref) all work without explicit
    /// conversion. The generic boundary is a thin wrapper that
    /// forwards to a monomorphic inner; binary size impact is
    /// negligible.
    pub fn put_with<K, V>(&self, key: K, value: V, opts: WriteOptions) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_inner(key.as_ref(), value.as_ref(), opts)
    }

    #[instrument(
        level = "debug",
        skip(self, value),
        fields(key_len = key.len(), value_len = value.len(), durable = opts.durable),
    )]
    fn put_with_inner(&self, key: &[u8], value: &[u8], opts: WriteOptions) -> Result<()> {
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
    /// `put_with(key, value, WriteOptions::default())`. Accepts
    /// any `AsRef<[u8]>` for both key and value.
    pub fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with(key, value, WriteOptions::default())
    }

    /// Delete `key`. A subsequent `get` returns `None`. Always
    /// durable on return; non-durable deletes are not yet exposed.
    /// Accepts any `AsRef<[u8]>` for the key.
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.delete_inner(key.as_ref())
    }

    #[instrument(level = "debug", skip(self), fields(key_len = key.len()))]
    fn delete_inner(&self, key: &[u8]) -> Result<()> {
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

    /// Read the current value for `key`, or `None` if absent or
    /// deleted. Accepts any `AsRef<[u8]>` for the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.get_dispatch(key.as_ref())
    }

    #[instrument(level = "trace", skip(self), fields(key_len = key.len()))]
    fn get_dispatch(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
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

    /// Range-bounded streaming scan. Yields visible
    /// `(key, value)` pairs whose key falls within
    /// `[start, end)`, in strictly increasing key order.
    ///
    /// `start = None` means "from the beginning of the keyspace";
    /// `end = None` means "to the end". The bounds are
    /// inclusive-exclusive to match the common Rust range
    /// idiom (`a..b`). Tombstones are filtered like
    /// [`Self::scan_iter`].
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// for c in b'a'..=b'g' {
    ///     db.put([c], b"v")?;
    /// }
    /// // [c, f) yields c, d, e
    /// let keys: Vec<_> = db
    ///     .scan_range(Some(b"c"), Some(b"f"))
    ///     .filter_map(Result::ok)
    ///     .map(|(k, _)| k)
    ///     .collect();
    /// assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[instrument(
        level = "debug",
        skip(self),
        fields(
            start_len = start.map_or(0, <[u8]>::len),
            end_len = end.map_or(0, <[u8]>::len),
        ),
    )]
    pub fn scan_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> ScanIter {
        let active_guard = self.inner.active.read();
        let state = self.inner.state.read().clone();
        let sstables: Vec<Arc<SSTableReader>> =
            state.all_sstables_newest_first().cloned().collect();
        ScanIter::with_bounds(
            &active_guard,
            state.frozen.as_deref(),
            sstables,
            None,
            start,
            end,
        )
    }

    /// Flush the current memtable to a new on-disk `SSTable`, then
    /// truncate the WAL. No-op if the memtable is empty.
    ///
    /// If the new `SSTable` lifts a level above
    /// [`LatticeBuilder::compaction_threshold`], an
    /// auto-compaction is scheduled on the dedicated background
    /// thread (since v1.19; pre-v1.19 the round ran inline on
    /// the writer's thread). Below
    /// [`LatticeBuilder::compaction_high_water_mark`] the
    /// scheduled round is fire-and-forget and `flush` returns
    /// immediately; once any level reaches the high-water mark
    /// the writer blocks on the in-flight compaction generation
    /// before returning, so level depth stays bounded.
    ///
    /// Tests that need a deterministic post-flush LSM layout
    /// should follow the flush with [`Self::compact`], which
    /// blocks until every level holds at most one `SSTable`.
    /// Errors from a background compaction are *not* propagated
    /// out of `flush`; they surface on the next [`Self::compact`]
    /// or [`CompactionHandle::wait`] call (and are emitted as
    /// `tracing::warn` events in real time).
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

    /// Compact every `SSTable` in `level_idx` together with the
    /// overlapping subset of `level_idx + 1` into a single table
    /// installed back into `level_idx + 1`. Strict-leveled selection
    /// (`RocksDB` style) introduced in v1.17: the merge inputs are
    /// the source level *plus* exactly the target-level tables
    /// whose key range intersects the source's combined range.
    /// Non-overlapping target tables are kept in place, retaining
    /// their sequence numbers and on-disk files, so a level can be
    /// rewritten incrementally without re-paying the cost of every
    /// previously compacted byte.
    #[allow(clippy::too_many_lines)]
    fn compact_level(&self, level_idx: usize) -> Result<()> {
        let started = Instant::now();
        let _mutation_guard = self.inner.mutation_lock.lock();

        // Snapshot the source level, the target level, the next
        // free sequence number, and whether tombstones can be
        // dropped at this boundary. Sources stay in insertion order
        // (oldest first) because `compact_all` is a
        // last-writer-wins merge that needs newer inputs later.
        let (sources, target_existing, new_seq, drop_tombstones) = {
            let state = self.inner.state.read();
            let Some(level) = state.levels.get(level_idx) else {
                return Ok(());
            };
            if level.len() < 2 {
                return Ok(());
            }
            let target_level = level_idx + 1;
            let target_existing: Vec<Arc<SSTableReader>> =
                state.levels.get(target_level).cloned().unwrap_or_default();
            // Tombstones in the source can shadow data physically
            // resident in any level deeper than the target. The
            // non-overlap subset of the target level is by
            // construction range-disjoint from the source, so it
            // cannot hold an older version of any key in the merge
            // and is irrelevant to this decision. Drop tombstones
            // only when no level deeper than the target holds any
            // tables.
            let drop_tombstones = state
                .levels
                .iter()
                .skip(target_level + 1)
                .all(Vec::is_empty);
            (
                level.clone(),
                target_existing,
                state.next_seq,
                drop_tombstones,
            )
        };

        // Combined range of the source level. Sources is non-empty
        // because we returned early on `level.len() < 2`.
        let source_min: Vec<u8> = sources
            .iter()
            .map(|r| r.min_key().to_vec())
            .min()
            .expect("sources non-empty");
        let source_max: Vec<u8> = sources
            .iter()
            .map(|r| r.max_key().to_vec())
            .max()
            .expect("sources non-empty");

        // Partition the existing target level into the overlap
        // subset (intersects [source_min, source_max]) and the
        // keep-in-place subset (range-disjoint from source).
        let (target_overlap, target_keep): (Vec<_>, Vec<_>) =
            target_existing.into_iter().partition(|r| {
                !(r.max_key() < source_min.as_slice() || r.min_key() > source_max.as_slice())
            });

        // Merge order is oldest-first for last-writer-wins. The
        // target level (deeper, level_idx+1) is older than the
        // source level (level_idx); within either level, sequence
        // order is insertion order.
        let mut readers_owned: Vec<Arc<SSTableReader>> =
            Vec::with_capacity(target_overlap.len() + sources.len());
        readers_owned.extend(target_overlap.iter().cloned());
        readers_owned.extend(sources.iter().cloned());
        let readers: Vec<&SSTableReader> = readers_owned.iter().map(Arc::as_ref).collect();

        // I/O outside any state lock.
        let final_path = sstable_path(&self.inner.path, new_seq);
        let tmp_path = self
            .inner
            .path
            .join(format!("{new_seq:0SSTABLE_DIGITS$}.sst.tmp"));
        let _ = fs::remove_file(&tmp_path);
        let outcome = compaction::compact_all(&readers, &tmp_path, drop_tombstones)?;
        drop(readers);
        drop(readers_owned);
        let new_reader: Option<Arc<SSTableReader>> = match outcome {
            compaction::CompactOutcome::Wrote => {
                fs::rename(&tmp_path, &final_path)?;
                sync_dir(&self.inner.path)?;
                Some(Arc::new(SSTableReader::open(&final_path, new_seq)?))
            }
            compaction::CompactOutcome::Empty => None,
        };

        // Sequence numbers we will physically delete: every source
        // table, plus only the overlap subset of the target. The
        // keep-in-place subset stays on disk and in memory.
        let old_seqs: Vec<u64> = sources
            .iter()
            .map(|r| r.seq())
            .chain(target_overlap.iter().map(|r| r.seq()))
            .collect();

        // Install: empty the source level; in the target level
        // keep the non-overlap subset, append the new merged
        // output (if any), and resort by min_key for a
        // deterministic layout. When the merge cancelled out we
        // skip allocating a new sstable but still advance
        // `next_seq` so subsequent flushes do not collide with the
        // unused number.
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
            let mut next_target = target_keep.clone();
            if let Some(reader) = new_reader {
                next_target.push(reader);
            }
            next_target.sort_by(|a, b| a.min_key().cmp(b.min_key()));
            new_levels[level_idx + 1] = next_target;
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
        drop(target_overlap);
        drop(target_keep);

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

    /// Effective runtime configuration of this handle, returned
    /// as an owned [`Config`] value. Cheap to call (one struct
    /// build, no locks). Useful for an operator who wants to
    /// verify the engine is running with the configuration they
    /// think it is, and for tests that need to assert a builder
    /// value actually stuck.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::builder(dir.path())
    ///     .compaction_threshold(2)
    ///     .open()?;
    /// assert_eq!(db.config().compaction_threshold, 2);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn config(&self) -> Config {
        Config {
            flush_threshold_bytes: self.inner.flush_threshold_bytes,
            compaction_threshold: self.inner.compaction_threshold,
            compaction_high_water_mark: self.inner.compaction_high_water_mark,
            commit_window: self.inner.commit_window,
            commit_batch: self.inner.commit_batch,
        }
    }

    /// Snapshot of operational counters, sized for dashboards
    /// and ops introspection. Cheap to call (one read lock on
    /// the LSM state, plus a handful of atomic loads); a single
    /// caller can poll it on a sub-second tick without
    /// measurably loading the engine.
    ///
    /// The returned [`Stats`] is a value snapshot, not a live
    /// view; subsequent operations through any handle do not
    /// change a previously returned `Stats`.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// db.put(b"k", b"v")?;
    /// let stats = db.stats();
    /// assert!(stats.memtable_bytes > 0);
    /// assert_eq!(stats.frozen_memtable_bytes, 0);
    /// assert_eq!(stats.total_sstables(), 0);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn stats(&self) -> Stats {
        let memtable_bytes = self.inner.active.read().approx_size();
        let state = self.inner.state.read().clone();
        let frozen_memtable_bytes = state.frozen.as_ref().map_or(0, |m| m.approx_size());
        let level_sstables: Vec<usize> = state.levels.iter().map(Vec::len).collect();
        let next_seq = state.next_seq;
        let pending_writes = self.inner.pending_writes.load(Ordering::Acquire);
        Stats {
            memtable_bytes,
            frozen_memtable_bytes,
            level_sstables,
            next_seq,
            pending_writes,
        }
    }

    /// Total bytes the engine currently occupies on disk: the
    /// sum of every live `SSTable` file size plus the current
    /// length of the WAL. Memtable bytes are explicitly *not*
    /// counted; for that, see [`Self::stats`] and the
    /// `memtable_bytes` field.
    ///
    /// The number is a point-in-time observation. A concurrent
    /// flush, compaction, or write may move the counter between
    /// the snapshot of the LSM state and the `metadata` calls;
    /// the returned value reflects the files that were live at
    /// the moment the LSM state was sampled, rounded to whatever
    /// the OS reports as their size at the metadata call.
    ///
    /// Useful for capacity-planning dashboards and operator
    /// alerts. Not a hot path: the call performs one
    /// `fs::metadata` syscall per live `SSTable` plus one for
    /// the WAL.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// db.put(b"k", b"v")?;
    /// db.flush()?;
    /// assert!(db.byte_size_on_disk()? > 0);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn byte_size_on_disk(&self) -> Result<u64> {
        let state = self.inner.state.read().clone();
        let mut total: u64 = 0;
        for level in &state.levels {
            for reader in level {
                let path = sstable_path(&self.inner.path, reader.seq());
                let len = fs::metadata(&path).map_or(0, |m| m.len());
                total = total.saturating_add(len);
            }
        }
        let wal_path = self.inner.path.join("wal.log");
        let wal_len = fs::metadata(&wal_path).map_or(0, |m| m.len());
        total = total.saturating_add(wal_len);
        Ok(total)
    }

    /// Deterministic 64-bit fingerprint of the visible
    /// `(key, value)` set in ascending key order. Two databases
    /// with the same logical state produce the same value;
    /// divergent state produces a different value. The hash is
    /// invariant under [`Self::flush`] and [`Self::compact`]
    /// because neither changes the visible set.
    ///
    /// The fingerprint is built from xxh3-64 over a stream of
    /// length-prefixed key/value pairs:
    /// `len(key) || key || len(value) || value`, lengths as
    /// little-endian `u64`. Length-prefixing is load-bearing:
    /// without it `("ab", "cd")` and `("a", "bcd")` would hash
    /// the same.
    ///
    /// Cost is O(visible keys + visible bytes) plus the I/O to
    /// stream the on-disk merge: not a hot path. Designed for
    /// cross-host divergence detection (replicas on the same
    /// logical state must agree on this fingerprint), test
    /// fences that want to assert state equivalence between two
    /// operation histories, and sanity checks after a recovery.
    ///
    /// # Example
    ///
    /// ```
    /// use lattice_core::Lattice;
    /// let dir = tempfile::tempdir()?;
    /// let db = Lattice::open(dir.path())?;
    /// let empty_hash = db.checksum()?;
    /// db.put(b"k", b"v")?;
    /// assert_ne!(empty_hash, db.checksum()?);
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
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
        // Auto-compaction is asynchronous since v1.19. When a
        // flush leaves a level above its threshold, this method
        // schedules a round on the dedicated compactor thread
        // and returns immediately; the writer's tail latency no
        // longer pays the merge cost.
        //
        // Through v1.18 the trigger was inline on the writer
        // thread, which kept post-flush state deterministic but
        // bounded throughput by the compaction wall-clock. The
        // worker captures the latest scheduled generation when
        // it wakes and runs `run_pending_compactions` to drain
        // every level above the threshold, so a burst of
        // back-to-back flushes coalesces into one cascade.
        //
        // Errors from the round are sticky on `CompactorShared`
        // and surface on the next [`Self::compact`] /
        // [`Self::compact_async`] wait. They are not propagated
        // out of `flush` because doing so would re-couple writer
        // latency to compaction errors, defeating the migration.
        // The compactor thread also emits a
        // `tracing::warn` event on failure, so a tracing
        // subscriber sees them in real time.
        //
        // Backpressure: when the shallowest level's depth
        // crosses [`LatticeBuilder::compaction_high_water_mark`]
        // (default `4 * compaction_threshold`), the writer
        // blocks on the latest scheduled compaction generation
        // before continuing. This keeps a runaway producer from
        // letting level depth grow unbounded while the compactor
        // catches up.
        let (needs_round, hit_high_water) = {
            let state = self.inner.state.read();
            let position = state
                .levels
                .iter()
                .position(|level| level.len() >= self.inner.compaction_threshold)
                .filter(|&idx| idx + 1 < MAX_LEVELS);
            let max_depth = state.levels.iter().map(Vec::len).max().unwrap_or(0);
            let result = (
                position.is_some(),
                max_depth >= self.inner.compaction_high_water_mark,
            );
            drop(state);
            result
        };
        if needs_round {
            let handle = self.compact_async();
            if hit_high_water {
                // Backpressure: the producer is outrunning the
                // compactor. Block this flush on the round we
                // just scheduled. Subsequent rounds are still
                // fire-and-forget; only the over-water flush
                // pays the wait.
                handle.wait()?;
            }
            // Otherwise drop the handle: the worker still runs
            // the round, and any error surfaces on a later
            // explicit compact_async().wait().
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

    /// Strict leveled invariant: a single compaction round from
    /// level N to N+1 rewrites only the subset of N+1 whose key
    /// range overlaps the merged source. Non-overlapping target
    /// tables retain their original sequence numbers and on-disk
    /// files.
    ///
    /// The pre-v1.17 algorithm appended the merged output to N+1
    /// without consulting existing tables, so a level could end up
    /// with overlapping sstables and large datasets paid the full
    /// rewrite cost on every compaction. The strict-leveled
    /// algorithm picks the overlap subset of N+1, merges it into
    /// the inputs, and replaces only that subset; non-overlapping
    /// tables in N+1 are untouched.
    #[test]
    fn strict_leveled_keeps_non_overlapping_l1_sstables_intact() {
        let dir = tempdir().unwrap();
        // Disable auto-compaction so flush() does not cascade and
        // pollute the level shape we are constructing by hand.
        let db = Lattice::builder(dir.path())
            .compaction_threshold(usize::MAX)
            .open()
            .unwrap();

        // Helper: insert two keys (one per memtable, separated by
        // an explicit flush), then run one round of compact_level(0)
        // to push the resulting two L0 tables into a single L1
        // sstable. After three rounds, L1 holds three sstables
        // covering disjoint key ranges.
        let put_flush_compact = |db: &Lattice, k1: &[u8], k2: &[u8]| {
            db.put(k1, b"v").unwrap();
            db.flush().unwrap();
            db.put(k2, b"v").unwrap();
            db.flush().unwrap();
            db.compact_level(0).unwrap();
        };

        put_flush_compact(&db, b"a01", b"a02");
        put_flush_compact(&db, b"m01", b"m02");
        put_flush_compact(&db, b"x01", b"x02");

        let l1_seqs_before: Vec<u64> = {
            let state = db.inner.state.read();
            state.levels[1].iter().map(|r| r.seq()).collect()
        };
        assert_eq!(
            l1_seqs_before.len(),
            3,
            "setup: L1 must hold 3 disjoint sstables before the strict-leveled round (got {l1_seqs_before:?})",
        );

        // Round 4: produce two new L0 tables whose combined range
        // overlaps only the first L1 sstable (the [a01..=a02] one).
        db.put(b"a015", b"v").unwrap();
        db.flush().unwrap();
        db.put(b"a016", b"v").unwrap();
        db.flush().unwrap();
        db.compact_level(0).unwrap();

        let l1_seqs_after: Vec<u64> = {
            let state = db.inner.state.read();
            state.levels[1].iter().map(|r| r.seq()).collect()
        };

        // Strict leveled: L1 still has exactly three sstables. The
        // [m01..=m02] and [x01..=x02] tables retain their original
        // seqs because nothing in the compacted input overlapped
        // their range.
        assert_eq!(
            l1_seqs_after.len(),
            3,
            "strict leveled: L1 must keep three sstables (only the overlapping subset is rewritten); got {l1_seqs_after:?}",
        );
        let kept = &l1_seqs_before[1..3];
        for seq in kept {
            assert!(
                l1_seqs_after.contains(seq),
                "strict leveled: non-overlapping L1 sstable seq {seq} must survive the round (after = {l1_seqs_after:?})",
            );
        }

        // All originally written data is still readable.
        for key in [
            b"a01".as_slice(),
            b"a02",
            b"a015",
            b"a016",
            b"m01",
            b"m02",
            b"x01",
            b"x02",
        ] {
            assert_eq!(
                db.get(key).unwrap().as_deref(),
                Some(b"v".as_slice()),
                "post-compaction read for {key:?} must still see the value",
            );
        }
    }

    /// `CompactionHandle::wait_timeout` returns `Ok(true)` once
    /// the round completes (or the compactor is shutting down)
    /// and `Ok(false)` if the deadline elapses first. v1.20
    /// introduces this so async callers, ops dashboards, and
    /// tests can bound how long they will block on a scheduled
    /// round.
    ///
    /// The completion case rides a real `compact_async` round on
    /// an empty database: the worker has nothing to do and
    /// publishes the captured generation immediately, so a
    /// 10-second deadline is far more than enough.
    #[test]
    fn compaction_handle_wait_timeout_returns_true_when_round_completes() {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();

        let handle = db.compact_async();
        let completed = handle.wait_timeout(Duration::from_secs(10)).unwrap();
        assert!(
            completed,
            "compact_async on a small database must complete within 10s",
        );
    }

    /// The timeout case constructs a handle whose target
    /// generation cannot be reached (`u64::MAX`) and waits a
    /// short, deterministic 50 ms. The wait must observe the
    /// deadline and return `Ok(false)` rather than block
    /// forever. Direct construction of `CompactionHandle` is
    /// `pub(crate)`, so this test lives inside the crate.
    #[test]
    fn compaction_handle_wait_timeout_returns_false_when_deadline_elapses() {
        let dir = tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();
        let shared = Arc::clone(&db.inner.compactor);
        let handle = crate::compactor::CompactionHandle {
            shared,
            target_generation: u64::MAX,
        };
        let started = std::time::Instant::now();
        let completed = handle.wait_timeout(Duration::from_millis(50)).unwrap();
        let elapsed = started.elapsed();
        assert!(
            !completed,
            "an unreachable target generation must time out, not complete",
        );
        // Sanity check that the wait actually paused, not
        // returned instantly with a stale flag.
        assert!(
            elapsed >= Duration::from_millis(40),
            "wait_timeout returned in {elapsed:?}; expected at least 40 ms before the deadline",
        );
    }
}
