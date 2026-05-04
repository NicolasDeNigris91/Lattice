//! Tokio wrapper around the synchronous engine.
//!
//! INVARIANT: every method on [`AsyncLattice`] runs the underlying
//! synchronous operation on tokio's blocking pool via
//! [`tokio::task::spawn_blocking`]. The engine itself is unchanged;
//! locks are still `parking_lot`, file I/O is still `std::fs`.
//! "Async-friendly" rather than "natively async". Replacing the
//! locks and the I/O is a v2.x rewrite, not part of this milestone.

use std::io;
use std::path::{Path, PathBuf};

use crate::compactor::CompactionHandle;
use crate::error::{Error, Result};
use crate::transaction::Transaction;
use crate::{Config, Lattice, Stats};

/// Async-friendly wrapper around [`Lattice`].
///
/// Cloning is cheap (the underlying `Arc<Inner>` is bumped). Methods
/// move owned keys and values into a closure that runs on tokio's
/// blocking pool, which keeps the calling executor task free while
/// the WAL `fsync` (the slow part) is in flight.
#[derive(Debug, Clone)]
pub struct AsyncLattice {
    inner: Lattice,
}

fn join_to_err(err: tokio::task::JoinError) -> Error {
    Error::Io(io::Error::other(err))
}

impl AsyncLattice {
    /// Open or create the database at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        // The borrow on `path` ends here; the owned `PathBuf` is what
        // moves into the blocking closure.
        let path: PathBuf = path.as_ref().to_path_buf();
        let inner = tokio::task::spawn_blocking(move || Lattice::open(path))
            .await
            .map_err(join_to_err)??;
        Ok(Self { inner })
    }

    /// Open the database directory at `path` in read-only mode.
    /// Equivalent to wrapping
    /// `Lattice::open_read_only(path)`. See
    /// [`crate::LatticeBuilder::read_only`] for the contract.
    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let inner = tokio::task::spawn_blocking(move || Lattice::open_read_only(path))
            .await
            .map_err(join_to_err)??;
        Ok(Self { inner })
    }

    /// Insert or overwrite a value for `key`.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || inner.put(&key, &value))
            .await
            .map_err(join_to_err)?
    }

    /// Delete `key`.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || inner.delete(&key))
            .await
            .map_err(join_to_err)?
    }

    /// Read the current value for `key`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || inner.get(&key))
            .await
            .map_err(join_to_err)?
    }

    /// Iterate live key-value pairs in key order. If `prefix` is
    /// `Some`, only keys starting with it are returned.
    pub async fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(<[u8]>::to_vec);
        tokio::task::spawn_blocking(move || inner.scan(prefix.as_deref()))
            .await
            .map_err(join_to_err)?
    }

    /// Force a memtable flush.
    pub async fn flush(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.flush())
            .await
            .map_err(join_to_err)?
    }

    /// Force a `fsync` of any pending non-durable WAL appends.
    pub async fn flush_wal(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.flush_wal())
            .await
            .map_err(join_to_err)?
    }

    /// Run a leveled compaction down to one table per non-empty
    /// level.
    pub async fn compact(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.compact())
            .await
            .map_err(join_to_err)?
    }

    /// Run a closure inside a snapshot-isolated transaction on
    /// tokio's blocking pool.
    ///
    /// The closure itself is synchronous: it cannot `await` inside
    /// the transaction body. This matches v1.5's "async-friendly"
    /// model where the engine remains synchronous and only the
    /// dispatch is asynchronous. For read-modify-write transactions
    /// that do not need to await external calls (the vast majority),
    /// this is the right shape; the closure runs on the blocking
    /// pool and the calling tokio task stays free for other work.
    ///
    /// For workflows that need to await between a read and a write,
    /// the pattern is to issue the reads, await whatever, and then
    /// open a fresh transaction. v1.6's conflict detection on the
    /// engine guarantees that the second transaction will abort
    /// with [`Error::TransactionConflict`] if the data it relied on
    /// changed in the meantime.
    pub async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.transaction(f))
            .await
            .map_err(join_to_err)?
    }

    /// Total bytes the engine currently occupies on disk:
    /// the sum of every live `SSTable` file size plus the
    /// current WAL length. Async wrapper for
    /// [`Lattice::byte_size_on_disk`]; runs the per-file
    /// `metadata` calls on tokio's blocking pool. Memtable
    /// bytes are not counted; for that, see [`Self::stats`].
    pub async fn byte_size_on_disk(&self) -> Result<u64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.byte_size_on_disk())
            .await
            .map_err(join_to_err)?
    }

    /// Deterministic xxh3-64 fingerprint of the visible
    /// `(key, value)` set in ascending key order. Async
    /// wrapper for [`Lattice::checksum`]; runs the merge
    /// scan on tokio's blocking pool. Two replicas on the
    /// same logical state produce the same value, regardless
    /// of how each got there.
    pub async fn checksum(&self) -> Result<u64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.checksum())
            .await
            .map_err(join_to_err)?
    }

    /// Copy this database into `dest` as a self-contained
    /// directory openable by [`Self::open`]. Async wrapper
    /// for [`Lattice::backup_to`]; the heavy file copies and
    /// WAL replay run on tokio's blocking pool so the
    /// calling task stays free.
    pub async fn backup_to(&self, dest: impl AsRef<Path>) -> Result<()> {
        let inner = self.inner.clone();
        let dest: PathBuf = dest.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || inner.backup_to(dest))
            .await
            .map_err(join_to_err)?
    }

    /// Operational counters snapshot. Cheap (one read lock
    /// plus a handful of atomic loads), so it runs inline on
    /// the calling tokio task rather than going through the
    /// blocking pool.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    /// Effective runtime configuration. Cheap (one struct
    /// build, no locks); runs inline on the calling task.
    #[must_use]
    pub fn config(&self) -> Config {
        self.inner.config()
    }

    /// Path to the database directory. Cheap field access;
    /// runs inline on the calling task.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Schedule a leveled compaction round on the background
    /// compactor and return the handle. Inline (no
    /// `spawn_blocking`): the call only bumps a generation
    /// counter and notifies the worker. Use
    /// [`Self::wait_compact`] to await the result without
    /// blocking the calling task.
    #[must_use]
    pub fn compact_async(&self) -> CompactionHandle {
        self.inner.compact_async()
    }

    /// Await a previously scheduled compaction round. The
    /// underlying [`CompactionHandle::wait`] is blocking, so
    /// the call goes through tokio's blocking pool to keep
    /// the calling task free.
    pub async fn wait_compact(&self, handle: CompactionHandle) -> Result<()> {
        tokio::task::spawn_blocking(move || handle.wait())
            .await
            .map_err(join_to_err)?
    }

    /// Bounded variant of [`Self::wait_compact`]. Returns
    /// `Ok(true)` if the round completed within `timeout`,
    /// `Ok(false)` if the deadline elapsed first, and
    /// `Err(Error::Compaction(...))` on a sticky failure.
    /// Mirrors [`crate::CompactionHandle::wait_timeout`] for
    /// async callers.
    pub async fn wait_compact_timeout(
        &self,
        handle: CompactionHandle,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        tokio::task::spawn_blocking(move || handle.wait_timeout(timeout))
            .await
            .map_err(join_to_err)?
    }

    /// Borrow the underlying synchronous handle. Useful for code
    /// paths that hold the engine in an async context but need to
    /// hand a sync clone to a function that does not yet take an
    /// async handle.
    pub const fn sync(&self) -> &Lattice {
        &self.inner
    }
}
