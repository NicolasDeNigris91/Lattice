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

use crate::Lattice;
use crate::error::{Error, Result};

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

    /// Borrow the underlying synchronous handle. Useful for code
    /// paths that hold the engine in an async context but need to
    /// hand a sync clone to a function that does not yet take an
    /// async handle.
    pub const fn sync(&self) -> &Lattice {
        &self.inner
    }
}
