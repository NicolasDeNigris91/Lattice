# Async I/O

v1.5 ships an optional async wrapper for callers that live inside
a tokio runtime:

```toml
[dependencies]
lattice-core = { version = "1.5", features = ["tokio"] }
```

```rust
use lattice_core::AsyncLattice;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = AsyncLattice::open("./data").await?;
    db.put(b"k", b"v").await?;
    assert_eq!(db.get(b"k").await?, Some(b"v".to_vec()));
    Ok(())
}
```

Cloning the handle is one atomic increment on the underlying
`Arc<Inner>`, exactly like the synchronous `Lattice` from v1.2.

## What "async" means here

`AsyncLattice` is a wrapper, not a rewrite. Every method runs the
synchronous engine on tokio's blocking pool via
`tokio::task::spawn_blocking`. The locks are still
`parking_lot::RwLock` and `parking_lot::Mutex`. The file I/O is
still `std::fs`. The WAL `fsync` still blocks an OS thread. What
changes is that the async caller's task is not blocked: the
runtime scheduler picks another ready task while the blocking
pool runs the put.

This is "async-friendly" rather than "natively async". Honest
about the trade-off:

| | sync API | `AsyncLattice` (v1.5) | native async (v2.x) |
|---|---|---|---|
| caller task blocked during fsync? | yes | no | no |
| OS thread blocked during fsync? | yes | yes (blocking pool) | no |
| extra dep | none | `tokio` (`rt`) | `tokio` (`fs`, `sync`) |
| code change inside engine | none | none | major |

For an embed inside an HTTP server that serves dozens of requests
per second, the wrapper is the right answer: simple, no engine
churn, async caller tasks stay free. For a service that needs
tens of thousands of concurrent operations per second, native
async would let the runtime poll many in-flight syscalls without
parking threads, and that is the v2.x rewrite.

## Method coverage

Every blocking operation on `Lattice` has an `AsyncLattice`
counterpart with the same signature except `async` and `Result`:

```rust
impl AsyncLattice {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn delete(&self, key: &[u8]) -> Result<()>;
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub async fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    pub async fn flush(&self) -> Result<()>;
    pub async fn flush_wal(&self) -> Result<()>;
    pub async fn compact(&self) -> Result<()>;
    pub fn sync(&self) -> &Lattice;
}
```

`sync()` borrows the underlying synchronous handle. Useful when an
async caller has the `AsyncLattice` and needs to hand a `Lattice`
clone to a function that has not yet been migrated to take an
async handle.

The transaction surface from chapter 11 does not yet have an async
companion. The closure signature `FnOnce(&mut Transaction<'_>) ->
Result<R>` does not compose with futures cleanly (the snapshot
must outlive every awaited write inside the closure), and v1.5
ships the simpler wrapper first. An async transaction landing in
v1.6 will likely take a `for<'tx> AsyncFnOnce(&'tx mut
AsyncTransaction<'_>)` and accept the lifetime gymnastics.

## Tests

`tests/async_api.rs` is gated by `cfg(feature = "tokio")` and runs
only when the feature is on:

- `async_put_then_get_returns_value`
- `async_delete_makes_get_return_none`
- `async_scan_returns_prefix_filtered_pairs`
- `async_concurrent_puts_from_many_tasks_all_persist` spawns 32
  tokio tasks that share the handle and confirms every key is
  present after the joins.

Run with `cargo test -p lattice-core --features tokio`.
