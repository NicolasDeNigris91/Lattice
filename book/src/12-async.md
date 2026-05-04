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

v1.6 adds `AsyncLattice::transaction(|tx| { ... })` with the
closure body remaining synchronous. The whole transaction (read,
stage, commit) runs on tokio's blocking pool via
`spawn_blocking`; the calling tokio task stays free, but the
closure cannot `await` between a read and a write.

For workflows that need to await mid-transaction, the recommended
pattern is:

```rust
let snapshot_value = db.transaction(|tx| {
    Ok::<_, lattice_core::Error>(tx.get(b"counter")?.unwrap_or_default())
}).await?;

let new_value = remote_increment(snapshot_value).await?;

db.transaction(|tx| {
    let current = tx.get(b"counter")?.unwrap_or_default();
    if current != snapshot_value {
        return Err(Error::TransactionConflict);
    }
    tx.put(b"counter", &new_value);
    Ok::<_, lattice_core::Error>(())
}).await?;
```

The conflict detection from chapter 11 guarantees the second
transaction aborts with `Error::TransactionConflict` if another
writer touched `counter` while the await was in flight, so the
two-step pattern preserves snapshot semantics across the await
boundary without the closure itself needing to be a future.

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

## Auto-compaction is async (v1.19)

The `AsyncLattice` wrapper is one half of the async story. The
other is auto-compaction itself. Through v1.18 a flush that
crossed [`LatticeBuilder::compaction_threshold`] ran the
merge inline on the writer thread; from v1.19 the same trigger
schedules a round on the dedicated compactor thread and
returns immediately. The writer's tail latency no longer pays
the compaction wall-clock under bursts; the I/O happens in
parallel with the next WAL appends, and the synchronous
`Lattice::compact()` keeps its blocking semantics for callers
that need a deterministic post-call layout. See chapter 5 for
the algorithm and the
[`LatticeBuilder::compaction_high_water_mark`] backpressure
knob, which keeps a runaway producer from letting level depth
grow unbounded while the compactor catches up.

The chapter-5 migration applies regardless of which API a
caller uses: `Lattice::flush` and `AsyncLattice::flush` both
ride the same fire-and-forget trigger. The async wrapper
gains the win for free.
