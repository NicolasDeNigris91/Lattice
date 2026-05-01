# Concurrency

Through v1.1 every method on `Lattice` took `&mut self`. That made
the borrow checker do the synchronisation for us: only one thread
could touch the database at a time, and a single handle could not
be shared across threads. It was the right starting point because
it made every other contract simpler to reason about, but it left
serious throughput on the table.

v1.2 turns `Lattice` into `Send + Sync + Clone` so multiple threads
can hold a handle and read in parallel.

## The shape

```rust
pub struct Lattice {
    inner: Arc<Inner>,
}
```

`Inner` is private. Cloning a `Lattice` is one atomic increment on
the `Arc`. Two clones see the same database; writes through one
are visible through the other immediately, with no reopen.

`Inner` holds the moving parts:

```rust
struct Inner {
    path: PathBuf,
    active: RwLock<MemTable>,
    state: RwLock<Arc<State>>,
    wal: Mutex<Wal>,
    pending_writes: AtomicUsize,
    mutation_lock: Mutex<()>,
    // configuration and the background flusher join handle.
}

struct State {
    frozen: Option<Arc<MemTable>>,
    sstables: Vec<Arc<SSTableReader>>,
    next_seq: u64,
}
```

The fjall 3.0 release notes describe almost the same shape, and
for the same reason: a single coarse `RwLock<Arc<State>>` plus a
small number of carefully scoped locks beats a graph of granular
locks for both correctness and throughput.

## Reads in parallel

`get` does this:

1. Take `active.read()`, look up. Drop the lock.
2. Take `state.read()`, clone the `Arc<State>`. Drop the lock.
3. If the snapshot's `frozen` is `Some`, look up there.
4. Walk the snapshot's `sstables` from newest to oldest.

Both lock acquisitions are reads, so any number of threads can do
this at the same time. The `Arc` clone in step 2 is one atomic
increment; nobody waits.

`scan` is the same pattern, just iterating instead of looking up
a single key.

## Writes serialise on the WAL

`put` and `delete` go through the same internal `append_entry`:

1. Take `wal.lock()`. Append the record into the `BufWriter`. If
   the write is durable, `fsync` immediately. Drop the WAL lock.
2. Take `active.write()`, mutate the memtable, drop the lock.
3. Check the memtable size; if over the configured threshold,
   call `flush()`.

Two concurrent writers serialise on `wal.lock()` but never block
on `active`, because the write lock on the memtable is held only
for the `BTreeMap::insert` call.

## Read-your-writes during flush

`flush` is the interesting case. It needs to drain the active
memtable into an SSTable on disk, but it must not block reads of
the data being drained. The solution is the `frozen` slot:

1. Under both `active.write()` and `state.write()`, atomically:
   - Replace `active` with an empty `MemTable`. Move the old one
     into an `Arc<MemTable>`.
   - Install a fresh `Arc<State>` whose `frozen` field holds
     that `Arc<MemTable>`.
2. Release both write locks. Reads now see `active = empty` and
   `frozen = Some(memtable)`. The data is still reachable.
3. Build the SSTable from the frozen memtable. This is the slow
   part and it runs without holding any lock that reads need.
4. Take `state.write()` again, install a fresh `Arc<State>` with
   `frozen = None` and the new SSTable appended. Release.
5. Persist the manifest, truncate the WAL, reset the pending
   counter.

A reader that looks up a key during step 3 finds it in the
frozen memtable. A reader that runs after step 4 finds it in the
SSTable. There is no window in which the data is missing.

The `mutation_lock` `Mutex<()>` serialises flushes against each
other and against compactions, so two background tasks cannot race
on `next_seq` or on the manifest write.

## Compaction

`compact` snapshots the sstables list under a brief read lock,
runs the merge I/O outside any state lock, then takes
`state.write()` once at the end to install the result. Concurrent
reads keep firing throughout the merge.

In v1.2 the caller still pays the wall-clock cost of compaction
(`compact()` is blocking). v1.3 brought leveled compaction (a
better algorithm, but kept on the caller's thread); moving the
work itself onto a background thread is a v2.x candidate
because the lock-discipline change is large and the call site
is rare enough in practice that the wall-clock cost has not
been the binding constraint.

## The background flusher

The group commit timer that v1.1 promised but did not yet wire is
now live. On `Lattice::open` the engine spawns a thread named
`lattice-flusher`. It holds a `Weak<Inner>` (so it does not keep
the database alive on its own) and loops:

1. `park_timeout(commit_window)`.
2. `Weak::upgrade`. If `None`, the engine has been dropped: exit.
3. If `pending_writes > 0` and `commit_window` has elapsed since
   the last sync, take `wal.lock()`, `sync_pending`, reset the
   counter.

The thread tracks its own `last_sync` and refuses to sync if not
enough time has passed, so spurious wakeups (which `park_timeout`
explicitly allows) do not silently turn non-durable writes durable
ahead of schedule. Drop on `Inner` signals stop, unparks the
thread, and joins it before performing one final sync of any
straggler bytes.

## Numbers

`cargo bench -p lattice-core --bench put_get -- concurrent` on the
development machine, with ten thousand pre-populated keys and
2500 random hits per thread:

| threads | wall-clock | aggregate reads/sec |
|---:|---:|---:|
| 1 | 44 ms | 56k |
| 2 | 69 ms | 72k |
| 4 | 75 ms | 134k |
| 8 | 140 ms | 143k |

Scaling 1 -> 8 threads is roughly 2.5x. The ceiling is set by the
active-memtable `RwLock` and the OS scheduler. Closing the gap to
linear scaling needs a sharded or lock-free memtable, which is on
the M3 roadmap.

## What did not change

- The on-disk format. A v1.0 directory opens cleanly under v1.2
  and vice versa.
- The CLI. It uses one handle on one thread, exactly as before.
- The default durability story. `db.put(k, v)` still `fsync`s
  before returning, just like every release since v0.1.

## Tests

`tests/concurrency.rs` pins the new contract:

- `lattice_is_send_and_sync` is a compile-time check.
- `cloned_handle_observes_writes_from_origin` covers shared state.
- `clone_keeps_database_alive_after_origin_drops` covers the Drop
  contract.
- `many_readers_and_one_writer_see_consistent_state` stresses
  eight reader threads against a writer streaming two thousand
  durable puts. Every observed value must be one the writer
  actually wrote; every key must be present after the writer
  joins.

`tests/group_commit.rs` adds
`background_flusher_syncs_within_commit_window`, which puts a
non-durable record, sleeps past the window, then `mem::forget`s
the handle to skip Drop. The reopen must still see the value,
which it does only because the timer fired.
