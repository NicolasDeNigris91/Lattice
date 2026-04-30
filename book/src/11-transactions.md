# Transactions

v1.4 introduces a closure-shaped transaction API:

```rust
use lattice_core::{Error, Lattice};
let db = Lattice::open("./data")?;

db.transaction(|tx| {
    if tx.get(b"balance:alice")?.is_none() {
        tx.put(b"balance:alice", b"100");
    }
    tx.put(b"balance:bob", b"50");
    Ok::<_, Error>(())
})?;
```

Three guarantees, one open question.

## Snapshot reads

The closure is called with a `&mut Transaction<'_>` whose `get`
returns the value of a key as it existed when the transaction
started. Concurrent writes through other handles do not change
what the transaction sees.

The mechanism is the `Snapshot` machinery from v1.2: at
transaction start, the engine clones the active and frozen
memtables, pins the SSTable readers across every level, and hands
the bundle to the transaction. Reads walk that bundle just like
any other snapshot read.

## Read-your-writes inside the transaction

`tx.put(k, v)` and `tx.delete(k)` stage writes into a
`BTreeMap<Vec<u8>, Option<Vec<u8>>>` on the transaction. A later
`tx.get(k)` consults the staged map first and the snapshot second,
so the caller always sees its own most recent staged write,
including a stage-then-delete pattern that returns `None`.

Other handles do not see the staged writes: the staging map is
per-transaction and only crosses the boundary at commit.

## Atomic commit, rollback on failure

If the closure returns `Ok(value)`, the engine iterates the
staged map in key order and applies each write via the regular
durable put or delete path. The application is atomic from the
outside view: every staged write lands or none do (commit cannot
partially fail because the staged writes are validated
ahead of time and the WAL appends are serialized through a single
mutex).

If the closure returns `Err(e)`, the staged map is dropped and the
error bubbles out unchanged. Same for a panic: the transaction's
`Drop` runs as the stack unwinds, the staged map is freed, and no
write reaches the WAL.

There is no explicit `tx.commit()` or `tx.rollback()`. The
closure's return value is the commit signal. This shape is
adopted from `sled` and `redb` and matches the way Rust callers
already think about scoped resource lifetimes.

## Conflict detection (v1.6)

v1.4 deferred conflict detection because the lock-discipline
change to keep the check and the apply atomic with respect to
plain puts deserved its own milestone. v1.6 ships it.

The engine stamps every put and delete with a monotonic
`write_seq`, bumped under the WAL mutex, and tracks the last
`write_seq` at which each key was modified in a `last_writes`
map. `Transaction` records the engine's `write_seq` at start
(`snapshot_seq`) and tracks every key it reads in a `read_set`.
On commit, the engine takes the WAL mutex once and atomically:

1. Walks every key in the read-set and the write-set.
2. If any key's `last_writes` entry exceeds the transaction's
   `snapshot_seq`, the commit aborts with
   [`Error::TransactionConflict`].
3. Otherwise it applies every staged write through an internal
   `apply_entry_locked` helper that assumes the caller already
   holds the WAL mutex. The held mutex stops any concurrent
   plain put or delete from racing the check against the apply.

Recovery: catch `Error::TransactionConflict` and retry the
closure. The retried transaction will see the up-to-date data
and either succeed or abort again on the next conflict.

Pinned by `concurrent_transactions_on_same_key_second_aborts_
with_conflict`, which uses a `std::sync::Barrier` to coordinate
two threads deterministically: T1 reads K, T2 writes K, T1 tries
to commit. The test asserts T1 aborts with
`TransactionConflict` and T2's value remains live.

## What is missing

Async transactions in v1.6 ship as `AsyncLattice::transaction`
(see chapter 12). The closure body itself is synchronous; awaiting
inside the transaction body would require a different shape that
v1.6 does not yet provide. The recommended pattern for workflows
that need to await mid-transaction is to do the reads, await
whatever, and open a fresh transaction; the conflict detection
above guarantees the second transaction will abort if the data
changed in the meantime.

## Tests

`tests/transactions.rs` pins the contract:

- `transaction_isolated_read_view`: a write through a clone after
  the transaction starts is invisible to reads inside it.
- `transaction_read_your_own_writes_within_tx`: stage-then-get
  returns the staged value; stage-delete-then-get returns `None`.
- `transaction_commit_applies_all_writes_atomically`: every
  staged write is visible after the closure returns and survives
  reopen.
- `transaction_rollback_when_closure_returns_err`: the staged set
  is discarded.
- `transaction_rollback_when_closure_panics`: the panic
  propagates and the staged set is discarded.
- `read_only_transaction_commits_with_no_writes`: empty staging
  is a trivial commit.
- `concurrent_transactions_on_disjoke_keys_both_apply`: two
  transactions on disjoint keys serialise through the WAL mutex
  and both writes land.

The conflict-detection test (`concurrent_transactions_on_same_key
_one_aborts`) is intentionally absent until v1.5.
