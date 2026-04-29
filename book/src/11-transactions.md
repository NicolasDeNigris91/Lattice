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

## What is missing: conflict detection

v1.4 does not yet detect conflicts between concurrent transactions
that touch the same key. If two transactions stage a write to key
`K`, both see the original value during their reads, both stage
their own write, and both commit; the second commit overwrites
the first. This is the **lost-update** anomaly that real
snapshot-isolation engines prevent.

The right fix is a per-key write-seq counter on the engine:

- The engine stamps every put and delete with a monotonic
  `write_seq`.
- A transaction records the engine's `write_seq` at start
  (`snapshot_seq`) and tracks every key it reads or stages.
- At commit time, before the apply step, the engine acquires the
  write lock and checks every key in the read-set or write-set:
  if any key's last `write_seq` is greater than the transaction's
  `snapshot_seq`, the commit aborts with
  `Error::TransactionConflict`. Otherwise the apply runs
  atomically (the held write lock keeps any plain put from
  racing the check).

Implementing this cleanly requires reshaping the put / delete
internals so the conflict check and the WAL append can share one
held wal-mutex without re-entrancy issues. That refactor is
substantial and lands in v1.5; v1.4 ships the read isolation,
the staged writes, and the atomic apply, with the conflict
detection clearly listed as the next step.

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
