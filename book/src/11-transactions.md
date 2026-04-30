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

## Trimming `last_writes` (v1.10)

v1.6 left `last_writes` to grow without bound. Every unique key
ever written kept an entry forever, so a long-running process
that touches a wide key set leaks memory in proportion to the
total distinct-key history.

v1.10 closes the leak. `Inner` gains an `active_tx` multiset
keyed by `snapshot_seq`. A new `ActiveTxGuard` registers the
transaction's `snapshot_seq` under `active_tx.lock()` at
transaction start and decrements on drop, including panic
unwinding. Every `put`, `delete`, and transaction commit calls
`maybe_trim_last_writes` after the WAL mutex is released. The
trim is a no-op below `LAST_WRITES_TRIM_THRESHOLD` (1024
entries); above the threshold, it reads the smallest
`snapshot_seq` from `active_tx` and retains only entries whose
`seq` strictly exceeds that cutoff. When no transaction is in
flight, the cutoff is the current `write_seq`, so the map is
cleared in full.

The soundness invariant is "an entry that could still trigger
a conflict for an in-flight or future transaction must
survive". The conflict check fires when `entry.seq >
tx.snapshot_seq`. After trimming with cutoff `C`, every
remaining entry has `seq > C`, and `C <= tx.snapshot_seq` for
every active transaction (because `C` is the minimum), so any
entry that *could* fire a conflict is preserved. New entries
are only added with `seq` greater than every active
`snapshot_seq` (since `write_seq` is monotonic), so the set of
preserved entries grows only by entries that might still be
relevant.

`active_tx` is locked separately from the WAL mutex and from
`last_writes`. The two locks have no ordering relationship: a
transaction takes `active_tx` to register, releases it,
acquires the WAL mutex, applies, releases the WAL mutex, and
finally takes `active_tx` again from the guard's drop to
deregister. The trim takes `active_tx` to read the cutoff and
`last_writes` (write) to retain, never holding the WAL mutex.
This is what lets `apply_entry_locked` keep its single-mutex
discipline.

Pinned by two unit tests inside `lib.rs`:

- `last_writes_does_not_grow_unbounded_without_active_transactions`
  writes 2048 distinct keys with no transaction in flight and
  asserts `last_writes.len() <= 1024`. The cycle is "fill to
  1025, trim to 0, fill again", so the final length is around
  1023.
- `trim_does_not_drop_entries_an_active_transaction_still_needs`
  has T1 hold a snapshot of `K` while T2 does enough
  non-transactional writes to push `last_writes` well past the
  trim threshold, including overwriting `K`. T1's commit must
  abort with `TransactionConflict`. If the trim ate the entry
  for `K`, the conflict goes silently undetected and T1 commits
  a lost-update; the test catches that regression.

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
- `concurrent_transactions_on_disjoint_keys_both_apply`: two
  transactions on disjoint keys serialise through the WAL mutex
  and both writes land.
- `concurrent_transactions_on_same_key_second_aborts_with_conflict`
  (v1.6): two transactions race on the same key; the second
  aborts with `TransactionConflict` and the outside write
  remains live.
