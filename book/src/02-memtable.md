# The memtable

Once the WAL has made a write durable, the same write also has to be
visible to the very next `get`. The memtable is where freshly-written data
lives until it is persisted into a sorted string table on disk.

## What the memtable is for

Two jobs:

1. Serve reads for keys that have been written recently.
2. Hold writes in a structure cheap enough to flush to disk in one
   ordered pass when it gets large.

The first job ranks recent writes ahead of older copies on disk, so an
overwrite is a put followed by a get in the same session and gets the new
value. The second job sets the shape of the data structure. We need
ordered iteration for the flush, which kills any unsorted hash map.

## Why ordered

A flush writes the memtable as a sorted string table, which requires
keys in ascending order. If the memtable were unordered we would have to
sort on every flush, which is fine for one flush but expensive in
aggregate. An ordered map pays the cost incrementally on every insert.

## What we use

Phase 1 stores the memtable as a `BTreeMap<Vec<u8>, Option<Vec<u8>>>`.
The value is wrapped in `Option` so that tombstones can live alongside
live writes:

```rust
pub(crate) struct MemTable {
    inner: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}
```

`Some(bytes)` means the key currently has that value. `None` means the
key was deleted in this memtable. Replay applies these the same way live
writes do.

Code lives in [`crates/lattice-core/src/memtable.rs`][memtable].

## Why a `BTreeMap` and not a skip list

Production LSM engines often use skip lists because they support
lock-free concurrent inserts well. Lattice is single-threaded by default
in Phase 1, so concurrency does not pay for itself yet. `BTreeMap` is in
the standard library, has predictable performance, and reads cleanly. We
revisit this when Phase 5 introduces concurrent reads on a frozen
memtable.

## Tombstones, three states, two states

A naive lookup against a memtable wants three answers: "live value
here", "deleted here", "I have never heard of this key". Phase 1 collapses
the last two into `None`, because there is nothing below the memtable to
ask. Phase 2 introduces SSTables, and the memtable will need a richer
return type so that the read path can distinguish "this key was
tombstoned in memory, do not read disk" from "this key is unknown to me,
read on".

For now `MemTable::lookup` returns `Option<&[u8]>`, and a tombstone is
indistinguishable from an absence at the call site. The `iter_live`
helper drops tombstones for the `scan` API so callers see only live
data.

## Trade-offs

`BTreeMap` allocates per insert. A skip list with a memory-pool arena
would allocate less. We are not measuring this yet, so we are not paying
for it yet. Phase 5 has a benchmark that says how much it actually
matters in Lattice's workload.

[memtable]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/memtable.rs
