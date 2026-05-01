# Streaming scan iterator

`Lattice::scan` materialises every visible `(key, value)` pair
into a `Vec` before returning. That is the right shape for a
small-cardinality probe (a prefix sweep over a few hundred keys)
but the wrong shape for an export, a replication backfill, or a
range walk over a multi-gigabyte database. v1.12 adds
`Lattice::scan_iter`, which exposes the same merge-and-dedupe
logic behind an `Iterator`, so callers walk the keyspace one
entry at a time and the engine only holds the merge frontier and
one decoded block per `SSTable` source.

## API

```rust
pub fn scan(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
pub fn scan_iter(&self, prefix: Option<&[u8]>) -> ScanIter;
```

`ScanIter` implements `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>`
and is `Send`, so callers can move it across threads. `scan` itself
is now a thin wrapper around `scan_iter().collect()`; the two APIs
are observationally equivalent under any history of puts, deletes,
flushes, and compactions, pinned by a property test in
`tests/property_durability.rs`.

## Algorithm

The scan composes one source iterator per LSM tier, in newest-first
order:

1. **Active memtable.** Snapshotted at `scan_iter` call time. The
   memtable is bounded by `flush_threshold_bytes` (default 2 MiB),
   so the snapshot is small.
2. **Frozen memtable**, if one is mid-flush. Same snapshot rule.
3. **Each `SSTable`**, walked newest-first within each level and
   shallowest-first across levels (level 0 first, then level 1, and
   so on). Each `SSTable` source is a lazy block-by-block iterator
   that holds an `Arc` to the reader and decodes one block at a
   time.

Each source yields its own entries in key order. A
[`BinaryHeap`][std-bh] holds one peeked entry per source. The
merge pops the smallest key, and on tie the smallest source index
wins (= the newest tier). Older sources at the same key are
drained and discarded. Tombstones are filtered after the dedupe,
so a deletion in a newer tier hides an older live value, exactly
matching the resolution rule used by `get`.

[std-bh]: https://doc.rust-lang.org/std/collections/struct.BinaryHeap.html

## Memory

The cost is independent of the total number of keys in the
database:

| Item                       | Cost                                  |
|----------------------------|---------------------------------------|
| Heap frontier              | `O(num_sources)`                      |
| Per-`SSTable` decoded block | `O(block_size)` (default 4 KiB)       |
| Memtable snapshots         | `O(memtable_size)`, bounded by flush  |

For a database with one active memtable, one frozen memtable, and
ten `SSTable`s across all levels, the scan holds twelve frontier
entries plus ten 4 KiB blocks plus the memtable snapshots; well
under a megabyte regardless of how many keys the scan ultimately
yields.

## Errors

A block read or parse failure surfaces as `Some(Err(...))` from
`next()`. The iterator is exhausted on the next call. Callers can
choose to abort or continue (e.g. log the bad block and skip).
This matches the `Result` shape every other public method returns.

## Why it matters

`scan` was an `O(N)` memory cliff: a 10 GiB scan needed 10 GiB of
result-buffer memory, which is silently fine on a 64 GiB host and
silently fatal on a 4 GiB container. `scan_iter` makes the cost
predictable and proportional to the merge fan-out, which is fixed
by the LSM topology and does not grow with the dataset.

The change is additive. `scan` keeps its old signature so existing
callers do not have to migrate; the implementation simply
delegates to `scan_iter().collect()`. New code that wants
streaming reaches for `scan_iter`; old code is unaffected.

## Tests

- `tests/scan_iter.rs` pins the public contract:
  `scan_iter_matches_scan_on_active_only`,
  `scan_iter_matches_scan_across_memtable_frozen_and_sstable`,
  `scan_iter_honours_prefix`,
  `scan_iter_yields_strictly_increasing_keys`, and
  `scan_iter_filters_tombstones_when_only_visible_in_sstable`.
- `tests/property_durability.rs` adds
  `scan_iter_matches_scan_under_random_history`, the fifth
  property fence: a random op sequence is applied; `scan_iter`
  and `scan` must yield the same `Vec` AND the result must match
  the `BTreeMap` reference's view of live keys.
