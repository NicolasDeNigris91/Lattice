# Compaction

Phase 2 introduced SSTables, and Phase 3 made misses cheap. Neither
addressed the elephant: every flush creates a new file, so a workload
that runs long enough piles up SSTables forever. Reads on present keys
are still fast (memtable plus one or two SSTables suffice for a
recently-written key) but space, open-file-count, and the scan path all
degrade linearly in the number of tables. Compaction is the back-pressure.

## What compaction is

Compaction reads `N` existing SSTables and writes one new one whose
contents are the merge of theirs, with two simplifications applied:

1. For any key written multiple times across the inputs, only the
   newest write survives.
2. Any key whose newest write is a tombstone is dropped entirely, since
   the merged output is the bottom of the LSM tree and there is nothing
   below for the tombstone to shadow.

The output is shorter than the inputs (sometimes much shorter). The
inputs are deleted afterwards.

## The strategy: simple tiered

There are two textbook compaction strategies, **tiered** and
**leveled**. Tiered keeps a small number of progressively larger tables
and rewrites them in big merges. Leveled keeps level `k` at roughly
`T^k` the size of level `k-1` and does smaller, more frequent merges.
Leveled has lower read amplification, tiered has lower write
amplification.

Lattice picks an extreme of tiered. There is one logical "tier" of any
size, and when it reaches a configurable count (default four),
**every** SSTable is merged into one. The new merged table is the only
SSTable on disk. The next four flushes refill the tier, and the cycle
repeats.

This trades aggressive read amplification (after each flush, the next
flush re-merges everything that was just merged) for design clarity. A
true tiered strategy with multiple tiers is a Phase 5 appendix exercise.

## The merge

Implemented in [`compaction.rs`][compaction] as a `BTreeMap`
accumulator. We iterate every input SSTable in order from oldest to
newest, calling `iter_all` on each. For each `(key, optional value)`
emitted, `accumulator.insert(key, value)` overwrites any prior entry
for the same key. After all inputs are drained, the accumulator
contains the newest value per key. We stream it out into a new
`SSTableWriter`, skipping entries whose value is `None`.

`BTreeMap` keeps the merge in memory. For Phase 4 workloads this is
fine. A streaming heap-based merge is on the Phase 5 list, useful for
SSTables that do not fit in RAM.

## The manifest

Compaction is the moment the engine can lose data the most easily, so
this phase introduces a real manifest. The manifest is a small file
named `MANIFEST` in the database directory, encoded with `bincode`,
with this shape:

```rust
struct Manifest {
    version: u32,
    next_seq: u64,
    table_seqs: Vec<u64>,
}
```

Every operation that changes the live set of SSTables (flush, compact)
writes a fresh manifest atomically: bytes go to `MANIFEST.tmp`,
`fsync`, then `rename` over `MANIFEST`. After the rename returns, the
manifest is the new truth. Old SSTable files that the new manifest no
longer references are then deleted.

A crash between the rename and the deletes leaves orphans on disk: real
files whose seq numbers are absent from the manifest. `Lattice::open`
scans the directory at startup, compares against the manifest, and
deletes any orphans before doing anything else. This is the cleanup
that makes the compact-then-delete sequence safe to interrupt at any
point.

## Bootstrapping

A directory created by an older Lattice (pre-v0.4) has no manifest.
`Lattice::open` notices, scans for `*.sst` files, treats them as live,
writes a manifest summarising what it found, and continues. The first
open after upgrade pays this one-time cost.

## What about reads during compaction

In Phase 4, compaction is synchronous: `Lattice::compact()` blocks the
thread until the new SSTable is durable and the manifest is updated.
There is no concurrency concern because there is no second thread.

The function does the work in this order:

1. Build the new SSTable in `*.sst.tmp`.
2. Rename to its final `*.sst` name.
3. Open the reader. If this fails, we still have all the inputs intact
   and can retry.
4. Replace the in-memory `Vec<SSTableReader>` with a one-element vec
   holding the new reader.
5. Persist the new manifest.
6. Best-effort delete the old SSTable files.

If the process dies anywhere from step 1 to step 4, the manifest still
points at the old set, so the next open rebuilds the engine from the
old SSTables and the orphan cleanup deletes the new file. If the
process dies between steps 5 and 6, the manifest points at the new
file, the orphan cleanup deletes the old files, and reads work the
moment the engine is up again.

## Trade-offs

**Read amplification.** With our "merge all" strategy, after each
compaction reads scan one SSTable. Then up to four more flushes can
happen before the next compaction, so reads can scan up to five
SSTables in the worst case. With bloom filters this is mostly free for
missing keys.

**Write amplification.** Every byte ever written to disk gets rewritten
on every compaction. For a workload that flushes many small batches,
this adds up. A real tiered strategy with multiple tiers would only
rewrite within a tier when that tier overflows, lowering write
amplification at the cost of a second tier of bloom checks. The book's
"what is not yet implemented" appendix discusses this.

**Synchronous behavior.** A long-running compaction blocks the writer.
Phase 5 splits compaction onto a background task and serializes the
manifest update with a mutex.

[compaction]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/compaction.rs
