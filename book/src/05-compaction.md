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

## v1.3: cascading levels

The original "merge everything into one table" worked for the
phases of the book, but it has the worst possible write
amplification: every byte ever written gets rewritten on every
compaction. v1.3 replaces it with a per-level cascade.

The on-disk shape gains levels:

```rust
struct Manifest {
    version: u32,                  // = 2
    next_seq: u64,
    levels: Vec<Vec<u64>>,         // levels[0] = L0, levels[1] = L1, ...
}
```

Manifests written by v1.0..v1.2 (`version = 1`) are still readable;
`Manifest::load` peeks the version byte and migrates legacy
`table_seqs` into `levels[0]`, where the leveled algorithm can
normalise them.

The algorithm
-------------

Per-level threshold (default four). When level `Ln` reaches the
threshold, the auto-compaction trigger picks the shallowest such
level, merges every table in `Ln` into one output, and pushes the
output to `L(n+1)`. Cascading is gradual: each subsequent flush
re-checks and runs at most one more round, so the writer never
pays for the entire cascade in a single call. The user-facing
`compact()` loops until no level holds two or more tables, which
preserves the v1.x semantics ("collapse to one table per non-empty
level") while internally using the leveled engine.

```text
flush     L0 = [t1]
flush     L0 = [t1, t2]
flush     L0 = [t1, t2, t3]
flush     L0 = [t1, t2, t3, t4]   # threshold hit
auto      L0 = []                  L1 = [t5]
flush     L0 = [t6]
... three more flushes ...
auto      L0 = []                  L1 = [t5, t10]
auto      L0 = []                  L1 = []         L2 = [t11]
```

Write amplification per level is roughly the threshold; over `H`
levels the total amplification is `~T * H = ~4 * log_T(N)`, much
better than the original `O(N)`.

Tombstone safety
----------------

`compact_all` no longer drops tombstones unconditionally. It now
takes a `drop_tombstones: bool` parameter; the engine sets it
`true` only when no level at or below the target holds any tables
(otherwise the tombstone in the source is still needed to shadow
older data physically resident at the target level or beyond).

The property test `arbitrary_ops_match_btreemap_after_reopen`
caught two bugs in the first cut of this code: an off-by-one in
the "is the target level the bottom" check, and a within-level
walk that iterated `L1+` in natural (oldest-first) order rather
than the newest-first order needed when tables in those levels can
still overlap by key range. Both are fixed; the property test
runs sixty-four random operation sequences per `cargo test` and
hardens any new edits against regression.

Trade-offs that remained through v1.16
--------------------------------------

The v1.3 through v1.16 algorithm was **size-tiered** rather than
strict leveled: tables within a single level could still overlap
by key range, because each merge produced one output covering the
full key range it saw. Strict leveled (each L1+ partitioned into
non-overlapping ranges, merging includes overlapping target-level
tables) was deferred while the surrounding plumbing solidified.

[compaction]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/compaction.rs

Strict leveled compaction (v1.17)
---------------------------------

v1.17 closes the size-tiered deferral. A round from level `N` to
`N+1` no longer just merges every source table into one new
output that gets appended to the target level. Instead the round
picks the **overlapping subset** of `N+1`, the tables whose
on-disk key range intersects the combined range of the source
level, and merges them together with the source. The
non-overlapping tables in `N+1` are kept in place: same sequence
numbers, same files, no rewrite cost.

Concretely:

1. Snapshot the source level (every table in level `N`).
2. Compute `(source_min, source_max)`, the inclusive combined
   range of the source tables. Each `SSTableReader` exposes
   `min_key()` and `max_key()`, cached at open time so the
   compactor never reaches for disk to make this decision.
3. Partition the existing `N+1` tables into `target_overlap`
   (range intersects `[source_min, source_max]`) and `target_keep`
   (range disjoint from the source).
4. Merge `target_overlap` plus the source level, oldest-first
   for last-writer-wins. The deeper level is older overall, and
   within a level insertion order is sequence-number order, so
   the iteration is `target_overlap.iter().chain(source.iter())`.
5. Install: empty source level; the new level `N+1` is
   `target_keep` plus the merged output, sorted by
   `min_key()` for a deterministic layout.
6. Delete the source files and the `target_overlap` files. Files
   in `target_keep` keep their inodes and their open `File`
   handles inside live `SSTableReader` `Arc`s.

A merge whose inputs cancel out (every key tombstoned, with
`drop_tombstones = true`) returns `CompactOutcome::Empty`: no
file is written and the install step skips appending a new
table. The source level still ends up empty, the
`target_overlap` files are still deleted, and `next_seq`
advances so a future flush does not collide with the unused
number.

The win for write amplification is significant on workloads
that touch a hot subset of the keyspace. A v1.16 compaction of
a 1 GB level `N+1` against a 100 MB level `N` rewrote the full
1.1 GB on every round; v1.17 rewrites only the overlap, often
under 200 MB, and keeps the 900 MB cold subset on disk
unchanged. The property fence
`arbitrary_ops_match_btreemap_after_reopen` runs unchanged: the
selective merge preserves the same observable
`(key, value)` set as the old algorithm.

The drop-tombstones rule tightens slightly. Pre-v1.17 it was
"no level at or below the target holds tables." With strict
leveled the non-overlap subset of the target level is by
construction range-disjoint from the source, so it cannot hold
an older version of any merged key. The refined rule is "no
level **deeper than** the target holds tables": the target
level's keep subset is irrelevant.

The `compact_level(0)` test fence
`strict_leveled_keeps_non_overlapping_l1_sstables_intact`
constructs `L1` with three disjoint ranges, then drives a
fourth round whose source covers only one of them. Pre-v1.17
the round produced four `L1` tables (the new merged output
appended to the existing three); v1.17 produces three (the
overlapping range rewritten in place, the other two untouched
by sequence number).

Non-blocking compaction (v1.13)
-------------------------------

Through v1.12 every compaction round ran on the caller's thread.
`Lattice::compact()` blocked until the merge produced a new
`SSTable`, the manifest was rewritten, and the orphan unlinks
finished. v1.13 hoists the round onto a dedicated background
thread, spawned at `Lattice::open` time and joined when the
last `Arc<Inner>` drops.

`Lattice::compact_async()` schedules a round and returns
immediately; the returned `CompactionHandle::wait()` blocks the
caller on the same `parking_lot::Condvar` the worker notifies
when the round publishes its `completed_generation`. Multiple
in-flight calls coalesce: the worker captures the latest
generation when it wakes, runs as many rounds as the level
layout requires, then publishes that captured generation as
completed. Every caller whose handle is no greater than the
captured value sees `wait()` return.

The synchronous `compact()` is now a one-line wrapper around
`compact_async().wait()`, so callers that need the blocking
shape pay nothing extra (one extra channel hop) but get the
same result. Auto-compaction at flush time stays inline; the
existing test fence relies on a deterministically settled state
after flush, and the win for fire-and-forget callers (a
streaming bulk-load that calls `compact_async` periodically and
never `wait()`s) is the new public API. A v2.x option to also
make auto-compaction async is documented in chapter 15.

Errors are sticky: a failed round records its message in
`CompactorShared::last_error`; every pending `wait()` returns
the cloned `Error::Compaction(...)`; the next successful round
clears the slot. Sticky errors mean a single transient I/O
failure surfaces to the next caller instead of disappearing
into a worker thread, which matches the long-standing
behaviour of the foreground path.

Async auto-compaction with backpressure (v1.19)
-----------------------------------------------

Through v1.18 the auto-compaction trigger inside `flush()`
ran inline on the writer thread: a flush that lifted a level
above its threshold paid the full compaction wall clock
before returning. The contract was simple and the post-flush
state was deterministic, but the writer's tail latency
tracked the merge cost, and a steady producer could not
overlap WAL appends with compaction I/O.

v1.19 hoists the auto-trigger onto the same background
compactor that `Lattice::compact_async` already used. The
flush now reads its post-install LSM state, schedules a
round if any level crosses
[`LatticeBuilder::compaction_threshold`], drops the
[`CompactionHandle`], and returns. The compactor thread runs
`run_pending_compactions` to drain every level above the
threshold, the same routine the foreground `compact()` calls.
Errors from the round are sticky on `CompactorShared` and
surface on the next user-driven
`compact()` / `compact_async().wait()`; they are not
propagated out of `flush` because doing so would re-couple
writer latency to compaction errors, which is exactly the
coupling the migration removed. The compactor thread also
emits a `tracing::warn` event on failure, so a tracing
subscriber sees them in real time.

Without a brake, a producer that outruns the compactor would
let level depth grow unbounded. v1.19 adds a high-water mark:
[`LatticeBuilder::compaction_high_water_mark`] (default
`4 * compaction_threshold`). Below the mark, the auto-trigger
is fire-and-forget; once any level reaches the mark, the
next flush waits on its scheduled compaction generation
before returning. Subsequent flushes resume the
fire-and-forget path until the level depth drops back below
the mark. The knob trades throughput for tail-latency: low
values smooth tails by stalling sooner; high values let
short bursts run flat-out while bounding worst-case depth.
`usize::MAX` disables backpressure.

The behavioural test fence
`auto_compaction_runs_on_background_thread_after_flush_returns`
in `tests/stats.rs` constructs a `compaction_threshold = 2`
database with backpressure off, drives twenty flushes, calls
`compact()` to settle, and asserts every key reads back to
its written value. The historical
`compaction_state_survives_reopen` integration test relaxes
its file-count assertion from `== 1` to `>= 1`: the
asynchronous trigger now runs the full
`run_pending_compactions` cascade on each scheduled round
rather than the legacy single-level inline trigger, so the
final layout can spread across more levels than the
sync algorithm produced. The load-bearing contract was always
that data survives reopen, not the precise on-disk layout.

The migration is complementary to the v1.17 strict-leveled
rewrite: strict-leveled cuts write amplification on a single
round; v1.19 lets the compactor consume that win in real
time without holding up the writer.

Bounded waits (v1.20)
---------------------

`CompactionHandle::wait` blocks indefinitely until the
scheduled round completes. v1.20 adds a bounded variant,
`CompactionHandle::wait_timeout(Duration)`, that returns
`Ok(true)` once the round completes, `Ok(false)` if the
deadline elapses first, and
`Err(Error::Compaction(...))` on a sticky compaction failure.
The handle is consumed in either case (matching `wait`); a
caller that needs to retry schedules a fresh handle through
`Lattice::compact_async`.

Operationally the helper closes the gap between
"fire-and-forget compaction" and "I want to know within
N seconds whether it finished". Tests, ops dashboards, and
async wrappers (`tokio::time::timeout` over an
`AsyncLattice` blocking task) all reach for the bounded
variant first.

The implementation rides
`parking_lot::Condvar::wait_for` and folds spurious wake-ups
into a deadline-aware loop: each iteration recomputes the
remaining time against an absolute `Instant + timeout`
deadline, so the caller always sees at most one wait of
`timeout`. The loom variant follows the same shape against
`loom::sync::Condvar::wait_timeout`, and the loom suite under
`lattice-loom-tests` continues to drive the same state
machine the production path uses.

The internal-mod test pair
`compaction_handle_wait_timeout_returns_true_when_round_completes`
and
`compaction_handle_wait_timeout_returns_false_when_deadline_elapses`
pin both branches: a real `compact_async` round with a
generous deadline, and a synthetic handle whose target
generation cannot be reached (`u64::MAX`) so the wait must
observe the deadline.
