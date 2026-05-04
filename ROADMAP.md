# Roadmap

This document is the public, structured complement to the
[CHANGELOG](CHANGELOG.md) and the [book's "what is not yet
implemented" chapter](https://lattice.nicolaspilegidenigris.dev/08-not-yet.html).
The CHANGELOG records what shipped; the book chapter records
the design rationale; this file records the planned arc, in
release order, with each entry shaped for an at-a-glance read.

The list is the author's plan, not a contract. Items move
between buckets when the design needs to. Open an issue if
something here is wrong, missing, or in the wrong order.

## v2.x targets

These are the multi-release sequences sketched in book chapter
15 ("Design notes for v2.x"). Each one needs at least one
design pass and several engineering sessions; none ship in a
single minor.

### Encryption at rest

Status: full design doc shipped in book chapter 19; ready
to begin phase A. Chapter 15 §3 keeps the original sketch
for context.

Scope: every byte the engine writes (WAL records, SSTable
blocks, manifest) flows through an authenticated-encryption
pipeline keyed by a user-supplied 32-byte key.
XChaCha20-Poly1305 with deterministic per-block nonces; the
on-disk format gains a version bump.

Blockers: none. The chapter-15 open questions are resolved
in chapter 19: cleartext-to-encrypted upgrade refuses by
default with an opt-in `allow_legacy_upgrade(true)` flag,
AAD binds `(sstable_seq, block_index)` for SSTable blocks
and equivalent location-tags for WAL records and the
manifest, and the test fence is a round-trip property
fence plus a wrong-key contract test plus a bit-flip fuzz
target. The implementation phases (A through F) are
listed in chapter 19 with per-phase test gates.

### WAL parallelisation (and the SkipMap memtable that follows)

Status: chapter 15 §2 documents the architectural finding
that the memtable lock is not the contention point on writes;
the WAL mutex serialises writers upstream. SkipMap memtable
is therefore the second step of a sequence whose first step
is splitting the WAL into independently-appendable shards (or
adopting group commit with contention-free append).

Scope (first step): per-thread WAL shards with a deterministic
merge-on-replay rule, OR a lock-free WAL ring buffer.
Scope (second step): swap `MemTable` from
`BTreeMap + RwLock` to `crossbeam-skiplist::SkipMap`.

Blockers: need to validate via criterion + bencher.dev that
the win at four-thread concurrent writes is at least 1.5x
before committing.

### Strict leveled compaction

Status: chapter 8 marks this as a v1.4+ deferral; today's
algorithm is size-tiered leveled (each level merges into the
next as one big rewrite).

Scope: each L1+ level partitions into non-overlapping key
ranges; compaction picks one source sstable from level N and
merges only the overlapping subset of level N+1; output is
split into per-range sstables when its size exceeds the
per-level target.

Blockers: per-level target sstable size, range-overlap
search, multi-output write path, manifest schema (probably
unchanged but needs verification). Roughly two engineering
sessions plus a property test refresh.

### Replication

Status: book chapter 8 deferral.

Scope: a primary plus replica that ship WAL records over a
network. Lattice's WAL is an obvious seed (it already records
every mutation in order); the work is the replication
protocol and the failure-mode story (split-brain, re-election,
durability under partition).

Blockers: this is its own project. Tracked because it shows
up on any "how does this compare to RocksDB" comparison
(book chapter 17).

### Native async

Status: book chapter 8 deferral.

Scope: replace `parking_lot` with `tokio::sync` and `std::fs`
with `tokio::fs` so `AsyncLattice` (the v1.5 wrapper) can
run without `spawn_blocking`. Includes the work from
"WAL parallelisation" above plus `tokio::fs::sync_data`
selectively-scoped.

Blockers: the lock discipline rewrite is the hard part; loom
already covers the conflict tracker and compactor, but a
tokio-native variant needs its own loom equivalent.

### YCSB-style benchmark vs sled / fjall / rocksdb

Status: book chapter 17 ("honest performance note") declines
to make any apples-to-apples claim until this exists.

Scope: a separate benchmark harness that runs YCSB workloads
A through F against Lattice and against the alternatives on
the same hardware, the same fsync policy, and the same value
sizes. Reports throughput and latency percentiles.

Blockers: this is its own project. The bencher.dev workflow
already tracks Lattice's own micro-benchmarks across releases;
this would be a new comparison harness on top.

## v1.x continuing

These are the smaller items that fit inside a v1.x minor and
do not need a multi-release sequence.

- **Auto-compaction async migration.** Today's auto-compaction
  at flush still runs inline (the writer pays the round
  cost). v1.13 added `compact_async`; switching the implicit
  trigger to it is the next step but makes the post-flush
  state non-deterministic, which the existing test fence
  asserts on. Tracked as a v2.x candidate to keep the test
  contract clean.
- **Backpressure for compact_async.** Today an unbounded
  request queue can grow if compaction falls behind the
  flush rate. RocksDB-style write stall when level depth
  crosses a high-water mark is the right shape. Builder
  option, not a hard-coded constant.
- **Cancellation token on `CompactionHandle`.** Today the
  handle can only be `wait()`-ed or dropped. A
  `wait_timeout` variant is a v2.x stretch goal; a
  `cancel()` on the synchronous path is open.
- **`Lattice::byte_size_on_disk()`.** Operational
  introspection: total bytes Lattice has written under the
  database directory. Cheap (sum sstable sizes from the
  manifest plus WAL length); useful for capacity planning.
- **`Lattice::checksum()`.** Replication-style validation:
  hash the entire visible keyspace (memtable + sstables) and
  return a fingerprint. Useful for cross-host divergence
  detection ahead of full replication.
- **More fuzz targets.** Today the fuzz matrix has
  `wal_open`, `sstable_open`, `manifest_open`, and
  `engine_history`. A `transaction_history` target that
  drives the transaction commit path with random conflict
  patterns is the next addition.

## What is intentionally out of scope

These are excluded by design, not by deferral. They are not
on the roadmap because they would change what Lattice is
trying to be.

- **Multi-host clustering.** The replication item above is
  the closest we go; horizontal scale-out is out of scope.
- **Network protocol surface.** Lattice does not open
  sockets. The library boundary is the `Lattice` handle.
- **Multi-tenant isolation.** No concept of users, quotas,
  or per-key access control inside the engine.
- **Plugin compaction strategies.** RocksDB has a shelf full;
  Lattice ships exactly one (size-tiered leveled, with
  strict leveled tracked as v2.x).
- **Time-series / column-family / secondary-index extensions.**
  All possible to build on top; out of scope inside the
  storage engine itself.
