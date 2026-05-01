# What is not yet implemented

A short catalogue of features intentionally left out of the
current release, with a sentence on why each was deferred.
None of these are bugs; each is a design choice that traded
scope for clarity. The list is kept honest: items that were
deferred at v1.0 and have since shipped are marked with the
release that closed them, so a reader can trace the project's
arc rather than mistake an old deferral for a current one.

## Closed in earlier releases

The following were deferred at v1.0 and ship today:

- **Group commit.** Deferred at v1.0 because the API surface
  needed `WriteOptions` and a commit window. Shipped in **v1.1**
  with `WriteOptions { durable: false }`, a per-database batch
  size, and a per-database commit window. Chapter 7 covers the
  durability trade-off.
- **`Send + Sync + Clone` plus parallel reads.** Deferred at
  v1.0 because the engine took `&mut self` for mutations.
  Shipped in **v1.2**: `Lattice` is now an `Arc<Inner>` clone
  with parking_lot internal locks; `&self` on every method.
  Many readers coexist with one writer.
- **Leveled compaction.** Deferred at v1.0 in favour of the
  simplest-possible "merge all tables" strategy. Shipped in
  **v1.3**: size-tiered leveled compaction with per-level
  thresholds and a manifest v2 format that records the level
  layout. Chapter 5 covers the algorithm.
- **Transactions and isolation levels.** Deferred at v1.0
  because honest transactions needed a write set, a snapshot,
  and a conflict detector. Shipped in **v1.4** (snapshot
  isolation, atomic commit) and **v1.6** (per-key conflict
  detection via `last_writes`, the lost-update guard).
  Chapter 11 covers the design.
- **An async wrapper.** Shipped in **v1.5** as `AsyncLattice`
  behind the `tokio` feature. Honest about being
  async-friendly rather than natively async; native async (no
  `spawn_blocking`, tokio file I/O all the way down) is still
  deferred (see below). Chapter 12 covers the wrapper and
  v1.6's `AsyncLattice::transaction`.
- **Tracing for distributed-tracing collectors.** Shipped in
  **v1.7** as `#[tracing::instrument]` on every public method.
  Chapter 13 covers it.
- **Metrics for operational dashboards.** Shipped in **v1.8**
  as opt-in counters and histograms via the `metrics` crate
  facade. Chapter 14 covers it.
- **Bounded transaction-conflict map.** Deferred at v1.6
  because the lock discipline change was its own milestone.
  Shipped in **v1.10**: `last_writes` trims based on the
  smallest in-flight transaction's `snapshot_seq`. Chapter 11
  covers the trim policy and the soundness invariant.
- **Loom model checking.** Deferred at v1.10 because it
  required extracting the conflict-detection state into a
  module that could be driven under `loom::sync`. Shipped in
  **v1.11**: `crates/lattice-loom-tests` exercises the
  `(write_seq` bump, `last_writes` insert) atomicity and the
  trim-safety invariant under every legal interleaving of
  two and three threads. Chapter 9 covers the suite.

## Still open

### Replication

A distributed log is a different project. Lattice's WAL is an
obvious seed: it already records every mutation in order. A
two-node primary plus replica that ships WAL records over the
network would be a few hundred lines of code and an entirely
different conversation about failure modes (split-brain,
re-election, durability under partition). Tracked as a v2.x
candidate.

### Native async

`AsyncLattice` (v1.5) wraps the synchronous engine on tokio's
blocking pool. The honest version replaces parking_lot with
`tokio::sync` and `std::fs` with `tokio::fs`, removes the
`spawn_blocking` per call, and flips `sync_data` into a
selectively-scoped blocking call. The lock discipline rewrite
is the hard part; tracked as a v2.x candidate.

### MVCC

Snapshot isolation today (v1.4+) clones the memtable and pins
SSTable readers; reads from the snapshot do not block writers,
but the clone is O(memtable size). True MVCC keeps a per-key
version chain with seq numbers, garbage-collects old versions
during compaction, and lets concurrent readers walk the chain
without locking. Tracked as a v2.x candidate.

### Strict RocksDB-style leveled compaction

v1.3's leveled compaction merges every overlapping table in
the source level into a single output. RocksDB partitions
levels into non-overlapping key ranges and only merges the
slice of overlapping tables. The arithmetic is the same; the
bookkeeping is a hundred lines and a test fixture. Tracked as
a v2.x candidate.

### Streaming scan

`Lattice::scan` returns a `Vec`, which means it materialises
every matching pair before the caller sees the first one. For
databases that fit in memory this is fine. The streaming
iterator (`scan_iter` returning `impl Iterator<Item =
Result<(Vec<u8>, Vec<u8>)>>`) walks memtable plus SSTables
through a `BinaryHeap` for the k-way merge. Tracked as a
v1.12 candidate.

### Encryption at rest

The WAL and SSTable formats both compose cleanly with an
encryption layer in front. The choices (authenticated
encryption with what associated data, key rotation, replay
tolerance under decryption failure) are interesting in their
own right and would distract from the storage path. No
release tracked yet.

### Fuzzing

`cargo-fuzz` against the WAL decoder, the SSTable footer
parser, and the bloom filter deserializer would harden the
read path against malformed input. The targets are obvious
(each one parses bytes from disk) and are tracked as a v1.10+
candidate; the implementation needs nightly toolchain plus
`cargo install cargo-fuzz`, both outside the project's
default contributor setup.

### A real benchmark suite

Chapter 7 has four micro-benchmarks. The number of useful
comparisons they support is approximately zero. A real bench
suite runs YCSB-style workloads against Lattice and against
`sled` and `fjall` on the same hardware, with the same
fsync policies, and reports both throughput and latency
percentiles. This is its own project.

## So what is here

Phase 1 through Phase 5 implemented the smallest set of
components that can call themselves an LSM-tree storage
engine and not be lying: durability via WAL, ordered
in-memory memtable, on-disk SSTables with sparse index and
LZ4 blocks, bloom filters, tiered compaction with a
manifest, and snapshot reads. Releases v1.1 through v1.10
added group commit, parallel reads, leveled compaction,
transactions with conflict detection, an async wrapper,
structured tracing, opt-in metrics, and a bounded
transaction-conflict map.

The book exists to document that path in code small enough
to read end to end. The list above is its mirror: the bits
that are honestly missing.
