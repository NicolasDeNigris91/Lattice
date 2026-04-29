# What is not yet implemented

A short catalogue of features intentionally left out of v1.0, with a
sentence on why each was deferred. None of these are bugs; each is a
design choice that traded scope for clarity.

## Transactions and isolation levels

A real transaction needs at minimum a write set, a conflict detector,
and the ability to undo or roll forward. Lattice has none of those.
Adding them honestly is the size of the project that already exists,
because every layer would have to learn about sequence numbers and
visibility. The right next project is "transactions on top of an LSM",
not "transactions added to this LSM".

## Leveled compaction

The current strategy is the simplest possible tiered: when N tables
exist, merge all of them into one. This pays high write amplification
for low read amplification. A real leveled implementation keeps level
`k` at roughly `T^k` the size of `k - 1` and only merges within a
level when it overflows. The arithmetic of leveled compaction is
elegant; the bookkeeping is a couple of hundred lines that did not pay
for themselves at this scale. Once Lattice is asked to host more than a
single SSTable at a time, leveled is the answer.

## Multi-thread writes

`Lattice` takes `&mut self` for every mutation. The whole engine is
single-writer. This is enforced by Rust's borrow checker, not a
runtime lock, which is pleasant when teaching but not when serving
traffic. A read-write lock around the memtable, plus a separate
flush-and-compact lock, would let many readers coexist with one writer
without changing the on-disk format.

## Replication

A distributed log is a different project. Lattice's WAL is an obvious
seed: it already records every mutation in order. A two-node primary
plus replica that ships WAL records over the network would be a few
hundred lines of code and an entirely different conversation about
failure modes.

## Encryption at rest

The WAL and SSTable formats both compose cleanly with an encryption
layer in front. The reason it is not in v1.0 is that the choices
(authenticated encryption with what associated data, key rotation,
replay tolerance under decryption failure) are interesting in their
own right and would distract from the storage path.

## Streaming scan

`Lattice::scan` returns a `Vec`, which means it materialises every
matching pair before the caller sees the first one. For databases
that fit in memory (Phase 1's intended scope) this is fine. For
larger workloads, the next iteration is to return an iterator that
streams across the memtable and SSTables with a `BinaryHeap` for
k-way merge. This is a minor refactor that we did not need, so we did
not do.

## Group commit

Every `put` calls `fsync` before returning. Group commit batches many
writes into one sync, trading latency for throughput. A real
implementation needs at least an opt-in API ("durable when this future
resolves") and probably a small commit thread. It is the largest
single gain available to a storage engine that has already done
everything else.

## A real benchmark suite

Chapter 7 has four micro-benchmarks. The number of useful comparisons
they support is approximately zero. A real bench suite runs YCSB-style
workloads against Lattice and against `sled` and `fjall` on the same
hardware, with the same fsync policies, and reports both throughput
and latency percentiles. This is its own project.

## So what is here

Phase 1 through Phase 5 implement the smallest set of components that
can call themselves an LSM-tree storage engine and not be lying:
durability via WAL, ordered in-memory memtable, on-disk SSTables with
sparse index and LZ4 blocks, bloom filters, tiered compaction with a
manifest, and snapshot reads. The book exists to document that path
in code small enough to read end to end.

Everything in this chapter is the natural continuation. Pick one and
keep going.
