# Design notes for v2.x

The chapters before this one document what Lattice does today.
This chapter documents three concrete design directions tracked
for v2.x. Each section names the problem, the proposed shape,
the trade-off space, and the open questions that need answers
before the work starts. These notes are deliberate scope: the
goal is to make the design legible for review before any code is
written, not to specify an implementation.

The ordering is by independence. `compact_async` and the
SkipMap memtable are local refactors; encryption at rest reaches
across the WAL, the SSTable format, and the manifest, so it
ships last and probably bumps the on-disk format version.

## 1. Non-blocking compaction follow-ups

The user-facing `compact_async()` API and the dedicated worker
thread that backs it are no longer a v2.x design item; the
shape and semantics now live in chapter 5 ("Non-blocking
compaction"). Three open questions remain for a future release:

- **Auto-compaction migration.** Today's auto-compaction at
  flush time stays inline (the writer still pays the cost when
  a level crosses its threshold). Moving auto-compaction to
  `compact_async` is the next step, but it makes the
  post-flush state non-deterministic and requires updating the
  existing test fence to wait for convergence rather than
  assert on it. Tracked as a v2.x candidate.
- **Backpressure**: if compaction falls behind the flush rate
  forever, levels grow unbounded. Today compaction is
  synchronous so the writer naturally backpressures itself.
  With async auto-compaction we need either a queue depth limit
  (compaction "owes" stays bounded by a fixed-size channel) or
  a write-side stall when level depth crosses a high-water
  mark. The latter matches RocksDB's behaviour and is probably
  the right call. The threshold needs to be a builder option,
  not a hard-coded constant.
- **Cancellation**: should `compact()` (the sync wrapper) be
  cancellable? The simplest answer is no: callers that want
  fire-and-forget use `compact_async`; callers that want a
  blocking wait accept the wait. A `wait_timeout` variant is
  a v2.x stretch goal.

The earlier "loom coverage" follow-up has shipped: the loom
suite under `lattice-loom-tests` now drives the compactor's
state machine. `wait_returns_after_paired_schedule_and_finish`
pins liveness (a `wait_for(N)` paired with a `schedule()` and
worker `finish(N, Ok)` returns under every interleaving), and
`shutdown_wakes_pending_waiter` pins the
shutdown-drains-waiters invariant (a `shutdown()` mid-wait
must wake the blocked waiter rather than leave it parked).
Chapter 9 covers the loom infrastructure.

## 2. SkipMap memtable

### Problem

Today the memtable is a `BTreeMap<Vec<u8>, Option<Vec<u8>>>`
behind an `RwLock`. Reads share the lock; writes serialise on
it. The intuition is that under a heavy write workload the
memtable lock is the critical-path bottleneck and a lock-free
skip-list (the `crossbeam-skiplist::SkipMap`) would remove it.

### Architectural finding

A second-pass read of the put path shows that the memtable
write lock is **not** the contention point on a heavy write
workload. `Lattice::put` runs through `apply_entry_locked`,
which holds the WAL mutex (`self.inner.wal.lock()`) for the
entire critical section: the WAL append, the WAL sync, the
`tracker.record_write`, and the memtable `active.write()` plus
insert. Concurrent writers serialise on the WAL mutex *before*
they ever reach the memtable lock, so the memtable lock is
uncontended for them.

A SkipMap memtable in isolation would therefore not move the
needle. To actually parallelise writes the WAL itself has to be
restructured (per-thread WAL shards, group commit with
contention-free append, or an entirely different log
structure). The memtable swap is the second step of that
multi-release sequence, not a single-shot improvement.

This deferral is upgraded from "bench-first decision" to "wait
for WAL parallelisation design". The implementation sketch
below is preserved for that future release; the bench-first
gate still applies (the win must show up on the dashboard).

### Proposal

Replace `MemTable` (currently `BTreeMap` + `RwLock`) with
`SkipMap<Vec<u8>, Option<Vec<u8>>>` (no outer lock). Snapshot
construction reads the SkipMap directly. Flush drains the
SkipMap into a sorted `Vec` then builds the SSTable.

The public memtable API (`put`, `delete`, `iter_all`,
`approx_size`) stays the same; the internal storage swaps out.
`Inner::active` becomes `MemTable` (no `RwLock`). All the
`self.inner.active.read()` / `self.inner.active.write()` call
sites become `self.inner.active.get(...)` /
`self.inner.active.insert(...)`.

### Trade-offs

- **Memory**: SkipMap's per-entry overhead is higher than
  `BTreeMap`'s (each node has multiple forward pointers). The
  flush threshold is denominated in `approx_size` bytes, so
  we'd need a SkipMap-aware estimator. A first cut overprovisions
  by ~30 % to account for skiplist overhead; benchmarks set the
  real number.
- **Throughput**: the win is real for concurrent writers but
  a single-threaded writer pays a small constant overhead per
  insert (more pointer chasing than a `BTreeMap`). Bench
  before committing.
- **Snapshot cost**: a SkipMap snapshot is conceptually free
  (epoch-pinned reads), but materialising the snapshot for
  flush still copies every entry into a `Vec`. Same cost as
  today.
- **Dependencies**: adds `crossbeam-skiplist` (and pulls
  `crossbeam-epoch`). Both are well-vetted, MIT/Apache. Not a
  problem for the licence audit.

### Open questions

- **Bench-first decision**: this change is justified only if
  the criterion concurrent-write benchmark shows a real win
  (say, > 1.5x throughput at 4 threads). If the win is not
  there, the design holds but the implementation waits.
  Bencher.dev tracks the baseline, so the speedup is visible
  on the dashboard the moment the swap lands.
- **Loom coverage**: SkipMap is itself loom-tested upstream,
  but our use of it (snapshot + flush) needs a loom test that
  pins the snapshot-during-flush invariant.
- **Iterator lifetime**: `SkipMap::iter` returns refs tied to
  an epoch guard. The `MemTable::iter_all` API today returns
  refs tied to the `RwLock` guard. Both shapes work, but the
  scan_iter `memtable_source` snapshot today copies into an
  owned `Vec` to escape the borrow; with SkipMap that copy is
  still needed because the epoch guard is bound to the call
  site.

## 3. Encryption at rest

### Problem

Today every byte the engine writes (WAL records, SSTable
blocks, the manifest) lands on disk in cleartext. A compromised
host or a forensic recovery off a discarded drive reads every
key and value. Many deployments need encryption at rest as a
baseline (PCI, HIPAA, internal compliance).

### Proposal

Wrap the I/O layer with an authenticated-encryption pipeline
keyed by a user-supplied 32-byte key. Encryption is an open-time
choice via `LatticeBuilder::encryption_key`; an unkeyed open of
an encrypted directory fails with a clear error, and a keyed
open of an unencrypted directory either fails or upgrades on
demand (TBD; see open questions).

The cipher is XChaCha20-Poly1305 (96-bit nonce gives
collision-resistant per-record nonces with a random prefix).
Each WAL record gets a random nonce written inline before the
ciphertext; each SSTable block gets a deterministic nonce
derived from `(sstable_seq, block_index)` so reads can decrypt
without seeking elsewhere. The manifest is a single small
record encrypted with a per-write random nonce.

The on-disk format version bumps; the SSTable footer gains a
flag bit that says "encrypted blocks". A legacy v0 SSTable
opens cleartext; a new v1 SSTable demands the key. Mixed
directories are supported (a re-encryption pass migrates v0
SSTables to v1 lazily, one per compaction round).

### Trade-offs

- **Performance**: XChaCha20-Poly1305 on modern CPUs is
  ~1 GiB/s per core. The bench suite needs a "with
  encryption" variant so the overhead is measurable. Expect a
  10-20% latency hit on small writes (per-record cipher
  init), bounded by AES-NI's absence on some targets.
- **Key management**: out of scope for the engine. The user
  supplies the key bytes; rotation is a re-encryption pass
  scheduled by the application. The engine does NOT touch
  KMS, envelope encryption, key derivation, or any of the
  ceremony around key handling. Documented as a non-goal.
- **Format compat**: the on-disk format gains a version bump.
  The reopen path checks the footer flag and demands the key
  if set; legacy directories still open cleartext. This means
  a forward-compat story: v1.x clients opening a v2.x
  encrypted directory fail loudly with a "this directory was
  written by a newer Lattice; please upgrade" error.
- **WAL replay**: encrypted WAL records need a per-record
  nonce inline. The WAL header gains a 24-byte nonce field
  per record under encryption; the unencrypted format keeps
  the existing layout under a flag bit in the global WAL
  header.

### Open questions

- **Cleartext-to-encrypted upgrade**: should opening an
  unencrypted directory with a key (a) refuse, (b) silently
  upgrade-on-write (new SSTables encrypted, old SSTables
  cleartext until compacted), or (c) require a deliberate
  `migrate_to_encrypted()` call? Option (b) is friendliest;
  option (c) is least surprising. Probably (c) with (b) as a
  builder option.
- **Authenticated additional data (AAD)**: every cipher call
  takes optional AAD. Binding `(sstable_seq, block_index)` as
  AAD prevents a swapped-block attack. Worth doing.
- **Test coverage**: a property test that "encrypted reopen
  with the correct key returns the same data, with the wrong
  key returns an authentication error" is the obvious
  contract. Add a fuzz target that flips bits inside an
  encrypted block and asserts the decrypt fails (no panic, no
  silent success).
- **Bencher.dev panel**: the encrypted write path needs its
  own benchmark group so the regression detector can track
  the cipher overhead independently of the cleartext
  baseline. See `benches/put_get.rs`.

## What this chapter is not

These are not promises. Each section will get a tracking issue
and a concrete release attribution before the work starts. The
chapter exists so a contributor (or future-me) reading the
deferral list in chapter 8 can find the design context behind
the one-line "tracked as a v2.x candidate" without first
reading every closed PR.
