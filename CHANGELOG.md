# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.3.0] - 2026-04-29

### Added
- Manifest on-disk format v2 (`version = 2`). The flat
  `table_seqs: Vec<u64>` becomes `levels: Vec<Vec<u64>>`, one
  entry per LSM level. Manifests written by v1.0..v1.2 are still
  readable; `Manifest::load` peeks the version byte and migrates
  legacy `table_seqs` into `levels[0]`.
- Size-tiered leveled compaction. Per-level threshold (default 4)
  triggers a single-round merge of an overfull level into one
  output pushed to the next level down. Cascading happens
  gradually across successive flushes, bounding the writer's
  worst-case latency. The user-facing `compact()` loops until no
  level holds two or more tables, preserving the v1.x "collapse
  everything" semantics on top of the new engine.
- `compact_all` now takes a `drop_tombstones: bool` parameter so
  the engine can keep tombstones in non-bottom levels (where they
  still need to shadow data physically resident in target or
  deeper levels).
- New `data_survives_cascading_leveled_compaction` integration
  test covers the L0 -> L1 -> L2 cascade end-to-end.
- Two new unit tests in `manifest::tests`:
  `manifest_v2_round_trip_through_disk` and
  `manifest_v1_on_disk_upgrades_to_v2_with_everything_in_l0`,
  written test-first.
- Book chapter 5 ("Compaction") gets a "v1.3: cascading levels"
  section explaining the new algorithm, the manifest evolution,
  the tombstone safety check, and the remaining gap to strict
  leveled.

### Changed
- `State::all_sstables_newest_first` now walks every level
  end-to-start (not just L0), because tables within a single L1+
  level can still overlap by key range under the size-tiered
  algorithm. The same change applies to `Snapshot::ssts_newest_first`.
- Internal hard guard `MAX_LEVELS = 7` caps the cascade depth.

### Notes
- Write amplification drops from `O(N)` (every compaction
  rewrites the entire dataset) to `~T * log_T(N)` (a level holds
  at most `T` tables before being pushed down). On the development
  machine, the existing `sequential_write_*` benches are unchanged
  by this commit because they do not stress compaction; a
  dedicated write-amplification bench is recorded as v1.4 work.
- v1.3 ships **size-tiered** compaction, not strict RocksDB-style
  leveled (which keeps L1+ partitioned into non-overlapping
  ranges). Strict leveled and the per-level size knobs
  (`level_size_multiplier`, `level0_target_bytes`) are tracked as
  v1.4 milestones.

## [1.2.0] - 2026-04-29

### Added
- `Lattice` is now `Send + Sync + Clone`. Cloning is one atomic
  increment on a shared `Arc<Inner>`; multiple threads can hold a
  handle and read the database in parallel. Writes still serialise
  through a single WAL mutex.
- `State` introduces a `frozen: Option<Arc<MemTable>>` slot so a
  flush can drain the active memtable without ever leaving readers
  unable to find data they previously wrote. Read-your-writes is
  preserved across the SSTable build.
- Background flusher thread `lattice-flusher` honours
  `LatticeBuilder::commit_window`. Non-durable writes are now
  guaranteed to reach stable storage within the configured window
  even when no further writes arrive to cross the batch threshold.
  The thread holds a `Weak<Inner>`, exits cleanly when the last
  handle drops, and is joined by `Inner::Drop` before the final
  sync.
- `tests/concurrency.rs`: `lattice_is_send_and_sync` (compile-time
  assertion), `cloned_handle_observes_writes_from_origin`,
  `clone_keeps_database_alive_after_origin_drops`, and
  `many_readers_and_one_writer_see_consistent_state` (stress: 8
  reader threads against 2000 durable puts).
- `tests/group_commit.rs`:
  `background_flusher_syncs_within_commit_window` (TDD-driven; the
  RED was watched before the timer thread was implemented).
- `bench_concurrent_random_read_hits` covers 1, 2, 4, 8 reader
  threads. Aggregate throughput on the development machine: 56k
  reads/s (1t) -> 143k reads/s (8t), a 2.55x scaling.
- Book chapter 9 ("Concurrency") explains the new shape, the
  frozen-memtable trick, the background flusher, and the
  measured numbers.

### Changed
- Every public method on `Lattice` now takes `&self` instead of
  `&mut self`. Source-compatible: code that compiled with `&mut
  self` continues to compile with `&self`.
- `Lattice::flush` no longer holds the active memtable lock for
  the duration of the SSTable build; it moves the data into
  `state.frozen` first, then releases the locks.

### Removed
- `Lattice::set_flush_threshold` and `Lattice::set_compaction_threshold`
  (both were `#[doc(hidden)]`). Migrate to
  `Lattice::builder(path).flush_threshold_bytes(n).compaction_threshold(n).open()`.

### Deferred
- Background compactor (`compact_async` / `compact_blocking`) was
  planned for M2.4 and pushed to v1.3. `compact()` remains
  blocking; rare in practice and the lock-discipline change is
  large enough to warrant its own milestone.

## [1.1.0] - 2026-04-29

### Added
- `WriteOptions { durable: bool }` and `Lattice::put_with(k, v, opts)`
  for opt-in amortised durability. The default remains "every write
  is `fsync`ed on return", so `db.put(k, v)` keeps its v1.0.x
  semantics. Non-durable writes coalesce in the WAL `BufWriter`
  until the configured `commit_batch` threshold (default 64), an
  explicit `flush_wal` call, or a graceful drop.
- `Lattice::flush_wal()` to checkpoint pending non-durable writes
  on demand. Also called from `Drop` so a normal close loses
  nothing.
- `LatticeBuilder` (reach via `Lattice::builder(path)`) with
  `flush_threshold_bytes`, `compaction_threshold`, `commit_window`,
  and `commit_batch` setters and a consuming `open()`.
  `Lattice::open(path)` is now a shorthand for
  `Lattice::builder(path).open()` and remains source-compatible.
- `bench_sequential_write_amortized_10k` criterion benchmark next to
  the existing `sequential_write_10k`. On the development machine the
  pair shows ten seconds versus a hundred and sixty milliseconds for
  ten thousand puts (~62x speedup).
- New integration test `builder_configures_flush_threshold` and
  six contract tests in `tests/group_commit.rs` written test-first.
- Dual license: the workspace is now `MIT OR Apache-2.0`. New
  `LICENSE-APACHE` (canonical Apache 2.0 text). The previous
  `LICENSE` was renamed to `LICENSE-MIT`.

### Changed
- Book chapter 1 ("the write ahead log") gains a "Group commit (v1.1)"
  section explaining the new path and the trade-off that the
  honesty test pins.

### Notes
- `LatticeBuilder::commit_window` is reserved API: the value is
  accepted and stored but not yet honoured, because the timer needs a
  background thread that lands cleanly with M2's concurrency rework.
  Setting a large duration here is safe today and will become
  meaningful once M2 ships.

## [1.0.1] - 2026-04-29

### Added
- Directory `fsync` after every rename that publishes a new on-disk
  file (manifest save, SSTable flush, SSTable compaction). Required on
  POSIX so the rename is durable across power loss; no-op on Windows
  where rename atomicity covers the dirent.
- Two new tests:
  - `snapshot_serves_multi_block_reads_after_files_unlinked` pins the
    contract that an `Arc<SSTableReader>` keeps a file readable on
    POSIX even after compaction unlinks the dirent.
  - `open_cleans_orphans_left_by_a_simulated_post_compact_crash`
    hand-rolls a manifest that references one SSTable while two more
    sit on disk, asserting the orphan sweep runs.
- `deploy/Dockerfile` and `deploy/Caddyfile` plus `railway.json` to
  build and serve the book on Railway as
  `lattice.nicolaspilegidenigris.dev`.
- `DEPLOY.md` documenting Railway and crates.io setup.

### Changed
- `Error::MalformedSstable` renamed to `Error::MalformedFormat`
  because it is also surfaced for malformed manifests. **Breaking**
  for anyone already depending on this variant by name (no published
  consumers).
- `bench_sequential_write` now returns the temp directory from the
  measured closure so its recursive removal happens after the timer
  stops, removing teardown noise from the reported number.
- `release.yml` skips `cargo publish` cleanly when
  `CARGO_REGISTRY_TOKEN` is absent on the repository, logging a
  warning instead of failing the workflow.

### Documentation
- Chapter 3 now describes the v2 SSTable layout (with bloom block and
  48-byte footer) and forward-references the Phase 3 evolution. The
  stale "we do not yet clean tmp files" line is replaced by an
  accurate reference to Phase 4's orphan sweep.

## [1.0.0] - 2026-04-29

### Added
- `Lattice::snapshot()` returns a read-only point-in-time view that
  ignores subsequent puts, deletes, flushes, and compactions on the
  parent.
- `Snapshot::get` and `Snapshot::scan` mirror the `Lattice` read path
  on a frozen memtable clone plus `Arc<SSTableReader>` references.
- `Snapshot` is `Clone + Send`, can be shipped across threads.
- Real Criterion benchmarks: `sequential_write_10k`,
  `random_read_hits_10k`, `random_read_misses_10k`, `scan_all_10k`.
- Book chapters 6 (snapshots), 7 (benchmarks with measured numbers),
  and 8 (what is not yet implemented).

### Changed
- `Lattice.sstables` is now `Vec<Arc<SSTableReader>>` so snapshots can
  share open handles with the engine without duplicating any state.
- `compaction::compact_all` now takes `&[&SSTableReader]`.
- `MemTable` derives `Clone` to enable snapshot creation.

### Known limitations
- A live `Snapshot` on Windows can prevent compaction from immediately
  deleting the obsolete `.sst` files, since the snapshot keeps the
  file handles open. The orphan sweep on next `Lattice::open` reclaims
  the disk. POSIX systems are not affected.

## [0.4.0] - 2026-04-29

### Added
- Tiered compaction that merges every live `SSTable` into a single
  replacement, dropping tombstones (safe at the bottom of the LSM).
- `Lattice::compact()` public API + `lattice compact` CLI subcommand.
- Auto-compaction when the live SSTable count reaches a configurable
  threshold (default 4).
- Persistent `MANIFEST` file (`bincode`, atomic save via temp + rename
  + `fsync`) tracking `next_seq` and the live SSTable sequence numbers.
- Orphan cleanup on `open`: any `*.sst` file whose sequence number is
  absent from the manifest is deleted, recovering the disk left over
  from a crash mid-compaction.
- Bootstrap path for directories that pre-date the manifest: scan
  existing `*.sst` files and write the manifest summarising them.
- 9 new integration tests covering manual compact, auto-compact at
  threshold, tombstone dropping, reopen after compact, manifest
  presence, orphan deletion, no-op compactions, and big-flush
  scenarios.
- Property test extended with an `Op::Compact` variant. Generated
  sequences mix put, delete, flush, and compact.
- Book chapter 5 (compaction).

## [0.3.0] - 2026-04-29

### Added
- Per-`SSTable` Bloom filter at ~1% false positive rate (10 bits per
  key, 7 hash functions via Kirsch-Mitzenmacher double-hashing on a
  single `xxh3_128` digest).
- `SSTableReader::get` short-circuits with `Absent` on a negative bloom
  probe, skipping index lookup, block read, decompression, and scan.
- `BloomFilter::serialize`/`deserialize` for a fixed wire format
  embedded in the SSTable file.
- Book chapter 4 (bloom filters).

### Changed
- **BREAKING**: `SSTable` format version bumped from 1 to 2. The footer
  grew from 32 to 48 bytes to hold `bloom_offset` and `bloom_length`.
  Phase 2 SSTables do not open under Phase 3.

## [0.2.0] - 2026-04-29

### Added
- Immutable on-disk sorted string tables (`SSTable`) with LZ4-compressed
  data blocks, sparse index, and 32-byte footer.
- `SSTableWriter` (streaming, key-ordered) and `SSTableReader` (footer
  parse, sparse index lookup, block-level scan).
- `Lattice::flush` API that drains the memtable into a new SSTable,
  renames atomically from `*.sst.tmp`, then truncates the WAL.
- Auto-flush at a configurable byte threshold (default 4 MiB).
- Three-state `Lookup` enum on the memtable, distinguishing tombstones
  from absence so the read path knows whether to consult older layers.
- Mixed-source read path: memtable first, then SSTables newest to
  oldest, returning on the first non-`Absent` answer.
- Newest-source-wins merge in `scan`, including across multiple
  SSTables.
- `discover_sstables` on `open`, sorting by sequence number derived from
  the filename.
- Property test extended with an `Op::Flush` variant so generated
  sequences exercise interleaved flushes plus reopen replay.
- Book chapter 3 (sorted string tables).

### Fixed
- WAL truncation now opens a separate write-mode file handle, working
  around Windows ACL behaviour where `FILE_APPEND_DATA` does not grant
  `FILE_WRITE_DATA`.

## [0.1.0] - 2026-04-29

### Added
- Workspace skeleton, CI on push and pull request, mdBook scaffolding.
- Project conventions documented in `CLAUDE.md` (untracked) and `README.md`.
- Append-only WAL with CRC32 record integrity, `fsync`-per-write
  durability, and torn-write tolerant replay.
- In-memory ordered memtable backed by `BTreeMap`, with tombstones for
  deletes.
- Public `Lattice::{open, put, get, delete, scan}` API on top of WAL plus
  memtable.
- `lattice` CLI with `put`, `get`, `delete`, `scan`, and `compact`
  (compaction surfaces a placeholder error until Phase 4).
- Property-based test suite covering arbitrary `put` and `delete`
  sequences against a `BTreeMap` reference, with a reopen step on every
  case to exercise replay.
- Book chapters 1 (the write ahead log) and 2 (the memtable).

[Unreleased]: https://github.com/NicolasDeNigris91/Lattice/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.3.0
[1.2.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.2.0
[1.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.1.0
[1.0.1]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.0.1
[1.0.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.0.0
[0.4.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.4.0
[0.3.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.3.0
[0.2.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.2.0
[0.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.1.0
