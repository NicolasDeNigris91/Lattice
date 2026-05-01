# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `deny.toml` at the workspace root, plus a `cargo deny check`
  job in CI. Audits advisories, licences (explicit allow list),
  duplicate dependencies (warn), wildcard versions (deny), and
  source registries (only the public crates.io). Superset of the
  existing `cargo audit` job, which stays for a second opinion
  on the advisory database.
- Coverage CI job using `cargo-llvm-cov`. Prints a summary on
  every run and uploads an `lcov.info` artifact (14-day
  retention). Not gated; the artifact is for review. Optional
  Codecov upload step that skips silently when no
  `CODECOV_TOKEN` secret is configured, so external
  contributors' fork PRs do not fail on a missing secret.
- `miri` CI job, nightly toolchain, warn-only. Catches
  undefined behaviour and aliasing violations in the lib unit
  tests (the bloom and manifest tests, plus the new
  trim tests). The job is informational; failures appear in
  the run but do not block CI. Lattice is
  `forbid(unsafe_code)`, so any miri finding here is a
  dependency or std diagnostic, not a project bug.
- `.github/dependabot.yml`. Weekly cargo and GitHub Actions
  updates, grouped so dev-deps and patches each become a single
  PR per week instead of a flood.
- `.github/ISSUE_TEMPLATE/{bug_report,feature_request,config}.yml`
  and `.github/PULL_REQUEST_TEMPLATE.md`. The PR template
  mirrors the CI bar so contributors can pre-flight before
  pushing.
- `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1).
- Two new property tests in `tests/property_durability.rs`:
  `snapshot_isolates_reads_from_subsequent_writes` (a snapshot
  taken at point `t` returns values as of `t`; subsequent
  writes do not change what the snapshot sees) and
  `compaction_preserves_last_writer_wins` (forced compaction
  is a pure data-preserving rearrangement; every key reads
  back to the reference's value without a reopen). Each runs
  64 cases per `cargo test`. Together with the existing
  reopen-roundtrip property they form the project's
  three-pillar correctness fence: replay-on-reopen, snapshot
  isolation, and compaction equivalence.
- `.github/workflows/book.yml` deploys the mdBook to GitHub
  Pages on every push that touches `book/**`. Acts as a free
  redundant mirror of the Railway-hosted book at
  `lattice.nicolaspilegidenigris.dev`. Concurrency is gated
  per Pages convention (the latest commit wins). Requires
  "Source: GitHub Actions" in the repository's Pages settings
  before the first deploy.
- Cross-platform binary release artifacts. The `release.yml`
  workflow now builds the `lattice` CLI for x86_64 / aarch64
  Linux, x86_64 / aarch64 macOS, and x86_64 Windows on every
  tag. Each archive ships the binary plus README, dual-licence
  files, and CHANGELOG, alongside a SHA-256 checksum file for
  verification. Users without a Rust toolchain can grab a
  binary from the GitHub release page instead of running
  `cargo install`.

### Changed
- `Cargo.toml` polish for crates.io publishing: workspace gains
  a `documentation = "https://docs.rs/lattice-core"` field
  inherited by both crates; each crate gains an `include`
  filter so `cargo publish` packages only sources and manifest,
  not stray local files. `lattice-cli` gains its own `keywords`
  and `categories` (the workspace defaults are library-shaped).
- `CONTRIBUTING.md` adds the `cargo deny` and coverage rows to
  the project bar and points to the new code of conduct.
- `README.md` is restructured for first-impression density: a
  Features section near the top, three "at a glance" examples
  (sync, transactional, async), and a "Where Lattice fits"
  paragraph that names sled, fjall, and RocksDB up front. The
  long roadmap table is collapsed under a `<details>` element.
  A new Contributing section points at the code of conduct and
  the security advisory form.

### Notes
- No version bump. Pure infrastructure; the next feature
  release rolls these in.

## [1.10.0] - 2026-04-30

Closes a memory leak that landed in v1.6 and was deferred in
the changelog at the time. The fix is local to `Inner`; no
public API changes.

### Added
- `Inner::active_tx`: a `Mutex<BTreeMap<u64, usize>>` multiset
  of currently-live transaction `snapshot_seq` values. The
  smallest key is the trim cutoff.
- `ActiveTxGuard`: scope guard returned by
  `Lattice::register_active_tx`. Decrements the multiset on
  drop, including panic unwinding.
- `Lattice::maybe_trim_last_writes`: size-triggered trim of the
  `last_writes` map. Threshold is 1024 entries
  (`LAST_WRITES_TRIM_THRESHOLD`). Above the threshold, retains
  only entries with `seq` strictly greater than the smallest
  active `snapshot_seq` (or the current `write_seq` when no
  transaction is in flight). Called from `put_with`, `delete`,
  and `transaction` after the WAL mutex is released.
- Two new unit tests inside `lib.rs`:
  - `last_writes_does_not_grow_unbounded_without_active_transactions`
    pins the resource bound: 2048 distinct-key writes leave
    `last_writes.len() <= 1024`.
  - `trim_does_not_drop_entries_an_active_transaction_still_needs`
    pins the soundness invariant: while T1 holds a snapshot of
    `K`, T2's overwrite plus 2048 noise writes must not let
    T1's commit silently succeed; T1 must still abort with
    `Error::TransactionConflict`.
- Book chapter 11 ("Transactions") gains a "Trimming
  `last_writes`" section explaining the cutoff, the soundness
  invariant, the lock discipline, and the two new tests.

### Changed
- `Lattice::transaction` now goes
  `snapshot()` -> `register_active_tx()` (captures
  `snapshot_seq` under the `active_tx` lock) -> closure -> WAL
  apply -> guard drop -> `maybe_trim_last_writes`. Capturing
  `snapshot_seq` inside the `active_tx` lock guarantees that a
  concurrent trim sees this transaction's registration before
  it computes its cutoff. Previous behaviour (snapshot first,
  then a free-running `write_seq.load`) is preserved at the
  observable level: any write between the snapshot and the seq
  capture is read-skew, not a phantom commit.
- The end of chapter 11's test list is brought up to date: the
  v1.6 conflict-detection test is now listed, and a typo in the
  disjoint-keys test name is fixed.

### Notes
- TDD discipline: the resource-bound test was written first,
  watched fail (`got 2048, expected <= 1024`), and went green
  on the implementation. The soundness test was written
  second, before any wiring of `register_active_tx` to the
  transaction path, so the test exercised both the registration
  and the trim cutoff.
- `active_tx` and the WAL mutex are deliberately unrelated.
  Transactions take `active_tx` to register, release it, then
  acquire the WAL mutex; the trim takes `active_tx` and
  `last_writes` (write) but never the WAL mutex. This keeps
  `apply_entry_locked`'s single-mutex discipline intact.
- 75 tests pass (73 prior integration tests plus the two new
  unit tests in `lib.rs`).

## [1.9.0] - 2026-04-30

### Added
- `tracing-test` 0.2 in workspace dev-dependencies. Three
  behaviour-pinning integration tests now carry
  `#[traced_test]`:
  - `transaction_commit_applies_all_writes_atomically`
  - `concurrent_transactions_on_same_key_second_aborts_with_conflict`
  - `commit_batch_threshold_makes_non_durable_durable_without_flush_wal`
  Each annotated test installs a per-test `tracing` subscriber
  for the duration of the test, scoped so concurrent tests do
  not see each other's events. The annotation also exposes the
  `logs_contain` and `logs_assert` helpers, available for
  assertions that check trace output as part of the contract
  surface.
- Book chapter 13 ("Observability") gains a "Tracing inside
  tests" section explaining the macro, when to reach for it,
  and how to combine it with `RUST_LOG=lattice_core=debug
  cargo test -- --nocapture` to surface engine events from a
  failing test without wiring a subscriber by hand.

### Notes
- Closes the second half of the M6 milestone alongside v1.7
  (spans on every method) and v1.8 (metrics opt-in). M6 is now
  complete: distributed-tracing collectors, operational
  dashboards, and a test harness that surfaces engine events
  on demand all share the same instrumentation.
- `#[traced_test]` only captures events on the test thread by
  default. Multi-threaded tests (the conflict-detection one in
  particular) emit spans from spawned threads, so the
  annotation is decorative there: the macro plays nicely, but
  `logs_contain` cannot assert across threads. The chapter
  flags this honestly.
- The annotated set is intentionally small. The contract is
  "this test should produce useful trace output when you
  reach for it", not "every test asserts on its trace
  output". Annotating every test would slow the suite without
  paying for itself.

## [1.8.0] - 2026-04-30

### Added
- Optional `metrics` feature flag. With the feature enabled,
  every public method on `Lattice` records counters and
  histograms through the [`metrics`] crate facade. Seven
  counters (`lattice_puts_total`, `lattice_deletes_total`,
  `lattice_gets_total`, `lattice_get_hits_total`,
  `lattice_get_misses_total`, `lattice_flushes_total`,
  `lattice_compactions_total`, `lattice_transaction_commits_total`,
  `lattice_transaction_conflicts_total`) and six histograms
  (`*_duration_seconds` for put, delete, get, flush, compaction,
  transaction). The user installs their own recorder
  (`metrics-exporter-prometheus`, statsd, OpenTelemetry, ...);
  the engine just records, so no exporter dependency leaks
  into `lattice-core`.
- New private `metrics_compat` module. With the `metrics`
  feature on, it forwards to the facade macros. With the
  feature off, every recording function is a `pub(crate) const
  fn` with an empty body, so call sites do not need their own
  `cfg` guards and the optimiser deletes the calls entirely.
- Book chapter 14 ("Metrics") documents every metric, the
  feature flag, how to wire a recorder (with a Prometheus
  example), how to choose histogram buckets, and the cost
  model.

### Changed
- `flush` and `transaction` now record their wall time alongside
  the existing put / delete / get / compaction call sites. A
  no-op `flush` (memtable empty) returns early before
  recording, so `lattice_flushes_total` only counts flushes
  that produced an `SSTable`. A `transaction` aborted with
  `Error::TransactionConflict` increments
  `lattice_transaction_conflicts_total` instead of the commit
  counter.
- Chapter 13 ("Observability") replaces its closing "what is
  not yet shipped" section with a one-paragraph cross-reference
  to chapter 14, since metrics now ship.

### Notes
- Zero-cost when no recorder is installed at runtime: the
  `metrics` macros expand to a load of a global atomic, a null
  check, and an early return; the optimiser folds the rest.
- Histograms use `Duration::as_secs_f64`. Bucket choice is the
  exporter's job, not Lattice's; the chapter suggests a wide
  range covering tens of microseconds (durable put on SSD) to
  several seconds (cascade compaction).
- Build the docs locally with `cargo doc --features
  tokio,metrics --no-deps` if you want both the async wrapper
  and the metric helpers in the same rendered page.

[`metrics`]: https://docs.rs/metrics

## [1.7.0] - 2026-04-29

### Added
- `tracing` spans on every public method via
  `#[tracing::instrument]`. Spans carry useful fields (key
  length, value length, durability flag for writes; prefix
  length for scans; full path for `open`) and skip the engine
  itself so subscribers do not see a `Lattice` debug dump on
  every call. Levels are tuned to the cost of the call: trace
  for `get`, debug for `put` / `delete` / `flush_wal` / `scan` /
  `snapshot`, info for `open` / `flush` / `compact` /
  `transaction`. The pre-existing `info!` and `warn!` events
  inside those methods now nest under the corresponding span,
  so a downstream collector (jaeger, tempo, otel-collector)
  sees a clean parent-child shape without further work.
- Book chapter 13 ("Observability") explains the span layout,
  the levels, and the recommended `RUST_LOG` filters.

### Notes
- No behavioural changes. Spans are zero-cost when no
  subscriber is installed, and inexpensive otherwise.
- Prometheus metrics opt-in (the `metrics` facade behind a
  feature flag) is tracked as v1.8 work; spans alone are
  enough to drive distributed-tracing systems and that is the
  bigger immediate win.

## [1.6.0] - 2026-04-29

Closes both deferrals from v1.4 (transaction conflict detection)
and v1.5 (async transaction wrapper).

### Added
- Transaction conflict detection. The engine now stamps every put
  and delete with a monotonic `write_seq` and tracks the last
  `write_seq` at which each key was modified in `last_writes`.
  `Transaction` records `snapshot_seq` at start and tracks every
  key it reads in a `read_set`. On commit, the engine takes the
  WAL mutex once and atomically (a) checks every key in the
  read-set or write-set against `last_writes` and (b) applies the
  staged writes through a new `apply_entry_locked` helper. If any
  key was modified after the transaction's snapshot, the commit
  aborts with `Error::TransactionConflict`. The held WAL mutex
  prevents any concurrent put or delete from racing the check
  against the apply, which was the lock-discipline gap the v1.4
  CHANGELOG called out.
- New `Error::TransactionConflict` variant.
- `AsyncLattice::transaction(|tx| { ... })`: closure-shaped
  transaction running on tokio's blocking pool. The closure body
  is synchronous (no `await` inside the transaction); read-then-
  await-then-retry is the documented pattern for workflows that
  need to await between reads and writes.
- New `concurrent_transactions_on_same_key_second_aborts_with_
  conflict` test uses a `std::sync::Barrier` to deterministically
  interleave a read inside one transaction with an outside write,
  then asserts the commit aborts with `Error::TransactionConflict`
  and the outside write remains live.
- New `async_transaction_commits_atomically` test pins the async
  wrapper's commit semantics.

### Changed
- `Transaction::get` records the looked-up key in the read-set.
- `Transaction::new` now takes a `snapshot_seq` and initialises a
  `read_set: BTreeSet<Vec<u8>>`. The struct is `pub(crate)`
  except for the public methods, so this is not a public API
  change.
- The internal `append_entry` helper has been replaced by
  `apply_entry_locked`, which assumes the caller holds the WAL
  mutex. `put_with`, `delete`, and the transaction commit all
  funnel through it. `apply_entry_locked` returns a `needs_flush`
  flag so the caller can release the WAL mutex before invoking
  `flush()` (which re-acquires the WAL mutex internally).
- `transaction_isolated_read_view` test now rolls back explicitly
  via `Err`, because under v1.6's conflict detection, attempting
  to commit after the closure has read a key and an outside
  writer has modified that key would (correctly) abort. The
  rollback isolates the test to what it was meant to assert
  (snapshot-isolated reads), with the conflict-detection
  behaviour pinned by the new dedicated test.

### Notes
- 73 tests pass workspace-wide with `--features tokio` (60 baseline
  + 1 new tx + 4 + 1 new async tx + the existing M3 tests). fmt
  clean, clippy strict clean both with and without the `tokio`
  feature, doc with `-D warnings` clean.
- The on-disk format is unchanged. v1.5 directories open under
  v1.6 unchanged, and vice versa.

## [1.5.1] - 2026-04-29

### Added
- CI matrix gains an `msrv` job that runs `cargo check
  --workspace --all-targets --all-features` on the rust 1.85
  toolchain, catching accidental use of newer std items before
  a release.
- README gets badges for CI status, crates.io version, docs.rs
  link, license, and MSRV. The crates.io and docs.rs badges
  start serving content the moment the workflow publishes the
  crate (this tag is the trigger).
- `CONTRIBUTING.md` documents the CI bar (fmt + clippy + test +
  doc + MSRV check), the TDD discipline applied across M1 to
  M5, the conventional-commits convention, and the dual-license
  contribution clause.
- README install snippet now includes the `--features tokio`
  variant for the async wrapper from v1.5.0 and the `cargo
  install lattice-cli` form that becomes available once this
  tag publishes.

### Notes
- `cargo-semver-checks` will be added in a follow-up patch
  release: the tool needs a previously-published version to
  compare against, so it is most useful after the first publish
  this tag triggers.

## [1.5.0] - 2026-04-29

### Added
- Optional `tokio` feature on `lattice-core` exposes `AsyncLattice`,
  a thin wrapper around the synchronous engine that runs each
  operation on tokio's blocking pool via `tokio::task::spawn_blocking`.
  Methods: `open`, `put`, `get`, `delete`, `scan`, `flush`,
  `flush_wal`, `compact`, `sync`. Cloning is one `Arc` bump on the
  underlying `Lattice`.
- Four contract tests in `tests/async_api.rs`, gated by the `tokio`
  feature: round-trip put/get, delete, prefix scan, and concurrent
  puts from many tokio tasks.
- Book chapter 12 ("Async I/O") explains the wrapper, the trade-off
  vs native async, and when each is the right call.

### Notes
- v1.5 is **async-friendly**, not natively async: locks remain
  `parking_lot` and file I/O remains `std::fs`, with each call's
  blocking work pushed to tokio's blocking pool. Native async
  (replacing the locks and the I/O with their tokio equivalents)
  is a v2.x rewrite, deferred so v1.x can ship the surface that
  unblocks the most embed cases first.
- The choice between async support and primary/replica WAL
  streaming for this milestone went to async because the
  ecosystem is overwhelmingly tokio-first; replication is a
  larger project (Raft, leader election, exactly-once) that
  belongs in v2.x.
- The `tokio` feature is opt-in and adds no compile-time cost or
  runtime overhead when off; the synchronous API is unchanged.

## [1.4.0] - 2026-04-29

### Added
- `Lattice::transaction(|tx| { ... })`: closure-style transaction
  with snapshot-isolated reads, in-memory write staging, atomic
  commit on `Ok`, and automatic rollback on `Err` or panic.
- `Transaction::get`, `Transaction::put`, `Transaction::delete`:
  read-your-writes within the transaction (staged writes shadow
  the snapshot) and accumulate the staged set for commit.
- New test file `tests/transactions.rs` with seven contract tests
  written test-first: snapshot isolation across a concurrent
  outside write, read-your-writes inside the transaction, atomic
  commit visibility (also after reopen), rollback on closure
  returning `Err`, rollback on closure panic, read-only
  transactions, and concurrent transactions on disjoint keys.
- Book chapter 11 ("Transactions") explains the API, the
  isolation contract, the rollback semantics, and the deferred
  conflict-detection work.

### Notes
- v1.4 ships **without conflict detection**. Two transactions
  that touch the same key may both commit; the second writer
  wins (the lost-update problem). This is documented in the
  book chapter. Real conflict detection (per-key write-seq
  tracking compared to the transaction's snapshot-seq) lands in
  v1.5 once the lock-discipline change to keep the check and
  the apply atomic with respect to plain puts is properly
  designed.
- Transactions have no on-disk format implications. v1.3
  directories open under v1.4 unchanged, and vice versa.

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

[Unreleased]: https://github.com/NicolasDeNigris91/Lattice/compare/v1.7.0...HEAD
[1.7.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.7.0
[1.6.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.6.0
[1.5.1]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.5.1
[1.5.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.5.0
[1.4.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.4.0
[1.3.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.3.0
[1.2.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.2.0
[1.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.1.0
[1.0.1]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.0.1
[1.0.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v1.0.0
[0.4.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.4.0
[0.3.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.3.0
[0.2.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.2.0
[0.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.1.0
