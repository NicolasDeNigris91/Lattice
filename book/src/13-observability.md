# Observability

v1.7 puts `tracing` spans on every public method on `Lattice`.
The `info!` and `warn!` events that already lived inside the
engine now nest under the corresponding span, so downstream
collectors (jaeger, tempo, otel-collector) see a clean
parent-child shape without further wiring. v1.9 carries that
same plumbing into the integration tests via `tracing-test`,
so a failing test can be re-run with `RUST_LOG=lattice_core=debug`
and the engine's traces become visible without manual
subscriber wiring.

## Span layout

| method        | level  | fields                                    |
|---------------|--------|-------------------------------------------|
| `open`        | info   | `path = <database directory>`             |
| `put_with`    | debug  | `key_len`, `value_len`, `durable`         |
| `delete`      | debug  | `key_len`                                 |
| `get`         | trace  | `key_len`                                 |
| `scan`        | debug  | `prefix_len`                              |
| `flush`       | info   | (no extra fields)                         |
| `compact`     | info   | (no extra fields)                         |
| `flush_wal`   | debug  | (no extra fields)                         |
| `snapshot`    | debug  | (no extra fields)                         |
| `transaction` | info   | (no extra fields)                         |

The engine itself is never logged on a span (`skip(self)` or
`skip_all` everywhere), so a subscriber does not see a `Lattice`
debug dump on every call.

## Choosing a filter

Reasonable starting points for `RUST_LOG`:

```bash
# Just the milestones: open, flush, compact, transaction.
RUST_LOG=lattice_core=info

# Plus per-write context. Useful for understanding throughput
# or spotting hot keys.
RUST_LOG=lattice_core=debug

# Per-read context, too. Verbose; reach for it when chasing a
# specific bad read.
RUST_LOG=lattice_core=trace
```

The same filters work for `tokio::tracing_subscriber::fmt()`,
`tracing_journald`, `tracing_opentelemetry`, or any other
subscriber.

## Wiring a subscriber

The simplest path:

```rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();

let db = lattice_core::Lattice::open("./data")?;
db.put(b"k", b"v")?;
// `RUST_LOG=lattice_core=debug cargo run` now prints a
// "put_with" span around the WAL append and the memtable
// update.
```

For a distributed system, swap the `fmt` subscriber for
`tracing-opentelemetry` or `tracing-tracy`; the spans are
collector-agnostic and pre-tagged with the fields above.

## Cost

`tracing` spans are zero-cost when no subscriber is installed
(an inlined `if false` check, basically). When a subscriber is
installed, the cost is dominated by the subscriber's work, not
by the span machinery. The `get` span sits at the trace level
specifically so a default `info` subscriber does not pay the
formatting cost on the read hot path.

## Tracing inside tests

v1.9 wires [`tracing-test`] into the integration tests. A handful
of behaviour-pinning tests carry `#[traced_test]`, which installs
a per-test `tracing` subscriber for the duration of the
annotated test. The subscriber is scoped: concurrent tests do
not see each other's events.

```rust
use tracing_test::traced_test;

#[test]
#[traced_test]
fn transaction_commit_applies_all_writes_atomically() {
    // ... existing assertions ...
}
```

Run with `RUST_LOG=lattice_core=debug cargo test -- --nocapture`
to see the engine's events for those tests on stderr. Without
`RUST_LOG`, `tracing-test` defaults to capturing only events
from the test crate, so the engine's events are filtered out.
The annotation also exposes `logs_contain` and `logs_assert`
helpers, which let a test assert on the captured trace output
when a behaviour is invariant of the recording mechanism.

The annotated set is intentionally small. The contract is "this
test should produce useful trace output when you reach for it",
not "the trace output is part of the assertion surface". Adding
the macro to every test would slow the suite without paying for
itself.

## Metrics

Counters and histograms via the `metrics` crate facade ship in
v1.8 behind the `metrics` feature flag. Spans drive
distributed-tracing systems; metrics drive operational
dashboards. See chapter 14.

## Inventory and fingerprinting (v1.18)

Two read-only methods land in v1.18 alongside the existing
`stats()` snapshot, each answering an operational question
that `stats()` does not.

`Lattice::byte_size_on_disk() -> Result<u64>` returns the
total bytes the engine occupies on disk: the sum of every
live `SSTable` file size plus the current WAL length. Memtable
bytes are explicitly not counted (those are in
`Stats::memtable_bytes`). The number is a point-in-time
observation; a concurrent flush or compaction may move the
counter between the LSM-state snapshot and the per-file
`metadata` syscalls. It is the right hook for a capacity
dashboard or a "disk is filling up" alert.

`Lattice::checksum() -> Result<u64>` returns a deterministic
xxh3-64 fingerprint of the visible `(key, value)` set in
ascending key order. The hash is invariant under `flush()`
and `compact()` because neither changes the visible set; it
changes when a put, delete, or value mutation perturbs the
state. Two databases that converged to the same logical
state, regardless of operation history, agree on the hash.
The contract is the cross-host divergence-detection one:
replicas on the same logical state must agree on this
fingerprint, and a discrepancy points at a divergence that
the rest of the engine cannot have already reported.

The fingerprint is built from a stream of length-prefixed
key/value pairs (`len(key) || key || len(value) || value`,
lengths as little-endian `u64`). Length-prefixing is
load-bearing: without it the pair `("ab", "cd")` would hash
the same as `("a", "bcd")`. Cost is O(visible keys plus
visible bytes) plus the I/O to stream the on-disk merge: not
a hot path, but cheap enough to run as a smoke check after a
recovery or in a periodic divergence sweep.

The property fence
`checksum_is_invariant_under_flush_and_compact` (in
`tests/property_durability.rs`) drives 64 random op histories
and asserts the hash agrees before and after the layout
move. Pre-v1.18 this property had no API to verify; v1.18
both ships the API and pins it under the property suite.

## Backup (v1.21)

`Lattice::backup_to(dest) -> Result<()>` produces a
self-contained directory that `Lattice::open` can open and
observe the same logical state as the source database. The
result is the atomic unit a backup tool would archive (tar,
upload to object storage, ship to a replica) and a future
restore workflow consumes directly.

Algorithm:

1. Take the engine's mutation lock and the active memtable's
   read lock. Together they freeze the in-memory and
   on-disk components for the duration of the copy:
   concurrent flushes, compactions, puts, and deletes block
   until the backup completes.
2. Copy each live `SSTable` file to `dest`, preserving its
   sequence-number filename so the manifest can refer to it.
3. Write a manifest at `dest` matching the source's level
   layout (`Manifest::save` is atomic, temp + rename + fsync).
4. Replay the frozen and active memtables into a fresh
   `wal.log` at `dest`, frozen first then active so
   last-writer-wins on replay matches the source's view.
5. `fsync` the destination directory.

The cross-host divergence-detection contract from v1.18
applies to the backup as a "replica": `restored.checksum() ==
source.checksum()` is the load-bearing equivalence assertion,
covered by an integration test plus a property fence
(`backup_to_is_state_equivalent_under_random_history`,
64 cases per `cargo test`).

Trade-offs:

- The simple lock-based approach is correct but stalls
  writers for the duration of the copy. For small to
  medium databases this is acceptable; for multi-GB databases
  a snapshot-based hard-link backup that does not stall
  writers is the next optimisation, listed in chapter 8.
- The backup directory is independent of the source: a
  backup taken at time `T` is a frozen view at `T`,
  insulated from post-backup writes. An integration test
  pins this independence by mutating the source after the
  backup and verifying the restored database still reflects
  the `T` state.

The design choice for replaying memtables into the backup's
WAL (rather than forcing a flush before the backup) keeps the
operation purely read-only against the source: a backup never
writes to the source directory and never advances the
source's `next_seq`.

[`tracing-test`]: https://docs.rs/tracing-test
