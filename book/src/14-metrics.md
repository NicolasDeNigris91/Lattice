# Metrics

v1.8 records counters and histograms through the [`metrics`]
crate facade behind the `metrics` feature flag. Tracing spans
(chapter 13) drive distributed-tracing systems; metrics drive
operational dashboards. They complement each other.

The engine never installs a recorder. The user wires their own
exporter (`metrics-exporter-prometheus`, `metrics-exporter-statsd`,
OpenTelemetry, ...) at process startup, and every recorded value
flows through it. With no recorder installed, the macros are
zero-cost.

## What is recorded

Every counter increments once per call. Every histogram records
elapsed wall time, in seconds, from the start of the public
method to the point where the call is about to return `Ok`. Failed
calls (returning `Err`) do not increment the counter.

| metric                                  | kind      | when                                                                      |
|-----------------------------------------|-----------|---------------------------------------------------------------------------|
| `lattice_puts_total`                    | counter   | every successful `put` / `put_with`                                       |
| `lattice_put_duration_seconds`          | histogram | per successful put                                                        |
| `lattice_deletes_total`                 | counter   | every successful `delete`                                                 |
| `lattice_delete_duration_seconds`       | histogram | per successful delete                                                     |
| `lattice_gets_total`                    | counter   | every successful `get`                                                    |
| `lattice_get_hits_total`                | counter   | when `get` returns `Some`                                                 |
| `lattice_get_misses_total`              | counter   | when `get` returns `None`                                                 |
| `lattice_get_duration_seconds`          | histogram | per successful get                                                        |
| `lattice_flushes_total`                 | counter   | per memtable flush that produced an `SSTable`                             |
| `lattice_flush_duration_seconds`        | histogram | flush wall time, including the SSTable build and the WAL truncate        |
| `lattice_compactions_total`             | counter   | per cascade level merged (one round, one increment)                      |
| `lattice_compaction_duration_seconds`   | histogram | wall time for that round                                                  |
| `lattice_transaction_commits_total`     | counter   | per `transaction` closure that returns `Ok` and clears the conflict check |
| `lattice_transaction_duration_seconds`  | histogram | full wall time, snapshot to last apply                                    |
| `lattice_transaction_conflicts_total`   | counter   | per `transaction` aborted with `Error::TransactionConflict`               |

A no-op `flush` (memtable empty) returns early before recording,
so `lattice_flushes_total` only counts flushes that produced an
SSTable.

## Wiring a recorder

The crate publishes the facade only. Pick an exporter that
matches your stack. For Prometheus:

```toml
[dependencies]
lattice-core = { version = "1.8", features = ["metrics"] }
metrics-exporter-prometheus = "0.15"
```

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9000))
    .install()?;

let db = lattice_core::Lattice::open("./data")?;
db.put(b"k", b"v")?;
// `curl localhost:9000/metrics` now shows lattice_puts_total{} 1
```

For a non-HTTP collector (statsd, OTel, journald), swap the
builder. The engine does not care which recorder is installed,
only that one exists.

## Choosing buckets

Histograms in the `metrics` crate are recorder-driven. Bucket
choice belongs to the exporter, not to Lattice. A reasonable
starting point for the Prometheus exporter:

```rust
PrometheusBuilder::new()
    .set_buckets_for_metric(
        Matcher::Suffix("_duration_seconds".into()),
        &[0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    )?
    .install()?;
```

The wide range is intentional. A durable `put` on an SSD finishes
in tens of microseconds; a `compact` cascading three levels can
take seconds. One bucket layout has to cover both.

## Cost

With no recorder installed, the metrics layer is zero-cost. The
`metrics` macros expand to a load of a global atomic, a null
check, and an early return. The optimiser inlines and folds the
whole thing. With a recorder installed, the cost is whatever the
recorder pays per record, dominated by formatting and the
exporter's I/O.

The histograms record `Duration::as_secs_f64`, so call sites pay
one `Instant::now` at entry and one at exit. The `get` hot path
already paid that cost in v1.7 for the tracing span; v1.8
reuses the same timestamp.

## What is not yet shipped

WAL append latency, SSTable build size, and bloom filter false
positive rate are not recorded yet. They are noisy and the
useful aggregations depend on the deployment, so a single set of
buckets does not fit. They are tracked as future work; the
existing seven counters and six histograms are the minimum set
that a dashboard needs to spot a flush stall, a compaction
cascade, or a transaction-conflict storm.

[`metrics`]: https://docs.rs/metrics

## Operational snapshot via `stats()`

The `metrics` facade is the right hook for a Prometheus-style
exporter. For dashboards, debugging sessions, and tests that
need to assert on engine state without spinning up a recorder,
v1.15 adds [`Lattice::stats`], which returns an owned `Stats`
value with the operational counters that do not need
aggregation:

- `memtable_bytes` and `frozen_memtable_bytes` for
  flush-trigger introspection.
- `level_sstables: Vec<usize>` for the per-level layout.
- `next_seq` for the sequence counter the next flush or
  compaction will use.
- `pending_writes` for the buffered non-durable WAL records.

`Stats::total_sstables` and `Stats::level_count` are convenience
helpers on top. The whole call is one `RwLock` read and a
handful of atomic loads; safe to poll on a sub-second tick
from a metrics exporter that does not want a `metrics`
recorder dependency.

`stats()` and the `metrics` facade are independent. A
deployment can use both: the exporter wires the recorder for
counters and histograms; the same process polls `stats()`
periodically to snapshot the gauge-like state the recorder
does not see. See `tests/stats.rs` for the contract.
