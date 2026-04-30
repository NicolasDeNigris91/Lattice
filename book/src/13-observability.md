# Observability

v1.7 puts `tracing` spans on every public method on `Lattice`.
The `info!` and `warn!` events that already lived inside the
engine now nest under the corresponding span, so downstream
collectors (jaeger, tempo, otel-collector) see a clean
parent-child shape without further wiring.

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

## Metrics

Counters and histograms via the `metrics` crate facade ship in
v1.8 behind the `metrics` feature flag. Spans drive
distributed-tracing systems; metrics drive operational
dashboards. See chapter 14.
