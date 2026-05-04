# Lattice vs the alternatives

A short, honest comparison against the three other Rust LSM
projects most people considering Lattice will already know:
[`sled`][sled], [`fjall`][fjall], and [`rocksdb`][rocksdb-rs]
(the C++ binding). The point is to help you decide which one
fits your problem, not to convince you to pick Lattice.

[sled]: https://crates.io/crates/sled
[fjall]: https://crates.io/crates/fjall
[rocksdb-rs]: https://crates.io/crates/rocksdb

## At a glance

|                                 | Lattice              | sled                 | fjall                  | rocksdb                       |
|---------------------------------|----------------------|----------------------|------------------------|-------------------------------|
| Implementation language         | Rust                 | Rust                 | Rust                   | C++ (Rust binding)            |
| LSM design                      | Strict leveled       | Bw-tree-ish          | Tiered leveled         | Strict leveled                |
| `unsafe` in the engine          | Forbidden            | Used (lock-free)     | Used (mmap, etc)       | Entire engine is C++          |
| Transactions                    | Snapshot isolation   | Limited (CAS only)   | None at engine level   | Optimistic / pessimistic      |
| Streaming scan                  | Yes (`scan_iter`)    | Yes                  | Yes                    | Yes                           |
| Background compaction           | Yes (`compact_async`)| Yes                  | Yes                    | Yes                           |
| `tokio` async surface           | Wrapper feature      | None                 | None                   | Wrapper crates exist          |
| Loom-tested concurrency         | Yes (tracker, compactor) | No (uses unsafe)  | No                     | N/A (C++)                    |
| Encryption at rest              | No (v2.x)            | No                   | No                     | Yes                           |
| Replication                     | No (v2.x)            | No                   | No                     | Yes (BlobDB, log shipping)    |
| Production deployments          | None (portfolio)     | Limited, no support  | New, growing           | Massive (Meta, LinkedIn, ...) |
| Companion documentation         | mdBook (every component) | API docs only    | API docs + blog        | Wiki + papers                 |

The table is reductive on purpose. Each cell is one row of a
truth table the reader can use to filter. The remaining
sections expand the rows that matter.

## When to pick which

### Pick `rocksdb` if

You are running a database product, your scale is "many TB
per node", and you need someone else to have already taken the
production hits. RocksDB has been in production at Meta,
LinkedIn, ByteDance, Yugabyte, CockroachDB, TiDB, Pebble, and
hundreds of other systems. It supports encryption at rest,
column families, BlobDB for large values, log-shipping
replication, and a shelf full of pluggable compaction
strategies. The Rust binding is mature.

The trade-off is C++. You inherit the C++ build toolchain, the
C++ memory model, and the surface area of a 250-thousand-line
codebase. If your Rust shop has zero appetite for FFI, this is
where the conversation stops.

### Pick `fjall` if

You want a pure-Rust LSM, you want background compaction and
streaming scans out of the box, you want a maintainer who is
actively shipping, and you do NOT need transactions at the
engine level. Fjall is the most actively developed pure-Rust
LSM today. It uses `unsafe` for some of the lock-free paths
but is otherwise idiomatic and well-tested.

The trade-off is feature scope. Fjall does not (yet) ship
snapshot-isolated transactions or a loom-tested concurrency
story. If your application can do its own transactions on top
of CAS primitives, fjall is the right pick.

### Pick `sled` if

You want the simplest possible embed-and-go API, you do not
need transactions beyond CAS, and you are comfortable running
a project whose maintenance is sporadic. Sled was the
trendsetter for "Rust embedded KV" and pioneered a lot of
ideas the rest of this list copies; it is in maintenance mode
today and the README has a long-standing "do not use in
production" warning that the maintainers themselves wrote.

The trade-off is risk. Sled's on-disk format has changed
incompatibly across releases, and the project's stability
guarantees are weaker than the others.

### Pick Lattice if

You want a pure-Rust, `unsafe`-free LSM with snapshot-isolated
transactions, a loom-checked concurrency story, a streaming
scan iterator, non-blocking background compaction, and a book
that explains every design decision in long form. You are
running it for a small or medium workload (gigabytes, not
terabytes), you do not need encryption at rest or replication
yet, and you value being able to read the whole engine in an
afternoon.

The trade-off is production maturity. Lattice has zero known
production deployments, the author is the only maintainer, the
on-disk format has changed several times in the v1.x series
(though backwards-compatible reopens have always shipped), and
features the larger projects take for granted (column
families, log-shipping replication, encryption at rest) are
explicitly out of scope until v2.x. The companion book's
[chapter 18](18-production-readiness.md) lays this out in
detail.

## Honest performance note

Numbers here would be misleading. The fair comparison is YCSB
or db_bench against the same hardware, the same fsync policy,
and the same value sizes; that is its own project (see
chapter 8's "A real benchmark suite" deferral). Until that
exists, no apples-to-apples claim is appropriate.

Lattice's micro-benchmarks (chapter 7) are calibrated against
the engine's own previous versions via [bencher.dev][bencher],
so a regression on Lattice's own hot paths is caught
automatically. They do NOT establish position relative to the
alternatives.

[bencher]: https://bencher.dev

## What this comparison is not

It is not an attempt to win. RocksDB is the right answer for
most production systems; fjall is the right answer for most
production-pure-Rust systems; sled was historically the right
answer for "I want to write Rust and not think about it";
Lattice is the right answer when the goal is to *read and
understand* the engine, plus a feature set that solves the
problem at hand.
