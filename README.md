# Lattice

[![ci](https://github.com/NicolasDeNigris91/Lattice/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/NicolasDeNigris91/Lattice/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/lattice-core.svg)](https://crates.io/crates/lattice-core)
[![docs.rs](https://docs.rs/lattice-core/badge.svg)](https://docs.rs/lattice-core)
[![license](https://img.shields.io/crates/l/lattice-core.svg)](#license)
[![msrv](https://img.shields.io/badge/msrv-1.85-blue.svg)](#)

An LSM-tree key-value storage engine, written from scratch in Rust.

Lattice is built for learning and portfolio purposes. It
implements the fundamental components of modern log-structured
merge-tree databases without depending on any existing storage
library, and the [companion book](https://lattice.nicolaspilegidenigris.dev)
documents every design decision in plain language. Eleven
public releases on [`main`](https://github.com/NicolasDeNigris91/Lattice/commits/main),
strict CI on Linux, macOS, and Windows, and an mdBook that
walks the storage path end to end.

* Crate: [`lattice-core`](crates/lattice-core)
* CLI: [`lattice-cli`](crates/lattice-cli)
* Book: <https://lattice.nicolaspilegidenigris.dev>
* Roadmap: see the [CHANGELOG](CHANGELOG.md) and the
  [book's "what is not yet" chapter](https://lattice.nicolaspilegidenigris.dev/08-not-yet.html).

## Features

- **Durable by default.** Every `put` and `delete` syncs the
  WAL before returning. Group commit is opt-in via
  `WriteOptions { durable: false }` plus a per-database batch
  size or commit window.
- **Snapshot-isolated transactions** (v1.4) with **conflict
  detection** (v1.6). The closure-shaped API
  (`db.transaction(|tx| { ... })`) is shared between the sync
  engine and the async wrapper.
- **Async wrapper** (v1.5, behind the `tokio` feature). Runs
  the synchronous engine on tokio's blocking pool. Honest about
  being async-friendly rather than natively async.
- **Observability without lock-in** (v1.7 - v1.9). Tracing
  spans on every public method, opt-in counters and histograms
  via the `metrics` facade behind the `metrics` feature flag,
  and `tracing-test` wired into the integration suite so
  `RUST_LOG=lattice_core=debug cargo test` produces useful
  output for failing tests.
- **Bounded memory** (v1.10). The transaction-conflict map
  trims itself based on the smallest in-flight transaction's
  snapshot, so a long-running process is not a leak.
- **Cross-platform.** CI runs the test suite on Linux, macOS,
  and Windows on every push.
- **No `unsafe` code.** `#![forbid(unsafe_code)]` at the crate
  root.
- **Strict CI bar.** `fmt`, `clippy --all-targets --all-features
  -- -D warnings`, the test suite under `--all-features`,
  rustdoc with `RUSTDOCFLAGS=-D warnings`, MSRV check on Rust
  1.85, `cargo audit`, `cargo deny check`, an mdBook build,
  and a line-coverage report.

## At a glance

Synchronous use:

```rust
use lattice_core::{Lattice, WriteOptions};

let db = Lattice::open("./data")?;
db.put(b"hello", b"world")?;
assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));

// Opt out of fsync per write; the engine batches the syncs.
db.put_with(b"fast", b"path", WriteOptions { durable: false })?;
db.flush_wal()?; // ensure durability before drop
```

Snapshot-isolated transaction with automatic conflict detection:

```rust
use lattice_core::{Error, Lattice};

let db = Lattice::open("./data")?;

db.transaction(|tx| {
    if tx.get(b"balance:alice")?.is_none() {
        tx.put(b"balance:alice", b"100");
    }
    tx.put(b"balance:bob", b"50");
    Ok::<_, Error>(())
})?;
```

Async wrapper (with `--features tokio`):

```rust
use lattice_core::AsyncLattice;

let db = AsyncLattice::open("./data").await?;
db.put(b"k", b"v").await?;
let v = db.get(b"k").await?;
assert_eq!(v, Some(b"v".to_vec()));
```

## Quickstart

```bash
# Library (sync)
cargo add lattice-core

# Library (async wrapper via tokio's blocking pool)
cargo add lattice-core --features tokio

# Library (with Prometheus / statsd / OTel counters and histograms)
cargo add lattice-core --features metrics

# CLI
cargo install lattice-cli           # once published
cargo install --path crates/lattice-cli  # from a checkout

lattice put hello world
lattice get hello
lattice scan --prefix h
lattice compact
```

## Where Lattice fits

Lattice is not a drop-in replacement for production storage
engines. It is a teaching reference and a portfolio piece. If
you need an embedded key-value store for real workloads,
[`sled`](https://github.com/spacejam/sled) and
[`fjall`](https://github.com/fjall-rs/fjall) are battle-tested;
RocksDB is the industry default for serious capacity. The bar
Lattice is held to is "small enough to read end to end" plus
"strict enough that the things it claims to do are pinned by
tests on every push".

## Roadmap

<details>
<summary>Twelve public releases. Click for the table.</summary>

| Tag     | Milestone                                              |
|---------|--------------------------------------------------------|
| v0.1.0  | WAL plus MemTable, durable replay                      |
| v0.2.0  | SSTable flush, mixed read path                         |
| v0.3.0  | Bloom filters per SSTable                              |
| v0.4.0  | Tiered compaction with manifest                        |
| v1.0.0  | Snapshots, criterion benches, finished book            |
| v1.0.1  | Directory `fsync` on rename + Railway/crates.io deploy |
| v1.1.0  | `WriteOptions`, group commit, dual MIT/Apache-2.0      |
| v1.2.0  | `Send + Sync + Clone`, parallel reads, flusher thread  |
| v1.3.0  | Manifest v2, size-tiered leveled compaction            |
| v1.4.0  | Transactions: snapshot isolation, atomic commit        |
| v1.5.0  | `AsyncLattice` behind the `tokio` feature flag         |
| v1.5.1  | Publish-readiness polish: MSRV CI + badges + CONTRIB   |
| v1.6.0  | Transaction conflict detection + AsyncTransaction      |
| v1.7.0  | Structured `tracing` spans on every public method      |
| v1.8.0  | Opt-in metrics via the `metrics` crate facade          |
| v1.9.0  | `tracing-test` wired into integration tests            |
| v1.10.0 | Bounded `last_writes` (closes v1.6 memory leak)        |

</details>

The book's [chapter on what is not yet
implemented](https://lattice.nicolaspilegidenigris.dev/08-not-yet.html)
is the source of truth for which trade-offs are intentional and
which are tracked as future work.

## Deployment

The book and the crate can both be published from this
repository. See [DEPLOY.md](DEPLOY.md) for the Railway and
`crates.io` setup.

## Contributing

Bug reports and well-scoped pull requests are welcome. Please
read [CONTRIBUTING.md](CONTRIBUTING.md) for the project bar
that the CI enforces, and the [Code of
Conduct](CODE_OF_CONDUCT.md) for community standards. Security
issues go through the
[security advisory form](https://github.com/NicolasDeNigris91/Lattice/security/advisories/new),
not public issues.

## License

Dual-licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or <https://www.apache.org/licenses/LICENSE-2.0>)

at your option. Contributions are accepted under the same dual
license unless otherwise stated.
