# Lattice

[![ci](https://github.com/NicolasDeNigris91/Lattice/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/NicolasDeNigris91/Lattice/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/lattice-core.svg)](https://crates.io/crates/lattice-core)
[![docs.rs](https://docs.rs/lattice-core/badge.svg)](https://docs.rs/lattice-core)
[![license](https://img.shields.io/crates/l/lattice-core.svg)](#license)
[![msrv](https://img.shields.io/badge/msrv-1.85-blue.svg)](#)

An LSM-tree key-value storage engine, written from scratch in Rust.

Lattice is built for learning and portfolio purposes. It implements
the fundamental components of modern log-structured merge-tree
databases (write ahead log, memtable, sorted string tables, bloom
filters, tiered compaction, manifest, snapshots) without depending on
any existing storage library. The companion book documents every
design decision in plain language.

* Crate: [`lattice-core`](crates/lattice-core)
* CLI: [`lattice-cli`](crates/lattice-cli)
* Book: https://lattice.nicolaspilegidenigris.dev

## Status

v1.9.0. Five didactic phases plus durability hardening (v1.0.1),
opt-in group commit (v1.1.0), `Send + Sync + Clone` with parallel
reads (v1.2.0), size-tiered leveled compaction with manifest v2
(v1.3.0), snapshot-isolated transactions (v1.4.0), an optional
tokio wrapper (v1.5.0), transaction conflict detection plus an
async transaction companion (v1.6.0), structured tracing spans
on every public method (v1.7.0), opt-in metrics through the
`metrics` crate facade (v1.8.0), and `tracing-test` wired into
the integration tests (v1.9.0). Seventy three tests across
integration, property, contract, concurrency, transaction, and
async suites, full mdBook published alongside the code. See the
[CHANGELOG](CHANGELOG.md) for what each tag delivered.

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

## Roadmap

| Tag    | Milestone                                              |
|--------|--------------------------------------------------------|
| v0.1.0 | WAL plus MemTable, durable replay                      |
| v0.2.0 | SSTable flush, mixed read path                         |
| v0.3.0 | Bloom filters per SSTable                              |
| v0.4.0 | Tiered compaction with manifest                        |
| v1.0.0 | Snapshots, criterion benches, finished book            |
| v1.0.1 | Directory `fsync` on rename + Railway/crates.io deploy |
| v1.1.0 | `WriteOptions`, group commit, dual MIT/Apache-2.0      |
| v1.2.0 | `Send + Sync + Clone`, parallel reads, flusher thread  |
| v1.3.0 | Manifest v2, size-tiered leveled compaction             |
| v1.4.0 | Transactions: snapshot isolation, atomic commit         |
| v1.5.0 | `AsyncLattice` behind the `tokio` feature flag          |
| v1.5.1 | Publish-readiness polish: MSRV CI + badges + CONTRIB    |
| v1.6.0 | Transaction conflict detection + AsyncTransaction       |
| v1.7.0 | Structured `tracing` spans on every public method       |
| v1.8.0 | Opt-in metrics via the `metrics` crate facade           |
| v1.9.0 | `tracing-test` wired into integration tests             |

## Deployment

The book and the crate can both be published from this repository.
See [DEPLOY.md](DEPLOY.md) for the Railway and `crates.io` setup.

## Why another KV store

Lattice is not a serious alternative to [`sled`](https://github.com/spacejam/sled),
[`fjall`](https://github.com/fjall-rs/fjall), or RocksDB. Its purpose
is to reproduce the building blocks of those systems with code small
enough to read end to end, and prose explaining each decision.

## License

Dual-licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)

at your option. Contributions are accepted under the same dual
license unless otherwise stated.
