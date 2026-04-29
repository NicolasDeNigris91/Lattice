# Lattice

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

v1.0.0. Five phases shipped, sixty plus tests across integration and
property suites, full mdBook published alongside the code. See the
[CHANGELOG](CHANGELOG.md) for what each tag delivered.

## Quickstart

```bash
# Library
cargo add lattice-core

# CLI
cargo install --path crates/lattice-cli

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
