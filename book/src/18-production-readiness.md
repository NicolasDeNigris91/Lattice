# Production readiness

This chapter is the project's honest self-assessment. Lattice
is built to a high quality bar inside the scope it has
committed to, and explicitly out of scope on a long list of
things production systems usually need. The aim is to give a
prospective user enough information to decide, before they
embed the crate, whether the engine fits their constraints.

The bar for "production ready" varies by industry. The
checklist below uses a common-sense superset: what most teams
operating a stateful service expect from the storage layer.
Each row is marked **shipped**, **deferred**, or **out of
scope**.

## Correctness

| Item | Status | Notes |
|---|---|---|
| Durability semantics documented | shipped | Chapter 1 (WAL); `WriteOptions { durable }` is the public surface. |
| Crash recovery from clean shutdown | shipped | Reopen replays the WAL into the memtable. |
| Crash recovery from mid-write power loss | shipped | The WAL is fsync'd before the put returns when `durable: true`; non-durable writes fall back to whatever the host OS flushed. |
| Crash recovery from mid-flush | shipped | Atomic rename of `*.sst.tmp` to `*.sst`; orphan cleanup on reopen. |
| Crash recovery from mid-compaction | shipped | Same atomic rename + orphan cleanup; compaction is restart-safe. |
| Snapshot isolation | shipped | Chapter 6 (snapshots), chapter 11 (transactions). |
| Per-key conflict detection | shipped | v1.6, loom-tested in v1.11. |
| Property-based testing | shipped | Four-pillar fence in `tests/property_durability.rs`: replay-on-reopen, snapshot isolation, compaction equivalence, transactional rollback, plus a fifth (`scan_iter` equivalence). |
| Fuzz coverage of on-disk decoders | shipped | `crates/lattice-core/fuzz/` has targets for the WAL, SSTable, and manifest parsers. |
| Loom model checking | shipped | `lattice-loom-tests` covers the conflict tracker and the compactor state machine under every legal interleaving. |
| Mutation testing | shipped (informational) | Weekly `cargo-mutants` sweep; not gated. |
| Miri | shipped (informational) | Per-PR job; not gated. |

## Operability

| Item | Status | Notes |
|---|---|---|
| Tracing instrumentation | shipped | `#[tracing::instrument]` on every public method (chapter 13). |
| Metrics facade | shipped | Opt-in `metrics` feature, chapter 14. Wires to Prometheus, statsd, OTel, etc. via the host process's recorder. |
| Inventory and fingerprint | shipped (v1.18) | `Lattice::byte_size_on_disk()` for capacity dashboards, `Lattice::checksum()` for cross-host divergence detection. CLI `lattice disk-size` / `lattice checksum` (v1.23) expose the same surface. |
| Online backup | shipped (v1.21) | `Lattice::backup_to(dest)` produces a self-contained directory openable by `Lattice::open`. Captures live SSTables and replays the in-memory memtables into a fresh WAL. CLI `lattice backup-to <dest>` exposes the same primitive. |
| Read-only handles | shipped (v1.25) | `LatticeBuilder::read_only(true)` and `Lattice::open_read_only`; mutations error with `Error::ReadOnly`, the flusher and compactor threads are not spawned. CLI `--read-only` flag mirrors it. |
| Online replication | out of scope (v2.x) | See chapter 8 ("Replication"). |
| Hot upgrade path | shipped | On-disk format version bumps include a forward-compat reopen path. |
| Pluggable storage backends | out of scope | Lattice owns the storage path end-to-end. |

## Security

| Item | Status | Notes |
|---|---|---|
| `forbid(unsafe_code)` on the engine | shipped | Audited by clippy strict on every PR. |
| Decoder hardening | shipped | Three cargo-fuzz targets exercise the WAL, SSTable, and manifest parsers against malformed input. |
| Book-deploy headers | shipped (v1.26) | The static book deploy (`deploy/Caddyfile`) carries `X-Frame-Options DENY`, a same-origin Content-Security-Policy with `'unsafe-inline'` for the mdBook theme, `Cross-Origin-Opener-Policy same-origin`, and `Cross-Origin-Resource-Policy same-origin` on top of the existing HSTS, Referrer-Policy, X-Content-Type-Options, and Permissions-Policy headers. |
| Container health probe | shipped (v1.26) | `deploy/Dockerfile` HEALTHCHECK polls the served root every 30 seconds via `wget --spider`; an orchestrator that drops the process can spot a wedge and recycle without the application having to expose a separate `/health` endpoint. |
| Encryption at rest | out of scope (v2.x) | Design sketch in chapter 15. |
| Encryption in transit | not applicable | Lattice does not open sockets. |
| Per-tenant isolation | not applicable | Single-process embedded library. |
| Vulnerability disclosure policy | shipped | `SECURITY.md` at the repo root with GitHub Security Advisories + email channel. |

## Performance

| Item | Status | Notes |
|---|---|---|
| Bounded write tail latency | shipped | v1.13 background compactor decouples compaction from the writer thread when `compact_async` is used. |
| Bounded scan memory | shipped | v1.12 `scan_iter` holds only the merge frontier (`O(num_sources)`) plus one decoded block per `SSTable`. |
| Concurrent reads | shipped | v1.2 `Arc<Inner>` plus `parking_lot::RwLock`s; many readers, one writer. |
| Concurrent writes | deferred | Writers serialise on the WAL mutex. SkipMap memtable plus WAL parallelism is a v2.x multi-release sequence (chapter 15). |
| Continuous regression detection | shipped (gated) | bencher.dev workflow gated on `BENCHER_API_TOKEN`; Welch's t-test against rolling baseline. |
| Comparative benchmark vs alternatives | deferred | Chapter 8's "A real benchmark suite" deferral. |

## Build & supply chain

| Item | Status | Notes |
|---|---|---|
| Multi-OS CI | shipped | Linux, macOS, Windows in the test matrix. |
| MSRV gate | shipped | Rust 1.85, checked per-PR. |
| Dependency audit | shipped | `cargo audit` and `cargo deny check` per-PR. |
| Licence allow list | shipped | `deny.toml` enforces the allow list. |
| Public API diff per PR | shipped | `cargo public-api` workflow uploads the diff as an artifact. |
| Reproducible builds | partial | `Cargo.lock` is committed; binary artefacts are not yet bit-reproducible. |
| Signed releases | deferred | GitHub release artefacts are SHA-256-checksummed; signing is a v2.x improvement. |

## Documentation

| Item | Status | Notes |
|---|---|---|
| Crate-level docs.rs landing page | shipped | Architecture, quick start, public surface, features. |
| Companion book covering every component | shipped | 18 chapters at <https://lattice.nicolaspilegidenigris.dev>. |
| Runnable examples | shipped | Five focused programs under `crates/lattice-core/examples/`. |
| Migration guides between minor releases | partial | `CHANGELOG.md` documents every behaviour change; no separate migration document yet. |
| Architectural decision records | shipped (book) | Chapter 15 design notes for v2.x; chapter 8 for closed/open deferrals. |

## Maturity & governance

| Item | Status | Notes |
|---|---|---|
| Production deployments | none | Lattice is a portfolio piece with zero known production users at v1.13. |
| Active maintainer | yes (single) | Nicolas Pilegi De Nigris is the sole author; response times are best-effort. |
| Issue and PR templates | shipped | `.github/ISSUE_TEMPLATE/` plus `PULL_REQUEST_TEMPLATE.md`. |
| Code of conduct | shipped | Contributor Covenant 2.1. |
| Public roadmap | shipped | Chapter 8 (open deferrals) and chapter 15 (v2.x design notes). |
| Semantic versioning discipline | shipped | Every minor release is documented in CHANGELOG with a behaviour rationale; the public API is diffed by CI on every PR. |

## Summary

Lattice is **production-ready for the workload it advertises**:
a single-process, single-host embedded LSM key-value store with
snapshot-isolated transactions, durable writes, and bounded
memory under scan. It is **not production-ready** for use cases
that need replication, encryption at rest, multi-host scale, or
the operational maturity that comes from hundreds of
production-years per release.

The honest recommendation is in chapter 17: pick Lattice when
the engine being readable and `unsafe`-free matters more than
the features it has not shipped yet. Pick something else when
the trade is the other way.
