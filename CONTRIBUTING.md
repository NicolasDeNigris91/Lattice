# Contributing to Lattice

Lattice is a single-author project written for learning and as a
reference implementation of an LSM-tree storage engine. Issues and
well-scoped pull requests are welcome; please read this short
document first.

## Before opening an issue

- Check the [book](https://lattice.nicolaspilegidenigris.dev) to see
  whether the topic is already addressed. Several "deferred"
  trade-offs (strict leveled compaction, native async, MVCC,
  replication) are explicitly tracked there.
- Reproductions are gold. Include the smallest sequence of
  operations that triggers the issue, the platform you saw it on,
  and the rust toolchain version (`rustc --version`).

## Before opening a pull request

The project bar is the same one the CI enforces:

- `cargo fmt --all -- --check` clean.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  clean.
- `cargo test --workspace --all-features --no-fail-fast` green.
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --all-features`
  green.
- `cargo check --workspace --all-targets --all-features` clean on
  the MSRV toolchain (currently 1.85).
- `cargo deny check --all-features --workspace` clean (advisories,
  licences, banned crates, source registries). Required when a PR
  touches `Cargo.toml` or `Cargo.lock`. The configuration lives in
  `deny.toml` at the workspace root; if your dependency change adds
  a licence outside the existing allow list, the PR should also
  update the allow list with a one-line justification.
- `cargo deny check` and the line-coverage report run on every CI
  build. Coverage is tracked but not gated; the lcov artifact is
  published per run for review.

A failing CI job blocks the PR.

## Test discipline

This project follows test-driven development for new behaviour:
write the failing test first, watch it fail for the right reason,
implement, watch it pass, refactor with the test as the safety
net. The existing test files (especially `tests/group_commit.rs`,
`tests/concurrency.rs`, `tests/transactions.rs`) follow this
shape; a PR that adds new behaviour without a test is hard to
review.

The property test in `tests/property_durability.rs` runs sixty
four random operation sequences per `cargo test` and is the most
valuable safety net the project has. If the property test starts
failing on a PR, the PR is the cause unless proven otherwise.

## Style

- Conventional commits (`feat`, `fix`, `refactor`, `chore`,
  `docs`, `test`, `bench`).
- Commit messages explain the **why**. The diff explains the
  **what**.
- No co-author trailers from automation; commits are authored by
  the contributor.
- Comments are reserved for non-obvious invariants. Self-evident
  code does not need a comment.

## Fuzzing

The `crates/lattice-core/fuzz/` directory holds three
`cargo-fuzz` targets that exercise the open-time decode paths
(WAL log, SSTable file, and manifest file) against arbitrary
bytes. Run them locally with a nightly toolchain:

```bash
rustup install nightly
cargo install cargo-fuzz
cd crates/lattice-core/fuzz
cargo +nightly fuzz run wal_open
cargo +nightly fuzz run sstable_open
cargo +nightly fuzz run manifest_open
```

CI runs each target for 30 seconds on every PR as an
informational job (`continue-on-error: true`); a longer
exhaustive sweep belongs to a separate schedule that
contributors run before a release.

The contract every fuzz target enforces is "no input causes a
panic". A malformed file must surface as `Err(Error::*)` from
`Lattice::open`, never as a process abort or out-of-bounds
access. New corpus inputs that catch a regression should be
checked in under `corpus/<target>/`.

## Code of conduct

Participation is governed by the
[Contributor Covenant](CODE_OF_CONDUCT.md). Reports go to
nicolas.denigris91@icloud.com.

## License

By contributing you agree that your contribution is licensed
under the same dual MIT OR Apache-2.0 terms as the rest of the
project (see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE)).
