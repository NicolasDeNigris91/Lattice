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

## License

By contributing you agree that your contribution is licensed
under the same dual MIT OR Apache-2.0 terms as the rest of the
project (see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE)).
