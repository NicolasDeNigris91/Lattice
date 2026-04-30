<!--
Thanks for the contribution. Please tick the boxes that apply
and remove any sections that do not. The CI will run the same
bar listed below; this checklist is a fast pre-flight.
-->

## What

<!-- One paragraph: the change in plain language. -->

## Why

<!--
The motivation. A bug? A user-visible improvement? A refactor
that pays for itself? Linking an issue is great but the PR
should still stand on its own.
-->

## How

<!--
The shape of the change. The diff explains the what; this section
explains the design choice and any trade-offs you considered.
-->

## Tests

<!--
- New behaviour MUST come with a test.
- A bug fix MUST come with a regression test that fails on `main`.
- If a test was hard to write, say why; that is useful information.
-->

## Pre-flight checklist

- [ ] `cargo fmt --all -- --check` clean
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean
- [ ] `cargo test --workspace --all-features --no-fail-fast` green
- [ ] `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --all-features` green
- [ ] `cargo deny check --all-features --workspace` clean (only if you touched dependencies)
- [ ] CHANGELOG entry under `[Unreleased]` (skip for pure refactors and CI-only changes)
- [ ] Book updated if behaviour changed
- [ ] Conventional Commits in commit messages
- [ ] No co-author trailers from automation
