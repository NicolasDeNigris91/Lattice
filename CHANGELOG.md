# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-04-29

### Added
- Workspace skeleton, CI on push and pull request, mdBook scaffolding.
- Project conventions documented in `CLAUDE.md` (untracked) and `README.md`.
- Append-only WAL with CRC32 record integrity, `fsync`-per-write
  durability, and torn-write tolerant replay.
- In-memory ordered memtable backed by `BTreeMap`, with tombstones for
  deletes.
- Public `Lattice::{open, put, get, delete, scan}` API on top of WAL plus
  memtable.
- `lattice` CLI with `put`, `get`, `delete`, `scan`, and `compact`
  (compaction surfaces a placeholder error until Phase 4).
- Property-based test suite covering arbitrary `put` and `delete`
  sequences against a `BTreeMap` reference, with a reopen step on every
  case to exercise replay.
- Book chapters 1 (the write ahead log) and 2 (the memtable).

[Unreleased]: https://github.com/NicolasDeNigris91/Lattice/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.1.0
