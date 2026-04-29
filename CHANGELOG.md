# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-04-29

### Added
- Immutable on-disk sorted string tables (`SSTable`) with LZ4-compressed
  data blocks, sparse index, and 32-byte footer.
- `SSTableWriter` (streaming, key-ordered) and `SSTableReader` (footer
  parse, sparse index lookup, block-level scan).
- `Lattice::flush` API that drains the memtable into a new SSTable,
  renames atomically from `*.sst.tmp`, then truncates the WAL.
- Auto-flush at a configurable byte threshold (default 4 MiB).
- Three-state `Lookup` enum on the memtable, distinguishing tombstones
  from absence so the read path knows whether to consult older layers.
- Mixed-source read path: memtable first, then SSTables newest to
  oldest, returning on the first non-`Absent` answer.
- Newest-source-wins merge in `scan`, including across multiple
  SSTables.
- `discover_sstables` on `open`, sorting by sequence number derived from
  the filename.
- Property test extended with an `Op::Flush` variant so generated
  sequences exercise interleaved flushes plus reopen replay.
- Book chapter 3 (sorted string tables).

### Fixed
- WAL truncation now opens a separate write-mode file handle, working
  around Windows ACL behaviour where `FILE_APPEND_DATA` does not grant
  `FILE_WRITE_DATA`.

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

[Unreleased]: https://github.com/NicolasDeNigris91/Lattice/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.2.0
[0.1.0]: https://github.com/NicolasDeNigris91/Lattice/releases/tag/v0.1.0
