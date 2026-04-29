//! Lattice, an LSM-tree key-value storage engine.
//!
//! This crate exposes a small embeddable key-value store backed by a write
//! ahead log, an in-memory memtable, sorted string tables, bloom filters,
//! tiered compaction, and snapshots.
//!
//! See the companion book at <https://lattice.nicolaspilegidenigris.dev>
//! for a chapter-by-chapter explanation of every component.

#![forbid(unsafe_code)]
