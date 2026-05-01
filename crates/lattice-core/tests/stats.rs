//! Contract tests for `Lattice::stats`.
//!
//! `stats` is the operational introspection hook added in v1.15.
//! It returns a [`Stats`] value snapshot sized for a metrics
//! scrape; the tests pin three properties:
//!
//! - The empty-database baseline: zero memtable, zero sstables.
//! - The accumulator: writes grow `memtable_bytes`; a flush
//!   moves the bytes into a level-0 sstable and resets
//!   `memtable_bytes`.
//! - The full-flow invariant: after a forced compact, every
//!   non-empty level holds at most one sstable and the
//!   `total_sstables` helper agrees with the sum.

use lattice_core::{Lattice, Stats};
use tempfile::tempdir;

#[test]
fn stats_baseline_is_empty() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    let stats = db.stats();
    assert_eq!(stats.memtable_bytes, 0);
    assert_eq!(stats.frozen_memtable_bytes, 0);
    // `level_sstables` may be `[]` or `[0, 0, ...]` depending on
    // how many empty trailing levels the bootstrap left in the
    // manifest; the property that matters is that no level
    // holds an actual table.
    assert!(stats.level_sstables.iter().all(|&n| n == 0));
    assert_eq!(stats.total_sstables(), 0);
    assert_eq!(stats.level_count(), 0);
    assert_eq!(stats.pending_writes, 0);
}

#[test]
fn stats_track_memtable_growth_and_flush_to_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"k1", b"value-bytes").unwrap();
    db.put(b"k2", b"value-bytes").unwrap();
    db.put(b"k3", b"value-bytes").unwrap();

    let after_writes = db.stats();
    assert!(
        after_writes.memtable_bytes > 0,
        "memtable should accumulate bytes; got {after_writes:?}",
    );
    assert_eq!(after_writes.total_sstables(), 0);

    db.flush().unwrap();

    let after_flush = db.stats();
    assert_eq!(after_flush.memtable_bytes, 0);
    assert_eq!(after_flush.frozen_memtable_bytes, 0);
    assert_eq!(after_flush.total_sstables(), 1);
    assert_eq!(after_flush.level_sstables, vec![1]);
    assert_eq!(after_flush.level_count(), 1);
}

#[test]
fn stats_after_compact_show_at_most_one_sstable_per_level() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    for batch in 0..6u32 {
        for i in 0..50u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    db.compact().unwrap();
    let stats = db.stats();

    for &count in &stats.level_sstables {
        assert!(
            count <= 1,
            "after full compact every level holds at most 1 sstable; got {stats:?}",
        );
    }
    assert!(stats.total_sstables() >= 1);
}

#[test]
fn stats_is_a_value_snapshot_not_a_live_view() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let snapshot = db.stats();
    let memtable_bytes_at_snapshot = snapshot.memtable_bytes;

    // Drain the memtable; the snapshot's value must not change.
    db.flush().unwrap();

    assert_eq!(
        snapshot.memtable_bytes, memtable_bytes_at_snapshot,
        "Stats is owned: a flush after the snapshot must not mutate it",
    );
    assert_eq!(snapshot.total_sstables(), 0);

    // The next snapshot reflects the new state.
    let after = db.stats();
    assert_eq!(after.memtable_bytes, 0);
    assert_eq!(after.total_sstables(), 1);
}

#[test]
fn stats_implements_debug_and_clone_for_metrics_pipelines() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    let stats: Stats = db.stats();
    let cloned = stats.clone();
    let debug_repr = format!("{stats:?}");

    assert_eq!(stats, cloned);
    assert!(debug_repr.contains("Stats"));
}
