//! Contract tests for the v1.18 inventory surface:
//! [`Lattice::byte_size_on_disk`] and [`Lattice::checksum`].
//!
//! `byte_size_on_disk` is for capacity planning and dashboards;
//! it sums the live `SSTable` file sizes plus the current WAL
//! length. `checksum` is a deterministic fingerprint over the
//! visible `(key, value)` set in ascending key order; same
//! visible state produces the same hash regardless of the
//! internal LSM layout (so flush and compact do not change it),
//! and divergent state produces a different hash, which makes
//! it useful for cross-host replication divergence detection
//! and for test fences that want to assert "these two
//! sequences of operations leave the database in the same
//! observable state".

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn byte_size_on_disk_starts_small_and_grows_on_flush() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // Empty database, possibly an empty or very small WAL file.
    let baseline = db.byte_size_on_disk().unwrap();

    // Memtable writes do not touch the SSTable layer; they do
    // append to the WAL, so the on-disk total grows.
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        let value = vec![b'v'; 256];
        db.put(key.as_bytes(), &value).unwrap();
    }
    let after_writes = db.byte_size_on_disk().unwrap();
    assert!(
        after_writes > baseline,
        "WAL appends should grow on-disk size: {baseline} -> {after_writes}",
    );

    // Flush moves the memtable into a new SSTable on L0 and
    // truncates the WAL. Total on-disk bytes must reflect the
    // SSTable.
    db.flush().unwrap();
    let after_flush = db.byte_size_on_disk().unwrap();
    assert!(
        after_flush > 0,
        "post-flush on-disk total must be positive: got {after_flush}",
    );

    // A second batch + flush appends a second SSTable. Total
    // grows again.
    for i in 100..200u32 {
        let key = format!("k{i:04}");
        let value = vec![b'v'; 256];
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();
    let after_second_flush = db.byte_size_on_disk().unwrap();
    assert!(
        after_second_flush > after_flush,
        "second SSTable should grow total: {after_flush} -> {after_second_flush}",
    );
}

#[test]
fn checksum_is_stable_across_flush_and_compact() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    for i in 0..50u32 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let checksum_in_memtable = db.checksum().unwrap();

    // Flush moves bytes from memtable to L0 SSTable. The
    // visible set is unchanged, so the checksum must match.
    db.flush().unwrap();
    let checksum_after_flush = db.checksum().unwrap();
    assert_eq!(
        checksum_in_memtable, checksum_after_flush,
        "flush is a layout move, not a state change; checksum must be stable",
    );

    // A second batch + flush + compact rearranges layers but
    // leaves the visible set unchanged for keys not touched.
    for i in 50..100u32 {
        let key = format!("key{i:04}");
        let value = format!("value{i:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    let checksum_after_two_flushes = db.checksum().unwrap();
    db.compact().unwrap();
    let checksum_after_compact = db.checksum().unwrap();
    assert_eq!(
        checksum_after_two_flushes, checksum_after_compact,
        "compact is a layout move; checksum must be stable across it",
    );
}

#[test]
fn checksum_changes_when_visible_set_changes() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"alpha", b"1").unwrap();
    db.put(b"bravo", b"2").unwrap();
    let h1 = db.checksum().unwrap();

    // Same key set with one value mutated -> different hash.
    db.put(b"alpha", b"1-prime").unwrap();
    let h2 = db.checksum().unwrap();
    assert_ne!(
        h1, h2,
        "value mutation must perturb the checksum: {h1:#x} -> {h2:#x}",
    );

    // Deleting a visible key removes it from the merge stream
    // and must perturb the checksum again.
    db.delete(b"bravo").unwrap();
    let h3 = db.checksum().unwrap();
    assert_ne!(
        h2, h3,
        "delete must perturb the checksum: {h2:#x} -> {h3:#x}",
    );
}

#[test]
fn checksum_is_path_independent_for_same_visible_state() {
    // Two databases that converge to the same visible
    // (key, value) set must produce the same checksum, even
    // when they were reached through different operation
    // histories. This is the cross-host divergence-detection
    // contract: replicas on the same logical state agree on
    // their fingerprint regardless of how the engine
    // physically arrived there.
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let db_a = Lattice::open(dir_a.path()).unwrap();
    let db_b = Lattice::open(dir_b.path()).unwrap();

    // db_a: write -> flush -> compact -> overwrite -> delete
    // a stale key.
    for i in 0..20u32 {
        let key = format!("k{i:02}");
        db_a.put(key.as_bytes(), b"first").unwrap();
    }
    db_a.flush().unwrap();
    db_a.compact().unwrap();
    db_a.put(b"k05", b"second").unwrap();
    db_a.delete(b"k10").unwrap();
    db_a.flush().unwrap();

    // db_b: write the final state directly, with no
    // intermediate flushes or compacts.
    for i in 0..20u32 {
        if i == 10 {
            continue;
        }
        let key = format!("k{i:02}");
        let value = if i == 5 { "second" } else { "first" };
        db_b.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    assert_eq!(
        db_a.checksum().unwrap(),
        db_b.checksum().unwrap(),
        "same visible state must produce the same checksum",
    );
}

#[test]
fn empty_database_has_a_well_defined_checksum() {
    // An empty database must produce a stable checksum: two
    // newly-opened empty databases agree, and that value
    // differs from any non-empty database's hash.
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let db_a = Lattice::open(dir_a.path()).unwrap();
    let db_b = Lattice::open(dir_b.path()).unwrap();

    let h_a = db_a.checksum().unwrap();
    let h_b = db_b.checksum().unwrap();
    assert_eq!(h_a, h_b, "empty databases must agree on the checksum");

    db_a.put(b"k", b"v").unwrap();
    assert_ne!(
        h_a,
        db_a.checksum().unwrap(),
        "writing a single key must change the checksum from empty",
    );
}
