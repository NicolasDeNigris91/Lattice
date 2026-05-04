//! Contract tests for [`Lattice::backup_to`].
//!
//! `backup_to` produces a self-contained directory that
//! [`Lattice::open`] can open and observe the same logical
//! state as the source database. The directory is the
//! atomic unit a backup tool would archive (tar, copy to
//! object storage, ship to a replica) and a future restore
//! workflow would consume directly.
//!
//! The pinned properties are:
//!
//! - The output directory is openable by `Lattice::open`.
//! - Every key the source could resolve resolves the same
//!   way against the backup (same value, same `None` for
//!   tombstoned keys).
//! - The backup's [`Lattice::checksum`] matches the source's,
//!   which is the cross-host divergence-detection contract
//!   from v1.18 applied to the backup as a "replica".
//! - The backup operation does not move data inside the
//!   source directory.

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn backup_to_produces_an_openable_self_contained_directory() {
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = Lattice::open(src_dir.path()).unwrap();

    db.put(b"alpha", b"1").unwrap();
    db.put(b"bravo", b"2").unwrap();
    db.flush().unwrap();
    db.put(b"charlie", b"3").unwrap();
    db.delete(b"alpha").unwrap();

    db.backup_to(backup_dir.path()).unwrap();

    // The source must be unchanged: a backup is a read-only
    // observation, not a destructive operation.
    assert_eq!(db.get(b"alpha").unwrap(), None);
    assert_eq!(db.get(b"bravo").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"charlie").unwrap(), Some(b"3".to_vec()));

    // Open the backup as a fresh database and verify reads
    // resolve to the same values.
    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(restored.get(b"alpha").unwrap(), None);
    assert_eq!(restored.get(b"bravo").unwrap(), Some(b"2".to_vec()));
    assert_eq!(restored.get(b"charlie").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn backup_checksum_matches_source_checksum() {
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = Lattice::open(src_dir.path()).unwrap();

    for i in 0u32..30 {
        let key = format!("key{i:03}");
        let value = format!("value{i:03}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        if i % 5 == 0 {
            db.flush().unwrap();
        }
    }
    db.delete(b"key007").unwrap();
    db.compact().unwrap();

    let source_checksum = db.checksum().unwrap();

    db.backup_to(backup_dir.path()).unwrap();
    drop(db);

    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(
        restored.checksum().unwrap(),
        source_checksum,
        "backup must produce a state-equivalent copy",
    );
}

#[test]
fn backup_captures_unflushed_memtable_entries() {
    // The memtable is part of the visible state but not yet
    // on disk in any SSTable. backup_to must capture it,
    // either by replaying it into the backup's WAL or by
    // forcing a flush; either way the backup's view of every
    // memtable-only key must match the source.
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = Lattice::open(src_dir.path()).unwrap();

    db.put(b"flushed_first", b"persisted").unwrap();
    db.flush().unwrap();
    db.put(b"only_in_memtable", b"transient").unwrap();
    db.put(b"also_only_in_memtable", b"transient2").unwrap();

    db.backup_to(backup_dir.path()).unwrap();

    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(
        restored.get(b"flushed_first").unwrap(),
        Some(b"persisted".to_vec()),
    );
    assert_eq!(
        restored.get(b"only_in_memtable").unwrap(),
        Some(b"transient".to_vec()),
        "backup must capture in-memory writes, not just SSTables",
    );
    assert_eq!(
        restored.get(b"also_only_in_memtable").unwrap(),
        Some(b"transient2".to_vec()),
    );
}

#[test]
fn backup_of_empty_database_is_openable_and_empty() {
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = Lattice::open(src_dir.path()).unwrap();

    db.backup_to(backup_dir.path()).unwrap();

    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(restored.get(b"any").unwrap(), None);
    assert_eq!(restored.checksum().unwrap(), db.checksum().unwrap());
}

#[test]
fn backup_directory_is_independent_of_the_source() {
    // After backup, mutations to the source must not affect
    // the backup. Operators rely on this for archival
    // semantics: a backup taken at time T is a frozen view
    // of the database at T.
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = Lattice::open(src_dir.path()).unwrap();

    db.put(b"k", b"backup-time-value").unwrap();
    db.flush().unwrap();
    db.backup_to(backup_dir.path()).unwrap();

    // Mutate the source extensively after the backup.
    db.put(b"k", b"post-backup-value").unwrap();
    db.put(b"new_key", b"only-in-source").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(
        restored.get(b"k").unwrap(),
        Some(b"backup-time-value".to_vec()),
        "backup must be insulated from post-backup source mutations",
    );
    assert_eq!(
        restored.get(b"new_key").unwrap(),
        None,
        "post-backup writes must not appear in the restored database",
    );
}
