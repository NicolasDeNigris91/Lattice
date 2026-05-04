//! Contract tests for the v1.25 read-only handle.
//!
//! `LatticeBuilder::read_only(true)` and the convenience
//! `Lattice::open_read_only` produce a handle that can read
//! existing data but rejects every mutation with
//! `Error::ReadOnly`. The flusher and compactor threads are
//! not spawned, so a read-only handle is operationally
//! cheap.

use lattice_core::{Error, Lattice};
use tempfile::tempdir;

fn seed_database(path: &std::path::Path) {
    let db = Lattice::open(path).unwrap();
    db.put(b"alpha", b"1").unwrap();
    db.put(b"bravo", b"2").unwrap();
    db.put(b"charlie", b"3").unwrap();
    db.flush().unwrap();
    db.put(b"delta", b"4").unwrap();
    // Drop closes the WAL; reopening as read-only will replay it.
}

#[test]
fn read_only_open_observes_existing_data() {
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let db = Lattice::open_read_only(dir.path()).unwrap();
    assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"bravo").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"charlie").unwrap(), Some(b"3".to_vec()));
    // delta lives in the memtable replayed from the WAL.
    assert_eq!(db.get(b"delta").unwrap(), Some(b"4".to_vec()));
}

#[test]
fn read_only_rejects_put_delete_flush_compact() {
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let db = Lattice::open_read_only(dir.path()).unwrap();

    assert!(matches!(db.put(b"new", b"v"), Err(Error::ReadOnly),));
    assert!(matches!(db.delete(b"alpha"), Err(Error::ReadOnly),));
    assert!(matches!(db.flush(), Err(Error::ReadOnly)));
    assert!(matches!(db.flush_wal(), Err(Error::ReadOnly)));
    assert!(matches!(db.compact(), Err(Error::ReadOnly)));

    // Reads still work after a rejected mutation: the handle
    // stays usable for its read-only purpose.
    assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn read_only_config_reflects_the_flag() {
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let rw = Lattice::open(dir.path()).unwrap();
    assert!(!rw.config().read_only);
    drop(rw);

    let ro = Lattice::open_read_only(dir.path()).unwrap();
    assert!(ro.config().read_only);
}

#[test]
fn read_only_snapshot_and_scan_iter_work_normally() {
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let db = Lattice::open_read_only(dir.path()).unwrap();

    // Snapshot reads.
    let snap = db.snapshot();
    assert_eq!(snap.get(b"alpha").unwrap(), Some(b"1".to_vec()));

    // Streaming scan.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = db.scan_iter(None).collect::<Result<_, _>>().unwrap();
    assert_eq!(pairs.len(), 4, "all four keys should be visible: {pairs:?}");

    // Inventory.
    assert!(db.byte_size_on_disk().unwrap() > 0);
    let _ = db.checksum().unwrap();
}

#[test]
fn read_only_transaction_with_writes_returns_read_only_error() {
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let db = Lattice::open_read_only(dir.path()).unwrap();

    // A transaction that stages a write must error with ReadOnly
    // at commit; the closure runs so the read-only path of a
    // tx is still usable for "compute over a snapshot".
    let result: Result<(), Error> = db.transaction(|tx| {
        let _ = tx.get(b"alpha")?;
        tx.put(b"new", b"v");
        Ok(())
    });
    assert!(matches!(result, Err(Error::ReadOnly)));

    // The would-be mutation did not land.
    assert_eq!(db.get(b"new").unwrap(), None);
}

#[test]
fn read_only_transaction_with_only_reads_succeeds() {
    // A transaction whose closure only reads (empty write_set)
    // is just a more-structured snapshot; it commits cleanly
    // even on a read-only handle.
    let dir = tempdir().unwrap();
    seed_database(dir.path());

    let db = Lattice::open_read_only(dir.path()).unwrap();

    let total_len: usize = db
        .transaction(|tx| {
            let a = tx.get(b"alpha")?.unwrap_or_default().len();
            let b = tx.get(b"bravo")?.unwrap_or_default().len();
            Ok::<_, Error>(a + b)
        })
        .unwrap();
    assert_eq!(total_len, 2);
}

#[test]
fn read_only_is_compatible_with_a_concurrent_read_write_handle() {
    // Two handles to the same directory: one read-write, one
    // read-only. The read-only handle observes whatever the
    // read-write handle has already made durable, and remains
    // unable to perturb the data itself.
    let dir = tempdir().unwrap();
    let rw = Lattice::open(dir.path()).unwrap();
    rw.put(b"k", b"v0").unwrap();
    rw.flush().unwrap();

    let ro = Lattice::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.get(b"k").unwrap(), Some(b"v0".to_vec()));

    // Mutating the read-only handle still errors regardless of
    // the read-write handle's existence.
    assert!(matches!(ro.put(b"x", b"y"), Err(Error::ReadOnly)));
}
