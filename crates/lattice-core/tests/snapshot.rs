//! Phase 5 integration tests for snapshot isolation.

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn snapshot_sees_state_at_creation() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap = db.snapshot();

    db.put(b"a", b"NEW").unwrap();
    db.put(b"c", b"3").unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(snap.get(b"c").unwrap(), None);

    // Live database shows the new state.
    assert_eq!(db.get(b"a").unwrap(), Some(b"NEW".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn snapshot_keeps_deleted_value_visible() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let snap = db.snapshot();
    db.delete(b"k").unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn snapshot_survives_flush() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap = db.snapshot();
    db.put(b"a", b"NEW").unwrap();
    db.flush().unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn snapshot_survives_compaction() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    // Mutate and compact: this changes the live SSTable set but the
    // snapshot's `Arc<SSTableReader>` instances stay alive.
    db.put(b"a", b"NEW").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(snap.get(b"missing").unwrap(), None);

    // Live database reflects the new value.
    assert_eq!(db.get(b"a").unwrap(), Some(b"NEW".to_vec()));
}

#[test]
fn snapshot_scan_returns_frozen_pairs() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"apple", b"red").unwrap();
    db.put(b"banana", b"yellow").unwrap();

    let snap = db.snapshot();
    db.put(b"cherry", b"red").unwrap();
    db.delete(b"apple").unwrap();

    let pairs = snap.scan(None).unwrap();
    assert_eq!(
        pairs,
        vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
        ]
    );
}

#[test]
fn multiple_snapshots_are_independent() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v1").unwrap();

    let s1 = db.snapshot();
    db.put(b"k", b"v2").unwrap();
    let s2 = db.snapshot();
    db.put(b"k", b"v3").unwrap();

    assert_eq!(s1.get(b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s2.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn snapshot_can_be_cloned() {
    let dir = tempdir().unwrap();
    let mut db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let s1 = db.snapshot();
    let s2 = s1.clone();
    db.put(b"k", b"NEW").unwrap();

    assert_eq!(s1.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(s2.get(b"k").unwrap(), Some(b"v".to_vec()));
}
