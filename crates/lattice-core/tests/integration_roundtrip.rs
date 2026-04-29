//! End-to-end round-trip tests for the basic key-value contract.
//!
//! These exercise the public API of `Lattice` against a real temporary
//! directory. They cover the in-memory path first, then add tests that
//! force write-ahead-log persistence by reopening the database.

use std::fs::OpenOptions;
use std::io::Write;

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn put_then_get_returns_value() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"alpha", b"1").unwrap();

    assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn put_persists_across_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();
    }
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn delete_persists_across_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
    }
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn last_write_wins_across_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"first").unwrap();
        db.put(b"k", b"second").unwrap();
        db.put(b"k", b"third").unwrap();
    }
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"third".to_vec()));
}

#[test]
fn empty_value_is_distinct_from_absent_key() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(Vec::new()));
    assert_eq!(db.get(b"never").unwrap(), None);
}

#[test]
fn torn_trailing_record_is_ignored() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
    }
    // Append a garbage tail to simulate a torn write at the end of the WAL.
    let mut wal = OpenOptions::new()
        .append(true)
        .open(dir.path().join("wal.log"))
        .unwrap();
    wal.write_all(&[0xFFu8; 32]).unwrap();
    drop(wal);

    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn scan_returns_pairs_in_key_order() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"banana", b"yellow").unwrap();
    db.put(b"apple", b"red").unwrap();
    db.put(b"cherry", b"red").unwrap();

    let pairs = db.scan(None).unwrap();

    assert_eq!(
        pairs,
        vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
            (b"cherry".to_vec(), b"red".to_vec()),
        ]
    );
}

#[test]
fn scan_with_prefix_filters_keys() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"user:1", b"alice").unwrap();
    db.put(b"user:2", b"bob").unwrap();
    db.put(b"product:1", b"book").unwrap();

    let pairs = db.scan(Some(b"user:")).unwrap();

    assert_eq!(
        pairs,
        vec![
            (b"user:1".to_vec(), b"alice".to_vec()),
            (b"user:2".to_vec(), b"bob".to_vec()),
        ]
    );
}

#[test]
fn scan_skips_tombstoned_keys() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.delete(b"a").unwrap();

    let pairs = db.scan(None).unwrap();

    assert_eq!(pairs, vec![(b"b".to_vec(), b"2".to_vec())]);
}

#[test]
fn many_keys_persist_in_order() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0u32..1000 {
            db.put(&i.to_be_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
    }
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..1000 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} mismatch"
        );
    }
}

#[test]
fn builder_configures_flush_threshold() {
    // The builder path with a 1 KiB flush threshold must trigger an
    // auto-flush after a few small puts, leaving the WAL truncated and
    // the data resident in an SSTable. Pins the documented surface of
    // `Lattice::builder(path).flush_threshold_bytes(n).open()`.
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .flush_threshold_bytes(1024)
        .compaction_threshold(usize::MAX)
        .open()
        .unwrap();

    for i in 0u32..32 {
        db.put(&i.to_be_bytes(), &[b'x'; 64]).unwrap();
    }

    let sst_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .count();
    assert!(
        sst_count >= 1,
        "auto-flush should have produced at least one sstable"
    );

    // Default `Lattice::open` must remain a working shorthand for the
    // builder with defaults: reopening reads back what we wrote.
    drop(db);
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(&0u32.to_be_bytes()).unwrap(), Some(vec![b'x'; 64]));
}
