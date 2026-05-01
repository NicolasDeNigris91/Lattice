//! Phase 2 integration tests covering flush to `SSTable`, mixed read path,
//! WAL truncation, and tombstone shadowing.

use std::fs;

use lattice_core::Lattice;
use tempfile::tempdir;

fn count_sst_files(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .count()
}

#[test]
fn explicit_flush_is_callable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
}

#[test]
fn flush_creates_sstable_and_truncates_wal() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"k", b"v").unwrap();
    let wal_size_before = fs::metadata(dir.path().join("wal.log")).unwrap().len();
    assert!(wal_size_before > 0);

    db.flush().unwrap();

    assert_eq!(
        fs::metadata(dir.path().join("wal.log")).unwrap().len(),
        0,
        "WAL must be truncated after a successful flush"
    );
    assert_eq!(count_sst_files(dir.path()), 1);
}

#[test]
fn data_readable_after_flush_in_same_session() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    db.flush().unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);
}

#[test]
fn data_persists_through_flush_and_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0u32..50 {
            db.put(i.to_be_bytes(), format!("value-{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }

    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..50 {
        assert_eq!(
            db.get(i.to_be_bytes()).unwrap(),
            Some(format!("value-{i}").into_bytes()),
            "key {i} missing after reopen"
        );
    }
}

#[test]
fn tombstone_in_memtable_shadows_older_sstable() {
    // The killer test for three-state lookup. If the memtable layer
    // collapsed "tombstoned here" with "absent here", the read path would
    // fall through and return the old SSTable value.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"k", b"old").unwrap();
    db.flush().unwrap(); // k=old now lives in SSTable
    db.delete(b"k").unwrap(); // tombstone in memtable

    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn newer_value_in_memtable_overrides_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v2").unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn newer_sstable_overrides_older_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(count_sst_files(dir.path()), 2);
}

#[test]
fn scan_merges_memtable_and_sstables_with_newest_winning() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // First SSTable: a=1, b=2
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();

    // Second SSTable: b=22, c=3
    db.put(b"b", b"22").unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();

    // Memtable: c is deleted, d=4
    db.delete(b"c").unwrap();
    db.put(b"d", b"4").unwrap();

    let pairs = db.scan(None).unwrap();
    assert_eq!(
        pairs,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"22".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]
    );
}

#[test]
fn scan_with_prefix_across_layers() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"user:1", b"alice").unwrap();
    db.put(b"product:1", b"book").unwrap();
    db.flush().unwrap();
    db.put(b"user:2", b"bob").unwrap();

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
fn many_keys_across_multiple_flushes() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        // Three batches of 200 keys each, flushed separately.
        for batch in 0u32..3 {
            for i in 0u32..200 {
                let key = (batch * 1000 + i).to_be_bytes();
                db.put(key, format!("v{batch}-{i}").as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }
    }
    assert!(count_sst_files(dir.path()) >= 3);

    let db = Lattice::open(dir.path()).unwrap();
    for batch in 0u32..3 {
        for i in 0u32..200 {
            let key = (batch * 1000 + i).to_be_bytes();
            assert_eq!(
                db.get(key).unwrap(),
                Some(format!("v{batch}-{i}").into_bytes()),
                "key {batch}/{i} missing after multi-flush reopen"
            );
        }
    }
}
