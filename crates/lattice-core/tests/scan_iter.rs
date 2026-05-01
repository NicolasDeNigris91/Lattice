//! Contract tests for the streaming scan iterator.
//!
//! `scan` materialises every visible `(key, value)` pair into a
//! `Vec` before returning. v1.12 introduces `scan_iter`, which
//! exposes the same merge-and-dedupe logic behind an `Iterator` so
//! callers can walk the keyspace one entry at a time without
//! holding the entire result in memory. The two APIs must be
//! observationally equivalent: same set of visible pairs, same key
//! order, same tombstone filtering.

use lattice_core::Lattice;
use tempfile::tempdir;

fn collect_iter(db: &Lattice, prefix: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.scan_iter(prefix)
        .map(|r| r.expect("scan_iter yielded an error"))
        .collect()
}

#[test]
fn scan_iter_matches_scan_on_active_only() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"alpha", b"1").unwrap();
    db.put(b"bravo", b"2").unwrap();
    db.put(b"charlie", b"3").unwrap();

    let from_scan = db.scan(None).unwrap();
    let from_iter = collect_iter(&db, None);

    assert_eq!(from_iter, from_scan);
    assert_eq!(
        from_iter,
        vec![
            (b"alpha".to_vec(), b"1".to_vec()),
            (b"bravo".to_vec(), b"2".to_vec()),
            (b"charlie".to_vec(), b"3".to_vec()),
        ],
    );
}

#[test]
fn scan_iter_matches_scan_across_memtable_frozen_and_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // SSTable: oldest writes.
    db.put(b"a", b"sst").unwrap();
    db.put(b"b", b"sst").unwrap();
    db.put(b"c", b"sst").unwrap();
    db.flush().unwrap();

    // Frozen memtable: middle writes overwrite some sstable keys.
    db.put(b"b", b"frozen").unwrap();
    db.put(b"d", b"frozen").unwrap();
    db.flush().unwrap();
    db.put(b"e", b"frozen").unwrap();
    // The line above lives in the active memtable. Flush again so it
    // becomes a sstable too, then add new active-memtable writes.
    db.flush().unwrap();

    // Active memtable: newest writes overwrite both prior tiers.
    db.put(b"a", b"active").unwrap();
    db.put(b"f", b"active").unwrap();
    db.delete(b"d").unwrap();

    let from_scan = db.scan(None).unwrap();
    let from_iter = collect_iter(&db, None);

    assert_eq!(from_iter, from_scan);
    // Spot-check the merge: `a` resolves to active's value, `b`
    // resolves to frozen's value (newest write to that key), `d` is
    // tombstoned by active so absent from the result.
    assert!(from_iter.contains(&(b"a".to_vec(), b"active".to_vec())));
    assert!(from_iter.contains(&(b"b".to_vec(), b"frozen".to_vec())));
    assert!(from_iter.contains(&(b"c".to_vec(), b"sst".to_vec())));
    assert!(!from_iter.iter().any(|(k, _)| k == b"d"));
    assert!(from_iter.contains(&(b"e".to_vec(), b"frozen".to_vec())));
    assert!(from_iter.contains(&(b"f".to_vec(), b"active".to_vec())));
}

#[test]
fn scan_iter_honours_prefix() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"app:1", b"a").unwrap();
    db.put(b"app:2", b"b").unwrap();
    db.put(b"other", b"x").unwrap();
    db.flush().unwrap();
    db.put(b"app:3", b"c").unwrap();

    let from_scan = db.scan(Some(b"app:")).unwrap();
    let from_iter = collect_iter(&db, Some(b"app:"));

    assert_eq!(from_iter, from_scan);
    assert_eq!(from_iter.len(), 3);
    assert!(from_iter.iter().all(|(k, _)| k.starts_with(b"app:")));
}

#[test]
fn scan_iter_yields_strictly_increasing_keys() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0..100u32 {
        db.put(&i.to_be_bytes(), format!("v{i}").as_bytes())
            .unwrap();
        if i % 17 == 0 {
            db.flush().unwrap();
        }
    }
    // Overwrite half of them in the active memtable so the merge
    // walks the same key from multiple tiers.
    for i in 0..50u32 {
        db.put(&i.to_be_bytes(), format!("v{i}-new").as_bytes())
            .unwrap();
    }

    let pairs = collect_iter(&db, None);
    assert_eq!(pairs.len(), 100);
    for window in pairs.windows(2) {
        assert!(
            window[0].0 < window[1].0,
            "scan_iter must yield strictly increasing keys; got {:?} then {:?}",
            window[0].0,
            window[1].0,
        );
    }
}

#[test]
fn scan_iter_filters_tombstones_when_only_visible_in_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    db.flush().unwrap();

    let pairs = collect_iter(&db, None);
    assert!(
        pairs.is_empty(),
        "scan_iter must hide tombstoned keys, got {pairs:?}",
    );
}
