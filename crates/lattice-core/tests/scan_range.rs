//! Contract tests for the range-bounded streaming scan
//! ([`Lattice::scan_range`]).
//!
//! `scan_range` yields visible `(key, value)` pairs whose key
//! falls within `[start, end)`, in strictly increasing key
//! order. The bounds are inclusive-exclusive to match the
//! standard `a..b` Rust range idiom. The tests pin the
//! observable contract: bound semantics, unbounded sides,
//! tier mixing, and the empty-range edge case.

use lattice_core::Lattice;
use tempfile::tempdir;

fn keys_of(db: &Lattice, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Vec<u8>> {
    db.scan_range(start, end)
        .filter_map(Result::ok)
        .map(|(k, _)| k)
        .collect()
}

#[test]
fn scan_range_inclusive_start_exclusive_end() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for c in b'a'..=b'g' {
        db.put([c], b"v").unwrap();
    }

    assert_eq!(
        keys_of(&db, Some(b"c"), Some(b"f")),
        vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()],
    );
}

#[test]
fn scan_range_unbounded_start_means_from_beginning() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for c in b'a'..=b'g' {
        db.put([c], b"v").unwrap();
    }

    assert_eq!(
        keys_of(&db, None, Some(b"c")),
        vec![b"a".to_vec(), b"b".to_vec()],
    );
}

#[test]
fn scan_range_unbounded_end_means_to_end() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for c in b'a'..=b'g' {
        db.put([c], b"v").unwrap();
    }

    assert_eq!(
        keys_of(&db, Some(b"e"), None),
        vec![b"e".to_vec(), b"f".to_vec(), b"g".to_vec()],
    );
}

#[test]
fn scan_range_unbounded_both_sides_yields_full_keyspace() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();

    let from_range: Vec<_> = db.scan_range(None, None).filter_map(Result::ok).collect();
    let from_iter = db.scan(None).unwrap();
    assert_eq!(from_range, from_iter);
}

#[test]
fn scan_range_empty_when_start_equals_or_exceeds_end() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for c in b'a'..=b'g' {
        db.put([c], b"v").unwrap();
    }

    // start == end: half-open `[c, c)` is empty.
    assert!(keys_of(&db, Some(b"c"), Some(b"c")).is_empty());

    // start > end: also empty.
    assert!(keys_of(&db, Some(b"f"), Some(b"b")).is_empty());
}

#[test]
fn scan_range_walks_across_memtable_frozen_and_sstable() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // Oldest writes -> first SSTable.
    db.put(b"a", b"old").unwrap();
    db.put(b"b", b"old").unwrap();
    db.put(b"c", b"old").unwrap();
    db.flush().unwrap();

    // Middle writes; some overwrite older keys.
    db.put(b"b", b"mid").unwrap();
    db.put(b"d", b"mid").unwrap();
    db.flush().unwrap();

    // Newest writes in the active memtable.
    db.put(b"a", b"new").unwrap();
    db.put(b"e", b"new").unwrap();

    let pairs: Vec<_> = db
        .scan_range(Some(b"b"), Some(b"e"))
        .filter_map(Result::ok)
        .collect();

    assert_eq!(
        pairs,
        vec![
            (b"b".to_vec(), b"mid".to_vec()),
            (b"c".to_vec(), b"old".to_vec()),
            (b"d".to_vec(), b"mid".to_vec()),
        ],
    );
}

#[test]
fn scan_range_filters_tombstones() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();
    db.delete(b"b").unwrap();

    assert_eq!(
        keys_of(&db, Some(b"a"), Some(b"d")),
        vec![b"a".to_vec(), b"c".to_vec()],
    );
}
