//! Contract tests for `WriteOptions` and amortised group commit.
//!
//! These pin the public API and durability semantics of the M1
//! milestone. Each test is a single behaviour pinned in isolation.

use lattice_core::{Lattice, WriteOptions};
use tempfile::tempdir;

#[test]
fn write_options_default_is_durable() {
    // The safety default is "fsync per write". Anyone reaching for the
    // amortised path has to opt out explicitly. This default is the
    // contract that makes `put` continue to mean what it always meant
    // for callers from v1.0.x.
    assert!(WriteOptions::default().durable);
}

#[test]
fn put_with_durable_default_persists_across_reopen() {
    // `put_with(k, v, WriteOptions::default())` is the long-form of
    // `put(k, v)`. Both must give the v1.0.x guarantee: the value is
    // visible after a fresh `open` with no explicit flush.
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put_with(b"k", b"v", WriteOptions::default()).unwrap();
    }
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn put_with_non_durable_is_visible_in_same_session() {
    // Read-your-own-writes within a single handle must hold even for
    // non-durable writes, because the memtable is updated in
    // lock-step with the WAL append regardless of fsync timing.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put_with(b"k", b"v", WriteOptions { durable: false })
        .unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn non_durable_writes_persist_after_explicit_flush_wal_and_reopen() {
    // The opt-in shape: stream writes through `put_with` with
    // `durable: false`, then call `flush_wal()` to force a single
    // fsync. After dropping the handle and reopening, every queued
    // write must be readable. This is the "I want amortised cost
    // but I will checkpoint myself" path.
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0u32..200 {
            db.put_with(&i.to_be_bytes(), b"v", WriteOptions { durable: false })
                .unwrap();
        }
        db.flush_wal().unwrap();
    }
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..200 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "key {i} missing after flush_wal + reopen"
        );
    }
}

#[test]
fn non_durable_writes_are_lost_when_drop_is_skipped() {
    // The honest test of the trade-off. With a very large commit
    // window and no explicit `flush_wal`, a non-durable put leaves
    // the bytes only in the user-space BufWriter inside the WAL
    // handle. `mem::forget` skips Drop (which would have flushed),
    // simulating a process that died before the next group-commit
    // tick. After reopen the value must be absent: this is what
    // "non-durable" actually means.
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .commit_window(std::time::Duration::from_secs(3600))
        .commit_batch(usize::MAX)
        .open()
        .unwrap();
    db.put_with(b"k", b"v", WriteOptions { durable: false })
        .unwrap();
    std::mem::forget(db);

    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        None,
        "non-durable put must be lost when Drop is skipped"
    );
}

#[test]
fn commit_batch_threshold_makes_non_durable_durable_without_flush_wal() {
    // With `commit_batch = 8` and the timer disabled, the eighth
    // non-durable put must trigger an automatic group commit. After
    // that point the bytes survive a skipped Drop, while the seventh
    // put alone would not. Pins the batch trigger semantics.
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .commit_window(std::time::Duration::from_secs(3600))
        .commit_batch(8)
        .open()
        .unwrap();
    for i in 0u32..8 {
        db.put_with(&i.to_be_bytes(), b"v", WriteOptions { durable: false })
            .unwrap();
    }
    std::mem::forget(db);

    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..8 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "key {i} must survive: the 8th non-durable put crossed the batch threshold"
        );
    }
}
