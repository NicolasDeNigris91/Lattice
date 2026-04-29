//! Contract tests for the M4 transaction surface.
//!
//! Pin snapshot isolation, read-your-writes within a transaction,
//! atomic commit, rollback on Err / Drop, and conflict detection
//! between concurrent transactions sharing a database handle.

use std::io;

use lattice_core::{Error, Lattice};
use tempfile::tempdir;

#[test]
fn transaction_isolated_read_view() {
    // A transaction snapshots the database at start. A concurrent
    // write through another handle clone must not be visible to
    // reads issued inside the transaction.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v0").unwrap();

    let db_outside = db.clone();
    db.transaction(|tx| {
        let before = tx.get(b"k").unwrap();
        // Outside write through a clone after the snapshot.
        db_outside.put(b"k", b"v1").unwrap();
        let after = tx.get(b"k").unwrap();
        assert_eq!(before, Some(b"v0".to_vec()));
        assert_eq!(after, before, "snapshot must not see outside writes");
        Ok(())
    })
    .unwrap();

    // Outside the transaction, the new value is live.
    assert_eq!(db.get(b"k").unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn transaction_read_your_own_writes_within_tx() {
    // A put inside the transaction must be visible to a subsequent
    // get inside the same transaction, even though the underlying
    // snapshot does not see it.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.transaction(|tx| {
        assert_eq!(tx.get(b"k").unwrap(), None);
        tx.put(b"k", b"v");
        assert_eq!(tx.get(b"k").unwrap(), Some(b"v".to_vec()));
        tx.delete(b"k");
        assert_eq!(tx.get(b"k").unwrap(), None);
        Ok(())
    })
    .unwrap();
}

#[test]
fn transaction_commit_applies_all_writes_atomically() {
    // After a successful commit, every write performed inside the
    // transaction is visible through the parent handle. The set is
    // applied atomically: either all become live or none do.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"existing", b"keep").unwrap();

    db.transaction(|tx| {
        tx.put(b"a", b"1");
        tx.put(b"b", b"2");
        tx.delete(b"existing");
        Ok(())
    })
    .unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"existing").unwrap(), None);

    // And after reopen.
    drop(db);
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"existing").unwrap(), None);
}

#[test]
fn transaction_rollback_when_closure_returns_err() {
    // The closure returning `Err` discards every staged write.
    // The error bubbles up to the caller unchanged.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"existing", b"keep").unwrap();

    let result = db.transaction(|tx| {
        tx.put(b"a", b"1");
        tx.delete(b"existing");
        Err::<(), _>(Error::Io(io::Error::other("user aborted")))
    });

    assert!(matches!(result, Err(Error::Io(_))));
    assert_eq!(db.get(b"a").unwrap(), None, "staged put must not apply");
    assert_eq!(
        db.get(b"existing").unwrap(),
        Some(b"keep".to_vec()),
        "staged delete must not apply"
    );
}

#[test]
fn transaction_rollback_when_closure_panics() {
    // A panic inside the closure unwinds past the commit step, so
    // staged writes are dropped on the floor. The database state
    // is identical to before the transaction was attempted.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"existing", b"keep").unwrap();

    let db_for_panic = db.clone();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = db_for_panic.transaction(|tx| {
            tx.put(b"a", b"1");
            panic!("simulated user panic");
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        });
    }));

    assert!(outcome.is_err(), "the panic must propagate");
    assert_eq!(db.get(b"a").unwrap(), None, "staged put must not apply");
    assert_eq!(db.get(b"existing").unwrap(), Some(b"keep".to_vec()));
}

#[test]
fn read_only_transaction_commits_with_no_writes() {
    // A transaction that only reads commits trivially: no writes
    // means no possibility of conflict and no state change.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let value = db
        .transaction(|tx| {
            let v = tx.get(b"k").unwrap();
            Ok::<_, lattice_core::Error>(v)
        })
        .unwrap();

    assert_eq!(value, Some(b"v".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn concurrent_transactions_on_disjoint_keys_both_apply() {
    // Two transactions writing different keys interleave through
    // the WAL mutex but neither overwrites the other. v1.4 has no
    // conflict detection, so the result for disjoint keys is
    // identical to the strict-isolation semantics that v1.5 will
    // pin: both writes land.
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    let t1 = {
        let db = db.clone();
        thread::spawn(move || {
            db.transaction(|tx| {
                tx.put(b"a", b"1");
                Ok::<_, lattice_core::Error>(())
            })
        })
    };
    let t2 = {
        let db = db.clone();
        thread::spawn(move || {
            db.transaction(|tx| {
                tx.put(b"b", b"2");
                Ok::<_, lattice_core::Error>(())
            })
        })
    };

    t1.join().unwrap().unwrap();
    t2.join().unwrap().unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}
