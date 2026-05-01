//! Contract tests for the M2 concurrency surface.
//!
//! Pin the post-refactor guarantees: `Lattice` is `Send + Sync`,
//! cheap to `Clone` (shared `Arc<Inner>`), and reads on one handle
//! observe writes committed via another handle that points at the
//! same database.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn lattice_is_send_and_sync() {
    // Compile-time assertion. If `Lattice` ever loses `Send` or
    // `Sync` (e.g. by holding a non-thread-safe interior), this
    // test stops compiling.
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Lattice>();
}

#[test]
fn cloned_handle_observes_writes_from_origin() {
    // Two handles to the same database share their `Arc<Inner>`,
    // so writes made through one are immediately visible through
    // the other without going through reopen.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    let h2 = db.clone();

    db.put(b"k", b"v").unwrap();

    assert_eq!(h2.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn clone_keeps_database_alive_after_origin_drops() {
    // Dropping the original handle must not close the database
    // while a clone still holds a reference. Pins that Drop on
    // `Inner` only fires when the last `Arc` goes away.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    let h2 = db.clone();
    drop(db);

    h2.put(b"k", b"v").unwrap();
    assert_eq!(h2.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn many_readers_and_one_writer_see_consistent_state() {
    // Stress: spawn N reader threads and 1 writer thread that
    // share a Lattice handle. The writer streams 0..M durable
    // puts of `i.to_be_bytes() -> i.to_le_bytes()`. Readers loop
    // checking that any key they observe maps to the value the
    // writer used: no torn reads, no zeroed values, no panics.
    //
    // The test does not assert linearizability ordering between
    // readers and writers, only that every observed value is one
    // the writer actually wrote. That is the contract.
    const READER_COUNT: usize = 8;
    const WRITE_COUNT: u32 = 2_000;

    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    let stop = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(AtomicUsize::new(0));

    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let observations = Arc::clone(&observations);
        readers.push(thread::spawn(move || {
            while stop.load(Ordering::Acquire) == 0 {
                for i in 0..WRITE_COUNT {
                    if let Some(v) = db.get(i.to_be_bytes()).unwrap() {
                        // Any value we observe must be the canonical
                        // one the writer used for this key.
                        assert_eq!(
                            v,
                            i.to_le_bytes().to_vec(),
                            "reader saw a value the writer never wrote for key {i}"
                        );
                        observations.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let writer = {
        let db = db.clone();
        thread::spawn(move || {
            for i in 0..WRITE_COUNT {
                db.put(i.to_be_bytes(), i.to_le_bytes()).unwrap();
            }
        })
    };

    writer.join().unwrap();
    stop.store(1, Ordering::Release);
    for r in readers {
        r.join().unwrap();
    }

    // All keys must be present after the writer finishes.
    for i in 0..WRITE_COUNT {
        assert_eq!(
            db.get(i.to_be_bytes()).unwrap(),
            Some(i.to_le_bytes().to_vec()),
            "key {i} missing after the writer joined"
        );
    }

    // Sanity: at least some reads landed during the writer's run.
    // The exact number is system-dependent, so the assertion is
    // weak.
    assert!(
        observations.load(Ordering::Relaxed) > 0,
        "no reader ever observed a value during the run"
    );
}
