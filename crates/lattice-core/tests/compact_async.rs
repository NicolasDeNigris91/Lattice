//! Contract tests for non-blocking compaction.
//!
//! `compact()` runs the merge on the caller's thread and blocks
//! until the manifest is rewritten and orphans are deleted. v1.13
//! adds `compact_async()` which spawns the round on a dedicated
//! background thread and returns a handle the caller can `wait()`
//! on. The synchronous `compact()` becomes a thin
//! `compact_async().wait()`, preserving the existing API.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use lattice_core::Lattice;
use tempfile::tempdir;

/// `compact_async` returns the handle in a bounded time even when
/// the round itself takes a long time. The whole point is the
/// caller does not wait for the I/O.
#[test]
fn compact_async_returns_immediately() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // Build enough on-disk state that a synchronous compact would
    // take measurable wall-clock time. Each flush installs an
    // SSTable; eight tables in level 0 forces the auto-compaction
    // threshold and the explicit compact() call would do real work.
    for batch in 0..8u32 {
        for i in 0..200u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    let started = Instant::now();
    let handle = db.compact_async();
    let return_latency = started.elapsed();

    assert!(
        return_latency < Duration::from_millis(50),
        "compact_async must return within 50ms regardless of the round's wall-clock cost; took {return_latency:?}",
    );

    // Drain so the next test sees a clean directory.
    handle.wait().unwrap();
}

/// `compact_async().wait()` is observationally equivalent to a
/// synchronous `compact()` on the same database state. The merged
/// output, the manifest, and the per-level layout all converge.
#[test]
fn compact_async_wait_matches_synchronous_compact() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    for batch in 0..6u32 {
        for i in 0..100u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    db.compact_async().wait().unwrap();

    // After a full compaction every non-empty level holds at most
    // one SSTable. Use the public Debug to read the layout.
    let dbg = format!("{db:?}");
    assert!(
        dbg.contains("sstables: 1") || dbg.contains("sstables: 0"),
        "after compact_async + wait the database must be at most one SSTable; got {dbg}",
    );

    // And every key must still read back.
    for batch in 0..6u32 {
        for i in 0..100u32 {
            let key = format!("k{batch:02}-{i:04}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(b"v".as_slice()),
                "key {key} lost during compact_async",
            );
        }
    }
}

/// Multiple in-flight `compact_async` calls coalesce: the
/// background thread runs whatever rounds the levels need; every
/// outstanding handle's `wait()` returns once the levels converge.
/// In particular, calling `wait()` on the LATEST handle is enough
/// to know every PRIOR request is also done.
#[test]
fn compact_async_calls_coalesce_under_concurrent_callers() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    for batch in 0..4u32 {
        for i in 0..150u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    // Fire several requests in quick succession; the early
    // handles are dropped without waiting. The generation
    // counter inside the compactor advances on each call, and
    // the worker coalesces them, so waiting on the highest
    // generation is enough.
    for _ in 0..4 {
        let _ = db.compact_async();
    }
    let last = db.compact_async();
    last.wait().unwrap();

    // Levels must be settled.
    let dbg = format!("{db:?}");
    assert!(
        dbg.contains("sstables: 1") || dbg.contains("sstables: 0"),
        "after coalesced waits the database must be at most one SSTable; got {dbg}",
    );
}

/// A writer thread that runs `put` while a `compact_async` round
/// runs in the background must not block on the round. Today,
/// `compact()` holds `mutation_lock` for the whole round and
/// stalls plain puts; the async variant must release the writer
/// to make progress.
#[test]
fn writer_thread_makes_progress_during_compact_async() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Lattice::open(dir.path()).unwrap());

    // Pre-populate enough levels to make a real compaction round.
    for batch in 0..6u32 {
        for i in 0..200u32 {
            let key = format!("seed{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut count = 0u64;
            while !stop.load(Ordering::Acquire) {
                let key = format!("live{count:08}");
                db.put(key.as_bytes(), b"x").unwrap();
                count += 1;
            }
            count
        })
    };

    let handle = db.compact_async();
    handle.wait().unwrap();
    stop.store(true, Ordering::Release);
    let n = writer.join().unwrap();

    assert!(
        n > 0,
        "writer thread must complete at least one put during the compact_async round; got n={n}",
    );
}

/// Dropping the `Lattice` while a background compaction is still
/// in flight must shut the worker thread down cleanly. The test
/// just exercises the path; absence of a panic and absence of a
/// zombie thread are the contract.
#[test]
fn drop_during_in_flight_compact_async_is_clean() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    for batch in 0..4u32 {
        for i in 0..100u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
    }

    let _handle = db.compact_async();
    // Drop the handle and the database without waiting; the
    // worker must finish or be cancelled cleanly during Inner's
    // Drop. If this hangs, the test runner kills the process.
    drop(db);
}
