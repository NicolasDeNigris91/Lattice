//! Non-blocking compaction via `Lattice::compact_async`. The
//! call schedules a background round and returns immediately;
//! the application can keep doing work while the merge happens
//! on the dedicated compactor thread. Demonstrates the
//! coalescing model (multiple in-flight calls collapse to one
//! captured generation) and the wait-on-handle pattern.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 05_compact_async
//! ```

use lattice_core::{Lattice, Result};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");
    let db = Lattice::open(dir.path())?;

    // Build enough on-disk state that a synchronous compact
    // would take measurable wall-clock time.
    for batch in 0..6u32 {
        for i in 0..400u32 {
            let key = format!("k{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"payload")?;
        }
        db.flush()?;
    }

    // Schedule the round. compact_async returns the handle in
    // bounded time; the I/O happens on the background worker.
    let started = Instant::now();
    let handle = db.compact_async();
    println!(
        "compact_async returned in {:?}; round runs on background thread",
        started.elapsed(),
    );

    // Application work the writer can do while compaction runs.
    for i in 0..1000u32 {
        let key = format!("live{i:08}");
        db.put(key.as_bytes(), b"x")?;
    }
    println!("writer thread did 1000 puts during background compaction");

    // Block until the round publishes its completion.
    let waited_at = Instant::now();
    handle.wait()?;
    println!(
        "wait() returned after {:?}; database is fully compacted",
        waited_at.elapsed(),
    );

    Ok(())
}
