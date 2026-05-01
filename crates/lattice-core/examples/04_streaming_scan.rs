//! Streaming scan over the keyspace via `Lattice::scan_iter`.
//! Demonstrates the bounded-memory iteration model: the engine
//! holds only the merge frontier (one entry per LSM source) plus
//! one decoded block per `SSTable`, regardless of how many keys
//! the scan ultimately yields.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 04_streaming_scan
//! ```

use lattice_core::{Lattice, Result};
use tempfile::tempdir;

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");
    let db = Lattice::open(dir.path())?;

    // Bulk-load a few thousand keys across multiple SSTables to
    // exercise the merge.
    for batch in 0..5u32 {
        for i in 0..500u32 {
            let key = format!("user:{batch:02}:{i:04}");
            db.put(key.as_bytes(), format!("payload-{i}").as_bytes())?;
        }
        db.flush()?;
    }

    // Walk the entire keyspace one entry at a time. The result
    // never lives in memory in full; the iterator yields each
    // entry as soon as the next merge step decides on the
    // winner.
    let mut count = 0usize;
    for entry in db.scan_iter(None) {
        let (key, _value) = entry?;
        if count < 3 {
            println!("scan_iter[{count}] = {}", String::from_utf8_lossy(&key));
        }
        count += 1;
    }
    println!("scan_iter walked {count} live keys");

    // Prefix scan: only the requested namespace.
    let prefix_count = db
        .scan_iter(Some(b"user:00:"))
        .filter_map(Result::ok)
        .count();
    println!("scan_iter(prefix=\"user:00:\") yielded {prefix_count} keys");

    Ok(())
}
