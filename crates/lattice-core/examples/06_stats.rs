//! Operational introspection via `Lattice::stats`. Walks an
//! `Stats` snapshot through a flush + compaction cycle so a
//! reader can see how each field reacts to engine state
//! transitions.
//!
//! Useful as a copy-paste template for a metrics exporter that
//! polls the engine without installing a `metrics` recorder.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 06_stats
//! ```

use lattice_core::{Lattice, Result};
use tempfile::tempdir;

fn print_stats(label: &str, db: &Lattice) {
    let s = db.stats();
    println!(
        "{label:<22} memtable={memtable:>8} bytes  frozen={frozen:>5} bytes  \
         levels={levels:?}  sstables={sst:<2}  next_seq={next_seq}  pending={pending}",
        label = label,
        memtable = s.memtable_bytes,
        frozen = s.frozen_memtable_bytes,
        levels = s.level_sstables,
        sst = s.total_sstables(),
        next_seq = s.next_seq,
        pending = s.pending_writes,
    );
}

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");
    let db = Lattice::open(dir.path())?;

    print_stats("after open:", &db);

    for i in 0..200u32 {
        let key = format!("k{i:04}");
        db.put(key.as_bytes(), b"payload-bytes")?;
    }
    print_stats("after 200 puts:", &db);

    db.flush()?;
    print_stats("after flush:", &db);

    // Push enough flushes to cross the auto-compaction threshold.
    for batch in 1..6u32 {
        for i in 0..100u32 {
            let key = format!("b{batch:02}-{i:04}");
            db.put(key.as_bytes(), b"payload-bytes")?;
        }
        db.flush()?;
    }
    print_stats("after 5 more flushes:", &db);

    // Force a full compaction; every level collapses to <=1 table.
    db.compact()?;
    print_stats("after compact:", &db);

    Ok(())
}
