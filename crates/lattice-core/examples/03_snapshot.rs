//! Snapshot isolation: a `Snapshot` taken at some point in time
//! returns the database state as of that point, regardless of
//! writes performed by other handles afterwards. Useful for
//! consistent reads, exports, and replication backfills.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 03_snapshot
//! ```

use lattice_core::{Lattice, Result};
use tempfile::tempdir;

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");
    let db = Lattice::open(dir.path())?;

    db.put(b"version", b"1.0.0")?;
    db.put(b"feature_flag", b"off")?;

    // Take a snapshot at the v1.0.0 point in time.
    let snap = db.snapshot();

    // Subsequent writes through the parent handle do NOT affect
    // what the snapshot sees.
    db.put(b"version", b"1.1.0")?;
    db.put(b"feature_flag", b"on")?;
    db.put(b"new_key", b"hello")?;

    println!(
        "live db: version={:?} feature_flag={:?} new_key={:?}",
        db.get(b"version")?.as_deref().map(String::from_utf8_lossy),
        db.get(b"feature_flag")?
            .as_deref()
            .map(String::from_utf8_lossy),
        db.get(b"new_key")?.as_deref().map(String::from_utf8_lossy),
    );
    println!(
        "snapshot:  version={:?} feature_flag={:?} new_key={:?}",
        snap.get(b"version")?
            .as_deref()
            .map(String::from_utf8_lossy),
        snap.get(b"feature_flag")?
            .as_deref()
            .map(String::from_utf8_lossy),
        snap.get(b"new_key")?
            .as_deref()
            .map(String::from_utf8_lossy),
    );

    Ok(())
}
