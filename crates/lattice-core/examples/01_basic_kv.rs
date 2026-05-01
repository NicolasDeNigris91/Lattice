//! Smallest possible example: open a database, write a few keys,
//! read them back. Demonstrates the durability-by-default story
//! (`put` returns only after the WAL is `fsync`-ed) and the
//! reopen path (the second `open` replays the WAL and finds
//! every committed write).
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 01_basic_kv
//! ```

use lattice_core::{Lattice, Result};
use tempfile::tempdir;

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");

    // First session: put three keys, observe them, then drop the
    // handle. Drop runs `Inner`'s destructor which joins the
    // background flusher and ensures every non-durable byte is
    // synced; here every put was durable so there is nothing
    // pending.
    {
        let db = Lattice::open(dir.path())?;
        db.put(b"alpha", b"first")?;
        db.put(b"bravo", b"second")?;
        db.put(b"charlie", b"third")?;

        assert_eq!(db.get(b"alpha")?.as_deref(), Some(b"first".as_slice()));
        println!("first session: wrote three keys, read alpha back as 'first'");
    }

    // Second session: reopen the same directory. The WAL replay
    // reconstructs the in-memory memtable; from the caller's
    // point of view the database picked up exactly where it left
    // off.
    let db = Lattice::open(dir.path())?;
    let alpha = db.get(b"alpha")?;
    let bravo = db.get(b"bravo")?;
    let charlie = db.get(b"charlie")?;

    println!(
        "reopen: alpha={:?} bravo={:?} charlie={:?}",
        alpha.as_deref().map(String::from_utf8_lossy),
        bravo.as_deref().map(String::from_utf8_lossy),
        charlie.as_deref().map(String::from_utf8_lossy),
    );

    Ok(())
}
