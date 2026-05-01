//! Fuzz the WAL decode path that runs at `Lattice::open`.
//!
//! Writes arbitrary bytes as `wal.log` in a fresh tempdir and asks
//! the engine to open it. The contract is that opening MUST NOT
//! panic for any input. A malformed WAL must surface as
//! `Err(Error::*)`, never as a process abort, an out-of-bounds
//! access, or an integer overflow. Run with:
//!
//! ```text
//! cargo +nightly fuzz run wal_open
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    let Ok(dir) = tempdir() else {
        return;
    };
    let wal_path = dir.path().join("wal.log");
    if std::fs::write(&wal_path, data).is_err() {
        return;
    }
    // The result is intentionally discarded: we are checking that
    // `open` does not panic, not that it succeeds.
    let _ = lattice_core::Lattice::open(dir.path());
});
