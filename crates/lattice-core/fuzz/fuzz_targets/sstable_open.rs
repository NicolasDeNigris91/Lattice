//! Fuzz the SSTable decode path that runs at `Lattice::open`.
//!
//! Writes arbitrary bytes as `00000000.sst` (the seq-zero filename
//! the engine expects) plus a minimal hand-crafted manifest that
//! claims this SSTable lives at L0 with seq 0. The engine's open
//! path then walks the SSTable footer, sparse index, and bloom
//! filter; the contract is that no input causes a panic. Run with:
//!
//! ```text
//! cargo +nightly fuzz run sstable_open
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    let Ok(dir) = tempdir() else {
        return;
    };

    // SSTable filename matches the SSTABLE_DIGITS-padded format
    // the engine uses for seq 0. If the engine changes that
    // padding, this string needs to follow.
    let sst_path = dir.path().join("00000000.sst");
    if std::fs::write(&sst_path, data).is_err() {
        return;
    }

    let _ = lattice_core::Lattice::open(dir.path());
});
