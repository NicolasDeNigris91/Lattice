//! Fuzz the manifest decode path.
//!
//! Writes arbitrary bytes as `MANIFEST` and asks the engine to
//! open the directory. The manifest decoder runs first and must
//! reject malformed input cleanly (returning `Err(Error::*)`),
//! never panic. Run with:
//!
//! ```text
//! cargo +nightly fuzz run manifest_open
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    let Ok(dir) = tempdir() else {
        return;
    };
    let manifest_path = dir.path().join("MANIFEST");
    if std::fs::write(&manifest_path, data).is_err() {
        return;
    }
    let _ = lattice_core::Lattice::open(dir.path());
});
