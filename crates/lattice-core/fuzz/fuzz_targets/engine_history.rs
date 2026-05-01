//! Fuzz the public engine API with a random sequence of
//! operations.
//!
//! Each fuzz input is interpreted as a script: a stream of opcodes
//! drives `put`, `delete`, `flush`, `compact`, `snapshot`, `get`,
//! and `scan_iter` in arbitrary order on a fresh tempdir-backed
//! `Lattice`. The contract is that no legal sequence of public
//! method calls may panic. The fuzzer does not assert on the
//! returned values; it asserts on the absence of a process abort,
//! an out-of-bounds access, an integer overflow, or a panic from
//! anywhere in the engine or its dependencies.
//!
//! This is the cross-cutting fuzz target that complements the
//! per-decoder targets (`wal_open`, `sstable_open`,
//! `manifest_open`). Those exercise the parsers in isolation;
//! this exercises the operational surface of `Lattice` itself
//! against the kind of program the application code might write.
//!
//! Run with:
//!
//! ```text
//! cargo +nightly fuzz run engine_history
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::hint::black_box;
use tempfile::tempdir;

const MAX_OPS: usize = 256;
const MAX_KEY_OR_VALUE_LEN: usize = 64;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let Ok(dir) = tempdir() else {
        return;
    };
    let Ok(db) = lattice_core::Lattice::open(dir.path()) else {
        return;
    };

    let mut cursor = 0usize;
    let mut ops_done = 0usize;

    while cursor < data.len() && ops_done < MAX_OPS {
        let opcode = data[cursor];
        cursor += 1;
        ops_done += 1;

        match opcode % 8 {
            0 => {
                // put
                let (key, next) = take_bytes(data, cursor);
                cursor = next;
                let (value, next) = take_bytes(data, cursor);
                cursor = next;
                let _ = db.put(&key, &value);
            }
            1 => {
                // delete
                let (key, next) = take_bytes(data, cursor);
                cursor = next;
                let _ = db.delete(&key);
            }
            2 => {
                // get
                let (key, next) = take_bytes(data, cursor);
                cursor = next;
                let _ = db.get(&key);
            }
            3 => {
                // scan no-prefix; consume up to 64 entries
                for entry in db.scan_iter(None).take(64) {
                    let _ = black_box(entry);
                }
            }
            4 => {
                // scan with prefix
                let (prefix, next) = take_bytes(data, cursor);
                cursor = next;
                for entry in db.scan_iter(Some(&prefix)).take(64) {
                    let _ = black_box(entry);
                }
            }
            5 => {
                // flush
                let _ = db.flush();
            }
            6 => {
                // snapshot + get one key from it
                let snap = db.snapshot();
                let (key, next) = take_bytes(data, cursor);
                cursor = next;
                let _ = snap.get(&key);
            }
            _ => {
                // schedule async compact, drop the handle
                let _ = db.compact_async();
            }
        }
    }
});

/// Pull a length-prefixed byte slice out of `data` starting at
/// `cursor`. The length byte is masked to `MAX_KEY_OR_VALUE_LEN`
/// so the fuzzer cannot starve itself on a single huge value, and
/// the slice is clipped to whatever bytes are actually available.
fn take_bytes(data: &[u8], cursor: usize) -> (Vec<u8>, usize) {
    if cursor >= data.len() {
        return (Vec::new(), cursor);
    }
    let raw_len = data[cursor] as usize;
    let len = (raw_len % (MAX_KEY_OR_VALUE_LEN + 1)).min(data.len().saturating_sub(cursor + 1));
    let start = cursor + 1;
    let end = start + len;
    (data[start..end].to_vec(), end)
}
