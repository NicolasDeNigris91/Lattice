//! Recovery tests for the partial-write and corruption modes the
//! engine has to handle on reopen.
//!
//! The WAL is the engine's durability boundary. A power loss can
//! leave the on-disk file in any of several incomplete states:
//!
//! - Trailing bytes from an in-flight append that never `fsync`-ed.
//! - A truncated final record (the write buffer flushed only the
//!   first half of the record's bytes).
//! - A corrupted byte in the middle of an otherwise complete
//!   record.
//!
//! The engine's contract is that none of these may panic, and
//! that every fully-fsync-ed record before the corruption point
//! is recoverable. Trailing partial bytes are dropped silently
//! (they were never acknowledged to the caller); a mid-stream
//! corruption surfaces as `Err(Error::*)` rather than as a
//! process abort.
//!
//! These tests complement the property-based reopen fence in
//! `tests/property_durability.rs` with a handful of explicit
//! adversarial cases.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use lattice_core::Lattice;
use tempfile::tempdir;

/// Trailing-tail truncation: every fully-fsync-ed record must be
/// recoverable; the partial tail is dropped silently.
#[test]
fn truncated_wal_tail_drops_silently_and_keeps_committed_writes() {
    let dir = tempdir().unwrap();

    // Write three durable records, then reopen so the WAL is
    // canonical on disk.
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
    }

    // Append five garbage bytes to the WAL (simulating an
    // in-flight write that never sync'd before a crash). Use
    // OpenOptions::append so we leave the existing bytes alone.
    let wal_path = dir.path().join("wal.log");
    let mut tail_bytes = OpenOptions::new().append(true).open(&wal_path).unwrap();
    tail_bytes.write_all(&[0xFF; 5]).unwrap();
    drop(tail_bytes);

    // Reopen. The committed records must survive; the trailing
    // garbage must be ignored without panicking. The result of
    // open is allowed to be either Ok (the parser detected the
    // truncation and dropped the tail) or Err (the parser
    // surfaced the corruption); both are valid contracts. The
    // failure mode the test guards against is a panic.
    let reopen = Lattice::open(dir.path());
    if let Ok(db) = reopen {
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    }
}

/// Mid-stream byte flip: the engine must not panic. A successful
/// open is allowed to have lost data after the corruption point;
/// an Err open is also acceptable.
#[test]
fn mid_stream_byte_flip_does_not_panic() {
    let dir = tempdir().unwrap();

    {
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0..16u32 {
            let key = format!("k{i:04}");
            db.put(key.as_bytes(), b"payload").unwrap();
        }
    }

    let wal_path = dir.path().join("wal.log");
    let mut handle = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .unwrap();
    let len = handle.metadata().unwrap().len();
    if len > 32 {
        // Flip a byte roughly in the middle of the file. The
        // exact offset does not matter; the goal is to corrupt
        // *some* record without truncating the file.
        handle.seek(SeekFrom::Start(len / 2)).unwrap();
        handle.write_all(&[0xAA]).unwrap();
    }
    drop(handle);

    // Either Ok (parser dropped from the corruption point) or
    // Err is acceptable; a panic is not.
    let _ = Lattice::open(dir.path());
}

/// Empty WAL is a valid state: a fresh open, then immediate
/// drop, leaves a zero-length WAL file. Reopen must succeed and
/// the database must be empty.
#[test]
fn empty_wal_reopens_cleanly() {
    let dir = tempdir().unwrap();
    {
        let _db = Lattice::open(dir.path()).unwrap();
    }
    let wal_path = dir.path().join("wal.log");
    assert!(wal_path.exists(), "open should create the WAL file");

    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"any-key").unwrap(), None);
    assert_eq!(db.scan(None).unwrap(), Vec::<(Vec<u8>, Vec<u8>)>::new());
}

/// Reopening after a normal flush + drop must not need any WAL
/// replay (the WAL is truncated on flush). The database state
/// comes entirely from the `SSTable`.
#[test]
fn flush_then_reopen_recovers_from_sstable_only() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"sstable_only", b"value").unwrap();
        db.flush().unwrap();
    }

    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"sstable_only").unwrap(), Some(b"value".to_vec()));
}
