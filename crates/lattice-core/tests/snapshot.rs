//! Phase 5 integration tests for snapshot isolation.

use lattice_core::Lattice;
use tempfile::tempdir;

#[test]
fn snapshot_sees_state_at_creation() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap = db.snapshot();

    db.put(b"a", b"NEW").unwrap();
    db.put(b"c", b"3").unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(snap.get(b"c").unwrap(), None);

    // Live database shows the new state.
    assert_eq!(db.get(b"a").unwrap(), Some(b"NEW".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn snapshot_keeps_deleted_value_visible() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let snap = db.snapshot();
    db.delete(b"k").unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn snapshot_survives_flush() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap = db.snapshot();
    db.put(b"a", b"NEW").unwrap();
    db.flush().unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn snapshot_survives_compaction() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    // Mutate and compact: this changes the live SSTable set but the
    // snapshot's `Arc<SSTableReader>` instances stay alive.
    db.put(b"a", b"NEW").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(snap.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(snap.get(b"missing").unwrap(), None);

    // Live database reflects the new value.
    assert_eq!(db.get(b"a").unwrap(), Some(b"NEW".to_vec()));
}

#[test]
fn snapshot_scan_returns_frozen_pairs() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"apple", b"red").unwrap();
    db.put(b"banana", b"yellow").unwrap();

    let snap = db.snapshot();
    db.put(b"cherry", b"red").unwrap();
    db.delete(b"apple").unwrap();

    let pairs = snap.scan(None).unwrap();
    assert_eq!(
        pairs,
        vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
        ]
    );
}

#[test]
fn multiple_snapshots_are_independent() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v1").unwrap();

    let s1 = db.snapshot();
    db.put(b"k", b"v2").unwrap();
    let s2 = db.snapshot();
    db.put(b"k", b"v3").unwrap();

    assert_eq!(s1.get(b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s2.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn snapshot_serves_multi_block_reads_after_files_unlinked() {
    // Forces an SSTable big enough to contain multiple data blocks
    // (the writer flushes a block roughly every 4 KiB), takes a
    // snapshot, then triggers a compaction whose successful file
    // removal on POSIX unlinks the original file. The snapshot must
    // continue serving reads from its `Arc<SSTableReader>` because
    // that reader holds the file open, and on POSIX the inode lives
    // until the last fd closes. Pins the contract under file removal.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    // ~64 KiB of payload per block fills well past the 4 KiB target.
    let large_value = vec![b'x'; 256];
    for i in 0u32..2000 {
        db.put(i.to_be_bytes(), &large_value).unwrap();
    }
    db.flush().unwrap();

    let snap = db.snapshot();

    // Rewrite a few values and force a compaction: the old SSTable's
    // path is removed (POSIX unlink succeeds; Windows logs a warning
    // and defers to orphan cleanup, both fine for the snapshot).
    for i in 0u32..50 {
        db.put(i.to_be_bytes(), b"new").unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Snapshot still sees the original values from the (possibly
    // unlinked) file via its Arc'd reader, including keys that span
    // multiple data blocks.
    for i in [0u32, 100, 999, 1500, 1999] {
        assert_eq!(
            snap.get(i.to_be_bytes()).unwrap(),
            Some(large_value.clone()),
            "snapshot lost key {i}"
        );
    }

    // Live database shows the rewrite.
    assert_eq!(db.get(0u32.to_be_bytes()).unwrap(), Some(b"new".to_vec()));
}

#[test]
fn snapshot_can_be_cloned() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();

    let s1 = db.snapshot();
    let s2 = s1.clone();
    db.put(b"k", b"NEW").unwrap();

    assert_eq!(s1.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(s2.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn snapshot_scan_iter_streams_visible_pairs_in_key_order() {
    // v1.24 lifts the streaming scan iterator from `Lattice`
    // onto `Snapshot`. The contract is the same as
    // `Lattice::scan_iter`: yield visible `(key, value)`
    // pairs in ascending key order, optionally
    // prefix-filtered, frozen at snapshot time.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"alpha-01", b"1").unwrap();
    db.put(b"alpha-02", b"2").unwrap();
    db.put(b"beta-01", b"3").unwrap();
    db.flush().unwrap();
    db.put(b"alpha-03", b"4").unwrap();

    let snap = db.snapshot();

    // Mutate the source after the snapshot to confirm the
    // iterator is frozen.
    db.delete(b"alpha-01").unwrap();
    db.put(b"gamma-01", b"5").unwrap();

    let alphas: Vec<(Vec<u8>, Vec<u8>)> = snap
        .scan_iter(Some(b"alpha"))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        alphas,
        vec![
            (b"alpha-01".to_vec(), b"1".to_vec()),
            (b"alpha-02".to_vec(), b"2".to_vec()),
            (b"alpha-03".to_vec(), b"4".to_vec()),
        ],
    );

    let all: Vec<(Vec<u8>, Vec<u8>)> = snap.scan_iter(None).collect::<Result<_, _>>().unwrap();
    assert_eq!(all.len(), 4, "snapshot pinned 4 keys; got {all:?}");
}

#[test]
fn snapshot_scan_range_yields_inclusive_exclusive_window() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..10 {
        let key = format!("k{i:02}");
        db.put(key.as_bytes(), i.to_le_bytes()).unwrap();
    }

    let snap = db.snapshot();

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = snap
        .scan_range(Some(b"k03"), Some(b"k07"))
        .collect::<Result<_, _>>()
        .unwrap();
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![
            b"k03".to_vec(),
            b"k04".to_vec(),
            b"k05".to_vec(),
            b"k06".to_vec(),
        ],
        "scan_range is inclusive-exclusive: k03..k07 should yield k03..k06",
    );
}

#[test]
fn snapshot_checksum_matches_lattice_checksum_at_snapshot_time() {
    // v1.24 mirrors the v1.18 checksum onto Snapshot. The
    // pinned contract is: snapshot.checksum() == lattice.checksum()
    // captured at the moment of `db.snapshot()`, regardless of
    // subsequent live mutations. This is the temporal
    // counterpart of the cross-host divergence-detection
    // contract: same logical state at time T produces the same
    // hash, even if the live tree has moved on by the time the
    // hash is computed.
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..20 {
        let key = format!("k{i:02}");
        db.put(key.as_bytes(), format!("v{i:02}").as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    let live_at_t = db.checksum().unwrap();
    let snap = db.snapshot();
    assert_eq!(snap.checksum().unwrap(), live_at_t);

    // After mutating the live tree, the snapshot's checksum
    // must stay frozen. The live tree's checksum diverges.
    db.put(b"k00", b"changed").unwrap();
    db.delete(b"k01").unwrap();
    db.put(b"new", b"x").unwrap();

    assert_eq!(
        snap.checksum().unwrap(),
        live_at_t,
        "snapshot checksum must be frozen at snapshot time",
    );
    assert_ne!(
        db.checksum().unwrap(),
        live_at_t,
        "live tree checksum must reflect post-snapshot mutations",
    );
}

#[test]
fn snapshot_byte_size_on_disk_reports_pinned_sstable_bytes() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0u32..30 {
        let key = format!("k{i:02}");
        db.put(key.as_bytes(), [b'v'; 256]).unwrap();
    }
    db.flush().unwrap();

    let snap = db.snapshot();
    let size = snap.byte_size_on_disk();

    assert!(
        size > 0,
        "snapshot pinning at least one SSTable should report positive bytes",
    );
}
