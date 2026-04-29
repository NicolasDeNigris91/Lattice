//! Phase 4 integration tests for compaction, manifest, and crash
//! recovery.

use std::fs;

use lattice_core::Lattice;
use tempfile::tempdir;

fn count_sst_files(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .count()
}

#[test]
fn manual_compact_collapses_multiple_sstables_into_one() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();

    assert_eq!(count_sst_files(dir.path()), 3);

    db.compact().unwrap();

    assert_eq!(count_sst_files(dir.path()), 1);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn compact_drops_tombstones_and_old_overrides() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"deleted", b"goneSoon").unwrap();
    db.flush().unwrap();
    db.put(b"a", b"2").unwrap();
    db.delete(b"deleted").unwrap();
    db.flush().unwrap();

    db.compact().unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"deleted").unwrap(), None);

    // After compaction, the new SSTable is the bottom of the LSM, so
    // the tombstone for `deleted` is gone (no older value to shadow).
    // We do not have a public API to count entries, but we can confirm
    // everything still resolves correctly after reopen.
    drop(db);
    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"deleted").unwrap(), None);
}

#[test]
fn auto_compaction_at_threshold() {
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .compaction_threshold(3)
        .open()
        .unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    // Third flush triggers auto-compaction inside `flush`.
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();

    assert_eq!(count_sst_files(dir.path()), 1);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn compaction_state_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0u32..10 {
            db.put(&i.to_be_bytes(), format!("v{i}").as_bytes())
                .unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();
    }

    let db = Lattice::open(dir.path()).unwrap();
    assert_eq!(count_sst_files(dir.path()), 1);
    for i in 0u32..10 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }
}

#[test]
fn manifest_is_created_on_open_when_missing() {
    let dir = tempdir().unwrap();
    let _ = Lattice::open(dir.path()).unwrap();
    assert!(dir.path().join("MANIFEST").is_file());
}

#[test]
fn orphan_sstable_is_deleted_on_open() {
    let dir = tempdir().unwrap();
    {
        let db = Lattice::open(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
    }
    // Drop a hand-crafted bogus SSTable seq into the directory. The
    // manifest does not list it, so the next open should clean it.
    let orphan = dir.path().join("999999.sst");
    fs::write(&orphan, b"not a real sstable").unwrap();
    assert!(orphan.exists());

    let _db = Lattice::open(dir.path()).unwrap();
    assert!(
        !orphan.exists(),
        "orphan SSTable should have been deleted on open"
    );
}

#[test]
fn compact_with_no_sstables_is_noop() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.compact().unwrap();
    assert_eq!(count_sst_files(dir.path()), 0);
}

#[test]
fn compact_with_one_sstable_is_noop() {
    let dir = tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    let before = fs::metadata(dir.path().join("000001.sst")).unwrap().len();

    db.compact().unwrap();

    let after_path = dir.path().join("000001.sst");
    let after = fs::metadata(&after_path).unwrap().len();
    assert_eq!(before, after, "single-table compaction should be a no-op");
    assert_eq!(count_sst_files(dir.path()), 1);
}

#[test]
fn open_cleans_orphans_left_by_a_simulated_post_compact_crash() {
    // Reproduces "compaction wrote the manifest pointing at the new
    // SSTable but crashed before deleting the old SSTables". We build
    // three SSTables via flush, then hand-roll a manifest that
    // references only the middle one, then reopen. The orphan sweep
    // must delete the unreferenced files and leave the live one
    // intact and readable.
    use std::path::Path;

    /// Mirrors `crate::manifest::Manifest`. Bincode encodes structs
    /// by field order, so as long as the field order matches we get
    /// a byte-for-byte compatible payload without exposing the real
    /// type publicly.
    #[derive(bincode::Encode)]
    struct ManifestShape {
        version: u32,
        next_seq: u64,
        table_seqs: Vec<u64>,
    }

    fn write_manifest(dir: &Path, version: u32, next_seq: u64, table_seqs: Vec<u64>) {
        let payload = ManifestShape {
            version,
            next_seq,
            table_seqs,
        };
        let bytes = bincode::encode_to_vec(payload, bincode::config::standard()).unwrap();
        fs::write(dir.join("MANIFEST"), bytes).unwrap();
    }

    let dir = tempdir().unwrap();
    {
        let db = Lattice::builder(dir.path())
            .compaction_threshold(usize::MAX)
            .open()
            .unwrap();
        db.put(b"alpha", b"1").unwrap();
        db.flush().unwrap();
        db.put(b"beta", b"2").unwrap();
        db.flush().unwrap();
        db.put(b"gamma", b"3").unwrap();
        db.flush().unwrap();
    }
    assert_eq!(count_sst_files(dir.path()), 3);

    // Hand-roll a manifest that pretends a compact-then-crash
    // happened: only seq 2 is live; 1 and 3 are orphans.
    write_manifest(dir.path(), 1, 4, vec![2]);

    let db = Lattice::open(dir.path()).unwrap();

    assert_eq!(
        count_sst_files(dir.path()),
        1,
        "orphans should have been swept on open"
    );
    assert!(dir.path().join("000002.sst").is_file());

    // The live SSTable's contents must still resolve. Note: keys that
    // were only in the deleted SSTables are now gone; that is the
    // simulated "post-compact" state, and is not a regression.
    assert_eq!(db.get(b"beta").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn data_survives_cascading_leveled_compaction() {
    // With `compaction_threshold = 2`, every other flush triggers
    // an auto-compaction. Sixteen flushes therefore cascade through
    // several levels (L0 -> L1 -> L2 -> ...). The explicit
    // `compact()` at the end forces every level to collapse to a
    // single table, exercising the user-facing semantics. Every
    // key must remain readable across the cascade.
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .compaction_threshold(2)
        .open()
        .unwrap();

    for i in 0u32..16 {
        db.put(&i.to_be_bytes(), format!("v{i}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    db.compact().unwrap();

    for i in 0u32..16 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} missing after cascading compaction"
        );
    }
}

#[test]
fn many_flushes_then_compact_preserves_all_keys() {
    let dir = tempdir().unwrap();
    let db = Lattice::builder(dir.path())
        .compaction_threshold(usize::MAX) // disable auto-compaction
        .open()
        .unwrap();

    for i in 0u32..200 {
        db.put(&i.to_be_bytes(), format!("v{i}").as_bytes())
            .unwrap();
        if i % 10 == 9 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    let before = count_sst_files(dir.path());
    assert!(before > 1);

    db.compact().unwrap();
    assert_eq!(count_sst_files(dir.path()), 1);

    for i in 0u32..200 {
        assert_eq!(
            db.get(&i.to_be_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} missing after compact"
        );
    }
}
