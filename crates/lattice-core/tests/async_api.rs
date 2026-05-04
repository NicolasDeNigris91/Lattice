//! Contract tests for the optional `tokio` async wrapper.
//!
//! These tests only build when the `tokio` feature is on. Run with
//! `cargo test -p lattice-core --features tokio --test async_api`.

#![cfg(feature = "tokio")]

use lattice_core::{AsyncLattice, Error, Lattice};
use tempfile::tempdir;

#[tokio::test]
async fn async_put_then_get_returns_value() {
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    db.put(b"k", b"v").await.unwrap();

    assert_eq!(db.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn async_delete_makes_get_return_none() {
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    db.put(b"k", b"v").await.unwrap();
    db.delete(b"k").await.unwrap();

    assert_eq!(db.get(b"k").await.unwrap(), None);
}

#[tokio::test]
async fn async_scan_returns_prefix_filtered_pairs() {
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    db.put(b"alpha", b"1").await.unwrap();
    db.put(b"alphabet", b"2").await.unwrap();
    db.put(b"beta", b"3").await.unwrap();

    let alphas = db.scan(Some(b"alpha")).await.unwrap();
    assert_eq!(
        alphas,
        vec![
            (b"alpha".to_vec(), b"1".to_vec()),
            (b"alphabet".to_vec(), b"2".to_vec()),
        ]
    );
}

#[tokio::test]
async fn async_transaction_commits_atomically() {
    // The async transaction wrapper runs the closure on tokio's
    // blocking pool. Reads observe the snapshot taken at start;
    // writes apply atomically on `Ok`. After commit, every staged
    // write is visible through the parent handle.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();
    db.put(b"existing", b"keep").await.unwrap();

    db.transaction(|tx| {
        assert_eq!(tx.get(b"existing").unwrap(), Some(b"keep".to_vec()));
        tx.put(b"a", b"1");
        tx.delete(b"existing");
        Ok::<_, Error>(())
    })
    .await
    .unwrap();

    assert_eq!(db.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"existing").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_concurrent_puts_from_many_tasks_all_persist() {
    // Multiple tokio tasks share the AsyncLattice handle (cheap
    // Arc clone) and put concurrently. The underlying engine
    // serialises through its WAL mutex; this test confirms the
    // async wrapper preserves the same end-state guarantee.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    let mut handles = Vec::new();
    for i in 0u32..32 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.put(&i.to_be_bytes(), &i.to_le_bytes()).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    for i in 0u32..32 {
        assert_eq!(
            db.get(&i.to_be_bytes()).await.unwrap(),
            Some(i.to_le_bytes().to_vec()),
            "key {i} missing after concurrent async puts"
        );
    }
}

#[tokio::test]
async fn async_byte_size_on_disk_grows_after_flush() {
    // The v1.18 inventory surface exposed a synchronous
    // `Lattice::byte_size_on_disk`. v1.22 lifts it into the
    // async wrapper so tokio callers do not need to drop to
    // `.sync()` for an inventory poll.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    let baseline = db.byte_size_on_disk().await.unwrap();
    for i in 0u32..50 {
        db.put(&i.to_be_bytes(), &[b'v'; 128]).await.unwrap();
    }
    db.flush().await.unwrap();
    let after_flush = db.byte_size_on_disk().await.unwrap();
    assert!(
        after_flush > baseline,
        "post-flush size should exceed empty baseline: {baseline} -> {after_flush}",
    );
}

#[tokio::test]
async fn async_checksum_matches_sync_handle() {
    // The async wrapper must hash the same byte stream as the
    // sync handle; the divergence-detection contract from v1.18
    // applies to AsyncLattice for free.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    db.put(b"alpha", b"1").await.unwrap();
    db.put(b"bravo", b"2").await.unwrap();
    db.delete(b"bravo").await.unwrap();
    db.flush().await.unwrap();

    let async_hash = db.checksum().await.unwrap();
    let sync_hash = db.sync().checksum().unwrap();
    assert_eq!(async_hash, sync_hash);
}

#[tokio::test]
async fn async_backup_to_produces_openable_directory() {
    // backup_to is heavy: copy SSTables, rewrite manifest,
    // replay memtables. The async wrapper must hand it to the
    // blocking pool so the calling task does not stall.
    let src_dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let db = AsyncLattice::open(src_dir.path()).await.unwrap();

    db.put(b"k", b"v").await.unwrap();
    db.flush().await.unwrap();
    db.put(b"only_in_memtable", b"transient").await.unwrap();

    db.backup_to(backup_dir.path()).await.unwrap();

    // Open the backup synchronously and verify the state.
    let restored = Lattice::open(backup_dir.path()).unwrap();
    assert_eq!(restored.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(
        restored.get(b"only_in_memtable").unwrap(),
        Some(b"transient".to_vec()),
    );
}

#[tokio::test]
async fn async_stats_and_config_are_cheap_inline_reads() {
    // `stats` and `config` are pure in-memory reads; the async
    // variants do not pay the cost of `spawn_blocking`. The
    // observable contract is just that the values match the
    // sync handle's at the moment of the call.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    let config = db.config();
    assert!(config.compaction_threshold > 0);
    assert!(config.flush_threshold_bytes > 0);

    let stats = db.stats();
    assert_eq!(stats.total_sstables(), 0);
    assert_eq!(stats.memtable_bytes, 0);

    db.put(b"k", b"v").await.unwrap();
    let stats_after = db.stats();
    assert!(stats_after.memtable_bytes > 0);

    // The async path() accessor returns the same path the
    // database was opened at.
    assert_eq!(db.path(), dir.path());
}

#[tokio::test]
async fn async_compact_async_returns_a_handle_that_can_be_awaited() {
    // compact_async does not need spawn_blocking: it just
    // bumps a generation and returns. The handle's wait()
    // does block, so it goes through spawn_blocking.
    let dir = tempdir().unwrap();
    let db = AsyncLattice::open(dir.path()).await.unwrap();

    db.put(b"k", b"v").await.unwrap();
    db.flush().await.unwrap();

    let handle = db.compact_async();
    db.wait_compact(handle).await.unwrap();
    assert_eq!(db.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}
