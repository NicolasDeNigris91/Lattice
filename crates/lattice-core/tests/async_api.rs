//! Contract tests for the optional `tokio` async wrapper.
//!
//! These tests only build when the `tokio` feature is on. Run with
//! `cargo test -p lattice-core --features tokio --test async_api`.

#![cfg(feature = "tokio")]

use lattice_core::AsyncLattice;
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
