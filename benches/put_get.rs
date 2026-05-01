//! Throughput benchmarks for `lattice-core`.
//!
//! Run with `cargo bench -p lattice-core`. Reports land at
//! `target/criterion/report/index.html`.

// `criterion_group!` and `criterion_main!` expand to `pub fn` items
// without doc comments. The workspace `missing_docs` lint would
// otherwise fire on every bench file.
#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lattice_core::{Lattice, WriteOptions};
use tempfile::TempDir;

const N: usize = 10_000;

const fn key(i: usize) -> [u8; 8] {
    (i as u64).to_be_bytes()
}

fn fresh_db() -> (TempDir, Lattice) {
    let dir = tempfile::tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    (dir, db)
}

/// `N` sequential puts on a fresh database. The temp directory is
/// kept by the closure return so its (slow on Windows) recursive
/// removal happens outside the timed routine.
fn bench_sequential_write(c: &mut Criterion) {
    c.bench_function("sequential_write_10k", |b| {
        b.iter_batched(
            fresh_db,
            |(dir, db)| {
                for i in 0..N {
                    db.put(key(i), b"value-bytes").unwrap();
                }
                // Return both so Criterion drops them after stopping
                // the wall-clock timer, not inside the measurement.
                (dir, db)
            },
            BatchSize::PerIteration,
        );
    });
}

/// `N` sequential puts using `put_with(.., WriteOptions { durable:
/// false })`. Same workload as `sequential_write_10k` but the WAL
/// `fsync` is amortised across the batch threshold instead of being
/// paid per call. The pair makes the M1 group commit speedup
/// measurable in `cargo bench`.
fn bench_sequential_write_amortized(c: &mut Criterion) {
    c.bench_function("sequential_write_amortized_10k", |b| {
        b.iter_batched(
            fresh_db,
            |(dir, db)| {
                let opts = WriteOptions { durable: false };
                for i in 0..N {
                    db.put_with(key(i), b"value-bytes", opts).unwrap();
                }
                // Force the final sync so the measured cost includes
                // the durability commitment, not just the buffered
                // writes.
                db.flush_wal().unwrap();
                (dir, db)
            },
            BatchSize::PerIteration,
        );
    });
}

/// `N` random reads against a database with `N` keys, all hits.
fn bench_random_read_hits(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0..N {
        db.put(key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("random_read_hits_10k", |b| {
        b.iter(|| {
            // Pseudo-random walk through the keyspace using a fixed
            // prime stride so probes do not hit the same page run.
            let mut i = 0usize;
            for _ in 0..N {
                i = (i + 7919) % N;
                let v = db.get(key(i)).unwrap();
                black_box(v);
            }
        });
    });
}

/// `N` reads against a database with `N` keys, every probe is a miss.
/// Measures the bloom filter short-circuit on the read path.
fn bench_random_read_misses(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0..N {
        db.put(key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("random_read_misses_10k", |b| {
        b.iter(|| {
            let mut i = N; // all keys here are above the populated range
            for _ in 0..N {
                i = i.saturating_add(1);
                let v = db.get(key(i)).unwrap();
                black_box(v);
            }
        });
    });
}

/// Full scan of a database with `N` keys.
fn bench_scan_all(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = Lattice::open(dir.path()).unwrap();
    for i in 0..N {
        db.put(key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("scan_all_10k", |b| {
        b.iter(|| {
            let pairs = db.scan(None).unwrap();
            black_box(pairs);
        });
    });
}

/// Throughput of `get` under concurrent readers sharing one
/// `Lattice` handle (cloned `Arc<Inner>`). Measures the wall-clock
/// time for `THREADS` threads to each issue `READS_PER_THREAD`
/// random hits, after a fresh database has been populated with `N`
/// keys. Pairs with `random_read_hits_10k` (single-threaded
/// baseline) to make the M2 concurrency speedup visible. The
/// reported time is per iteration of all threads taken together.
fn bench_concurrent_random_read_hits(c: &mut Criterion) {
    const READS_PER_THREAD: usize = N / 4;

    for threads in [1usize, 2, 4, 8] {
        let dir = tempfile::tempdir().unwrap();
        let db = Lattice::open(dir.path()).unwrap();
        for i in 0..N {
            db.put(key(i), b"value-bytes").unwrap();
        }
        db.flush().unwrap();

        let id = format!("concurrent_random_read_hits_{threads}t_{READS_PER_THREAD}r");
        c.bench_function(&id, |b| {
            b.iter(|| {
                let stop = Arc::new(AtomicBool::new(false));
                let mut handles = Vec::with_capacity(threads);
                for t in 0..threads {
                    let db = db.clone();
                    let stop = Arc::clone(&stop);
                    handles.push(thread::spawn(move || {
                        let mut i = t * 7919 % N;
                        for _ in 0..READS_PER_THREAD {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            i = (i + 7919) % N;
                            let v = db.get(key(i)).unwrap();
                            black_box(v);
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        // Hold onto resources until the bench iteration completes;
        // dropping inside `iter` would skew the reported time.
        drop(db);
        drop(dir);
    }
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_sequential_write_amortized,
    bench_random_read_hits,
    bench_random_read_misses,
    bench_scan_all,
    bench_concurrent_random_read_hits,
);
criterion_main!(benches);
