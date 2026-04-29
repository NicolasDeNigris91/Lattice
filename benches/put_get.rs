//! Throughput benchmarks for `lattice-core`.
//!
//! Run with `cargo bench -p lattice-core`. Reports land at
//! `target/criterion/report/index.html`.

use std::hint::black_box;

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
                    db.put(&key(i), b"value-bytes").unwrap();
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
                    db.put_with(&key(i), b"value-bytes", opts).unwrap();
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
        db.put(&key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("random_read_hits_10k", |b| {
        b.iter(|| {
            // Pseudo-random walk through the keyspace using a fixed
            // prime stride so probes do not hit the same page run.
            let mut i = 0usize;
            for _ in 0..N {
                i = (i + 7919) % N;
                let v = db.get(&key(i)).unwrap();
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
        db.put(&key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("random_read_misses_10k", |b| {
        b.iter(|| {
            let mut i = N; // all keys here are above the populated range
            for _ in 0..N {
                i = i.saturating_add(1);
                let v = db.get(&key(i)).unwrap();
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
        db.put(&key(i), b"value-bytes").unwrap();
    }
    db.flush().unwrap();

    c.bench_function("scan_all_10k", |b| {
        b.iter(|| {
            let pairs = db.scan(None).unwrap();
            black_box(pairs);
        });
    });
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_sequential_write_amortized,
    bench_random_read_hits,
    bench_random_read_misses,
    bench_scan_all,
);
criterion_main!(benches);
