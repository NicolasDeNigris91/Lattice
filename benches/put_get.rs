//! Throughput benchmarks for `lattice-core`.
//!
//! Real benchmarks land alongside Phase 5. This stub exists so the bench
//! target declared in `crates/lattice-core/Cargo.toml` builds cleanly from
//! day one.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_placeholder(c: &mut Criterion) {
    c.bench_function("placeholder", |b| b.iter(|| 1u64 + 1u64));
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
