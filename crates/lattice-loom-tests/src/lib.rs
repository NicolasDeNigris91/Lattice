//! Empty placeholder. The loom suite lives in `tests/loom_conflict.rs`
//! and is gated behind `#[cfg(loom)]`, so default `cargo test`
//! produces an empty test binary while
//! `RUSTFLAGS="--cfg loom" cargo test -p lattice-loom-tests --release`
//! exercises the conflict tracker under every legal interleaving.
