//! Integration tests for the v1.23 operational subcommands:
//! `stats`, `checksum`, `disk-size`, `backup`, and `flush`.
//!
//! Each test invokes the binary built by `cargo` (via the
//! `CARGO_BIN_EXE_lattice` environment variable that cargo
//! sets for crates with a `[[bin]]` target), points it at a
//! `tempfile::tempdir()`, and asserts on stdout / stderr /
//! exit status. Bytes-on-stdout contracts matter: operators
//! pipe these into shell scripts and other tooling.

use std::path::Path;
use std::process::{Command, Output};
use tempfile::tempdir;

const fn lattice_bin() -> &'static str {
    env!("CARGO_BIN_EXE_lattice")
}

fn run(path: &Path, args: &[&str]) -> Output {
    Command::new(lattice_bin())
        .arg("--path")
        .arg(path)
        .args(args)
        .output()
        .expect("failed to spawn lattice binary")
}

fn assert_success(output: &Output, hint: &str) {
    assert!(
        output.status.success(),
        "{hint}: exit {:?}, stderr = {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn stats_subcommand_prints_human_readable_summary() {
    let dir = tempdir().unwrap();
    // Seed some state.
    assert_success(&run(dir.path(), &["put", "k", "v"]), "put");
    assert_success(&run(dir.path(), &["flush"]), "flush");

    let output = run(dir.path(), &["stats"]);
    assert_success(&output, "stats");

    let stdout = String::from_utf8(output.stdout).unwrap();
    // Must mention each Stats field at least once. Operators
    // build alerts on these labels.
    for label in [
        "memtable_bytes",
        "frozen_memtable_bytes",
        "next_seq",
        "pending_writes",
        "level_sstables",
        "total_sstables",
    ] {
        assert!(
            stdout.contains(label),
            "stats output missing label `{label}`:\n{stdout}",
        );
    }
}

#[test]
fn checksum_subcommand_prints_hex_fingerprint_to_stdout() {
    let dir = tempdir().unwrap();
    assert_success(&run(dir.path(), &["put", "alpha", "1"]), "put alpha");
    assert_success(&run(dir.path(), &["put", "bravo", "2"]), "put bravo");

    let output = run(dir.path(), &["checksum"]);
    assert_success(&output, "checksum");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    // Hex-only output: scriptable by xargs / pipe / diff.
    assert_eq!(
        trimmed.len(),
        16,
        "checksum should be 16 hex chars: `{trimmed}`"
    );
    assert!(
        trimmed.chars().all(|c| c.is_ascii_hexdigit()),
        "checksum must be hex digits only: `{trimmed}`",
    );

    // Two databases with the same logical state must produce
    // the same hash. This is the cross-host divergence-
    // detection contract from v1.18 exposed at the CLI.
    let dir2 = tempdir().unwrap();
    assert_success(&run(dir2.path(), &["put", "alpha", "1"]), "put alpha");
    assert_success(&run(dir2.path(), &["put", "bravo", "2"]), "put bravo");
    let output2 = run(dir2.path(), &["checksum"]);
    let stdout2 = String::from_utf8(output2.stdout).unwrap();
    assert_eq!(
        trimmed,
        stdout2.trim(),
        "same logical state must hash equal"
    );
}

#[test]
fn disk_size_subcommand_prints_bytes_to_stdout() {
    let dir = tempdir().unwrap();
    assert_success(&run(dir.path(), &["put", "k", "v"]), "put");
    assert_success(&run(dir.path(), &["flush"]), "flush");

    let output = run(dir.path(), &["disk-size"]);
    assert_success(&output, "disk-size");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    let bytes: u64 = trimmed
        .parse()
        .unwrap_or_else(|_| panic!("disk-size output `{trimmed}` is not a u64"));
    assert!(
        bytes > 0,
        "non-empty database must have positive on-disk bytes"
    );
}

#[test]
fn flush_subcommand_drains_memtable_to_sstable() {
    let dir = tempdir().unwrap();
    assert_success(&run(dir.path(), &["put", "k", "v"]), "put");

    // Before flush, no SSTable exists yet.
    let stats_before = run(dir.path(), &["stats"]);
    assert_success(&stats_before, "stats before flush");
    let stdout_before = String::from_utf8(stats_before.stdout).unwrap();
    assert!(
        stdout_before.contains("total_sstables: 0"),
        "expected 0 SSTables before flush:\n{stdout_before}",
    );

    assert_success(&run(dir.path(), &["flush"]), "flush");

    let stats_after = run(dir.path(), &["stats"]);
    assert_success(&stats_after, "stats after flush");
    let stdout_after = String::from_utf8(stats_after.stdout).unwrap();
    assert!(
        stdout_after.contains("total_sstables: 1"),
        "expected 1 SSTable after flush:\n{stdout_after}",
    );
}

#[test]
fn backup_subcommand_produces_openable_directory() {
    let src = tempdir().unwrap();
    let backup = tempdir().unwrap();

    assert_success(&run(src.path(), &["put", "k", "v"]), "put");
    assert_success(&run(src.path(), &["flush"]), "flush");
    assert_success(
        &run(src.path(), &["put", "memtable_only", "x"]),
        "memtable put",
    );

    // backup-to <dest>
    let backup_arg = backup.path().to_str().unwrap();
    let output = run(src.path(), &["backup-to", backup_arg]);
    assert_success(&output, "backup-to");

    // Open the backup as a fresh database via the CLI and
    // verify reads.
    let restored_get = run(backup.path(), &["get", "k"]);
    assert_success(&restored_get, "get from backup");
    let value = String::from_utf8(restored_get.stdout).unwrap();
    assert_eq!(value.trim(), "v");

    let restored_memtable = run(backup.path(), &["get", "memtable_only"]);
    assert_success(&restored_memtable, "get memtable-only key from backup");
    let value2 = String::from_utf8(restored_memtable.stdout).unwrap();
    assert_eq!(value2.trim(), "x");
}
