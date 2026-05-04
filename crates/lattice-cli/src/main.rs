//! Lattice command line interface.

#![forbid(unsafe_code)]

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use lattice_core::Lattice;

#[derive(Debug, Parser)]
#[command(name = "lattice", version, about = "LSM-tree key-value store")]
struct Cli {
    /// Path to the database directory.
    #[arg(long, default_value = "./data.lattice")]
    path: PathBuf,

    /// Open the database in read-only mode (v1.25). Mutating
    /// subcommands (`put`, `delete`, `flush`, `compact`,
    /// `backup-to`) error with `read-only handle` when this
    /// flag is set; reading subcommands (`get`, `scan`,
    /// `stats`, `checksum`, `disk-size`) work normally.
    #[arg(long)]
    read_only: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Insert or overwrite a value.
    Put { key: String, value: String },
    /// Read a value by key. Exit code 1 if the key is absent.
    Get { key: String },
    /// Delete a key.
    Delete { key: String },
    /// Iterate keys, optionally filtered by prefix.
    Scan {
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Force a compaction pass.
    Compact,
    /// Force a memtable flush to a new on-disk `SSTable`.
    Flush,
    /// Print operational counters as `key: value` lines.
    Stats,
    /// Print the deterministic xxh3-64 fingerprint of the
    /// visible key/value set as 16 hex chars.
    Checksum,
    /// Print the on-disk byte footprint (live `SSTable`s + WAL).
    DiskSize,
    /// Copy this database into `dest` as a self-contained,
    /// openable directory.
    BackupTo { dest: PathBuf },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing();

    match run(cli) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(filter)
        .init();
}

fn open_db(path: &PathBuf, read_only: bool) -> Result<Lattice> {
    if read_only {
        Lattice::open_read_only(path).context("open database (read-only)")
    } else {
        Lattice::open(path).context("open database")
    }
}

fn run(cli: Cli) -> Result<ExitCode> {
    let Cli {
        path,
        read_only,
        command,
    } = cli;
    match command {
        Command::Put { key, value } => {
            let db = open_db(&path, read_only)?;
            db.put(key.as_bytes(), value.as_bytes())
                .context("put failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Get { key } => {
            let db = open_db(&path, read_only)?;
            match db.get(key.as_bytes()).context("get failed")? {
                Some(value) => {
                    let mut stdout = io::stdout().lock();
                    stdout.write_all(&value)?;
                    stdout.write_all(b"\n")?;
                    Ok(ExitCode::SUCCESS)
                }
                None => Ok(ExitCode::from(1)),
            }
        }
        Command::Delete { key } => {
            let db = open_db(&path, read_only)?;
            db.delete(key.as_bytes()).context("delete failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Scan { prefix } => {
            let db = open_db(&path, read_only)?;
            let pairs = db
                .scan(prefix.as_deref().map(str::as_bytes))
                .context("scan failed")?;
            let mut stdout = io::stdout().lock();
            for (key, value) in pairs {
                stdout.write_all(&key)?;
                stdout.write_all(b"\t")?;
                stdout.write_all(&value)?;
                stdout.write_all(b"\n")?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Compact => {
            let db = open_db(&path, read_only)?;
            db.compact().context("compact failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Flush => {
            let db = open_db(&path, read_only)?;
            db.flush().context("flush failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Stats => {
            let db = open_db(&path, read_only)?;
            let stats = db.stats();
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "memtable_bytes: {}", stats.memtable_bytes)?;
            writeln!(
                stdout,
                "frozen_memtable_bytes: {}",
                stats.frozen_memtable_bytes
            )?;
            writeln!(stdout, "next_seq: {}", stats.next_seq)?;
            writeln!(stdout, "pending_writes: {}", stats.pending_writes)?;
            writeln!(stdout, "level_sstables: {:?}", stats.level_sstables)?;
            writeln!(stdout, "total_sstables: {}", stats.total_sstables())?;
            writeln!(stdout, "level_count: {}", stats.level_count())?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Checksum => {
            let db = open_db(&path, read_only)?;
            let hash = db.checksum().context("checksum failed")?;
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "{hash:016x}")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::DiskSize => {
            let db = open_db(&path, read_only)?;
            let bytes = db.byte_size_on_disk().context("byte_size_on_disk failed")?;
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "{bytes}")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::BackupTo { dest } => {
            let db = open_db(&path, read_only)?;
            db.backup_to(&dest).context("backup_to failed")?;
            Ok(ExitCode::SUCCESS)
        }
    }
}
