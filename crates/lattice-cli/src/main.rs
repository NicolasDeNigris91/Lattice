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
    /// Force a compaction pass. (Phase 4.)
    Compact,
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

fn run(cli: Cli) -> Result<ExitCode> {
    match cli.command {
        Command::Put { key, value } => {
            let db = Lattice::open(&cli.path).context("open database")?;
            db.put(key.as_bytes(), value.as_bytes())
                .context("put failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Get { key } => {
            let db = Lattice::open(&cli.path).context("open database")?;
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
            let db = Lattice::open(&cli.path).context("open database")?;
            db.delete(key.as_bytes()).context("delete failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Scan { prefix } => {
            let db = Lattice::open(&cli.path).context("open database")?;
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
            let db = Lattice::open(&cli.path).context("open database")?;
            db.compact().context("compact failed")?;
            Ok(ExitCode::SUCCESS)
        }
    }
}
