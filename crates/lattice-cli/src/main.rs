//! Lattice command line interface.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "lattice", version, about = "LSM-tree key-value store")]
struct Cli {
    /// Path to the database directory.
    #[arg(long, default_value = "./data.lattice")]
    path: std::path::PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Insert or overwrite a value.
    Put { key: String, value: String },
    /// Read a value by key.
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
}

fn main() -> ExitCode {
    let _cli = Cli::parse();
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    anyhow::bail!("storage engine not wired yet, see roadmap in README")
}
