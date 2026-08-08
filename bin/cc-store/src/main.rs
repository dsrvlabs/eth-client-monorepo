//! `cc-store` — offline store tool (CC-4J).
//!
//! ```text
//! cc-store verify  --path <dir>
//! cc-store compact --path <dir>
//! cc-store dump    --path <dir> --table <name> --key <hex>
//! ```
//!
//! Opens a store the node is **not** running. There is no `repair` subcommand.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

use cc_store_tool::{compact, dump, verify};

#[derive(Debug, Parser)]
#[command(
    name = "cc-store",
    version,
    about = "Offline store tool: verify, compact, dump (CC-4J)",
    long_about = "Companion to a corruption report. Runs against a store copied from \
a stopped node.\n\n\
verify  — eight §2.7 invariants, read-only; refuses a live lock\n\
compact — on-demand engine compaction; prints before/after file lengths\n\
dump    — one key as length + hex prefix (no consensus decode)\n\n\
There is no repair subcommand: Phase 4 never rewrites a store to make an \
invariant true."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run all eight CC-4H store invariants (read-only).
    Verify {
        /// Data directory containing `store.redb`.
        #[arg(long)]
        path: PathBuf,
    },
    /// Compact `store.redb` and print before/after lengths.
    Compact {
        /// Data directory containing `store.redb`.
        #[arg(long)]
        path: PathBuf,
    },
    /// Print one opaque value as length + hex prefix (no SSZ decode).
    Dump {
        /// Data directory containing `store.redb`.
        #[arg(long)]
        path: PathBuf,
        /// Table name (e.g. `blocks_hot`).
        #[arg(long)]
        table: String,
        /// Key as hex (`0x` optional).
        #[arg(long)]
        key: String,
    },
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Verify { path } => verify(&path),
        Command::Compact { path } => compact(&path),
        Command::Dump { path, table, key } => dump(&path, &table, &key),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
