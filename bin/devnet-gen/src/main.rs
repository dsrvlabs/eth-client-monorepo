//! `cc-devnet-gen` — self-devnet genesis / chain fixture generator (CC-2Ja).

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use cc_devnet_gen::params::DevnetParams;
use cc_devnet_gen::generate;

/// Generate genesis, config, keys, and a pre-signed block+sidecar chain.
#[derive(Debug, Parser)]
#[command(name = "cc-devnet-gen", version, about)]
struct Cli {
    /// Path to `devnet/devnet.toml`.
    #[arg(long, default_value = "devnet/devnet.toml")]
    config: PathBuf,

    /// Override output directory (defaults to `output_dir` in the TOML).
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let params = DevnetParams::from_toml_file(&cli.config)?;
    let out = cli.out.as_deref();
    let result = generate(&params, out)?;
    println!(
        "devnet-gen complete: slots={} gvr={} head={}",
        params.slot_count,
        result.manifest.genesis_validators_root,
        result.manifest.head_block_root
    );
    println!(
        "artifacts under {}",
        out.unwrap_or(params.output_dir.as_path()).display()
    );
    Ok(())
}
