//! Self-devnet generator library (CC-2Ja).
//!
//! Build-time tool: genesis, config, keys, and a pre-generated signed chain
//! with valid data-column sidecars. Not linked by any service.

#![allow(missing_docs)]

pub mod chain;
pub mod config_emit;
pub mod genesis;
pub mod inclusion;
pub mod keys;
pub mod kzg_columns;
pub mod manifest;
pub mod params;

use std::path::Path;

use anyhow::{Context, Result};
use cc_types::config::ChainConfig;
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::Root;
use cc_types::BeaconState;

use crate::chain::generate_chain;
use crate::config_emit::{assert_devnet_config, write_config};
use crate::genesis::{build_genesis, encode_genesis_ssz, write_genesis_ssz};
use crate::keys::{derive_keys, write_keys};
use crate::kzg_columns::load_kzg;
use crate::manifest::Manifest;
use crate::params::DevnetParams;

/// Outcome of a generation run.
#[derive(Debug, Clone)]
pub struct GenerateResult {
    /// Parsed chain config.
    pub config: ChainConfig,
    /// Genesis validators root.
    pub genesis_validators_root: Root,
    /// Head block root at the last slot.
    pub head_block_root: Root,
    /// Manifest written to disk.
    pub manifest: Manifest,
    /// Per-slot blob counts.
    pub blobs_per_block: Vec<u64>,
    /// Per-slot block roots.
    pub block_roots: Vec<Root>,
}

/// Run the full generator into `params.output_dir` (or override).
pub fn generate(params: &DevnetParams, output_dir: Option<&Path>) -> Result<GenerateResult> {
    let out = output_dir.unwrap_or(params.output_dir.as_path());
    std::fs::create_dir_all(out).with_context(|| format!("mkdir {}", out.display()))?;

    match params.preset.as_str() {
        "minimal" => generate_with_preset::<Minimal>(params, out),
        "mainnet" => generate_with_preset::<Mainnet>(params, out),
        other => anyhow::bail!("unsupported preset {other}"),
    }
}

fn generate_with_preset<P: Preset>(
    params: &DevnetParams,
    out: &Path,
) -> Result<GenerateResult> {
    let seed = params.seed_bytes()?;
    let keys = derive_keys(&seed, params.validator_count)?;
    write_keys(&out.join("keys"), &keys)?;

    let config = write_config(&out.join("config.yaml"), params)?;
    assert_devnet_config(&config, params)?;

    let mut state = build_genesis::<P>(params, &keys, &config)?;
    let gvr = state.genesis_validators_root();
    write_genesis_ssz(&out.join("genesis.ssz"), &encode_genesis_ssz(&state))?;

    let kzg = load_kzg()?;
    let artifacts = generate_chain(&mut state, params, &keys, &config, &kzg, &out.join("chain"))?;

    let blobs_per_block: Vec<u64> = artifacts.iter().map(|a| a.blob_count).collect();
    let block_roots: Vec<Root> = artifacts.iter().map(|a| a.block_root).collect();
    let head = block_roots
        .last()
        .copied()
        .unwrap_or(Root::ZERO);

    let gvr_bytes = *gvr.as_array();
    let head_bytes = *head.as_array();
    let block_root_arrays: Vec<[u8; 32]> =
        block_roots.iter().map(|r| *r.as_array()).collect();

    let manifest = Manifest::new(
        params,
        gvr_bytes,
        head_bytes,
        blobs_per_block.clone(),
        block_root_arrays,
    );
    manifest.write(&out.join("manifest.json"))?;

    // Silence unused mut if we only needed genesis snapshot.
    let _: &BeaconState<P> = &state;

    Ok(GenerateResult {
        config,
        genesis_validators_root: gvr,
        head_block_root: head,
        manifest,
        blobs_per_block,
        block_roots,
    })
}
