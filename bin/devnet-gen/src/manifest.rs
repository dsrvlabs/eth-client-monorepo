//! Reproducibility manifest (`manifest.json`).

use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::params::DevnetParams;

/// Full generator manifest written to `devnet/out/manifest.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// Generator git SHA (best-effort).
    pub generator_git_sha: String,
    /// Seed hex (no 0x).
    pub seed: String,
    /// Preset name.
    pub preset: String,
    /// Validator count.
    pub validator_count: u64,
    /// Slot count (blocks at 1..=slot_count).
    pub slot_count: u64,
    /// Seconds per slot.
    pub seconds_per_slot: u64,
    /// Gossip disparity ms.
    pub maximum_gossip_clock_disparity_ms: u64,
    /// BPO epoch 1.
    pub bpo_1_epoch: u64,
    /// BPO epoch 2.
    pub bpo_2_epoch: u64,
    /// Genesis validators root (0x-prefixed hex).
    pub genesis_validators_root: String,
    /// Head block root at the last generated slot (0x-prefixed hex).
    pub head_block_root: String,
    /// Per-slot blob counts for slots 1..=slot_count.
    pub blobs_per_block: Vec<u64>,
    /// Per-slot block roots (0x-prefixed hex), same length as `blobs_per_block`.
    pub block_roots: Vec<String>,
}

/// Expected half committed in-repo for dual-run comparison (subset of fields).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpectedManifest {
    /// Seed hex.
    pub seed: String,
    /// Slot count.
    pub slot_count: u64,
    /// Seconds per slot.
    pub seconds_per_slot: u64,
    /// BPO epochs.
    pub bpo_1_epoch: u64,
    /// BPO epoch 2.
    pub bpo_2_epoch: u64,
    /// Blob-count cycle (template; expanded lengths live in full manifest).
    pub blobs_per_block_cycle: Vec<u64>,
    /// Filled after first golden run — empty until generator produces values.
    #[serde(default)]
    pub genesis_validators_root: Option<String>,
    /// Head root after golden run.
    #[serde(default)]
    pub head_block_root: Option<String>,
}

impl Manifest {
    /// Build from params + generated roots.
    pub fn new(
        params: &DevnetParams,
        genesis_validators_root: [u8; 32],
        head_block_root: [u8; 32],
        blobs_per_block: Vec<u64>,
        block_roots: Vec<[u8; 32]>,
    ) -> Self {
        Self {
            generator_git_sha: git_sha(),
            seed: hex::encode(params.seed_bytes().unwrap_or([0u8; 32])),
            preset: params.preset.clone(),
            validator_count: params.validator_count,
            slot_count: params.slot_count,
            seconds_per_slot: params.seconds_per_slot,
            maximum_gossip_clock_disparity_ms: params.gossip_disparity_ms(),
            bpo_1_epoch: params.bpo_1_epoch,
            bpo_2_epoch: params.bpo_2_epoch,
            genesis_validators_root: format!("0x{}", hex::encode(genesis_validators_root)),
            head_block_root: format!("0x{}", hex::encode(head_block_root)),
            blobs_per_block,
            block_roots: block_roots
                .into_iter()
                .map(|r| format!("0x{}", hex::encode(r)))
                .collect(),
        }
    }

    /// Write pretty JSON.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(self).context("serialize manifest")?;
        fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
    }

    /// Load from JSON.
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str(&text).context("parse manifest.json")
    }

    /// Compare reproducibility-critical fields.
    pub fn roots_match(&self, other: &Self) -> bool {
        self.genesis_validators_root == other.genesis_validators_root
            && self.head_block_root == other.head_block_root
            && self.block_roots == other.block_roots
            && self.blobs_per_block == other.blobs_per_block
            && self.seed == other.seed
            && self.slot_count == other.slot_count
    }
}

impl ExpectedManifest {
    /// Build the committed expected half from params (roots filled later).
    pub fn from_params(params: &DevnetParams) -> Self {
        Self {
            seed: hex::encode(params.seed_bytes().unwrap_or([0u8; 32])),
            slot_count: params.slot_count,
            seconds_per_slot: params.seconds_per_slot,
            bpo_1_epoch: params.bpo_1_epoch,
            bpo_2_epoch: params.bpo_2_epoch,
            blobs_per_block_cycle: params.blobs_per_block_cycle.clone(),
            genesis_validators_root: None,
            head_block_root: None,
        }
    }

    /// Load committed expected half from JSON.
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str(&text).context("parse expected-manifest.json")
    }

    /// Cross-check structural fields against generator params (not roots).
    pub fn matches_params(&self, params: &DevnetParams) -> Result<()> {
        let seed = hex::encode(params.seed_bytes()?);
        if self.seed != seed {
            anyhow::bail!("expected seed {} vs params {}", self.seed, seed);
        }
        if self.slot_count != params.slot_count {
            anyhow::bail!(
                "expected slot_count {} vs params {}",
                self.slot_count,
                params.slot_count
            );
        }
        if self.seconds_per_slot != params.seconds_per_slot {
            anyhow::bail!(
                "expected seconds_per_slot {} vs params {}",
                self.seconds_per_slot,
                params.seconds_per_slot
            );
        }
        if self.bpo_1_epoch != params.bpo_1_epoch || self.bpo_2_epoch != params.bpo_2_epoch {
            anyhow::bail!("expected BPO epochs do not match params");
        }
        if self.blobs_per_block_cycle != params.blobs_per_block_cycle {
            anyhow::bail!("expected blobs_per_block_cycle does not match params");
        }
        Ok(())
    }

    /// Assert GVR / head match a full generate manifest when roots are filled.
    pub fn assert_roots_match(&self, manifest: &Manifest) -> Result<()> {
        if let Some(ref gvr) = self.genesis_validators_root
            && gvr != &manifest.genesis_validators_root
        {
            anyhow::bail!(
                "GVR drift: expected {gvr} got {}",
                manifest.genesis_validators_root
            );
        }
        if let Some(ref head) = self.head_block_root
            && head != &manifest.head_block_root
        {
            anyhow::bail!(
                "head root drift: expected {head} got {}",
                manifest.head_block_root
            );
        }
        Ok(())
    }

    /// Write committed expected file.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(path, bytes)?;
        Ok(())
    }
}

fn git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into())
}
