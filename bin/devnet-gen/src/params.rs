//! Generator parameters loaded from `devnet/devnet.toml`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Default reference slot time used to scale gossip clock disparity (mainnet).
pub const REFERENCE_SECONDS_PER_SLOT: u64 = 12;
/// Default mainnet gossip clock disparity in milliseconds.
pub const REFERENCE_GOSSIP_DISPARITY_MS: u64 = 500;

/// Committed generator input (reproducibility root).
#[derive(Debug, Clone, Deserialize)]
pub struct DevnetParams {
    /// 32-byte seed as 64 hex chars (optional `0x` prefix).
    pub seed: String,
    /// Compile-time preset: `"minimal"` (default) or `"mainnet"`.
    #[serde(default = "default_preset")]
    pub preset: String,
    /// Active validator count at genesis.
    #[serde(default = "default_validator_count")]
    pub validator_count: u64,
    /// Number of post-genesis slots to generate (blocks at slots `1..=slot_count`).
    #[serde(default = "default_slot_count")]
    pub slot_count: u64,
    /// Slot duration in seconds (devnet default 3).
    #[serde(default = "default_seconds_per_slot")]
    pub seconds_per_slot: u64,
    /// Optional override for `MAXIMUM_GOSSIP_CLOCK_DISPARITY` (ms). When absent,
    /// scales as `500 * seconds_per_slot / 12`.
    pub maximum_gossip_clock_disparity_ms: Option<u64>,
    /// Genesis unix timestamp.
    #[serde(default = "default_genesis_time")]
    pub genesis_time: u64,
    /// First BPO epoch (`BLOB_SCHEDULE[0].EPOCH`).
    #[serde(default = "default_bpo_1_epoch")]
    pub bpo_1_epoch: u64,
    /// Max blobs at/after `bpo_1_epoch`.
    #[serde(default = "default_bpo_1_max_blobs")]
    pub bpo_1_max_blobs: u64,
    /// Second BPO epoch.
    #[serde(default = "default_bpo_2_epoch")]
    pub bpo_2_epoch: u64,
    /// Max blobs at/after `bpo_2_epoch`.
    #[serde(default = "default_bpo_2_max_blobs")]
    pub bpo_2_max_blobs: u64,
    /// Deterministic blob-count cycle (non-zero). Index by `(slot - 1) % len`.
    #[serde(default = "default_blobs_per_block_cycle")]
    pub blobs_per_block_cycle: Vec<u64>,
    /// Network config name written into `config.yaml`.
    #[serde(default = "default_config_name")]
    pub config_name: String,
    /// Output directory (default `devnet/out` relative to CWD).
    #[serde(default = "default_output_dir")]
    pub output_dir: PathBuf,
}

fn default_preset() -> String {
    "minimal".into()
}
fn default_validator_count() -> u64 {
    64
}
fn default_slot_count() -> u64 {
    512
}
fn default_seconds_per_slot() -> u64 {
    3
}
fn default_genesis_time() -> u64 {
    1_700_000_000
}
fn default_bpo_1_epoch() -> u64 {
    5
}
fn default_bpo_1_max_blobs() -> u64 {
    6
}
fn default_bpo_2_epoch() -> u64 {
    10
}
fn default_bpo_2_max_blobs() -> u64 {
    9
}
fn default_blobs_per_block_cycle() -> Vec<u64> {
    vec![1, 2, 3, 1, 2]
}
fn default_config_name() -> String {
    "cc-devnet".into()
}
fn default_output_dir() -> PathBuf {
    PathBuf::from("devnet/out")
}

impl DevnetParams {
    /// Load and validate a TOML parameter file.
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        let params: Self = toml::from_str(&text).context("parse devnet.toml")?;
        params.validate()?;
        Ok(params)
    }

    /// Construct params for unit tests with a short chain.
    pub fn for_test(slot_count: u64, seed: [u8; 32]) -> Self {
        Self {
            seed: hex::encode(seed),
            preset: "minimal".into(),
            validator_count: 64,
            slot_count,
            seconds_per_slot: 3,
            maximum_gossip_clock_disparity_ms: None,
            genesis_time: 1_700_000_000,
            bpo_1_epoch: 5,
            bpo_1_max_blobs: 6,
            bpo_2_epoch: 10,
            bpo_2_max_blobs: 9,
            blobs_per_block_cycle: vec![1, 2, 3, 1, 2],
            config_name: "cc-devnet-test".into(),
            output_dir: PathBuf::from("devnet/out"),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.slot_count == 0 {
            bail!("slot_count must be >= 1");
        }
        if self.validator_count < 32 {
            bail!("validator_count must be >= 32 (minimal SYNC_COMMITTEE_SIZE)");
        }
        if self.seconds_per_slot == 0 {
            bail!("seconds_per_slot must be > 0");
        }
        if self.bpo_2_epoch <= self.bpo_1_epoch {
            bail!("bpo_2_epoch must be > bpo_1_epoch");
        }
        if self.blobs_per_block_cycle.is_empty() {
            bail!("blobs_per_block_cycle must be non-empty");
        }
        if self.blobs_per_block_cycle.contains(&0) {
            bail!("blobs_per_block_cycle entries must be non-zero (R-4)");
        }
        match self.preset.as_str() {
            "minimal" | "mainnet" => {}
            other => bail!("unsupported preset {other:?} (use minimal|mainnet)"),
        }
        let _ = self.seed_bytes()?;
        Ok(())
    }

    /// Decode the 32-byte seed.
    pub fn seed_bytes(&self) -> Result<[u8; 32]> {
        let s = self.seed.trim();
        let s = s.strip_prefix("0x").unwrap_or(s);
        let bytes = hex::decode(s).context("seed hex")?;
        if bytes.len() != 32 {
            bail!("seed must be 32 bytes (got {})", bytes.len());
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(out)
    }

    /// Scaled (or overridden) gossip clock disparity in milliseconds.
    pub fn gossip_disparity_ms(&self) -> u64 {
        if let Some(ms) = self.maximum_gossip_clock_disparity_ms {
            return ms;
        }
        // 500 ms * (slot_time / 12 s), integer ceil at least 1.
        let scaled = REFERENCE_GOSSIP_DISPARITY_MS
            .saturating_mul(self.seconds_per_slot)
            .div_ceil(REFERENCE_SECONDS_PER_SLOT);
        scaled.max(1)
    }

    /// Blob count for a block at `slot` (post-genesis slots start at 1).
    pub fn blobs_for_slot(&self, slot: u64) -> u64 {
        let cycle = &self.blobs_per_block_cycle;
        let idx = ((slot.saturating_sub(1)) as usize) % cycle.len();
        cycle[idx]
    }

    /// Cap blob count by the BPO schedule at `epoch` (pre-schedule uses 9).
    pub fn capped_blobs_for_epoch(&self, epoch: u64, requested: u64) -> u64 {
        let max = if epoch >= self.bpo_2_epoch {
            self.bpo_2_max_blobs
        } else if epoch >= self.bpo_1_epoch {
            self.bpo_1_max_blobs
        } else {
            9 // Electra base
        };
        requested.min(max).max(1)
    }
}
