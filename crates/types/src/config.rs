//! Runtime chain config and blob schedule (Architecture §2.3, CC-1G file half).
//!
//! `BlobSchedule` validates at construction: non-empty, sorted, strictly increasing epochs.
//! Pre-schedule blob-bound fallback lives solely in [`BlobSchedule::get_blob_parameters`] (§5.6).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::preset::Preset;
use crate::primitives::{Epoch, ExecutionAddress, ForkVersion, HexParseError, parse_hex_bytes};

/// Which compile-time preset a runtime config is based on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PresetName {
    /// Mainnet preset (also Hoodi).
    Mainnet,
    /// Minimal preset.
    Minimal,
}

impl PresetName {
    /// Parse from `PRESET_BASE` string.
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "mainnet" => Ok(Self::Mainnet),
            "minimal" => Ok(Self::Minimal),
            other => Err(ConfigError::UnknownPresetBase(other.to_string())),
        }
    }

    /// Spec name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Minimal => "minimal",
        }
    }
}

/// One BPO / blob-parameter schedule entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobParameters {
    /// Epoch at which these parameters activate (inclusive).
    pub epoch: Epoch,
    /// Maximum blobs per block from this epoch until the next entry.
    pub max_blobs_per_block: u64,
}

/// Sorted, non-empty, strictly increasing-epoch blob schedule (Architecture §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobSchedule(Vec<BlobParameters>);

impl BlobSchedule {
    /// Validating constructor: rejects empty, unsorted, and duplicate-epoch schedules.
    pub fn try_from_entries(entries: Vec<BlobParameters>) -> Result<Self, BlobScheduleError> {
        if entries.is_empty() {
            return Err(BlobScheduleError::Empty);
        }
        for window in entries.windows(2) {
            let prev = window[0].epoch.as_u64();
            let next = window[1].epoch.as_u64();
            if next == prev {
                return Err(BlobScheduleError::DuplicateEpoch(window[0].epoch));
            }
            if next < prev {
                return Err(BlobScheduleError::Unsorted {
                    prev: window[0].epoch,
                    next: window[1].epoch,
                });
            }
        }
        Ok(Self(entries))
    }

    /// Borrow the ordered entries.
    pub fn entries(&self) -> &[BlobParameters] {
        &self.0
    }

    /// Binary search for the last entry with `entry.epoch <= epoch`.
    ///
    /// Before the first entry, returns Electra-era base parameters per Fulu
    /// specs: `(base_epoch, max_blobs_per_block)` where `base_epoch` is the
    /// network's `ELECTRA_FORK_EPOCH` (not genesis) and `max_blobs_per_block`
    /// is the caller's `MAX_BLOBS_PER_BLOCK_ELECTRA`. Prefer
    /// [`ChainConfig::get_blob_parameters`], which supplies both from config.
    pub fn get_blob_parameters(
        &self,
        epoch: Epoch,
        base_epoch: Epoch,
        max_blobs_per_block: u64,
    ) -> BlobParameters {
        let idx = self
            .0
            .partition_point(|e| e.epoch.as_u64() <= epoch.as_u64());
        if idx == 0 {
            BlobParameters {
                epoch: base_epoch,
                max_blobs_per_block,
            }
        } else {
            self.0[idx - 1]
        }
    }
}

impl TryFrom<Vec<BlobParameters>> for BlobSchedule {
    type Error = BlobScheduleError;

    fn try_from(value: Vec<BlobParameters>) -> Result<Self, Self::Error> {
        Self::try_from_entries(value)
    }
}

/// Typed errors from [`BlobSchedule`] construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobScheduleError {
    /// Schedule had no entries.
    #[error("blob schedule is empty")]
    Empty,
    /// Epochs were not strictly increasing.
    #[error("blob schedule is unsorted: epoch {prev} followed by {next}")]
    Unsorted {
        /// Previous entry epoch.
        prev: Epoch,
        /// Following entry epoch.
        next: Epoch,
    },
    /// Two entries shared the same epoch.
    #[error("blob schedule has duplicate epoch {0}")]
    DuplicateEpoch(Epoch),
}

/// Runtime chain configuration (fork schedule, timing, deposit, blob schedule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainConfig {
    /// Compile-time preset base (`mainnet` / `minimal`).
    pub preset_base: PresetName,
    /// Free-form network name (`mainnet`, `hoodi`, …).
    pub config_name: String,
    /// Genesis fork version.
    pub genesis_fork_version: ForkVersion,
    /// Altair fork version.
    pub altair_fork_version: ForkVersion,
    /// Altair activation epoch.
    pub altair_fork_epoch: Epoch,
    /// Bellatrix fork version.
    pub bellatrix_fork_version: ForkVersion,
    /// Bellatrix activation epoch.
    pub bellatrix_fork_epoch: Epoch,
    /// Capella fork version.
    pub capella_fork_version: ForkVersion,
    /// Capella activation epoch.
    pub capella_fork_epoch: Epoch,
    /// Deneb fork version.
    pub deneb_fork_version: ForkVersion,
    /// Deneb activation epoch.
    pub deneb_fork_epoch: Epoch,
    /// Electra fork version.
    pub electra_fork_version: ForkVersion,
    /// Electra activation epoch.
    pub electra_fork_epoch: Epoch,
    /// Fulu fork version.
    pub fulu_fork_version: ForkVersion,
    /// Fulu activation epoch.
    pub fulu_fork_epoch: Epoch,
    /// Slot duration in seconds.
    ///
    /// Resolved from `SECONDS_PER_SLOT`, else `SLOT_DURATION_MS / 1000`, else 12.
    pub seconds_per_slot: u64,
    /// Validated blob parameter schedule.
    pub blob_schedule: BlobSchedule,
    /// Deposit contract chain id.
    pub deposit_chain_id: u64,
    /// Deposit contract address.
    pub deposit_contract_address: ExecutionAddress,
    /// `CHURN_LIMIT_QUOTIENT` (mainnet 65536).
    pub churn_limit_quotient: u64,
    /// `MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA` in Gwei (mainnet 128000000000).
    pub min_per_epoch_churn_limit_electra: u64,
    /// `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT` in Gwei (mainnet 256000000000).
    pub max_per_epoch_activation_exit_churn_limit: u64,
    /// `SHARD_COMMITTEE_PERIOD` (mainnet 256 epochs).
    pub shard_committee_period: Epoch,
    /// `MAX_BLOBS_PER_BLOCK_ELECTRA` (mainnet 9). Pre-BPO fallback bound.
    pub max_blobs_per_block_electra: u64,
}

impl ChainConfig {
    /// Load and validate a consensus-specs style YAML config from `path`.
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_yaml_str(&text)
    }

    /// Parse and validate YAML text (shipping parse path).
    pub fn from_yaml_str(text: &str) -> Result<Self, ConfigError> {
        warn_unknown_yaml_keys(text)?;
        let raw: RawChainConfig =
            serde_yaml::from_str(text).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        Self::try_from(raw)
    }

    /// Fulu `get_blob_parameters(epoch)` against this network's schedule.
    ///
    /// Pre-schedule fallback is
    /// `BlobParameters { epoch: self.electra_fork_epoch, max_blobs: self.max_blobs_per_block_electra }`
    /// matching consensus-specs
    /// `return BlobParameters(ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA)`.
    pub fn get_blob_parameters<P: Preset>(&self, epoch: Epoch) -> BlobParameters {
        self.blob_schedule.get_blob_parameters(
            epoch,
            self.electra_fork_epoch,
            self.max_blobs_per_block_electra,
        )
    }
}

impl TryFrom<RawChainConfig> for ChainConfig {
    type Error = ConfigError;

    fn try_from(raw: RawChainConfig) -> Result<Self, Self::Error> {
        let blob_entries = raw
            .blob_schedule
            .into_iter()
            .map(|e| {
                Ok(BlobParameters {
                    epoch: Epoch::new(e.epoch),
                    max_blobs_per_block: e.max_blobs_per_block,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;

        let blob_schedule = BlobSchedule::try_from_entries(blob_entries)?;

        Ok(Self {
            preset_base: PresetName::parse(&raw.preset_base)?,
            config_name: raw.config_name,
            genesis_fork_version: fork_version(&raw.genesis_fork_version)?,
            altair_fork_version: fork_version(&raw.altair_fork_version)?,
            altair_fork_epoch: Epoch::new(raw.altair_fork_epoch),
            bellatrix_fork_version: fork_version(&raw.bellatrix_fork_version)?,
            bellatrix_fork_epoch: Epoch::new(raw.bellatrix_fork_epoch),
            capella_fork_version: fork_version(&raw.capella_fork_version)?,
            capella_fork_epoch: Epoch::new(raw.capella_fork_epoch),
            deneb_fork_version: fork_version(&raw.deneb_fork_version)?,
            deneb_fork_epoch: Epoch::new(raw.deneb_fork_epoch),
            electra_fork_version: fork_version(&raw.electra_fork_version)?,
            electra_fork_epoch: Epoch::new(raw.electra_fork_epoch),
            fulu_fork_version: fork_version(&raw.fulu_fork_version)?,
            fulu_fork_epoch: Epoch::new(raw.fulu_fork_epoch),
            seconds_per_slot: resolve_seconds_per_slot(raw.seconds_per_slot, raw.slot_duration_ms)?,
            blob_schedule,
            deposit_chain_id: raw.deposit_chain_id,
            deposit_contract_address: execution_address(&raw.deposit_contract_address)?,
            churn_limit_quotient: raw.churn_limit_quotient,
            min_per_epoch_churn_limit_electra: raw.min_per_epoch_churn_limit_electra,
            max_per_epoch_activation_exit_churn_limit: raw
                .max_per_epoch_activation_exit_churn_limit,
            shard_committee_period: Epoch::new(raw.shard_committee_period),
            max_blobs_per_block_electra: raw.max_blobs_per_block_electra,
        })
    }
}

fn fork_version(s: &str) -> Result<ForkVersion, ConfigError> {
    let bytes = parse_hex_bytes::<4>(s).map_err(ConfigError::Hex)?;
    Ok(ForkVersion::from_array(bytes))
}

fn execution_address(s: &str) -> Result<ExecutionAddress, ConfigError> {
    let bytes = parse_hex_bytes::<20>(s).map_err(ConfigError::Hex)?;
    Ok(ExecutionAddress::from_array(bytes))
}

/// Config load / parse errors (fail-before-bind).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Filesystem error reading the config path.
    #[error("failed to read config {path}: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// YAML deserialization failed.
    #[error("yaml parse error: {0}")]
    Yaml(String),
    /// Hex field parse failed.
    #[error("hex parse error: {0}")]
    Hex(#[from] HexParseError),
    /// `PRESET_BASE` was not `mainnet` or `minimal`.
    #[error("unknown PRESET_BASE: {0}")]
    UnknownPresetBase(String),
    /// `BLOB_SCHEDULE` failed validation.
    #[error(transparent)]
    BlobSchedule(#[from] BlobScheduleError),
    /// `SECONDS_PER_SLOT` and `SLOT_DURATION_MS` disagree.
    #[error("SECONDS_PER_SLOT ({seconds}) and SLOT_DURATION_MS ({ms}) are inconsistent")]
    SlotDurationMismatch {
        /// `SECONDS_PER_SLOT` value.
        seconds: u64,
        /// `SLOT_DURATION_MS` value.
        ms: u64,
    },
    /// `SLOT_DURATION_MS` is below one second, so `SECONDS_PER_SLOT` cannot be derived.
    #[error("SLOT_DURATION_MS {0} is less than 1000")]
    InvalidSlotDurationMs(u64),
}

/// Serde shape matching consensus-specs / eth-clients YAML keys.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct RawChainConfig {
    preset_base: String,
    config_name: String,
    genesis_fork_version: String,
    altair_fork_version: String,
    altair_fork_epoch: u64,
    bellatrix_fork_version: String,
    bellatrix_fork_epoch: u64,
    capella_fork_version: String,
    capella_fork_epoch: u64,
    deneb_fork_version: String,
    deneb_fork_epoch: u64,
    electra_fork_version: String,
    electra_fork_epoch: u64,
    fulu_fork_version: String,
    fulu_fork_epoch: u64,
    #[serde(default)]
    seconds_per_slot: Option<u64>,
    /// Used when `SECONDS_PER_SLOT` is absent.
    #[serde(default)]
    slot_duration_ms: Option<u64>,
    #[serde(default)]
    blob_schedule: Vec<RawBlobParameters>,
    deposit_chain_id: u64,
    deposit_contract_address: String,
    #[serde(default = "default_churn_limit_quotient")]
    churn_limit_quotient: u64,
    #[serde(default = "default_min_per_epoch_churn_limit_electra")]
    min_per_epoch_churn_limit_electra: u64,
    #[serde(default = "default_max_per_epoch_activation_exit_churn_limit")]
    max_per_epoch_activation_exit_churn_limit: u64,
    #[serde(default = "default_shard_committee_period")]
    shard_committee_period: u64,
    #[serde(default = "default_max_blobs_per_block_electra")]
    max_blobs_per_block_electra: u64,
}

/// Serde names of [`RawChainConfig`] (`rename_all = "SCREAMING_SNAKE_CASE"`).
fn is_known_chain_config_key(key: &str) -> bool {
    matches!(
        key,
        "PRESET_BASE"
            | "CONFIG_NAME"
            | "GENESIS_FORK_VERSION"
            | "ALTAIR_FORK_VERSION"
            | "ALTAIR_FORK_EPOCH"
            | "BELLATRIX_FORK_VERSION"
            | "BELLATRIX_FORK_EPOCH"
            | "CAPELLA_FORK_VERSION"
            | "CAPELLA_FORK_EPOCH"
            | "DENEB_FORK_VERSION"
            | "DENEB_FORK_EPOCH"
            | "ELECTRA_FORK_VERSION"
            | "ELECTRA_FORK_EPOCH"
            | "FULU_FORK_VERSION"
            | "FULU_FORK_EPOCH"
            | "SECONDS_PER_SLOT"
            | "SLOT_DURATION_MS"
            | "BLOB_SCHEDULE"
            | "DEPOSIT_CHAIN_ID"
            | "DEPOSIT_CONTRACT_ADDRESS"
            | "CHURN_LIMIT_QUOTIENT"
            | "MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA"
            | "MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT"
            | "SHARD_COMMITTEE_PERIOD"
            | "MAX_BLOBS_PER_BLOCK_ELECTRA"
    )
}

/// Capture leftover keys and WARN each one. Not `#[serde(flatten)]` onto
/// `serde_yaml::Value`: serde's flatten `Content` buffer cannot hold the
/// `u128` mainnet `TERMINAL_TOTAL_DIFFICULTY`.
fn warn_unknown_yaml_keys(text: &str) -> Result<(), ConfigError> {
    let keys: BTreeMap<String, IgnoredAny> =
        serde_yaml::from_str(text).map_err(|e| ConfigError::Yaml(e.to_string()))?;
    for key in keys.keys() {
        if !is_known_chain_config_key(key) {
            tracing::warn!(key = %key, "unknown chain config key");
        }
    }
    Ok(())
}

/// Mainnet slot duration when neither YAML key is present.
const DEFAULT_SECONDS_PER_SLOT: u64 = 12;

/// `SECONDS_PER_SLOT`, else `SLOT_DURATION_MS / 1000`, else 12.
fn resolve_seconds_per_slot(
    seconds_per_slot: Option<u64>,
    slot_duration_ms: Option<u64>,
) -> Result<u64, ConfigError> {
    match (seconds_per_slot, slot_duration_ms) {
        (Some(seconds), Some(ms)) if seconds.checked_mul(1000) != Some(ms) => {
            Err(ConfigError::SlotDurationMismatch { seconds, ms })
        }
        (Some(seconds), _) => Ok(seconds),
        (None, Some(ms)) => {
            let derived = ms / 1000;
            if derived == 0 {
                Err(ConfigError::InvalidSlotDurationMs(ms))
            } else {
                Ok(derived)
            }
        }
        (None, None) => Ok(DEFAULT_SECONDS_PER_SLOT),
    }
}

/// Mainnet `CHURN_LIMIT_QUOTIENT` (`configs/mainnet.yaml`).
const fn default_churn_limit_quotient() -> u64 {
    65_536
}

/// Mainnet `MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA` (`configs/mainnet.yaml`).
const fn default_min_per_epoch_churn_limit_electra() -> u64 {
    128_000_000_000
}

/// Mainnet `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT` (`configs/mainnet.yaml`).
const fn default_max_per_epoch_activation_exit_churn_limit() -> u64 {
    256_000_000_000
}

/// Mainnet `SHARD_COMMITTEE_PERIOD` (`configs/mainnet.yaml`).
const fn default_shard_committee_period() -> u64 {
    256
}

/// Mainnet `MAX_BLOBS_PER_BLOCK_ELECTRA` (`configs/mainnet.yaml`).
const fn default_max_blobs_per_block_electra() -> u64 {
    9
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct RawBlobParameters {
    epoch: u64,
    max_blobs_per_block: u64,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::preset::Mainnet;

    fn entry(epoch: u64, max: u64) -> BlobParameters {
        BlobParameters {
            epoch: Epoch::new(epoch),
            max_blobs_per_block: max,
        }
    }

    #[test]
    fn blob_schedule_rejects_empty() {
        let err = BlobSchedule::try_from_entries(vec![]).unwrap_err();
        assert!(matches!(err, BlobScheduleError::Empty));
    }

    #[test]
    fn blob_schedule_rejects_unsorted() {
        let err = BlobSchedule::try_from_entries(vec![entry(100, 15), entry(50, 21)]).unwrap_err();
        assert!(matches!(err, BlobScheduleError::Unsorted { .. }));
    }

    #[test]
    fn blob_schedule_rejects_duplicate_epochs() {
        let err = BlobSchedule::try_from_entries(vec![entry(100, 15), entry(100, 21)]).unwrap_err();
        assert!(matches!(err, BlobScheduleError::DuplicateEpoch(_)));
    }

    #[test]
    fn get_blob_parameters_hoodi_boundaries() {
        // Hoodi BPO: 52480 → 15, 54016 → 21; ELECTRA_FORK_EPOCH = 2048.
        let electra = Epoch::new(2_048);
        let schedule = BlobSchedule::try_from_entries(vec![entry(52_480, 15), entry(54_016, 21)])
            .unwrap_or_else(|e| panic!("{e:?}"));

        // Before first entry → (ELECTRA_FORK_EPOCH, caller-supplied Electra max).
        let before = schedule.get_blob_parameters(Epoch::new(52_479), electra, 9);
        assert_eq!(
            before,
            BlobParameters {
                epoch: electra,
                max_blobs_per_block: 9,
            }
        );

        // At / one before / one after first boundary — assert both fields.
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(52_480), electra, 9),
            entry(52_480, 15)
        );
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(52_479), electra, 9),
            BlobParameters {
                epoch: electra,
                max_blobs_per_block: 9,
            }
        );
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(52_481), electra, 9),
            entry(52_480, 15)
        );

        // At / one before / one after second boundary.
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(54_016), electra, 9),
            entry(54_016, 21)
        );
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(54_015), electra, 9),
            entry(52_480, 15)
        );
        assert_eq!(
            schedule.get_blob_parameters(Epoch::new(54_017), electra, 9),
            entry(54_016, 21)
        );
        // Parsed Electra max is the fallback — not P::MAX_BLOBS_PER_BLOCK_BASE.
        assert_eq!(
            schedule
                .get_blob_parameters(Epoch::new(52_479), electra, 11)
                .max_blobs_per_block,
            11
        );
    }

    #[test]
    fn hoodi_and_mainnet_config_yaml_roundtrip() {
        let hoodi_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/hoodi-config.yaml"
        );
        let hoodi = ChainConfig::from_yaml_file(hoodi_path)
            .unwrap_or_else(|e| panic!("parse hoodi-config.yaml: {e}"));
        assert_eq!(hoodi.config_name, "hoodi");
        assert_eq!(hoodi.preset_base, PresetName::Mainnet);
        assert_eq!(hoodi.fulu_fork_epoch, Epoch::new(50_688));
        assert_eq!(hoodi.electra_fork_epoch, Epoch::new(2_048));
        assert_eq!(hoodi.blob_schedule.entries().len(), 2);
        assert_eq!(hoodi.blob_schedule.entries()[0].epoch, Epoch::new(52_480));
        assert_eq!(hoodi.blob_schedule.entries()[0].max_blobs_per_block, 15);
        assert_eq!(hoodi.blob_schedule.entries()[1].epoch, Epoch::new(54_016));
        assert_eq!(hoodi.blob_schedule.entries()[1].max_blobs_per_block, 21);
        assert_eq!(hoodi.seconds_per_slot, 12);
        assert_eq!(hoodi.deposit_chain_id, 560_048);
        assert_eq!(hoodi.churn_limit_quotient, 65_536);
        assert_eq!(hoodi.min_per_epoch_churn_limit_electra, 128_000_000_000);
        assert_eq!(
            hoodi.max_per_epoch_activation_exit_churn_limit,
            256_000_000_000
        );
        assert_eq!(hoodi.shard_committee_period, Epoch::new(256));
        assert_eq!(hoodi.max_blobs_per_block_electra, 9);

        let mainnet_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/mainnet-config.yaml"
        );
        let mainnet = ChainConfig::from_yaml_file(mainnet_path)
            .unwrap_or_else(|e| panic!("parse mainnet-config.yaml: {e}"));
        assert_eq!(mainnet.config_name, "mainnet");
        assert_eq!(mainnet.preset_base, PresetName::Mainnet);
        assert_eq!(mainnet.fulu_fork_epoch, Epoch::new(411_392));
        assert_eq!(mainnet.electra_fork_epoch, Epoch::new(364_032));
        assert_eq!(mainnet.blob_schedule.entries().len(), 2);
        assert_eq!(
            mainnet.blob_schedule.entries()[0].epoch,
            Epoch::new(412_672)
        );
        assert_eq!(mainnet.blob_schedule.entries()[0].max_blobs_per_block, 15);
        assert_eq!(
            mainnet.blob_schedule.entries()[1].epoch,
            Epoch::new(419_072)
        );
        assert_eq!(mainnet.blob_schedule.entries()[1].max_blobs_per_block, 21);
        assert_eq!(mainnet.deposit_chain_id, 1);
        assert_eq!(mainnet.churn_limit_quotient, 65_536);
        assert_eq!(mainnet.min_per_epoch_churn_limit_electra, 128_000_000_000);
        assert_eq!(
            mainnet.max_per_epoch_activation_exit_churn_limit,
            256_000_000_000
        );
        assert_eq!(mainnet.shard_committee_period, Epoch::new(256));
        assert_eq!(mainnet.max_blobs_per_block_electra, 9);

        // Shipping lookup path: both fields for pre-BPO / at-BPO (F1).
        // Hoodi Fulu window [50688, 52480): Electra base (2048, 9).
        assert_eq!(
            hoodi.get_blob_parameters::<Mainnet>(Epoch::new(50_688)),
            BlobParameters {
                epoch: Epoch::new(2_048),
                max_blobs_per_block: 9,
            }
        );
        assert_eq!(
            hoodi.get_blob_parameters::<Mainnet>(Epoch::new(51_000)),
            BlobParameters {
                epoch: Epoch::new(2_048),
                max_blobs_per_block: 9,
            }
        );
        assert_eq!(
            hoodi.get_blob_parameters::<Mainnet>(Epoch::new(52_480)),
            entry(52_480, 15)
        );
        assert_eq!(
            hoodi.get_blob_parameters::<Mainnet>(Epoch::new(54_016)),
            entry(54_016, 21)
        );

        // Mainnet pre-first BPO uses Electra epoch 364032.
        assert_eq!(
            mainnet.get_blob_parameters::<Mainnet>(Epoch::new(411_392)),
            BlobParameters {
                epoch: Epoch::new(364_032),
                max_blobs_per_block: 9,
            }
        );
        assert_eq!(
            mainnet.get_blob_parameters::<Mainnet>(Epoch::new(412_672)),
            entry(412_672, 15)
        );
    }

    fn minimal_yaml_body() -> &'static str {
        r#"
PRESET_BASE: mainnet
CONFIG_NAME: defaults
GENESIS_FORK_VERSION: 0x00000000
ALTAIR_FORK_VERSION: 0x01000000
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_VERSION: 0x02000000
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_VERSION: 0x03000000
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_VERSION: 0x04000000
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_VERSION: 0x05000000
ELECTRA_FORK_EPOCH: 0
FULU_FORK_VERSION: 0x06000000
FULU_FORK_EPOCH: 0
SECONDS_PER_SLOT: 12
DEPOSIT_CHAIN_ID: 1
DEPOSIT_CONTRACT_ADDRESS: 0x00000000219ab540356cBB839Cbe05303d7705Fa
BLOB_SCHEDULE:
  - EPOCH: 100
    MAX_BLOBS_PER_BLOCK: 15
"#
    }

    fn yaml_without_seconds_per_slot() -> String {
        minimal_yaml_body()
            .lines()
            .filter(|line| !line.starts_with("SECONDS_PER_SLOT:"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn slot_duration_ms_only_derives_seconds_per_slot() {
        let yaml = format!(
            "{}\nSLOT_DURATION_MS: 12000\n",
            yaml_without_seconds_per_slot()
        );
        let cfg = ChainConfig::from_yaml_str(&yaml)
            .unwrap_or_else(|e| panic!("SLOT_DURATION_MS-only must parse: {e}"));
        assert_eq!(cfg.seconds_per_slot, 12);
    }

    #[test]
    fn minimal_slot_duration_ms_derives_six_seconds() {
        let yaml = format!(
            "{}\nSLOT_DURATION_MS: 6000\n",
            yaml_without_seconds_per_slot()
        );
        let cfg = ChainConfig::from_yaml_str(&yaml)
            .unwrap_or_else(|e| panic!("minimal SLOT_DURATION_MS must parse: {e}"));
        assert_eq!(cfg.seconds_per_slot, 6);
    }

    #[test]
    fn omitted_slot_keys_default_to_twelve_seconds() {
        let cfg = ChainConfig::from_yaml_str(&yaml_without_seconds_per_slot())
            .unwrap_or_else(|e| panic!("neither slot key must parse: {e}"));
        assert_eq!(cfg.seconds_per_slot, DEFAULT_SECONDS_PER_SLOT);
    }

    #[test]
    fn agreeing_slot_keys_keep_seconds() {
        let yaml = format!("{}\nSLOT_DURATION_MS: 12000\n", minimal_yaml_body());
        let cfg = ChainConfig::from_yaml_str(&yaml)
            .unwrap_or_else(|e| panic!("agreeing keys must parse: {e}"));
        assert_eq!(cfg.seconds_per_slot, 12);
    }

    #[test]
    fn disagreeing_slot_keys_fail() {
        let yaml = format!("{}\nSLOT_DURATION_MS: 6000\n", minimal_yaml_body());
        let err = ChainConfig::from_yaml_str(&yaml).expect_err("disagreeing keys must fail");
        assert!(
            matches!(
                err,
                ConfigError::SlotDurationMismatch {
                    seconds: 12,
                    ms: 6000
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn sub_second_slot_duration_ms_fails() {
        let yaml = format!(
            "{}\nSLOT_DURATION_MS: 500\n",
            yaml_without_seconds_per_slot()
        );
        let err = ChainConfig::from_yaml_str(&yaml).expect_err("sub-second ms must fail");
        assert!(
            matches!(err, ConfigError::InvalidSlotDurationMs(500)),
            "{err}"
        );
    }

    #[test]
    fn omitted_p002_keys_default_to_mainnet() {
        let cfg = ChainConfig::from_yaml_str(minimal_yaml_body())
            .unwrap_or_else(|e| panic!("omitted keys must still parse: {e}"));
        assert_eq!(cfg.churn_limit_quotient, default_churn_limit_quotient());
        assert_eq!(
            cfg.min_per_epoch_churn_limit_electra,
            default_min_per_epoch_churn_limit_electra()
        );
        assert_eq!(
            cfg.max_per_epoch_activation_exit_churn_limit,
            default_max_per_epoch_activation_exit_churn_limit()
        );
        assert_eq!(
            cfg.shard_committee_period,
            Epoch::new(default_shard_committee_period())
        );
        assert_eq!(
            cfg.max_blobs_per_block_electra,
            default_max_blobs_per_block_electra()
        );
        assert_eq!(
            cfg.get_blob_parameters::<Mainnet>(Epoch::new(0))
                .max_blobs_per_block,
            9
        );
    }

    #[test]
    fn parsed_p002_keys_override_mainnet_defaults() {
        let yaml = format!(
            "{}\nCHURN_LIMIT_QUOTIENT: 7\nMIN_PER_EPOCH_CHURN_LIMIT_ELECTRA: 11\n\
             MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT: 13\nSHARD_COMMITTEE_PERIOD: 17\n\
             MAX_BLOBS_PER_BLOCK_ELECTRA: 11\n",
            minimal_yaml_body()
        );
        let cfg = ChainConfig::from_yaml_str(&yaml)
            .unwrap_or_else(|e| panic!("override keys must parse: {e}"));
        assert_eq!(cfg.churn_limit_quotient, 7);
        assert_eq!(cfg.min_per_epoch_churn_limit_electra, 11);
        assert_eq!(cfg.max_per_epoch_activation_exit_churn_limit, 13);
        assert_eq!(cfg.shard_committee_period, Epoch::new(17));
        assert_eq!(cfg.max_blobs_per_block_electra, 11);
        assert_eq!(
            cfg.get_blob_parameters::<Mainnet>(Epoch::new(0)),
            BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 11,
            }
        );
        assert_eq!(
            cfg.get_blob_parameters::<Mainnet>(Epoch::new(100)),
            entry(100, 15)
        );
    }

    #[test]
    fn churn_limit_quotient_from_yaml_is_not_discarded() {
        let yaml = format!("{}\nCHURN_LIMIT_QUOTIENT: 7\n", minimal_yaml_body());
        let cfg = ChainConfig::from_yaml_str(&yaml)
            .unwrap_or_else(|e| panic!("CHURN_LIMIT_QUOTIENT must parse: {e}"));
        assert_eq!(cfg.churn_limit_quotient, 7);
    }

    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs<F, T>(f: F) -> (T, String)
    where
        F: FnOnce() -> T,
    {
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let make_writer = {
            let buf = Arc::clone(&buf);
            move || TestWriter(Arc::clone(&buf))
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(make_writer)
            .with_ansi(false)
            .with_level(true)
            .finish();
        let out = tracing::subscriber::with_default(subscriber, f);
        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        (out, logged)
    }

    #[test]
    fn unknown_key_load_succeeds_and_warns_once_naming_the_key() {
        let yaml = format!("{}\nHEZE_FORK_EPOCH: 1\n", minimal_yaml_body());
        let (result, logged) = capture_logs(|| ChainConfig::from_yaml_str(&yaml));
        let cfg = result.unwrap_or_else(|e| panic!("unknown key must not fail load: {e}"));
        assert_eq!(cfg.config_name, "defaults");
        let warn_lines: Vec<&str> = logged
            .lines()
            .filter(|line| line.contains("WARN"))
            .collect();
        assert_eq!(
            warn_lines.len(),
            1,
            "expected one WARN naming the unknown key; got:\n{logged}"
        );
        assert!(
            logged.contains("HEZE_FORK_EPOCH"),
            "WARN must name the unknown key; got:\n{logged}"
        );
        assert!(
            logged.contains("unknown chain config key"),
            "WARN must identify the event; got:\n{logged}"
        );
    }

    #[test]
    fn known_keys_emit_no_unknown_key_warn() {
        let (_cfg, logged) = capture_logs(|| {
            ChainConfig::from_yaml_str(minimal_yaml_body())
                .unwrap_or_else(|e| panic!("known keys must parse: {e}"))
        });
        assert!(
            !logged.contains("WARN"),
            "known-only YAML must not WARN; got:\n{logged}"
        );
    }

    #[test]
    fn slot_duration_ms_is_a_known_key() {
        let yaml = format!("{}\nSLOT_DURATION_MS: 12000\n", minimal_yaml_body());
        let (result, logged) = capture_logs(|| ChainConfig::from_yaml_str(&yaml));
        result.unwrap_or_else(|e| panic!("SLOT_DURATION_MS must parse: {e}"));
        assert!(
            !logged.contains("WARN"),
            "SLOT_DURATION_MS must not WARN as unknown; got:\n{logged}"
        );
    }

    #[test]
    fn unknown_u128_key_is_warned_not_rejected() {
        let yaml = format!(
            "{}\nTERMINAL_TOTAL_DIFFICULTY: 58750000000000000000000\n",
            minimal_yaml_body()
        );
        let (result, logged) = capture_logs(|| ChainConfig::from_yaml_str(&yaml));
        result.unwrap_or_else(|e| panic!("u128 unknown key must not fail load: {e}"));
        let warn_lines: Vec<&str> = logged
            .lines()
            .filter(|line| line.contains("WARN"))
            .collect();
        assert_eq!(
            warn_lines.len(),
            1,
            "expected one WARN for TTD; got:\n{logged}"
        );
        assert!(
            logged.contains("TERMINAL_TOTAL_DIFFICULTY"),
            "WARN must name the u128 key; got:\n{logged}"
        );
    }
}
