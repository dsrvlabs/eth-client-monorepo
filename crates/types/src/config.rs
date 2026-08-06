//! Runtime chain config and blob schedule (Architecture §2.3, CC-1G file half).
//!
//! `BlobSchedule` validates at construction: non-empty, sorted, strictly increasing epochs.
//! Pre-schedule blob-bound fallback lives solely in [`BlobSchedule::get_blob_parameters`] (§5.6).

use std::fs;
use std::path::Path;

use serde::Deserialize;

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
    /// specs: `(base_epoch, preset Electra max blobs)` where `base_epoch` is
    /// the network's `ELECTRA_FORK_EPOCH` (not genesis). Prefer
    /// [`ChainConfig::get_blob_parameters`], which supplies
    /// `electra_fork_epoch` automatically. Sole runtime site for the preset
    /// base max-blobs associated const lives in this fallback (§5.6).
    pub fn get_blob_parameters<P: Preset>(
        &self,
        epoch: Epoch,
        base_epoch: Epoch,
    ) -> BlobParameters {
        let idx = self
            .0
            .partition_point(|e| e.epoch.as_u64() <= epoch.as_u64());
        if idx == 0 {
            BlobParameters {
                epoch: base_epoch,
                max_blobs_per_block: P::MAX_BLOBS_PER_BLOCK_BASE,
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
    pub seconds_per_slot: u64,
    /// Validated blob parameter schedule.
    pub blob_schedule: BlobSchedule,
    /// Deposit contract chain id.
    pub deposit_chain_id: u64,
    /// Deposit contract address.
    pub deposit_contract_address: ExecutionAddress,
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
        let raw: RawChainConfig =
            serde_yaml::from_str(text).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        Self::try_from(raw)
    }

    /// Fulu `get_blob_parameters(epoch)` against this network's schedule.
    ///
    /// Pre-schedule fallback is
    /// `BlobParameters { epoch: self.electra_fork_epoch, max_blobs: Electra base }`
    /// matching consensus-specs
    /// `return BlobParameters(ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA)`.
    pub fn get_blob_parameters<P: Preset>(&self, epoch: Epoch) -> BlobParameters {
        self.blob_schedule
            .get_blob_parameters::<P>(epoch, self.electra_fork_epoch)
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
            seconds_per_slot: raw.seconds_per_slot,
            blob_schedule,
            deposit_chain_id: raw.deposit_chain_id,
            deposit_contract_address: execution_address(&raw.deposit_contract_address)?,
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
    seconds_per_slot: u64,
    #[serde(default)]
    blob_schedule: Vec<RawBlobParameters>,
    deposit_chain_id: u64,
    deposit_contract_address: String,
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

        // Before first entry → (ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA=9).
        let before = schedule.get_blob_parameters::<Mainnet>(Epoch::new(52_479), electra);
        assert_eq!(
            before,
            BlobParameters {
                epoch: electra,
                max_blobs_per_block: 9,
            }
        );

        // At / one before / one after first boundary — assert both fields.
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(52_480), electra),
            entry(52_480, 15)
        );
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(52_479), electra),
            BlobParameters {
                epoch: electra,
                max_blobs_per_block: 9,
            }
        );
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(52_481), electra),
            entry(52_480, 15)
        );

        // At / one before / one after second boundary.
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(54_016), electra),
            entry(54_016, 21)
        );
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(54_015), electra),
            entry(52_480, 15)
        );
        assert_eq!(
            schedule.get_blob_parameters::<Mainnet>(Epoch::new(54_017), electra),
            entry(54_016, 21)
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
}
