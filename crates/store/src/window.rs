//! Block serve-window floor (Architecture §5.1 / CC-4A).
//!
//! `MIN_EPOCHS_FOR_BLOCK_REQUESTS` was removed from the consensus-specs configs
//! at `v1.7.0-alpha.13`. The authority is the computed function
//! [`compute_min_epochs_for_block_requests`]; any vestigial config field is a
//! **cross-check** only (startup refusal on mismatch).
//!
//! This module owns **only** the computed constant and the cross-check.
//! `CC-48` adds the derivation of `earliest_available_slot`; `CC-49` adds
//! the two-branch advertisement rule. Sequential ownership of this file is
//! named in all three issues.
//!
//! ## Security notes
//!
//! - **SEC-4A-1** — arithmetic is **fail-closed** (`checked_div` / `checked_add`);
//!   overflow never wraps to a silent wrong floor.
//! - **SEC-4A-2** (dual authority with Phase 2 `services/p2p`) — **wontfix here**.
//!   `services/p2p` still holds a mainnet/hoodi floor constant for Phase 2 serve
//!   handlers. Wiring a single authority is `CC-49` (Stream N / advertisement
//!   branch); this Stream S issue must not touch `services/p2p` beyond the
//!   `33024` grep hygiene already applied.

use std::fs;
use std::path::Path;

use serde::Deserialize;

/// Config scalars that enter the block serve-window floor.
///
/// Architecture §5.1 writes these as fields of `ChainConfig`. They are not yet
/// on [`cc_types::ChainConfig`] (see [`crate::schema::ConfigDigestInput`]), so
/// this thin view carries exactly what the function needs — including the
/// optional vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` field for the cross-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockServeWindowCfg {
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`.
    pub min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`.
    pub churn_limit_quotient: u64,
    /// Vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` if present (Hoodi); `None` on
    /// mainnet-spec configs at `v1.7.0-alpha.13`. Never used as the authority.
    pub min_epochs_for_block_requests: Option<u64>,
}

impl BlockServeWindowCfg {
    /// Construct from the two required scalars with no vestigial field.
    #[must_use]
    pub const fn new(
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
    ) -> Self {
        Self {
            min_validator_withdrawability_delay,
            churn_limit_quotient,
            min_epochs_for_block_requests: None,
        }
    }

    /// Construct with an optional vestigial config field for cross-check.
    #[must_use]
    pub const fn with_vestigial(
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
        min_epochs_for_block_requests: Option<u64>,
    ) -> Self {
        Self {
            min_validator_withdrawability_delay,
            churn_limit_quotient,
            min_epochs_for_block_requests,
        }
    }

    /// Load the three relevant keys from a consensus-specs / eth-clients YAML
    /// config file. Unknown keys are ignored; the two required scalars must be
    /// present.
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, WindowConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| WindowConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_yaml_str(&text)
    }

    /// Parse the three relevant keys from YAML text.
    pub fn from_yaml_str(text: &str) -> Result<Self, WindowConfigError> {
        let raw: RawServeWindowYaml =
            serde_yaml::from_str(text).map_err(|e| WindowConfigError::Yaml(e.to_string()))?;
        Ok(Self {
            min_validator_withdrawability_delay: raw.min_validator_withdrawability_delay,
            churn_limit_quotient: raw.churn_limit_quotient,
            min_epochs_for_block_requests: raw.min_epochs_for_block_requests,
        })
    }
}

/// CC-4A: computed, never read from config. The config field, if present, is a
/// cross-check ([`check_min_epochs_for_block_requests`]).
///
/// Spec (`phase0/p2p-interface.md` @ `v1.7.0-alpha.13`):
/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT // 2`.
///
/// Hoodi / mainnet: `256 + 65536 / 2 = 33024` (see `docs/serve-windows.md`).
///
/// **SEC-4A-1:** uses [`u64::checked_div`] / [`u64::checked_add`] and returns
/// [`WindowConfigError::ArithmeticOverflow`] rather than wrapping. A wrapped
/// floor would under-advertise and under-retain history.
pub fn compute_min_epochs_for_block_requests(
    cfg: &BlockServeWindowCfg,
) -> Result<u64, WindowConfigError> {
    let half = cfg
        .churn_limit_quotient
        .checked_div(2)
        .ok_or(WindowConfigError::ArithmeticOverflow {
            op: "CHURN_LIMIT_QUOTIENT / 2",
            min_validator_withdrawability_delay: cfg.min_validator_withdrawability_delay,
            churn_limit_quotient: cfg.churn_limit_quotient,
        })?;
    cfg.min_validator_withdrawability_delay
        .checked_add(half)
        .ok_or(WindowConfigError::ArithmeticOverflow {
            op: "MIN_VALIDATOR_WITHDRAWABILITY_DELAY + (CHURN_LIMIT_QUOTIENT / 2)",
            min_validator_withdrawability_delay: cfg.min_validator_withdrawability_delay,
            churn_limit_quotient: cfg.churn_limit_quotient,
        })
}

/// Startup cross-check: if the loaded config supplies
/// `MIN_EPOCHS_FOR_BLOCK_REQUESTS`, it must equal the computed value or the
/// node **refuses to start**. Absent field → ok (mainnet-spec configs).
///
/// On success returns the computed floor (the authority). Overflow in the
/// compute path is also a refuse-to-start (SEC-4A-1 fail-closed).
pub fn check_min_epochs_for_block_requests(
    cfg: &BlockServeWindowCfg,
) -> Result<u64, WindowConfigError> {
    let computed = compute_min_epochs_for_block_requests(cfg)?;
    if let Some(stated) = cfg.min_epochs_for_block_requests
        && stated != computed
    {
        return Err(WindowConfigError::MinEpochsMismatch { stated, computed });
    }
    Ok(computed)
}

/// Errors from serve-window config load and the vestigial-field cross-check.
#[derive(Debug, thiserror::Error)]
pub enum WindowConfigError {
    /// Filesystem error reading a config path.
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
    /// Vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` disagrees with the computed floor.
    ///
    /// `Display` names **both** numbers so an operator can see the mismatch
    /// without re-running the arithmetic by hand (CC-4A /2).
    #[error(
        "MIN_EPOCHS_FOR_BLOCK_REQUESTS mismatch: config has {stated}, computed {computed} \
         (MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2)"
    )]
    MinEpochsMismatch {
        /// Value from the config file.
        stated: u64,
        /// Value from [`compute_min_epochs_for_block_requests`].
        computed: u64,
    },
    /// Checked arithmetic overflow (SEC-4A-1). Refuse rather than wrap.
    #[error(
        "serve-window arithmetic overflow in {op}: \
         MIN_VALIDATOR_WITHDRAWABILITY_DELAY={min_validator_withdrawability_delay}, \
         CHURN_LIMIT_QUOTIENT={churn_limit_quotient}"
    )]
    ArithmeticOverflow {
        /// Which step overflowed (`/ 2` or `+`).
        op: &'static str,
        /// Left operand of the formula.
        min_validator_withdrawability_delay: u64,
        /// Right operand of the formula (before `/ 2`).
        churn_limit_quotient: u64,
    },
}

/// Serde shape for the three keys this module reads from network YAML.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct RawServeWindowYaml {
    min_validator_withdrawability_delay: u64,
    churn_limit_quotient: u64,
    #[serde(default)]
    min_epochs_for_block_requests: Option<u64>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// Hoodi / mainnet scalars (V-1, retrieved 2026-08-08 from eth-clients/hoodi).
    const HOODI_WITHDRAWABILITY: u64 = 256;
    const HOODI_CHURN: u64 = 65_536;
    /// Expected floor: 256 + 65536/2. Written only in tests (CC-4A /4).
    const HOODI_FLOOR: u64 = 33_024;

    fn types_fixture(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures")
            .join(name)
    }

    /// CC-4A /1 — first half: Hoodi loaded config → 33 024.
    #[test]
    fn compute_hoodi_fixture_is_33024() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi-config.yaml serve-window fields");
        assert_eq!(cfg.min_validator_withdrawability_delay, HOODI_WITHDRAWABILITY);
        assert_eq!(cfg.churn_limit_quotient, HOODI_CHURN);
        assert_eq!(
            compute_min_epochs_for_block_requests(&cfg).expect("hoodi must compute"),
            HOODI_FLOOR
        );
    }

    /// CC-4A /1 — second half: different `CHURN_LIMIT_QUOTIENT` → different value.
    ///
    /// A test that only asserts `== 33024` would pass with a hard-coded constant;
    /// this half discharges the "computed from cfg fields" claim.
    #[test]
    fn compute_different_churn_limit_quotient_differs() {
        // 256 + 32768/2 = 256 + 16384 = 16640 (issue example: 32768 → 16 640).
        let cfg = BlockServeWindowCfg::new(HOODI_WITHDRAWABILITY, 32_768);
        let value = compute_min_epochs_for_block_requests(&cfg).expect("must compute");
        assert_eq!(value, 16_640);
        assert_ne!(value, HOODI_FLOOR);
    }

    /// SEC-4A-1 — overflow fails closed (no wrap to a silent wrong floor).
    #[test]
    fn compute_overflow_fails_closed() {
        // u64::MAX + (2/2) overflows checked_add.
        let cfg = BlockServeWindowCfg::new(u64::MAX, 2);
        let err = compute_min_epochs_for_block_requests(&cfg).expect_err("must overflow");
        assert!(
            matches!(err, WindowConfigError::ArithmeticOverflow { .. }),
            "got {err:?}"
        );
        // check path is also refuse-to-start.
        let err = check_min_epochs_for_block_requests(&cfg).expect_err("check must refuse");
        assert!(matches!(err, WindowConfigError::ArithmeticOverflow { .. }));
    }

    /// CC-4A /2 positive — Hoodi `config.yaml` vestigial field present and equal → starts.
    #[test]
    fn vestigial_equal_starts() {
        // V-1: Hoodi still carries MIN_EPOCHS_FOR_BLOCK_REQUESTS (fixture mirrors eth-clients/hoodi).
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi-config.yaml");
        assert_eq!(
            cfg.min_epochs_for_block_requests,
            Some(HOODI_FLOOR),
            "hoodi fixture must carry the vestigial field for the equality path"
        );
        let floor = check_min_epochs_for_block_requests(&cfg).expect("equal vestigial must start");
        assert_eq!(floor, HOODI_FLOOR);
    }

    /// CC-4A /2 negative — same config with field mutated to 33023 → refuse with both numbers.
    #[test]
    fn vestigial_unequal_refuses_naming_both() {
        let mut text = std::fs::read_to_string(types_fixture("hoodi-config.yaml")).unwrap();
        // Mutate the vestigial field only (keep the two live scalars).
        text = text.replace(
            "MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33024",
            "MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33023",
        );
        assert!(
            text.contains("MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33023"),
            "mutation must land"
        );
        let cfg = BlockServeWindowCfg::from_yaml_str(&text).expect("mutated hoodi yaml");
        assert_eq!(cfg.min_epochs_for_block_requests, Some(33_023));
        let err = check_min_epochs_for_block_requests(&cfg).expect_err("must refuse");
        let msg = err.to_string();
        match err {
            WindowConfigError::MinEpochsMismatch {
                stated: s,
                computed: c,
            } => {
                assert_eq!(s, 33_023);
                assert_eq!(c, HOODI_FLOOR);
            }
            other => panic!("expected MinEpochsMismatch, got {other:?}"),
        }
        assert!(
            msg.contains("33023"),
            "error must name stated value 33023: {msg}"
        );
        assert!(
            msg.contains("33024"),
            "error must name computed value 33024: {msg}"
        );
    }

    /// CC-4A /3 — mainnet-spec config (field absent) starts normally.
    #[test]
    fn mainnet_absent_field_starts() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("mainnet-config.yaml"))
            .expect("parse mainnet-config.yaml serve-window fields");
        assert_eq!(cfg.min_epochs_for_block_requests, None);
        assert_eq!(cfg.min_validator_withdrawability_delay, HOODI_WITHDRAWABILITY);
        assert_eq!(cfg.churn_limit_quotient, HOODI_CHURN);
        let floor =
            check_min_epochs_for_block_requests(&cfg).expect("absent field must start");
        assert_eq!(floor, HOODI_FLOOR);
    }

    /// Hoodi fixture file itself must also start (vestigial may be absent from
    /// the excerpt; when present and equal it is covered by vestigial_equal).
    #[test]
    fn hoodi_fixture_check_starts() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi");
        check_min_epochs_for_block_requests(&cfg).expect("hoodi fixture must start");
    }
}
