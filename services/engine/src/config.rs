//! Engine service config (CC-30a / Architecture §3.6, §7; CC-31 §3.4 / §12/7).
//!
//! Five transport timeout knobs + multiplier, JWT secret path, EL endpoint,
//! the inputs for the **runtime-derived** soft deadline
//! (`ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000`), and the **loaded** EL
//! fork schedule (`[el_forks]`) used by the version gate.
//!
//! Soft deadline uses `SLOT_DURATION_MS`, never `SECONDS_PER_SLOT` (deprecated
//! in Hoodi's live config; Architecture delta 12 / CC-30a).
//!
//! **Naming:** Architecture §3.6 TOML uses `[timeouts].multiplier` (env
//! `CC_ENGINE_TIMEOUTS__MULTIPLIER`). The PRD/issue prose name
//! `execution_timeout_multiplier` is the same knob; the nested wire name is
//! canonical so it lives next to the five values it scales.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::version::ElForkSchedule;

/// Default transport timeouts from the Engine API / Architecture §3.6 (ms).
pub const DEFAULT_NEW_PAYLOAD_MS: u64 = 8_000;
pub const DEFAULT_FORKCHOICE_UPDATED_MS: u64 = 8_000;
pub const DEFAULT_GET_BLOBS_MS: u64 = 1_000;
pub const DEFAULT_EXCHANGE_CAPABILITIES_MS: u64 = 1_000;
pub const DEFAULT_ETH_SYNCING_MS: u64 = 1_000;
pub const DEFAULT_MULTIPLIER: f64 = 1.0;

/// Hoodi defaults for soft-deadline inputs (Architecture §3.6).
pub const DEFAULT_SLOT_DURATION_MS: u64 = 12_000;
pub const DEFAULT_ATTESTATION_DUE_BPS: u64 = 3_333;

/// Per-method transport timeouts after applying `multiplier`.
#[derive(Debug, Clone, PartialEq)]
pub struct TransportTimeouts {
    pub new_payload: Duration,
    pub forkchoice_updated: Duration,
    pub get_blobs: Duration,
    pub exchange_capabilities: Duration,
    pub eth_syncing: Duration,
}

impl TransportTimeouts {
    /// Build from raw ms knobs and the operator multiplier.
    ///
    /// Multiplier scales **only** these five values — never the soft deadline.
    #[must_use]
    pub fn from_knobs(knobs: &TimeoutKnobs) -> Self {
        let m = if knobs.multiplier.is_finite() && knobs.multiplier > 0.0 {
            knobs.multiplier
        } else {
            DEFAULT_MULTIPLIER
        };
        let scale = |ms: u64| Duration::from_secs_f64((ms as f64) * m / 1_000.0);
        Self {
            new_payload: scale(knobs.new_payload_ms),
            forkchoice_updated: scale(knobs.forkchoice_updated_ms),
            get_blobs: scale(knobs.get_blobs_ms),
            exchange_capabilities: scale(knobs.exchange_capabilities_ms),
            eth_syncing: scale(knobs.eth_syncing_ms),
        }
    }
}

/// Five transport knobs + multiplier (TOML `[timeouts]`).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TimeoutKnobs {
    #[serde(default = "default_new_payload_ms")]
    pub new_payload_ms: u64,
    #[serde(default = "default_forkchoice_updated_ms")]
    pub forkchoice_updated_ms: u64,
    #[serde(default = "default_get_blobs_ms")]
    pub get_blobs_ms: u64,
    #[serde(default = "default_exchange_capabilities_ms")]
    pub exchange_capabilities_ms: u64,
    #[serde(default = "default_eth_syncing_ms")]
    pub eth_syncing_ms: u64,
    /// Scales the five transport values only. Default `1.0`.
    ///
    /// Wire name is `multiplier` under `[timeouts]` (Architecture §3.6). Alias
    /// accepts the PRD prose name `execution_timeout_multiplier` at the same
    /// nested key for operator familiarity.
    #[serde(default = "default_multiplier", alias = "execution_timeout_multiplier")]
    pub multiplier: f64,
}

impl Default for TimeoutKnobs {
    fn default() -> Self {
        Self {
            new_payload_ms: DEFAULT_NEW_PAYLOAD_MS,
            forkchoice_updated_ms: DEFAULT_FORKCHOICE_UPDATED_MS,
            get_blobs_ms: DEFAULT_GET_BLOBS_MS,
            exchange_capabilities_ms: DEFAULT_EXCHANGE_CAPABILITIES_MS,
            eth_syncing_ms: DEFAULT_ETH_SYNCING_MS,
            multiplier: DEFAULT_MULTIPLIER,
        }
    }
}

fn default_new_payload_ms() -> u64 {
    DEFAULT_NEW_PAYLOAD_MS
}
fn default_forkchoice_updated_ms() -> u64 {
    DEFAULT_FORKCHOICE_UPDATED_MS
}
fn default_get_blobs_ms() -> u64 {
    DEFAULT_GET_BLOBS_MS
}
fn default_exchange_capabilities_ms() -> u64 {
    DEFAULT_EXCHANGE_CAPABILITIES_MS
}
fn default_eth_syncing_ms() -> u64 {
    DEFAULT_ETH_SYNCING_MS
}
fn default_multiplier() -> f64 {
    DEFAULT_MULTIPLIER
}
fn default_slot_duration_ms() -> u64 {
    DEFAULT_SLOT_DURATION_MS
}
fn default_attestation_due_bps() -> u64 {
    DEFAULT_ATTESTATION_DUE_BPS
}
fn default_el_endpoint() -> String {
    "http://127.0.0.1:8551".into()
}
fn default_jwt_secret_path() -> PathBuf {
    PathBuf::from("secrets/jwt.hex")
}
fn default_p2p_uri() -> String {
    // Plain config URI — **not** under `[peers]` (ADR P3-02 / CC-38a).
    // p2p restarting must not make engine NOT_SERVING.
    "http://127.0.0.1:9002".into()
}

/// EL fork schedule TOML table (`[el_forks]`, CC-31 / §12/7).
///
/// Loaded from config, never hard-coded in production Rust. Hoodi values and
/// retrieval date live in `config/engine.toml` (same style as p2p bootnodes).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ElForksConfig {
    /// Unix seconds: Osaka activation (`OsakaTime`).
    pub osaka_time: u64,
    /// Unix seconds: BPO1 activation (`BPO1Time`).
    #[serde(default)]
    pub bpo1_time: Option<u64>,
    /// Unix seconds: BPO2 activation (`BPO2Time`).
    #[serde(default)]
    pub bpo2_time: Option<u64>,
    /// Unix seconds: Amsterdam activation (`AmsterdamTime`). Unset on Hoodi.
    #[serde(default)]
    pub amsterdam_time: Option<u64>,
}

impl ElForksConfig {
    /// Convert to the version-gate schedule type.
    #[must_use]
    pub fn schedule(&self) -> ElForkSchedule {
        ElForkSchedule {
            osaka_time: self.osaka_time,
            bpo1_time: self.bpo1_time,
            bpo2_time: self.bpo2_time,
            amsterdam_time: self.amsterdam_time,
        }
    }
}

/// Engine-only configuration fields (flattened beside [`cc_config::ServiceConfig`]).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct EngineTransportConfig {
    /// Authenticated Engine API HTTP endpoint (default `http://127.0.0.1:8551`).
    #[serde(default = "default_el_endpoint")]
    pub el_endpoint: String,
    /// Path to the hex-encoded 32-byte JWT secret (override: `CC_ENGINE_JWT_SECRET_PATH`).
    #[serde(default = "default_jwt_secret_path")]
    pub jwt_secret_path: PathBuf,
    /// Five transport timeouts + multiplier.
    #[serde(default)]
    pub timeouts: TimeoutKnobs,
    /// Slot duration in **milliseconds** (soft-deadline input). Never seconds.
    #[serde(default = "default_slot_duration_ms")]
    pub slot_duration_ms: u64,
    /// Attestation due time in basis points of the slot (soft-deadline input).
    #[serde(default = "default_attestation_due_bps")]
    pub attestation_due_bps: u64,
    /// EL fork schedule for the version gate (CC-31). Optional only so older
    /// partial TOML fixtures still deserialise; production `config/engine.toml`
    /// always supplies `[el_forks]`.
    #[serde(default)]
    pub el_forks: Option<ElForksConfig>,
    /// gRPC URI for the ninth-contract `EngineStream` client (CC-38a).
    ///
    /// **Plain config key — not a `[peers]` entry** (ADR P3-02): the fast path is
    /// an accelerator, and `p2p` restarting must not make `engine` `NOT_SERVING`.
    #[serde(default = "default_p2p_uri")]
    pub p2p_uri: String,
}

impl Default for EngineTransportConfig {
    fn default() -> Self {
        Self {
            el_endpoint: default_el_endpoint(),
            jwt_secret_path: default_jwt_secret_path(),
            timeouts: TimeoutKnobs::default(),
            slot_duration_ms: DEFAULT_SLOT_DURATION_MS,
            attestation_due_bps: DEFAULT_ATTESTATION_DUE_BPS,
            el_forks: None,
            p2p_uri: default_p2p_uri(),
        }
    }
}

impl EngineTransportConfig {
    /// Soft deadline in milliseconds:
    /// `ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000`.
    ///
    /// Hoodi: `3333 × 12000 / 10000 = 3999.6`. Gloas-like: `2500 × 12000 / 10000 = 3000`.
    ///
    /// Derived at runtime; warns and counts on exceed, **never aborts**.
    #[must_use]
    pub fn soft_deadline_ms(&self) -> f64 {
        soft_deadline_ms(self.attestation_due_bps, self.slot_duration_ms)
    }

    /// Soft deadline as [`Duration`] (sub-millisecond via secs_f64).
    #[must_use]
    pub fn soft_deadline(&self) -> Duration {
        Duration::from_secs_f64(self.soft_deadline_ms() / 1_000.0)
    }

    /// Transport timeouts after multiplier.
    #[must_use]
    pub fn transport_timeouts(&self) -> TransportTimeouts {
        TransportTimeouts::from_knobs(&self.timeouts)
    }

    /// EL fork schedule for [`crate::version::method_for`], if configured.
    #[must_use]
    pub fn el_fork_schedule(&self) -> Option<ElForkSchedule> {
        self.el_forks.as_ref().map(ElForksConfig::schedule)
    }
}

/// Pure soft-deadline derivation (Architecture §3.6 / delta 12).
///
/// Uses `SLOT_DURATION_MS`, never `SECONDS_PER_SLOT`.
#[must_use]
pub fn soft_deadline_ms(attestation_due_bps: u64, slot_duration_ms: u64) -> f64 {
    (attestation_due_bps as f64) * (slot_duration_ms as f64) / 10_000.0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn soft_deadline_is_derived() {
        // Hoodi: 3333 bps × 12000 ms / 10000 = 3999.6 ms.
        let hoodi = soft_deadline_ms(3_333, 12_000);
        assert!((hoodi - 3999.6).abs() < 1e-9, "hoodi deadline = {hoodi}");

        // Gloas-like: 2500 bps → 3000 ms.
        let gloas = soft_deadline_ms(2_500, 12_000);
        assert!((gloas - 3000.0).abs() < 1e-9, "gloas deadline = {gloas}");

        let cfg = EngineTransportConfig {
            attestation_due_bps: 3_333,
            slot_duration_ms: 12_000,
            ..EngineTransportConfig::default()
        };
        assert!((cfg.soft_deadline_ms() - 3999.6).abs() < 1e-9);
    }

    #[test]
    fn timeout_knobs() {
        let knobs = TimeoutKnobs::default();
        assert_eq!(knobs.new_payload_ms, 8_000);
        assert_eq!(knobs.forkchoice_updated_ms, 8_000);
        assert_eq!(knobs.get_blobs_ms, 1_000);
        assert_eq!(knobs.exchange_capabilities_ms, 1_000);
        assert_eq!(knobs.eth_syncing_ms, 1_000);
        assert_eq!(knobs.multiplier, 1.0);

        let base = TransportTimeouts::from_knobs(&knobs);
        assert_eq!(base.new_payload, Duration::from_millis(8_000));
        assert_eq!(base.get_blobs, Duration::from_millis(1_000));
        assert_eq!(base.eth_syncing, Duration::from_millis(1_000));

        let mut scaled_knobs = knobs.clone();
        scaled_knobs.multiplier = 2.0;
        let scaled = TransportTimeouts::from_knobs(&scaled_knobs);
        assert_eq!(scaled.new_payload, Duration::from_millis(16_000));
        assert_eq!(scaled.forkchoice_updated, Duration::from_millis(16_000));
        assert_eq!(scaled.get_blobs, Duration::from_millis(2_000));
        assert_eq!(scaled.exchange_capabilities, Duration::from_millis(2_000));
        assert_eq!(scaled.eth_syncing, Duration::from_millis(2_000));

        // Soft deadline and raw knobs are untouched by the multiplier.
        let cfg = EngineTransportConfig {
            timeouts: scaled_knobs,
            attestation_due_bps: 3_333,
            slot_duration_ms: 12_000,
            ..EngineTransportConfig::default()
        };
        assert!((cfg.soft_deadline_ms() - 3999.6).abs() < 1e-9);
        assert_eq!(cfg.timeouts.eth_syncing_ms, 1_000); // raw floor unchanged
    }

    #[test]
    fn multiplier_accepts_execution_timeout_multiplier_alias() {
        // PRD prose name is accepted as a nested alias; wire name stays multiplier.
        let knobs: TimeoutKnobs = serde_json::from_value(serde_json::json!({
            "new_payload_ms": 8000,
            "execution_timeout_multiplier": 1.5
        }))
        .expect("alias deserialises");
        assert!((knobs.multiplier - 1.5).abs() < 1e-9);
        let scaled = TransportTimeouts::from_knobs(&knobs);
        assert_eq!(scaled.new_payload, Duration::from_millis(12_000));
    }

    /// `CC-31` /3: `[el_forks]` is loaded from config and drives the version gate.
    ///
    /// Hoodi activation times live **only** in `config/engine.toml` — this test
    /// must not embed them as Rust literals (grep acceptance).
    #[test]
    fn el_forks_loaded_from_config() {
        use crate::methods::names;
        use crate::version::method_for;
        use std::path::PathBuf;

        // Resolve config relative to workspace root (CWD for `cargo test -p cc-engine`).
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/engine.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            text.contains("[el_forks]"),
            "config/engine.toml must define [el_forks]"
        );
        assert!(
            text.contains("Retrieval date:"),
            "el_forks table must carry its retrieval date (p2p bootnode style)"
        );

        // Parse only the nested table via a minimal wrapper so we do not need
        // ServiceConfig fields from the full file.
        #[derive(Deserialize)]
        struct File {
            el_forks: ElForksConfig,
        }
        let file: File = toml::from_str(&text).expect("engine.toml el_forks deserialises");
        assert!(file.el_forks.osaka_time > 0, "osaka_time must be set");
        assert!(file.el_forks.bpo1_time.is_some(), "bpo1_time must be set");
        assert!(file.el_forks.bpo2_time.is_some(), "bpo2_time must be set");
        assert!(
            file.el_forks.amsterdam_time.is_none(),
            "AmsterdamTime unset on Hoodi"
        );

        let schedule = file.el_forks.schedule();
        // Gate reads the loaded times: Osaka and BPO2 both select V4.
        assert_eq!(
            method_for(schedule.osaka_time, &schedule).unwrap(),
            names::NEW_PAYLOAD_V4
        );
        let bpo2 = schedule.bpo2_time.expect("bpo2 from config");
        assert_eq!(method_for(bpo2, &schedule).unwrap(), names::NEW_PAYLOAD_V4);
        // Pre-Osaka still V4 (Prague window).
        assert_eq!(
            method_for(schedule.osaka_time - 1, &schedule).unwrap(),
            names::NEW_PAYLOAD_V4
        );
    }

    /// CC-38a / ADR P3-02: `p2p_uri` is a plain config key, not a health peer.
    #[test]
    fn p2p_uri_is_plain_config_not_health_peer() {
        use std::path::PathBuf;

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/engine.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            text.contains("p2p_uri"),
            "config/engine.toml must declare p2p_uri"
        );
        // Parse as generic value so we can inspect tables without fighting
        // the house-style "keys after [peers] until next table" layout.
        let value: toml::Value = toml::from_str(&text).expect("engine.toml parses");
        let p2p_uri = value
            .get("p2p_uri")
            .and_then(|v| v.as_str())
            .expect("root-level p2p_uri");
        assert!(
            p2p_uri.starts_with("http://"),
            "p2p_uri must be a plain gRPC URI"
        );
        let peers = value
            .get("peers")
            .and_then(|v| v.as_table())
            .expect("[peers] table");
        assert!(
            !peers.contains_key("p2p"),
            "p2p must not appear under [peers] (ADR P3-02): p2p restarting must not make engine NOT_SERVING"
        );
        assert!(peers.contains_key("chain"), "chain remains the health peer");
        // Default constructor also carries the URI.
        assert!(
            EngineTransportConfig::default()
                .p2p_uri
                .starts_with("http://")
        );
    }
}
