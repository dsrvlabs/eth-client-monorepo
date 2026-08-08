//! Devnet-only dangerous knobs (CC-4D / Architecture §2.5, §4.7, §10.4).
//!
//! One guard, three knobs:
//! - `storage.retention_override` (compressed retention venue)
//! - `storage.debug.crash_point` (fault-injection abort between put and commit)
//! - `chain.event_ring_bytes` shrink below the production default (clause-2 four-minute test)
//!
//! Each is accepted **only** when the loaded config's `genesis_validators_root` is
//! neither Hoodi's nor mainnet's. Config-based (not env) so
//! `scripts/check-no-env-reads.sh` stays green.

use std::fmt;

/// Hoodi `genesis_validators_root` (canonical hex with `0x` prefix).
pub const HOODI_GENESIS_VALIDATORS_ROOT: &str =
    "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f";

/// Mainnet `genesis_validators_root` (canonical hex with `0x` prefix).
pub const MAINNET_GENESIS_VALIDATORS_ROOT: &str =
    "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95";

/// Self-devnet GVR from `devnet/expected-manifest.json` (CC-2Ja seed).
///
/// Used by tests and as the documented non-production root that permits the
/// three dangerous knobs.
pub const DEVNET_GENESIS_VALIDATORS_ROOT: &str =
    "0x4d04ab2dc363bf4d5e09d605f2872f49edf76c0bc09bcd14ad875f11742d11d0";

/// Default production event-ring hard byte ceiling (`chain.event_ring_bytes` =
/// 64 MiB). A configured value **strictly below** this is the ring-shrinking
/// override gated by [`require_devnet_gvr`].
pub const DEFAULT_EVENT_RING_BYTES: usize = 64 * 1024 * 1024;

/// Field path names for error messages (match TOML keys / Architecture wording).
pub const KNOB_RETENTION_OVERRIDE: &str = "storage.retention_override";
pub const KNOB_CRASH_POINT: &str = "storage.debug.crash_point";
pub const KNOB_EVENT_RING_BYTES_SHRINK: &str = "chain.event_ring_bytes";

/// Error from a dangerous-knob guard refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DangerousKnobError {
    /// Config path of the knob being refused (e.g. `storage.retention_override`).
    pub knob: String,
    /// Normalised GVR that triggered the refusal (or empty if missing).
    pub genesis_validators_root: String,
    /// Human-readable reason.
    pub reason: String,
}

impl fmt::Display for DangerousKnobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "refusing dangerous config knob `{}` (genesis_validators_root={}): {}",
            self.knob,
            if self.genesis_validators_root.is_empty() {
                "<missing>"
            } else {
                &self.genesis_validators_root
            },
            self.reason
        )
    }
}

impl std::error::Error for DangerousKnobError {}

/// Strip `0x`/`0X` and lowercase for equality.
fn normalize_gvr(hex: &str) -> String {
    let s = hex.trim();
    let body = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    body.to_ascii_lowercase()
}

/// Whether `gvr` is Hoodi's or mainnet's (production networks where non-spec
/// retention / crash points / ring shrink must never run).
#[must_use]
pub fn is_production_network_gvr(gvr: &str) -> bool {
    let n = normalize_gvr(gvr);
    n == normalize_gvr(HOODI_GENESIS_VALIDATORS_ROOT)
        || n == normalize_gvr(MAINNET_GENESIS_VALIDATORS_ROOT)
}

/// Refuse a dangerous config knob unless `genesis_validators_root` is present
/// and is neither Hoodi's nor mainnet's.
///
/// Call sites: storage (retention override, crash_point) and chain (ring shrink).
/// One function, three knobs — the knob name is only for the error surface.
pub fn require_devnet_gvr(
    knob: &str,
    genesis_validators_root: Option<&str>,
) -> Result<(), DangerousKnobError> {
    let Some(raw) = genesis_validators_root.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err(DangerousKnobError {
            knob: knob.to_owned(),
            genesis_validators_root: String::new(),
            reason: format!(
                "set genesis_validators_root to a non-Hoodi, non-mainnet root \
                 before enabling `{knob}` (devnet-only; see CC-4D / Architecture §10.4)"
            ),
        });
    };

    if is_production_network_gvr(raw) {
        let network = if normalize_gvr(raw) == normalize_gvr(HOODI_GENESIS_VALIDATORS_ROOT) {
            "Hoodi"
        } else {
            "mainnet"
        };
        return Err(DangerousKnobError {
            knob: knob.to_owned(),
            genesis_validators_root: raw.to_owned(),
            reason: format!(
                "`{knob}` is illegal on {network}: compressed retention, crash-point \
                 injection, and event-ring shrink are devnet-only (CC-4D)"
            ),
        });
    }

    Ok(())
}

/// Whether `event_ring_bytes` is a shrink relative to the production default.
#[must_use]
pub fn is_event_ring_bytes_shrink(event_ring_bytes: usize) -> bool {
    event_ring_bytes < DEFAULT_EVENT_RING_BYTES
}

/// Guard the three dangerous knobs that may be set together.
///
/// - `retention_override_set` — any `[retention_override]` table present
/// - `crash_point` — `Some` non-empty string under `[debug].crash_point`
/// - `event_ring_bytes` — `Some` when the chain field is known; shrink is gated
///
/// Returns the first refusal. Safe to call with all knobs inactive (`Ok(())`).
pub fn check_dangerous_knobs(
    genesis_validators_root: Option<&str>,
    retention_override_set: bool,
    crash_point: Option<&str>,
    event_ring_bytes: Option<usize>,
) -> Result<(), DangerousKnobError> {
    if retention_override_set {
        require_devnet_gvr(KNOB_RETENTION_OVERRIDE, genesis_validators_root)?;
    }
    if let Some(cp) = crash_point.map(str::trim).filter(|s| !s.is_empty()) {
        let _ = cp;
        require_devnet_gvr(KNOB_CRASH_POINT, genesis_validators_root)?;
    }
    if let Some(bytes) = event_ring_bytes
        && is_event_ring_bytes_shrink(bytes)
    {
        require_devnet_gvr(KNOB_EVENT_RING_BYTES_SHRINK, genesis_validators_root)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn workspace_devnet_retention() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../devnet/retention-compressed.toml")
    }

    /// Minimal parse of retention-compressed.toml fields used by the guard tests.
    #[derive(Debug, serde::Deserialize)]
    struct RetentionProfile {
        genesis_validators_root: Option<String>,
        #[serde(default)]
        retention_override: Option<RetentionOverrideToml>,
        #[serde(default)]
        event_ring_bytes: Option<usize>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct RetentionOverrideToml {
        columns_epochs: u64,
        blocks_epochs: u64,
    }

    fn load_retention_profile() -> RetentionProfile {
        let path = workspace_devnet_retention();
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        toml::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
    }

    // ── six assertions: both directions × three knobs ───────────────────────

    #[test]
    fn retention_override_refused_on_hoodi_gvr() {
        let profile = load_retention_profile();
        assert!(
            profile.retention_override.is_some(),
            "retention-compressed.toml must set [retention_override]"
        );
        let ro = profile.retention_override.as_ref().unwrap();
        assert_eq!(ro.columns_epochs, 64);
        assert_eq!(ro.blocks_epochs, 256);

        let err = check_dangerous_knobs(
            Some(HOODI_GENESIS_VALIDATORS_ROOT),
            true,
            None,
            None,
        )
        .expect_err("Hoodi + retention_override must refuse");
        assert_eq!(err.knob, KNOB_RETENTION_OVERRIDE);
        assert!(
            err.to_string().contains("storage.retention_override"),
            "error must name the field, got: {err}"
        );
        assert!(
            err.to_string().contains("Hoodi") || err.to_string().contains("illegal"),
            "error must name the network, got: {err}"
        );
    }

    #[test]
    fn retention_override_accepted_on_devnet_gvr() {
        let profile = load_retention_profile();
        let gvr = profile
            .genesis_validators_root
            .as_deref()
            .unwrap_or(DEVNET_GENESIS_VALIDATORS_ROOT);
        assert_eq!(
            normalize_gvr(gvr),
            normalize_gvr(DEVNET_GENESIS_VALIDATORS_ROOT),
            "profile GVR should match expected-manifest devnet root"
        );
        check_dangerous_knobs(Some(gvr), true, None, None)
            .expect("devnet GVR + retention_override must start");
    }

    #[test]
    fn crash_point_refused_on_hoodi_gvr() {
        let err = check_dangerous_knobs(
            Some(HOODI_GENESIS_VALIDATORS_ROOT),
            false,
            Some("after_put_before_commit"),
            None,
        )
        .expect_err("Hoodi + crash_point must refuse");
        assert_eq!(err.knob, KNOB_CRASH_POINT);
        assert!(
            err.to_string().contains("storage.debug.crash_point"),
            "error must name the field, got: {err}"
        );
    }

    #[test]
    fn crash_point_accepted_on_devnet_gvr() {
        check_dangerous_knobs(
            Some(DEVNET_GENESIS_VALIDATORS_ROOT),
            false,
            Some("after_put_before_commit"),
            None,
        )
        .expect("devnet GVR + crash_point must start");
    }

    #[test]
    fn event_ring_bytes_shrink_refused_on_hoodi_gvr() {
        let one_mib = 1024 * 1024;
        assert!(is_event_ring_bytes_shrink(one_mib));
        let err = check_dangerous_knobs(
            Some(HOODI_GENESIS_VALIDATORS_ROOT),
            false,
            None,
            Some(one_mib),
        )
        .expect_err("Hoodi + ring shrink must refuse");
        assert_eq!(err.knob, KNOB_EVENT_RING_BYTES_SHRINK);
        assert!(
            err.to_string().contains("chain.event_ring_bytes"),
            "error must name the field, got: {err}"
        );
    }

    #[test]
    fn event_ring_bytes_shrink_accepted_on_devnet_gvr() {
        let profile = load_retention_profile();
        let bytes = profile.event_ring_bytes.unwrap_or(1024 * 1024);
        assert!(
            is_event_ring_bytes_shrink(bytes),
            "profile event_ring_bytes={bytes} should be a shrink vs default {DEFAULT_EVENT_RING_BYTES}"
        );
        check_dangerous_knobs(
            Some(DEVNET_GENESIS_VALIDATORS_ROOT),
            false,
            None,
            Some(bytes),
        )
        .expect("devnet GVR + ring shrink must start");
    }

    #[test]
    fn mainnet_gvr_also_refused() {
        let err = require_devnet_gvr(KNOB_RETENTION_OVERRIDE, Some(MAINNET_GENESIS_VALIDATORS_ROOT))
            .expect_err("mainnet must refuse");
        assert!(err.to_string().contains("mainnet"), "got: {err}");
    }

    #[test]
    fn missing_gvr_refused_when_knob_set() {
        let err = require_devnet_gvr(KNOB_CRASH_POINT, None).expect_err("missing GVR");
        assert!(err.genesis_validators_root.is_empty());
        assert!(err.to_string().contains("missing") || err.to_string().contains("set genesis"));
    }

    #[test]
    fn default_ring_bytes_not_a_shrink() {
        assert!(!is_event_ring_bytes_shrink(DEFAULT_EVENT_RING_BYTES));
        assert!(!is_event_ring_bytes_shrink(DEFAULT_EVENT_RING_BYTES + 1));
        check_dangerous_knobs(
            Some(HOODI_GENESIS_VALIDATORS_ROOT),
            false,
            None,
            Some(DEFAULT_EVENT_RING_BYTES),
        )
        .expect("production default ring size is not a dangerous knob");
    }

    #[test]
    fn inactive_knobs_ok_even_on_hoodi() {
        check_dangerous_knobs(Some(HOODI_GENESIS_VALIDATORS_ROOT), false, None, None)
            .expect("no knobs → no guard");
        check_dangerous_knobs(None, false, None, None).expect("no knobs, no gvr → ok");
    }

    #[test]
    fn gvr_comparison_is_case_and_prefix_insensitive() {
        let upper = HOODI_GENESIS_VALIDATORS_ROOT.to_ascii_uppercase();
        assert!(is_production_network_gvr(&upper));
        let bare = HOODI_GENESIS_VALIDATORS_ROOT.trim_start_matches("0x");
        assert!(is_production_network_gvr(bare));
    }
}
