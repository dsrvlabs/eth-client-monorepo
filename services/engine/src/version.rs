//! Engine API method version selection from `executionPayload.timestamp`
//! (CC-31 / Architecture §3.4).
//!
//! Selection reads the **payload timestamp** only — never a consensus-layer
//! time base. geth's own gate is `LatestFork(timestamp)`; the two diverge
//! exactly at a fork boundary — which is the only moment the answer matters.
//!
//! Phase 3 method surface (delta 1 / V-1):
//! - `engine_newPayloadV4` — Prague → Osaka **and all BPOs** (not V5; V5 is Amsterdam)
//! - `engine_forkchoiceUpdatedV3` — same window
//! - Amsterdam-or-later → loud pre-flight [`VersionError`] (never a silent V4 send)
//!
//! **CC-14 correction:** Phase 1's landed
//! `plans/20260806 - phase-1-chain-core/issues/CC-14 - Execution Engine Seam.md`
//! said V5; that is wrong for Fulu/Osaka. `execution-apis` `osaka.md` defines
//! only `getPayloadV5` / `getBlobsV2` / `getBlobsV3` and does not close
//! `newPayloadV4` / `forkchoiceUpdatedV3`; `amsterdam.md` closes those windows
//! with `-38005` for Amsterdam timestamps. geth v1.17.5 gates `NewPayloadV4` on
//! Prague/Osaka/BPO1…BPO5 and `NewPayloadV5` on Amsterdam.

use std::fmt;

use crate::methods::names;
use crate::metrics::EngineMetrics;

/// EL fork identity derived from a payload timestamp and the loaded schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElFork {
    /// Pre-Osaka (Prague-era window still uses V4).
    Prague,
    /// Osaka activation ≤ t < first BPO (or Amsterdam if no BPO).
    Osaka,
    /// A BPO fork window (BPO1, BPO2, …). Still selects V4.
    Bpo(u8),
    /// Amsterdam — not implemented in Phase 3.
    Amsterdam,
    /// Reserved for a post-Amsterdam fork once the schedule grows a later bound.
    ///
    /// **Not returned by [`ElForkSchedule::fork_at`] today:** when `amsterdam_time`
    /// is set, timestamps ≥ that bound map to [`Self::Amsterdam`]; when unset,
    /// post-BPO timestamps stay in [`Self::Bpo`]. Both `Amsterdam` and `Beyond`
    /// select the same [`VersionError`] arm in [`method_for`].
    Beyond,
}

/// Loud pre-flight failure when the payload timestamp is past our method window.
///
/// Raised **before any HTTP call** (R-9 / Architecture §3.4). Counted once on
/// `cc_engine_unsupported_fork_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionError {
    /// Timestamp maps to Amsterdam or beyond; we have no method for this fork.
    UnsupportedFork { timestamp: u64 },
}

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFork { timestamp } => write!(
                f,
                "unsupported EL fork for executionPayload.timestamp={timestamp}: \
                 Amsterdam-or-later is not implemented (Phase 3 method window ends at Osaka/BPO)"
            ),
        }
    }
}

impl std::error::Error for VersionError {}

/// EL fork schedule loaded from `config/engine.toml` `[el_forks]` (never compiled in).
///
/// Hoodi values live only in the config file (§12/7).
///
/// - `amsterdam_time = Some(t)` and `timestamp >= t` → [`ElFork::Amsterdam`]
///   (loud pre-flight failure in [`method_for`]).
/// - `amsterdam_time = None` (Hoodi today) → post-BPO timestamps stay
///   [`ElFork::Bpo`] and still select V4 until operators set Amsterdam.
/// - [`ElFork::Beyond`] is reserved for a future schedule bound; see its docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElForkSchedule {
    /// Unix seconds: Osaka activation (`OsakaTime`).
    pub osaka_time: u64,
    /// Unix seconds: BPO1 activation (`BPO1Time`).
    pub bpo1_time: Option<u64>,
    /// Unix seconds: BPO2 activation (`BPO2Time`).
    pub bpo2_time: Option<u64>,
    /// Unix seconds: Amsterdam activation (`AmsterdamTime`). Unset on Hoodi today.
    pub amsterdam_time: Option<u64>,
}

impl ElForkSchedule {
    /// Classify `timestamp` (execution payload / attributes unix seconds).
    #[must_use]
    pub fn fork_at(&self, timestamp: u64) -> ElFork {
        if let Some(ams) = self.amsterdam_time
            && timestamp >= ams
        {
            // Upper bound of the Phase 3 method window. Beyond stays unused
            // until a later fork is added to this schedule.
            return ElFork::Amsterdam;
        }

        // BPO windows (highest first).
        if let Some(bpo2) = self.bpo2_time
            && timestamp >= bpo2
        {
            return ElFork::Bpo(2);
        }
        if let Some(bpo1) = self.bpo1_time
            && timestamp >= bpo1
        {
            return ElFork::Bpo(1);
        }
        if timestamp >= self.osaka_time {
            return ElFork::Osaka;
        }
        ElFork::Prague
    }
}

/// Select the `engine_newPayload*` JSON-RPC method for `timestamp`.
///
/// Reads **`executionPayload.timestamp`** only (not a consensus-layer clock).
///
/// Prague, Osaka, and every BPO arm return [`names::NEW_PAYLOAD_V4`].
/// Amsterdam (and any future beyond-arm) return [`VersionError::UnsupportedFork`].
pub fn method_for(timestamp: u64, cfg: &ElForkSchedule) -> Result<&'static str, VersionError> {
    match cfg.fork_at(timestamp) {
        ElFork::Prague | ElFork::Osaka | ElFork::Bpo(_) => Ok(names::NEW_PAYLOAD_V4),
        ElFork::Amsterdam | ElFork::Beyond => Err(VersionError::UnsupportedFork { timestamp }),
    }
}

/// Select the `engine_forkchoiceUpdated*` method for a payload-attributes timestamp.
///
/// Phase 3 always sends `null` attributes, so geth's attributes-timestamp check
/// does not run; the arm is still gated the same way for completeness.
pub fn forkchoice_method_for(
    timestamp: u64,
    cfg: &ElForkSchedule,
) -> Result<&'static str, VersionError> {
    match cfg.fork_at(timestamp) {
        ElFork::Prague | ElFork::Osaka | ElFork::Bpo(_) => Ok(names::FORKCHOICE_UPDATED_V3),
        ElFork::Amsterdam | ElFork::Beyond => Err(VersionError::UnsupportedFork { timestamp }),
    }
}

/// Pre-flight version gate: resolve the method or fail loudly with metric + log.
///
/// On [`VersionError`], increments `cc_engine_unsupported_fork_total` **once**
/// and returns the error **before any HTTP call**.
///
/// **Handoff (CC-32b):** the real `newPayload` adapter must call this before
/// `transport.call`, and must call [`observe_el_unsupported_fork`] exactly once
/// on EL `-38005` (never both for one failure; never retry).
pub fn resolve_new_payload_method(
    timestamp: u64,
    cfg: &ElForkSchedule,
    metrics: Option<&EngineMetrics>,
) -> Result<&'static str, VersionError> {
    match method_for(timestamp, cfg) {
        Ok(m) => Ok(m),
        Err(e) => {
            record_unsupported_fork(&e, metrics);
            Err(e)
        }
    }
}

/// Record an unsupported-fork observation (our pre-flight gate **or** EL `-38005`).
///
/// Call once per failure; callers must not retry after `-38005` (CC-31 /5).
pub fn record_unsupported_fork(err: &VersionError, metrics: Option<&EngineMetrics>) {
    if let Some(m) = metrics {
        m.unsupported_fork.inc();
    }
    match err {
        VersionError::UnsupportedFork { timestamp } => {
            tracing::error!(
                timestamp,
                "unsupported EL fork (Amsterdam-or-later): refusing Engine API call pre-flight"
            );
        }
    }
}

/// Observe an EL-supplied `-38005 Unsupported fork` (never retried).
pub fn observe_el_unsupported_fork(message: &str, metrics: Option<&EngineMetrics>) {
    if let Some(m) = metrics {
        m.unsupported_fork.inc();
    }
    tracing::error!(
        message = %message,
        code = -38005_i64,
        "EL returned -38005 unsupported fork; not retrying"
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::EngineMetrics;
    use prometheus_client::registry::Registry;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    /// Synthetic schedule — deliberately not Hoodi's live times (those live only
    /// in `config/engine.toml`; see `el_forks_loaded_from_config`).
    fn synthetic_schedule() -> ElForkSchedule {
        ElForkSchedule {
            osaka_time: 1_000_000,
            bpo1_time: Some(2_000_000),
            bpo2_time: Some(3_000_000),
            amsterdam_time: Some(4_000_000),
        }
    }

    /// Wiremock-free HTTP counter: tracks whether any transport call was issued.
    #[derive(Default)]
    struct HttpProbe {
        requests: AtomicUsize,
    }

    impl HttpProbe {
        fn inc(&self) {
            self.requests.fetch_add(1, Ordering::SeqCst);
        }
        fn count(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    /// Table-driven version gate over Prague / Osaka / BPO2 / Amsterdam.
    #[test]
    fn method_for_timestamp() {
        let cfg = synthetic_schedule();
        let osaka = cfg.osaka_time;
        let bpo2 = cfg.bpo2_time.expect("synthetic has BPO2");
        let prague = osaka.saturating_sub(1);
        let amsterdam = cfg.amsterdam_time.expect("synthetic has Amsterdam");

        let cases: [(&str, u64, Result<&str, ()>); 4] = [
            ("Prague-era", prague, Ok(names::NEW_PAYLOAD_V4)),
            ("Osaka-era", osaka, Ok(names::NEW_PAYLOAD_V4)),
            ("BPO2-era", bpo2, Ok(names::NEW_PAYLOAD_V4)),
            ("Amsterdam-era", amsterdam, Err(())),
        ];

        for (label, ts, expected) in cases {
            let got = method_for(ts, &cfg);
            match expected {
                Ok(method) => {
                    assert_eq!(
                        got.as_ref().copied().ok(),
                        Some(method),
                        "{label}: timestamp={ts} expected {method}, got {got:?}"
                    );
                }
                Err(()) => {
                    assert!(
                        matches!(got, Err(VersionError::UnsupportedFork { timestamp }) if timestamp == ts),
                        "{label}: timestamp={ts} expected VersionError, got {got:?}"
                    );
                }
            }
        }
    }

    /// Fulu/Osaka payload selects the literal `engine_newPayloadV4` (not V5).
    #[test]
    fn fulu_payload_selects_new_payload_v4() {
        let cfg = synthetic_schedule();
        let method = method_for(cfg.osaka_time, &cfg).expect("Osaka is V4");
        assert_eq!(method, "engine_newPayloadV4");
        assert_eq!(method, names::NEW_PAYLOAD_V4);
        // BPO-era still V4 (D1) — same window as Osaka.
        let bpo2 = cfg.bpo2_time.expect("bpo2");
        assert_eq!(
            method_for(bpo2, &cfg).unwrap(),
            "engine_newPayloadV4",
            "BPO-era timestamp still uses newPayloadV4"
        );
    }

    /// Amsterdam arm is pre-flight: metric +1, zero HTTP.
    #[test]
    fn amsterdam_fails_before_any_http() {
        let cfg = synthetic_schedule();
        let amsterdam = cfg.amsterdam_time.expect("amsterdam");

        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.unsupported_fork.get();

        let probe = Arc::new(HttpProbe::default());
        // The gate itself never touches HTTP; we still pass a probe that would
        // fire if a caller incorrectly issued a request after a VersionError.
        let result = resolve_new_payload_method(amsterdam, &cfg, Some(&metrics));
        // Deliberately do not call HTTP on error.
        if result.is_ok() {
            probe.inc();
        }

        assert!(
            matches!(
                result,
                Err(VersionError::UnsupportedFork { timestamp }) if timestamp == amsterdam
            ),
            "Amsterdam must be VersionError, got {result:?}"
        );
        assert_eq!(
            metrics.unsupported_fork.get(),
            before + 1,
            "cc_engine_unsupported_fork_total must increment by exactly 1"
        );
        assert_eq!(probe.count(), 0, "no HTTP request may be issued");
    }

    /// EL `-38005` is counted once and never retried.
    #[tokio::test]
    async fn unsupported_fork_not_retried() {
        use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
        use crate::errors::EngineError;
        use crate::jwt::JwtSecret;
        use crate::metrics::EngineMethod;
        use crate::transport::{EngineTransport, Lane};
        use serde_json::json;
        use wiremock::matchers::method as http_method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_c = Arc::clone(&hits);
        Mock::given(http_method("POST"))
            .respond_with(move |_req: &wiremock::Request| {
                hits_c.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {
                        "code": -38005,
                        "message": "Unsupported fork"
                    }
                }))
            })
            .mount(&server)
            .await;

        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.unsupported_fork.get();

        let t = EngineTransport::from_parts(
            server.uri(),
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs::default()),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            Some(metrics.clone()),
        );

        // Single attempt — transport never retries newPayload (ADR P3-09);
        // -38005 is Fatal and must not be re-issued by the caller either.
        let err = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([{}]),
            )
            .await
            .expect_err("-38005");
        assert!(
            matches!(err, EngineError::UnsupportedFork { .. }),
            "expected UnsupportedFork, got {err:?}"
        );
        // Observe once (CC-31 /5).
        observe_el_unsupported_fork(&err.to_string(), Some(&metrics));

        assert_eq!(
            metrics.unsupported_fork.get(),
            before + 1,
            "exactly one unsupported_fork increment"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1, "zero retries");
        assert_eq!(err.retry_class(), crate::errors::RetryClass::Fatal);
    }

    /// Full cycle at Osaka timestamp must not issue legacy V1/V4 get methods.
    #[tokio::test]
    async fn legacy_methods_never_sent() {
        use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
        use crate::jwt::JwtSecret;
        use crate::metrics::EngineMethod;
        use crate::transport::{EngineTransport, Lane};
        use serde_json::{Value, json};
        use std::sync::Mutex as StdMutex;
        use wiremock::matchers::method as http_method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let seen_c = Arc::clone(&seen);

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(move |req: &wiremock::Request| {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                if let Some(m) = body.get("method").and_then(|m| m.as_str()) {
                    seen_c.lock().unwrap().push(m.to_owned());
                }
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"status": "VALID"}
                }))
            })
            .mount(&server)
            .await;

        let cfg = synthetic_schedule();
        let ts = cfg.osaka_time;
        let method = method_for(ts, &cfg).expect("Osaka → V4");
        assert_eq!(method, names::NEW_PAYLOAD_V4);

        let t = EngineTransport::from_parts(
            server.uri(),
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs::default()),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            None,
        );

        // Request cycle at Osaka: newPayload + forkchoice only (no getBlobsV1 / getPayloadV4).
        let _ = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                method,
                json!([{}]),
            )
            .await;
        let fcu = forkchoice_method_for(ts, &cfg).unwrap();
        let _ = t
            .call(
                Lane::Ordered,
                EngineMethod::ForkchoiceUpdatedV3,
                fcu,
                json!([{}, Value::Null]),
            )
            .await;

        let methods = seen.lock().unwrap().clone();
        assert!(
            !methods.iter().any(|m| m == "engine_getBlobsV1"),
            "must not send engine_getBlobsV1 at Osaka: {methods:?}"
        );
        assert!(
            !methods.iter().any(|m| m == "engine_getPayloadV4"),
            "must not send engine_getPayloadV4 at Osaka: {methods:?}"
        );
        assert!(
            methods.iter().any(|m| m == "engine_newPayloadV4"),
            "expected newPayloadV4: {methods:?}"
        );
        assert!(
            methods.iter().any(|m| m == "engine_forkchoiceUpdatedV3"),
            "expected forkchoiceUpdatedV3: {methods:?}"
        );
    }
}
