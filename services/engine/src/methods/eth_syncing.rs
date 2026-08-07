//! `eth_syncing` upcheck (CC-36a / Architecture §3.7).
//!
//! One JSON-RPC call on the **upcheck lane** with the 1 s eth_syncing timeout.
//! Never rides the ordered lane — a stalled health probe must not block
//! `newPayload` (CC-36 /2, ADR P3-08).

use serde_json::{Value, json};

use crate::errors::EngineError;
use crate::methods::names;
use crate::metrics::{EngineMethod, EngineMetrics};
use crate::transport::{EngineTransport, Lane};

/// Outcome of a single `eth_syncing` probe (before state-machine side effects).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EthSyncingResult {
    /// EL reported `false` — fully synced.
    NotSyncing,
    /// EL reported a non-`false` JSON value (object or `true`) — still syncing.
    Syncing(Value),
}

/// Issue one `eth_syncing` call on the upcheck lane.
///
/// Observes `cc_engine_upcheck_seconds` when metrics are present. Auth /
/// transport failures propagate as [`EngineError`] for the state machine to
/// classify (`AuthFailed` / `Offline`).
pub async fn eth_syncing(
    transport: &EngineTransport,
    metrics: Option<&EngineMetrics>,
) -> Result<EthSyncingResult, EngineError> {
    let started = std::time::Instant::now();
    let result = transport
        .call(
            Lane::Upcheck,
            EngineMethod::EthSyncing,
            names::ETH_SYNCING,
            json!([]),
        )
        .await;
    if let Some(m) = metrics {
        m.upcheck_seconds
            .observe(started.elapsed().as_secs_f64());
    }
    let value = result?;
    Ok(classify_eth_syncing_result(&value))
}

/// `false` ⇒ not-syncing; anything else (including `true` and sync objects) ⇒ syncing.
#[must_use]
pub fn classify_eth_syncing_result(value: &Value) -> EthSyncingResult {
    match value {
        Value::Bool(false) => EthSyncingResult::NotSyncing,
        other => EthSyncingResult::Syncing(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::methods::names;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    #[test]
    fn classify_false_is_not_syncing() {
        assert_eq!(
            classify_eth_syncing_result(&json!(false)),
            EthSyncingResult::NotSyncing
        );
    }

    #[test]
    fn classify_true_and_object_are_syncing() {
        assert!(matches!(
            classify_eth_syncing_result(&json!(true)),
            EthSyncingResult::Syncing(_)
        ));
        assert!(matches!(
            classify_eth_syncing_result(&json!({"startingBlock":"0x0","currentBlock":"0x1","highestBlock":"0x2"})),
            EthSyncingResult::Syncing(_)
        ));
    }

    /// CC-36 /2: a 30 s `eth_syncing` stall must not block `newPayload`.
    #[tokio::test]
    async fn eth_syncing_stall_does_not_block_new_payload() {
        struct StallEthSyncing {
            new_payload_hits: Arc<AtomicUsize>,
        }
        impl Respond for StallEthSyncing {
            fn respond(&self, req: &Request) -> ResponseTemplate {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
                if method == names::ETH_SYNCING {
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(30))
                        .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":false}))
                } else {
                    self.new_payload_hits.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "status": "VALID",
                            "latestValidHash": null,
                            "validationError": null
                        }
                    }))
                }
            }
        }

        let hits = Arc::new(AtomicUsize::new(0));
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(StallEthSyncing {
                new_payload_hits: Arc::clone(&hits),
            })
            .mount(&server)
            .await;

        let jwt = JwtSecret::from_bytes([0x11; 32]);
        let timeouts = TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 2_000,
            forkchoice_updated_ms: 2_000,
            get_blobs_ms: 1_000,
            exchange_capabilities_ms: 1_000,
            eth_syncing_ms: 1_000,
            multiplier: 1.0,
        });
        let t = Arc::new(EngineTransport::from_parts(
            server.uri(),
            jwt,
            timeouts,
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            None,
        ));

        let t_up = Arc::clone(&t);
        let upcheck = tokio::spawn(async move { eth_syncing(t_up.as_ref(), None).await });
        // Let the stalled upcheck acquire its lane.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = std::time::Instant::now();
        // Minimal SSZ that fails decode is fine for lane-separation — but we
        // exercise the ordered lane with a raw transport call for the stall
        // criterion (same lane as newPayload).
        let np = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([{}]),
            )
            .await;
        let elapsed = started.elapsed();
        assert!(np.is_ok(), "newPayload must complete during eth_syncing stall: {np:?}");
        assert!(
            elapsed < Duration::from_secs(5),
            "ordered lane blocked by upcheck stall: {elapsed:?}"
        );
        assert!(
            hits.load(Ordering::SeqCst) >= 1,
            "newPayload must have hit the mock during the stall"
        );
        upcheck.abort();
    }

    /// Wrong JWT / HTTP 401 on `eth_syncing` → terminal `AuthFailed` (external Offline).
    #[tokio::test]
    async fn wrong_jwt_reaches_auth_failed() {
        use crate::capabilities::CapabilityCache;
        use crate::state::{EngineState, EngineStateHandle, EngineStateInternal};

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("missing token"))
            .mount(&server)
            .await;

        let jwt = JwtSecret::from_bytes([0x33; 32]);
        let timeouts = TransportTimeouts::from_knobs(&TimeoutKnobs {
            eth_syncing_ms: 500,
            ..TimeoutKnobs::default()
        });
        let t = EngineTransport::from_parts(
            server.uri(),
            jwt,
            timeouts,
            Duration::from_secs(4),
            None,
        );
        let handle = EngineStateHandle::new(
            Arc::new(CapabilityCache::new()),
            None,
            Duration::from_secs(12),
        );
        handle.run_upcheck(&t, None).await;
        assert_eq!(
            handle.internal().await,
            EngineStateInternal::AuthFailed,
            "HTTP 401 must reach AuthFailed"
        );
        assert_eq!(handle.external().await, EngineState::Offline);
        // Terminal: further upchecks leave it there.
        for _ in 0..3 {
            handle.run_upcheck(&t, None).await;
            assert_eq!(handle.internal().await, EngineStateInternal::AuthFailed);
        }
        assert!(
            !handle
                .transition_log()
                .await
                .iter()
                .any(|tr| tr.from == EngineStateInternal::AuthFailed
                    && tr.to == EngineStateInternal::Offline),
            "AuthFailed must not back off into Offline"
        );
        assert!(!handle.admits_el_call().await, "fail-closed while AuthFailed");
    }

    /// ADR P3-09: `newPayload` is never retried inside `engine`.
    #[tokio::test]
    async fn new_payload_never_retried_in_engine() {
        let hits = Arc::new(AtomicUsize::new(0));
        struct OnceFail {
            hits: Arc<AtomicUsize>,
        }
        impl Respond for OnceFail {
            fn respond(&self, _req: &Request) -> ResponseTemplate {
                self.hits.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32603, "message": "internal"}
                }))
            }
        }

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(OnceFail {
                hits: Arc::clone(&hits),
            })
            .mount(&server)
            .await;

        let jwt = JwtSecret::from_bytes([0x22; 32]);
        let timeouts = TransportTimeouts::from_knobs(&TimeoutKnobs::default());
        let t = EngineTransport::from_parts(
            server.uri(),
            jwt,
            timeouts,
            Duration::from_secs(4),
            None,
        );
        // Transport path that newPayload uses once — no nested retry.
        let err = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([{}]),
            )
            .await
            .expect_err("must fail");
        assert!(
            matches!(err, EngineError::InternalError { .. }),
            "expected InternalError, got {err:?}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "newPayload must produce exactly one request; requeue is chain's"
        );
    }
}
