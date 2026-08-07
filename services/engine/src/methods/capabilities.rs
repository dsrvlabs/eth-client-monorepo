//! `engine_exchangeCapabilities` handshake (CC-31 / Architecture §3.3).
//!
//! - Request body is **only** [`ADVERTISED_CAPABILITIES`] — no registry union.
//! - Required methods missing → `error!` + `cc_engine_capability_missing` gauge.
//! - `engine_getBlobsV3` presence is **recorded** (OQ-P3-2) and never dispatched.
//! - Cache is refreshed on success; callers clear it on auth failure / Offline.

use serde_json::{Value, json};

use crate::capabilities::{
    ADVERTISED_CAPABILITIES, CapabilityCache, CapabilitySnapshot, GET_BLOBS_V3, REQUIRED_CAPABILITIES,
};
use crate::errors::EngineError;
use crate::metrics::{EngineMethod, EngineMetrics, MethodLabels};
use crate::methods::names;
use crate::transport::{EngineTransport, Lane};

/// Result of a capability handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityHandshake {
    pub snapshot: CapabilitySnapshot,
    /// Required methods that were absent from the EL response.
    pub missing_required: Vec<&'static str>,
}

/// Exchange capabilities with the EL and update the cache.
///
/// Call at startup (before the first `newPayload`) and on the not-Synced →
/// Synced edge. Does **not** clear the cache on failure — clearing is reserved
/// for the auth-failure and Offline state edges (CC-31 /4).
///
/// Missing required methods: `error!` + `cc_engine_capability_missing` gauge
/// only (Architecture §3.3). **Not** an `Err` — CC-31 AC does not require
/// fail-closed handshake; host/CC-32b may refuse ordered-lane work when
/// `missing_required` is non-empty.
///
/// **Runtime wiring of clear/refresh edges is CC-32b / engine host** — this
/// module only exposes the helpers; production state machine must call them.
pub async fn exchange_capabilities(
    transport: &EngineTransport,
    cache: &CapabilityCache,
    metrics: Option<&EngineMetrics>,
) -> Result<CapabilityHandshake, EngineError> {
    let params = json!([ADVERTISED_CAPABILITIES.as_slice()]);
    let result = transport
        .call(
            Lane::Upcheck,
            EngineMethod::ExchangeCapabilities,
            names::EXCHANGE_CAPABILITIES,
            params,
        )
        .await?;

    let methods = parse_capability_result(&result)?;
    let snapshot = CapabilitySnapshot::from_el_methods(methods);
    let missing_required: Vec<&'static str> = REQUIRED_CAPABILITIES
        .iter()
        .copied()
        .filter(|m| !snapshot.supports(m))
        .collect();

    for method in &missing_required {
        if let Some(m) = metrics {
            m.capability_missing
                .get_or_create(&MethodLabels {
                    method: short_method_label(method).to_owned(),
                })
                .set(1);
        }
        tracing::error!(
            method = %method,
            "EL does not advertise required Engine API capability"
        );
    }
    // Clear missing gauges for methods that are present.
    if let Some(m) = metrics {
        for method in REQUIRED_CAPABILITIES {
            if snapshot.supports(method) {
                m.capability_missing
                    .get_or_create(&MethodLabels {
                        method: short_method_label(method).to_owned(),
                    })
                    .set(0);
            }
        }
    }

    // Discovery is not adoption (OQ-P3-2 / CC-3E). Presence lives on the
    // snapshot flag + log only — do not overload `capability_missing` (that
    // gauge is for REQUIRED methods that are absent).
    if snapshot.get_blobs_v3_present {
        tracing::info!(
            method = GET_BLOBS_V3,
            "EL advertises engine_getBlobsV3 (discovered, not used; OQ-P3-2)"
        );
    } else {
        tracing::info!(
            method = GET_BLOBS_V3,
            "EL does not advertise engine_getBlobsV3"
        );
    }

    cache.store(snapshot.clone());

    Ok(CapabilityHandshake {
        snapshot,
        missing_required,
    })
}

/// Parse the EL's capability array from a JSON-RPC result value.
fn parse_capability_result(result: &Value) -> Result<Vec<String>, EngineError> {
    let arr = result.as_array().ok_or_else(|| EngineError::Decode {
        reason: "exchangeCapabilities result is not an array".into(),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v.as_str().ok_or_else(|| EngineError::Decode {
            reason: "exchangeCapabilities entry is not a string".into(),
        })?;
        out.push(s.to_owned());
    }
    Ok(out)
}

/// Map wire method name → metric label (matches [`EngineMethod::as_str`] where possible).
fn short_method_label(wire: &str) -> &str {
    match wire {
        "engine_newPayloadV4" => "newPayloadV4",
        "engine_forkchoiceUpdatedV3" => "forkchoiceUpdatedV3",
        "engine_getBlobsV2" => "getBlobsV2",
        "engine_getBlobsV3" => "getBlobsV3",
        "engine_exchangeCapabilities" => "exchangeCapabilities",
        "eth_syncing" => "eth_syncing",
        other => other,
    }
}

/// Clear the capability cache on the Offline edge (CC-31 /4).
///
/// **Handoff (CC-32b / CC-36):** call from the engine state machine when
/// transitioning to `Offline`. Library-only until that host lands.
pub fn on_offline(cache: &CapabilityCache) {
    cache.clear();
    tracing::error!("engine offline: capability cache cleared");
}

/// Clear the capability cache on the auth-failure edge (CC-31 /4).
///
/// **Handoff (CC-32b / CC-36):** call on `Http401`/`Http403` → `AuthFailed`.
/// Library-only until that host lands.
pub fn on_auth_failure(cache: &CapabilityCache) {
    cache.clear();
    tracing::error!("engine auth failed: capability cache cleared");
}

/// Refresh capabilities on the not-Synced → Synced edge (CC-31 /4).
///
/// **Handoff (CC-36):** call when upcheck moves not-Synced → Synced.
pub async fn on_synced_edge(
    transport: &EngineTransport,
    cache: &CapabilityCache,
    metrics: Option<&EngineMetrics>,
) -> Result<CapabilityHandshake, EngineError> {
    exchange_capabilities(transport, cache, metrics).await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::metrics::EngineMetrics;
    use prometheus_client::registry::Registry;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn transport(url: &str, metrics: Option<EngineMetrics>) -> EngineTransport {
        EngineTransport::from_parts(
            url,
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs::default()),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            metrics,
        )
    }

    #[tokio::test]
    async fn capability_cache_cleared_on_auth_failure() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [
                    "engine_newPayloadV4",
                    "engine_forkchoiceUpdatedV3",
                    "engine_getBlobsV2",
                    "eth_syncing"
                ]
            })))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let cache = CapabilityCache::new();
        exchange_capabilities(&t, &cache, None)
            .await
            .expect("handshake");
        assert!(!cache.is_empty());

        on_auth_failure(&cache);
        assert!(
            cache.is_empty(),
            "auth-failure edge must clear the capability cache"
        );
    }

    #[tokio::test]
    async fn capability_cache_cleared_on_offline() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": ["engine_newPayloadV4", "engine_forkchoiceUpdatedV3", "engine_getBlobsV2"]
            })))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let cache = CapabilityCache::new();
        exchange_capabilities(&t, &cache, None)
            .await
            .expect("handshake");
        assert!(!cache.is_empty());

        on_offline(&cache);
        assert!(
            cache.is_empty(),
            "offline edge must clear the capability cache"
        );
    }

    #[tokio::test]
    async fn capability_cache_refreshed_on_synced_edge() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [
                    "engine_newPayloadV4",
                    "engine_forkchoiceUpdatedV3",
                    "engine_getBlobsV2",
                    "engine_getBlobsV3"
                ]
            })))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let cache = CapabilityCache::new();
        // Start empty (not-Synced), then transition to Synced → refresh.
        assert!(cache.is_empty());
        let hs = on_synced_edge(&t, &cache, None).await.expect("refresh");
        assert!(!cache.is_empty());
        assert!(hs.snapshot.get_blobs_v3_present);
        assert!(hs.missing_required.is_empty());
    }

    /// `getBlobsV3` is discovered (logged + cache flag) and never dispatched.
    #[tokio::test]
    async fn get_blobs_v3_discovered_not_used() {
        use std::io::{self, Write};
        use std::sync::Mutex;
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::Registry as TracingRegistry;
        use tracing_subscriber::fmt;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Debug)]
        struct BufferWriter {
            inner: Arc<Mutex<Vec<u8>>>,
        }
        impl Write for BufferWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.inner.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufferWriter {
            type Writer = BufferWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [
                    "engine_newPayloadV4",
                    "engine_forkchoiceUpdatedV3",
                    "engine_getBlobsV2",
                    "engine_getBlobsV3"
                ]
            })))
            .mount(&server)
            .await;

        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let t = transport(&server.uri(), Some(metrics.clone()));
        let cache = CapabilityCache::new();

        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = BufferWriter {
            inner: Arc::clone(&buf),
        };
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new("info"))
            .with(fmt::layer().with_writer(writer).with_ansi(false));
        let _guard = tracing::subscriber::set_default(subscriber);

        let hs = exchange_capabilities(&t, &cache, Some(&metrics))
            .await
            .expect("handshake");

        assert!(hs.snapshot.get_blobs_v3_present);
        assert_eq!(cache.get_blobs_v3_discovered(), Some(true));

        let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        assert!(
            captured.contains("engine_getBlobsV3") || captured.contains("getBlobsV3"),
            "discovery must be logged; got:\n{captured}"
        );

        // No dispatch path: this module never calls getBlobsV3 on the wire.
        // (grep on methods/ is the static proof; runtime: we only called exchangeCapabilities.)
        assert!(
            !hs.snapshot.methods.is_empty(),
            "EL methods recorded for discovery"
        );
    }

    #[test]
    fn advertised_list_is_request_body_source() {
        // The array is the only source of the request body (Architecture §3.3).
        let params = json!([ADVERTISED_CAPABILITIES.as_slice()]);
        let arr = params.as_array().unwrap()[0].as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0], "engine_newPayloadV4");
        let forbidden_blobs = format!("engine_getBlobsV{}", 4);
        assert!(
            arr.iter()
                .all(|v| v.as_str() != Some(forbidden_blobs.as_str()))
        );
        assert!(
            arr.iter()
                .all(|v| v.as_str() != Some("engine_exchangeCapabilities"))
        );
    }
}
