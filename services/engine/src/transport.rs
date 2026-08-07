//! Engine API HTTP/JSON-RPC transport with three lanes (CC-30a / §2.2, §3.1).
//!
//! One `reqwest` client (connection pool) shared by:
//! - **ordered** (`newPayload` + `fcU`): concurrency **1** via `tokio::sync::Mutex`
//!   held **across** the HTTP call
//! - **fastpath** (`getBlobs`): concurrency **2** via `Semaphore`
//! - **upcheck** (`eth_syncing` / `exchangeCapabilities`): concurrency **1**
//!
//! Response bodies are capped **before any parse**. Default ceiling is **1 MiB**
//! (Architecture §3.1 — HTML auth/error pages). `engine_getBlobsV2` success
//! bodies use a larger method-specific ceiling so a Hoodi-scale Complete
//! response (many `BlobAndProofV2`s as hex JSON) is readable (CC-37a review F1).
//! Status → error mapping follows Architecture §3.1 (plain-text 401/403 are normal).

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};

use crate::config::{EngineTransportConfig, TransportTimeouts};
use crate::errors::EngineError;
use crate::jwt::JwtSecret;
use crate::metrics::{EngineMethod, EngineMetrics, ErrorCodeLabels, MethodLabels};

/// Default response-body ceiling (Architecture §3.1). Applied to ordered/upcheck
/// success paths and **all** non-2xx bodies (auth/error pages stay small).
pub const MAX_BODY_BYTES: usize = 1024 * 1024; // 1 MiB

/// `engine_getBlobsV2` **success** body ceiling (CC-37a F1).
///
/// Rough hex JSON size ≈ 0.28 MiB per `BlobAndProofV2` (131072-byte blob + 128×48
/// proofs, hex-doubled, plus JSON punctuation). Hoodi `max_blobs_per_block` is
/// 15/21 (CC-1G); 21 × ~0.3 MiB ≈ 6.3 MiB + envelope. **16 MiB** gives headroom
/// above that without opening the ordered/auth paths to multi-MiB HTML.
pub const MAX_BODY_BYTES_GET_BLOBS: usize = 16 * 1024 * 1024;

/// Body ceiling for a **successful** (2xx) response of `method`.
///
/// Non-2xx always use [`MAX_BODY_BYTES`] regardless of method so a hostile or
/// verbose auth-error page cannot force a multi-MiB allocation on the fastpath.
#[must_use]
pub const fn max_body_bytes_for_success(method: EngineMethod) -> usize {
    match method {
        EngineMethod::GetBlobsV2 => MAX_BODY_BYTES_GET_BLOBS,
        EngineMethod::NewPayloadV4
        | EngineMethod::ForkchoiceUpdatedV3
        | EngineMethod::ExchangeCapabilities
        | EngineMethod::EthSyncing => MAX_BODY_BYTES,
    }
}

/// Which EL call lane a request rides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// `newPayload` + `forkchoiceUpdated` — serialised.
    Ordered,
    /// `getBlobs*` — bounded concurrency 2.
    Fastpath,
    /// `eth_syncing` / `exchangeCapabilities` — concurrency 1, never blocks import.
    Upcheck,
}

/// Per-request overrides for **container IT only** (CC-30b).
///
/// Hidden from rustdoc: not part of the production Engine API surface. Exists so
/// `services/engine/tests/auth_container.rs` can skew `iat` and override `Host`
/// without forking the transport. Cannot be `#[cfg(test)]` / `pub(crate)` —
/// integration tests compile as a separate crate.
///
/// **Invariant:** production method adapters must never pass untrusted
/// `host`/`iat` through here (self-DoS via stale token or sticky 403). Use
/// [`EngineTransport::call`] (always `iat=now`, no Host override).
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct RequestOverrides {
    /// Explicit JWT `iat` (unix seconds). `None` → sign with wall-clock now.
    pub iat: Option<u64>,
    /// Override the outgoing HTTP `Host` header (vhost rejection tests only).
    /// Does **not** retarget TCP — endpoint URL stays fixed (no SSRF).
    pub host: Option<String>,
}

/// Shared Engine API transport.
#[derive(Debug)]
pub struct EngineTransport {
    client: reqwest::Client,
    endpoint: String,
    jwt: JwtSecret,
    timeouts: TransportTimeouts,
    soft_deadline: Duration,
    /// Held across the entire ordered-lane HTTP call (not just request build).
    ordered: Mutex<()>,
    fastpath: Semaphore,
    upcheck: Semaphore,
    metrics: Option<EngineMetrics>,
    /// Monotonic JSON-RPC id.
    next_id: std::sync::atomic::AtomicU64,
}

impl EngineTransport {
    /// Build a transport from config + loaded secret.
    pub fn new(
        cfg: &EngineTransportConfig,
        jwt: JwtSecret,
        metrics: Option<EngineMetrics>,
    ) -> Result<Self, EngineError> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| EngineError::Transport {
                detail: format!("http client build: {e}"),
            })?;
        Ok(Self {
            client,
            endpoint: cfg.el_endpoint.clone(),
            jwt,
            timeouts: cfg.transport_timeouts(),
            soft_deadline: cfg.soft_deadline(),
            ordered: Mutex::new(()),
            fastpath: Semaphore::new(2),
            upcheck: Semaphore::new(1),
            metrics,
            next_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Test/constructor helper with explicit pieces.
    #[must_use]
    pub fn from_parts(
        endpoint: impl Into<String>,
        jwt: JwtSecret,
        timeouts: TransportTimeouts,
        soft_deadline: Duration,
        metrics: Option<EngineMetrics>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
            jwt,
            timeouts,
            soft_deadline,
            ordered: Mutex::new(()),
            fastpath: Semaphore::new(2),
            upcheck: Semaphore::new(1),
            metrics,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Soft deadline used for the warn/count path (never aborts).
    #[must_use]
    pub fn soft_deadline(&self) -> Duration {
        self.soft_deadline
    }

    /// Transport timeouts after multiplier.
    #[must_use]
    pub fn timeouts(&self) -> &TransportTimeouts {
        &self.timeouts
    }

    fn timeout_for(&self, method: EngineMethod) -> Duration {
        match method {
            EngineMethod::NewPayloadV4 => self.timeouts.new_payload,
            EngineMethod::ForkchoiceUpdatedV3 => self.timeouts.forkchoice_updated,
            EngineMethod::GetBlobsV2 => self.timeouts.get_blobs,
            EngineMethod::ExchangeCapabilities => self.timeouts.exchange_capabilities,
            EngineMethod::EthSyncing => self.timeouts.eth_syncing,
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Issue a JSON-RPC call on the given lane.
    ///
    /// For [`Lane::Ordered`] the mutex is held **across** the HTTP round-trip.
    pub async fn call(
        &self,
        lane: Lane,
        method: EngineMethod,
        rpc_method: &str,
        params: Value,
    ) -> Result<Value, EngineError> {
        self.call_with(lane, method, rpc_method, params, RequestOverrides::default())
            .await
    }

    /// Acquire the ordered-lane mutex (held across admit + fcU HTTP, CC-33 F1).
    ///
    /// Callers that already hold this guard must use [`Self::call_with_ordered_held`]
    /// so they do not re-lock (would deadlock).
    pub async fn lock_ordered(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.ordered.lock().await
    }

    /// JSON-RPC call **without** taking the ordered mutex.
    ///
    /// # Safety (logical)
    ///
    /// Caller must already hold [`Self::lock_ordered`] for this transport for
    /// the entire duration of the future (admit + HTTP atomicity for fcU).
    pub async fn call_with_ordered_held(
        &self,
        method: EngineMethod,
        rpc_method: &str,
        params: Value,
    ) -> Result<Value, EngineError> {
        self.call_inner(
            method,
            rpc_method,
            params,
            RequestOverrides::default(),
        )
        .await
    }

    /// Like [`Self::call`], with explicit JWT `iat` and/or `Host` overrides.
    ///
    /// **IT / diagnostics only** (CC-30b). See [`RequestOverrides`]. Not for
    /// production method adapters.
    #[doc(hidden)]
    pub async fn call_with(
        &self,
        lane: Lane,
        method: EngineMethod,
        rpc_method: &str,
        params: Value,
        overrides: RequestOverrides,
    ) -> Result<Value, EngineError> {
        match lane {
            Lane::Ordered => {
                // Guard held across the call — ordering MUST on the same wire.
                let _guard = self.ordered.lock().await;
                self.call_inner(method, rpc_method, params, overrides).await
            }
            Lane::Fastpath => {
                let _permit =
                    self.fastpath
                        .acquire()
                        .await
                        .map_err(|_| EngineError::Transport {
                            detail: "fastpath semaphore closed".into(),
                        })?;
                self.call_inner(method, rpc_method, params, overrides).await
            }
            Lane::Upcheck => {
                let _permit = self
                    .upcheck
                    .acquire()
                    .await
                    .map_err(|_| EngineError::Transport {
                        detail: "upcheck semaphore closed".into(),
                    })?;
                self.call_inner(method, rpc_method, params, overrides).await
            }
        }
    }

    async fn call_inner(
        &self,
        method: EngineMethod,
        rpc_method: &str,
        params: Value,
        overrides: RequestOverrides,
    ) -> Result<Value, EngineError> {
        let started = Instant::now();
        let timeout = self.timeout_for(method);
        let result = self
            .send_jsonrpc(rpc_method, params, timeout, method, overrides)
            .await;
        let elapsed = started.elapsed();

        let labels = MethodLabels {
            method: method.as_str().to_owned(),
        };
        if let Some(m) = &self.metrics {
            m.request_seconds
                .get_or_create(&labels)
                .observe(elapsed.as_secs_f64());
        }
        // Soft deadline: warn+count for methods on the attestation path; never abort.
        // fcU is off the attestation path (CC-33 /5) — excluded from the alarm so
        // an 8 s forkchoiceUpdated does not make the soft-deadline counter useless.
        if elapsed > self.soft_deadline && method != EngineMethod::ForkchoiceUpdatedV3 {
            if let Some(m) = &self.metrics {
                m.soft_deadline_exceeded.get_or_create(&labels).inc();
            }
            tracing::warn!(
                method = method.as_str(),
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                soft_deadline_ms = self.soft_deadline.as_secs_f64() * 1000.0,
                "engine call exceeded soft attestation deadline"
            );
        }

        if let Err(ref e) = result {
            if let Some(m) = &self.metrics {
                m.errors_total
                    .get_or_create(&ErrorCodeLabels {
                        code: e.metric_code().as_str().to_owned(),
                    })
                    .inc();
                if matches!(e, EngineError::Timeout { .. }) {
                    let labels = MethodLabels {
                        method: method.as_str().to_owned(),
                    };
                    m.transport_timeout.get_or_create(&labels).inc();
                }
            }
            // geth auth/vhost failures are plain-text HTTP bodies, never JSON-RPC
            // objects. Log the body **verbatim** at error! — the body *is* the
            // diagnosis (CC-30b / el-runbook).
            match e {
                EngineError::Http401 { body } => {
                    tracing::error!(
                        method = method.as_str(),
                        status = 401u16,
                        body = %body,
                        "HTTP 401 from EL (auth rejected)"
                    );
                }
                EngineError::Http403 { body } => {
                    tracing::error!(
                        method = method.as_str(),
                        status = 403u16,
                        body = %body,
                        "HTTP 403 from EL (host rejected)"
                    );
                }
                EngineError::ServerError {
                    data_err: Some(err),
                    message,
                } => {
                    // -32000 data.err is the only JSON-RPC code with a diagnostic string.
                    tracing::warn!(
                        method = method.as_str(),
                        message = %message,
                        data_err = %err,
                        "JSON-RPC -32000 server error"
                    );
                }
                _ => {}
            }
        }

        result
    }

    async fn send_jsonrpc(
        &self,
        rpc_method: &str,
        params: Value,
        timeout: Duration,
        method: EngineMethod,
        overrides: RequestOverrides,
    ) -> Result<Value, EngineError> {
        let id = self.next_id();
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": rpc_method,
            "params": params,
        });

        let token = match overrides.iat {
            Some(iat) => self.jwt.sign_iat(iat),
            None => self.jwt.sign_now(),
        }
        .map_err(|e| EngineError::Transport {
            detail: format!("jwt sign: {e}"),
        })?;

        let mut request = self
            .client
            .post(&self.endpoint)
            .timeout(timeout)
            .bearer_auth(token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body);

        if let Some(host) = overrides.host.as_deref() {
            // Override Host for vhost-rejection tests. reqwest rewrites Host from
            // the URL unless we set it explicitly after building the request.
            request = request.header(reqwest::header::HOST, host);
        }

        let response = match request.send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() || e.is_request() && e.to_string().contains("timed out") => {
                return Err(EngineError::Timeout {
                    method: method.as_str().to_owned(),
                });
            }
            Err(e) => {
                return Err(EngineError::Transport {
                    detail: e.to_string(),
                });
            }
        };

        let status = response.status().as_u16();
        // Non-2xx: always the 1 MiB default (auth/error pages). 2xx getBlobsV2:
        // raised ceiling so multi-blob Complete JSON fits (CC-37a F1).
        let max_bytes = if (200..300).contains(&status) {
            max_body_bytes_for_success(method)
        } else {
            MAX_BODY_BYTES
        };
        let bytes = read_body_capped(response, max_bytes).await?;

        if !(200..300).contains(&status) {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            return Err(EngineError::from_http_status(status, text));
        }

        parse_jsonrpc_envelope(&bytes)
    }
}

/// Read the response body with a hard byte ceiling **before any parse**.
///
/// Rejects oversized `Content-Length` up front; aborts mid-stream past the cap.
/// Accumulated body storage never exceeds `max_bytes` (chunk is discarded when
/// it would overflow). Full counting-`GlobalAlloc` proof is left to CC-22e-style
/// integration when needed; the invariant is structural here.
pub async fn read_body_capped(
    resp: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, EngineError> {
    if let Some(cl) = resp.content_length()
        && cl as usize > max_bytes
    {
        // Fail closed without reading a single body byte.
        return Err(EngineError::Decode {
            reason: format!("Content-Length {cl} exceeds max {max_bytes} bytes"),
        });
    }

    // Capacity ceiling = max_bytes so growth cannot reserve multi-MiB slabs.
    let mut out = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| EngineError::Transport {
            detail: format!("body stream: {e}"),
        })?;
        let next = out.len().saturating_add(chunk.len());
        if next > max_bytes {
            // Drop the oversized chunk; keep only what was already under the cap.
            debug_assert!(out.len() <= max_bytes);
            return Err(EngineError::Decode {
                reason: format!("body exceeds max {max_bytes} bytes (got at least {next})"),
            });
        }
        out.extend_from_slice(&chunk);
        debug_assert!(out.len() <= max_bytes);
    }
    Ok(out)
}

/// Pure chunk-accumulation helper (unit-tested without HTTP).
///
/// Returns `Ok(bytes)` when the full stream fits, or `Err(held_len)` when a
/// chunk would push past `max_bytes` — `held_len` is always `≤ max_bytes`.
pub fn accumulate_body_capped(
    chunks: impl IntoIterator<Item = Vec<u8>>,
    max_bytes: usize,
) -> Result<Vec<u8>, usize> {
    let mut out = Vec::with_capacity(max_bytes.min(64 * 1024));
    for chunk in chunks {
        let next = out.len().saturating_add(chunk.len());
        if next > max_bytes {
            return Err(out.len());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Parse a JSON-RPC 2.0 success/error envelope from a capped body.
pub fn parse_jsonrpc_envelope(bytes: &[u8]) -> Result<Value, EngineError> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| EngineError::Decode {
        reason: format!("json: {e}"),
    })?;
    if let Some(err) = v.get("error") {
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(i64::MIN);
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_owned();
        let data_err = err.get("data").and_then(|d| {
            d.get("err")
                .and_then(|e| e.as_str())
                .map(str::to_owned)
                .or_else(|| d.as_str().map(str::to_owned))
        });
        return Err(EngineError::from_jsonrpc(code, message, data_err));
    }
    match v.get("result") {
        Some(result) => Ok(result.clone()),
        None => Err(EngineError::Decode {
            reason: "JSON-RPC envelope missing both result and error".into(),
        }),
    }
}

/// Shared handle.
pub type SharedTransport = Arc<EngineTransport>;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::methods::names;
    use crate::metrics::EngineMetrics;
    use prometheus_client::registry::Registry;
    use proptest::prelude::*;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    fn test_jwt() -> JwtSecret {
        JwtSecret::from_bytes([0x11; 32])
    }

    fn test_timeouts() -> TransportTimeouts {
        TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 2_000,
            forkchoice_updated_ms: 2_000,
            get_blobs_ms: 1_000,
            exchange_capabilities_ms: 1_000,
            eth_syncing_ms: 1_000,
            multiplier: 1.0,
        })
    }

    fn transport(url: &str, metrics: Option<EngineMetrics>) -> EngineTransport {
        EngineTransport::from_parts(
            url,
            test_jwt(),
            test_timeouts(),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            metrics,
        )
    }

    /// Stall `eth_syncing`, answer everything else immediately.
    struct StallEthSyncing;

    impl Respond for StallEthSyncing {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
            if method == names::ETH_SYNCING {
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(30))
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":false}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"status": "VALID"}
                }))
            }
        }
    }

    struct CountingResponder {
        in_flight: StdArc<AtomicUsize>,
        max_in_flight: StdArc<AtomicUsize>,
    }

    impl Respond for CountingResponder {
        fn respond(&self, _req: &Request) -> ResponseTemplate {
            let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let mut prev = self.max_in_flight.load(Ordering::SeqCst);
            while cur > prev {
                match self.max_in_flight.compare_exchange(
                    prev,
                    cur,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(p) => prev = p,
                }
            }
            std::thread::sleep(Duration::from_millis(30));
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": true
            }))
        }
    }

    #[tokio::test]
    async fn lanes_are_independent() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(StallEthSyncing)
            .mount(&server)
            .await;

        let t = StdArc::new(transport(&server.uri(), None));
        let t_up = StdArc::clone(&t);
        let upcheck = tokio::spawn(async move {
            t_up.call(
                Lane::Upcheck,
                EngineMethod::EthSyncing,
                names::ETH_SYNCING,
                json!([]),
            )
            .await
        });

        // Give the upcheck a head start so it holds the upcheck lane.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = Instant::now();
        let np = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([{}]),
            )
            .await;
        let elapsed = started.elapsed();
        assert!(np.is_ok(), "newPayload must complete: {np:?}");
        assert!(
            elapsed < Duration::from_secs(5),
            "ordered lane blocked by upcheck stall: {elapsed:?}"
        );

        // Abort the long upcheck so the test does not wait 30 s to exit.
        upcheck.abort();
    }

    #[tokio::test]
    async fn ordered_lane_serialises() {
        let server = MockServer::start().await;
        let in_flight = StdArc::new(AtomicUsize::new(0));
        let max_in_flight = StdArc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(CountingResponder {
                in_flight: StdArc::clone(&in_flight),
                max_in_flight: StdArc::clone(&max_in_flight),
            })
            .mount(&server)
            .await;

        let t = StdArc::new(transport(&server.uri(), None));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let t = StdArc::clone(&t);
            handles.push(tokio::spawn(async move {
                t.call(
                    Lane::Ordered,
                    EngineMethod::NewPayloadV4,
                    names::NEW_PAYLOAD_V4,
                    json!([]),
                )
                .await
            }));
        }
        for h in handles {
            let r = h.await.unwrap();
            assert!(r.is_ok(), "{r:?}");
        }
        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            1,
            "ordered lane must serialise; saw overlap"
        );
    }

    #[tokio::test]
    async fn body_cap_precedes_parse() {
        let server = MockServer::start().await;
        let big = vec![b'x'; 4 * 1024 * 1024]; // 4 MiB
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_bytes(big))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let err = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([]),
            )
            .await
            .expect_err("4 MiB body must fail");
        assert!(
            matches!(err, EngineError::Decode { .. }),
            "expected Decode, got {err:?}"
        );
        match &err {
            EngineError::Decode { reason } => {
                assert!(
                    reason.contains("exceeds max") || reason.contains("Content-Length"),
                    "reason={reason}"
                );
                // Error payload is a short reason string — not the 4 MiB body.
                assert!(
                    reason.len() < 256,
                    "Decode must not retain the oversized body"
                );
            }
            _ => unreachable!(),
        }

        // Structural cap: chunk accumulation never holds more than max_bytes.
        // (Full CountingAlloc GlobalAlloc harness is Phase-2 CC-22e style and
        // not cheap in this unit binary; the invariant is tested here directly.)
        let held = accumulate_body_capped(
            [vec![0u8; MAX_BODY_BYTES - 100], vec![0u8; 200]],
            MAX_BODY_BYTES,
        )
        .expect_err("second chunk must trip the cap");
        assert!(
            held <= MAX_BODY_BYTES,
            "held body after cap trip must be ≤ max (held={held})"
        );
        assert_eq!(held, MAX_BODY_BYTES - 100);

        // Exact-fit stream is OK.
        let ok = accumulate_body_capped([vec![1u8; 512], vec![2u8; 512]], 1024).expect("exact fit");
        assert_eq!(ok.len(), 1024);
    }

    /// CC-37a F1: getBlobsV2 2xx uses the raised ceiling; ordered stays at 1 MiB.
    #[test]
    fn get_blobs_success_body_cap_exceeds_default() {
        const {
            assert!(MAX_BODY_BYTES == 1024 * 1024);
            assert!(MAX_BODY_BYTES_GET_BLOBS > MAX_BODY_BYTES);
            // 21 blobs × ~0.28 MiB ≈ 6 MiB; 16 MiB ceiling covers Hoodi Complete.
            assert!(MAX_BODY_BYTES_GET_BLOBS >= 16 * 1024 * 1024);
        }
        assert_eq!(
            max_body_bytes_for_success(EngineMethod::GetBlobsV2),
            MAX_BODY_BYTES_GET_BLOBS
        );
        assert_eq!(
            max_body_bytes_for_success(EngineMethod::NewPayloadV4),
            MAX_BODY_BYTES
        );
    }

    /// A multi-MiB 2xx getBlobs body is accepted (would fail under 1 MiB).
    #[tokio::test]
    async fn get_blobs_accepts_multi_mib_complete_body() {
        let server = MockServer::start().await;
        // ~2.5 MiB JSON null-ish payload: large enough to trip 1 MiB, under 16 MiB.
        // Real Complete arrays are larger per blob; this proves the method ceiling.
        let big_hex = "00".repeat(1_250_000); // 2.5e6 hex chars ≈ 2.5 MiB in the string
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":[{{"blob":"0x{big_hex}","proofs":[]}}]}}"#
        );
        assert!(body.len() > MAX_BODY_BYTES);
        assert!(body.len() < MAX_BODY_BYTES_GET_BLOBS);
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        // Decode of proofs will fail (wrong length) — we only care that the
        // transport **reads** past 1 MiB rather than Decode-cap-tripping first.
        let result = t
            .call(
                Lane::Fastpath,
                EngineMethod::GetBlobsV2,
                names::GET_BLOBS_V2,
                json!([[]]),
            )
            .await;
        // Transport succeeded past the body cap; result is Ok(Value) or a later
        // parse shape. Cap failure would be Decode with "exceeds max".
        match result {
            Ok(_) => {}
            Err(EngineError::Decode { reason }) => {
                assert!(
                    !reason.contains("exceeds max") && !reason.contains("Content-Length"),
                    "must not fail the 1 MiB body cap on getBlobs 2xx: {reason}"
                );
            }
            Err(e) => panic!("unexpected transport error: {e:?}"),
        }
    }

    /// Non-2xx on the getBlobs method still uses the 1 MiB default (auth pages).
    #[tokio::test]
    async fn get_blobs_error_body_stays_at_one_mib() {
        let server = MockServer::start().await;
        let big = vec![b'x'; 4 * 1024 * 1024];
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_bytes(big))
            .mount(&server)
            .await;

        let t = transport(&server.uri(), None);
        let err = t
            .call(
                Lane::Fastpath,
                EngineMethod::GetBlobsV2,
                names::GET_BLOBS_V2,
                json!([[]]),
            )
            .await
            .expect_err("4 MiB 401 must trip the default cap");
        assert!(
            matches!(err, EngineError::Decode { .. }),
            "expected Decode body cap, got {err:?}"
        );
    }

    /// Hostile 4xx/5xx bodies never panic — pure classification (≥ 1000 cases).
    #[test]
    fn non_json_4xx_never_panics() {
        // Seed corpus: HTML, empty, truncated, invalid-UTF-8.
        let seeds: Vec<(u16, Vec<u8>)> = {
            let mut v = Vec::new();
            for status in [401u16, 403, 500] {
                v.push((status, b"<html>nope</html>".to_vec()));
                v.push((status, Vec::new()));
                v.push((status, b"{\"jsonrpc\":".to_vec()));
                v.push((status, vec![0xff, 0xfe, 0xfd]));
                v.push((status, b"missing token".to_vec()));
                v.push((status, b"stale token".to_vec()));
                v.push((status, b"future token".to_vec()));
                v.push((status, b"invalid host specified".to_vec()));
            }
            v
        };

        let mut cases = seeds;
        // Deterministic expansion to ≥ 1000.
        while cases.len() < 1_000 {
            let i = cases.len();
            let status = [401u16, 403, 404, 500, 503][i % 5];
            let mut body = format!("hostile-{i}-").into_bytes();
            if i.is_multiple_of(11) {
                body.extend_from_slice(&[0xff, 0x00, 0xfe]);
            }
            if i.is_multiple_of(13) {
                body.extend_from_slice(b"<html>");
            }
            cases.push((status, body));
        }
        assert!(cases.len() >= 1_000);

        for (status, body) in &cases {
            // Classification must never panic and always produce an error.
            let text = String::from_utf8_lossy(body).into_owned();
            let err = EngineError::from_http_status(*status, text);
            let _ = err.metric_code();
            let _ = err.retry_class();
            let _ = err.to_string();
            // 2xx path: parse of hostile body is also non-panicking.
            let _ = parse_jsonrpc_envelope(body);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn non_json_4xx_proptest(status in prop_oneof![Just(401u16), Just(403u16), Just(500u16)],
                               body in prop::collection::vec(any::<u8>(), 0..512)) {
            let text = String::from_utf8_lossy(&body).into_owned();
            let err = EngineError::from_http_status(status, text);
            let _ = (err.metric_code(), err.retry_class(), err.to_string());
            let _ = parse_jsonrpc_envelope(&body);
        }
    }

    #[tokio::test]
    async fn non_json_4xx_http_roundtrip_sample() {
        // A handful of real HTTP round-trips over hostile bodies.
        let samples: Vec<(u16, &[u8])> = vec![
            (401, b"missing token"),
            (401, b"<html>auth</html>"),
            (403, b"invalid host specified"),
            (403, b""),
            (500, b"{\"jsonrpc\":"),
            (500, &[0xff, 0xfe, 0xfd]),
        ];
        for (status, body) in samples {
            let server = MockServer::start().await;
            Mock::given(http_method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_bytes(body.to_vec()))
                .mount(&server)
                .await;
            let t = transport(&server.uri(), None);
            let result = t
                .call(
                    Lane::Ordered,
                    EngineMethod::NewPayloadV4,
                    names::NEW_PAYLOAD_V4,
                    json!([]),
                )
                .await;
            assert!(result.is_err(), "status={status} must be Err");
        }
    }

    #[tokio::test]
    async fn timeout_knobs_soft_vs_transport() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(50))
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":true})),
            )
            .mount(&server)
            .await;

        let t = EngineTransport::from_parts(
            server.uri(),
            test_jwt(),
            test_timeouts(),
            Duration::from_millis(10),
            Some(metrics.clone()),
        );
        let r = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([]),
            )
            .await;
        assert!(r.is_ok(), "soft deadline must not abort: {r:?}");
        let soft = metrics
            .soft_deadline_exceeded
            .get_or_create(&MethodLabels {
                method: "newPayloadV4".into(),
            })
            .get();
        assert!(soft >= 1, "soft deadline counter must increment");

        let server2 = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":true})),
            )
            .mount(&server2)
            .await;
        let short = TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 100,
            forkchoice_updated_ms: 100,
            get_blobs_ms: 100,
            exchange_capabilities_ms: 100,
            eth_syncing_ms: 100,
            multiplier: 1.0,
        });
        let t2 = EngineTransport::from_parts(
            server2.uri(),
            test_jwt(),
            short,
            Duration::from_secs(60),
            Some(metrics.clone()),
        );
        let err = t2
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([]),
            )
            .await
            .expect_err("must time out");
        assert!(
            matches!(err, EngineError::Timeout { .. }),
            "expected Timeout, got {err:?}"
        );
        let to = metrics
            .transport_timeout
            .get_or_create(&MethodLabels {
                method: "newPayloadV4".into(),
            })
            .get();
        assert!(to >= 1, "transport timeout counter must increment");
    }

    #[tokio::test]
    async fn jwt_not_leaked_in_logs() {
        use std::io::{self, Write};
        use std::sync::{Arc, Mutex};
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

        let secret = JwtSecret::from_bytes([0x42; 32]);
        let secret_hex = secret.to_hex();
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc":"2.0","id":1,"result":true
            })))
            .mount(&server)
            .await;

        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = BufferWriter {
            inner: Arc::clone(&buf),
        };
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new("trace"))
            .with(fmt::layer().with_writer(writer).with_ansi(false));
        let t = EngineTransport::from_parts(
            server.uri(),
            secret,
            test_timeouts(),
            Duration::from_secs(4),
            None,
        );

        let _guard = tracing::subscriber::set_default(subscriber);
        let _ = t
            .call(
                Lane::Ordered,
                EngineMethod::NewPayloadV4,
                names::NEW_PAYLOAD_V4,
                json!([]),
            )
            .await;
        let s = JwtSecret::from_bytes([0x42; 32]);
        tracing::trace!(jwt = ?s, "debug jwt");

        let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        assert!(
            !captured.contains(&secret_hex),
            "secret hex leaked into logs"
        );
        assert!(
            !captured.contains("Jwt(["),
            "raw secret bytes must not appear"
        );
        assert!(
            !captured.contains("eyJ"),
            "JWS token leaked into logs:\n{captured}"
        );
    }
}
