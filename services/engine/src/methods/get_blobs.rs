//! `engine_getBlobsV2` adapter (CC-37a / Architecture §5.3).
//!
//! - Rides the **fastpath** lane (concurrency 2, 1 s transport timeout).
//! - Response is **all-or-nothing**: JSON `null` is a [`GetBlobsOutcome::Miss`],
//!   never an [`EngineError`].
//! - Three legitimate null causes ([`NullCause`]) are classified and counted as
//!   `cc_engine_getblobs_total{result="miss"}`. None of them moves the CC-36a
//!   state machine.
//! - A transport timeout is an **error** (`result="error"`), distinct from miss.
//!
//! Cell reconstruction / transpose / subscribe filter is **CC-37b** (composed on
//! the fastpath worker Complete path). The ninth contract inject is **CC-38**.

use std::time::Instant;

use serde_json::{Value, json};

use crate::errors::EngineError;
use crate::methods::names;
use crate::metrics::{
    EngineMethod, EngineMetrics, FastpathStage, FastpathStageLabels, GetBlobsResult,
    GetBlobsResultLabels,
};
use crate::transport::{EngineTransport, Lane};

/// Engine API / geth MUST support at least this many versioned hashes per call.
///
/// geth: `if len(hashes) > 128 { return TooLargeRequest (-38004) }`.
/// Hoodi `max_blobs_per_block` is ≤ 21 (CC-1G runtime), so `-38004` is
/// **unreachable** from a correctly gated request (CC-37 /8).
pub const GET_BLOBS_V2_MAX_HASHES: usize = 128;

/// KZG versioned-hash version byte (`VERSIONED_HASH_VERSION_KZG = 0x01`).
pub const VERSIONED_HASH_VERSION_KZG: u8 = 0x01;

/// Bytes per blob (`BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB`).
pub const BYTES_PER_BLOB: usize = 32 * 4096;

/// Cell proofs per blob in `BlobAndProofV2` (`CELLS_PER_EXT_BLOB` = 128).
pub const CELL_PROOFS_PER_BLOB: usize = 128;

/// Bytes per KZG proof / commitment.
pub const BYTES_PER_KZG_PROOF: usize = 48;

/// Why `engine_getBlobsV2` returned JSON `null`.
///
/// Always a **miss**, never an error. Three arms only — a fourth cause from a
/// future EL must extend this enum (compile error) rather than fall into a
/// silent catch-all (CC-37a AC: no `_ =>` in the null-cause match).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullCause {
    /// EL holds a subset of the requested blobs; V2 refuses a partial response.
    PartialHit,
    /// EL blob pool pruned (or never held) the blobs; we are not necessarily a
    /// peer of the proposer's mempool path.
    PrunedPool,
    /// EL head is below Osaka — geth returns `nil, nil` **before** any pool
    /// lookup (delta 18). During EL catch-up the fast path is simply dead.
    ElHeadPreOsaka,
}

impl NullCause {
    /// Stable name for logs / tests.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PartialHit => "partial_hit",
            Self::PrunedPool => "pruned_pool",
            Self::ElHeadPreOsaka => "el_head_pre_osaka",
        }
    }
}

/// Outcome of a successful transport round-trip (no [`EngineError`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetBlobsOutcome {
    /// Full set of blobs + cell proofs (all-or-nothing hit).
    Complete(Vec<BlobAndProofV2>),
    /// JSON `null` (or classified partial) — "not now", never an error.
    Miss(NullCause),
}

/// Engine API `BlobAndProofV2` (Osaka).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobAndProofV2 {
    /// Raw blob bytes (`BYTES_PER_BLOB`).
    pub blob: Vec<u8>,
    /// Exactly [`CELL_PROOFS_PER_BLOB`] cell proofs (48 B each).
    pub proofs: Vec<Vec<u8>>,
}

/// Context used to classify a JSON `null` (or partial) response.
///
/// The wire result is only `null | Array` — pre-Osaka is known from the EL head
/// / fork schedule, not from the body. Partial vs pruned is a diagnostic split
/// for tests and future EL extensions; production post-Osaka null defaults to
/// [`NullCause::PrunedPool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullContext {
    /// Caller asserts EL head is pre-Osaka (delta 18).
    ElHeadPreOsaka,
    /// Caller asserts a partial-hit scenario (subset present).
    PartialHit,
    /// Default post-Osaka pool miss.
    PrunedPool,
}

/// Classify a null / partial response into a named [`NullCause`].
#[must_use]
pub fn classify_null(ctx: NullContext) -> NullCause {
    match ctx {
        NullContext::ElHeadPreOsaka => NullCause::ElHeadPreOsaka,
        NullContext::PartialHit => NullCause::PartialHit,
        NullContext::PrunedPool => NullCause::PrunedPool,
    }
}

/// Spec `kzg_commitment_to_versioned_hash`.
///
/// `VERSIONED_HASH_VERSION_KZG || sha256(commitment)[1:]`.
///
/// Implemented locally (sha2) so versioned-hash derivation does not require a
/// KZG backend; CC-37b binds blobs via `blob_to_kzg_commitment` separately.
#[must_use]
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(commitment);
    let mut out = [0u8; 32];
    out[0] = VERSIONED_HASH_VERSION_KZG;
    out[1..].copy_from_slice(&digest[1..]);
    out
}

/// Map a list of 48-byte KZG commitments to versioned hashes.
#[must_use]
pub fn versioned_hashes_from_commitments(commitments: &[[u8; 48]]) -> Vec<[u8; 32]> {
    commitments
        .iter()
        .map(kzg_commitment_to_versioned_hash)
        .collect()
}

/// Build JSON-RPC params: single array of `0x`-prefixed versioned hashes.
pub fn build_get_blobs_v2_params(versioned_hashes: &[[u8; 32]]) -> Value {
    let hashes: Vec<Value> = versioned_hashes
        .iter()
        .map(|h| Value::String(bytes_to_hex(h)))
        .collect();
    json!([hashes])
}

/// Issue `engine_getBlobsV2` on the fastpath lane.
///
/// - JSON `null` → [`GetBlobsOutcome::Miss`] (never `Err`).
/// - Full array → [`GetBlobsOutcome::Complete`].
/// - Array with nulls / wrong length → miss with [`NullCause::PartialHit`].
/// - Transport / decode hard failures → `Err` and `result="error"`.
/// - `-38004` TooLargeRequest is treated as a programming error (unreachable
///   when gated by `max_blobs_per_block`); still counted as error so it is
///   visible if the gate regresses.
pub async fn get_blobs_v2(
    transport: &EngineTransport,
    metrics: Option<&EngineMetrics>,
    versioned_hashes: &[[u8; 32]],
    null_ctx: NullContext,
) -> Result<GetBlobsOutcome, EngineError> {
    let started = Instant::now();
    let params = build_get_blobs_v2_params(versioned_hashes);
    let result = transport
        .call(
            Lane::Fastpath,
            EngineMethod::GetBlobsV2,
            names::GET_BLOBS_V2,
            params,
        )
        .await;
    let elapsed = started.elapsed().as_secs_f64();
    if let Some(m) = metrics {
        m.fastpath_seconds
            .get_or_create(&FastpathStageLabels {
                stage: FastpathStage::Fetch.as_str().to_owned(),
            })
            .observe(elapsed);
    }

    match result {
        Ok(value) => {
            let outcome = decode_get_blobs_v2_result(&value, versioned_hashes.len(), null_ctx)?;
            observe_getblobs_outcome(metrics, &outcome);
            Ok(outcome)
        }
        Err(EngineError::TooLargeRequest { message }) => {
            // CC-37 /8: asserted unreachable when gated by runtime max_blobs.
            tracing::error!(
                %message,
                n_hashes = versioned_hashes.len(),
                "engine_getBlobsV2 returned -38004 TooLargeRequest; bound gate regressed"
            );
            observe_getblobs_result(metrics, GetBlobsResult::Error);
            Err(EngineError::TooLargeRequest { message })
        }
        Err(e) => {
            observe_getblobs_result(metrics, GetBlobsResult::Error);
            Err(e)
        }
    }
}

/// Decode a JSON-RPC `result` for `engine_getBlobsV2`.
pub fn decode_get_blobs_v2_result(
    value: &Value,
    expected_len: usize,
    null_ctx: NullContext,
) -> Result<GetBlobsOutcome, EngineError> {
    match value {
        Value::Null => Ok(GetBlobsOutcome::Miss(classify_null(null_ctx))),
        Value::Array(items) => {
            // V1-style partial arrays (null elements) or length mismatch are a
            // partial hit — still "not now", not a transport error.
            if items.len() != expected_len {
                return Ok(GetBlobsOutcome::Miss(NullCause::PartialHit));
            }
            if items.iter().any(Value::is_null) {
                return Ok(GetBlobsOutcome::Miss(NullCause::PartialHit));
            }
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                out.push(decode_blob_and_proof_v2(item, i)?);
            }
            Ok(GetBlobsOutcome::Complete(out))
        }
        other => Err(EngineError::Decode {
            reason: format!(
                "engine_getBlobsV2 result must be null or array, got {}",
                value_kind(other)
            ),
        }),
    }
}

fn decode_blob_and_proof_v2(value: &Value, index: usize) -> Result<BlobAndProofV2, EngineError> {
    let obj = value.as_object().ok_or_else(|| EngineError::Decode {
        reason: format!("BlobAndProofV2[{index}] is not an object"),
    })?;
    let blob_hex = obj
        .get("blob")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EngineError::Decode {
            reason: format!("BlobAndProofV2[{index}].blob missing"),
        })?;
    let blob = hex_to_bytes(blob_hex).map_err(|e| EngineError::Decode {
        reason: format!("BlobAndProofV2[{index}].blob: {e}"),
    })?;
    if blob.len() != BYTES_PER_BLOB {
        return Err(EngineError::Decode {
            reason: format!(
                "BlobAndProofV2[{index}].blob length {} != {BYTES_PER_BLOB}",
                blob.len()
            ),
        });
    }
    let proofs_val = obj
        .get("proofs")
        .and_then(|v| v.as_array())
        .ok_or_else(|| EngineError::Decode {
            reason: format!("BlobAndProofV2[{index}].proofs missing or not array"),
        })?;
    if proofs_val.len() != CELL_PROOFS_PER_BLOB {
        return Err(EngineError::Decode {
            reason: format!(
                "BlobAndProofV2[{index}].proofs length {} != {CELL_PROOFS_PER_BLOB}",
                proofs_val.len()
            ),
        });
    }
    let mut proofs = Vec::with_capacity(CELL_PROOFS_PER_BLOB);
    for (j, p) in proofs_val.iter().enumerate() {
        let s = p.as_str().ok_or_else(|| EngineError::Decode {
            reason: format!("BlobAndProofV2[{index}].proofs[{j}] not hex string"),
        })?;
        let bytes = hex_to_bytes(s).map_err(|e| EngineError::Decode {
            reason: format!("BlobAndProofV2[{index}].proofs[{j}]: {e}"),
        })?;
        if bytes.len() != BYTES_PER_KZG_PROOF {
            return Err(EngineError::Decode {
                reason: format!(
                    "BlobAndProofV2[{index}].proofs[{j}] length {} != {BYTES_PER_KZG_PROOF}",
                    bytes.len()
                ),
            });
        }
        proofs.push(bytes);
    }
    Ok(BlobAndProofV2 { blob, proofs })
}

/// Observe `cc_engine_getblobs_total` for a successful outcome.
pub fn observe_getblobs_outcome(metrics: Option<&EngineMetrics>, outcome: &GetBlobsOutcome) {
    match outcome {
        GetBlobsOutcome::Complete(_) => observe_getblobs_result(metrics, GetBlobsResult::Complete),
        GetBlobsOutcome::Miss(cause) => {
            // Exhaustive three-way match — no `_ =>` (CC-37a AC).
            match cause {
                NullCause::PartialHit => observe_getblobs_result(metrics, GetBlobsResult::Miss),
                NullCause::PrunedPool => observe_getblobs_result(metrics, GetBlobsResult::Miss),
                NullCause::ElHeadPreOsaka => observe_getblobs_result(metrics, GetBlobsResult::Miss),
            }
        }
    }
}

fn observe_getblobs_result(metrics: Option<&EngineMetrics>, result: GetBlobsResult) {
    if let Some(m) = metrics {
        m.getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: result.as_str().to_owned(),
            })
            .inc();
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if !hex.len().is_multiple_of(2) {
        return Err(format!("odd-length hex ({})", hex.len()));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex byte {b}")),
    }
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    fn test_jwt() -> JwtSecret {
        JwtSecret::from_bytes([0x22; 32])
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

    fn metrics() -> EngineMetrics {
        let mut registry = Registry::default();
        EngineMetrics::register(&mut registry)
    }

    fn sample_hash(n: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = VERSIONED_HASH_VERSION_KZG;
        h[31] = n;
        h
    }

    #[test]
    fn null_is_miss_with_named_causes() {
        for (ctx, cause) in [
            (NullContext::PartialHit, NullCause::PartialHit),
            (NullContext::PrunedPool, NullCause::PrunedPool),
            (NullContext::ElHeadPreOsaka, NullCause::ElHeadPreOsaka),
        ] {
            let out = decode_get_blobs_v2_result(&Value::Null, 1, ctx).unwrap();
            assert_eq!(out, GetBlobsOutcome::Miss(cause));
        }
    }

    #[test]
    fn partial_array_is_partial_hit_miss() {
        // V1-style: array with null elements → partial hit, not a transport error.
        let v = json!([null, null]);
        let out = decode_get_blobs_v2_result(&v, 2, NullContext::PrunedPool).unwrap();
        assert_eq!(out, GetBlobsOutcome::Miss(NullCause::PartialHit));
        // Length mismatch is also partial.
        let v2 = json!([]);
        let out2 = decode_get_blobs_v2_result(&v2, 1, NullContext::PrunedPool).unwrap();
        assert_eq!(out2, GetBlobsOutcome::Miss(NullCause::PartialHit));
    }

    #[test]
    fn versioned_hash_sets_version_byte() {
        let c = [0xab_u8; 48];
        let h = kzg_commitment_to_versioned_hash(&c);
        assert_eq!(h[0], VERSIONED_HASH_VERSION_KZG);
        // Different commitments → different hashes (sanity).
        let c2 = [0xcd_u8; 48];
        assert_ne!(kzg_commitment_to_versioned_hash(&c2), h);
    }

    #[test]
    fn params_are_single_array_of_hex_hashes() {
        let h = sample_hash(7);
        let p = build_get_blobs_v2_params(&[h]);
        let arr = p.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        let hashes = arr[0].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        let s = hashes[0].as_str().unwrap();
        assert!(s.starts_with("0x"));
        assert_eq!(s.len(), 2 + 64);
    }

    /// CC-37 /2: null response increments miss, not error.
    #[tokio::test]
    async fn get_blobs_null_counts_as_miss() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": null
            })))
            .mount(&server)
            .await;

        let m = metrics();
        let t = transport(&server.uri(), Some(m.clone()));
        let out = get_blobs_v2(&t, Some(&m), &[sample_hash(1)], NullContext::PrunedPool)
            .await
            .expect("null is Ok(Miss)");
        assert_eq!(out, GetBlobsOutcome::Miss(NullCause::PrunedPool));
        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        let err = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "error".into(),
            })
            .get();
        assert!(miss >= 1);
        assert_eq!(err, 0);
    }

    /// Timeout is an error, not a miss.
    #[tokio::test]
    async fn get_blobs_timeout_counts_as_error() {
        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":null})),
            )
            .mount(&server)
            .await;

        let m = metrics();
        // Short get_blobs timeout for a fast test.
        let short = TransportTimeouts::from_knobs(&TimeoutKnobs {
            new_payload_ms: 8_000,
            forkchoice_updated_ms: 8_000,
            get_blobs_ms: 100,
            exchange_capabilities_ms: 1_000,
            eth_syncing_ms: 1_000,
            multiplier: 1.0,
        });
        let t = EngineTransport::from_parts(
            server.uri(),
            test_jwt(),
            short,
            Duration::from_secs(60),
            Some(m.clone()),
        );
        let err = get_blobs_v2(&t, Some(&m), &[sample_hash(1)], NullContext::PrunedPool)
            .await
            .expect_err("must time out");
        assert!(matches!(err, EngineError::Timeout { .. }));
        let miss = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "miss".into(),
            })
            .get();
        let error = m
            .getblobs_total
            .get_or_create(&GetBlobsResultLabels {
                result: "error".into(),
            })
            .get();
        assert_eq!(miss, 0);
        assert!(error >= 1);
        let to = m
            .transport_timeout
            .get_or_create(&crate::metrics::MethodLabels {
                method: "getBlobsV2".into(),
            })
            .get();
        assert!(to >= 1);
    }

    /// Fastpath stall must not block ordered-lane newPayload (CC-37 /10).
    #[tokio::test]
    async fn stalled_get_blobs_does_not_delay_new_payload() {
        struct StallGetBlobs {
            np_hits: Arc<AtomicUsize>,
        }
        impl Respond for StallGetBlobs {
            fn respond(&self, req: &Request) -> ResponseTemplate {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
                if method == names::GET_BLOBS_V2 {
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(5))
                        .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":null}))
                } else {
                    self.np_hits.fetch_add(1, Ordering::SeqCst);
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

        let server = MockServer::start().await;
        let np_hits = Arc::new(AtomicUsize::new(0));
        Mock::given(http_method("POST"))
            .respond_with(StallGetBlobs {
                np_hits: Arc::clone(&np_hits),
            })
            .mount(&server)
            .await;

        let t = Arc::new(transport(&server.uri(), None));
        let t_blob = Arc::clone(&t);
        let blobs = tokio::spawn(async move {
            get_blobs_v2(
                t_blob.as_ref(),
                None,
                &[sample_hash(1)],
                NullContext::PrunedPool,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

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
            elapsed < Duration::from_secs(2),
            "ordered lane blocked by getBlobs stall: {elapsed:?}"
        );
        assert!(np_hits.load(Ordering::SeqCst) >= 1);
        blobs.abort();
    }
}
