//! `engine_newPayloadV4` adapter (CC-32b / Architecture §1.2, §3.4, §4.3).
//!
//! Decodes the chain-supplied SSZ `ExecutionPayload` (Mainnet preset), encodes
//! the Engine API JSON/hex object **once** here, issues the ordered-lane call,
//! and maps `PayloadStatusV1` onto the closed metric label set.

use std::time::Instant;

use cc_types::execution::ExecutionPayload;
use cc_types::operations::Withdrawal;
use cc_types::preset::{Mainnet, Preset};
use cc_types::primitives::{ExecutionAddress, Root};
use serde_json::{Value, json};
use ssz::Decode;

use crate::errors::EngineError;
use crate::methods::names;
use crate::metrics::{EngineMethod, EngineMetrics, PayloadStatus, PayloadStatusLabels};
use crate::transport::{EngineTransport, Lane};
use crate::version::{ElForkSchedule, observe_el_unsupported_fork, resolve_new_payload_method};

/// Decode SSZ → JSON-hex encode → ordered-lane `engine_newPayloadV4`.
///
/// Preset is hard-wired [`Mainnet`] (≠13/5 / `services/chain/src/main.rs:240`).
pub async fn new_payload_v4(
    transport: &EngineTransport,
    schedule: &ElForkSchedule,
    metrics: Option<&EngineMetrics>,
    ssz: &[u8],
    versioned_hashes: &[Vec<u8>],
    parent_beacon_block_root: &[u8],
    execution_requests: &[Vec<u8>],
) -> Result<DecodedPayloadStatus, EngineError> {
    let encode_started = Instant::now();

    let payload =
        ExecutionPayload::<Mainnet>::from_ssz_bytes(ssz).map_err(|e| EngineError::Decode {
            reason: format!("ExecutionPayload SSZ: {e:?}"),
        })?;

    let params = build_new_payload_v4_params(
        &payload,
        versioned_hashes,
        parent_beacon_block_root,
        execution_requests,
    )?;

    if let Some(m) = metrics {
        m.encode_seconds
            .get_or_create(&crate::metrics::MethodLabels {
                method: EngineMethod::NewPayloadV4.as_str().to_owned(),
            })
            .observe(encode_started.elapsed().as_secs_f64());
    }

    let method = resolve_new_payload_method(payload.timestamp, schedule, metrics).map_err(|e| {
        EngineError::UnsupportedFork {
            message: e.to_string(),
        }
    })?;
    debug_assert_eq!(method, names::NEW_PAYLOAD_V4);

    let result = transport
        .call(
            Lane::Ordered,
            EngineMethod::NewPayloadV4,
            names::NEW_PAYLOAD_V4,
            params,
        )
        .await;

    match result {
        Ok(value) => {
            let status = decode_payload_status(&value, /*allow_accepted*/ true)?;
            observe_payload_status(metrics, EngineMethod::NewPayloadV4, &status);
            Ok(status)
        }
        Err(EngineError::UnsupportedFork { message }) => {
            observe_el_unsupported_fork(&message, metrics);
            Err(EngineError::UnsupportedFork { message })
        }
        Err(e) => Err(e),
    }
}

/// Build the four-element `params` array for `engine_newPayloadV4`.
pub fn build_new_payload_v4_params<P: Preset>(
    payload: &ExecutionPayload<P>,
    versioned_hashes: &[Vec<u8>],
    parent_beacon_block_root: &[u8],
    execution_requests: &[Vec<u8>],
) -> Result<Value, EngineError> {
    let exec = encode_execution_payload_v3(payload)?;
    for (i, h) in versioned_hashes.iter().enumerate() {
        if h.len() != 32 {
            return Err(EngineError::Decode {
                reason: format!("versioned_hashes[{i}] must be 32 bytes, got {}", h.len()),
            });
        }
    }
    let hashes: Vec<Value> = versioned_hashes
        .iter()
        .map(|h| Value::String(bytes_to_hex(h)))
        .collect();
    if parent_beacon_block_root.len() != 32 {
        return Err(EngineError::Decode {
            reason: format!(
                "parent_beacon_block_root must be 32 bytes, got {}",
                parent_beacon_block_root.len()
            ),
        });
    }
    let parent = bytes_to_hex(parent_beacon_block_root);
    let requests: Vec<Value> = execution_requests
        .iter()
        .map(|r| Value::String(bytes_to_hex(r)))
        .collect();
    Ok(json!([exec, hashes, parent, requests]))
}

/// Encode `ExecutionPayload` as the Engine API `ExecutionPayloadV3` JSON object.
pub fn encode_execution_payload_v3<P: Preset>(
    payload: &ExecutionPayload<P>,
) -> Result<Value, EngineError> {
    let transactions: Vec<Value> = payload
        .transactions
        .iter()
        .map(|tx| Value::String(bytes_to_hex(tx.as_ref())))
        .collect();
    let withdrawals: Vec<Value> = payload.withdrawals.iter().map(encode_withdrawal).collect();

    Ok(json!({
        "parentHash": root_to_hex(&payload.parent_hash),
        "feeRecipient": address_to_hex(&payload.fee_recipient),
        "stateRoot": root_to_hex(&payload.state_root),
        "receiptsRoot": root_to_hex(&payload.receipts_root),
        "logsBloom": bytes_to_hex(payload.logs_bloom.as_ref()),
        "prevRandao": root_to_hex(&payload.prev_randao),
        "blockNumber": quantity(payload.block_number),
        "gasLimit": quantity(payload.gas_limit),
        "gasUsed": quantity(payload.gas_used),
        "timestamp": quantity(payload.timestamp),
        "extraData": bytes_to_hex(payload.extra_data.as_ref()),
        "baseFeePerGas": quantity_u256(&payload.base_fee_per_gas),
        "blockHash": root_to_hex(&payload.block_hash),
        "transactions": transactions,
        "withdrawals": withdrawals,
        "blobGasUsed": quantity(payload.blob_gas_used),
        "excessBlobGas": quantity(payload.excess_blob_gas),
    }))
}

fn encode_withdrawal(w: &Withdrawal) -> Value {
    json!({
        "index": quantity(w.index),
        "validatorIndex": quantity(w.validator_index.as_u64()),
        "address": address_to_hex(&w.address),
        "amount": quantity(w.amount.as_u64()),
    })
}

/// Decoded Engine API payload status (wire strings → closed enum).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPayloadStatus {
    pub status: PayloadStatus,
    pub latest_valid_hash: Option<[u8; 32]>,
    pub validation_error: Option<String>,
}

impl DecodedPayloadStatus {
    /// Wire status string (VALID | INVALID | …).
    #[must_use]
    pub fn status_str(&self) -> &'static str {
        self.status.as_str()
    }
}

/// Decode a JSON `PayloadStatusV1` object.
///
/// When `allow_accepted` is false (forkchoiceUpdatedV3), `ACCEPTED` and
/// `INVALID_BLOCK_HASH` are rejected as decode errors — the asymmetry asserted
/// by `payload_status_label_asymmetry` (CC-32 /4, CC-33 decoder half).
pub fn decode_payload_status(
    value: &Value,
    allow_accepted: bool,
) -> Result<DecodedPayloadStatus, EngineError> {
    let obj = value.as_object().ok_or_else(|| EngineError::Decode {
        reason: "PayloadStatusV1 is not an object".into(),
    })?;
    let status_str =
        obj.get("status")
            .and_then(|s| s.as_str())
            .ok_or_else(|| EngineError::Decode {
                reason: "PayloadStatusV1.status missing".into(),
            })?;
    let status = match status_str {
        "VALID" => PayloadStatus::Valid,
        "INVALID" => PayloadStatus::Invalid,
        "SYNCING" => PayloadStatus::Syncing,
        "ACCEPTED" if allow_accepted => PayloadStatus::Accepted,
        "INVALID_BLOCK_HASH" if allow_accepted => PayloadStatus::InvalidBlockHash,
        "ACCEPTED" | "INVALID_BLOCK_HASH" => {
            return Err(EngineError::Decode {
                reason: format!("PayloadStatusV1.status {status_str} not allowed for this method"),
            });
        }
        other => {
            return Err(EngineError::Decode {
                reason: format!("unknown PayloadStatusV1.status {other}"),
            });
        }
    };

    let latest_valid_hash = match obj.get("latestValidHash") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(hex_to_32(s)?),
        Some(_) => {
            return Err(EngineError::Decode {
                reason: "latestValidHash must be hex string or null".into(),
            });
        }
    };

    let validation_error = match obj.get("validationError") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(EngineError::Decode {
                reason: "validationError must be string or null".into(),
            });
        }
    };

    Ok(DecodedPayloadStatus {
        status,
        latest_valid_hash,
        validation_error,
    })
}

/// Observe `cc_engine_payload_status_total{method,status}`.
pub fn observe_payload_status(
    metrics: Option<&EngineMetrics>,
    method: EngineMethod,
    status: &DecodedPayloadStatus,
) {
    if let Some(m) = metrics {
        m.payload_status
            .get_or_create(&PayloadStatusLabels {
                method: method.as_str().to_owned(),
                status: status.status.as_str().to_owned(),
            })
            .inc();
    }
}

// ── hex helpers ─────────────────────────────────────────────────────────────

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn root_to_hex(root: &Root) -> String {
    bytes_to_hex(root.as_slice())
}

fn address_to_hex(addr: &ExecutionAddress) -> String {
    bytes_to_hex(addr.as_slice())
}

fn quantity(n: u64) -> String {
    if n == 0 {
        return "0x0".into();
    }
    format!("0x{n:x}")
}

fn quantity_u256(n: &alloy_primitives::U256) -> String {
    if n.is_zero() {
        return "0x0".into();
    }
    // Trim leading zeros from the full 32-byte big-endian hex.
    let full = format!("{n:x}");
    let trimmed = full.trim_start_matches('0');
    if trimmed.is_empty() {
        "0x0".into()
    } else {
        format!("0x{trimmed}")
    }
}

fn hex_to_32(s: &str) -> Result<[u8; 32], EngineError> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 64 {
        return Err(EngineError::Decode {
            reason: format!("expected 32-byte hex, got len {}", hex.len() / 2),
        });
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let byte =
            u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| EngineError::Decode {
                reason: format!("hex decode: {e}"),
            })?;
        out[i] = byte;
    }
    Ok(out)
}

/// Re-export of the CC-33 fcU adapter (body lives in [`crate::methods::fcu`]).
pub use crate::methods::fcu::forkchoice_updated_v3;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::metrics::EngineMetrics;
    use prometheus_client::registry::Registry;
    use ssz::Encode;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn empty_payload_ssz() -> Vec<u8> {
        ExecutionPayload::<Mainnet>::default().as_ssz_bytes()
    }

    fn test_schedule() -> ElForkSchedule {
        ElForkSchedule {
            osaka_time: 0,
            bpo1_time: None,
            bpo2_time: None,
            amsterdam_time: None,
        }
    }

    fn transport(url: &str, metrics: Option<EngineMetrics>) -> EngineTransport {
        EngineTransport::from_parts(
            url,
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs {
                new_payload_ms: 2_000,
                forkchoice_updated_ms: 2_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            metrics,
        )
    }

    #[test]
    fn encode_default_payload_has_required_keys() {
        let p = ExecutionPayload::<Mainnet>::default();
        let v = encode_execution_payload_v3(&p).unwrap();
        for key in [
            "parentHash",
            "feeRecipient",
            "stateRoot",
            "receiptsRoot",
            "logsBloom",
            "prevRandao",
            "blockNumber",
            "gasLimit",
            "gasUsed",
            "timestamp",
            "extraData",
            "baseFeePerGas",
            "blockHash",
            "transactions",
            "withdrawals",
            "blobGasUsed",
            "excessBlobGas",
        ] {
            assert!(v.get(key).is_some(), "missing {key}");
        }
        assert_eq!(v["blockNumber"], "0x0");
        assert!(v["parentHash"].as_str().unwrap().starts_with("0x"));
    }

    #[test]
    fn decode_all_five_new_payload_statuses() {
        for (wire, expected) in [
            ("VALID", PayloadStatus::Valid),
            ("INVALID", PayloadStatus::Invalid),
            ("SYNCING", PayloadStatus::Syncing),
            ("ACCEPTED", PayloadStatus::Accepted),
            ("INVALID_BLOCK_HASH", PayloadStatus::InvalidBlockHash),
        ] {
            let v = json!({"status": wire, "latestValidHash": null, "validationError": null});
            let d = decode_payload_status(&v, true).unwrap();
            assert_eq!(d.status, expected);
        }
    }

    #[test]
    fn fcu_decoder_rejects_accepted_and_invalid_block_hash() {
        for wire in ["ACCEPTED", "INVALID_BLOCK_HASH"] {
            let v = json!({"status": wire});
            let err = decode_payload_status(&v, false).unwrap_err();
            assert!(
                matches!(err, EngineError::Decode { .. }),
                "fcU must reject {wire}: {err:?}"
            );
        }
        for wire in ["VALID", "INVALID", "SYNCING"] {
            let v = json!({"status": wire});
            decode_payload_status(&v, false).unwrap_or_else(|e| panic!("{wire}: {e}"));
        }
    }

    /// CC-32 /4: five labels under newPayloadV4; exactly three under fcU.
    #[tokio::test]
    async fn payload_status_label_asymmetry() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);

        // Drive all five newPayload statuses via a mock EL.
        let statuses = [
            "VALID",
            "INVALID",
            "SYNCING",
            "ACCEPTED",
            "INVALID_BLOCK_HASH",
        ];
        for status in statuses {
            let server = MockServer::start().await;
            Mock::given(http_method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "status": status,
                        "latestValidHash": null,
                        "validationError": null
                    }
                })))
                .mount(&server)
                .await;
            let t = transport(&server.uri(), Some(metrics.clone()));
            let r = new_payload_v4(
                &t,
                &test_schedule(),
                Some(&metrics),
                &empty_payload_ssz(),
                &[],
                &[0u8; 32],
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{status}: {e}"));
            assert_eq!(r.status_str(), status);
        }

        // Drive the three fcU statuses.
        for status in ["VALID", "INVALID", "SYNCING"] {
            let server = MockServer::start().await;
            Mock::given(http_method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "payloadStatus": {
                            "status": status,
                            "latestValidHash": null,
                            "validationError": null
                        },
                        "payloadId": null
                    }
                })))
                .mount(&server)
                .await;
            let t = transport(&server.uri(), Some(metrics.clone()));
            let r = forkchoice_updated_v3(
                &t,
                &test_schedule(),
                Some(&metrics),
                &[0u8; 32],
                &[0u8; 32],
                &[0u8; 32],
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("fcU {status}: {e}"));
            assert_eq!(r.status_str(), status);
        }

        // Assert metric series: five distinct under newPayloadV4 with count ≥ 1,
        // and only VALID/INVALID/SYNCING under forkchoiceUpdatedV3 with count ≥ 1.
        let mut np_seen = BTreeSet::new();
        let mut fcu_seen = BTreeSet::new();
        for status in PayloadStatus::ALL {
            let np = metrics
                .payload_status
                .get_or_create(&PayloadStatusLabels {
                    method: "newPayloadV4".into(),
                    status: status.as_str().to_owned(),
                })
                .get();
            if np >= 1 {
                np_seen.insert(status.as_str());
            }
            let fcu = metrics
                .payload_status
                .get_or_create(&PayloadStatusLabels {
                    method: "forkchoiceUpdatedV3".into(),
                    status: status.as_str().to_owned(),
                })
                .get();
            if fcu >= 1 {
                fcu_seen.insert(status.as_str());
            }
        }
        assert_eq!(
            np_seen,
            BTreeSet::from([
                "VALID",
                "INVALID",
                "SYNCING",
                "ACCEPTED",
                "INVALID_BLOCK_HASH",
            ]),
            "newPayloadV4 must observe all five status labels"
        );
        assert_eq!(
            fcu_seen,
            BTreeSet::from(["VALID", "INVALID", "SYNCING"]),
            "forkchoiceUpdatedV3 must observe exactly three status labels"
        );
    }

    /// Offline transport taxonomy: mock EL returns -32602 and the metric +1.
    ///
    /// Case-specific construction against real geth lives in
    /// `tests/requests_32602.rs` (`#[ignore]`, Amendment 1 names). These unit
    /// tests do **not** claim geth validation of list shape.
    #[tokio::test]
    async fn mock_el_invalid_params_increments_metric() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.errors_total_count(crate::metrics::ErrorCode::InvalidParams);

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": { "code": -32602, "message": "Invalid params" }
            })))
            .mount(&server)
            .await;
        let t = transport(&server.uri(), Some(metrics.clone()));
        let hostile = vec![vec![0x02u8, 0xaa], vec![0x00u8, 0xbb]];
        let err = new_payload_v4(
            &t,
            &test_schedule(),
            Some(&metrics),
            &empty_payload_ssz(),
            &[],
            &[0u8; 32],
            &hostile,
        )
        .await
        .expect_err("must be -32602");
        assert!(
            matches!(err, EngineError::InvalidParams { .. }),
            "expected InvalidParams, got {err:?}"
        );
        let after = metrics.errors_total_count(crate::metrics::ErrorCode::InvalidParams);
        assert!(
            after > before,
            "cc_engine_errors_total{{code=\"-32602\"}} must +1 (before={before} after={after})"
        );
        let _ = Arc::new(());
    }

    #[test]
    fn versioned_hash_wrong_length_is_decode_error() {
        let p = ExecutionPayload::<Mainnet>::default();
        let err = build_new_payload_v4_params(
            &p,
            &[vec![0u8; 16]], // not 32
            &[0u8; 32],
            &[],
        )
        .unwrap_err();
        assert!(
            matches!(err, EngineError::Decode { .. }),
            "expected Decode, got {err:?}"
        );
    }
}
