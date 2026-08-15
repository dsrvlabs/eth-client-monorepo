//! `engine_forkchoiceUpdatedV3` adapter (CC-33 / Architecture §3.8).
//!
//! - Ordered-lane call with **null** payload attributes (field is not in the
//!   proto at all — Phase 7 adds production attrs).
//! - Payload-status decoder accepts only `VALID | INVALID | SYNCING`
//!   (`ACCEPTED` / `INVALID_BLOCK_HASH` are protocol violations with their own
//!   counter path).
//! - Monotonic sequence high-water mark drops stale emissions; **resets on
//!   every reconnect** so a restarted peer does not silent-drop every fcU.
//! - `-38002` / `-38006` typed, counted, `error!`-logged, never retried with
//!   the same arguments.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};

use crate::errors::{EngineError, RetryClass};
use crate::methods::names;
use crate::methods::new_payload::{
    DecodedPayloadStatus, decode_payload_status, observe_payload_status,
};
use crate::metrics::{EngineMethod, EngineMetrics, ErrorCode, ErrorCodeLabels};
use crate::transport::{EngineTransport, Lane};
use crate::version::ElForkSchedule;

// ── sequence high-water ─────────────────────────────────────────────────────

/// Engine-side high-water mark for `ForkchoiceUpdatedRequest.sequence` (§3.8/2).
///
/// Admits only sequences strictly greater than the last admitted value. A
/// reconnect (new `session_id`, process restart, or explicit [`Self::reset`])
/// clears the mark so a surviving peer sequence does not appear stale against
/// a dead session.
///
/// **Ordering:** callers must call [`Self::try_admit`] only while holding the
/// transport ordered-lane lock through the subsequent EL HTTP call, so two
/// admitted sequences cannot race the mutex and hit the EL out of order.
#[derive(Debug, Default)]
pub struct FcuSequenceGate {
    high_water: AtomicU64,
    /// Last non-zero `session_id` observed; change → reset high-water.
    last_session_id: AtomicU64,
}

impl FcuSequenceGate {
    /// Fresh gate (high-water = 0).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current high-water (last admitted sequence, or 0).
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
    }

    /// Last observed chain session id (0 = never set).
    #[must_use]
    pub fn last_session_id(&self) -> u64 {
        self.last_session_id.load(Ordering::SeqCst)
    }

    /// Reset on reconnect / new engine session (§3.8/2).
    pub fn reset(&self) {
        self.high_water.store(0, Ordering::SeqCst);
    }

    /// Observe `session_id` from the request. Non-zero and different from the
    /// previous value → reset high-water (chain reconnect / restart).
    ///
    /// Returns `true` if a reset occurred.
    pub fn note_session(&self, session_id: u64) -> bool {
        if session_id == 0 {
            return false;
        }
        let prev = self.last_session_id.swap(session_id, Ordering::SeqCst);
        if prev != 0 && prev != session_id {
            self.reset();
            tracing::info!(
                prev_session = prev,
                session_id,
                "fcU sequence high-water reset on session change"
            );
            return true;
        }
        if prev == 0 {
            // First non-zero session stamps without a spurious "change" log.
            // High-water stays 0 (already).
        }
        false
    }

    /// Admit `sequence` if it is strictly newer than the high-water mark.
    ///
    /// On admit: updates high-water to `sequence` and returns `true`.
    /// On stale: leaves high-water unchanged and returns `false`.
    ///
    /// Must be called under the ordered-lane lock (with the EL call) — see
    /// module docs.
    pub fn try_admit(&self, sequence: u64) -> bool {
        let mut cur = self.high_water.load(Ordering::SeqCst);
        loop {
            if sequence <= cur {
                return false;
            }
            match self.high_water.compare_exchange_weak(
                cur,
                sequence,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// Typed outcome when a sequence is dropped as stale (not a success).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcuDroppedStale {
    pub sequence: u64,
    pub high_water: u64,
}

impl std::fmt::Display for FcuDroppedStale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "forkchoiceUpdated sequence {} dropped as stale (high_water={})",
            self.sequence, self.high_water
        )
    }
}

impl std::error::Error for FcuDroppedStale {}

// ── adapter ─────────────────────────────────────────────────────────────────

/// Issue `engine_forkchoiceUpdatedV3` with null attributes (CC-33).
///
/// Acquires the ordered lane for the HTTP call only. Prefer
/// [`forkchoice_updated_v3_gated`] when a sequence gate must admit under the
/// same lock (production gRPC path).
pub async fn forkchoice_updated_v3(
    transport: &EngineTransport,
    schedule: &ElForkSchedule,
    metrics: Option<&EngineMetrics>,
    head_block_hash: &[u8],
    safe_block_hash: &[u8],
    finalized_block_hash: &[u8],
    head_slot: Option<u64>,
) -> Result<DecodedPayloadStatus, EngineError> {
    let params = prepare_fcu_params(
        schedule,
        head_block_hash,
        safe_block_hash,
        finalized_block_hash,
    )?;
    call_fcu_http(
        transport,
        metrics,
        params,
        head_block_hash,
        safe_block_hash,
        finalized_block_hash,
        head_slot,
        false,
    )
    .await
}

/// Admit `sequence` under the ordered-lane lock, then issue fcU (CC-33 F1).
///
/// Session change on `session_id` resets the high-water before admit (§3.8/2).
/// Stale sequences return [`FcuDroppedStale`] (never a spoofed VALID).
#[allow(clippy::too_many_arguments)]
pub async fn forkchoice_updated_v3_gated(
    transport: &EngineTransport,
    gate: &FcuSequenceGate,
    schedule: &ElForkSchedule,
    metrics: Option<&EngineMetrics>,
    sequence: u64,
    session_id: u64,
    head_block_hash: &[u8],
    safe_block_hash: &[u8],
    finalized_block_hash: &[u8],
    head_slot: Option<u64>,
) -> Result<DecodedPayloadStatus, FcuGatedError> {
    let params = prepare_fcu_params(
        schedule,
        head_block_hash,
        safe_block_hash,
        finalized_block_hash,
    )
    .map_err(FcuGatedError::Engine)?;

    // Hold ordered lock across note_session + try_admit + HTTP so admitted
    // sequences cannot reorder on the EL wire.
    let _ordered = transport.lock_ordered().await;
    let _ = gate.note_session(session_id);
    if !gate.try_admit(sequence) {
        if let Some(m) = metrics {
            m.fcu_dropped_stale.inc();
        }
        let dropped = FcuDroppedStale {
            sequence,
            high_water: gate.high_water(),
        };
        tracing::debug!(
            sequence = dropped.sequence,
            high_water = dropped.high_water,
            "forkchoiceUpdated dropped as stale"
        );
        return Err(FcuGatedError::DroppedStale(dropped));
    }

    call_fcu_http(
        transport,
        metrics,
        params,
        head_block_hash,
        safe_block_hash,
        finalized_block_hash,
        head_slot,
        true, // ordered already held
    )
    .await
    .map_err(FcuGatedError::Engine)
}

/// Error from the gated fcU path.
#[derive(Debug)]
pub enum FcuGatedError {
    /// Sequence was superseded; EL was not contacted.
    DroppedStale(FcuDroppedStale),
    /// Engine / transport failure after admit.
    Engine(EngineError),
}

impl std::fmt::Display for FcuGatedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DroppedStale(d) => write!(f, "{d}"),
            Self::Engine(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for FcuGatedError {}

fn prepare_fcu_params(
    schedule: &ElForkSchedule,
    head_block_hash: &[u8],
    safe_block_hash: &[u8],
    finalized_block_hash: &[u8],
) -> Result<Value, EngineError> {
    // fcU with null attributes has no timestamp gate on geth; still use the
    // schedule's Osaka arm so Amsterdam fails loud if we ever pass attributes.
    let _method =
        crate::version::forkchoice_method_for(schedule.osaka_time, schedule).map_err(|e| {
            EngineError::UnsupportedFork {
                message: e.to_string(),
            }
        })?;

    if head_block_hash.len() != 32
        || safe_block_hash.len() != 32
        || finalized_block_hash.len() != 32
    {
        return Err(EngineError::Decode {
            reason: "forkchoice hashes must be 32 bytes each".into(),
        });
    }
    Ok(build_fcu_params(
        head_block_hash,
        safe_block_hash,
        finalized_block_hash,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn call_fcu_http(
    transport: &EngineTransport,
    metrics: Option<&EngineMetrics>,
    params: Value,
    head_block_hash: &[u8],
    safe_block_hash: &[u8],
    finalized_block_hash: &[u8],
    head_slot: Option<u64>,
    ordered_held: bool,
) -> Result<DecodedPayloadStatus, EngineError> {
    // Single attempt for Fatal codes. Transient (`-32603`/`-32000`) may retry
    // once after 250 ms — never `-38002` / `-38006` with the same args (CC-33/6,8).
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let result = if ordered_held {
            transport
                .call_with_ordered_held(
                    EngineMethod::ForkchoiceUpdatedV3,
                    names::FORKCHOICE_UPDATED_V3,
                    params.clone(),
                )
                .await
        } else {
            transport
                .call(
                    Lane::Ordered,
                    EngineMethod::ForkchoiceUpdatedV3,
                    names::FORKCHOICE_UPDATED_V3,
                    params.clone(),
                )
                .await
        };

        match result {
            Ok(value) => {
                return decode_fcu_result(value, metrics);
            }
            Err(e) => {
                handle_fcu_error(
                    &e,
                    metrics,
                    head_block_hash,
                    safe_block_hash,
                    finalized_block_hash,
                    head_slot,
                );
                // Retry only when we can re-acquire the lane ourselves.
                if !ordered_held && e.retry_class() == RetryClass::Transient && attempts < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// Build the two-element params array: ForkchoiceStateV1 + null attributes.
pub fn build_fcu_params(
    head_block_hash: &[u8],
    safe_block_hash: &[u8],
    finalized_block_hash: &[u8],
) -> Value {
    json!([
        {
            "headBlockHash": bytes_to_hex(head_block_hash),
            "safeBlockHash": bytes_to_hex(safe_block_hash),
            "finalizedBlockHash": bytes_to_hex(finalized_block_hash),
        },
        Value::Null
    ])
}

/// Decode `{ payloadStatus, payloadId }` and enforce the three-value set.
pub fn decode_fcu_result(
    result: Value,
    metrics: Option<&EngineMetrics>,
) -> Result<DecodedPayloadStatus, EngineError> {
    let payload_status_val =
        result
            .get("payloadStatus")
            .cloned()
            .ok_or_else(|| EngineError::Decode {
                reason: "forkchoiceUpdated result missing payloadStatus".into(),
            })?;
    // allow_accepted = false: fcU accepts only VALID | INVALID | SYNCING.
    match decode_payload_status(&payload_status_val, false) {
        Ok(status) => {
            observe_payload_status(metrics, EngineMethod::ForkchoiceUpdatedV3, &status);
            Ok(status)
        }
        Err(e) => {
            observe_fcu_protocol_violation(metrics, &e);
            Err(e)
        }
    }
}

/// Decode a bare `PayloadStatusV1` for the fcU decoder tests (three-value set).
pub fn decode_fcu_payload_status(
    value: &Value,
    metrics: Option<&EngineMetrics>,
) -> Result<DecodedPayloadStatus, EngineError> {
    match decode_payload_status(value, false) {
        Ok(status) => {
            observe_payload_status(metrics, EngineMethod::ForkchoiceUpdatedV3, &status);
            Ok(status)
        }
        Err(e) => {
            observe_fcu_protocol_violation(metrics, &e);
            Err(e)
        }
    }
}

fn observe_fcu_protocol_violation(metrics: Option<&EngineMetrics>, err: &EngineError) {
    if let Some(m) = metrics {
        m.errors_total
            .get_or_create(&ErrorCodeLabels {
                code: ErrorCode::Decode.as_str().to_owned(),
            })
            .inc();
    }
    tracing::error!(
        method = "forkchoiceUpdatedV3",
        error = %err,
        "fcU payloadStatus protocol violation (only VALID|INVALID|SYNCING allowed)"
    );
}

fn handle_fcu_error(
    err: &EngineError,
    _metrics: Option<&EngineMetrics>,
    head: &[u8],
    safe: &[u8],
    finalized: &[u8],
    head_slot: Option<u64>,
) {
    // Transport already counted `cc_engine_errors_total` for wire errors.
    match err {
        EngineError::InvalidForkchoiceState { message } => {
            tracing::error!(
                head = %bytes_to_hex(head),
                safe = %bytes_to_hex(safe),
                finalized = %bytes_to_hex(finalized),
                head_slot = head_slot.unwrap_or(u64::MAX),
                message = %message,
                "JSON-RPC -38002 invalid forkchoice state (never retry with the same arguments)"
            );
        }
        EngineError::InvalidRange { message } => {
            // -38006: unreachable for a follower; classify and log, no recovery.
            tracing::error!(
                head = %bytes_to_hex(head),
                safe = %bytes_to_hex(safe),
                finalized = %bytes_to_hex(finalized),
                message = %message,
                "JSON-RPC -38006 too deep reorg: the EL and CL disagree about finality"
            );
        }
        _ => {}
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

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::{TimeoutKnobs, TransportTimeouts, soft_deadline_ms};
    use crate::jwt::JwtSecret;
    use crate::methods::new_payload::new_payload_v4;
    use crate::metrics::{EngineMetrics, MethodLabels, PayloadStatus};
    use cc_types::execution::ExecutionPayload;
    use cc_types::preset::Mainnet;
    use prometheus_client::registry::Registry;
    use ssz::Encode;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use wiremock::matchers::method as http_method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
                new_payload_ms: 8_000,
                forkchoice_updated_ms: 8_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            Duration::from_secs_f64(soft_deadline_ms(3_333, 12_000) / 1_000.0),
            metrics,
        )
    }

    fn empty_payload_ssz() -> Vec<u8> {
        ExecutionPayload::<Mainnet>::default().as_ssz_bytes()
    }

    /// Capture tracing error lines into a buffer.
    fn capture_logs() -> (
        tracing::subscriber::DefaultGuard,
        Arc<std::sync::Mutex<Vec<u8>>>,
    ) {
        use std::io::Write;
        use tracing_subscriber::fmt;

        #[derive(Clone)]
        struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Buf(Arc::clone(&buf));
        let subscriber = fmt::Subscriber::builder()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(writer)
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (guard, buf)
    }

    // ── decoder three-value set (CC-34 /1 second half) ──────────────────────

    #[test]
    fn fcu_decoder_rejects_accepted() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.errors_total_count(ErrorCode::Decode);

        let v = json!({"status": "ACCEPTED", "latestValidHash": null, "validationError": null});
        let err = decode_fcu_payload_status(&v, Some(&metrics)).unwrap_err();
        assert!(
            matches!(err, EngineError::Decode { .. }),
            "fcU must reject ACCEPTED as protocol violation: {err:?}"
        );
        let after = metrics.errors_total_count(ErrorCode::Decode);
        assert!(
            after > before,
            "fcU protocol-violation counter (decode) must +1"
        );

        // newPayload decoder accepts the same value without error.
        let ok = decode_payload_status(&v, true).expect("newPayload accepts ACCEPTED");
        assert_eq!(ok.status, PayloadStatus::Accepted);
    }

    #[test]
    fn fcu_decoder_rejects_invalid_block_hash() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.errors_total_count(ErrorCode::Decode);

        let v = json!({
            "status": "INVALID_BLOCK_HASH",
            "latestValidHash": null,
            "validationError": null
        });
        let err = decode_fcu_payload_status(&v, Some(&metrics)).unwrap_err();
        assert!(
            matches!(err, EngineError::Decode { .. }),
            "fcU must reject INVALID_BLOCK_HASH as protocol violation: {err:?}"
        );
        let after = metrics.errors_total_count(ErrorCode::Decode);
        assert!(
            after > before,
            "fcU protocol-violation counter (decode) must +1"
        );

        let ok = decode_payload_status(&v, true).expect("newPayload accepts INVALID_BLOCK_HASH");
        assert_eq!(ok.status, PayloadStatus::InvalidBlockHash);
    }

    #[test]
    fn fcu_decoder_accepts_three() {
        for wire in ["VALID", "INVALID", "SYNCING"] {
            let v = json!({"status": wire});
            decode_fcu_payload_status(&v, None).unwrap_or_else(|e| panic!("{wire}: {e}"));
        }
    }

    // ── sequence gate ───────────────────────────────────────────────────────

    #[test]
    fn sequence_gate_drops_stale_and_resets() {
        let gate = FcuSequenceGate::new();
        assert!(gate.try_admit(1));
        assert!(gate.try_admit(5));
        assert!(!gate.try_admit(5), "equal is stale");
        assert!(!gate.try_admit(3), "lower is stale");
        assert!(gate.try_admit(6));

        gate.reset();
        assert_eq!(gate.high_water(), 0);
        // After reconnect, a previously-"old" sequence is admitted again.
        assert!(gate.try_admit(1), "post-reset sequence must be issued");
        assert!(gate.try_admit(6));
    }

    #[test]
    fn sequence_gate_resets_on_session_change() {
        let gate = FcuSequenceGate::new();
        assert!(
            !gate.note_session(7),
            "first session is a stamp, not a change"
        );
        assert!(gate.try_admit(10));
        assert_eq!(gate.high_water(), 10);

        assert!(
            gate.note_session(99),
            "new session_id must reset high-water"
        );
        assert_eq!(gate.high_water(), 0);
        assert!(
            gate.try_admit(1),
            "post-session-change sequence 1 must admit"
        );
    }

    /// CC-33: high-water resets on reconnect (session_id change) — next fcU is
    /// issued, not dropped; `cc_engine_fcu_dropped_stale_total` does not
    /// increment across the session edge.
    #[tokio::test]
    async fn fcu_sequence_resets_on_reconnect() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let gate = FcuSequenceGate::new();

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "payloadStatus": {
                        "status": "VALID",
                        "latestValidHash": null,
                        "validationError": null
                    },
                    "payloadId": null
                }
            })))
            .mount(&server)
            .await;
        let t = transport(&server.uri(), Some(metrics.clone()));

        // Session A: drive sequences 1..3.
        for seq in [1u64, 2, 3] {
            let r = forkchoice_updated_v3_gated(
                &t,
                &gate,
                &test_schedule(),
                Some(&metrics),
                seq,
                1, // session A
                &[0x11; 32],
                &[0x22; 32],
                &[0x33; 32],
                Some(10),
            )
            .await
            .unwrap();
            assert_eq!(r.status_str(), "VALID");
        }
        assert_eq!(gate.high_water(), 3);
        let dropped_before = metrics.fcu_dropped_stale.get();

        // Sever / re-establish: new session_id (chain reconnect).
        // Sequence restarts at 1 and must be issued, not dropped.
        let r = forkchoice_updated_v3_gated(
            &t,
            &gate,
            &test_schedule(),
            Some(&metrics),
            1,
            2, // session B
            &[0x11; 32],
            &[0x22; 32],
            &[0x33; 32],
            Some(11),
        )
        .await
        .expect("post-reconnect sequence 1 must be issued");
        assert_eq!(r.status_str(), "VALID");
        assert_eq!(gate.high_water(), 1);

        let dropped_after = metrics.fcu_dropped_stale.get();
        assert_eq!(
            dropped_after, dropped_before,
            "cc_engine_fcu_dropped_stale_total must not increment across reconnect"
        );

        // Same session, stale sequence → DroppedStale (not VALID).
        let err = forkchoice_updated_v3_gated(
            &t,
            &gate,
            &test_schedule(),
            Some(&metrics),
            1,
            2,
            &[0x11; 32],
            &[0x22; 32],
            &[0x33; 32],
            None,
        )
        .await
        .expect_err("stale must not succeed");
        assert!(
            matches!(err, FcuGatedError::DroppedStale(_)),
            "stale must be typed drop, not success: {err:?}"
        );
        assert!(metrics.fcu_dropped_stale.get() > dropped_after);
    }

    /// Admitted sequences execute on the EL in sequence order (no post-admit reorder).
    #[tokio::test]
    async fn fcu_admit_and_el_order_under_lock() {
        use std::sync::Mutex as StdMutex;
        let order: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        let order_c = Arc::clone(&order);

        let server = MockServer::start().await;
        // Custom responder records the headBlockHash first byte as "seq" proxy.
        Mock::given(http_method("POST"))
            .respond_with(move |req: &wiremock::Request| {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
                let head = body
                    .pointer("/params/0/headBlockHash")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                // Encode sequence in the second byte of the hash (we set it below).
                if head.len() >= 6 {
                    // 0xNN... → take first data byte after 0x
                    if let Ok(b) = u8::from_str_radix(&head[2..4], 16) {
                        order_c.lock().unwrap().push(u64::from(b));
                    }
                }
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "payloadStatus": {
                            "status": "VALID",
                            "latestValidHash": null,
                            "validationError": null
                        },
                        "payloadId": null
                    }
                }))
            })
            .mount(&server)
            .await;

        let t = Arc::new(transport(&server.uri(), None));
        let gate = Arc::new(FcuSequenceGate::new());
        let schedule = test_schedule();

        let mut handles = Vec::new();
        for seq in [1u64, 2, 3, 4, 5] {
            let t = Arc::clone(&t);
            let gate = Arc::clone(&gate);
            let schedule = schedule.clone();
            handles.push(tokio::spawn(async move {
                let mut head = [0u8; 32];
                head[0] = seq as u8;
                forkchoice_updated_v3_gated(
                    t.as_ref(),
                    gate.as_ref(),
                    &schedule,
                    None,
                    seq,
                    1,
                    &head,
                    &[0u8; 32],
                    &[0u8; 32],
                    Some(seq),
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        let seen = order.lock().unwrap().clone();
        assert_eq!(seen.len(), 5, "all five must reach EL: {seen:?}");
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        assert_eq!(seen, sorted, "EL order must match sequence order: {seen:?}");
    }

    // ── soft deadline exclusion ─────────────────────────────────────────────

    /// CC-33 /5: a slow fcU does not trip the soft-deadline alarm; newPayload does.
    #[tokio::test]
    async fn fcu_excluded_from_soft_deadline() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);

        // Soft deadline ≈ 4 s (Hoodi).  Delay both methods by 6 s so both would
        // exceed it if counted. Transport timeout is 8 s so the call completes.
        let delay = Duration::from_millis(200); // keep unit test fast
        // Use a tiny soft deadline so 200 ms exceeds it without a 6 s sleep.
        let soft = Duration::from_millis(50);

        // ── fcU: must leave soft_deadline_exceeded at zero for this method ──
        let server_fcu = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "payloadStatus": {
                                "status": "VALID",
                                "latestValidHash": null,
                                "validationError": null
                            },
                            "payloadId": null
                        }
                    })),
            )
            .mount(&server_fcu)
            .await;
        let t_fcu = EngineTransport::from_parts(
            server_fcu.uri(),
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs {
                new_payload_ms: 5_000,
                forkchoice_updated_ms: 5_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            soft,
            Some(metrics.clone()),
        );
        let before_fcu = metrics
            .soft_deadline_exceeded
            .get_or_create(&MethodLabels {
                method: EngineMethod::ForkchoiceUpdatedV3.as_str().to_owned(),
            })
            .get();
        forkchoice_updated_v3(
            &t_fcu,
            &test_schedule(),
            Some(&metrics),
            &[0u8; 32],
            &[0u8; 32],
            &[0u8; 32],
            None,
        )
        .await
        .unwrap();
        let after_fcu = metrics
            .soft_deadline_exceeded
            .get_or_create(&MethodLabels {
                method: EngineMethod::ForkchoiceUpdatedV3.as_str().to_owned(),
            })
            .get();
        assert_eq!(
            after_fcu, before_fcu,
            "fcU must not increment soft_deadline_exceeded (before={before_fcu} after={after_fcu})"
        );

        // ── newPayload: same delay MUST increment its own soft deadline ─────
        let server_np = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "status": "VALID",
                            "latestValidHash": null,
                            "validationError": null
                        }
                    })),
            )
            .mount(&server_np)
            .await;
        let t_np = EngineTransport::from_parts(
            server_np.uri(),
            JwtSecret::from_bytes([0x11; 32]),
            TransportTimeouts::from_knobs(&TimeoutKnobs {
                new_payload_ms: 5_000,
                forkchoice_updated_ms: 5_000,
                get_blobs_ms: 1_000,
                exchange_capabilities_ms: 1_000,
                eth_syncing_ms: 1_000,
                multiplier: 1.0,
            }),
            soft,
            Some(metrics.clone()),
        );
        let before_np = metrics
            .soft_deadline_exceeded
            .get_or_create(&MethodLabels {
                method: EngineMethod::NewPayloadV4.as_str().to_owned(),
            })
            .get();
        new_payload_v4(
            &t_np,
            &test_schedule(),
            Some(&metrics),
            &empty_payload_ssz(),
            &[],
            &[0u8; 32],
            &[],
        )
        .await
        .unwrap();
        let after_np = metrics
            .soft_deadline_exceeded
            .get_or_create(&MethodLabels {
                method: EngineMethod::NewPayloadV4.as_str().to_owned(),
            })
            .get();
        assert!(
            after_np > before_np,
            "newPayload must increment soft_deadline_exceeded (before={before_np} after={after_np})"
        );
    }

    // ── -38002 / -38006 ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn invalid_forkchoice_state_is_typed_and_final() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.errors_total_count(ErrorCode::InvalidForkchoiceState);
        let call_count = Arc::new(AtomicU64::new(0));

        let server = MockServer::start().await;
        let count = Arc::clone(&call_count);
        Mock::given(http_method("POST"))
            .respond_with(move |_req: &wiremock::Request| {
                count.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {
                        "code": -38002,
                        "message": "Invalid forkchoice state"
                    }
                }))
            })
            .mount(&server)
            .await;

        let (_guard, log_buf) = capture_logs();
        let t = transport(&server.uri(), Some(metrics.clone()));
        let head = [0xAAu8; 32];
        let safe = [0xBBu8; 32];
        let finalized = [0xCCu8; 32];
        let err = forkchoice_updated_v3(
            &t,
            &test_schedule(),
            Some(&metrics),
            &head,
            &safe,
            &finalized,
            Some(99),
        )
        .await
        .expect_err("-38002");
        assert!(
            matches!(err, EngineError::InvalidForkchoiceState { .. }),
            "typed -38002, got {err:?}"
        );
        let after = metrics.errors_total_count(ErrorCode::InvalidForkchoiceState);
        assert!(
            after > before,
            "cc_engine_errors_total{{code=\"-38002\"}} must +1"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "must never retry -38002 with the same arguments"
        );

        let log_bytes = log_buf.lock().unwrap().clone();
        let logs = String::from_utf8_lossy(&log_bytes);
        assert!(
            logs.contains("0xaa") || logs.contains("0xAA") || logs.contains(&bytes_to_hex(&head)),
            "error! must carry head hash: {logs}"
        );
        assert!(
            logs.contains(&bytes_to_hex(&safe)),
            "error! must carry safe hash: {logs}"
        );
        assert!(
            logs.contains(&bytes_to_hex(&finalized)),
            "error! must carry finalized hash: {logs}"
        );
        assert!(
            logs.contains("99") || logs.contains("head_slot"),
            "error! must carry head slot: {logs}"
        );
    }

    #[tokio::test]
    async fn too_deep_reorg_is_classified() {
        let mut registry = Registry::default();
        let metrics = EngineMetrics::register(&mut registry);
        let before = metrics.errors_total_count(ErrorCode::InvalidRange);

        let server = MockServer::start().await;
        Mock::given(http_method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -38006,
                    "message": "Too deep reorg"
                }
            })))
            .mount(&server)
            .await;

        let (_guard, log_buf) = capture_logs();
        let t = transport(&server.uri(), Some(metrics.clone()));
        let err = forkchoice_updated_v3(
            &t,
            &test_schedule(),
            Some(&metrics),
            &[0u8; 32],
            &[0u8; 32],
            &[0u8; 32],
            None,
        )
        .await
        .expect_err("-38006");
        assert!(
            matches!(err, EngineError::InvalidRange { .. }),
            "typed -38006, got {err:?}"
        );
        let after = metrics.errors_total_count(ErrorCode::InvalidRange);
        assert!(
            after > before,
            "cc_engine_errors_total{{code=\"-38006\"}} must +1"
        );

        let log_bytes = log_buf.lock().unwrap().clone();
        let logs = String::from_utf8_lossy(&log_bytes);
        assert!(
            logs.contains("the EL and CL disagree about finality"),
            "log must name the disagreement: {logs}"
        );
    }

    #[test]
    fn fcu_params_have_null_attributes_not_a_field() {
        let p = build_fcu_params(&[1u8; 32], &[2u8; 32], &[3u8; 32]);
        let arr = p.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr[1].is_null(), "second param must be JSON null");
        // Production body (above this tests module) must not define attrs as a field.
        let src = include_str!("fcu.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        let needle = ["payload", "attributes"].join("_");
        assert!(
            !prod.to_lowercase().contains(&needle),
            "attrs field name must be absent from fcu production code"
        );
    }
}
