//! Positive / negative serve-window probe logic (CC-4B).

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use cc_types::{ForkDigest, Slot};

use crate::client::{ProbeClient, ProbeClientError};
use crate::codec::ResponseChunk;
use crate::protocols::{BlocksByRangeRequest, ColumnsByRangeRequest, Protocol};
use crate::sample::{
    MAX_SAMPLE_COUNT, NEGATIVE_LOOKBACK_SLOTS, negative_range, positive_sample_budget, sample_slots,
};

/// CLI / harness configuration for one probe run.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    /// Target multiaddr.
    pub peer: String,
    /// Required fork digest.
    pub fork_digest: ForkDigest,
    /// Positive-side sample count (ignored when `full_window`).
    pub slots: usize,
    /// When true, sample every slot in `[eas, head]`.
    pub full_window: bool,
    /// Negative-side sample count.
    pub below: usize,
    /// Column indices to request (empty → skip column protocols).
    pub columns: Vec<u64>,
}

/// One side's result.
#[derive(Debug, Clone, Default)]
pub struct SideResult {
    /// Whether every sampled slot passed.
    pub pass: bool,
    /// Slot → failure reason.
    pub failures: BTreeMap<u64, String>,
    /// Slot → latency milliseconds.
    pub latencies_ms: BTreeMap<u64, u64>,
}

impl SideResult {
    /// Sorted failing slot list.
    #[must_use]
    pub fn failing_slots(&self) -> Vec<u64> {
        self.failures.keys().copied().collect()
    }
}

/// Full probe outcome after Status + both sides.
#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    /// Peer's advertised earliest available slot.
    pub earliest_available_slot: u64,
    /// Peer's advertised head slot.
    pub head_slot: u64,
    /// Identify agent version (may be empty).
    pub agent_version: String,
    /// Positive side.
    pub positive: SideResult,
    /// Negative side.
    pub negative: SideResult,
}

impl ProbeOutcome {
    /// Overall pass when both sides pass.
    #[must_use]
    pub fn pass(&self) -> bool {
        self.positive.pass && self.negative.pass
    }
}

/// Probe-level error (transport / status before sides run).
#[derive(Debug)]
pub enum ProbeError {
    /// Client / transport failure.
    Client(ProbeClientError),
    /// Peer Status or CLI produced an unusable window (SEC-4B-2).
    InvalidWindow(String),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(e) => write!(f, "{e}"),
            Self::InvalidWindow(m) => write!(f, "invalid serve window: {m}"),
        }
    }
}

impl std::error::Error for ProbeError {}

impl From<ProbeClientError> for ProbeError {
    fn from(value: ProbeClientError) -> Self {
        Self::Client(value)
    }
}

/// Dial, Status handshake, run positive + negative sides.
pub async fn run_probe(cfg: &ProbeConfig) -> Result<ProbeOutcome, ProbeError> {
    // Blocks-by-range client (Status + single Full multi protocol).
    let mut blocks = ProbeClient::dial(&cfg.peer, cfg.fork_digest).await?;
    let info = blocks.peer_info();
    let eas = info.status.earliest_available_slot.as_u64();
    let head = info.status.head_slot.as_u64();

    eprintln!(
        "status: peer={} agent_version={:?} earliest_available_slot={} head_slot={}",
        info.peer_id, info.agent_version, eas, head
    );

    // Optional second dial for columns — multi request_response can only
    // outbound one Full data protocol per behaviour (multistream first-match).
    let mut columns_client = if cfg.columns.is_empty() {
        None
    } else {
        Some(ProbeClient::dial_columns(&cfg.peer, cfg.fork_digest).await?)
    };

    let positive = run_positive(&mut blocks, columns_client.as_mut(), eas, head, cfg).await?;
    let negative = run_negative(&mut blocks, columns_client.as_mut(), eas, cfg).await;

    Ok(ProbeOutcome {
        earliest_available_slot: eas,
        head_slot: head,
        agent_version: info.agent_version,
        positive,
        negative,
    })
}

async fn run_positive(
    blocks: &mut ProbeClient,
    mut columns: Option<&mut ProbeClient>,
    eas: u64,
    head: u64,
    cfg: &ProbeConfig,
) -> Result<SideResult, ProbeError> {
    // SEC-4B-2: checked window math + hard sample ceiling (never trust peer
    // Status for unbounded allocation).
    let n = positive_sample_budget(eas, head, cfg.slots, cfg.full_window).ok_or_else(|| {
        ProbeError::InvalidWindow(format!(
            "head_slot ({head}) − earliest_available_slot ({eas}) + 1 overflows u64 \
             (fail closed, SEC-4B-2); refuse full-window materialisation"
        ))
    })?;
    if cfg.full_window && n == MAX_SAMPLE_COUNT && head > eas {
        eprintln!(
            "warn: --full-window capped at {MAX_SAMPLE_COUNT} samples \
             (peer window may be larger; SEC-4B-2)"
        );
    }
    let slots = if head < eas || n == 0 {
        Vec::new()
    } else {
        sample_slots(eas, head, n)
    };

    let mut failures = BTreeMap::new();
    let mut latencies_ms = BTreeMap::new();

    for slot in slots {
        let (ok, reason, elapsed) =
            probe_positive_slot(blocks, columns.as_deref_mut(), slot, &cfg.columns).await;
        latencies_ms.insert(slot, duration_ms(elapsed));
        if !ok {
            failures.insert(slot, reason);
        }
    }

    Ok(SideResult {
        pass: failures.is_empty(),
        failures,
        latencies_ms,
    })
}

async fn run_negative(
    blocks: &mut ProbeClient,
    mut columns: Option<&mut ProbeClient>,
    eas: u64,
    cfg: &ProbeConfig,
) -> SideResult {
    let Some((lo, hi)) = negative_range(eas, NEGATIVE_LOOKBACK_SLOTS) else {
        // Nothing below eas — vacuously pass.
        return SideResult {
            pass: true,
            failures: BTreeMap::new(),
            latencies_ms: BTreeMap::new(),
        };
    };
    // SEC-4B-3: clamp --below.
    let below = cfg.below.min(MAX_SAMPLE_COUNT);
    let slots = sample_slots(lo, hi, below);
    let mut failures = BTreeMap::new();
    let mut latencies_ms = BTreeMap::new();

    for slot in slots {
        let (ok, reason, elapsed) =
            probe_negative_slot(blocks, columns.as_deref_mut(), slot, &cfg.columns).await;
        latencies_ms.insert(slot, duration_ms(elapsed));
        if !ok {
            failures.insert(slot, reason);
        }
    }

    SideResult {
        pass: failures.is_empty(),
        failures,
        latencies_ms,
    }
}

async fn probe_positive_slot(
    blocks: &mut ProbeClient,
    columns: Option<&mut ProbeClient>,
    slot: u64,
    column_indices: &[u64],
) -> (bool, String, Duration) {
    let start = std::time::Instant::now();
    let req = BlocksByRangeRequest {
        start_slot: Slot::new(slot),
        count: 1,
    };
    let chunks = match blocks
        .request_chunks(Protocol::BeaconBlocksByRangeV2, req.to_ssz_bytes().to_vec())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                false,
                format!("blocks_by_range transport/codec: {e}"),
                start.elapsed(),
            );
        }
    };

    // Positive: ResourceUnavailable is failure. Empty stream = known empty slot = pass.
    // Success chunk(s) with SSZ = block present = pass.
    if let Some(reason) = positive_block_verdict(&chunks) {
        return (false, reason, start.elapsed());
    }

    if !column_indices.is_empty() {
        let Some(cols_client) = columns else {
            return (
                false,
                "columns requested but no columns client".to_owned(),
                start.elapsed(),
            );
        };
        let creq = ColumnsByRangeRequest {
            start_slot: Slot::new(slot),
            count: 1,
            columns: column_indices.to_vec(),
        };
        let cchunks = match cols_client
            .request_chunks(Protocol::DataColumnSidecarsByRangeV1, creq.to_ssz_bytes())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return (
                    false,
                    format!("columns_by_range transport/codec: {e}"),
                    start.elapsed(),
                );
            }
        };
        if let Some(reason) = positive_column_verdict(&cchunks, column_indices.len()) {
            return (false, reason, start.elapsed());
        }
    }

    (true, String::new(), start.elapsed())
}

/// `None` = pass; `Some(reason)` = fail.
fn positive_block_verdict(chunks: &[ResponseChunk]) -> Option<String> {
    if chunks.is_empty() {
        // Known empty slot — pass.
        return None;
    }
    for c in chunks {
        if c.is_resource_unavailable() {
            return Some(
                "positive side: ResourceUnavailable (code 3) — expected block or known empty slot"
                    .to_owned(),
            );
        }
        if let ResponseChunk::Error { code, message } = c {
            let msg = String::from_utf8_lossy(message);
            return Some(format!("positive side: error result byte {code}: {msg}"));
        }
        if let ResponseChunk::Success { ssz, .. } = c
            && ssz.is_empty()
        {
            return Some(
                "positive side: empty success payload (not a known empty slot stream)".to_owned(),
            );
        }
        // Non-empty block SSZ — pass for this chunk.
    }
    None
}

fn positive_column_verdict(chunks: &[ResponseChunk], expected_cols: usize) -> Option<String> {
    let _ = expected_cols;
    if chunks.iter().any(|c| c.is_resource_unavailable()) {
        return Some(
            "positive side: columns ResourceUnavailable (code 3) — expected sidecars or empty slot"
                .to_owned(),
        );
    }
    if chunks
        .iter()
        .any(|c| matches!(c, ResponseChunk::Error { .. }))
    {
        return Some("positive side: columns error result byte".to_owned());
    }
    // Empty stream on columns for an empty block slot is acceptable.
    // Non-empty success chunks = peer served something — pass (exact set
    // cardinality is peer-policy dependent; zero ResourceUnavailable is the gate).
    None
}

async fn probe_negative_slot(
    blocks: &mut ProbeClient,
    columns: Option<&mut ProbeClient>,
    slot: u64,
    column_indices: &[u64],
) -> (bool, String, Duration) {
    let start = std::time::Instant::now();
    let req = BlocksByRangeRequest {
        start_slot: Slot::new(slot),
        count: 1,
    };
    let chunks = match blocks
        .request_chunks(Protocol::BeaconBlocksByRangeV2, req.to_ssz_bytes().to_vec())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return (
                false,
                format!("blocks_by_range transport/codec: {e}"),
                start.elapsed(),
            );
        }
    };

    if let Some(reason) = negative_verdict(&chunks, slot, "blocks") {
        return (false, reason, start.elapsed());
    }

    if !column_indices.is_empty() {
        let Some(cols_client) = columns else {
            return (
                false,
                "columns requested but no columns client".to_owned(),
                start.elapsed(),
            );
        };
        let creq = ColumnsByRangeRequest {
            start_slot: Slot::new(slot),
            count: 1,
            columns: column_indices.to_vec(),
        };
        let cchunks = match cols_client
            .request_chunks(Protocol::DataColumnSidecarsByRangeV1, creq.to_ssz_bytes())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return (
                    false,
                    format!("columns_by_range transport/codec: {e}"),
                    start.elapsed(),
                );
            }
        };
        if let Some(reason) = negative_verdict(&cchunks, slot, "columns") {
            return (false, reason, start.elapsed());
        }
    }

    (true, String::new(), start.elapsed())
}

/// Negative side: every response must be result byte `3`. Empty success is failure.
///
/// `None` = pass; `Some(reason)` = fail (reason names the slot).
pub fn negative_verdict(chunks: &[ResponseChunk], slot: u64, kind: &str) -> Option<String> {
    // Empty stream = empty success = probe failure naming the slot.
    if chunks.is_empty() {
        return Some(format!(
            "negative side empty success on {kind} for slot {slot}: expected ResourceUnavailable (3)"
        ));
    }
    for c in chunks {
        match c {
            ResponseChunk::Success { ssz, .. } => {
                return Some(format!(
                    "negative side empty success on {kind} for slot {slot}: got success chunk (ssz_len={}), expected ResourceUnavailable (3)",
                    ssz.len()
                ));
            }
            ResponseChunk::Error { code, .. } => {
                if *code != 3 {
                    return Some(format!(
                        "negative side on {kind} for slot {slot}: result byte {code}, expected ResourceUnavailable (3)"
                    ));
                }
            }
        }
    }
    None
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::{SszLimits, encode_error_chunk, encode_success_chunk};

    #[test]
    fn negative_empty_stream_fails_naming_slot() {
        let reason = negative_verdict(&[], 42, "blocks").unwrap();
        assert!(reason.contains("slot 42"), "{reason}");
        assert!(reason.contains("empty success"), "{reason}");
    }

    #[test]
    fn negative_success_chunk_fails_naming_slot() {
        let limits = SszLimits {
            min: 0,
            max: 10 * 1024 * 1024,
        };
        let framed = encode_success_chunk(b"block", true, Some([0; 4]), limits).unwrap();
        let chunks = crate::codec::decode_response_chunks(&framed, true, limits).unwrap();
        let reason = negative_verdict(&chunks, 99, "blocks").unwrap();
        assert!(reason.contains("slot 99"), "{reason}");
    }

    #[test]
    fn negative_resource_unavailable_passes() {
        let framed = encode_error_chunk(3, b"out of window").unwrap();
        let limits = SszLimits {
            min: 0,
            max: 10 * 1024 * 1024,
        };
        let chunks = crate::codec::decode_response_chunks(&framed, true, limits).unwrap();
        assert!(negative_verdict(&chunks, 7, "blocks").is_none());
    }

    #[test]
    fn positive_empty_stream_is_known_empty_pass() {
        assert!(positive_block_verdict(&[]).is_none());
    }

    #[test]
    fn positive_resource_unavailable_fails() {
        let framed = encode_error_chunk(3, b"missing").unwrap();
        let limits = SszLimits {
            min: 0,
            max: 10 * 1024 * 1024,
        };
        let chunks = crate::codec::decode_response_chunks(&framed, true, limits).unwrap();
        assert!(
            positive_block_verdict(&chunks)
                .unwrap()
                .contains("ResourceUnavailable")
        );
    }
}
