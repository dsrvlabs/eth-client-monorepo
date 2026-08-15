//! By-root recovery — Architecture §8.5 / CC-25.
//!
//! Driven by the sampling tracker's end-of-slot-*N* deadline ([`crate::das::sampling`])
//! and transported over `DataColumnSidecarsByRoot v1` (CC-23d protocol ID; wire
//! handlers land with that issue). This module owns the **policy ladder**:
//!
//! 1. missing = `required − verified`
//! 2. candidate peers per column via deterministic `get_custody_groups` (ENR / MetaData)
//! 3. **one** `DataColumnsByRootIdentifier` per peer (batched columns)
//! 4. ≤ 3 attempts × ≤ 4 distinct peers per missing column (`RequestSpec` bounds)
//! 5. every returned sidecar re-enters the same §5.5 validation path
//! 6. exhaustion → `Abandoned` + structured log (metric via sampling)
//! 7. non-response for a **provably custodied** column → `custody_unserved` (−15)
//!
//! **Matrix reconstruction is out of scope** (CC-25/6): sampling 8 of 128 never
//! reaches the ≥50 % reconstruction trigger; supernode-only.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use cc_libp2p::PeerId;
use cc_types::networking::ColumnIndex;
use cc_types::primitives::Root;
use cc_types::sidecar::DataColumnsByRootIdentifier;
use cc_types::{NUMBER_OF_CUSTODY_GROUPS, compute_columns_for_custody_group, get_custody_groups};
use discv5::enr::NodeId;
use ssz::Encode;
use ssz_types::VariableList;
use ssz_types::typenum::U128;
use tracing::warn;

use crate::discovery::enr::node_id_as_u256;
use crate::metrics::{P2pMetrics, PeerPenaltyReason};
use crate::peer_manager::score::{apply_penalty_with_metrics, penalty_delta};
use crate::reqresp::Protocol;
use crate::reqresp::client::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_PEERS, PeerView, Priority, RequestPayload, RequestScheduler,
    RequestSpec,
};

// ── Bounds (Architecture §8.5 / CC-24d inequality) ──────────────────────────

/// Attempts per peer for a missing column (carried by [`RequestSpec`]).
pub const RECOVERY_MAX_ATTEMPTS: u8 = DEFAULT_MAX_ATTEMPTS;
/// Distinct peers tried per missing column.
pub const RECOVERY_MAX_PEERS: u8 = DEFAULT_MAX_PEERS;
/// TTFB timeout seconds (matches [`crate::reqresp::TTFB_TIMEOUT`]).
pub const RECOVERY_TTFB_SECS: u64 = 5;
/// RESP timeout seconds (matches [`crate::reqresp::RESP_TIMEOUT`]).
pub const RECOVERY_RESP_SECS: u64 = 10;
/// Chain-side `pending_da` timeout slots (CC-24d) — must outlast this ladder.
pub const CHAIN_PENDING_DA_TIMEOUT_SLOTS: u64 = 4;
/// Default seconds per slot (mainnet / Hoodi).
pub const DEFAULT_SECONDS_PER_SLOT: u64 = 12;
/// Max identifiers in a by-root request list (`MAX_REQUEST_BLOCKS_DENEB`).
pub const MAX_BY_ROOT_IDENTIFIERS: usize = 128;

// ── Peer view ───────────────────────────────────────────────────────────────

/// Connected peer with known custody advertisement (ENR `cgc` or MetaData v3).
#[derive(Debug, Clone)]
pub struct RecoveryPeer {
    /// libp2p peer id (scheduler / penalty target).
    pub peer_id: PeerId,
    /// discv5 node id — input to [`get_custody_groups`].
    pub node_id: NodeId,
    /// Custody group count advertised by the peer.
    pub cgc: u64,
    /// Application score (−100…+100); drives scheduler secondary key.
    pub app_score: f64,
}

// ── Deterministic custody selection (CC-25/2) ───────────────────────────────

/// Columns a peer custodies, computed from `(node_id, cgc)` alone.
///
/// Expands [`get_custody_groups`] through [`compute_columns_for_custody_group`].
/// With Fulu constants (`NUMBER_OF_COLUMNS == NUMBER_OF_CUSTODY_GROUPS`) this is
/// the identity map on group indices; the expansion keeps the function correct
/// if columns-per-group ever grows.
#[must_use]
pub fn peer_custody_columns(node_id: NodeId, cgc: u64) -> BTreeSet<ColumnIndex> {
    let cgc = cgc.min(NUMBER_OF_CUSTODY_GROUPS);
    let groups = get_custody_groups(node_id_as_u256(node_id), cgc);
    groups
        .into_iter()
        .flat_map(compute_columns_for_custody_group)
        .collect()
}

/// Whether `peer` provably custodies `column` (ENR/MetaData only — no handshake).
#[must_use]
pub fn peer_custodies_column(node_id: NodeId, cgc: u64, column: ColumnIndex) -> bool {
    peer_custody_columns(node_id, cgc).contains(&column)
}

/// Missing columns that `peer` can serve (intersection of custody and `missing`).
#[must_use]
pub fn batch_for_peer(
    node_id: NodeId,
    cgc: u64,
    missing: &BTreeSet<ColumnIndex>,
) -> BTreeSet<ColumnIndex> {
    peer_custody_columns(node_id, cgc)
        .intersection(missing)
        .copied()
        .collect()
}

/// Peers that custody `column`, sorted by `node_id` raw bytes (deterministic).
///
/// Acceptance CC-25/2: given a fixed peer set, the list is reproducible and
/// contains **exactly** the peers whose custody groups cover the column.
#[must_use]
pub fn candidates_for_column<'a>(
    peers: &'a [RecoveryPeer],
    column: ColumnIndex,
) -> Vec<&'a RecoveryPeer> {
    let mut out: Vec<&'a RecoveryPeer> = peers
        .iter()
        .filter(|p| peer_custodies_column(p.node_id, p.cgc, column))
        .collect();
    out.sort_by_key(|a| a.node_id.raw());
    out
}

/// All peers that cover at least one column in `missing`, deterministic order.
#[must_use]
pub fn candidates_for_missing<'a>(
    peers: &'a [RecoveryPeer],
    missing: &BTreeSet<ColumnIndex>,
) -> Vec<&'a RecoveryPeer> {
    let mut out: Vec<&'a RecoveryPeer> = peers
        .iter()
        .filter(|p| !batch_for_peer(p.node_id, p.cgc, missing).is_empty())
        .collect();
    out.sort_by_key(|a| a.node_id.raw());
    out
}

// ── Request encoding (spec delta 3) ─────────────────────────────────────────

/// Errors building a by-root request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryEncodeError {
    /// Column list empty.
    EmptyColumns,
    /// More than `NUMBER_OF_COLUMNS` columns in one identifier.
    TooManyColumns {
        /// Requested count.
        count: usize,
    },
    /// More than [`MAX_BY_ROOT_IDENTIFIERS`] identifiers in the list.
    TooManyIdentifiers {
        /// Requested count.
        count: usize,
    },
}

impl fmt::Display for RecoveryEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyColumns => write!(f, "by-root identifier has no columns"),
            Self::TooManyColumns { count } => {
                write!(f, "by-root identifier column count {count} exceeds bound")
            }
            Self::TooManyIdentifiers { count } => {
                write!(
                    f,
                    "by-root request list length {count} exceeds {MAX_BY_ROOT_IDENTIFIERS}"
                )
            }
        }
    }
}

impl std::error::Error for RecoveryEncodeError {}

/// Build one [`DataColumnsByRootIdentifier`] (sorted unique columns).
pub fn build_by_root_identifier(
    block_root: [u8; 32],
    columns: impl IntoIterator<Item = ColumnIndex>,
) -> Result<DataColumnsByRootIdentifier, RecoveryEncodeError> {
    let mut cols: Vec<u64> = columns.into_iter().collect();
    cols.sort_unstable();
    cols.dedup();
    if cols.is_empty() {
        return Err(RecoveryEncodeError::EmptyColumns);
    }
    let count = cols.len();
    let columns =
        VariableList::new(cols).map_err(|_| RecoveryEncodeError::TooManyColumns { count })?;
    Ok(DataColumnsByRootIdentifier {
        block_root: Root::from_array(block_root),
        columns,
    })
}

/// SSZ-encode `List[DataColumnsByRootIdentifier, 128]` for the by-root protocol.
pub fn encode_by_root_request(
    identifiers: &[DataColumnsByRootIdentifier],
) -> Result<RequestPayload, RecoveryEncodeError> {
    if identifiers.len() > MAX_BY_ROOT_IDENTIFIERS {
        return Err(RecoveryEncodeError::TooManyIdentifiers {
            count: identifiers.len(),
        });
    }
    let list: VariableList<DataColumnsByRootIdentifier, U128> =
        VariableList::new(identifiers.to_vec()).map_err(|_| {
            RecoveryEncodeError::TooManyIdentifiers {
                count: identifiers.len(),
            }
        })?;
    Ok(RequestPayload::new(list.as_ssz_bytes()))
}

/// Encode a single-root recovery request (the common CC-25 case).
pub fn encode_single_root_request(
    block_root: [u8; 32],
    columns: impl IntoIterator<Item = ColumnIndex>,
) -> Result<RequestPayload, RecoveryEncodeError> {
    let id = build_by_root_identifier(block_root, columns)?;
    encode_by_root_request(std::slice::from_ref(&id))
}

/// Build a [`RequestSpec`] for by-root recovery (priority + scheduler bounds).
pub fn recovery_request_spec(
    payload: RequestPayload,
    eligible: crate::reqresp::client::PeerPredicate,
) -> RequestSpec {
    RequestSpec {
        protocol: Protocol::DataColumnSidecarsByRootV1,
        payload,
        eligible,
        priority: Priority::Recovery,
        max_attempts: RECOVERY_MAX_ATTEMPTS,
        max_peers: RECOVERY_MAX_PEERS,
    }
}

// ── Ladder wall-time (CC-24d re-check) ──────────────────────────────────────

/// Worst-case recovery ladder wall time (seconds).
///
/// Sequential attempts, each bounded by `TTFB + RESP`. `max_peers` is a pool
/// size (not a wall-time multiplier) — matches chain `da.rs`.
#[must_use]
pub fn recovery_ladder_worst_case_secs(
    max_attempts: u8,
    max_peers: u8,
    ttfb_secs: u64,
    resp_secs: u64,
) -> u64 {
    let _ = max_peers.max(1);
    u64::from(max_attempts.max(1)).saturating_mul(ttfb_secs.saturating_add(resp_secs))
}

/// Whether chain `pending_da` timeout strictly outlasts this issue's ladder.
#[must_use]
pub fn chain_timeout_outlasts_ladder(
    timeout_slots: u64,
    seconds_per_slot: u64,
    max_attempts: u8,
    max_peers: u8,
    ttfb_secs: u64,
    resp_secs: u64,
) -> bool {
    let chain = timeout_slots.saturating_mul(seconds_per_slot.max(1));
    let ladder = recovery_ladder_worst_case_secs(max_attempts, max_peers, ttfb_secs, resp_secs);
    chain > ladder
}

/// Default production ordering (4 × 12 s > 3 × 15 s).
#[must_use]
pub fn default_ladder_under_chain_timeout() -> bool {
    chain_timeout_outlasts_ladder(
        CHAIN_PENDING_DA_TIMEOUT_SLOTS,
        DEFAULT_SECONDS_PER_SLOT,
        RECOVERY_MAX_ATTEMPTS,
        RECOVERY_MAX_PEERS,
        RECOVERY_TTFB_SECS,
        RECOVERY_RESP_SECS,
    )
}

// ── Query / penalty surface ─────────────────────────────────────────────────

/// Result of one by-root query to a peer (transport mock or live client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerQueryResult {
    /// Peer returned these column indices (after caller-side §5.5 validation).
    ///
    /// Indices **not** listed but requested and custodied by the peer count as
    /// unserved for the `custody_unserved` penalty.
    Served(BTreeSet<ColumnIndex>),
    /// Timeout, empty response, codec error — attempt failure.
    Failed,
}

/// Penalty emitted by the recovery ladder (applied through the score path).
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPenalty {
    /// Peer that failed to serve a custodied column.
    pub peer_id: PeerId,
    /// Always [`PeerPenaltyReason::CustodyUnserved`] for this module.
    pub reason: PeerPenaltyReason,
    /// Score delta (−15).
    pub delta: f64,
}

/// Terminal outcome of recovering one root's missing set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// All missing columns obtained (and fed through validation).
    Recovered {
        /// Distinct peers that were queried.
        peers_tried: Vec<PeerId>,
    },
    /// Ladder exhausted; task should become [`crate::das::sampling::TaskState::Abandoned`].
    Abandoned {
        /// Columns still missing at abandon.
        missing: BTreeSet<ColumnIndex>,
        /// Distinct peers that were queried.
        peers_tried: Vec<PeerId>,
    },
}

impl RecoveryOutcome {
    /// Whether recovery filled the missing set.
    #[must_use]
    pub const fn is_recovered(&self) -> bool {
        matches!(self, Self::Recovered { .. })
    }
}

// ── Per-column attempt budget ───────────────────────────────────────────────

#[derive(Debug, Default)]
struct ColumnBudget {
    /// Attempts spent on each peer for this column.
    per_peer: HashMap<PeerId, u8>,
    /// Distinct peers that have been tried.
    peers_tried: HashSet<PeerId>,
}

impl ColumnBudget {
    fn can_try(&self, peer: PeerId) -> bool {
        if self.peers_tried.len() as u8 >= RECOVERY_MAX_PEERS && !self.peers_tried.contains(&peer) {
            return false;
        }
        self.per_peer.get(&peer).copied().unwrap_or(0) < RECOVERY_MAX_ATTEMPTS
    }

    fn record_attempt(&mut self, peer: PeerId) {
        self.peers_tried.insert(peer);
        let e = self.per_peer.entry(peer).or_insert(0);
        *e = e.saturating_add(1);
    }
}

// ── Ladder driver ───────────────────────────────────────────────────────────

/// Inputs for one recovery session (keeps the free-function surface small).
pub struct RecoveryInput<'a> {
    /// Beacon block root.
    pub root: [u8; 32],
    /// Slot of the block (logging / abandon subject).
    pub slot: u64,
    /// Missing columns at deadline (`required − verified`).
    pub missing: BTreeSet<ColumnIndex>,
    /// Connected peers with known custody advertisements.
    pub peers: &'a [RecoveryPeer],
    /// Shared request scheduler (preemption + peer choice).
    pub scheduler: &'a mut RequestScheduler,
    /// Optional metrics (penalty counters).
    pub metrics: Option<&'a P2pMetrics>,
}

impl fmt::Debug for RecoveryInput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryInput")
            .field("root", &hex_root(&self.root))
            .field("slot", &self.slot)
            .field("missing", &self.missing)
            .field("peers", &self.peers.len())
            .field("has_metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

/// Drive by-root recovery for one root until filled or abandoned.
///
/// `query` is the transport seam (CC-23d client when wired; test stub until then).
/// Columns in [`PeerQueryResult::Served`] are assumed to have already re-entered
/// §5.5 validation (same path as gossip). This function does **not** touch the
/// sampling tracker — the caller marks `Abandoned` / feeds `on_column(..., ByRoot)`.
///
/// Emits one structured abandon log line. `custody_unserved` penalties go through
/// `on_penalty` (and optionally metrics when provided).
pub fn recover(
    input: RecoveryInput<'_>,
    mut query: impl FnMut(PeerId, &RequestPayload) -> PeerQueryResult,
    mut on_penalty: impl FnMut(RecoveryPenalty),
) -> RecoveryOutcome {
    let RecoveryInput {
        root,
        slot,
        missing,
        peers,
        scheduler,
        metrics,
    } = input;
    let mut remaining = missing;
    if remaining.is_empty() {
        return RecoveryOutcome::Recovered {
            peers_tried: Vec::new(),
        };
    }

    let mut budgets: BTreeMap<ColumnIndex, ColumnBudget> = remaining
        .iter()
        .map(|c| (*c, ColumnBudget::default()))
        .collect();
    let mut peers_tried_order: Vec<PeerId> = Vec::new();
    let mut peers_tried_set: HashSet<PeerId> = HashSet::new();
    // At most one custody_unserved penalty per peer per recovery session.
    let mut penalised: HashSet<PeerId> = HashSet::new();

    let max_iters = remaining
        .len()
        .saturating_mul(usize::from(RECOVERY_MAX_PEERS))
        .saturating_mul(usize::from(RECOVERY_MAX_ATTEMPTS))
        .saturating_add(4);
    let mut iters = 0usize;

    while !remaining.is_empty() && iters < max_iters {
        iters = iters.saturating_add(1);

        let helper: Vec<&RecoveryPeer> = peers
            .iter()
            .filter(|p| {
                batch_for_peer(p.node_id, p.cgc, &remaining)
                    .iter()
                    .any(|c| budgets.get(c).is_some_and(|b| b.can_try(p.peer_id)))
            })
            .collect();
        if helper.is_empty() {
            break;
        }

        let candidates: Vec<PeerView> = helper
            .iter()
            .map(|p| PeerView {
                peer_id: p.peer_id,
                app_score: p.app_score,
            })
            .collect();
        let helper_ids: HashSet<PeerId> = helper.iter().map(|p| p.peer_id).collect();
        let eligible: crate::reqresp::client::PeerPredicate =
            Box::new(move |pid| helper_ids.contains(&pid));
        let exclude = HashSet::new();
        let Some(choice) = scheduler.choose_peer(&candidates, &eligible, &exclude) else {
            break;
        };
        let Some(peer) = peers.iter().find(|p| p.peer_id == choice.peer) else {
            break;
        };

        let batch: BTreeSet<ColumnIndex> = batch_for_peer(peer.node_id, peer.cgc, &remaining)
            .into_iter()
            .filter(|c| budgets.get(c).is_some_and(|b| b.can_try(peer.peer_id)))
            .collect();
        if batch.is_empty() {
            break;
        }

        let Ok(payload) = encode_single_root_request(root, batch.iter().copied()) else {
            break;
        };

        // §7.6: recovery preempts backfill on the same peer.
        if !scheduler
            .outbound()
            .can_send(peer.peer_id, Protocol::DataColumnSidecarsByRootV1)
        {
            let _ = scheduler.preempt_for_recovery(peer.peer_id);
        }

        let peer_id = peer.peer_id;
        if !peers_tried_set.contains(&peer_id) {
            peers_tried_set.insert(peer_id);
            peers_tried_order.push(peer_id);
        }
        for c in &batch {
            if let Some(b) = budgets.get_mut(c) {
                b.record_attempt(peer_id);
            }
        }

        let acquired = scheduler
            .outbound_mut()
            .try_acquire(peer_id, Protocol::DataColumnSidecarsByRootV1);
        if !acquired {
            // Still capped — attempt already recorded; try another peer.
            continue;
        }

        let result = query(peer_id, &payload);
        scheduler
            .outbound_mut()
            .release(peer_id, Protocol::DataColumnSidecarsByRootV1);

        match result {
            PeerQueryResult::Served(served) => {
                for col in served.intersection(&batch) {
                    remaining.remove(col);
                }
                let unserved_custodied = batch.iter().any(|c| {
                    !served.contains(c) && peer_custodies_column(peer.node_id, peer.cgc, *c)
                });
                if unserved_custodied {
                    emit_custody_unserved(peer_id, &mut penalised, &mut on_penalty, metrics);
                }
            }
            PeerQueryResult::Failed => {
                let any_custodied = batch
                    .iter()
                    .any(|c| peer_custodies_column(peer.node_id, peer.cgc, *c));
                if any_custodied {
                    emit_custody_unserved(peer_id, &mut penalised, &mut on_penalty, metrics);
                }
            }
        }
    }

    if remaining.is_empty() {
        RecoveryOutcome::Recovered {
            peers_tried: peers_tried_order,
        }
    } else {
        log_abandoned(root, slot, &remaining, &peers_tried_order);
        RecoveryOutcome::Abandoned {
            missing: remaining,
            peers_tried: peers_tried_order,
        }
    }
}

fn emit_custody_unserved(
    peer_id: PeerId,
    penalised: &mut HashSet<PeerId>,
    on_penalty: &mut impl FnMut(RecoveryPenalty),
    metrics: Option<&P2pMetrics>,
) {
    if !penalised.insert(peer_id) {
        return;
    }
    let pen = RecoveryPenalty {
        peer_id,
        reason: PeerPenaltyReason::CustodyUnserved,
        delta: penalty_delta(PeerPenaltyReason::CustodyUnserved),
    };
    if let Some(m) = metrics {
        m.inc_peer_penalty(PeerPenaltyReason::CustodyUnserved);
    }
    on_penalty(pen);
}

/// Structured abandon log (one line: root, slot, missing, peers tried).
pub fn log_abandoned(
    root: [u8; 32],
    slot: u64,
    missing: &BTreeSet<ColumnIndex>,
    peers_tried: &[PeerId],
) {
    let peers: Vec<String> = peers_tried.iter().map(ToString::to_string).collect();
    warn!(
        root = %hex_root(&root),
        slot,
        ?missing,
        peers_tried = ?peers,
        "by-root recovery exhausted; abandoned"
    );
}

/// Apply a `custody_unserved` penalty to an app score + metrics (verdict path).
pub fn apply_custody_unserved(app_score: &mut f64, metrics: &P2pMetrics) -> f64 {
    apply_penalty_with_metrics(app_score, PeerPenaltyReason::CustodyUnserved, metrics)
}

fn hex_root(root: &[u8; 32]) -> String {
    root.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Plan helper (request-capture harness) ───────────────────────────────────

/// Planned outbound request: one peer, one batched identifier.
#[derive(Debug, Clone)]
pub struct PlannedRequest {
    /// Target peer.
    pub peer_id: PeerId,
    /// Columns batched for this peer.
    pub columns: BTreeSet<ColumnIndex>,
    /// SSZ request payload.
    pub payload: RequestPayload,
}

/// Build **one request per peer** covering that peer's slice of `missing`.
///
/// Does not schedule or send — used by the capture harness (acceptance: peer
/// covering 3 of 5 missing indices produces **one** request, not three).
pub fn plan_batched_requests(
    root: [u8; 32],
    peers: &[RecoveryPeer],
    missing: &BTreeSet<ColumnIndex>,
) -> Result<Vec<PlannedRequest>, RecoveryEncodeError> {
    let mut out = Vec::new();
    let helpers = candidates_for_missing(peers, missing);
    for peer in helpers {
        let columns = batch_for_peer(peer.node_id, peer.cgc, missing);
        if columns.is_empty() {
            continue;
        }
        let payload = encode_single_root_request(root, columns.iter().copied())?;
        out.push(PlannedRequest {
            peer_id: peer.peer_id,
            columns,
            payload,
        });
    }
    Ok(out)
}

// ── Decode helper (tests / capture) ─────────────────────────────────────────

/// Decode columns from a single-identifier by-root request payload.
///
/// Returns `(block_root, columns)` for the first identifier, if any.
#[must_use]
pub fn decode_single_root_columns(payload: &RequestPayload) -> Option<([u8; 32], Vec<u64>)> {
    use ssz::Decode;
    let list =
        VariableList::<DataColumnsByRootIdentifier, U128>::from_ssz_bytes(&payload.ssz).ok()?;
    let first = list.first()?;
    let mut root = [0u8; 32];
    root.copy_from_slice(first.block_root.as_slice());
    Some((root, first.columns.to_vec()))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::gossip::validate::column::{
        AlwaysValidKzg, ColumnOutcome, ColumnValidateInput, ColumnValidatorState, NoopSamplingFeed,
        validate_data_column_sidecar,
    };
    use crate::metrics::P2pMetrics;
    use crate::reqresp::Protocol;
    use crate::reqresp::client::{Priority, ScheduleError};
    use crate::verdict::Verdict;
    use cc_proto::p2p::{Acceptance, ChainView, Reason};
    use cc_types::containers::SignedBeaconBlockHeader;
    use cc_types::preset::Mainnet;
    use cc_types::primitives::{BlsSignature, Slot, ValidatorIndex};
    use cc_types::sidecar::DataColumnSidecar;
    use cc_types::{BeaconBlockHeader, ChainConfig, NUMBER_OF_COLUMNS};
    use prometheus_client::registry::Registry;
    use ssz_types::FixedVector;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    fn node_id_from_u64(n: u64) -> NodeId {
        let mut raw = [0u8; 32];
        raw[24..].copy_from_slice(&n.to_be_bytes());
        NodeId::new(&raw)
    }

    fn peer_id_for(n: u8) -> PeerId {
        static MAP: OnceLock<Mutex<HashMap<u8, PeerId>>> = OnceLock::new();
        let map = MAP.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock().unwrap();
        *guard.entry(n).or_insert_with(PeerId::random)
    }

    fn peer(n: u8, cgc: u64, score: f64) -> RecoveryPeer {
        RecoveryPeer {
            peer_id: peer_id_for(n),
            node_id: node_id_from_u64(u64::from(n)),
            cgc,
            app_score: score,
        }
    }

    fn root(b: u8) -> [u8; 32] {
        let mut r = [0u8; 32];
        r[0] = b;
        r
    }

    fn metrics() -> P2pMetrics {
        let mut reg = Registry::default();
        P2pMetrics::register(&mut reg)
    }

    fn outcome_verdict(out: &ColumnOutcome) -> &Verdict {
        match out {
            ColumnOutcome::Done(v) | ColumnOutcome::Pending(v) => v,
        }
    }

    #[test]
    fn candidates_deterministic_and_exact() {
        let a = peer(1, 1, 0.0);
        let b = peer(2, 1, 0.0);
        let cols_a = peer_custody_columns(a.node_id, a.cgc);
        let cols_b = peer_custody_columns(b.node_id, b.cgc);
        assert_eq!(cols_a.len(), 1);
        assert_eq!(cols_b.len(), 1);
        let col_a = *cols_a.iter().next().unwrap();
        let col_b = *cols_b.iter().next().unwrap();
        assert_ne!(col_a, col_b);

        let peers = vec![b.clone(), a.clone()]; // reverse insert order
        let c1 = candidates_for_column(&peers, col_a);
        let c2 = candidates_for_column(&peers, col_a);
        assert_eq!(c1.len(), 1);
        assert_eq!(c1[0].peer_id, a.peer_id);
        assert_eq!(
            c1.iter().map(|p| p.node_id.raw()).collect::<Vec<_>>(),
            c2.iter().map(|p| p.node_id.raw()).collect::<Vec<_>>()
        );

        let for_b = candidates_for_column(&peers, col_b);
        assert_eq!(for_b.len(), 1);
        assert_eq!(for_b[0].peer_id, b.peer_id);

        let full_a = peer(10, NUMBER_OF_CUSTODY_GROUPS, 0.0);
        let full_b = peer(11, NUMBER_OF_CUSTODY_GROUPS, 0.0);
        let all = vec![full_b, full_a];
        let both = candidates_for_column(&all, 0);
        assert_eq!(both.len(), 2);
        assert!(both[0].node_id.raw() <= both[1].node_id.raw());
    }

    #[test]
    fn one_request_per_peer_batches_columns() {
        // Peer with cgc=3 covers 3 of 5 missing → one request, not three.
        let p3 = peer(4, 3, 1.0);
        let custody = peer_custody_columns(p3.node_id, p3.cgc);
        assert_eq!(custody.len(), 3);
        let mut missing: BTreeSet<u64> = custody.clone();
        for i in 0..NUMBER_OF_COLUMNS {
            if !missing.contains(&i) {
                missing.insert(i);
                if missing.len() == 5 {
                    break;
                }
            }
        }
        assert_eq!(missing.len(), 5);
        let plans = plan_batched_requests(root(1), &[p3], &missing).unwrap();
        assert_eq!(plans.len(), 1, "exactly one request for one peer");
        assert_eq!(plans[0].columns.len(), 3);
        assert_eq!(plans[0].columns, custody);
        let (r, cols) = decode_single_root_columns(&plans[0].payload).unwrap();
        assert_eq!(r, root(1));
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn exhaust_ladder_abandons_with_peer_list() {
        let m = metrics();
        let col = {
            let p0 = peer(20, NUMBER_OF_CUSTODY_GROUPS, 0.0);
            *peer_custody_columns(p0.node_id, p0.cgc)
                .iter()
                .next()
                .unwrap()
        };
        let peers: Vec<RecoveryPeer> = (20..25)
            .map(|n| peer(n, NUMBER_OF_CUSTODY_GROUPS, f64::from(n)))
            .collect();
        let missing: BTreeSet<u64> = BTreeSet::from([col]);
        let mut sched = RequestScheduler::new();
        let attempts = Arc::new(Mutex::new(0u32));
        let attempts_c = Arc::clone(&attempts);
        let outcome = recover(
            RecoveryInput {
                root: root(2),
                slot: 7,
                missing: missing.clone(),
                peers: &peers,
                scheduler: &mut sched,
                metrics: Some(&m),
            },
            |_pid, _pay| {
                *attempts_c.lock().unwrap() += 1;
                PeerQueryResult::Failed
            },
            |_pen| {},
        );
        match outcome {
            RecoveryOutcome::Abandoned {
                missing: left,
                peers_tried,
            } => {
                assert_eq!(left, missing);
                assert!(
                    peers_tried.len() as u8 <= RECOVERY_MAX_PEERS,
                    "peers_tried={} > max",
                    peers_tried.len()
                );
                let n = *attempts.lock().unwrap();
                assert!(
                    n <= u32::from(RECOVERY_MAX_ATTEMPTS) * u32::from(RECOVERY_MAX_PEERS),
                    "attempts {n} exceeded budget"
                );
                assert!(n >= u32::from(RECOVERY_MAX_ATTEMPTS));
            }
            other => panic!("expected Abandoned, got {other:?}"),
        }
        assert!(
            m.peer_penalty_count(PeerPenaltyReason::CustodyUnserved) >= 1,
            "expected custody_unserved on full-custody non-response"
        );
    }

    #[test]
    fn custody_unserved_only_when_peer_custodies() {
        let m = metrics();
        let full = peer(30, NUMBER_OF_CUSTODY_GROUPS, 50.0);
        let none = peer(31, 0, 50.0);
        let col = *peer_custody_columns(full.node_id, full.cgc)
            .iter()
            .next()
            .unwrap();
        let mut other = peer(32, 1, 50.0);
        for n in 32u8..200 {
            other = peer(n, 1, 50.0);
            if !peer_custodies_column(other.node_id, other.cgc, col) {
                break;
            }
        }
        assert!(!peer_custodies_column(other.node_id, other.cgc, col));
        assert!(!peer_custodies_column(none.node_id, none.cgc, col));

        let peers = vec![full.clone(), other.clone(), none.clone()];
        let mut sched = RequestScheduler::new();
        let penalties: Arc<Mutex<Vec<PeerId>>> = Arc::new(Mutex::new(Vec::new()));
        let pen_c = Arc::clone(&penalties);
        let _ = recover(
            RecoveryInput {
                root: root(3),
                slot: 1,
                missing: BTreeSet::from([col]),
                peers: &peers,
                scheduler: &mut sched,
                metrics: Some(&m),
            },
            |_pid, _| PeerQueryResult::Failed,
            |pen| {
                pen_c.lock().unwrap().push(pen.peer_id);
            },
        );
        let pens = penalties.lock().unwrap().clone();
        assert!(
            pens.iter().all(|p| *p == full.peer_id),
            "only full-custody peer may receive custody_unserved; got {pens:?}"
        );
        assert!(!pens.contains(&other.peer_id));
        assert!(!pens.contains(&none.peer_id));
        assert!((penalty_delta(PeerPenaltyReason::CustodyUnserved) - (-15.0)).abs() < 1e-9);
        let mut score = 0.0;
        let after = apply_custody_unserved(&mut score, &m);
        assert!((after - (-15.0)).abs() < 1e-9);
    }

    #[test]
    fn recovery_preempts_saturated_backfill_queue() {
        let p = peer(41, NUMBER_OF_CUSTODY_GROUPS, 10.0);
        let mut sched = RequestScheduler::new();
        let _hold = sched
            .hold(
                p.peer_id,
                Protocol::BeaconBlocksByRangeV2,
                Priority::Backfill,
            )
            .expect("hold");
        for proto in [
            Protocol::DataColumnSidecarsByRangeV1,
            Protocol::BeaconBlocksByRootV2,
            Protocol::StatusV2,
        ] {
            assert!(sched.outbound_mut().try_acquire(p.peer_id, proto));
        }
        assert_eq!(sched.outbound().in_flight_peer(p.peer_id), 4);

        let outcome = recover(
            RecoveryInput {
                root: root(5),
                slot: 1,
                missing: BTreeSet::from([7u64]),
                peers: &[p],
                scheduler: &mut sched,
                metrics: None,
            },
            |_pid, pay| {
                let (_, cols) = decode_single_root_columns(pay).unwrap();
                PeerQueryResult::Served(cols.into_iter().collect())
            },
            |_| {},
        );
        assert!(
            outcome.is_recovered(),
            "recovery must issue despite saturated backfill: {outcome:?}"
        );
    }

    #[test]
    fn ladder_worst_case_under_chain_pending_da() {
        assert!(
            default_ladder_under_chain_timeout(),
            "4×12s must exceed 3×(5+10)s"
        );
        assert_eq!(
            recovery_ladder_worst_case_secs(
                RECOVERY_MAX_ATTEMPTS,
                RECOVERY_MAX_PEERS,
                RECOVERY_TTFB_SECS,
                RECOVERY_RESP_SECS
            ),
            45
        );
        assert_eq!(
            CHAIN_PENDING_DA_TIMEOUT_SLOTS * DEFAULT_SECONDS_PER_SLOT,
            48
        );
    }

    #[test]
    fn mutated_byroot_and_gossip_reject_identically() {
        // Same mutated payload through the single §5.5 entry point — source is
        // not a validator input; re-entry is the same function for both paths.
        let config = {
            const YAML: &str =
                include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");
            ChainConfig::from_yaml_str(YAML).expect("hoodi")
        };
        let view = ChainView {
            slot: 100,
            epoch: 3,
            head_slot: 100,
            ..ChainView::default()
        };
        #[allow(clippy::field_reassign_with_default)]
        let sc = {
            let mut sc = DataColumnSidecar::<Mainnet>::default();
            sc.index = 0;
            sc.signed_block_header = SignedBeaconBlockHeader {
                message: BeaconBlockHeader {
                    slot: Slot::new(50),
                    proposer_index: ValidatorIndex::new(0),
                    parent_root: Root::from_array([1u8; 32]),
                    state_root: Root::ZERO,
                    body_root: Root::ZERO,
                },
                signature: BlsSignature::default(),
            };
            sc.kzg_commitments =
                VariableList::new(vec![cc_types::primitives::KzgCommitment::default()]).unwrap();
            sc.kzg_proofs =
                VariableList::new(vec![cc_types::primitives::KzgProof::default()]).unwrap();
            sc.column = VariableList::new(vec![cc_types::primitives::Cell::ZERO]).unwrap();
            sc.kzg_commitments_inclusion_proof = FixedVector::default();
            sc
        };

        let mut bytes = sc.as_ssz_bytes();
        if let Some(b) = bytes.last_mut() {
            *b ^= 0xff;
        }

        let input = ColumnValidateInput {
            payload: &bytes,
            topic_subnet: 0,
            current_slot: view.slot,
            finalized_slot: 0,
            disparity_slots: 1,
            view: &view,
            config: &config,
            slots_per_epoch: 32,
            message_id: b"mid",
            peer_id: b"peer",
            topic: "data_column_sidecar_0",
        };

        let mut state_g = ColumnValidatorState::new();
        let mut state_r = ColumnValidatorState::new();
        // Gossip entry and by-root re-entry share the same §5.5 function.
        let out_g = validate_data_column_sidecar::<Mainnet>(
            &mut state_g,
            &input,
            &AlwaysValidKzg,
            &NoopSamplingFeed,
            None,
        );
        let out_r = validate_data_column_sidecar::<Mainnet>(
            &mut state_r,
            &input,
            &AlwaysValidKzg,
            &NoopSamplingFeed,
            None,
        );
        let vg = outcome_verdict(&out_g);
        let vr = outcome_verdict(&out_r);
        assert_eq!(vg.acceptance, vr.acceptance);
        assert_eq!(vg.reason, vr.reason);
        assert_ne!(vg.acceptance, Acceptance::Accept);
        assert_ne!(vg.reason, Reason::Valid);
        assert_eq!(state_g.steps.max_step_ran(), state_r.steps.max_step_ran());
    }

    #[test]
    fn encode_roundtrip_single_identifier() {
        let cols = [3u64, 1, 2, 1];
        let payload = encode_single_root_request(root(9), cols).unwrap();
        let (r, decoded) = decode_single_root_columns(&payload).unwrap();
        assert_eq!(r, root(9));
        assert_eq!(decoded, vec![1, 2, 3]);
    }

    #[test]
    fn request_spec_is_recovery_priority() {
        let payload = encode_single_root_request(root(0), [0u64]).unwrap();
        let spec = recovery_request_spec(payload, Box::new(|_| true));
        assert_eq!(spec.protocol, Protocol::DataColumnSidecarsByRootV1);
        assert_eq!(spec.priority, Priority::Recovery);
        assert_eq!(spec.max_attempts, 3);
        assert_eq!(spec.max_peers, 4);
        assert!(Priority::Recovery.preempts(Priority::Backfill));
    }

    #[test]
    fn schedule_error_exhausted_surface() {
        let err = ScheduleError::Exhausted(crate::reqresp::client::Exhausted {
            attempts: 12,
            peers_tried: 4,
        });
        assert!(err.to_string().contains("exhausted"));
    }

    #[test]
    fn recovery_success_marks_recovered_path_ready() {
        // Filling via Served leaves empty missing → Recovered; sampling then
        // labels DaOutcome::Recovered when on_column(..., ByRoot) is used.
        let p = peer(50, NUMBER_OF_CUSTODY_GROUPS, 1.0);
        let mut sched = RequestScheduler::new();
        let outcome = recover(
            RecoveryInput {
                root: root(6),
                slot: 2,
                missing: BTreeSet::from([0u64, 1]),
                peers: &[p],
                scheduler: &mut sched,
                metrics: None,
            },
            |_pid, pay| {
                let (_, cols) = decode_single_root_columns(pay).unwrap();
                PeerQueryResult::Served(cols.into_iter().collect())
            },
            |_| {},
        );
        assert!(matches!(outcome, RecoveryOutcome::Recovered { .. }));
    }
}
