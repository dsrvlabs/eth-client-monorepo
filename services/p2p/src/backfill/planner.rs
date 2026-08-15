//! Backfill planner and gap recovery — Architecture §9.1–§9.4 / CC-26b + §6 / CC-47a.
//!
//! | Surface | Role |
//! |---------|------|
//! | [`GapTrigger`] / [`GapDetected`] | **Five** gap sources → one event (CC-47a fifth) |
//! | [`GapDetector`] | Head jump, clock stall, peer Status, reconnect, **serve-window holes** |
//! | [`plan_batches`] | Split a gap into ≤ [`BATCH_SLOT_LIMIT`]-slot ranges |
//! | [`BackfillPlanner`] | ≤ 4 concurrent batches, peer retry, oldest-first import |
//! | [`BackfillPlanner::custodied_columns`] | Below-anchor mode requests **custodied 4**, not sampled 8 |
//! | [`parent_linkage_walk`] | Post-run completion criterion (clause 4) |
//! | [`feed_backfill_to_sampling`] | No DA bypass — same sampling tracker as gossip |
//!
//! Below-anchor verification (parent-root + whole-batch BLS, one domain) lives
//! in [`super::below`]. Transport is a callback seam: unit tests drive the
//! planner with mock fetches; the host wires live ByRange clients.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use cc_libp2p::PeerId;
use cc_types::primitives::Slot;
use tracing::{debug, warn};

use crate::backfill::rate::OutboundBlockBudget;
use crate::das::sampling::SamplingTracker;
use crate::metrics::{ColumnSource, DaOutcome, P2pMetrics};
use crate::reqresp::Protocol;
use crate::reqresp::blocks::BlocksByRangeRequest;
use crate::reqresp::client::{
    DEFAULT_MAX_ATTEMPTS, PeerView, Priority, RequestPayload, RequestScheduler, RequestSpec,
};
use crate::reqresp::codec::{RESP_TIMEOUT, TTFB_TIMEOUT};
use crate::reqresp::columns::ColumnsByRangeRequest;

// ── Bounds (Architecture §9.2) ──────────────────────────────────────────────

/// Max slots per by-range batch (half of `MAX_REQUEST_BLOCKS_DENEB` = 128).
pub const BATCH_SLOT_LIMIT: u64 = 64;
/// Concurrent in-flight batches (RequestScheduler / §7.6).
pub const MAX_CONCURRENT_BATCHES: usize = 4;
/// Retries on **different** peers before abandon.
pub const BATCH_MAX_PEER_ATTEMPTS: u8 = DEFAULT_MAX_ATTEMPTS;
/// Hard cap on per-batch wall time.
pub const BATCH_TIMEOUT_CAP: Duration = Duration::from_secs(60);
/// Peer Status head gap: `peer.head − our.head >` this → gap.
pub const PEER_STATUS_GAP_THRESHOLD: u64 = 4;
/// Clock advanced with no import: `clock − last_imported ≥` this → gap.
pub const CLOCK_STALL_THRESHOLD: u64 = 2;
/// Head advanced by more than this many slots → gap.
pub const HEAD_JUMP_THRESHOLD: u64 = 1;

// ── Gap detection (§9.1) ────────────────────────────────────────────────────

/// Which of the five gap sensors produced the gap (Architecture §9.1 + §4.6 / CC-47a).
///
/// The first four are about `chain`'s head versus `p2p`'s view and **cannot see
/// a hole in a store they do not read**. The fifth is the serve-window path:
/// storage reports a durable hole (or the window is above its target).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GapTrigger {
    /// `ChainView` head_slot jumped by more than one.
    HeadJump,
    /// Slot clock advanced ≥ 2 slots with no import.
    ClockStall,
    /// Peer's `Status` head is far ahead (eclipse without disconnect).
    PeerStatusAhead,
    /// Transport reconnect after a disconnect.
    TransportReconnect,
    /// Serve window above its target, **or `ServeWindow.holes` non-empty** (§4.6).
    /// Distinct from the Phase 2 four; drives below-anchor `PutBackfillBatch`.
    ServeWindowHoles,
}

impl GapTrigger {
    /// Stable label for logs / metrics (five reasons; fifth distinct).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeadJump => "head_jump",
            Self::ClockStall => "clock_stall",
            Self::PeerStatusAhead => "peer_status_ahead",
            Self::TransportReconnect => "transport_reconnect",
            Self::ServeWindowHoles => "serve_window_holes",
        }
    }

    /// All five trigger reasons (seed + tests).
    pub const ALL: [Self; 5] = [
        Self::HeadJump,
        Self::ClockStall,
        Self::PeerStatusAhead,
        Self::TransportReconnect,
        Self::ServeWindowHoles,
    ];
}

/// One detected gap range (inclusive endpoints in slot units).
///
/// `from_slot` is the first missing slot (last contiguous + 1); `to_slot` is
/// the known head / peer head we need to catch up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GapDetected {
    /// First missing slot (inclusive).
    pub from_slot: Slot,
    /// Last slot to fetch (inclusive).
    pub to_slot: Slot,
    /// Which sensor fired.
    pub trigger: GapTrigger,
}

impl GapDetected {
    /// Inclusive slot count (0 if inverted).
    #[must_use]
    pub fn slot_count(self) -> u64 {
        let from = self.from_slot.as_u64();
        let to = self.to_slot.as_u64();
        to.saturating_add(1).saturating_sub(from)
    }
}

/// Stateful gap detector — four independent sensors, one event type.
#[derive(Debug, Clone)]
pub struct GapDetector {
    /// Last observed local head slot (`ChainView.head_slot`).
    last_head_slot: Option<u64>,
    /// Last slot we successfully imported (contiguous head of our chain).
    last_imported_slot: Option<u64>,
    /// Whether we are currently disconnected from the stream/swarm.
    disconnected: bool,
}

impl Default for GapDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GapDetector {
    /// Fresh detector (no history).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last_head_slot: None,
            last_imported_slot: None,
            disconnected: false,
        }
    }

    /// Seed last-imported (e.g. after StreamHello / full view).
    pub fn set_last_imported(&mut self, slot: u64) {
        self.last_imported_slot = Some(slot);
        if self.last_head_slot.is_none() {
            self.last_head_slot = Some(slot);
        }
    }

    /// Current last-imported slot.
    #[must_use]
    pub const fn last_imported_slot(&self) -> Option<u64> {
        self.last_imported_slot
    }

    /// Record a successful import of `slot` (advances contiguous marker only
    /// when `slot == last + 1` or first import).
    pub fn note_import(&mut self, slot: u64) {
        match self.last_imported_slot {
            None => self.last_imported_slot = Some(slot),
            Some(prev) if slot == prev.saturating_add(1) || slot == prev => {
                self.last_imported_slot = Some(slot);
            }
            Some(_) => {
                // Non-contiguous: leave the marker; planner's oldest-first path
                // is what advances it after a gap fill.
            }
        }
    }

    /// Force the contiguous marker (planner after ordered import).
    pub fn set_contiguous_imported(&mut self, slot: u64) {
        self.last_imported_slot = Some(slot);
    }

    /// §9.1 trigger 1: head advanced by more than one slot.
    pub fn on_head_update(&mut self, head_slot: u64) -> Option<GapDetected> {
        let prev = self.last_head_slot.replace(head_slot)?;
        if head_slot <= prev {
            return None;
        }
        let delta = head_slot - prev;
        if delta <= HEAD_JUMP_THRESHOLD {
            // Single-slot advance — live gossip should cover it.
            return None;
        }
        let from = self
            .last_imported_slot
            .map(|s| s.saturating_add(1))
            .unwrap_or(prev.saturating_add(1));
        if from > head_slot {
            return None;
        }
        Some(GapDetected {
            from_slot: Slot::new(from),
            to_slot: Slot::new(head_slot),
            trigger: GapTrigger::HeadJump,
        })
    }

    /// §9.1 trigger 2: clock advanced with no import (≥ 2 slots).
    pub fn on_clock_tick(&self, clock_slot: u64) -> Option<GapDetected> {
        let last = self.last_imported_slot?;
        if clock_slot < last.saturating_add(CLOCK_STALL_THRESHOLD) {
            return None;
        }
        let from = last.saturating_add(1);
        if from > clock_slot {
            return None;
        }
        Some(GapDetected {
            from_slot: Slot::new(from),
            to_slot: Slot::new(clock_slot),
            trigger: GapTrigger::ClockStall,
        })
    }

    /// §9.1 trigger 3: peer Status head far ahead (eclipse without disconnect).
    ///
    /// Threshold: `peer.head_slot − our.head_slot > PEER_STATUS_GAP_THRESHOLD`.
    pub fn on_peer_status(&self, our_head_slot: u64, peer_head_slot: u64) -> Option<GapDetected> {
        if peer_head_slot <= our_head_slot {
            return None;
        }
        let delta = peer_head_slot - our_head_slot;
        if delta <= PEER_STATUS_GAP_THRESHOLD {
            return None;
        }
        let from = self
            .last_imported_slot
            .map(|s| s.saturating_add(1))
            .unwrap_or(our_head_slot.saturating_add(1));
        if from > peer_head_slot {
            return None;
        }
        Some(GapDetected {
            from_slot: Slot::new(from),
            to_slot: Slot::new(peer_head_slot),
            trigger: GapTrigger::PeerStatusAhead,
        })
    }

    /// §9.1 trigger 4: mark disconnect (no gap until reconnect).
    pub fn on_disconnect(&mut self) {
        self.disconnected = true;
    }

    /// Whether currently flagged disconnected.
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        self.disconnected
    }

    /// §9.1 trigger 4: transport reconnect after disconnect → gap of any size.
    pub fn on_reconnect(&mut self, current_head: u64) -> Option<GapDetected> {
        if !self.disconnected {
            return None;
        }
        self.disconnected = false;
        let from = self
            .last_imported_slot
            .map(|s| s.saturating_add(1))
            .unwrap_or(current_head);
        if from > current_head {
            // Nothing missing — still fire a zero-width signal? Spec says
            // "any"; emit only when there is at least one missing slot.
            return None;
        }
        if from == current_head && self.last_imported_slot == Some(current_head) {
            return None;
        }
        Some(GapDetected {
            from_slot: Slot::new(from),
            to_slot: Slot::new(current_head),
            trigger: GapTrigger::TransportReconnect,
        })
    }

    /// §4.6 / CC-47a fifth trigger: durable serve-window holes, or the window
    /// is above its column/block target.
    ///
    /// `holes` is a list of half-open `[start, end)` ranges from
    /// `WatchServeWindow`. When non-empty the planner enters below-anchor mode
    /// for exactly that range. When empty but `earliest_available_slot > target`,
    /// the gap is `[target, earliest)`.
    pub fn on_serve_window(
        &self,
        holes: &[(u64, u64)],
        earliest_available_slot: u64,
        target_slot: u64,
    ) -> Option<GapDetected> {
        // Prefer an explicit durable hole.
        if let Some(&(start, end)) = holes.first()
            && end > start
        {
            return Some(GapDetected {
                from_slot: Slot::new(start),
                // GapDetected uses inclusive end; holes are half-open.
                to_slot: Slot::new(end.saturating_sub(1)),
                trigger: GapTrigger::ServeWindowHoles,
            });
        }
        // Window above target (serve obligation not yet met).
        if earliest_available_slot > target_slot {
            return Some(GapDetected {
                from_slot: Slot::new(target_slot),
                to_slot: Slot::new(earliest_available_slot.saturating_sub(1)),
                trigger: GapTrigger::ServeWindowHoles,
            });
        }
        None
    }
}

// ── Batch planning (§9.2) ───────────────────────────────────────────────────

/// One planned by-range batch (≤ [`BATCH_SLOT_LIMIT`] slots).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchPlan {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots (`1..=BATCH_SLOT_LIMIT`).
    pub count: u64,
}

impl BatchPlan {
    /// Inclusive end slot.
    #[must_use]
    pub fn end_slot(self) -> Slot {
        Slot::new(
            self.start_slot
                .as_u64()
                .saturating_add(self.count.saturating_sub(1)),
        )
    }

    /// Build the blocks-by-range request body.
    #[must_use]
    pub fn blocks_request(self) -> BlocksByRangeRequest {
        BlocksByRangeRequest::new(self.start_slot, self.count)
    }

    /// Build the columns-by-range request for **sampled** indices (CC-26/1).
    #[must_use]
    pub fn columns_request(self, sampled_columns: &[u64]) -> ColumnsByRangeRequest {
        ColumnsByRangeRequest {
            start_slot: self.start_slot,
            count: self.count,
            columns: sampled_columns.to_vec(),
        }
    }
}

/// Split `[from, to]` (inclusive) into batches of ≤ [`BATCH_SLOT_LIMIT`] slots.
#[must_use]
pub fn plan_batches(from: Slot, to: Slot) -> Vec<BatchPlan> {
    let mut start = from.as_u64();
    let end = to.as_u64();
    if start > end {
        return Vec::new();
    }
    let mut out = Vec::new();
    while start <= end {
        let remaining = end - start + 1;
        let count = remaining.min(BATCH_SLOT_LIMIT);
        out.push(BatchPlan {
            start_slot: Slot::new(start),
            count,
        });
        start = start.saturating_add(count);
    }
    out
}

/// Per-batch timeout: `TTFB + expected_chunks × RESP`, capped at 60 s.
///
/// `expected_chunks` is blocks + column sidecars the peer should stream.
#[must_use]
pub fn batch_timeout(expected_chunks: u64) -> Duration {
    let chunks = expected_chunks.max(1);
    let body = RESP_TIMEOUT.saturating_mul(chunks as u32);
    let total = TTFB_TIMEOUT.saturating_add(body);
    total.min(BATCH_TIMEOUT_CAP)
}

/// Expected response chunks for a batch: `count` blocks + `count × sampled`.
#[must_use]
pub fn expected_chunks(count: u64, sampled_len: usize) -> u64 {
    count.saturating_add(count.saturating_mul(sampled_len as u64))
}

// ── Peer eligibility ────────────────────────────────────────────────────────

/// Connected peer view for backfill eligibility.
#[derive(Debug, Clone)]
pub struct BackfillPeer {
    /// libp2p peer id.
    pub peer_id: PeerId,
    /// Application score (−100…+100).
    pub app_score: f64,
    /// Peer's advertised earliest available slot.
    pub earliest_available_slot: Slot,
    /// Whether the peer's fork digest matches ours (Status / MetaData).
    pub fork_digest_matches: bool,
}

impl BackfillPeer {
    /// Eligible for a batch starting at `batch_from` (§9.2).
    #[must_use]
    pub fn eligible_for(&self, batch_from: Slot) -> bool {
        self.fork_digest_matches && self.earliest_available_slot.as_u64() <= batch_from.as_u64()
    }
}

/// Peers eligible for `batch_from`, for scheduler predicates / tests.
#[must_use]
pub fn eligible_peers(peers: &[BackfillPeer], batch_from: Slot) -> Vec<&BackfillPeer> {
    peers
        .iter()
        .filter(|p| p.eligible_for(batch_from))
        .collect()
}

// ── Fetched material ────────────────────────────────────────────────────────

/// One block returned by a by-range fetch (caller already decoded SSZ).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock {
    /// Slot.
    pub slot: Slot,
    /// Block root.
    pub root: [u8; 32],
    /// Parent root (for linkage walk / UNKNOWN_PARENT guard).
    pub parent_root: [u8; 32],
    /// Blob commitment count (0 → zero-blob R-4 path).
    pub commitment_count: u64,
}

/// One column sidecar returned by a by-range fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedColumn {
    /// Slot.
    pub slot: Slot,
    /// Block root the column attests.
    pub root: [u8; 32],
    /// Column index.
    pub column_index: u64,
}

/// Successful batch fetch payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFetch {
    /// Blocks in any order (planner re-sorts for oldest-first import).
    pub blocks: Vec<FetchedBlock>,
    /// Columns in any order.
    pub columns: Vec<FetchedColumn>,
}

// ── Planner state ───────────────────────────────────────────────────────────

/// Lifecycle of one planned batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchStatus {
    /// Waiting for a free concurrent slot / peer.
    Pending,
    /// Request in flight.
    InFlight,
    /// Fetched; waiting for oldest-first import release.
    Ready,
    /// Fully imported (or skipped as empty).
    Done,
    /// Abandoned after max peer attempts.
    Abandoned,
}

/// Runtime state for one batch.
#[derive(Debug, Clone)]
pub struct BatchState {
    /// Stable id (index in the gap's batch list).
    pub id: u32,
    /// Planned range.
    pub plan: BatchPlan,
    /// Status.
    pub status: BatchStatus,
    /// Distinct peers already tried.
    pub peers_tried: HashSet<PeerId>,
    /// Current peer (if in flight).
    pub current_peer: Option<PeerId>,
    /// Fetch result when Ready.
    pub fetch: Option<BatchFetch>,
}

/// A block ready for the import path (after oldest-first release).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReady {
    /// Slot.
    pub slot: Slot,
    /// Block root.
    pub root: [u8; 32],
    /// Parent root.
    pub parent_root: [u8; 32],
    /// Commitment count.
    pub commitment_count: u64,
    /// Columns fetched for this block (sampled set, possibly incomplete).
    pub columns: Vec<u64>,
}

/// Outcome of feeding a backfilled block into the sampling tracker (no DA bypass).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillDaResult {
    /// All sampled columns present → DA complete (Imported path).
    Available,
    /// Missing at least one sampled column → deferred (same as gossip).
    Deferred,
    /// Zero-blob block completed immediately.
    ZeroBlob,
}

/// Three §9.4 completion criteria.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionStatus {
    /// No batch outstanding (Pending / InFlight / Ready).
    pub no_batch_outstanding: bool,
    /// `head_slot − last_contiguous_imported_slot ≤ 1`.
    pub head_caught_up: bool,
    /// Parent-linkage walk from anchor shows no gap.
    pub parent_linkage_clean: bool,
}

impl CompletionStatus {
    /// All three required.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.no_batch_outstanding && self.head_caught_up && self.parent_linkage_clean
    }
}

/// Recorded abandoned gap (keep running, never exit — Phase 1 §9.4 policy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedGap {
    /// Batch that was abandoned.
    pub batch: BatchPlan,
    /// Peers tried.
    pub peers_tried: Vec<PeerId>,
}

/// Backfill range planner (control plane).
pub struct BackfillPlanner {
    /// Sampled column indices (length 8 at Phase 2 default) — forward/DA path.
    sampled_columns: Vec<u64>,
    /// Custodied column indices (length 4 at Phase 2 default) — below-anchor path.
    custodied_columns: Vec<u64>,
    /// Batches for the active gap (ordered by start slot).
    batches: Vec<BatchState>,
    /// Next contiguous slot expected for import (oldest-first cursor).
    next_import_slot: Option<u64>,
    /// Last contiguous imported slot.
    last_contiguous_imported: Option<u64>,
    /// Blocks held for oldest-first release, keyed by slot.
    hold: BTreeMap<u64, ImportReady>,
    /// Roots already seen (gossip or prior import) — skip request / stream once.
    seen_roots: HashSet<[u8; 32]>,
    /// Slots already held in the backfill cache — skip re-request.
    cached_slots: BTreeSet<u64>,
    /// Roots already sent down the chain stream this run.
    streamed_roots: HashSet<[u8; 32]>,
    /// Abandoned batches (gap record; planner keeps running).
    recorded_gaps: Vec<RecordedGap>,
    /// Optional metrics.
    metrics: Option<P2pMetrics>,
    /// Parent chain for linkage walk: root → parent.
    parents: HashMap<[u8; 32], [u8; 32]>,
    /// Slot → root for linkage.
    slot_roots: BTreeMap<u64, [u8; 32]>,
    /// Anchor root for parent walk (first imported / known).
    anchor_root: Option<[u8; 32]>,
    /// Anchor slot.
    anchor_slot: Option<u64>,
    /// Trigger reason of the most recently accepted gap (five labels).
    last_trigger: Option<GapTrigger>,
    /// How many times each trigger was accepted (test-visible).
    trigger_counts: HashMap<GapTrigger, u64>,
    /// Outbound block self-limit (CC-47b /5 — 128 blocks / 10 s per peer).
    outbound_blocks: OutboundBlockBudget,
}

impl fmt::Debug for BackfillPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackfillPlanner")
            .field("sampled", &self.sampled_columns.len())
            .field("batches", &self.batches.len())
            .field("next_import", &self.next_import_slot)
            .field("last_contiguous", &self.last_contiguous_imported)
            .field("hold", &self.hold.len())
            .field("recorded_gaps", &self.recorded_gaps.len())
            .finish_non_exhaustive()
    }
}

impl BackfillPlanner {
    /// New planner with the node's **sampled** column set (must be the 8-wide set).
    ///
    /// Custodied defaults to the first four of the sampled set (Phase 2 default:
    /// custody ⊆ sample). Override with [`Self::with_custodied`].
    #[must_use]
    pub fn new(sampled_columns: impl IntoIterator<Item = u64>) -> Self {
        let mut cols: Vec<u64> = sampled_columns.into_iter().collect();
        cols.sort_unstable();
        cols.dedup();
        let custodied: Vec<u64> = cols.iter().copied().take(4).collect();
        Self {
            sampled_columns: cols,
            custodied_columns: custodied,
            batches: Vec::new(),
            next_import_slot: None,
            last_contiguous_imported: None,
            hold: BTreeMap::new(),
            seen_roots: HashSet::new(),
            cached_slots: BTreeSet::new(),
            streamed_roots: HashSet::new(),
            recorded_gaps: Vec::new(),
            metrics: None,
            parents: HashMap::new(),
            slot_roots: BTreeMap::new(),
            anchor_root: None,
            anchor_slot: None,
            last_trigger: None,
            trigger_counts: HashMap::new(),
            outbound_blocks: OutboundBlockBudget::new(),
        }
    }

    /// Override the custodied set (CC-47a / das-core custody groups).
    #[must_use]
    pub fn with_custodied(mut self, custodied: impl IntoIterator<Item = u64>) -> Self {
        let mut cols: Vec<u64> = custodied.into_iter().collect();
        cols.sort_unstable();
        cols.dedup();
        self.custodied_columns = cols;
        self
    }

    /// Attach metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: P2pMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Borrow the outbound block budget (CC-47b /5 counters + bound checks).
    #[must_use]
    pub fn outbound_block_budget(&self) -> &OutboundBlockBudget {
        &self.outbound_blocks
    }

    /// Mutable budget (tests that inject time / force reserves).
    pub fn outbound_block_budget_mut(&mut self) -> &mut OutboundBlockBudget {
        &mut self.outbound_blocks
    }

    /// Sampled columns this planner will request on the **forward** path
    /// (CC-26/1: 8, not custodied 4).
    #[must_use]
    pub fn sampled_columns(&self) -> &[u64] {
        &self.sampled_columns
    }

    /// Custodied columns requested on the **below-anchor** path (CC-47a: 4).
    #[must_use]
    pub fn custodied_columns(&self) -> &[u64] {
        &self.custodied_columns
    }

    /// Mode for a planned batch given the current anchor (Architecture §6.1).
    #[must_use]
    pub fn mode_for(&self, plan: BatchPlan) -> super::below::BackfillMode {
        let anchor = self.anchor_slot.unwrap_or(u64::MAX);
        super::below::mode_for_batch(Slot::new(anchor), plan)
    }

    /// Columns-by-range for a batch: **custodied** below anchor, **sampled** forward.
    #[must_use]
    pub fn columns_request_for_mode(&self, plan: BatchPlan) -> ColumnsByRangeRequest {
        match self.mode_for(plan) {
            super::below::BackfillMode::Below => plan.columns_request(&self.custodied_columns),
            super::below::BackfillMode::Forward => plan.columns_request(&self.sampled_columns),
        }
    }

    /// How many times `trigger` has been accepted this run.
    #[must_use]
    pub fn trigger_count(&self, trigger: GapTrigger) -> u64 {
        self.trigger_counts.get(&trigger).copied().unwrap_or(0)
    }

    /// Most recent accepted trigger.
    #[must_use]
    pub const fn last_trigger(&self) -> Option<GapTrigger> {
        self.last_trigger
    }

    /// Seed contiguous import cursor (last known good slot).
    pub fn set_last_contiguous(&mut self, slot: u64) {
        self.last_contiguous_imported = Some(slot);
        self.next_import_slot = Some(slot.saturating_add(1));
        if self.anchor_slot.is_none() {
            self.anchor_slot = Some(slot);
        }
    }

    /// Seed anchor for parent-linkage walk.
    pub fn set_anchor(&mut self, slot: u64, root: [u8; 32]) {
        self.anchor_slot = Some(slot);
        self.anchor_root = Some(root);
        self.slot_roots.insert(slot, root);
        self.set_last_contiguous(slot);
    }

    /// Note a root already known via gossip / cache (dedup §9.3).
    pub fn note_seen_root(&mut self, root: [u8; 32]) {
        self.seen_roots.insert(root);
    }

    /// Note a slot already present in the backfill cache (skip re-request).
    pub fn note_cached_slot(&mut self, slot: u64) {
        self.cached_slots.insert(slot);
    }

    /// Whether `root` is known (seen set).
    #[must_use]
    pub fn has_seen_root(&self, root: &[u8; 32]) -> bool {
        self.seen_roots.contains(root)
    }

    /// Last contiguous imported slot.
    #[must_use]
    pub const fn last_contiguous_imported(&self) -> Option<u64> {
        self.last_contiguous_imported
    }

    /// Batches currently tracked.
    #[must_use]
    pub fn batches(&self) -> &[BatchState] {
        &self.batches
    }

    /// Recorded abandoned gaps (planner still running).
    #[must_use]
    pub fn recorded_gaps(&self) -> &[RecordedGap] {
        &self.recorded_gaps
    }

    /// How many batches are Pending or InFlight.
    #[must_use]
    pub fn outstanding_count(&self) -> usize {
        self.batches
            .iter()
            .filter(|b| matches!(b.status, BatchStatus::Pending | BatchStatus::InFlight))
            .count()
    }

    /// How many are Ready (fetched, awaiting oldest-first import).
    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.batches
            .iter()
            .filter(|b| b.status == BatchStatus::Ready)
            .count()
    }

    /// In-flight batch count.
    #[must_use]
    pub fn inflight_count(&self) -> usize {
        self.batches
            .iter()
            .filter(|b| b.status == BatchStatus::InFlight)
            .count()
    }

    /// Accept a gap: merge with existing pending work, plan batches.
    pub fn on_gap(&mut self, gap: GapDetected) {
        if gap.from_slot.as_u64() > gap.to_slot.as_u64() {
            return;
        }
        self.last_trigger = Some(gap.trigger);
        *self.trigger_counts.entry(gap.trigger).or_insert(0) += 1;

        // Below-anchor / serve-window holes: do **not** force-align to the
        // forward import cursor — the range is absolute on the store.
        let below_anchor_gap = matches!(gap.trigger, GapTrigger::ServeWindowHoles)
            || self.anchor_slot.is_some_and(|a| gap.to_slot.as_u64() <= a);

        let from = if below_anchor_gap {
            gap.from_slot
        } else {
            // Align from_slot with contiguous cursor when known (forward path).
            match self.next_import_slot {
                Some(n) => Slot::new(n.max(gap.from_slot.as_u64())),
                None => {
                    self.next_import_slot = Some(gap.from_slot.as_u64());
                    gap.from_slot
                }
            }
        };
        if from.as_u64() > gap.to_slot.as_u64() {
            return;
        }

        let planned = if below_anchor_gap {
            super::below::plan_batches_descending(from, gap.to_slot)
        } else {
            plan_batches(from, gap.to_slot)
        };
        let existing: HashSet<(u64, u64)> = self
            .batches
            .iter()
            .filter(|b| !matches!(b.status, BatchStatus::Done | BatchStatus::Abandoned))
            .map(|b| (b.plan.start_slot.as_u64(), b.plan.count))
            .collect();

        let mut next_id = self.batches.iter().map(|b| b.id).max().unwrap_or(0);
        for plan in planned {
            let key = (plan.start_slot.as_u64(), plan.count);
            if existing.contains(&key) {
                continue;
            }
            // Skip batches fully covered by cache.
            if (0..plan.count).all(|i| {
                self.cached_slots
                    .contains(&(plan.start_slot.as_u64().saturating_add(i)))
            }) {
                continue;
            }
            next_id = next_id.saturating_add(1);
            self.batches.push(BatchState {
                id: next_id,
                plan,
                status: BatchStatus::Pending,
                peers_tried: HashSet::new(),
                current_peer: None,
                fetch: None,
            });
        }
        debug!(
            trigger = gap.trigger.as_str(),
            from = from.as_u64(),
            to = gap.to_slot.as_u64(),
            batches = self.batches.len(),
            "backfill gap accepted"
        );
    }

    /// Columns-by-range request body for a batch — always **sampled** indices.
    #[must_use]
    pub fn columns_request_for(&self, plan: BatchPlan) -> ColumnsByRangeRequest {
        plan.columns_request(&self.sampled_columns)
    }

    /// Blocks-by-range SSZ payload for scheduling.
    #[must_use]
    pub fn blocks_payload(plan: BatchPlan) -> RequestPayload {
        RequestPayload::new(plan.blocks_request().to_ssz_bytes().to_vec())
    }

    /// Columns-by-range SSZ payload (sampled set).
    #[must_use]
    pub fn columns_payload(&self, plan: BatchPlan) -> RequestPayload {
        RequestPayload::new(self.columns_request_for(plan).to_ssz_bytes())
    }

    /// Build a backfill `RequestSpec` for blocks by range.
    #[must_use]
    pub fn blocks_request_spec(
        plan: BatchPlan,
        eligible: crate::reqresp::client::PeerPredicate,
    ) -> RequestSpec {
        RequestSpec {
            protocol: Protocol::BeaconBlocksByRangeV2,
            payload: Self::blocks_payload(plan),
            eligible,
            priority: Priority::Backfill,
            max_attempts: 1, // planner owns multi-peer rotation
            max_peers: 1,
        }
    }

    /// Assign pending batches to peers (≤ [`MAX_CONCURRENT_BATCHES`] in flight).
    ///
    /// Prefers distinct peers. Returns `(batch_id, peer, plan)` assignments.
    ///
    /// Respects the outbound block budget (CC-47b /5): a peer that would exceed
    /// 128 blocks / 10 s is skipped so the planner cannot concentrate the whole
    /// queue on the single deepest window.
    pub fn schedule(
        &mut self,
        peers: &[BackfillPeer],
        scheduler: &mut RequestScheduler,
    ) -> Vec<(u32, PeerId, BatchPlan)> {
        self.schedule_at(peers, scheduler, Instant::now())
    }

    /// [`Self::schedule`] with an explicit clock (unit tests inject time).
    pub fn schedule_at(
        &mut self,
        peers: &[BackfillPeer],
        scheduler: &mut RequestScheduler,
        now: Instant,
    ) -> Vec<(u32, PeerId, BatchPlan)> {
        let mut assignments = Vec::new();
        let mut peers_in_use: HashSet<PeerId> = self
            .batches
            .iter()
            .filter_map(|b| {
                if b.status == BatchStatus::InFlight {
                    b.current_peer
                } else {
                    None
                }
            })
            .collect();

        // Batches are marked InFlight as they are assigned, so the live
        // inflight_count alone caps concurrency (do not also add assignments.len()).
        while self.inflight_count() < MAX_CONCURRENT_BATCHES {
            let Some(idx) = self
                .batches
                .iter()
                .position(|b| b.status == BatchStatus::Pending)
            else {
                break;
            };
            let plan = self.batches[idx].plan;
            let tried = self.batches[idx].peers_tried.clone();
            let count = plan.count;

            // Pre-filter peers with enough outbound budget for this batch count.
            // (available mutates only the peer's token-bucket refill timestamp.)
            let mut with_budget: HashSet<PeerId> = HashSet::new();
            for p in peers {
                if p.eligible_for(plan.start_slot)
                    && !tried.contains(&p.peer_id)
                    && self.outbound_blocks.available(p.peer_id, now) >= count
                {
                    with_budget.insert(p.peer_id);
                }
            }

            let candidates: Vec<PeerView> = peers
                .iter()
                .filter(|p| with_budget.contains(&p.peer_id) && !peers_in_use.contains(&p.peer_id))
                .map(|p| PeerView {
                    peer_id: p.peer_id,
                    app_score: p.app_score,
                })
                .collect();

            // Fall back: allow peer reuse if no distinct peer free (still rate-capped).
            let candidates = if candidates.is_empty() {
                peers
                    .iter()
                    .filter(|p| with_budget.contains(&p.peer_id))
                    .map(|p| PeerView {
                        peer_id: p.peer_id,
                        app_score: p.app_score,
                    })
                    .collect()
            } else {
                candidates
            };

            if candidates.is_empty() {
                // No eligible peer with budget — wait for refill / new peers.
                // Do not abandon solely on rate limit (peers_tried is for failures).
                break;
            }

            let eligible_ids: HashSet<PeerId> = candidates.iter().map(|c| c.peer_id).collect();
            let eligible: crate::reqresp::client::PeerPredicate =
                Box::new(move |pid| eligible_ids.contains(&pid));
            let exclude = HashSet::new();
            let Some(choice) = scheduler.choose_peer(&candidates, &eligible, &exclude) else {
                break;
            };

            // Debit budget; if a race emptied it, try another peer next loop.
            if !self.outbound_blocks.try_reserve(choice.peer, count, now) {
                break;
            }

            // Local schedule attach: batch must still be Pending. If not
            // (concurrent mutation), refund the reservation cheaply.
            if self.batches[idx].status != BatchStatus::Pending {
                let _ = self.outbound_blocks.refund(choice.peer, count, now);
                continue;
            }

            let batch = &mut self.batches[idx];
            batch.status = BatchStatus::InFlight;
            batch.current_peer = Some(choice.peer);
            batch.peers_tried.insert(choice.peer);
            peers_in_use.insert(choice.peer);
            assignments.push((batch.id, choice.peer, plan));
        }
        assignments
    }

    /// Refund outbound block budget for an assignment the host could not
    /// dispatch (local failure before the wire). Does not touch batch state.
    pub fn refund_outbound_reservation(&mut self, peer: PeerId, count: u64, now: Instant) -> bool {
        self.outbound_blocks.refund(peer, count, now)
    }

    /// Record a successful fetch for `batch_id`.
    pub fn on_batch_success(&mut self, batch_id: u32, fetch: BatchFetch) {
        let Some(idx) = self.batches.iter().position(|b| b.id == batch_id) else {
            return;
        };
        if self.batches[idx].status != BatchStatus::InFlight {
            return;
        }
        // Drop blocks already in seen/cache from the hold path; still mark Ready.
        let mut filtered = BatchFetch {
            blocks: Vec::new(),
            columns: fetch.columns,
        };
        for b in fetch.blocks {
            if self.seen_roots.contains(&b.root) || self.cached_slots.contains(&b.slot.as_u64()) {
                // Dedup: do not re-import / re-stream.
                continue;
            }
            filtered.blocks.push(b);
        }
        let batch = &mut self.batches[idx];
        batch.fetch = Some(filtered);
        batch.status = BatchStatus::Ready;
        batch.current_peer = None;
        self.ingest_ready_batches();
    }

    /// Record a failed fetch; retry on a different peer or abandon.
    ///
    /// **Never exits the planner** — abandon records a gap and continues.
    pub fn on_batch_failure(&mut self, batch_id: u32) {
        let Some(idx) = self.batches.iter().position(|b| b.id == batch_id) else {
            return;
        };
        let batch = &mut self.batches[idx];
        if batch.status != BatchStatus::InFlight {
            return;
        }
        batch.current_peer = None;
        if batch.peers_tried.len() as u8 >= BATCH_MAX_PEER_ATTEMPTS {
            self.abandon_batch(idx);
        } else {
            batch.status = BatchStatus::Pending;
            batch.fetch = None;
        }
    }

    fn abandon_batch(&mut self, idx: usize) {
        let batch = &mut self.batches[idx];
        batch.status = BatchStatus::Abandoned;
        batch.current_peer = None;
        let peers: Vec<PeerId> = batch.peers_tried.iter().copied().collect();
        let plan = batch.plan;
        self.recorded_gaps.push(RecordedGap {
            batch: plan,
            peers_tried: peers.clone(),
        });
        if let Some(m) = &self.metrics {
            m.inc_backfill_batch_abandoned();
        }
        warn!(
            start = plan.start_slot.as_u64(),
            count = plan.count,
            peers = peers.len(),
            "backfill batch abandoned; recording gap and continuing"
        );
    }

    /// Move Ready batch contents into the oldest-first hold.
    fn ingest_ready_batches(&mut self) {
        for batch in &mut self.batches {
            if batch.status != BatchStatus::Ready {
                continue;
            }
            let Some(fetch) = batch.fetch.take() else {
                continue;
            };
            // Index columns by root.
            let mut cols_by_root: HashMap<[u8; 32], Vec<u64>> = HashMap::new();
            for c in fetch.columns {
                cols_by_root.entry(c.root).or_default().push(c.column_index);
            }
            for b in fetch.blocks {
                let cols = cols_by_root.remove(&b.root).unwrap_or_default();
                self.hold.insert(
                    b.slot.as_u64(),
                    ImportReady {
                        slot: b.slot,
                        root: b.root,
                        parent_root: b.parent_root,
                        commitment_count: b.commitment_count,
                        columns: cols,
                    },
                );
                self.parents.insert(b.root, b.parent_root);
                self.slot_roots.insert(b.slot.as_u64(), b.root);
            }
            batch.status = BatchStatus::Done;
        }
    }

    /// Drain import-ready blocks **strictly oldest-first**.
    ///
    /// Only releases slot `next_import_slot`, then `+1`, … — never out of order,
    /// even when higher batches completed first (prevents `UNKNOWN_PARENT`).
    pub fn drain_imports(&mut self) -> Vec<ImportReady> {
        let mut out = Vec::new();
        while let Some(next) = self.next_import_slot {
            // Skip slots known empty / already cached with no hold entry.
            if let Some(item) = self.hold.remove(&next) {
                self.next_import_slot = Some(next.saturating_add(1));
                self.last_contiguous_imported = Some(next);
                if let Some(m) = &self.metrics {
                    m.set_backfill_progress_slots(next as i64);
                }
                self.seen_roots.insert(item.root);
                out.push(item);
                continue;
            }
            // If this slot was cached/skipped, advance without import.
            if self.cached_slots.contains(&next) {
                self.next_import_slot = Some(next.saturating_add(1));
                self.last_contiguous_imported = Some(next);
                if let Some(m) = &self.metrics {
                    m.set_backfill_progress_slots(next as i64);
                }
                continue;
            }
            // Hole: wait for the batch covering `next` (or abandon recorded).
            break;
        }
        out
    }

    /// Attempt to send a block down the chain stream once (dedup §9.3).
    ///
    /// Returns `true` if this is the first send for `root` (caller should
    /// increment `cc_p2p_chain_objects_sent_total`).
    pub fn try_stream_once(&mut self, root: [u8; 32]) -> bool {
        self.streamed_roots.insert(root)
    }

    /// Evaluate §9.4 completion criteria.
    #[must_use]
    pub fn completion(&self, head_slot: u64, parent_walk_clean: bool) -> CompletionStatus {
        let outstanding = self.batches.iter().any(|b| {
            matches!(
                b.status,
                BatchStatus::Pending | BatchStatus::InFlight | BatchStatus::Ready
            )
        }) || !self.hold.is_empty();
        let last = self.last_contiguous_imported.unwrap_or(0);
        let lag = head_slot.saturating_sub(last);
        CompletionStatus {
            no_batch_outstanding: !outstanding,
            head_caught_up: lag <= 1,
            parent_linkage_clean: parent_walk_clean,
        }
    }

    /// Parent-linkage walk from anchor forward over known slot_roots.
    ///
    /// Returns `Ok(())` when every consecutive pair links parent→child, or
    /// `Err(slot)` at the first gap / broken link.
    pub fn parent_linkage_walk(&self) -> Result<(), u64> {
        parent_linkage_walk(
            self.anchor_slot,
            self.anchor_root,
            &self.slot_roots,
            &self.parents,
        )
    }
}

/// Runnable parent-linkage check (clause 4 / §9.4 criterion 3).
///
/// Walks `slot_roots` from `anchor_slot` to the highest known slot. Each step
/// requires `parents[child] == prev_root`. Missing slots or broken parents fail.
pub fn parent_linkage_walk(
    anchor_slot: Option<u64>,
    anchor_root: Option<[u8; 32]>,
    slot_roots: &BTreeMap<u64, [u8; 32]>,
    parents: &HashMap<[u8; 32], [u8; 32]>,
) -> Result<(), u64> {
    let Some(start) = anchor_slot else {
        return Ok(());
    };
    let Some(mut prev_root) = anchor_root else {
        return Ok(());
    };
    let Some((&max_slot, _)) = slot_roots.iter().next_back() else {
        return Ok(());
    };
    for slot in (start + 1)..=max_slot {
        let Some(root) = slot_roots.get(&slot) else {
            return Err(slot);
        };
        match parents.get(root) {
            Some(p) if p == &prev_root => {
                prev_root = *root;
            }
            _ => return Err(slot),
        }
    }
    Ok(())
}

/// Feed a backfilled block + its columns into the **same** sampling tracker
/// used by gossip (CC-26/1 — no DA bypass).
///
/// Columns are attributed with [`ColumnSource::ByRange`]. A missing sampled
/// column leaves the task incomplete and records `da_outcome{deferred}` exactly
/// as a gossiped block would.
pub fn feed_backfill_to_sampling(
    tracker: &mut SamplingTracker,
    item: &ImportReady,
) -> BackfillDaResult {
    tracker.on_block(item.root, item.slot.as_u64(), item.commitment_count);
    if item.commitment_count == 0 {
        // Zero-blob: on_block completes immediately when no samples held.
        if tracker.get(&item.root).is_some_and(|t| t.is_complete()) {
            return BackfillDaResult::ZeroBlob;
        }
    }
    for &col in &item.columns {
        tracker.on_column(item.root, item.slot.as_u64(), col, ColumnSource::ByRange);
    }
    match tracker.get(&item.root) {
        Some(t) if t.is_complete() => BackfillDaResult::Available,
        Some(t) if t.missing().is_empty() && t.is_complete() => BackfillDaResult::Available,
        _ => BackfillDaResult::Deferred,
    }
}

/// Assert that a backfilled incomplete sample set is deferred identically to
/// gossip (test helper used by the no-DA-bypass acceptance test).
pub fn da_deferred_count(metrics: &P2pMetrics) -> u64 {
    metrics.da_outcome(DaOutcome::Deferred)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::das::sampling::{FixedDeadline, SamplingTracker};
    use crate::metrics::P2pMetrics;
    use prometheus_client::registry::Registry;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Instant;

    fn peer_from_byte(n: u8) -> PeerId {
        static MAP: OnceLock<Mutex<HashMap<u8, PeerId>>> = OnceLock::new();
        let map = MAP.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock().unwrap();
        *guard.entry(n).or_insert_with(PeerId::random)
    }

    fn root(n: u8) -> [u8; 32] {
        let mut r = [0u8; 32];
        r[0] = n;
        r
    }

    fn parent_of(n: u8) -> [u8; 32] {
        if n == 0 { [0u8; 32] } else { root(n - 1) }
    }

    fn sampled_eight() -> Vec<u64> {
        (0..8).collect()
    }

    fn custodied_four() -> Vec<u64> {
        (0..4).collect()
    }

    fn metrics() -> P2pMetrics {
        let mut reg = Registry::default();
        P2pMetrics::register(&mut reg)
    }

    fn bf_peer(n: u8, earliest: u64, digest_ok: bool) -> BackfillPeer {
        BackfillPeer {
            peer_id: peer_from_byte(n),
            app_score: f64::from(n),
            earliest_available_slot: Slot::new(earliest),
            fork_digest_matches: digest_ok,
        }
    }

    fn make_block(slot: u64, n: u8) -> FetchedBlock {
        FetchedBlock {
            slot: Slot::new(slot),
            root: root(n),
            parent_root: parent_of(n),
            commitment_count: 1,
        }
    }

    fn make_columns(slot: u64, n: u8, indices: &[u64]) -> Vec<FetchedColumn> {
        indices
            .iter()
            .map(|&column_index| FetchedColumn {
                slot: Slot::new(slot),
                root: root(n),
                column_index,
            })
            .collect()
    }

    // ── Gap triggers ────────────────────────────────────────────────────────

    #[test]
    fn trigger_head_jump_fires_when_delta_gt_1() {
        let mut d = GapDetector::new();
        d.set_last_imported(10);
        assert!(d.on_head_update(10).is_none());
        assert!(d.on_head_update(11).is_none(), "delta=1 is not a gap");
        let g = d.on_head_update(15).expect("delta=4 must fire");
        assert_eq!(g.trigger, GapTrigger::HeadJump);
        assert_eq!(g.from_slot, Slot::new(11));
        assert_eq!(g.to_slot, Slot::new(15));
    }

    #[test]
    fn trigger_clock_stall_fires_at_ge_2() {
        let mut d = GapDetector::new();
        d.set_last_imported(100);
        assert!(d.on_clock_tick(100).is_none());
        assert!(d.on_clock_tick(101).is_none(), "delta=1 no fire");
        let g = d.on_clock_tick(102).expect("delta=2 fires");
        assert_eq!(g.trigger, GapTrigger::ClockStall);
        assert_eq!(g.from_slot, Slot::new(101));
        assert_eq!(g.to_slot, Slot::new(102));
    }

    #[test]
    fn trigger_peer_status_ahead_catches_eclipse() {
        let mut d = GapDetector::new();
        d.set_last_imported(50);
        // delta ≤ 4: no fire (threshold is strictly greater than 4)
        assert!(d.on_peer_status(50, 54).is_none());
        // delta = 5 > 4: fires — catches eclipse without disconnect
        let g = d
            .on_peer_status(50, 55)
            .expect("peer 5 slots ahead must fire");
        assert_eq!(g.trigger, GapTrigger::PeerStatusAhead);
        assert_eq!(g.from_slot, Slot::new(51));
        assert_eq!(g.to_slot, Slot::new(55));
    }

    #[test]
    fn trigger_transport_reconnect_any_gap() {
        let mut d = GapDetector::new();
        d.set_last_imported(20);
        assert!(d.on_reconnect(25).is_none(), "not disconnected yet");
        d.on_disconnect();
        assert!(d.is_disconnected());
        let g = d.on_reconnect(25).expect("reconnect after disconnect");
        assert_eq!(g.trigger, GapTrigger::TransportReconnect);
        assert_eq!(g.from_slot, Slot::new(21));
        assert_eq!(g.to_slot, Slot::new(25));
        assert!(!d.is_disconnected());
    }

    #[test]
    fn fifth_trigger_fires_on_serve_window_holes() {
        let d = GapDetector::new();
        // Empty holes + window at target → no fire.
        assert!(
            d.on_serve_window(&[], /*earliest*/ 1_000, /*target*/ 1_000)
                .is_none()
        );
        // Non-empty holes → fifth trigger for exactly that range.
        let g = d
            .on_serve_window(&[(500, 564)], 1_000, 100)
            .expect("holes must fire fifth trigger");
        assert_eq!(g.trigger, GapTrigger::ServeWindowHoles);
        assert_eq!(g.trigger.as_str(), "serve_window_holes");
        assert_eq!(g.from_slot, Slot::new(500));
        assert_eq!(g.to_slot, Slot::new(563)); // half-open end exclusive → inclusive

        // Window above target with no holes.
        let g2 = d
            .on_serve_window(&[], 2_000, 1_000)
            .expect("above target fires");
        assert_eq!(g2.trigger, GapTrigger::ServeWindowHoles);
        assert_eq!(g2.from_slot, Slot::new(1_000));
        assert_eq!(g2.to_slot, Slot::new(1_999));

        // Five distinct reason labels.
        let labels: HashSet<&str> = GapTrigger::ALL.iter().map(|t| t.as_str()).collect();
        assert_eq!(labels.len(), 5);
        assert!(labels.contains("serve_window_holes"));
    }

    #[test]
    fn fifth_trigger_enters_below_anchor_mode_on_planner() {
        let mut planner = BackfillPlanner::new(sampled_eight()).with_custodied(custodied_four());
        planner.set_anchor(10_000, root(0));
        let d = GapDetector::new();
        let gap = d
            .on_serve_window(&[(9_000, 9_064)], 10_000, 100)
            .expect("hole");
        planner.on_gap(gap);
        assert_eq!(planner.last_trigger(), Some(GapTrigger::ServeWindowHoles));
        assert_eq!(planner.trigger_count(GapTrigger::ServeWindowHoles), 1);
        assert!(!planner.batches().is_empty());
        // Every planned batch in this hole is below the anchor.
        for b in planner.batches() {
            assert_eq!(
                planner.mode_for(b.plan),
                crate::backfill::below::BackfillMode::Below
            );
            let req = planner.columns_request_for_mode(b.plan);
            assert_eq!(
                req.columns,
                custodied_four(),
                "below-anchor must request custodied 4, not sampled 8"
            );
            assert_ne!(req.columns, sampled_eight());
        }
    }

    // ── Batch planning ──────────────────────────────────────────────────────

    #[test]
    fn batches_are_at_most_64_slots() {
        let plans = plan_batches(Slot::new(1), Slot::new(200));
        assert!(!plans.is_empty());
        for p in &plans {
            assert!(p.count <= BATCH_SLOT_LIMIT, "count {}", p.count);
            assert!(p.count >= 1);
        }
        let total: u64 = plans.iter().map(|p| p.count).sum();
        assert_eq!(total, 200);
        // 64+64+64+8
        assert_eq!(plans.len(), 4);
        assert_eq!(plans[3].count, 8);
    }

    #[test]
    fn batch_timeout_capped_at_60s() {
        let huge = batch_timeout(10_000);
        assert_eq!(huge, BATCH_TIMEOUT_CAP);
        let small = batch_timeout(1);
        assert!(small <= BATCH_TIMEOUT_CAP);
        assert_eq!(small, TTFB_TIMEOUT + RESP_TIMEOUT);
    }

    #[test]
    fn columns_request_uses_sampled_not_custodied() {
        let planner = BackfillPlanner::new(sampled_eight());
        assert_eq!(planner.sampled_columns().len(), 8);
        assert_ne!(planner.sampled_columns(), custodied_four().as_slice());
        let plan = BatchPlan {
            start_slot: Slot::new(10),
            count: 4,
        };
        let req = planner.columns_request_for(plan);
        assert_eq!(req.columns, sampled_eight());
        assert_eq!(req.columns.len(), 8);
        assert_ne!(req.columns, custodied_four());
    }

    #[test]
    fn below_anchor_requests_custodied_four_not_sampled_eight() {
        // cgc = 4, sampling_size = 8: planner must request exactly the 4 custodied.
        let planner = BackfillPlanner::new(sampled_eight()).with_custodied(custodied_four());
        assert_eq!(planner.sampled_columns().len(), 8);
        assert_eq!(planner.custodied_columns().len(), 4);
        planner_assert_anchor_below(&planner);
    }

    fn planner_assert_anchor_below(planner: &BackfillPlanner) {
        // Anchor high so the plan is below.
        let mut p = BackfillPlanner::new(planner.sampled_columns().to_vec())
            .with_custodied(planner.custodied_columns().to_vec());
        p.set_anchor(10_000, root(0));
        let plan = BatchPlan {
            start_slot: Slot::new(100),
            count: 64,
        };
        assert_eq!(
            p.mode_for(plan),
            crate::backfill::below::BackfillMode::Below
        );
        let req = p.columns_request_for_mode(plan);
        assert_eq!(req.columns.len(), 4, "must request exactly 4 custodied");
        assert_eq!(req.columns, custodied_four());
        assert_ne!(
            req.columns.len(),
            8,
            "requesting 8 (sampled) fails this criterion"
        );
    }

    // ── Concurrent batches + peer retry / abandon ───────────────────────────

    #[test]
    fn at_most_four_concurrent_on_distinct_peers() {
        let mut planner = BackfillPlanner::new(sampled_eight());
        planner.set_last_contiguous(0);
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(256), // 4 × 64
            trigger: GapTrigger::HeadJump,
        });
        assert_eq!(planner.batches().len(), 4);

        let peers: Vec<BackfillPeer> = (1..=6).map(|n| bf_peer(n, 0, true)).collect();
        let mut sched = RequestScheduler::new();
        let a1 = planner.schedule(&peers, &mut sched);
        assert_eq!(a1.len(), MAX_CONCURRENT_BATCHES);
        let peer_set: HashSet<PeerId> = a1.iter().map(|(_, p, _)| *p).collect();
        assert_eq!(
            peer_set.len(),
            MAX_CONCURRENT_BATCHES,
            "distinct peers preferred"
        );
        // No more while 4 in flight.
        let a2 = planner.schedule(&peers, &mut sched);
        assert!(a2.is_empty());
    }

    #[test]
    fn failed_batch_retries_different_peer_then_abandons_without_exit() {
        let m = metrics();
        let mut planner = BackfillPlanner::new(sampled_eight()).with_metrics(m.clone());
        planner.set_last_contiguous(0);
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(32),
            trigger: GapTrigger::ClockStall,
        });
        let peers: Vec<BackfillPeer> = (1..=4).map(|n| bf_peer(n, 0, true)).collect();
        let mut sched = RequestScheduler::new();

        let mut tried = HashSet::new();
        for attempt in 0..BATCH_MAX_PEER_ATTEMPTS {
            let assigns = planner.schedule(&peers, &mut sched);
            assert_eq!(assigns.len(), 1, "attempt {attempt}");
            let (id, peer, _) = assigns[0];
            assert!(
                tried.insert(peer),
                "must retry a different peer (attempt {attempt})"
            );
            planner.on_batch_failure(id);
        }
        assert_eq!(m.backfill_batch_abandoned(), 1);
        assert_eq!(planner.recorded_gaps().len(), 1);
        // Planner still alive — can accept another gap.
        planner.on_gap(GapDetected {
            from_slot: Slot::new(100),
            to_slot: Slot::new(110),
            trigger: GapTrigger::HeadJump,
        });
        assert!(
            planner
                .batches()
                .iter()
                .any(|b| b.status == BatchStatus::Pending),
            "planner keeps running after abandon"
        );
    }

    // ── Oldest-first import ─────────────────────────────────────────────────

    #[test]
    fn imports_strictly_oldest_first_over_200_slot_gap() {
        let m = metrics();
        let mut planner = BackfillPlanner::new(sampled_eight()).with_metrics(m.clone());
        // Anchor at slot 0 / root 0.
        planner.set_anchor(0, root(0));
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(200),
            trigger: GapTrigger::HeadJump,
        });
        assert_eq!(planner.batches().len(), 4);

        let peers: Vec<BackfillPeer> = (1..=4).map(|n| bf_peer(n, 0, true)).collect();
        let mut sched = RequestScheduler::new();

        // Complete batches out of order: 3, 1, 4, 2 (by batch index).
        let assigns = planner.schedule(&peers, &mut sched);
        assert_eq!(assigns.len(), 4);
        // Map batch id → plan
        let mut by_id: HashMap<u32, BatchPlan> = HashMap::new();
        for (id, _, plan) in &assigns {
            by_id.insert(*id, *plan);
        }
        let mut ids: Vec<u32> = by_id.keys().copied().collect();
        ids.sort_unstable();
        // Out-of-order completion: highest start first.
        let mut ordered_ids = ids.clone();
        ordered_ids.sort_by_key(|id| std::cmp::Reverse(by_id[id].start_slot.as_u64()));

        for id in ordered_ids {
            let plan = by_id[&id];
            let mut blocks = Vec::new();
            let mut columns = Vec::new();
            for i in 0..plan.count {
                let slot = plan.start_slot.as_u64() + i;
                // root byte = slot as u8 (slots 1..=200 fit).
                let n = slot as u8;
                blocks.push(make_block(slot, n));
                columns.extend(make_columns(slot, n, &sampled_eight()));
            }
            planner.on_batch_success(id, BatchFetch { blocks, columns });
        }

        let imported = planner.drain_imports();
        assert_eq!(imported.len(), 200, "all 200 slots released");
        // Strictly ascending slot order.
        for (i, item) in imported.iter().enumerate() {
            assert_eq!(item.slot.as_u64(), (i as u64) + 1);
        }
        // Parent chain: each child's parent is previous root → zero UNKNOWN_PARENT.
        let mut prev = root(0);
        for item in &imported {
            assert_eq!(
                item.parent_root,
                prev,
                "UNKNOWN_PARENT at slot {}",
                item.slot.as_u64()
            );
            prev = item.root;
        }
        assert!(m.backfill_progress_slots() >= 200);
        assert!(planner.parent_linkage_walk().is_ok());
    }

    // ── Dedup ───────────────────────────────────────────────────────────────

    #[test]
    fn cached_block_is_not_reimported_and_stream_once() {
        let m = metrics();
        let mut planner = BackfillPlanner::new(sampled_eight()).with_metrics(m.clone());
        planner.set_anchor(0, root(0));
        // Slot 5 already in cache / seen via gossip.
        planner.note_cached_slot(5);
        planner.note_seen_root(root(5));
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(8),
            trigger: GapTrigger::HeadJump,
        });
        let peers = vec![bf_peer(1, 0, true)];
        let mut sched = RequestScheduler::new();
        let assigns = planner.schedule(&peers, &mut sched);
        assert_eq!(assigns.len(), 1);
        let (id, _, plan) = assigns[0];
        let mut blocks = Vec::new();
        for i in 0..plan.count {
            let slot = plan.start_slot.as_u64() + i;
            blocks.push(make_block(slot, slot as u8));
        }
        // Include the seen root for slot 5 — must be filtered.
        planner.on_batch_success(
            id,
            BatchFetch {
                blocks,
                columns: Vec::new(),
            },
        );
        let imported = planner.drain_imports();
        // Slot 5 skipped (cached); others present. Contiguous drain advances
        // through cached slots without emitting them.
        assert!(
            !imported.iter().any(|i| i.slot.as_u64() == 5),
            "cached slot must not be re-imported"
        );
        // Dual-path stream-once.
        assert!(planner.try_stream_once(root(1)));
        assert!(
            !planner.try_stream_once(root(1)),
            "second path must not re-send"
        );
        // Simulate metric: only first send counts.
        if planner.try_stream_once(root(2)) {
            m.inc_chain_objects_sent();
        }
        if planner.try_stream_once(root(2)) {
            m.inc_chain_objects_sent();
        }
        assert_eq!(m.chain_objects_sent(), 1);
    }

    // ── No DA bypass ────────────────────────────────────────────────────────

    #[test]
    fn no_da_bypass_missing_sampled_column_is_deferred() {
        let m = metrics();
        let far = Instant::now() + Duration::from_secs(3600);
        let mut tracker = SamplingTracker::new(
            sampled_eight().into_iter().collect(),
            Arc::new(FixedDeadline(far)),
            Some(m.clone()),
            None,
        );

        // Backfilled block with only 7 of 8 sampled columns.
        let item = ImportReady {
            slot: Slot::new(42),
            root: root(42),
            parent_root: root(41),
            commitment_count: 4,
            columns: vec![0, 1, 2, 3, 4, 5, 6], // missing 7
        };
        let result = feed_backfill_to_sampling(&mut tracker, &item);
        assert_eq!(result, BackfillDaResult::Deferred);
        assert_eq!(m.da_outcome(DaOutcome::Deferred), 1);
        assert_eq!(m.da_outcome(DaOutcome::Imported), 0);
        // Head must not advance on deferred alone — modelled as: task incomplete.
        assert!(!tracker.get(&root(42)).unwrap().is_complete());

        // Gossip path with the same incompleteness is also deferred.
        let m2 = metrics();
        let mut t2 = SamplingTracker::new(
            sampled_eight().into_iter().collect(),
            Arc::new(FixedDeadline(far)),
            Some(m2.clone()),
            None,
        );
        t2.on_block(root(99), 99, 4);
        for col in 0..7u64 {
            t2.on_column(root(99), 99, col, ColumnSource::Gossip);
        }
        assert_eq!(m2.da_outcome(DaOutcome::Deferred), 1);
        assert!(!t2.get(&root(99)).unwrap().is_complete());
    }

    #[test]
    fn full_sampled_columns_complete_via_byrange_source() {
        let m = metrics();
        let far = Instant::now() + Duration::from_secs(3600);
        let mut tracker = SamplingTracker::new(
            sampled_eight().into_iter().collect(),
            Arc::new(FixedDeadline(far)),
            Some(m.clone()),
            None,
        );
        let item = ImportReady {
            slot: Slot::new(7),
            root: root(7),
            parent_root: root(6),
            commitment_count: 2,
            columns: sampled_eight(),
        };
        let result = feed_backfill_to_sampling(&mut tracker, &item);
        assert_eq!(result, BackfillDaResult::Available);
        assert_eq!(m.da_outcome(DaOutcome::Imported), 1);
        assert_eq!(m.columns_received(ColumnSource::ByRange), 8);
    }

    // ── Completion criteria ─────────────────────────────────────────────────

    #[test]
    fn three_completion_criteria_and_parent_walk() {
        let mut planner = BackfillPlanner::new(sampled_eight());
        planner.set_anchor(0, root(0));
        // Empty work: outstanding none; head lag 0; linkage clean.
        let walk = planner.parent_linkage_walk();
        assert!(walk.is_ok());
        let c = planner.completion(0, walk.is_ok());
        assert!(c.no_batch_outstanding);
        assert!(c.head_caught_up);
        assert!(c.parent_linkage_clean);
        assert!(c.is_complete());

        // Break linkage: slot 2 with wrong parent.
        planner.slot_roots.insert(1, root(1));
        planner.parents.insert(root(1), root(0));
        planner.slot_roots.insert(2, root(2));
        planner.parents.insert(root(2), root(99)); // wrong
        assert_eq!(planner.parent_linkage_walk(), Err(2));

        // Outstanding batch fails criterion 1.
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(10),
            trigger: GapTrigger::HeadJump,
        });
        let c2 = planner.completion(10, false);
        assert!(!c2.no_batch_outstanding);
        assert!(!c2.is_complete());
    }

    #[test]
    fn progress_gauge_moves_during_import() {
        let m = metrics();
        let mut planner = BackfillPlanner::new(sampled_eight()).with_metrics(m.clone());
        planner.set_anchor(0, root(0));
        assert_eq!(m.backfill_progress_slots(), 0);
        planner.on_gap(GapDetected {
            from_slot: Slot::new(1),
            to_slot: Slot::new(3),
            trigger: GapTrigger::HeadJump,
        });
        let peers = vec![bf_peer(1, 0, true)];
        let mut sched = RequestScheduler::new();
        let assigns = planner.schedule(&peers, &mut sched);
        let (id, _, _) = assigns[0];
        planner.on_batch_success(
            id,
            BatchFetch {
                blocks: vec![make_block(1, 1), make_block(2, 2), make_block(3, 3)],
                columns: Vec::new(),
            },
        );
        let _ = planner.drain_imports();
        assert_eq!(m.backfill_progress_slots(), 3);
    }

    #[test]
    fn peer_eligibility_requires_digest_and_earliest() {
        let peers = vec![
            bf_peer(1, 100, true), // earliest too high for batch@50
            bf_peer(2, 0, false),  // digest mismatch
            bf_peer(3, 0, true),   // ok
        ];
        let ok = eligible_peers(&peers, Slot::new(50));
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].peer_id, peer_from_byte(3));
    }

    #[test]
    fn expected_chunks_counts_blocks_and_sampled_columns() {
        // 64 blocks × (1 + 8 cols) = 576
        assert_eq!(expected_chunks(64, 8), 64 + 64 * 8);
    }

    // ── CC-47b outbound bound ───────────────────────────────────────────────

    #[test]
    fn schedule_respects_outbound_128_blocks_per_10s_per_peer() {
        // One peer advertising a deep window: planner must not schedule more
        // than 128 blocks against it inside one 10 s budget window.
        let mut planner = BackfillPlanner::new(sampled_eight());
        planner.set_anchor(10_000, root(0));
        // 4 × 64 = 256 slots of below-anchor work.
        planner.on_gap(GapDetected {
            from_slot: Slot::new(9_744),
            to_slot: Slot::new(9_999),
            trigger: GapTrigger::ServeWindowHoles,
        });
        assert!(planner.batches().len() >= 4);

        let peers = vec![bf_peer(1, 0, true)];
        let mut sched = RequestScheduler::new();
        let t0 = Instant::now();
        let a1 = planner.schedule_at(&peers, &mut sched, t0);
        // Capacity 128 → at most two 64-slot batches on one peer.
        assert!(
            a1.len() <= 2,
            "single peer must not receive >128 blocks in one window (got {})",
            a1.len()
        );
        let total: u64 = a1.iter().map(|(_, _, p)| p.count).sum();
        assert!(total <= 128, "reserved {total} blocks");
        // Further schedule at same instant: no more budget.
        let a2 = planner.schedule_at(&peers, &mut sched, t0);
        assert!(a2.is_empty(), "budget exhausted at t0");
        assert!(planner.outbound_block_budget().within_bound());
        assert_eq!(
            planner
                .outbound_block_budget()
                .total_requested(peer_from_byte(1)),
            total
        );
    }

    #[test]
    fn schedule_spreads_across_peers_under_bound() {
        let mut planner = BackfillPlanner::new(sampled_eight());
        planner.set_anchor(10_000, root(0));
        planner.on_gap(GapDetected {
            from_slot: Slot::new(9_744),
            to_slot: Slot::new(9_999),
            trigger: GapTrigger::ServeWindowHoles,
        });
        let peers: Vec<BackfillPeer> = (1..=4).map(|n| bf_peer(n, 0, true)).collect();
        let mut sched = RequestScheduler::new();
        let t0 = Instant::now();
        let assigns = planner.schedule_at(&peers, &mut sched, t0);
        // Four concurrent batches on four peers = 256 slots, each peer ≤ 64.
        assert_eq!(
            assigns.len(),
            MAX_CONCURRENT_BATCHES.min(planner.batches().len())
        );
        assert!(planner.outbound_block_budget().within_bound());
        let totals = planner.outbound_block_budget().per_peer_totals();
        // Distribution is recorded so concentration is visible.
        assert!(!totals.is_empty());
        for (_, c) in &totals {
            assert!(*c <= 128);
        }
    }

    #[test]
    fn refund_outbound_on_local_dispatch_failure() {
        let mut planner = BackfillPlanner::new(sampled_eight());
        planner.set_anchor(10_000, root(0));
        // Two 64-slot batches so a single peer can take both only after refund.
        planner.on_gap(GapDetected {
            from_slot: Slot::new(9_872),
            to_slot: Slot::new(9_999),
            trigger: GapTrigger::ServeWindowHoles,
        });
        let peers = vec![bf_peer(1, 0, true), bf_peer(2, 0, true)];
        let mut sched = RequestScheduler::new();
        let t0 = Instant::now();
        let assigns = planner.schedule_at(&peers, &mut sched, t0);
        assert!(!assigns.is_empty());
        let (_id, peer, plan) = assigns[0];
        let before = planner.outbound_block_budget_mut().available(peer, t0);
        assert_eq!(before, 128 - plan.count);
        // Host cannot dispatch (local) — refund restores that peer's window budget.
        assert!(planner.refund_outbound_reservation(peer, plan.count, t0));
        assert_eq!(planner.outbound_block_budget_mut().available(peer, t0), 128);
        // Cumulative counter is not reduced (attempts scheduled, not window).
        assert!(planner.outbound_block_budget().total_requested(peer) >= plan.count);
    }
}
