//! P2P service Prometheus metrics (CC-29a / Architecture §12).
//!
//! Registered into [`cc_bootstrap::Bootstrap::registry`] between `init` and
//! `serve` — the Phase 0 §4.1 seam. **No producers** in this issue: families
//! are declared, bucket boundaries are exact (§12.1), labelled series are
//! seeded to zero so `soak-report.sh` sees `0` rather than absent.
//!
//! The `libp2p-metrics` sub-registry uses prefix `cc_p2p_libp2p` and is wired
//! only via [`cc_libp2p::Metrics`] (CC-20/1: no direct `libp2p*` deps here).
//! Nested scrape names are `cc_p2p_libp2p_libp2p_*` because upstream
//! `Metrics::new` always adds its own `libp2p` sub-prefix (N2 wontfix).

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};

// ── bucket boundaries (§12.1, verbatim) ─────────────────────────────────────

/// `cc_p2p_peer_score` upper bounds. Boundary at **exactly −4000** (clause 6).
pub(crate) const PEER_SCORE_BUCKETS: [f64; 9] = [
    -16000.0, -8000.0, -4000.0, -1000.0, -100.0, -10.0, 0.0, 10.0, 100.0,
];

/// `cc_p2p_app_score` — same ladder as peer score (no separate §12.1 table).
pub(crate) const APP_SCORE_BUCKETS: [f64; 9] = PEER_SCORE_BUCKETS;

/// `cc_p2p_head_lag_slots` upper bounds. Boundary at **exactly 1** (clause 3).
pub(crate) const HEAD_LAG_SLOTS_BUCKETS: [f64; 9] =
    [0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 16.0, 32.0, 64.0];

/// `cc_p2p_sampling_seconds` upper bounds. Boundary at **exactly 0.2**.
pub(crate) const SAMPLING_BUCKETS: [f64; 9] = [0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 2.0, 4.0];

/// `cc_p2p_verdict_latency_seconds` upper bounds. Boundary at **exactly 0.1**.
pub(crate) const VERDICT_LATENCY_BUCKETS: [f64; 9] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0];

/// `cc_p2p_gossip_validation_seconds` — reuses the sampling ladder (sub-second DA work).
pub(crate) const GOSSIP_VALIDATION_BUCKETS: [f64; 9] = SAMPLING_BUCKETS;

/// `cc_p2p_da_verdict_slot_delta` — slot-delta ladder (same shape as head lag).
pub(crate) const DA_VERDICT_SLOT_DELTA_BUCKETS: [f64; 9] = HEAD_LAG_SLOTS_BUCKETS;

// ── label sets ──────────────────────────────────────────────────────────────
//
// SEC-1 / SEC-2: several label fields are `String` for prometheus-client's
// `EncodeLabelSet` ergonomics. **Producers MUST only pass allowlisted / fixed
// enum values** (e.g. `PeerPenaltyReason::as_str()`, `QueueName::as_str()`,
// topic/protocol constants). Unbounded or peer-supplied strings explode
// cardinality (SEC-1) and can inject label-syntax noise if not escaped by the
// encoder path (SEC-2). Prefer the fixed enums in this module; do not invent
// catch-all `"other"` buckets for closed sets.

/// Labels for `cc_p2p_peers` / `cc_p2p_idontwant_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct DirectionLabels {
    pub direction: String,
}

/// Labels for `cc_p2p_peers_below_threshold`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ThresholdLabels {
    pub threshold: String,
}

/// Labels for gossip topic-only families.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct TopicLabels {
    pub topic: String,
}

/// Labels for `cc_p2p_gossip_messages_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct GossipMessageLabels {
    pub topic: String,
    pub verdict: String,
}

/// Labels for `cc_p2p_peer_penalty_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct PenaltyReasonLabels {
    pub reason: String,
}

/// Labels for `cc_p2p_da_outcome_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct DaOutcomeLabels {
    pub result: String,
}

/// Labels for `cc_p2p_columns_received_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ColumnSourceLabels {
    pub source: String,
}

/// Labels for `cc_p2p_queue_depth` (§2.2 — one gauge, eleven series).
/// Labels for `cc_p2p_queue_depth` (§2.2 — one gauge, ten series).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct QueueLabels {
    pub q: String,
}

/// Labels for `cc_p2p_reqresp_{inbound,outbound}_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ReqrespLabels {
    pub protocol: String,
    pub result: String,
}

/// Labels for `cc_p2p_reqresp_ratelimit_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ReqrespRatelimitLabels {
    pub peer_kind: String,
    pub protocol: String,
}

/// Labels for `cc_p2p_worker_panics_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct WorkerLabels {
    pub worker: String,
}

// ── fixed label enums (no catch-all) ────────────────────────────────────────

/// `direction` label values for peers / IDONTWANT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Remote dialed us.
    Inbound,
    /// We dialed the remote.
    Outbound,
}

impl Direction {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 2] = [Self::Inbound, Self::Outbound];
}

/// `reason` label values for `cc_p2p_peer_penalty_total` (CC-29/3 / §3.7).
///
/// Six values, enumerable, **no catch-all** — an unattributed penalty is a bug.
/// Deltas live in [`crate::peer_manager::score::penalty_delta`] (CC-22c).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerPenaltyReason {
    /// Gossip REJECT attributed to this peer (−10).
    GossipInvalid,
    /// Late-detected invalid import after ACCEPT (−25).
    ImportInvalid,
    /// Req/resp timeout or malformed response (−5).
    ReqrespFault,
    /// Non-response for a column the peer custodies (−15).
    CustodyUnserved,
    /// GossipSub behavioural penalty observed / P7 (−5).
    Behavioural,
    /// Rate-limit violation against us (−5).
    RateLimit,
}

impl PeerPenaltyReason {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GossipInvalid => "gossip_invalid",
            Self::ImportInvalid => "import_invalid",
            Self::ReqrespFault => "reqresp_fault",
            Self::CustodyUnserved => "custody_unserved",
            Self::Behavioural => "behavioural",
            Self::RateLimit => "rate_limit",
        }
    }

    /// All six variants (seed + tests). Order matches §3.7 prose.
    pub const ALL: [Self; 6] = [
        Self::GossipInvalid,
        Self::ImportInvalid,
        Self::ReqrespFault,
        Self::CustodyUnserved,
        Self::Behavioural,
        Self::RateLimit,
    ];
}

/// `result` label values for `cc_p2p_da_outcome_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DaOutcome {
    /// All sampled columns arrived via gossip (or zero-blob).
    Imported,
    /// Sampling incomplete after the block was seen (awaiting recovery).
    Deferred,
    /// Completed with at least one column via by-root.
    Recovered,
    /// Deadline / recovery exhausted without a full set.
    Abandoned,
}

impl DaOutcome {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Imported => "imported",
            Self::Deferred => "deferred",
            Self::Recovered => "recovered",
            Self::Abandoned => "abandoned",
        }
    }

    /// All four variants (seed + tests).
    pub const ALL: [Self; 4] = [
        Self::Imported,
        Self::Deferred,
        Self::Recovered,
        Self::Abandoned,
    ];
}

/// `source` label values for `cc_p2p_columns_received_total`.
///
/// Four values: `gossip|byroot|byrange|engine` (CC-38b adds `engine` — additive
/// label value on the existing family; no new metric family).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnSource {
    /// Gossip pipeline.
    Gossip,
    /// By-root recovery.
    ByRoot,
    /// By-range backfill.
    ByRange,
    /// Engine inject stream (CC-38b / ninth contract).
    Engine,
}

impl ColumnSource {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gossip => "gossip",
            Self::ByRoot => "byroot",
            Self::ByRange => "byrange",
            Self::Engine => "engine",
        }
    }

    /// All four variants (seed + tests). Order: gossip, byroot, byrange, engine.
    pub const ALL: [Self; 4] = [Self::Gossip, Self::ByRoot, Self::ByRange, Self::Engine];
}

/// `q` label values for `cc_p2p_queue_depth` (§2.2 / CC-22/5 / CC-24c / CC-26a / CC-2B / CC-2D).
///
/// Seventeen values on **one** gauge family — not seventeen separate metrics.
/// `SeenColumn` / `SeenBlock` / `SeenSync` hold gossip seen-set **entry counts**
/// (not backfill cache bytes — those use `cc_p2p_cache_*` exclusively).
/// `Sampling` is sampling-task map occupancy (CC-24c; bound 64).
/// Operation index sets (`SeenVoluntaryExit` …) are CC-2B anti-replay occupancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueName {
    Gossip,
    ReqrespIn,
    Conn,
    Kzg,
    Publish,
    Cmd,
    Outstanding,
    PendingSidecar,
    PendingBlock,
    /// Gossip column seen-set occupancy (entry count).
    SeenColumn,
    /// Gossip block seen-set occupancy (entry count).
    SeenBlock,
    /// Gossip sync-committee seen-set occupancy (entry count; CC-2D, bound 4096).
    SeenSync,
    /// Sampling-task map occupancy (CC-24c; bound 64).
    Sampling,
    /// `voluntary_exit_indices` occupancy (CC-2B; bound 4 096).
    SeenVoluntaryExit,
    /// `proposer_slashing_indices` occupancy (CC-2B; bound 4 096).
    SeenProposerSlashing,
    /// `attester_slashing_indices` occupancy (CC-2B; bound 4 096).
    SeenAttesterSlashing,
    /// `bls_to_execution_change_indices` occupancy (CC-2B; bound 4 096).
    SeenBlsToExecutionChange,
}

impl QueueName {
    /// Prometheus label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gossip => "gossip",
            Self::ReqrespIn => "reqresp_in",
            Self::Conn => "conn",
            Self::Kzg => "kzg",
            Self::Publish => "publish",
            Self::Cmd => "cmd",
            Self::Outstanding => "outstanding",
            Self::PendingSidecar => "pending_sidecar",
            Self::PendingBlock => "pending_block",
            Self::SeenColumn => "seen_column",
            Self::SeenBlock => "seen_block",
            Self::SeenSync => "seen_sync",
            Self::Sampling => "sampling",
            Self::SeenVoluntaryExit => "seen_voluntary_exit",
            Self::SeenProposerSlashing => "seen_proposer_slashing",
            Self::SeenAttesterSlashing => "seen_attester_slashing",
            Self::SeenBlsToExecutionChange => "seen_bls_to_execution_change",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 17] = [
        Self::Gossip,
        Self::ReqrespIn,
        Self::Conn,
        Self::Kzg,
        Self::Publish,
        Self::Cmd,
        Self::Outstanding,
        Self::PendingSidecar,
        Self::PendingBlock,
        Self::SeenColumn,
        Self::SeenBlock,
        Self::SeenSync,
        Self::Sampling,
        Self::SeenVoluntaryExit,
        Self::SeenProposerSlashing,
        Self::SeenAttesterSlashing,
        Self::SeenBlsToExecutionChange,
    ];
}

// ── metric handles ──────────────────────────────────────────────────────────

/// All P2P §12 metric families (CC-29a).
///
/// Cheap to clone (each field is a handle into shared series storage).
/// Producers for each family land in the requirement that owns them — this
/// issue only declares and seeds.
#[derive(Debug, Clone)]
pub struct P2pMetrics {
    // Peers
    pub(crate) peers: Family<DirectionLabels, Gauge>,
    pub(crate) peers_custody_compatible: Gauge,
    pub(crate) peers_below_threshold: Family<ThresholdLabels, Gauge>,
    // Gossip
    pub(crate) gossip_messages: Family<GossipMessageLabels, Counter>,
    pub(crate) gossip_validation: Family<TopicLabels, Histogram>,
    pub(crate) idontwant: Family<DirectionLabels, Counter>,
    pub(crate) gossip_duplicate_bytes: Family<TopicLabels, Counter>,
    pub(crate) gossip_shed: Family<TopicLabels, Counter>,
    // Scoring
    pub(crate) peer_score: Histogram,
    pub(crate) app_score: Histogram,
    pub(crate) peer_penalty: Family<PenaltyReasonLabels, Counter>,
    // DA
    pub(crate) da_outcome: Family<DaOutcomeLabels, Counter>,
    pub(crate) sampling: Histogram,
    pub(crate) da_verdict_slot_delta: Histogram,
    pub(crate) columns_received: Family<ColumnSourceLabels, Counter>,
    pub(crate) inclusion_proof_verifications: Counter,
    // Chain view
    pub(crate) head_lag_slots: Histogram,
    // Backfill / memory
    pub(crate) backfill_progress_slots: Gauge,
    pub(crate) backfill_batch_abandoned: Counter,
    pub(crate) cache_occupancy_bytes: Gauge,
    pub(crate) cache_bound_bytes: Gauge,
    pub(crate) earliest_available_slot: Gauge,
    /// §5.5 fail-closed collapse after `window_stale_grace` (CC-48).
    pub(crate) window_collapsed: Counter,
    pub(crate) queue_depth: Family<QueueLabels, Gauge>,
    // Stream
    pub(crate) chain_stream_saturation_ratio: Gauge,
    pub(crate) verdict_latency: Histogram,
    pub(crate) verdict_late: Counter,
    pub(crate) verdict_timeout: Counter,
    pub(crate) chain_objects_sent: Counter,
    pub(crate) chain_verdicts_received: Counter,
    // Req/resp
    pub(crate) reqresp_inbound: Family<ReqrespLabels, Counter>,
    pub(crate) reqresp_outbound: Family<ReqrespLabels, Counter>,
    pub(crate) reqresp_ratelimit: Family<ReqrespRatelimitLabels, Counter>,
    // Health
    pub(crate) worker_panics: Family<WorkerLabels, Counter>,
    pub(crate) swarm_stall_seconds: Gauge,
}

impl P2pMetrics {
    /// Create and register every §12 family on `registry`, plus the
    /// `libp2p-metrics` sub-registry under prefix `cc_p2p_libp2p`.
    ///
    /// Call between [`cc_bootstrap::init`] and [`cc_bootstrap::serve`]. Seeds
    /// labelled series so exposition always emits HELP/TYPE (prometheus-client
    /// omits empty families).
    pub fn register(registry: &mut Registry) -> Self {
        // ── construct ───────────────────────────────────────────────────────
        let peers = Family::<DirectionLabels, Gauge>::default();
        let peers_custody_compatible = Gauge::default();
        let peers_below_threshold = Family::<ThresholdLabels, Gauge>::default();

        let gossip_messages = Family::<GossipMessageLabels, Counter>::default();
        let gossip_validation = Family::<TopicLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(GOSSIP_VALIDATION_BUCKETS)
        });
        let idontwant = Family::<DirectionLabels, Counter>::default();
        let gossip_duplicate_bytes = Family::<TopicLabels, Counter>::default();
        let gossip_shed = Family::<TopicLabels, Counter>::default();

        let peer_score = Histogram::new(PEER_SCORE_BUCKETS);
        let app_score = Histogram::new(APP_SCORE_BUCKETS);
        let peer_penalty = Family::<PenaltyReasonLabels, Counter>::default();

        let da_outcome = Family::<DaOutcomeLabels, Counter>::default();
        let sampling = Histogram::new(SAMPLING_BUCKETS);
        let da_verdict_slot_delta = Histogram::new(DA_VERDICT_SLOT_DELTA_BUCKETS);
        let columns_received = Family::<ColumnSourceLabels, Counter>::default();
        let inclusion_proof_verifications = Counter::default();

        let head_lag_slots = Histogram::new(HEAD_LAG_SLOTS_BUCKETS);

        let backfill_progress_slots = Gauge::default();
        let backfill_batch_abandoned = Counter::default();
        let cache_occupancy_bytes = Gauge::default();
        let cache_bound_bytes = Gauge::default();
        let earliest_available_slot = Gauge::default();
        let window_collapsed = Counter::default();
        let queue_depth = Family::<QueueLabels, Gauge>::default();

        let chain_stream_saturation_ratio = Gauge::default();
        let verdict_latency = Histogram::new(VERDICT_LATENCY_BUCKETS);
        let verdict_late = Counter::default();
        let verdict_timeout = Counter::default();
        let chain_objects_sent = Counter::default();
        let chain_verdicts_received = Counter::default();

        let reqresp_inbound = Family::<ReqrespLabels, Counter>::default();
        let reqresp_outbound = Family::<ReqrespLabels, Counter>::default();
        let reqresp_ratelimit = Family::<ReqrespRatelimitLabels, Counter>::default();

        let worker_panics = Family::<WorkerLabels, Counter>::default();
        let swarm_stall_seconds = Gauge::default();

        // ── register (OpenMetrics appends `_total` for counters) ────────────
        registry.register(
            "cc_p2p_peers",
            "Connected peers by direction (inbound|outbound)",
            peers.clone(),
        );
        registry.register(
            "cc_p2p_peers_custody_compatible",
            "Peers whose custody coverage is compatible with our requirements",
            peers_custody_compatible.clone(),
        );
        registry.register(
            "cc_p2p_peers_below_threshold",
            "Peers scoring below a named threshold",
            peers_below_threshold.clone(),
        );

        registry.register(
            "cc_p2p_gossip_messages",
            "Gossip messages observed (topic, verdict)",
            gossip_messages.clone(),
        );
        registry.register_with_unit(
            "cc_p2p_gossip_validation",
            "Wall time of gossip validation (topic)",
            Unit::Seconds,
            gossip_validation.clone(),
        );
        registry.register(
            "cc_p2p_idontwant",
            "IDONTWANT control messages (direction)",
            idontwant.clone(),
        );
        registry.register(
            "cc_p2p_gossip_duplicate_bytes",
            "Duplicate gossip payload bytes (topic)",
            gossip_duplicate_bytes.clone(),
        );
        registry.register(
            "cc_p2p_gossip_shed",
            "Gossip messages shed under load (topic)",
            gossip_shed.clone(),
        );

        registry.register(
            "cc_p2p_peer_score",
            "Gossipsub peer score distribution (boundary at -4000)",
            peer_score.clone(),
        );
        registry.register(
            "cc_p2p_app_score",
            "Application peer score distribution",
            app_score.clone(),
        );
        registry.register(
            "cc_p2p_peer_penalty",
            "Peer penalties by reason (no catch-all; §3.7)",
            peer_penalty.clone(),
        );

        registry.register(
            "cc_p2p_da_outcome",
            "Data-availability outcomes (result=imported|deferred|recovered|abandoned)",
            da_outcome.clone(),
        );
        registry.register_with_unit(
            "cc_p2p_sampling",
            "Wall time of column sampling (boundary at 0.2 s)",
            Unit::Seconds,
            sampling.clone(),
        );
        registry.register(
            "cc_p2p_da_verdict_slot_delta",
            "Slot delta between block and DA verdict",
            da_verdict_slot_delta.clone(),
        );
        registry.register(
            "cc_p2p_columns_received",
            "Data columns received by source (gossip|byroot|byrange|engine)",
            columns_received.clone(),
        );
        registry.register(
            "cc_p2p_inclusion_proof_verifications",
            "KZG commitments inclusion-proof verifications",
            inclusion_proof_verifications.clone(),
        );

        registry.register(
            "cc_p2p_head_lag_slots",
            "Slots the local head trails network head (boundary at 1)",
            head_lag_slots.clone(),
        );

        registry.register(
            "cc_p2p_backfill_progress_slots",
            "Backfill progress in slots",
            backfill_progress_slots.clone(),
        );
        registry.register(
            "cc_p2p_backfill_batch_abandoned",
            "Backfill batches abandoned",
            backfill_batch_abandoned.clone(),
        );
        registry.register(
            "cc_p2p_cache_occupancy_bytes",
            "P2P cache occupancy in bytes",
            cache_occupancy_bytes.clone(),
        );
        registry.register(
            "cc_p2p_cache_bound_bytes",
            "P2P cache hard bound in bytes",
            cache_bound_bytes.clone(),
        );
        registry.register(
            "cc_p2p_earliest_available_slot",
            "Earliest slot available from the network / cache",
            earliest_available_slot.clone(),
        );
        registry.register(
            "cc_p2p_window_collapsed",
            "Serve-window fail-closed collapses after window_stale_grace (CC-48 §5.5)",
            window_collapsed.clone(),
        );
        registry.register(
            "cc_p2p_queue_depth",
            "In-process queue / set depth (q=gossip|reqresp_in|conn|kzg|publish|cmd|outstanding|pending_sidecar|pending_block|seen_column|seen_block)",
            queue_depth.clone(),
        );

        registry.register(
            "cc_p2p_chain_stream_saturation_ratio",
            "Chain stream saturation ratio (0–1 scale as integer milli later; seeded 0)",
            chain_stream_saturation_ratio.clone(),
        );
        registry.register_with_unit(
            "cc_p2p_verdict_latency",
            "Wall time from object send to chain verdict (boundary at 0.1 s)",
            Unit::Seconds,
            verdict_latency.clone(),
        );
        registry.register(
            "cc_p2p_verdict_late",
            "Chain verdicts arriving after the budget",
            verdict_late.clone(),
        );
        registry.register(
            "cc_p2p_verdict_timeout",
            "Chain verdict waits that timed out",
            verdict_timeout.clone(),
        );
        registry.register(
            "cc_p2p_chain_objects_sent",
            "Objects sent on the chain stream",
            chain_objects_sent.clone(),
        );
        registry.register(
            "cc_p2p_chain_verdicts_received",
            "Verdicts received on the chain stream",
            chain_verdicts_received.clone(),
        );

        registry.register(
            "cc_p2p_reqresp_inbound",
            "Inbound req/resp exchanges (protocol, result)",
            reqresp_inbound.clone(),
        );
        registry.register(
            "cc_p2p_reqresp_outbound",
            "Outbound req/resp exchanges (protocol, result)",
            reqresp_outbound.clone(),
        );
        registry.register(
            "cc_p2p_reqresp_ratelimit",
            "Req/resp rate-limit hits (peer_kind, protocol)",
            reqresp_ratelimit.clone(),
        );

        registry.register(
            "cc_p2p_worker_panics",
            "Worker task panics (worker); cumulative across respawns (§2.4)",
            worker_panics.clone(),
        );
        registry.register(
            "cc_p2p_swarm_stall_seconds",
            "Seconds the swarm event loop has been stalled",
            swarm_stall_seconds.clone(),
        );

        // ── libp2p-metrics sub-registry (§3.4 / CC-29a) ─────────────────────
        // Prefix `cc_p2p_libp2p`; upstream Metrics::new nests a further `libp2p`
        // prefix, so scrape names are `cc_p2p_libp2p_libp2p_*` (documented
        // wontfix — AC only requires a `cc_p2p_libp2p_*` prefix match).
        // Families appear at Metrics::new — no live swarm required.
        let libp2p_sub = registry.sub_registry_with_prefix("cc_p2p_libp2p");
        let _libp2p_metrics = cc_libp2p::Metrics::new(libp2p_sub);

        let metrics = Self {
            peers,
            peers_custody_compatible,
            peers_below_threshold,
            gossip_messages,
            gossip_validation,
            idontwant,
            gossip_duplicate_bytes,
            gossip_shed,
            peer_score,
            app_score,
            peer_penalty,
            da_outcome,
            sampling,
            da_verdict_slot_delta,
            columns_received,
            inclusion_proof_verifications,
            head_lag_slots,
            backfill_progress_slots,
            backfill_batch_abandoned,
            cache_occupancy_bytes,
            cache_bound_bytes,
            earliest_available_slot,
            window_collapsed,
            queue_depth,
            chain_stream_saturation_ratio,
            verdict_latency,
            verdict_late,
            verdict_timeout,
            chain_objects_sent,
            chain_verdicts_received,
            reqresp_inbound,
            reqresp_outbound,
            reqresp_ratelimit,
            worker_panics,
            swarm_stall_seconds,
        };
        metrics.seed_exposition();
        metrics
    }

    /// Set `cc_p2p_peers{direction}` (established count by direction).
    pub fn set_peers(&self, direction: Direction, count: i64) {
        self.peers
            .get_or_create(&DirectionLabels {
                direction: direction.as_str().to_owned(),
            })
            .set(count);
    }

    /// Read `cc_p2p_peers{direction}`.
    #[must_use]
    pub fn peers(&self, direction: Direction) -> i64 {
        self.peers
            .get_or_create(&DirectionLabels {
                direction: direction.as_str().to_owned(),
            })
            .get()
    }

    /// Set `cc_p2p_peers_custody_compatible` (computation filled by CC-24a).
    pub fn set_peers_custody_compatible(&self, count: i64) {
        self.peers_custody_compatible.set(count);
    }

    /// Read `cc_p2p_peers_custody_compatible`.
    #[must_use]
    pub fn peers_custody_compatible(&self) -> i64 {
        self.peers_custody_compatible.get()
    }

    /// Set `cc_p2p_queue_depth{q}` (bounded channel occupancy).
    pub fn set_queue_depth(&self, q: QueueName, depth: i64) {
        self.queue_depth
            .get_or_create(&QueueLabels {
                q: q.as_str().to_owned(),
            })
            .set(depth);
    }

    /// Set `cc_p2p_cache_occupancy_bytes` — **backfill cache only** (CC-26a).
    ///
    /// Gossip seen-set occupancy uses [`QueueName::SeenColumn`] /
    /// [`QueueName::SeenBlock`] on `cc_p2p_queue_depth`, not these gauges.
    pub fn set_cache_occupancy_bytes(&self, n: i64) {
        self.cache_occupancy_bytes.set(n);
    }

    /// Set `cc_p2p_cache_bound_bytes` — **backfill cache only** (1 GiB ceiling).
    pub fn set_cache_bound_bytes(&self, n: i64) {
        self.cache_bound_bytes.set(n);
    }

    /// Set `cc_p2p_earliest_available_slot` (WatchServeWindow / CC-48 sole writer).
    pub fn set_earliest_available_slot(&self, slot: i64) {
        self.earliest_available_slot.set(slot);
    }

    /// Increment `cc_p2p_window_collapsed_total` (§5.5 fail-closed collapse).
    pub fn inc_window_collapsed(&self) {
        self.window_collapsed.inc();
    }

    /// Read `cc_p2p_window_collapsed_total`.
    #[must_use]
    pub fn window_collapsed(&self) -> u64 {
        self.window_collapsed.get()
    }

    /// Set `cc_p2p_backfill_progress_slots` (CC-26b — last contiguous imported).
    pub fn set_backfill_progress_slots(&self, slot: i64) {
        self.backfill_progress_slots.set(slot);
    }

    /// Read `cc_p2p_backfill_progress_slots`.
    #[must_use]
    pub fn backfill_progress_slots(&self) -> i64 {
        self.backfill_progress_slots.get()
    }

    /// Increment `cc_p2p_backfill_batch_abandoned_total` (CC-26b).
    pub fn inc_backfill_batch_abandoned(&self) {
        self.backfill_batch_abandoned.inc();
    }

    /// Read `cc_p2p_backfill_batch_abandoned_total`.
    #[must_use]
    pub fn backfill_batch_abandoned(&self) -> u64 {
        self.backfill_batch_abandoned.get()
    }

    /// Read `cc_p2p_earliest_available_slot`.
    #[must_use]
    pub fn earliest_available_slot(&self) -> i64 {
        self.earliest_available_slot.get()
    }

    /// Read `cc_p2p_cache_occupancy_bytes`.
    #[must_use]
    pub fn cache_occupancy_bytes(&self) -> i64 {
        self.cache_occupancy_bytes.get()
    }

    /// Read `cc_p2p_cache_bound_bytes`.
    #[must_use]
    pub fn cache_bound_bytes(&self) -> i64 {
        self.cache_bound_bytes.get()
    }

    /// Read `cc_p2p_queue_depth{q}`.
    #[must_use]
    pub fn queue_depth(&self, q: QueueName) -> i64 {
        self.queue_depth
            .get_or_create(&QueueLabels {
                q: q.as_str().to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_worker_panics_total{worker}` (cumulative across respawns).
    pub fn inc_worker_panics(&self, worker: &str) {
        self.worker_panics
            .get_or_create(&WorkerLabels {
                worker: worker.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_worker_panics_total{worker}`.
    #[must_use]
    pub fn worker_panics(&self, worker: &str) -> u64 {
        self.worker_panics
            .get_or_create(&WorkerLabels {
                worker: worker.to_owned(),
            })
            .get()
    }

    // ── chain-stream producers (CC-27b) ─────────────────────────────────────

    /// Increment `cc_p2p_chain_objects_sent_total`.
    pub fn inc_chain_objects_sent(&self) {
        self.chain_objects_sent.inc();
    }

    /// Read `cc_p2p_chain_objects_sent_total`.
    #[must_use]
    pub fn chain_objects_sent(&self) -> u64 {
        self.chain_objects_sent.get()
    }

    /// Increment `cc_p2p_chain_verdicts_received_total`.
    pub fn inc_chain_verdicts_received(&self) {
        self.chain_verdicts_received.inc();
    }

    /// Read `cc_p2p_chain_verdicts_received_total`.
    #[must_use]
    pub fn chain_verdicts_received(&self) -> u64 {
        self.chain_verdicts_received.get()
    }

    /// Increment `cc_p2p_verdict_timeout_total`.
    pub fn inc_verdict_timeout(&self) {
        self.verdict_timeout.inc();
    }

    /// Read `cc_p2p_verdict_timeout_total`.
    #[must_use]
    pub fn verdict_timeout(&self) -> u64 {
        self.verdict_timeout.get()
    }

    /// Increment `cc_p2p_verdict_late_total`.
    pub fn inc_verdict_late(&self) {
        self.verdict_late.inc();
    }

    /// Read `cc_p2p_verdict_late_total`.
    #[must_use]
    pub fn verdict_late(&self) -> u64 {
        self.verdict_late.get()
    }

    /// Observe `cc_p2p_verdict_latency_seconds`.
    pub fn observe_verdict_latency(&self, seconds: f64) {
        self.verdict_latency.observe(seconds);
    }

    /// Observe `cc_p2p_sampling_seconds` (boundary at exactly 0.2 — CC-24b / CC-29a).
    ///
    /// Producer: time from "8th sampled column received" to verification complete.
    pub fn observe_sampling(&self, seconds: f64) {
        self.sampling.observe(seconds);
    }

    /// Observe `cc_p2p_da_verdict_slot_delta` — **separate** series from sampling
    /// wall time (CC-24/6): slot delta between the block and the DA verdict.
    pub fn observe_da_verdict_slot_delta(&self, slots: f64) {
        self.da_verdict_slot_delta.observe(slots);
    }

    /// Set `cc_p2p_chain_stream_saturation_ratio` as milli-units (0–1000 ⇒ 0.0–1.0).
    pub fn set_saturation_ratio_milli(&self, milli: i64) {
        self.chain_stream_saturation_ratio.set(milli);
    }

    /// Convenience: set saturation from a 0.0–1.0 ratio.
    pub fn set_saturation_ratio(&self, ratio: f64) {
        let milli = (ratio.clamp(0.0, 1.0) * 1000.0).round() as i64;
        self.set_saturation_ratio_milli(milli);
    }

    /// Read saturation milli-units.
    #[must_use]
    pub fn saturation_ratio_milli(&self) -> i64 {
        self.chain_stream_saturation_ratio.get()
    }

    /// Increment `cc_p2p_gossip_shed_total{topic}`.
    pub fn inc_gossip_shed(&self, topic: &str) {
        self.gossip_shed
            .get_or_create(&TopicLabels {
                topic: topic.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_gossip_shed_total{topic}`.
    #[must_use]
    pub fn gossip_shed(&self, topic: &str) -> u64 {
        self.gossip_shed
            .get_or_create(&TopicLabels {
                topic: topic.to_owned(),
            })
            .get()
    }

    // ── scoring producers (CC-22c) ──────────────────────────────────────────

    /// Increment `cc_p2p_peer_penalty_total{reason}` (CC-29/3 label set).
    pub fn inc_peer_penalty(&self, reason: PeerPenaltyReason) {
        self.peer_penalty
            .get_or_create(&PenaltyReasonLabels {
                reason: reason.as_str().to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_peer_penalty_total{reason}`.
    #[must_use]
    pub fn peer_penalty_count(&self, reason: PeerPenaltyReason) -> u64 {
        self.peer_penalty
            .get_or_create(&PenaltyReasonLabels {
                reason: reason.as_str().to_owned(),
            })
            .get()
    }

    /// Observe a GossipSub peer score sample (`cc_p2p_peer_score`).
    pub fn observe_peer_score(&self, score: f64) {
        self.peer_score.observe(score);
    }

    /// Observe an application score sample (`cc_p2p_app_score`).
    pub fn observe_app_score(&self, score: f64) {
        self.app_score.observe(score);
    }

    // ── req/resp producers (CC-23a) ─────────────────────────────────────────

    /// Increment `cc_p2p_reqresp_inbound_total{protocol,result}`.
    pub fn inc_reqresp_inbound(&self, protocol: &str, result: &str) {
        self.reqresp_inbound
            .get_or_create(&ReqrespLabels {
                protocol: protocol.to_owned(),
                result: result.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_reqresp_inbound_total{protocol,result}`.
    #[must_use]
    pub fn reqresp_inbound_count(&self, protocol: &str, result: &str) -> u64 {
        self.reqresp_inbound
            .get_or_create(&ReqrespLabels {
                protocol: protocol.to_owned(),
                result: result.to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_reqresp_outbound_total{protocol,result}`.
    pub fn inc_reqresp_outbound(&self, protocol: &str, result: &str) {
        self.reqresp_outbound
            .get_or_create(&ReqrespLabels {
                protocol: protocol.to_owned(),
                result: result.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_reqresp_outbound_total{protocol,result}`.
    #[must_use]
    pub fn reqresp_outbound_count(&self, protocol: &str, result: &str) -> u64 {
        self.reqresp_outbound
            .get_or_create(&ReqrespLabels {
                protocol: protocol.to_owned(),
                result: result.to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_reqresp_ratelimit_total{peer_kind,protocol}`.
    pub fn inc_reqresp_ratelimit(&self, peer_kind: &str, protocol: &str) {
        self.reqresp_ratelimit
            .get_or_create(&ReqrespRatelimitLabels {
                peer_kind: peer_kind.to_owned(),
                protocol: protocol.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_reqresp_ratelimit_total{peer_kind,protocol}`.
    #[must_use]
    pub fn reqresp_ratelimit_count(&self, peer_kind: &str, protocol: &str) -> u64 {
        self.reqresp_ratelimit
            .get_or_create(&ReqrespRatelimitLabels {
                peer_kind: peer_kind.to_owned(),
                protocol: protocol.to_owned(),
            })
            .get()
    }

    // ── gossip / DA producers (CC-22d) ──────────────────────────────────────

    /// Increment `cc_p2p_gossip_messages_total{topic,verdict}`.
    pub fn inc_gossip_messages(&self, topic: &str, verdict: &str) {
        self.gossip_messages
            .get_or_create(&GossipMessageLabels {
                topic: topic.to_owned(),
                verdict: verdict.to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_gossip_messages_total{topic,verdict}`.
    #[must_use]
    pub fn gossip_messages(&self, topic: &str, verdict: &str) -> u64 {
        self.gossip_messages
            .get_or_create(&GossipMessageLabels {
                topic: topic.to_owned(),
                verdict: verdict.to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_columns_received_total{source}`.
    pub fn inc_columns_received(&self, source: ColumnSource) {
        self.columns_received
            .get_or_create(&ColumnSourceLabels {
                source: source.as_str().to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_columns_received_total{source}`.
    #[must_use]
    pub fn columns_received(&self, source: ColumnSource) -> u64 {
        self.columns_received
            .get_or_create(&ColumnSourceLabels {
                source: source.as_str().to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_da_outcome_total{result}` (CC-24c).
    pub fn inc_da_outcome(&self, result: DaOutcome) {
        self.da_outcome
            .get_or_create(&DaOutcomeLabels {
                result: result.as_str().to_owned(),
            })
            .inc();
    }

    /// Read `cc_p2p_da_outcome_total{result}`.
    #[must_use]
    pub fn da_outcome(&self, result: DaOutcome) -> u64 {
        self.da_outcome
            .get_or_create(&DaOutcomeLabels {
                result: result.as_str().to_owned(),
            })
            .get()
    }

    /// Increment `cc_p2p_inclusion_proof_verifications_total`.
    pub fn inc_inclusion_proof_verifications(&self) {
        self.inclusion_proof_verifications.inc();
    }

    /// Read `cc_p2p_inclusion_proof_verifications_total`.
    #[must_use]
    pub fn inclusion_proof_verifications(&self) -> u64 {
        self.inclusion_proof_verifications.get()
    }

    /// Set `cc_p2p_peers_below_threshold{threshold}` (R-3 early warning).
    pub fn set_peers_below_threshold(&self, threshold: &str, count: i64) {
        self.peers_below_threshold
            .get_or_create(&ThresholdLabels {
                threshold: threshold.to_owned(),
            })
            .set(count);
    }

    /// Read `cc_p2p_peers_below_threshold{threshold}`.
    #[must_use]
    pub fn peers_below_threshold(&self, threshold: &str) -> i64 {
        self.peers_below_threshold
            .get_or_create(&ThresholdLabels {
                threshold: threshold.to_owned(),
            })
            .get()
    }

    /// Set `cc_p2p_swarm_stall_seconds` (last observed stall duration).
    pub fn set_swarm_stall_seconds(&self, seconds: f64) {
        // Gauge is i64; store milliseconds so sub-second stalls are visible.
        let ms = (seconds * 1000.0).round() as i64;
        self.swarm_stall_seconds.set(ms);
    }

    /// Read stall duration in milliseconds (see [`Self::set_swarm_stall_seconds`]).
    #[must_use]
    pub fn swarm_stall_ms(&self) -> i64 {
        self.swarm_stall_seconds.get()
    }

    /// Ensure every labelled family has its fixed series so HELP/TYPE appear
    /// and soak queries return 0 rather than absent.
    fn seed_exposition(&self) {
        // Peers
        for direction in Direction::ALL {
            self.peers
                .get_or_create(&DirectionLabels {
                    direction: direction.as_str().to_owned(),
                })
                .set(0);
        }
        self.peers_custody_compatible.set(0);
        self.peers_below_threshold
            .get_or_create(&ThresholdLabels {
                threshold: "gossip".to_owned(),
            })
            .set(0);

        // Gossip — open topic/verdict labels: one placeholder series each.
        let _ = self
            .gossip_messages
            .get_or_create(&GossipMessageLabels {
                topic: "none".to_owned(),
                verdict: "none".to_owned(),
            })
            .get();
        self.gossip_validation
            .get_or_create(&TopicLabels {
                topic: "none".to_owned(),
            })
            .observe(0.0);
        for direction in Direction::ALL {
            let _ = self
                .idontwant
                .get_or_create(&DirectionLabels {
                    direction: direction.as_str().to_owned(),
                })
                .get();
        }
        let _ = self
            .gossip_duplicate_bytes
            .get_or_create(&TopicLabels {
                topic: "none".to_owned(),
            })
            .get();
        let _ = self
            .gossip_shed
            .get_or_create(&TopicLabels {
                topic: "none".to_owned(),
            })
            .get();

        // Scoring
        self.peer_score.observe(0.0);
        self.app_score.observe(0.0);
        for reason in PeerPenaltyReason::ALL {
            let _ = self
                .peer_penalty
                .get_or_create(&PenaltyReasonLabels {
                    reason: reason.as_str().to_owned(),
                })
                .get();
        }

        // DA
        for result in DaOutcome::ALL {
            let _ = self
                .da_outcome
                .get_or_create(&DaOutcomeLabels {
                    result: result.as_str().to_owned(),
                })
                .get();
        }
        self.sampling.observe(0.0);
        self.da_verdict_slot_delta.observe(0.0);
        for source in ColumnSource::ALL {
            let _ = self
                .columns_received
                .get_or_create(&ColumnSourceLabels {
                    source: source.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.inclusion_proof_verifications.get();

        // Chain view
        self.head_lag_slots.observe(0.0);

        // Backfill / memory
        self.backfill_progress_slots.set(0);
        let _ = self.backfill_batch_abandoned.get();
        self.cache_occupancy_bytes.set(0);
        self.cache_bound_bytes.set(0);
        self.earliest_available_slot.set(0);
        let _ = self.window_collapsed.get();
        for q in QueueName::ALL {
            self.queue_depth
                .get_or_create(&QueueLabels {
                    q: q.as_str().to_owned(),
                })
                .set(0);
        }

        // Stream
        self.chain_stream_saturation_ratio.set(0);
        self.verdict_latency.observe(0.0);
        let _ = self.verdict_late.get();
        let _ = self.verdict_timeout.get();
        let _ = self.chain_objects_sent.get();
        let _ = self.chain_verdicts_received.get();

        // Req/resp — open protocol/result: one placeholder each.
        let _ = self
            .reqresp_inbound
            .get_or_create(&ReqrespLabels {
                protocol: "none".to_owned(),
                result: "none".to_owned(),
            })
            .get();
        let _ = self
            .reqresp_outbound
            .get_or_create(&ReqrespLabels {
                protocol: "none".to_owned(),
                result: "none".to_owned(),
            })
            .get();
        let _ = self
            .reqresp_ratelimit
            .get_or_create(&ReqrespRatelimitLabels {
                peer_kind: "none".to_owned(),
                protocol: "none".to_owned(),
            })
            .get();

        // Health
        let _ = self
            .worker_panics
            .get_or_create(&WorkerLabels {
                worker: "none".to_owned(),
            })
            .get();
        self.swarm_stall_seconds.set(0);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use prometheus_client::encoding::text::encode;
    use std::collections::BTreeSet;

    /// Every §12 family name as it appears on OpenMetrics `# TYPE` lines after
    /// register+seed (prometheus-client: counters omit `_total` on TYPE; samples
    /// still append `_total`). Duration histograms keep the `_seconds` unit
    /// suffix. A family added later without updating this list fails
    /// [`family_name_fixture_matches_exposition`] (set equality, not substring).
    const EXPECTED_FAMILIES: &[&str] = &[
        // Peers
        "cc_p2p_peers",
        "cc_p2p_peers_custody_compatible",
        "cc_p2p_peers_below_threshold",
        // Gossip
        "cc_p2p_gossip_messages",
        "cc_p2p_gossip_validation_seconds",
        "cc_p2p_idontwant",
        "cc_p2p_gossip_duplicate_bytes",
        "cc_p2p_gossip_shed",
        // Scoring
        "cc_p2p_peer_score",
        "cc_p2p_app_score",
        "cc_p2p_peer_penalty",
        // DA
        "cc_p2p_da_outcome",
        "cc_p2p_sampling_seconds",
        "cc_p2p_da_verdict_slot_delta",
        "cc_p2p_columns_received",
        "cc_p2p_inclusion_proof_verifications",
        // Chain view
        "cc_p2p_head_lag_slots",
        // Backfill / memory
        "cc_p2p_backfill_progress_slots",
        "cc_p2p_backfill_batch_abandoned",
        "cc_p2p_cache_occupancy_bytes",
        "cc_p2p_cache_bound_bytes",
        "cc_p2p_earliest_available_slot",
        "cc_p2p_window_collapsed",
        "cc_p2p_queue_depth",
        // Stream
        "cc_p2p_chain_stream_saturation_ratio",
        "cc_p2p_verdict_latency_seconds",
        "cc_p2p_verdict_late",
        "cc_p2p_verdict_timeout",
        "cc_p2p_chain_objects_sent",
        "cc_p2p_chain_verdicts_received",
        // Req/resp
        "cc_p2p_reqresp_inbound",
        "cc_p2p_reqresp_outbound",
        "cc_p2p_reqresp_ratelimit",
        // Health
        "cc_p2p_worker_panics",
        "cc_p2p_swarm_stall_seconds",
    ];

    /// Parse OpenMetrics `# TYPE name …` / `# HELP name …` family names that
    /// are our §12 surface (`cc_p2p_*` but not the `cc_p2p_libp2p_*` sub-registry).
    ///
    /// Token-based (not substring) so `cc_p2p_peers` is distinct from
    /// `cc_p2p_peers_custody_compatible`.
    fn parse_cc_p2p_family_names(buf: &str, kind: &str) -> BTreeSet<String> {
        let prefix = match kind {
            "TYPE" => "# TYPE ",
            "HELP" => "# HELP ",
            other => panic!("unknown openmetrics meta kind: {other}"),
        };
        let mut set = BTreeSet::new();
        for line in buf.lines() {
            let Some(rest) = line.strip_prefix(prefix) else {
                continue;
            };
            let Some(name) = rest.split_whitespace().next() else {
                continue;
            };
            if name.starts_with("cc_p2p_") && !name.starts_with("cc_p2p_libp2p_") {
                set.insert(name.to_owned());
            }
        }
        set
    }

    #[test]
    fn peer_score_buckets_match_section_12_1_literal() {
        assert_eq!(
            PEER_SCORE_BUCKETS,
            [
                -16000.0, -8000.0, -4000.0, -1000.0, -100.0, -10.0, 0.0, 10.0, 100.0
            ]
        );
        assert!(PEER_SCORE_BUCKETS.contains(&-4000.0));
    }

    #[test]
    fn head_lag_slots_buckets_match_section_12_1_literal() {
        assert_eq!(
            HEAD_LAG_SLOTS_BUCKETS,
            [0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 16.0, 32.0, 64.0]
        );
        assert!(HEAD_LAG_SLOTS_BUCKETS.contains(&1.0));
    }

    #[test]
    fn sampling_buckets_match_section_12_1_literal() {
        assert_eq!(
            SAMPLING_BUCKETS,
            [0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 2.0, 4.0]
        );
        assert!(SAMPLING_BUCKETS.contains(&0.2));
    }

    #[test]
    fn verdict_latency_buckets_match_section_12_1_literal() {
        assert_eq!(
            VERDICT_LATENCY_BUCKETS,
            [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
        );
        assert!(VERDICT_LATENCY_BUCKETS.contains(&0.1));
    }

    #[test]
    fn peer_penalty_reason_has_exactly_six_variants_no_catchall() {
        assert_eq!(PeerPenaltyReason::ALL.len(), 6);
        let labels: BTreeSet<&str> = PeerPenaltyReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(
            labels,
            BTreeSet::from([
                "gossip_invalid",
                "import_invalid",
                "reqresp_fault",
                "custody_unserved",
                "behavioural",
                "rate_limit",
            ])
        );
        // No "other" / catch-all.
        assert!(
            !labels
                .iter()
                .any(|s| s.contains("other") || *s == "unknown")
        );
    }

    #[test]
    fn peer_penalty_emits_one_series_per_reason_when_incremented() {
        let mut registry = Registry::default();
        let m = P2pMetrics::register(&mut registry);
        for reason in PeerPenaltyReason::ALL {
            m.peer_penalty
                .get_or_create(&PenaltyReasonLabels {
                    reason: reason.as_str().to_owned(),
                })
                .inc();
        }
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        for reason in PeerPenaltyReason::ALL {
            let needle = format!(
                "cc_p2p_peer_penalty_total{{reason=\"{}\"}}",
                reason.as_str()
            );
            assert!(
                buf.contains(&needle),
                "missing penalty series for {}:\n{buf}",
                reason.as_str()
            );
        }
    }

    #[test]
    fn queue_depth_q_label_has_exactly_seventeen_values() {
        assert_eq!(QueueName::ALL.len(), 17);
        let labels: BTreeSet<&str> = QueueName::ALL.iter().map(|q| q.as_str()).collect();
        assert_eq!(
            labels,
            BTreeSet::from([
                "gossip",
                "reqresp_in",
                "conn",
                "kzg",
                "publish",
                "cmd",
                "outstanding",
                "pending_sidecar",
                "pending_block",
                "seen_column",
                "seen_block",
                "seen_sync",
                "sampling",
                "seen_voluntary_exit",
                "seen_proposer_slashing",
                "seen_attester_slashing",
                "seen_bls_to_execution_change",
            ])
        );
    }

    #[test]
    fn queue_depth_is_single_gauge_with_all_seeded_series() {
        let mut registry = Registry::default();
        let _m = P2pMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        // One metric family name.
        assert!(buf.contains("cc_p2p_queue_depth"));
        for q in QueueName::ALL {
            let needle = format!("cc_p2p_queue_depth{{q=\"{}\"}}", q.as_str());
            assert!(
                buf.contains(&needle),
                "missing queue_depth series for {}:\n{buf}",
                q.as_str()
            );
        }
    }

    #[test]
    fn exposition_emits_exact_bucket_boundaries() {
        let mut registry = Registry::default();
        let _m = P2pMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        for le in [
            "-16000.0", "-8000.0", "-4000.0", "-1000.0", "-100.0", "-10.0", "0.0", "10.0", "100.0",
        ] {
            let needle = format!("cc_p2p_peer_score_bucket{{le=\"{le}\"}}");
            assert!(buf.contains(&needle), "missing peer_score le={le}:\n{buf}");
        }
        for le in [
            "0.0", "1.0", "2.0", "3.0", "5.0", "8.0", "16.0", "32.0", "64.0",
        ] {
            let needle = format!("cc_p2p_head_lag_slots_bucket{{le=\"{le}\"}}");
            assert!(
                buf.contains(&needle),
                "missing head_lag_slots le={le}:\n{buf}"
            );
        }
        for le in [
            "0.01", "0.025", "0.05", "0.1", "0.2", "0.4", "0.8", "2.0", "4.0",
        ] {
            let needle = format!("cc_p2p_sampling_seconds_bucket{{le=\"{le}\"}}");
            assert!(buf.contains(&needle), "missing sampling le={le}:\n{buf}");
        }
        for le in [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1.0", "2.0",
        ] {
            let needle = format!("cc_p2p_verdict_latency_seconds_bucket{{le=\"{le}\"}}");
            assert!(
                buf.contains(&needle),
                "missing verdict_latency le={le}:\n{buf}"
            );
        }
    }

    #[test]
    fn family_name_fixture_matches_exposition() {
        let mut registry = Registry::default();
        let _m = P2pMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        let expected: BTreeSet<&str> = EXPECTED_FAMILIES.iter().copied().collect();
        assert_eq!(
            expected.len(),
            EXPECTED_FAMILIES.len(),
            "EXPECTED_FAMILIES must not contain duplicates"
        );

        let type_names = parse_cc_p2p_family_names(&buf, "TYPE");
        let help_names = parse_cc_p2p_family_names(&buf, "HELP");

        let type_as_str: BTreeSet<&str> = type_names.iter().map(String::as_str).collect();
        let help_as_str: BTreeSet<&str> = help_names.iter().map(String::as_str).collect();

        assert_eq!(
            type_as_str,
            expected,
            "TYPE family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&type_as_str).collect::<Vec<_>>(),
            type_as_str.difference(&expected).collect::<Vec<_>>(),
        );
        assert_eq!(
            help_as_str,
            expected,
            "HELP family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&help_as_str).collect::<Vec<_>>(),
            help_as_str.difference(&expected).collect::<Vec<_>>(),
        );

        // Explicit whole-token check: bare `cc_p2p_peers` has its own TYPE line
        // (substring presence of sibling names must not satisfy this).
        assert!(
            buf.lines().any(|l| l == "# TYPE cc_p2p_peers gauge"),
            "missing exact TYPE line for cc_p2p_peers:\n{buf}"
        );
    }

    #[test]
    fn seeded_zeros_for_soak_critical_series() {
        let mut registry = Registry::default();
        let _m = P2pMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        // da_outcome all four results at 0
        for result in DaOutcome::ALL {
            let needle = format!("cc_p2p_da_outcome_total{{result=\"{}\"}}", result.as_str());
            assert!(
                buf.contains(&needle),
                "missing seeded da_outcome {result:?}:\n{buf}"
            );
            // Value line ends with 0 (seeded, not incremented).
            assert!(
                buf.lines()
                    .any(|l| l.starts_with(&needle) && l.ends_with(" 0")),
                "da_outcome {result:?} not zero:\n{buf}"
            );
        }

        // peer_penalty all six reasons at 0
        for reason in PeerPenaltyReason::ALL {
            let needle = format!(
                "cc_p2p_peer_penalty_total{{reason=\"{}\"}}",
                reason.as_str()
            );
            assert!(
                buf.lines()
                    .any(|l| l.starts_with(&needle) && l.ends_with(" 0")),
                "peer_penalty {reason:?} not zero:\n{buf}"
            );
        }

        // peers_custody_compatible queryable at 0
        assert!(
            buf.lines()
                .any(|l| l.starts_with("cc_p2p_peers_custody_compatible") && l.ends_with(" 0")),
            "peers_custody_compatible not zero:\n{buf}"
        );
    }

    #[test]
    fn libp2p_sub_registry_prefix_cc_p2p_libp2p_appears() {
        let mut registry = Registry::default();
        let _m = P2pMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("cc_p2p_libp2p_"),
            "expected at least one cc_p2p_libp2p_* family after Metrics::new:\n{buf}"
        );
    }
}
