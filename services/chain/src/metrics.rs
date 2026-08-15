//! Chain service Prometheus metrics (CC-1C / Architecture §11; CC-3Aa / §9).
//!
//! Registered into [`cc_bootstrap::Bootstrap::registry`] between `init` and
//! `serve` — the Phase 0 §4.1 seam. Bucket boundaries for the two budgeted
//! histograms include exact `0.4` and `1.0` so soak p95 is a counting question
//! (ADR-P1-15 / §11.2), not a quantile interpolation.
//!
//! Phase 3 (CC-3Aa) appends engine-seam / optimism / deferral families and
//! **`cc_chain_process_block_local_seconds`**, which is declared **and observed**
//! here (the exclusive half of §6.4's Trigger A / Trigger B discrimination).
//! Both `cc_chain_engine_call_seconds` and `cc_chain_process_block_local_seconds`
//! reuse [`PROCESS_BLOCK_BUCKETS`] by reference so the three histograms cannot
//! drift apart.

use std::time::Instant;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};

use crate::events::Occupancy;

// ── budgets (production bar; CI uses 2× ceilings — see tests/timing.rs) ─────

/// Production block-path budget (seconds). Soak p95 must stay at or under this.
pub const BLOCK_BUDGET_SECS: f64 = 0.4;

/// Production epoch-path budget (seconds). Soak p95 must stay at or under this.
pub const EPOCH_BUDGET_SECS: f64 = 1.0;

/// CI 2× loose ceiling for one block observation (seconds). **Not** the budget.
pub const CI_BLOCK_CEILING_SECS: f64 = 0.8;

/// CI 2× loose ceiling for one epoch observation (seconds). **Not** the budget.
pub const CI_EPOCH_CEILING_SECS: f64 = 2.0;

// ── bucket boundaries (§11.2, verbatim) ─────────────────────────────────────

/// `cc_chain_process_block_seconds` upper bounds. Boundary at **exactly 0.4**.
pub const PROCESS_BLOCK_BUCKETS: [f64; 13] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
];

/// `cc_chain_process_epoch_seconds` upper bounds. Boundary at **exactly 1.0**.
pub const PROCESS_EPOCH_BUCKETS: [f64; 12] = [
    0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0,
];

/// Shared bucket set for non-budgeted duration histograms (slots / htr / import).
///
/// Reuses the block ladder so sub-second work is resolved; not a budget surface.
pub const AUX_DURATION_BUCKETS: [f64; 13] = PROCESS_BLOCK_BUCKETS;

// ── label sets ──────────────────────────────────────────────────────────────

/// Labels for `cc_chain_state_hash_tree_root_seconds`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HashPathLabels {
    pub path: String,
}

/// Labels for `cc_chain_import_seconds`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ImportStageLabels {
    pub stage: String,
}

/// Labels for `cc_chain_import_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ImportResultLabels {
    pub result: String,
}

/// Labels for `cc_chain_optimistic_transitions_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OptimisticDirectionLabels {
    pub direction: String,
}

/// Labels for `cc_chain_event_buffer_occupancy` (ring + deepest subscriber).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BufferLabels {
    pub buffer: String,
}

/// Labels for `cc_chain_budget_exceeded_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BudgetOpLabels {
    pub op: String,
}

/// Labels for `cc_chain_bootstrap_attempts_total` (CC-19a / §8.1).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BootstrapLabels {
    pub provider: String,
    pub result: String,
}

/// `result` label values for `cc_chain_bootstrap_attempts_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BootstrapResult {
    /// Provider returned a verified checkpoint.
    Success,
    /// Provider failed (network, decode, verification, unsupported fork, …).
    Failure,
}

impl BootstrapResult {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// `path` label values for `cc_chain_state_hash_tree_root_seconds` (R-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashPath {
    /// Incremental / cached `BeaconState::canonical_root` path.
    Cached,
    /// Full uncached `TreeHash::tree_hash_root` path.
    Cold,
}

impl HashPath {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cached => "cached",
            Self::Cold => "cold",
        }
    }
}

/// `stage` label values for `cc_chain_import_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImportStage {
    Decode,
    Transition,
    ForkChoice,
    Publish,
}

impl ImportStage {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Transition => "transition",
            Self::ForkChoice => "fork_choice",
            Self::Publish => "publish",
        }
    }
}

/// `result` label values for `cc_chain_import_total` (Architecture §11.1 / §9.1).
///
/// `DeferredEngine` is the Phase 3 third deferral outcome (`result="deferred_engine"`);
/// observations land in CC-36a — this issue only declares the closed label value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImportResult {
    Imported,
    Duplicate,
    Deferred,
    /// Engine unavailable / errored — `result="deferred_engine"` (CC-36 / §4.9).
    DeferredEngine,
    UnknownParent,
    Invalid,
}

impl ImportResult {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Imported => "imported",
            Self::Duplicate => "duplicate",
            Self::Deferred => "deferred",
            Self::DeferredEngine => "deferred_engine",
            Self::UnknownParent => "unknown_parent",
            Self::Invalid => "invalid",
        }
    }
}

/// `direction` label values for `cc_chain_optimistic_transitions_total` (§9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptimisticDirection {
    Validated,
    Invalidated,
}

impl OptimisticDirection {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Invalidated => "invalidated",
        }
    }

    /// All variants (seed + tests).
    pub const ALL: [Self; 2] = [Self::Validated, Self::Invalidated];
}

/// `op` label values for `cc_chain_budget_exceeded_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BudgetOp {
    Block,
    Epoch,
}

impl BudgetOp {
    /// Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Epoch => "epoch",
        }
    }
}

/// `buffer` label values for `cc_chain_event_buffer_occupancy`.
pub const BUFFER_RING: &str = "ring";
/// Deepest per-subscriber live-queue occupancy.
pub const BUFFER_SUBSCRIBER: &str = "subscriber";

// ── metric handles ──────────────────────────────────────────────────────────

/// All chain Phase-1 + Phase-3 metric families (CC-1C / §11.1; CC-3Aa / §9.1).
///
/// Cheap to clone (each field is a handle into shared series storage).
///
/// CC-18b adds root-mismatch / backpressure counters and the body-ring gauge
/// (Architecture §7.2 / §7.5); registration stays in this module.
///
/// CC-3Aa appends engine-call / process-block-local / optimism / pending_engine
/// families. Most are declare-only; **`process_block_local` is observed** on the
/// block-processing path so the pre-engine baseline is captured.
#[derive(Debug, Clone)]
pub struct ChainMetrics {
    pub process_block: Histogram,
    pub process_epoch: Histogram,
    pub process_slots: Histogram,
    pub state_hash_tree_root: Family<HashPathLabels, Histogram>,
    pub import_seconds: Family<ImportStageLabels, Histogram>,
    pub head_slot: Gauge,
    pub head_lag_slots: Gauge,
    pub finalized_epoch: Gauge,
    pub import_total: Family<ImportResultLabels, Counter>,
    pub import_queue_depth: Gauge,
    pub event_buffer_occupancy: Family<BufferLabels, Gauge>,
    /// Accounted ring occupancy in bytes (CC-44a; separate from occupancy labels).
    pub event_buffer_bytes: Gauge,
    /// Hard byte ceiling for the event ring (CC-44a).
    pub event_buffer_bytes_bound: Gauge,
    pub resident_states: Gauge,
    pub subscribers: Gauge,
    pub budget_exceeded: Family<BudgetOpLabels, Counter>,
    /// Supplied `ImportBlockRequest.root` ≠ decoded `hash_tree_root` (CC-18b).
    pub import_root_mismatch: Counter,
    /// Command channel full after `send_timeout(2s)` (CC-18b).
    pub import_rejected_backpressure: Counter,
    /// Bodies retained for shallow-reorg replay (cap 64; CC-18b).
    pub body_ring_len: Gauge,
    /// Events dropped because the events channel was full/closed (SEC-2 / F1).
    pub event_publish_dropped: Counter,
    /// Events rejected because payload exceeded [`crate::events::MAX_EVENT_PAYLOAD_BYTES`] (SEC-44a-2).
    pub event_payload_rejected: Counter,
    /// Checkpoint bootstrap attempts per provider (CC-19a / §8.1).
    pub bootstrap_attempts: Family<BootstrapLabels, Counter>,
    /// Blocks dropped from `pending_da` (timeout or capacity eviction; CC-24d).
    pub da_pending_dropped: Counter,
    /// Current `pending_da` occupancy (CC-24d).
    pub da_pending_occupancy: Gauge,
    /// Current PeerDAS available-root set occupancy (CC-24d).
    pub da_available_occupancy: Gauge,
    // ── CC-3Aa / §9.1 chain-side additions ─────────────────────────────────
    /// Inclusive engine-call wall time (declare-only until CC-32b).
    pub engine_call: Histogram,
    /// Exclusive local block-processing time (declared **and** observed here).
    pub process_block_local: Histogram,
    /// Count of proto-array nodes with Optimistic execution status (ADR P3-10 / ≠13/8).
    pub optimistic_nodes: Gauge,
    /// Head is optimistic (0/1).
    pub is_optimistic: Gauge,
    /// Optimistic ↔ validated/invalidated transitions.
    pub optimistic_transitions: Family<OptimisticDirectionLabels, Counter>,
    /// Valid → Invalid hard-error path (EL consensus failure; expected zero forever).
    pub valid_became_invalid: Counter,
    /// Justified checkpoint invalidated (exit path).
    pub justified_invalidated: Counter,
    /// Nodes marked invalid by a backwards walk.
    pub invalidated_nodes: Counter,
    /// Current `pending_engine` occupancy.
    pub pending_engine_occupancy: Gauge,
    /// Blocks dropped from `pending_engine` (timeout or capacity).
    pub pending_engine_dropped: Counter,
}

impl ChainMetrics {
    /// Create and register every §11.1 family on `registry`.
    ///
    /// Call between [`cc_bootstrap::init`] and [`cc_bootstrap::serve`]. Seeds
    /// one series per labelled family so exposition always emits HELP/TYPE
    /// (prometheus-client omits empty families).
    pub fn register(registry: &mut Registry) -> Self {
        let process_block = Histogram::new(PROCESS_BLOCK_BUCKETS);
        let process_epoch = Histogram::new(PROCESS_EPOCH_BUCKETS);
        let process_slots = Histogram::new(AUX_DURATION_BUCKETS);
        let state_hash_tree_root =
            Family::<HashPathLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(AUX_DURATION_BUCKETS)
            });
        let import_seconds = Family::<ImportStageLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(AUX_DURATION_BUCKETS)
        });
        let head_slot = Gauge::default();
        let head_lag_slots = Gauge::default();
        let finalized_epoch = Gauge::default();
        let import_total = Family::<ImportResultLabels, Counter>::default();
        let import_queue_depth = Gauge::default();
        let event_buffer_occupancy = Family::<BufferLabels, Gauge>::default();
        let event_buffer_bytes = Gauge::default();
        let event_buffer_bytes_bound = Gauge::default();
        let resident_states = Gauge::default();
        let subscribers = Gauge::default();
        let budget_exceeded = Family::<BudgetOpLabels, Counter>::default();
        let import_root_mismatch = Counter::default();
        let import_rejected_backpressure = Counter::default();
        let body_ring_len = Gauge::default();
        let event_publish_dropped = Counter::default();
        let event_payload_rejected = Counter::default();
        let bootstrap_attempts = Family::<BootstrapLabels, Counter>::default();
        let da_pending_dropped = Counter::default();
        let da_pending_occupancy = Gauge::default();
        let da_available_occupancy = Gauge::default();

        // CC-3Aa: reuse PROCESS_BLOCK_BUCKETS by reference (never copy the ladder).
        let engine_call = Histogram::new(PROCESS_BLOCK_BUCKETS);
        let process_block_local = Histogram::new(PROCESS_BLOCK_BUCKETS);
        let optimistic_nodes = Gauge::default();
        let is_optimistic = Gauge::default();
        let optimistic_transitions = Family::<OptimisticDirectionLabels, Counter>::default();
        let valid_became_invalid = Counter::default();
        let justified_invalidated = Counter::default();
        let invalidated_nodes = Counter::default();
        let pending_engine_occupancy = Gauge::default();
        let pending_engine_dropped = Counter::default();

        registry.register_with_unit(
            "cc_chain_process_block",
            "Wall time of process_block (budgeted; boundary at 0.4 s)",
            Unit::Seconds,
            process_block.clone(),
        );
        registry.register_with_unit(
            "cc_chain_process_epoch",
            "Wall time of process_epoch (budgeted; boundary at 1.0 s)",
            Unit::Seconds,
            process_epoch.clone(),
        );
        registry.register_with_unit(
            "cc_chain_process_slots",
            "Wall time of process_slots",
            Unit::Seconds,
            process_slots.clone(),
        );
        registry.register_with_unit(
            "cc_chain_state_hash_tree_root",
            "Wall time of state hash_tree_root (path=cached|cold; R-5 attribution)",
            Unit::Seconds,
            state_hash_tree_root.clone(),
        );
        registry.register_with_unit(
            "cc_chain_import",
            "Wall time of import pipeline stages",
            Unit::Seconds,
            import_seconds.clone(),
        );
        registry.register("cc_chain_head_slot", "Current head slot", head_slot.clone());
        registry.register(
            "cc_chain_head_lag_slots",
            "Slots the local head trails wall-clock / network head",
            head_lag_slots.clone(),
        );
        registry.register(
            "cc_chain_finalized_epoch",
            "Current finalized epoch",
            finalized_epoch.clone(),
        );
        // OpenMetrics appends `_total` for counters — do not include it in the name.
        registry.register(
            "cc_chain_import",
            "Block import outcomes (result=imported|duplicate|deferred|deferred_engine|unknown_parent|invalid)",
            import_total.clone(),
        );
        registry.register(
            "cc_chain_import_queue_depth",
            "In-flight / queued import requests",
            import_queue_depth.clone(),
        );
        registry.register(
            "cc_chain_event_buffer_occupancy",
            "Event bus occupancy (buffer=ring|subscriber)",
            event_buffer_occupancy.clone(),
        );
        // CC-44a: bytes are separate gauges — not labels on occupancy (R-13 early warning).
        registry.register(
            "cc_chain_event_buffer_bytes",
            "Event ring accounted occupancy in bytes (CC-44a / §4.3)",
            event_buffer_bytes.clone(),
        );
        registry.register(
            "cc_chain_event_buffer_bytes_bound",
            "Event ring hard byte ceiling (chain.event_ring_bytes; CC-44a)",
            event_buffer_bytes_bound.clone(),
        );
        registry.register(
            "cc_chain_resident_states",
            "Number of BeaconState values retained in memory",
            resident_states.clone(),
        );
        registry.register(
            "cc_chain_subscribers",
            "Active SubscribeEvents subscribers",
            subscribers.clone(),
        );
        // OpenMetrics appends `_total` for counters — do not include it in the name.
        registry.register(
            "cc_chain_budget_exceeded",
            "Single observations over the production budget (op=block|epoch); never fatal",
            budget_exceeded.clone(),
        );
        registry.register(
            "cc_chain_import_root_mismatch",
            "ImportBlock requests whose supplied root ≠ decoded hash_tree_root",
            import_root_mismatch.clone(),
        );
        registry.register(
            "cc_chain_import_rejected_backpressure",
            "ImportBlock requests rejected after command-channel send_timeout",
            import_rejected_backpressure.clone(),
        );
        registry.register(
            "cc_chain_body_ring",
            "SignedBeaconBlock bodies retained for shallow-reorg replay (cap 64)",
            body_ring_len.clone(),
        );
        registry.register(
            "cc_chain_event_publish_dropped",
            "Events lost when the events channel is closed (F1 loud path)",
            event_publish_dropped.clone(),
        );
        registry.register(
            "cc_chain_event_payload_rejected",
            "Events rejected because payload exceeded MAX_EVENT_PAYLOAD_BYTES (SEC-44a-2)",
            event_payload_rejected.clone(),
        );
        // OpenMetrics appends `_total` for counters — do not include it in the name.
        registry.register(
            "cc_chain_bootstrap_attempts",
            "Checkpoint bootstrap attempts (provider URL, result=success|failure)",
            bootstrap_attempts.clone(),
        );
        // OpenMetrics appends `_total` for counters — do not include it in the name.
        registry.register(
            "cc_chain_da_pending_dropped",
            "Blocks dropped from pending_da (timeout or capacity eviction; CC-24d)",
            da_pending_dropped.clone(),
        );
        registry.register(
            "cc_chain_da_pending_occupancy",
            "Current pending_da map occupancy (bound 64; CC-24d)",
            da_pending_occupancy.clone(),
        );
        registry.register(
            "cc_chain_da_available_occupancy",
            "Current PeerDAS available-root set occupancy (CC-24d)",
            da_available_occupancy.clone(),
        );

        // ── CC-3Aa / §9.1 ───────────────────────────────────────────────────
        registry.register_with_unit(
            "cc_chain_engine_call",
            "Wall time of engine API calls from chain (PROCESS_BLOCK_BUCKETS; boundary at 0.4 s)",
            Unit::Seconds,
            engine_call.clone(),
        );
        registry.register_with_unit(
            "cc_chain_process_block_local",
            "Exclusive local process_block wall time (PROCESS_BLOCK_BUCKETS; §6.4 Trigger A/B)",
            Unit::Seconds,
            process_block_local.clone(),
        );
        registry.register(
            "cc_chain_optimistic_nodes",
            "Proto-array nodes with Optimistic execution status (node count, not a root set; ≠13/8)",
            optimistic_nodes.clone(),
        );
        registry.register(
            "cc_chain_is_optimistic",
            "Whether the chain head is optimistic (0/1)",
            is_optimistic.clone(),
        );
        registry.register(
            "cc_chain_optimistic_transitions",
            "Optimistic status transitions (direction=validated|invalidated)",
            optimistic_transitions.clone(),
        );
        registry.register(
            "cc_chain_valid_became_invalid",
            "Valid → Invalid hard errors (EL consensus failure; expected zero forever)",
            valid_became_invalid.clone(),
        );
        registry.register(
            "cc_chain_justified_invalidated",
            "Justified-checkpoint invalidation exits",
            justified_invalidated.clone(),
        );
        registry.register(
            "cc_chain_invalidated_nodes",
            "Proto-array nodes marked Invalid by a backwards walk",
            invalidated_nodes.clone(),
        );
        registry.register(
            "cc_chain_pending_engine_occupancy",
            "Current pending_engine map occupancy",
            pending_engine_occupancy.clone(),
        );
        registry.register(
            "cc_chain_pending_engine_dropped",
            "Blocks dropped from pending_engine (timeout or capacity eviction)",
            pending_engine_dropped.clone(),
        );

        let metrics = Self {
            process_block,
            process_epoch,
            process_slots,
            state_hash_tree_root,
            import_seconds,
            head_slot,
            head_lag_slots,
            finalized_epoch,
            import_total,
            import_queue_depth,
            event_buffer_occupancy,
            event_buffer_bytes,
            event_buffer_bytes_bound,
            resident_states,
            subscribers,
            budget_exceeded,
            import_root_mismatch,
            import_rejected_backpressure,
            body_ring_len,
            event_publish_dropped,
            event_payload_rejected,
            bootstrap_attempts,
            da_pending_dropped,
            da_pending_occupancy,
            da_available_occupancy,
            engine_call,
            process_block_local,
            optimistic_nodes,
            is_optimistic,
            optimistic_transitions,
            valid_became_invalid,
            justified_invalidated,
            invalidated_nodes,
            pending_engine_occupancy,
            pending_engine_dropped,
        };
        metrics.seed_exposition();
        metrics
    }

    /// Ensure every labelled family has at least one series so HELP/TYPE appear.
    fn seed_exposition(&self) {
        // Histograms: zero observation creates the series with all bucket lines.
        self.process_block.observe(0.0);
        self.process_epoch.observe(0.0);
        self.process_slots.observe(0.0);
        for path in [HashPath::Cached, HashPath::Cold] {
            self.state_hash_tree_root
                .get_or_create(&HashPathLabels {
                    path: path.as_str().to_owned(),
                })
                .observe(0.0);
        }
        for stage in [
            ImportStage::Decode,
            ImportStage::Transition,
            ImportStage::ForkChoice,
            ImportStage::Publish,
        ] {
            self.import_seconds
                .get_or_create(&ImportStageLabels {
                    stage: stage.as_str().to_owned(),
                })
                .observe(0.0);
        }
        for result in [
            ImportResult::Imported,
            ImportResult::Duplicate,
            ImportResult::Deferred,
            ImportResult::DeferredEngine,
            ImportResult::UnknownParent,
            ImportResult::Invalid,
        ] {
            let _ = self
                .import_total
                .get_or_create(&ImportResultLabels {
                    result: result.as_str().to_owned(),
                })
                .get();
        }
        for buffer in [BUFFER_RING, BUFFER_SUBSCRIBER] {
            self.event_buffer_occupancy
                .get_or_create(&BufferLabels {
                    buffer: buffer.to_owned(),
                })
                .set(0);
        }
        self.event_buffer_bytes.set(0);
        self.event_buffer_bytes_bound.set(0);
        for op in [BudgetOp::Block, BudgetOp::Epoch] {
            let _ = self
                .budget_exceeded
                .get_or_create(&BudgetOpLabels {
                    op: op.as_str().to_owned(),
                })
                .get();
        }
        self.head_slot.set(0);
        self.head_lag_slots.set(0);
        self.finalized_epoch.set(0);
        self.import_queue_depth.set(0);
        self.resident_states.set(0);
        self.subscribers.set(0);
        self.body_ring_len.set(0);
        let _ = self.import_root_mismatch.get();
        let _ = self.import_rejected_backpressure.get();
        let _ = self.event_publish_dropped.get();
        let _ = self.event_payload_rejected.get();
        // Seed one series so HELP/TYPE always appear (provider is runtime-known).
        let _ = self
            .bootstrap_attempts
            .get_or_create(&BootstrapLabels {
                provider: "none".to_owned(),
                result: BootstrapResult::Success.as_str().to_owned(),
            })
            .get();
        let _ = self
            .bootstrap_attempts
            .get_or_create(&BootstrapLabels {
                provider: "none".to_owned(),
                result: BootstrapResult::Failure.as_str().to_owned(),
            })
            .get();
        let _ = self.da_pending_dropped.get();
        self.da_pending_occupancy.set(0);
        self.da_available_occupancy.set(0);

        // CC-3Aa seeds — declare-only families start at zero.
        //
        // **Do not** seed-observe `process_block_local`: a fake `observe(0.0)`
        // would make `_count ≥ 1` at register, so curl non-zero cannot prove the
        // import-path observation this issue must land before the engine (M1 /
        // §6.4 / §16/9). HELP/TYPE still appear because the histogram is
        // registered unlabelled (prometheus-client emits empty classic histos).
        self.engine_call.observe(0.0);
        self.optimistic_nodes.set(0);
        self.is_optimistic.set(0);
        for direction in OptimisticDirection::ALL {
            let _ = self
                .optimistic_transitions
                .get_or_create(&OptimisticDirectionLabels {
                    direction: direction.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.valid_became_invalid.get();
        let _ = self.justified_invalidated.get();
        let _ = self.invalidated_nodes.get();
        self.pending_engine_occupancy.set(0);
        let _ = self.pending_engine_dropped.get();
    }

    /// Increment `cc_chain_da_pending_dropped_total` by `n`.
    pub fn inc_da_pending_dropped(&self, n: u64) {
        for _ in 0..n {
            self.da_pending_dropped.inc();
        }
    }

    /// Set `cc_chain_da_pending_occupancy`.
    pub fn set_da_pending_occupancy(&self, n: u64) {
        self.da_pending_occupancy.set(n as i64);
    }

    /// Set `cc_chain_da_available_occupancy`.
    pub fn set_da_available_occupancy(&self, n: u64) {
        self.da_available_occupancy.set(n as i64);
    }

    /// Increment `cc_chain_pending_engine_dropped_total` by `n` (CC-36a).
    pub fn inc_pending_engine_dropped(&self, n: u64) {
        for _ in 0..n {
            self.pending_engine_dropped.inc();
        }
    }

    /// Set `cc_chain_pending_engine_occupancy` (CC-36a).
    pub fn set_pending_engine_occupancy(&self, n: u64) {
        self.pending_engine_occupancy.set(n as i64);
    }

    // ── process_block / process_epoch (budgeted) ───────────────────────────

    /// Record one `process_block` duration.
    ///
    /// On over-budget: `warn` log with slot/epoch/duration/validator_count and
    /// increment `cc_chain_budget_exceeded_total{op="block"}`. **Never fatal.**
    pub fn observe_process_block(
        &self,
        duration_secs: f64,
        slot: u64,
        epoch: u64,
        validator_count: u64,
    ) {
        self.process_block.observe(duration_secs);
        if duration_secs > BLOCK_BUDGET_SECS {
            self.budget_exceeded
                .get_or_create(&BudgetOpLabels {
                    op: BudgetOp::Block.as_str().to_owned(),
                })
                .inc();
            tracing::warn!(
                slot,
                epoch,
                duration = duration_secs,
                validator_count,
                op = BudgetOp::Block.as_str(),
                "process_block exceeded budget"
            );
        }
    }

    /// Record exclusive local `process_block` duration (CC-3Aa / §6.4).
    ///
    /// Prefer [`Self::observe_process_block_with_local`] from the import path so
    /// inclusive and exclusive stay wired through one production call site.
    pub fn observe_process_block_local(&self, duration_secs: f64) {
        self.process_block_local.observe(duration_secs);
    }

    /// Production dual observation for pre-engine block processing (CC-3Aa / §6.4).
    ///
    /// **This is the call site `finish_imported` uses.** Inclusive
    /// (`process_block`) and exclusive (`process_block_local`) share the same
    /// duration until the engine is in the path; after CC-32b only local stays
    /// exclusive. Tests for placement must exercise **this** method (or import),
    /// not dual-feed the two helpers independently.
    pub fn observe_process_block_with_local(
        &self,
        duration_secs: f64,
        slot: u64,
        epoch: u64,
        validator_count: u64,
    ) {
        self.observe_process_block(duration_secs, slot, epoch, validator_count);
        self.observe_process_block_local(duration_secs);
    }

    /// Record one engine-call duration (declare-only until CC-32b observes it).
    pub fn observe_engine_call(&self, duration_secs: f64) {
        self.engine_call.observe(duration_secs);
    }

    /// Record one `process_epoch` duration and emit the structured epoch log.
    ///
    /// Always emits **one** info line with `slot`, `epoch`, `duration`, and
    /// `validator_count` (CC-1C/2). On over-budget: additional `warn` + counter
    /// for `op="epoch"`. **Never fatal.**
    pub fn observe_process_epoch(
        &self,
        duration_secs: f64,
        slot: u64,
        epoch: u64,
        validator_count: u64,
    ) {
        self.process_epoch.observe(duration_secs);
        // CC-1C/2 — one structured line per epoch transition (JSON subscriber).
        tracing::info!(
            slot,
            epoch,
            duration = duration_secs,
            validator_count,
            "epoch transition"
        );
        if duration_secs > EPOCH_BUDGET_SECS {
            self.budget_exceeded
                .get_or_create(&BudgetOpLabels {
                    op: BudgetOp::Epoch.as_str().to_owned(),
                })
                .inc();
            tracing::warn!(
                slot,
                epoch,
                duration = duration_secs,
                validator_count,
                op = BudgetOp::Epoch.as_str(),
                "process_epoch exceeded budget"
            );
        }
    }

    /// Time `f` and record as `process_block`. Returns `f`'s result.
    pub fn time_process_block<R>(
        &self,
        slot: u64,
        epoch: u64,
        validator_count: u64,
        f: impl FnOnce() -> R,
    ) -> R {
        let start = Instant::now();
        let out = f();
        self.observe_process_block(start.elapsed().as_secs_f64(), slot, epoch, validator_count);
        out
    }

    /// Time `f` and record as `process_epoch` (including the structured log).
    pub fn time_process_epoch<R>(
        &self,
        slot: u64,
        epoch: u64,
        validator_count: u64,
        f: impl FnOnce() -> R,
    ) -> R {
        let start = Instant::now();
        let out = f();
        self.observe_process_epoch(start.elapsed().as_secs_f64(), slot, epoch, validator_count);
        out
    }

    // ── unbudgeted helpers ─────────────────────────────────────────────────

    /// Record `process_slots` duration.
    pub fn observe_process_slots(&self, duration_secs: f64) {
        self.process_slots.observe(duration_secs);
    }

    /// Record state `hash_tree_root` duration for `path` (cached or cold).
    pub fn observe_state_hash_tree_root(&self, path: HashPath, duration_secs: f64) {
        self.state_hash_tree_root
            .get_or_create(&HashPathLabels {
                path: path.as_str().to_owned(),
            })
            .observe(duration_secs);
    }

    /// Time `f` as a state hash_tree_root observation.
    pub fn time_state_hash_tree_root<R>(&self, path: HashPath, f: impl FnOnce() -> R) -> R {
        let start = Instant::now();
        let out = f();
        self.observe_state_hash_tree_root(path, start.elapsed().as_secs_f64());
        out
    }

    /// Encode a single `path` label's sample count via a throwaway registry
    /// scrape (tests / CC-19b). `prometheus-client` gates `Histogram::count`
    /// behind `test-util`, so we read the OpenMetrics text instead.
    pub fn state_hash_tree_root_count(&self, path: HashPath) -> u64 {
        // Ensure the series exists so the scrape includes it.
        let _ = self.state_hash_tree_root.get_or_create(&HashPathLabels {
            path: path.as_str().to_owned(),
        });
        let mut registry = Registry::default();
        registry.register_with_unit(
            "cc_chain_state_hash_tree_root",
            "test scrape",
            Unit::Seconds,
            self.state_hash_tree_root.clone(),
        );
        let mut buf = String::new();
        if prometheus_client::encoding::text::encode(&mut buf, &registry).is_err() {
            return 0;
        }
        let needle = format!(
            "cc_chain_state_hash_tree_root_seconds_count{{path=\"{}\"}}",
            path.as_str()
        );
        for line in buf.lines() {
            if let Some(rest) = line.strip_prefix(&needle) {
                let n = rest.trim();
                if let Ok(v) = n.parse::<u64>() {
                    return v;
                }
                if let Ok(v) = n.parse::<f64>() {
                    return v as u64;
                }
            }
        }
        0
    }

    /// Record one import-stage duration.
    pub fn observe_import_stage(&self, stage: ImportStage, duration_secs: f64) {
        self.import_seconds
            .get_or_create(&ImportStageLabels {
                stage: stage.as_str().to_owned(),
            })
            .observe(duration_secs);
    }

    /// Increment import outcome counter.
    pub fn inc_import_result(&self, result: ImportResult) {
        self.import_total
            .get_or_create(&ImportResultLabels {
                result: result.as_str().to_owned(),
            })
            .inc();
    }

    /// Set head / lag / finalized gauges.
    pub fn set_head(&self, head_slot: u64, lag_slots: u64, finalized_epoch: u64) {
        self.head_slot.set(head_slot as i64);
        self.head_lag_slots.set(lag_slots as i64);
        self.finalized_epoch.set(finalized_epoch as i64);
    }

    /// Set import queue depth.
    pub fn set_import_queue_depth(&self, depth: u64) {
        self.import_queue_depth.set(depth as i64);
    }

    /// Set resident state count.
    pub fn set_resident_states(&self, n: u64) {
        self.resident_states.set(n as i64);
    }

    /// Set body-ring occupancy (CC-18b).
    pub fn set_body_ring_len(&self, n: u64) {
        self.body_ring_len.set(n as i64);
    }

    /// Read resident-states gauge (tests / AC metrics assertion).
    pub fn resident_states_value(&self) -> i64 {
        self.resident_states.get()
    }

    /// Read body-ring gauge (tests / AC metrics assertion).
    pub fn body_ring_len_value(&self) -> i64 {
        self.body_ring_len.get()
    }

    /// Increment dropped-event counter (channel closed / F1 loud path).
    pub fn inc_event_publish_dropped(&self) {
        self.event_publish_dropped.inc();
    }

    /// Read dropped-event counter (tests).
    pub fn event_publish_dropped_count(&self) -> u64 {
        self.event_publish_dropped.get()
    }

    /// Increment oversize-payload rejection counter (SEC-44a-2).
    pub fn inc_event_payload_rejected(&self) {
        self.event_payload_rejected.inc();
    }

    /// Read oversize-payload rejection counter (tests).
    pub fn event_payload_rejected_count(&self) -> u64 {
        self.event_payload_rejected.get()
    }

    /// Increment root-mismatch counter (CC-18b).
    pub fn inc_import_root_mismatch(&self) {
        self.import_root_mismatch.inc();
    }

    /// Read root-mismatch counter (tests).
    pub fn import_root_mismatch_count(&self) -> u64 {
        self.import_root_mismatch.get()
    }

    /// Increment backpressure rejection counter (CC-18b).
    pub fn inc_import_rejected_backpressure(&self) {
        self.import_rejected_backpressure.inc();
    }

    /// Read backpressure rejection counter (tests).
    pub fn import_rejected_backpressure_count(&self) -> u64 {
        self.import_rejected_backpressure.get()
    }

    /// Read import outcome counter (tests).
    pub fn import_result_count(&self, result: ImportResult) -> u64 {
        self.import_total
            .get_or_create(&ImportResultLabels {
                result: result.as_str().to_owned(),
            })
            .get()
    }

    /// Set active subscriber count.
    pub fn set_subscribers(&self, n: u64) {
        self.subscribers.set(n as i64);
    }

    /// Set event-buffer occupancy gauges from live [`Occupancy`] (CC-18c / CC-1C / CC-44a).
    ///
    /// Occupancy labels stay **counts only** (`ring` / `subscriber`). Bytes use
    /// dedicated gauges.
    pub fn sync_from_occupancy(&self, occupancy: &Occupancy) {
        self.event_buffer_occupancy
            .get_or_create(&BufferLabels {
                buffer: BUFFER_RING.to_owned(),
            })
            .set(occupancy.ring() as i64);
        self.event_buffer_occupancy
            .get_or_create(&BufferLabels {
                buffer: BUFFER_SUBSCRIBER.to_owned(),
            })
            .set(occupancy.deepest_subscriber() as i64);
        self.event_buffer_bytes.set(occupancy.bytes() as i64);
        self.event_buffer_bytes_bound
            .set(occupancy.bytes_bound() as i64);
        self.set_subscribers(occupancy.subscribers() as u64);
    }

    /// Seed the byte-ceiling gauge at events-task spawn (CC-44a).
    pub fn set_event_buffer_bytes_bound(&self, n: u64) {
        self.event_buffer_bytes_bound.set(n as i64);
    }

    /// Read `cc_chain_budget_exceeded_total{op}` (tests).
    pub fn budget_exceeded_count(&self, op: BudgetOp) -> u64 {
        self.budget_exceeded
            .get_or_create(&BudgetOpLabels {
                op: op.as_str().to_owned(),
            })
            .get()
    }

    /// Increment checkpoint bootstrap attempt counter (CC-19a).
    pub fn inc_bootstrap_attempt(&self, provider: &str, result: BootstrapResult) {
        self.bootstrap_attempts
            .get_or_create(&BootstrapLabels {
                provider: provider.to_owned(),
                result: result.as_str().to_owned(),
            })
            .inc();
    }

    /// Read bootstrap attempt counter (tests / CC-19/5).
    pub fn bootstrap_attempt_count(&self, provider: &str, result: BootstrapResult) -> u64 {
        self.bootstrap_attempts
            .get_or_create(&BootstrapLabels {
                provider: provider.to_owned(),
                result: result.as_str().to_owned(),
            })
            .get()
    }

    /// Increment `cc_chain_justified_invalidated_total` (CC-35 /8 exit path).
    pub fn inc_justified_invalidated(&self) {
        self.justified_invalidated.inc();
    }

    /// Read `cc_chain_justified_invalidated_total` (tests).
    pub fn justified_invalidated_count(&self) -> u64 {
        self.justified_invalidated.get()
    }

    /// Increment `cc_chain_invalidated_nodes_total` by `n` (CC-35 walk).
    pub fn inc_invalidated_nodes(&self, n: u64) {
        for _ in 0..n {
            self.invalidated_nodes.inc();
        }
    }

    /// Read `cc_chain_invalidated_nodes_total` (tests).
    pub fn invalidated_nodes_count(&self) -> u64 {
        self.invalidated_nodes.get()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use prometheus_client::encoding::text::encode;
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

    impl BufferWriter {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let inner = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    inner: Arc::clone(&inner),
                },
                inner,
            )
        }
    }

    impl Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut g = self
                .inner
                .lock()
                .map_err(|e| io::Error::other(e.to_string()))?;
            g.extend_from_slice(buf);
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

    fn with_json_subscriber<R>(writer: BufferWriter, f: impl FnOnce() -> R) -> R {
        let subscriber = TracingRegistry::default()
            .with(EnvFilter::new("info"))
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            );
        tracing::subscriber::with_default(subscriber, f)
    }

    #[test]
    fn block_buckets_include_exact_0_4_and_match_spec() {
        assert_eq!(
            PROCESS_BLOCK_BUCKETS,
            [
                0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0
            ]
        );
        assert!(PROCESS_BLOCK_BUCKETS.contains(&0.4));
    }

    #[test]
    fn epoch_buckets_include_exact_1_0_and_match_spec() {
        assert_eq!(
            PROCESS_EPOCH_BUCKETS,
            [
                0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0
            ]
        );
        assert!(PROCESS_EPOCH_BUCKETS.contains(&1.0));
    }

    #[test]
    fn exposition_emits_exact_bucket_boundaries() {
        let mut registry = Registry::default();
        let _m = ChainMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        // OpenMetrics encodes 1.0 as "1.0" (not Display's "1"). Match literals.
        for le in [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "0.75", "1.0",
            "2.0", "5.0",
        ] {
            let needle = format!("cc_chain_process_block_seconds_bucket{{le=\"{le}\"}}");
            assert!(
                buf.contains(&needle),
                "missing block bucket le={le}:\n{buf}"
            );
        }
        for le in [
            "0.05", "0.1", "0.25", "0.5", "0.75", "1.0", "1.25", "1.5", "2.0", "3.0", "5.0", "10.0",
        ] {
            let needle = format!("cc_chain_process_epoch_seconds_bucket{{le=\"{le}\"}}");
            assert!(
                buf.contains(&needle),
                "missing epoch bucket le={le}:\n{buf}"
            );
        }
        assert!(buf.contains("le=\"0.4\""));
        assert!(buf.contains("cc_chain_process_epoch_seconds_bucket{le=\"1.0\"}"));
    }

    #[test]
    fn budget_exceeded_block_warns_and_increments_without_panic() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);
        let before = m.budget_exceeded_count(BudgetOp::Block);

        let (writer, buf) = BufferWriter::new();
        with_json_subscriber(writer, || {
            m.observe_process_block(0.5, 10, 0, 100); // > 0.4 budget
        });

        assert_eq!(m.budget_exceeded_count(BudgetOp::Block), before + 1);
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("process_block exceeded budget") || text.contains("exceeded budget"),
            "expected warn log, got:\n{text}"
        );
        // Under-budget must not increment.
        m.observe_process_block(0.1, 11, 0, 100);
        assert_eq!(m.budget_exceeded_count(BudgetOp::Block), before + 1);
    }

    #[test]
    fn budget_exceeded_epoch_warns_and_increments_without_panic() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);
        let before = m.budget_exceeded_count(BudgetOp::Epoch);

        let (writer, buf) = BufferWriter::new();
        with_json_subscriber(writer, || {
            m.observe_process_epoch(1.5, 32, 1, 200); // > 1.0 budget
        });

        assert_eq!(m.budget_exceeded_count(BudgetOp::Epoch), before + 1);
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("exceeded budget"),
            "expected warn log, got:\n{text}"
        );
        m.observe_process_epoch(0.5, 64, 2, 200);
        assert_eq!(m.budget_exceeded_count(BudgetOp::Epoch), before + 1);
    }

    #[test]
    fn epoch_transition_emits_one_structured_line() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);
        let (writer, buf) = BufferWriter::new();
        with_json_subscriber(writer, || {
            m.observe_process_epoch(0.2, 3649472, 114046, 1_000_000);
        });

        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        // Exactly one info "epoch transition" line (budget warn would be extra; under budget).
        let epoch_lines: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|l| l.contains("epoch transition"))
            .collect();
        assert_eq!(
            epoch_lines.len(),
            1,
            "expected exactly one epoch transition line, got:\n{text}"
        );
        let v: serde_json::Value = serde_json::from_str(epoch_lines[0]).unwrap();
        // Fields may be nested under "fields" depending on formatter; check both.
        let get = |key: &str| {
            v.get(key)
                .or_else(|| v.pointer(&format!("/fields/{key}")))
                .cloned()
        };
        assert!(get("slot").is_some(), "missing slot: {v}");
        assert!(get("epoch").is_some(), "missing epoch: {v}");
        assert!(get("duration").is_some(), "missing duration: {v}");
        assert!(
            get("validator_count").is_some(),
            "missing validator_count: {v}"
        );
    }

    #[test]
    fn hash_tree_root_records_cached_and_cold() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);
        m.observe_state_hash_tree_root(HashPath::Cached, 0.001);
        m.observe_state_hash_tree_root(HashPath::Cold, 0.5);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("path=\"cached\""),
            "missing cached path:\n{buf}"
        );
        assert!(buf.contains("path=\"cold\""), "missing cold path:\n{buf}");
        assert!(buf.contains("cc_chain_state_hash_tree_root_seconds"));
    }

    /// M1: after register alone, local `_count` is **0** (no seed observation).
    /// After one production dual-site observation, `_count` is non-zero.
    #[test]
    fn process_block_local_zero_until_observed() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        let before = histogram_snapshot(&buf, "cc_chain_process_block_local_seconds");
        assert_eq!(
            before.count, 0,
            "seed must not observe process_block_local; count after register alone:\n{buf}"
        );
        // HELP/TYPE + empty bucket lines still present without a fake observation.
        assert!(
            buf.contains("# TYPE cc_chain_process_block_local_seconds histogram"),
            "declared family must still emit TYPE without seed observe:\n{buf}"
        );

        // Production call site used by finish_imported (not dual-feed helpers).
        m.observe_process_block_with_local(0.05, 1, 0, 10);

        buf.clear();
        encode(&mut buf, &registry).unwrap();
        let after = histogram_snapshot(&buf, "cc_chain_process_block_local_seconds");
        assert_eq!(
            after.count, 1,
            "one dual-site observation must yield local count=1:\n{buf}"
        );
    }

    /// Pre-engine exclusive ≡ inclusive when driven only through the production
    /// dual observation site (`observe_process_block_with_local` / `finish_imported`).
    ///
    /// Inclusive may have a Phase-1 seed sample; equality is asserted on **deltas**
    /// so a seed observation cannot mask a missing local wire-up (M1/M2).
    #[test]
    fn local_equals_inclusive_before_engine() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        let before_inc = histogram_snapshot(&buf, "cc_chain_process_block_seconds");
        let before_loc = histogram_snapshot(&buf, "cc_chain_process_block_local_seconds");
        assert_eq!(
            before_loc.count, 0,
            "local must start at 0 (no seed observe)"
        );

        // Sole production dual-site — the same method finish_imported calls.
        // Dual-feeding observe_process_block + observe_process_block_local
        // independently would not catch a missing call in finish_imported.
        let fixture = [0.001_f64, 0.05, 0.2, 0.45, 1.1];
        for d in fixture {
            m.observe_process_block_with_local(d, 1, 0, 10);
        }

        buf.clear();
        encode(&mut buf, &registry).unwrap();
        let after_inc = histogram_snapshot(&buf, "cc_chain_process_block_seconds");
        let after_loc = histogram_snapshot(&buf, "cc_chain_process_block_local_seconds");

        let delta_inc = after_inc.count - before_inc.count;
        let delta_loc = after_loc.count - before_loc.count;
        assert_eq!(
            delta_inc, delta_loc,
            "pre-engine dual site must advance both by the same count: \
             inc {before_inc:?}→{after_inc:?} loc {before_loc:?}→{after_loc:?}"
        );
        assert_eq!(
            delta_loc,
            fixture.len() as u64,
            "fixture must drive {n} local observations",
            n = fixture.len()
        );

        // Bucket deltas must match element-for-element (same samples, same ladder).
        assert_eq!(
            after_inc.buckets.len(),
            after_loc.buckets.len(),
            "bucket label sets must align"
        );
        for ((le_i, v_i), (le_l, v_l)) in after_inc.buckets.iter().zip(after_loc.buckets.iter()) {
            assert_eq!(le_i, le_l, "bucket le mismatch");
            let before_i = before_inc
                .buckets
                .iter()
                .find(|(le, _)| le == le_i)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            let before_l = before_loc
                .buckets
                .iter()
                .find(|(le, _)| le == le_l)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            assert_eq!(
                v_i - before_i,
                v_l - before_l,
                "bucket delta mismatch at le={le_i}"
            );
        }
    }

    #[test]
    fn process_block_local_emits_bucket_lines() {
        let mut registry = Registry::default();
        let m = ChainMetrics::register(&mut registry);
        // Real observation so bucket samples are non-zero (declaration alone
        // still emits zero-count lines via register).
        m.observe_process_block_with_local(0.01, 1, 0, 1);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        // OpenMetrics encodes whole floats as "1.0" / "2.0" (not Display's "1"/"2").
        for le in [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "0.75", "1.0",
            "2.0", "5.0",
        ] {
            let needle = format!("cc_chain_process_block_local_seconds_bucket{{le=\"{le}\"}}");
            assert!(
                buf.contains(&needle),
                "missing local bucket le={le}:\n{buf}"
            );
        }
        assert!(
            buf.contains("cc_chain_process_block_local_seconds_sum"),
            "missing _sum:\n{buf}"
        );
        assert!(
            buf.contains("cc_chain_process_block_local_seconds_count"),
            "missing _count:\n{buf}"
        );
        // 13 classic upper bounds + +Inf.
        let bucket_lines = buf
            .lines()
            .filter(|l| l.starts_with("cc_chain_process_block_local_seconds_bucket{"))
            .count();
        assert_eq!(
            bucket_lines, 14,
            "expected 13 bounds + +Inf = 14 bucket lines, got {bucket_lines}:\n{buf}"
        );
    }

    #[test]
    fn process_block_buckets_reused_not_copied() {
        // Source-level: three production Histogram::new(PROCESS_BLOCK_BUCKETS)
        // (process_block, engine_call, process_block_local). Count only the
        // `register` body so this test's own string literals are excluded.
        let src = include_str!("metrics.rs");
        let register_body = src
            .split("pub fn register(registry: &mut Registry)")
            .nth(1)
            .and_then(|s| s.split("metrics.seed_exposition()").next())
            .expect("register body");
        let constructions = register_body
            .matches("Histogram::new(PROCESS_BLOCK_BUCKETS)")
            .count();
        assert_eq!(
            constructions, 3,
            "expected three Histogram::new(PROCESS_BLOCK_BUCKETS) uses in register, got {constructions}"
        );
        assert_eq!(PROCESS_BLOCK_BUCKETS.len(), 13);
        assert!(PROCESS_BLOCK_BUCKETS.contains(&0.4));
        assert!(PROCESS_BLOCK_BUCKETS.contains(&0.75));
    }

    /// M2: production import path must use the dual observation site (not
    /// independently dual-feed the two helpers).
    #[test]
    fn finish_imported_wires_dual_observation_site() {
        let import_src = include_str!("import.rs");
        assert!(
            import_src.contains("observe_process_block_with_local"),
            "finish_imported must call observe_process_block_with_local"
        );
        // Guard against re-introducing independent dual-feed at the site.
        let after_comment = import_src
            .split("// Inclusive + exclusive dual observation")
            .nth(1)
            .unwrap_or("");
        let site = after_comment
            .split("if let Some(post)")
            .next()
            .unwrap_or("");
        assert!(
            site.contains("observe_process_block_with_local"),
            "dual observation site missing in finish_imported:\n{site}"
        );
        assert!(
            !site.contains("observe_process_block_local("),
            "finish_imported must not call observe_process_block_local separately:\n{site}"
        );
        assert!(
            !site.contains("observe_process_block("),
            "finish_imported must not call observe_process_block separately:\n{site}"
        );
    }

    #[test]
    fn optimistic_nodes_not_roots_name() {
        let mut registry = Registry::default();
        let _m = ChainMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("cc_chain_optimistic_nodes"),
            "expected optimistic_nodes gauge:\n{buf}"
        );
        // Forbidden legacy name (≠13/8) — assembled so source grep stays clean.
        let forbidden = format!("cc_chain_optimistic_{}", "roots");
        assert!(
            !buf.contains(&forbidden),
            "{forbidden} must not exist (≠13/8):\n{buf}"
        );
        assert!(
            buf.contains("result=\"deferred_engine\""),
            "deferred_engine label must be seeded:\n{buf}"
        );
    }

    #[derive(Debug)]
    struct HistSnap {
        count: u64,
        buckets: Vec<(String, u64)>,
    }

    fn histogram_snapshot(buf: &str, family: &str) -> HistSnap {
        let count_prefix = format!("{family}_count");
        let bucket_prefix = format!("{family}_bucket{{");
        let mut count = 0_u64;
        let mut buckets = Vec::new();
        for line in buf.lines() {
            if let Some(rest) = line.strip_prefix(&count_prefix) {
                let n = rest.trim();
                count = n
                    .parse::<f64>()
                    .map(|v| v as u64)
                    .or_else(|_| n.parse::<u64>())
                    .unwrap_or(0);
            }
            if let Some(rest) = line.strip_prefix(&bucket_prefix) {
                // le="0.4"} 1
                if let Some(le_start) = rest.find("le=\"") {
                    let after = &rest[le_start + 4..];
                    if let Some(le_end) = after.find('"') {
                        let le = after[..le_end].to_owned();
                        let value_part = rest.rsplit_once(' ').map(|(_, v)| v).unwrap_or("0");
                        let v = value_part
                            .parse::<f64>()
                            .map(|x| x as u64)
                            .or_else(|_| value_part.parse::<u64>())
                            .unwrap_or(0);
                        buckets.push((le, v));
                    }
                }
            }
        }
        HistSnap { count, buckets }
    }
}
