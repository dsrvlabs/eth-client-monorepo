//! Chain service Prometheus metrics (CC-1C / Architecture §11).
//!
//! Registered into [`cc_bootstrap::Bootstrap::registry`] between `init` and
//! `serve` — the Phase 0 §4.1 seam. Bucket boundaries for the two budgeted
//! histograms include exact `0.4` and `1.0` so soak p95 is a counting question
//! (ADR-P1-15 / §11.2), not a quantile interpolation.

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

/// `result` label values for `cc_chain_import_total` (Architecture §11.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImportResult {
    Imported,
    Duplicate,
    Deferred,
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
            Self::UnknownParent => "unknown_parent",
            Self::Invalid => "invalid",
        }
    }
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

/// All chain Phase-1 metric families (CC-1C / §11.1).
///
/// Cheap to clone (each field is a handle into shared series storage).
///
/// CC-18b adds root-mismatch / backpressure counters and the body-ring gauge
/// (Architecture §7.2 / §7.5); registration stays in this module.
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
    pub resident_states: Gauge,
    pub subscribers: Gauge,
    pub budget_exceeded: Family<BudgetOpLabels, Counter>,
    /// Supplied `ImportBlockRequest.root` ≠ decoded `hash_tree_root` (CC-18b).
    pub import_root_mismatch: Counter,
    /// Command channel full after `send_timeout(2s)` (CC-18b).
    pub import_rejected_backpressure: Counter,
    /// Bodies retained for shallow-reorg replay (cap 64; CC-18b).
    pub body_ring_len: Gauge,
    /// Events dropped because the events channel was full/closed (SEC-2).
    pub event_publish_dropped: Counter,
    /// Checkpoint bootstrap attempts per provider (CC-19a / §8.1).
    pub bootstrap_attempts: Family<BootstrapLabels, Counter>,
    /// Blocks dropped from `pending_da` (timeout or capacity eviction; CC-24d).
    pub da_pending_dropped: Counter,
    /// Current `pending_da` occupancy (CC-24d).
    pub da_pending_occupancy: Gauge,
    /// Current PeerDAS available-root set occupancy (CC-24d).
    pub da_available_occupancy: Gauge,
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
        let resident_states = Gauge::default();
        let subscribers = Gauge::default();
        let budget_exceeded = Family::<BudgetOpLabels, Counter>::default();
        let import_root_mismatch = Counter::default();
        let import_rejected_backpressure = Counter::default();
        let body_ring_len = Gauge::default();
        let event_publish_dropped = Counter::default();
        let bootstrap_attempts = Family::<BootstrapLabels, Counter>::default();
        let da_pending_dropped = Counter::default();
        let da_pending_occupancy = Gauge::default();
        let da_available_occupancy = Gauge::default();

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
            "Block import outcomes (result=imported|duplicate|deferred|unknown_parent|invalid)",
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
            "Core-thread events dropped when the events channel is full or closed",
            event_publish_dropped.clone(),
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
            resident_states,
            subscribers,
            budget_exceeded,
            import_root_mismatch,
            import_rejected_backpressure,
            body_ring_len,
            event_publish_dropped,
            bootstrap_attempts,
            da_pending_dropped,
            da_pending_occupancy,
            da_available_occupancy,
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

    /// Increment dropped-event counter (SEC-2 non-blocking publish).
    pub fn inc_event_publish_dropped(&self) {
        self.event_publish_dropped.inc();
    }

    /// Read dropped-event counter (tests).
    pub fn event_publish_dropped_count(&self) -> u64 {
        self.event_publish_dropped.get()
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

    /// Set event-buffer occupancy gauges from live [`Occupancy`] (CC-18c / CC-1C).
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
        self.set_subscribers(occupancy.subscribers() as u64);
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
}
