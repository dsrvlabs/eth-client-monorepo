//! Sampling tracker — Architecture §8.4 / CC-24c.
//!
//! One map of at most [`SAMPLING_TASK_BOUND`] concurrent per-root tasks (two
//! epochs). Completion is **all-or-nothing set equality** over the sampled
//! column set (`verified == required` on [`BTreeSet`]s) — never a length
//! threshold (spec delta 6 / CC-24/2).
//!
//! Tasks are created on the first sight of **either** the block or any column
//! for that root. Zero-blob blocks complete immediately (R-4). At the end of
//! slot *N*, incomplete tasks enter [`TaskState::Recovering`] and emit a
//! [`RecoveryTrigger`] for CC-25's by-root ladder; exhaustion marks
//! [`TaskState::Abandoned`].

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_proto::p2p::DataAvailable;
use cc_types::networking::ColumnIndex;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::clock::SlotClock;
use crate::gossip::validate::column::SamplingFeed;
use crate::metrics::{ColumnSource, DaOutcome, P2pMetrics, QueueName};

// ── Constants ───────────────────────────────────────────────────────────────

/// Concurrent sampling tasks (two epochs of head-following work).
pub const SAMPLING_TASK_BOUND: usize = 64;

// ── Task state ──────────────────────────────────────────────────────────────

/// Lifecycle of one per-root sampling task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    /// Waiting for sampled columns (and optionally the block header).
    Pending,
    /// Past deadline; CC-25 will drive by-root recovery from here.
    Recovering {
        /// Recovery attempts so far (CC-25).
        attempts: u32,
    },
    /// `verified == required`; [`DataAvailable`] emitted.
    Complete,
    /// Deadline expired without full set (pre-CC-25), or recovery exhausted.
    Abandoned,
}

/// One root's sampling progress.
#[derive(Debug, Clone)]
pub struct SamplingTask {
    /// Beacon block root.
    pub root: [u8; 32],
    /// Slot of the block / sidecars.
    pub slot: u64,
    /// Sampled column indices this node must retrieve.
    ///
    /// Empty **only** for an explicit zero-blob block ([`Self::zero_blob`]) —
    /// never because the construction-time template was empty (M1).
    pub required: BTreeSet<ColumnIndex>,
    /// Verified column indices (subset of the global matrix; only those in
    /// [`Self::required`] advance completion).
    pub verified: BTreeSet<ColumnIndex>,
    /// Block (or header via sidecar) has been observed.
    pub header_seen: bool,
    /// Explicit zero-blob from `on_block(..., commitment_count = 0)` (R-4).
    ///
    /// Empty `required`/`verified` equality is valid **only** when this is set;
    /// an empty construction template must not auto-complete (M1 / H1).
    pub zero_blob: bool,
    /// End of slot *N* — CC-25's recovery trigger.
    pub deadline: Instant,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Any verified column arrived via by-root (outcome label selection).
    pub saw_byroot: bool,
    /// [`DaOutcome::Deferred`] recorded once for this task.
    deferred_recorded: bool,
    /// [`DataAvailable`] emitted once for this task.
    da_emitted: bool,
}

impl SamplingTask {
    /// Whether sampling is fully satisfied (`verified == required`).
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.state, TaskState::Complete)
    }

    /// Missing columns still required for completion.
    #[must_use]
    pub fn missing(&self) -> BTreeSet<ColumnIndex> {
        self.required.difference(&self.verified).copied().collect()
    }
}

// ── Deadline clock ──────────────────────────────────────────────────────────

/// Produces the monotonic deadline for the end of slot *N*.
pub trait DeadlineClock: Send + Sync {
    /// Instant at which slot `slot` ends (start of `slot + 1`).
    fn end_of_slot(&self, slot: u64) -> Instant;
}

impl DeadlineClock for SlotClock {
    fn end_of_slot(&self, slot: u64) -> Instant {
        let end_unix = self.slot_start(slot.saturating_add(1));
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let now = Instant::now();
        if end_unix <= now_unix {
            now
        } else {
            now + Duration::from_secs(end_unix - now_unix)
        }
    }
}

/// Fixed deadline for every slot (unit tests).
#[derive(Debug, Clone, Copy)]
pub struct FixedDeadline(pub Instant);

impl DeadlineClock for FixedDeadline {
    fn end_of_slot(&self, _slot: u64) -> Instant {
        self.0
    }
}

// ── Tracker ─────────────────────────────────────────────────────────────────

/// Per-root sampling coordinator (Architecture §8.4).
pub struct SamplingTracker {
    /// Sampled column set template (copied into each non-zero-blob task).
    required_template: BTreeSet<ColumnIndex>,
    /// Active tasks keyed by block root.
    tasks: HashMap<[u8; 32], SamplingTask>,
    /// Insertion order for oldest-first eviction.
    order: VecDeque<[u8; 32]>,
    /// Capacity (default [`SAMPLING_TASK_BOUND`]).
    bound: usize,
    /// Cumulative oldest-evicted tasks.
    evictions: u64,
    /// Deadline source.
    deadlines: Arc<dyn DeadlineClock>,
    /// Metrics (optional for pure unit tests).
    metrics: Option<P2pMetrics>,
    /// CC-27 stream edge: completed roots (tests capture this).
    da_tx: Option<mpsc::UnboundedSender<DataAvailable>>,
}

impl std::fmt::Debug for SamplingTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SamplingTracker")
            .field("required_template", &self.required_template)
            .field("tasks", &self.tasks.len())
            .field("bound", &self.bound)
            .field("evictions", &self.evictions)
            .finish_non_exhaustive()
    }
}

/// Internal result of deadline expiry.
enum ExpireAction {
    Complete(DaOutcome),
    /// Enter recovery (CC-25); task left in [`TaskState::Recovering`].
    NeedsRecovery {
        slot: u64,
        missing: BTreeSet<ColumnIndex>,
    },
}

/// Work item emitted when a sampling task hits the end-of-slot-*N* deadline.
///
/// The host / recovery driver runs [`crate::das::recovery::recover`] and then
/// either feeds columns via [`SamplingTracker::on_column`] (`ColumnSource::ByRoot`)
/// or calls [`SamplingTracker::mark_abandoned`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryTrigger {
    /// Beacon block root.
    pub root: [u8; 32],
    /// Slot of the block.
    pub slot: u64,
    /// `required − verified` at deadline.
    pub missing: BTreeSet<ColumnIndex>,
}

impl SamplingTracker {
    /// Build a tracker with the node's sampled column set.
    #[must_use]
    pub fn new(
        required_columns: BTreeSet<ColumnIndex>,
        deadlines: Arc<dyn DeadlineClock>,
        metrics: Option<P2pMetrics>,
        da_tx: Option<mpsc::UnboundedSender<DataAvailable>>,
    ) -> Self {
        Self {
            required_template: required_columns,
            tasks: HashMap::new(),
            order: VecDeque::new(),
            bound: SAMPLING_TASK_BOUND,
            evictions: 0,
            deadlines,
            metrics,
            da_tx,
        }
    }

    /// Override the concurrent-task bound (tests).
    #[must_use]
    pub fn with_bound(mut self, bound: usize) -> Self {
        self.bound = bound.max(1);
        self
    }

    /// Configured bound.
    #[must_use]
    pub const fn bound(&self) -> usize {
        self.bound
    }

    /// Current number of tasks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Cumulative oldest-first evictions.
    #[must_use]
    pub const fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Borrow a task by root.
    #[must_use]
    pub fn get(&self, root: &[u8; 32]) -> Option<&SamplingTask> {
        self.tasks.get(root)
    }

    /// Sampled-column template used for non-zero-blob tasks.
    #[must_use]
    pub const fn required_template(&self) -> &BTreeSet<ColumnIndex> {
        &self.required_template
    }

    // ── Entry points ────────────────────────────────────────────────────────

    /// First sight (or re-sight) of a block for `root`.
    ///
    /// `commitment_count == 0` is the zero-blob case (R-4): required becomes
    /// empty and the task completes immediately — **only** when no verified
    /// samples are already held for this root (H1). Partial samples from a
    /// column-first path are never wiped by a later zero-commitment claim.
    pub fn on_block(&mut self, root: [u8; 32], slot: u64, commitment_count: u64) {
        self.ensure_task(root, slot);
        let refresh_deadline = self
            .tasks
            .get(&root)
            .is_some_and(|t| t.slot == 0 && slot != 0);
        let new_deadline = if refresh_deadline {
            Some(self.deadlines.end_of_slot(slot))
        } else {
            None
        };
        let template = self.required_template.clone();

        {
            let Some(task) = self.tasks.get_mut(&root) else {
                return;
            };
            if matches!(task.state, TaskState::Complete | TaskState::Abandoned) {
                return;
            }
            task.header_seen = true;
            if let Some(dl) = new_deadline {
                task.slot = slot;
                task.deadline = dl;
            }

            if commitment_count == 0 {
                // R-4 / H1: true zero-blob only when we hold no samples yet.
                // Never clear `verified` from a non-zero sampling task.
                if task.verified.is_empty() {
                    task.zero_blob = true;
                    task.required.clear();
                    // verified already empty — empty==empty completes below.
                } else {
                    // Columns already attributed to this root: keep required
                    // (sampled set) and verified intact; do not rebrand as
                    // zero-blob or wipe partial progress.
                    debug!(
                        root = %hex_root(&root),
                        ?task.verified,
                        "on_block(commitment_count=0) ignored wipe: partial samples present"
                    );
                }
            } else {
                // Non-zero blob block: required is the sampled set. Never clear
                // verified. Apply template when still empty (empty construction
                // template stays empty — will not false-complete, M1).
                task.zero_blob = false;
                if task.required.is_empty() && !template.is_empty() {
                    task.required = template;
                }
            }
        }

        self.finish_if_ready(&root);
        self.maybe_record_deferred(&root);
    }

    /// A verified column arrived from `source`.
    ///
    /// Creates the task if absent (column-before-block). Only indices in the
    /// required set advance verification. Increments
    /// `cc_p2p_columns_received_total{source}` for every call (including
    /// duplicates — source accounting is receipt-side).
    pub fn on_column(
        &mut self,
        root: [u8; 32],
        slot: u64,
        column_index: ColumnIndex,
        source: ColumnSource,
    ) {
        if let Some(m) = &self.metrics {
            m.inc_columns_received(source);
        }

        self.ensure_task(root, slot);
        let refresh_deadline = self
            .tasks
            .get(&root)
            .is_some_and(|t| t.slot == 0 && slot != 0);
        let new_deadline = if refresh_deadline {
            Some(self.deadlines.end_of_slot(slot))
        } else {
            None
        };

        {
            let Some(task) = self.tasks.get_mut(&root) else {
                return;
            };
            if matches!(task.state, TaskState::Complete | TaskState::Abandoned) {
                return;
            }
            if let Some(dl) = new_deadline {
                task.slot = slot;
                task.deadline = dl;
            }
            // A column's signed header authenticates the block (D1 / CC-24/5).
            task.header_seen = true;

            if source == ColumnSource::ByRoot {
                task.saw_byroot = true;
            }

            // Only sampled indices count toward completion.
            if task.required.contains(&column_index) {
                task.verified.insert(column_index);
            }
        }

        self.finish_if_ready(&root);
        self.maybe_record_deferred(&root);
    }

    /// Drive deadline expiry for all pending tasks.
    ///
    /// Incomplete tasks past `deadline` (end of slot *N*) enter
    /// [`TaskState::Recovering`] and yield a [`RecoveryTrigger`] for the
    /// by-root ladder (CC-25). There is no unbounded wait path: either recovery
    /// fills the set or the driver calls [`Self::mark_abandoned`].
    pub fn poll_deadlines(&mut self, now: Instant) -> Vec<RecoveryTrigger> {
        let expired: Vec<[u8; 32]> = self
            .tasks
            .iter()
            .filter(|(_, t)| {
                matches!(t.state, TaskState::Pending) && t.deadline <= now && !t.is_complete()
            })
            .map(|(r, _)| *r)
            .collect();

        let mut triggers = Vec::with_capacity(expired.len());
        for root in expired {
            if let Some(t) = self.expire_task(&root) {
                triggers.push(t);
            }
        }
        self.sync_gauge();
        triggers
    }

    /// Mark a recovering task abandoned after the by-root ladder is exhausted.
    ///
    /// Records `cc_p2p_da_outcome_total{result="abandoned"}` once and emits
    /// the structured abandon log (root, slot, missing, peers tried).
    pub fn mark_abandoned(&mut self, root: &[u8; 32], peers_tried: &[String]) {
        self.maybe_record_deferred(root);
        let (slot, missing) = {
            let Some(task) = self.tasks.get_mut(root) else {
                return;
            };
            if matches!(task.state, TaskState::Complete | TaskState::Abandoned) {
                return;
            }
            task.state = TaskState::Abandoned;
            (task.slot, task.missing())
        };
        if let Some(m) = &self.metrics {
            m.inc_da_outcome(DaOutcome::Abandoned);
        }
        warn!(
            root = %hex_root(root),
            slot,
            ?missing,
            peers_tried = ?peers_tried,
            "by-root recovery exhausted; abandoned"
        );
        self.sync_gauge();
    }

    /// Bump the recovery attempt counter on a [`TaskState::Recovering`] task.
    pub fn note_recovery_attempt(&mut self, root: &[u8; 32]) {
        if let Some(task) = self.tasks.get_mut(root)
            && let TaskState::Recovering { attempts } = &mut task.state
        {
            *attempts = attempts.saturating_add(1);
        }
    }

    // ── Internals ───────────────────────────────────────────────────────────

    fn ensure_task(&mut self, root: [u8; 32], slot: u64) {
        if self.tasks.contains_key(&root) {
            return;
        }
        while self.tasks.len() >= self.bound {
            self.evict_oldest();
        }
        let deadline = self.deadlines.end_of_slot(slot);
        let task = SamplingTask {
            root,
            slot,
            required: self.required_template.clone(),
            verified: BTreeSet::new(),
            header_seen: false,
            zero_blob: false,
            deadline,
            state: TaskState::Pending,
            saw_byroot: false,
            deferred_recorded: false,
            da_emitted: false,
        };
        self.tasks.insert(root, task);
        self.order.push_back(root);
        self.sync_gauge();
    }

    fn evict_oldest(&mut self) {
        while let Some(old) = self.order.pop_front() {
            if self.tasks.remove(&old).is_some() {
                self.evictions = self.evictions.saturating_add(1);
                debug!(
                    root = %hex_root(&old),
                    evictions = self.evictions,
                    "sampling task map full; oldest-evicted"
                );
                self.sync_gauge();
                return;
            }
        }
    }

    fn finish_if_ready(&mut self, root: &[u8; 32]) {
        let outcome = {
            let Some(task) = self.tasks.get_mut(root) else {
                return;
            };
            if matches!(task.state, TaskState::Complete | TaskState::Abandoned) {
                return;
            }
            // All-or-nothing: set equality, never a length threshold.
            if task.verified != task.required {
                return;
            }
            // M1: empty==empty is valid only for an explicit zero-blob block.
            // An empty construction `required_template` must not auto-complete.
            if task.required.is_empty() && !task.zero_blob {
                return;
            }
            task.state = TaskState::Complete;
            if task.saw_byroot {
                DaOutcome::Recovered
            } else {
                DaOutcome::Imported
            }
        };
        self.emit_complete(root, outcome);
    }

    fn emit_complete(&mut self, root: &[u8; 32], outcome: DaOutcome) {
        let (slot, already) = {
            let Some(task) = self.tasks.get_mut(root) else {
                return;
            };
            if task.da_emitted {
                return;
            }
            task.da_emitted = true;
            (task.slot, false)
        };
        let _ = already;

        if let Some(m) = &self.metrics {
            m.inc_da_outcome(outcome);
        }

        let da = DataAvailable {
            root: root.to_vec(),
            slot,
        };
        if let Some(tx) = &self.da_tx
            && tx.send(da).is_err()
        {
            warn!(
                root = %hex_root(root),
                slot,
                "DataAvailable receiver dropped"
            );
        }
        debug!(
            root = %hex_root(root),
            slot,
            outcome = outcome.as_str(),
            "sampling complete; DataAvailable emitted"
        );
    }

    fn maybe_record_deferred(&mut self, root: &[u8; 32]) {
        let record = {
            let Some(task) = self.tasks.get_mut(root) else {
                return;
            };
            if task.deferred_recorded {
                return;
            }
            if matches!(
                task.state,
                TaskState::Complete | TaskState::Abandoned | TaskState::Recovering { .. }
            ) {
                return;
            }
            // Incomplete with a known non-empty required set → deferred once.
            if task.header_seen && !task.required.is_empty() && task.verified != task.required {
                task.deferred_recorded = true;
                true
            } else {
                false
            }
        };
        if record && let Some(m) = &self.metrics {
            m.inc_da_outcome(DaOutcome::Deferred);
        }
    }

    fn expire_task(&mut self, root: &[u8; 32]) -> Option<RecoveryTrigger> {
        // Ensure deferred is counted before recovery / abandon.
        self.maybe_record_deferred(root);

        let action = {
            let task = self.tasks.get_mut(root)?;
            if !matches!(task.state, TaskState::Pending) {
                return None;
            }
            if task.verified == task.required {
                // Race: completed between poll and expire.
                task.state = TaskState::Complete;
                let outcome = if task.saw_byroot {
                    DaOutcome::Recovered
                } else {
                    DaOutcome::Imported
                };
                ExpireAction::Complete(outcome)
            } else {
                // CC-25/1: end of slot *N* → Recovering; recovery driver runs ladder.
                task.state = TaskState::Recovering { attempts: 0 };
                let slot = task.slot;
                let missing = task.missing();
                ExpireAction::NeedsRecovery { slot, missing }
            }
        };

        match action {
            ExpireAction::Complete(outcome) => {
                self.emit_complete(root, outcome);
                None
            }
            ExpireAction::NeedsRecovery { slot, missing } => {
                debug!(
                    root = %hex_root(root),
                    slot,
                    ?missing,
                    "sampling deadline expired; entering by-root recovery"
                );
                Some(RecoveryTrigger {
                    root: *root,
                    slot,
                    missing,
                })
            }
        }
    }

    fn sync_gauge(&self) {
        if let Some(m) = &self.metrics {
            m.set_queue_depth(QueueName::Sampling, self.tasks.len() as i64);
        }
    }
}

fn hex_root(root: &[u8; 32]) -> String {
    root.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Shared handle (SamplingFeed + multi-task access) ────────────────────────

/// `Arc<Mutex<…>>` handle implementing [`SamplingFeed`].
#[derive(Debug, Clone)]
pub struct SamplingHandle {
    inner: Arc<Mutex<SamplingTracker>>,
}

impl SamplingHandle {
    /// Wrap a tracker.
    #[must_use]
    pub fn new(tracker: SamplingTracker) -> Self {
        Self {
            inner: Arc::new(Mutex::new(tracker)),
        }
    }

    /// Lock the tracker (tests / block path).
    pub fn lock(&self) -> std::sync::MutexGuard<'_, SamplingTracker> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl SamplingFeed for SamplingHandle {
    fn on_column_accepted(&self, slot: u64, column_index: u64, block_root: [u8; 32]) {
        let mut guard = self.lock();
        guard.on_column(block_root, slot, column_index, ColumnSource::Gossip);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::clock::ClockConfig;
    use prometheus_client::registry::Registry;
    use std::time::Duration;

    fn root(n: u8) -> [u8; 32] {
        let mut r = [0u8; 32];
        r[0] = n;
        r
    }

    fn required_eight() -> BTreeSet<ColumnIndex> {
        (0..8).collect()
    }

    fn tracker_with(
        required: BTreeSet<ColumnIndex>,
        deadline: Instant,
    ) -> (
        SamplingTracker,
        mpsc::UnboundedReceiver<DataAvailable>,
        P2pMetrics,
    ) {
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let (tx, rx) = mpsc::unbounded_channel();
        let t = SamplingTracker::new(
            required,
            Arc::new(FixedDeadline(deadline)),
            Some(metrics.clone()),
            Some(tx),
        );
        (t, rx, metrics)
    }

    #[test]
    fn completion_is_set_equality_not_length() {
        // 7 of 8 must not complete.
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), far);
        let r = root(1);
        t.on_block(r, 10, 4);
        for col in 0..7u64 {
            t.on_column(r, 10, col, ColumnSource::Gossip);
        }
        let task = t.get(&r).expect("task");
        assert!(!task.is_complete());
        assert_eq!(task.state, TaskState::Pending);
        assert!(rx.try_recv().is_err(), "no DataAvailable on 7/8");
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 0);
        assert_eq!(metrics.da_outcome(DaOutcome::Deferred), 1);
        // 8th column completes via equality.
        t.on_column(r, 10, 7, ColumnSource::Gossip);
        let task = t.get(&r).expect("task");
        assert!(task.is_complete());
        assert_eq!(task.verified, task.required);
        let da = rx.try_recv().expect("DataAvailable");
        assert_eq!(da.root, r.to_vec());
        assert_eq!(da.slot, 10);
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 1);
    }

    #[test]
    fn seven_of_eight_never_uses_len_threshold() {
        // Grep-guarded property: completion compares sets, not a length count.
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, _m) = tracker_with(required_eight(), far);
        let r = root(2);
        // Insert 8 columns that are NOT the required set (9..17) — a length
        // threshold would pass; set equality must not.
        t.on_block(r, 5, 1);
        for col in 9..17u64 {
            t.on_column(r, 5, col, ColumnSource::Gossip);
        }
        let task = t.get(&r).unwrap();
        assert!(task.verified.is_empty(), "out-of-set columns ignored");
        assert!(!task.is_complete());
    }

    #[test]
    fn created_on_first_column_without_block() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, _m) = tracker_with(required_eight(), far);
        let r = root(3);
        t.on_column(r, 42, 0, ColumnSource::Gossip);
        let task = t.get(&r).expect("task from column");
        assert_eq!(task.required, required_eight());
        assert!(task.header_seen, "column authenticates header (D1)");
        assert_eq!(task.slot, 42);
        assert!(task.verified.contains(&0));
    }

    #[test]
    fn created_on_first_block_without_columns() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, _m) = tracker_with(required_eight(), far);
        let r = root(4);
        t.on_block(r, 42, 3);
        let task = t.get(&r).expect("task from block");
        assert_eq!(task.required, required_eight());
        assert!(task.header_seen);
        assert!(task.verified.is_empty());
        assert_eq!(task.state, TaskState::Pending);
    }

    #[test]
    fn column_and_block_produce_same_required_set() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t1, _, _) = tracker_with(required_eight(), far);
        let (mut t2, _, _) = tracker_with(required_eight(), far);
        let r = root(5);
        t1.on_column(r, 7, 1, ColumnSource::Gossip);
        t2.on_block(r, 7, 2);
        assert_eq!(t1.get(&r).unwrap().required, t2.get(&r).unwrap().required);
    }

    #[test]
    fn zero_blob_block_completes_immediately() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), far);
        let r = root(6);
        t.on_block(r, 100, 0);
        let task = t.get(&r).unwrap();
        assert!(task.is_complete());
        assert!(task.zero_blob);
        assert!(task.required.is_empty());
        assert!(task.verified.is_empty());
        assert_eq!(task.verified, task.required);
        let da = rx.try_recv().expect("zero-blob DataAvailable");
        assert_eq!(da.slot, 100);
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 1);
        assert_eq!(metrics.da_outcome(DaOutcome::Deferred), 0);
    }

    /// H1: `on_block(commitment_count=0)` must not wipe partial samples.
    #[test]
    fn on_block_zero_does_not_wipe_partial_samples() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), far);
        let r = root(60);
        // Column-first sampling progress for a non-zero sample set.
        t.on_column(r, 50, 0, ColumnSource::Gossip);
        t.on_column(r, 50, 1, ColumnSource::Gossip);
        let before = t.get(&r).unwrap().verified.clone();
        assert_eq!(before, BTreeSet::from([0, 1]));
        assert_eq!(t.get(&r).unwrap().required, required_eight());

        // Contradictory zero-commitment claim must not clear verified / required.
        t.on_block(r, 50, 0);
        let task = t.get(&r).unwrap();
        assert!(!task.zero_blob, "partial samples ⇒ not rebranded zero-blob");
        assert_eq!(task.verified, before, "verified must be preserved (H1)");
        assert_eq!(
            task.required,
            required_eight(),
            "required must stay sampled set"
        );
        assert!(!task.is_complete());
        assert!(rx.try_recv().is_err(), "no false zero-blob DataAvailable");
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 0);

        // Non-zero block + remaining columns still complete normally.
        t.on_block(r, 50, 3);
        for col in 2..8u64 {
            t.on_column(r, 50, col, ColumnSource::Gossip);
        }
        assert!(t.get(&r).unwrap().is_complete());
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 1);
    }

    /// M1: empty construction template must not auto-complete as zero-blob.
    #[test]
    fn empty_required_template_does_not_autocomple_without_zero_blob() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(BTreeSet::new(), far);
        let r = root(61);
        t.on_column(r, 1, 0, ColumnSource::Gossip);
        assert!(
            !t.get(&r).unwrap().is_complete(),
            "empty template + column must not complete (M1)"
        );
        t.on_block(r, 1, 2);
        assert!(
            !t.get(&r).unwrap().is_complete(),
            "empty template + non-zero block must not complete (M1)"
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 0);

        // True zero-blob still completes with empty required.
        let r2 = root(62);
        t.on_block(r2, 2, 0);
        assert!(t.get(&r2).unwrap().is_complete());
        assert!(t.get(&r2).unwrap().zero_blob);
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 1);
    }

    /// Non-zero `on_block` never clears verified (H1 companion).
    #[test]
    fn non_zero_on_block_preserves_verified() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, _m) = tracker_with(required_eight(), far);
        let r = root(63);
        t.on_column(r, 9, 3, ColumnSource::Gossip);
        t.on_column(r, 9, 5, ColumnSource::Gossip);
        let before = t.get(&r).unwrap().verified.clone();
        t.on_block(r, 9, 4);
        assert_eq!(t.get(&r).unwrap().verified, before);
        assert!(!t.get(&r).unwrap().zero_blob);
        assert_eq!(t.get(&r).unwrap().required, required_eight());
    }

    #[test]
    fn bound_64_oldest_evicted_with_counter() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, metrics) = tracker_with(required_eight(), far);
        assert_eq!(t.bound(), 64);
        for i in 0..100u8 {
            let mut r = [0u8; 32];
            r[0] = i;
            r[1] = i.wrapping_mul(3);
            t.on_block(r, u64::from(i), 1);
        }
        assert_eq!(t.len(), 64);
        assert_eq!(t.evictions(), 36);
        assert_eq!(metrics.queue_depth(QueueName::Sampling), 64);
        // Oldest roots (0..36) gone.
        for i in 0..36u8 {
            let mut r = [0u8; 32];
            r[0] = i;
            r[1] = i.wrapping_mul(3);
            assert!(t.get(&r).is_none(), "root {i} should be evicted");
        }
        for i in 36..100u8 {
            let mut r = [0u8; 32];
            r[0] = i;
            r[1] = i.wrapping_mul(3);
            assert!(t.get(&r).is_some(), "root {i} should remain");
        }
    }

    #[test]
    fn data_available_emitted_once_across_duplicates_and_many_blocks() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), far);
        const N: u64 = 200;
        for i in 0..N {
            let mut r = [0u8; 32];
            r[..8].copy_from_slice(&i.to_le_bytes());
            t.on_block(r, i, 2);
            for col in 0..8u64 {
                t.on_column(r, i, col, ColumnSource::Gossip);
                // Duplicate injection.
                t.on_column(r, i, col, ColumnSource::Gossip);
            }
        }
        let mut count = 0u64;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, N, "exactly one DataAvailable per completed task");
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), N);
    }

    #[test]
    fn recovered_when_any_column_by_root() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), far);
        let r = root(8);
        t.on_block(r, 11, 1);
        for col in 0..7u64 {
            t.on_column(r, 11, col, ColumnSource::Gossip);
        }
        // Eighth column via by-root recovery path.
        t.on_column(r, 11, 7, ColumnSource::ByRoot);
        assert!(t.get(&r).unwrap().is_complete());
        let _ = rx.try_recv().unwrap();
        assert_eq!(metrics.da_outcome(DaOutcome::Recovered), 1);
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 0);
    }

    #[test]
    fn columns_received_labels_all_three_sources() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (mut t, _rx, metrics) = tracker_with(required_eight(), far);
        let r = root(9);
        t.on_column(r, 1, 0, ColumnSource::Gossip);
        t.on_column(r, 1, 1, ColumnSource::ByRoot);
        t.on_column(r, 1, 2, ColumnSource::ByRange);
        assert_eq!(metrics.columns_received(ColumnSource::Gossip), 1);
        assert_eq!(metrics.columns_received(ColumnSource::ByRoot), 1);
        assert_eq!(metrics.columns_received(ColumnSource::ByRange), 1);
    }

    #[test]
    fn deadline_is_end_of_slot_n_against_slot_clock() {
        let clock = SlotClock::new(ClockConfig {
            genesis_time: 1_000,
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            maximum_gossip_clock_disparity: Duration::from_millis(500),
            slot_clock_offset_seconds: 0,
        });
        // Slot 5 starts at 1000 + 5*12 = 1060; ends at 1072.
        assert_eq!(clock.slot_start(5), 1_060);
        assert_eq!(clock.slot_start(6), 1_072);

        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut t =
            SamplingTracker::new(required_eight(), Arc::new(clock), Some(metrics), Some(tx));
        let r = root(10);
        // Create with a past slot so end_of_slot is ≤ now → deadline ≈ Instant::now().
        t.on_block(r, 0, 1);
        let task = t.get(&r).unwrap();
        // Deadline should not be far in the future for a genesis-era slot when
        // wall clock is 2026; end_unix is 1012, long past → Instant::now().
        assert!(
            task.deadline <= Instant::now() + Duration::from_millis(50),
            "past slot deadline should be ~now"
        );
    }

    #[test]
    fn expiry_enters_recovering_then_abandon_via_driver() {
        let past = Instant::now() - Duration::from_secs(1);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), past);
        let r = root(11);
        t.on_block(r, 3, 1);
        for col in 0..7u64 {
            t.on_column(r, 3, col, ColumnSource::Gossip);
        }
        let triggers = t.poll_deadlines(Instant::now());
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].root, r);
        assert_eq!(triggers[0].slot, 3);
        assert_eq!(triggers[0].missing, BTreeSet::from([7u64]));
        let task = t.get(&r).unwrap();
        assert!(matches!(task.state, TaskState::Recovering { attempts: 0 }));
        assert!(rx.try_recv().is_err(), "no DA while recovering");
        assert!(metrics.da_outcome(DaOutcome::Deferred) >= 1);
        assert_eq!(metrics.da_outcome(DaOutcome::Abandoned), 0);

        // Recovery fills last column via by-root → Recovered.
        t.on_column(r, 3, 7, ColumnSource::ByRoot);
        assert_eq!(t.get(&r).unwrap().state, TaskState::Complete);
        assert_eq!(metrics.da_outcome(DaOutcome::Recovered), 1);
        assert_eq!(rx.try_recv().unwrap().slot, 3);
    }

    #[test]
    fn expiry_abandon_after_recovery_exhaustion() {
        let past = Instant::now() - Duration::from_secs(1);
        let (mut t, mut rx, metrics) = tracker_with(required_eight(), past);
        let r = root(13);
        t.on_block(r, 4, 1);
        t.poll_deadlines(Instant::now());
        assert!(matches!(
            t.get(&r).unwrap().state,
            TaskState::Recovering { .. }
        ));
        t.note_recovery_attempt(&r);
        t.mark_abandoned(&r, &["peer-a".into(), "peer-b".into()]);
        assert_eq!(t.get(&r).unwrap().state, TaskState::Abandoned);
        assert!(rx.try_recv().is_err(), "no DA on abandon");
        assert_eq!(metrics.da_outcome(DaOutcome::Abandoned), 1);
        assert_eq!(metrics.da_outcome(DaOutcome::Imported), 0);
    }

    #[test]
    fn sampling_feed_handle_forwards_gossip_columns() {
        let far = Instant::now() + Duration::from_secs(3600);
        let (t, _rx, metrics) = tracker_with(required_eight(), far);
        let handle = SamplingHandle::new(t);
        let r = root(12);
        handle.on_column_accepted(8, 3, r);
        let guard = handle.lock();
        assert!(guard.get(&r).unwrap().verified.contains(&3));
        assert_eq!(metrics.columns_received(ColumnSource::Gossip), 1);
    }

    #[test]
    fn da_outcome_four_labels_only() {
        // Exhaustive set is DaOutcome::ALL — four values.
        assert_eq!(DaOutcome::ALL.len(), 4);
        let labels: BTreeSet<&str> = DaOutcome::ALL.iter().map(|d| d.as_str()).collect();
        assert_eq!(
            labels,
            BTreeSet::from(["imported", "deferred", "recovered", "abandoned"])
        );
    }
}
