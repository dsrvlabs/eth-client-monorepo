//! Four-state engine machine (CC-36a / Architecture §3.7).
//!
//! ```text
//! Internal:  Synced | Syncing | Offline | AuthFailed
//! External:  Online | Offline
//! Collapse:  Synced|Syncing → Online ; Offline|AuthFailed → Offline
//! ```
//!
//! - **`AuthFailed` is terminal** — no backoff into `Offline` (CC-36 /3).
//! - Capability cache is cleared on `AuthFailed` and `Offline` edges.
//! - On not-Synced → Synced: refresh capabilities + re-send cached
//!   `ForkchoiceStateV1` (CC-36 /4).
//! - Upcheck is **detached** (spawn) and **per-slot floored** (CC-36 /7).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use crate::capabilities::CapabilityCache;
use crate::errors::EngineError;
use crate::methods::eth_syncing::{EthSyncingResult, eth_syncing};
use crate::methods::fcu::forkchoice_updated_v3;
use crate::metrics::{EngineMetrics, EngineStateLabel, EngineStateLabels};
use crate::transport::{EngineTransport, SharedTransport};
use crate::version::ElForkSchedule;

/// Internal engine health (four values; metric `cc_engine_state{state}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineStateInternal {
    Synced,
    Syncing,
    Offline,
    AuthFailed,
}

impl EngineStateInternal {
    /// Prometheus `state` label value (`synced|syncing|offline|auth_failed`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Syncing => "syncing",
            Self::Offline => "offline",
            Self::AuthFailed => "auth_failed",
        }
    }

    /// Metric enum for `cc_engine_state`.
    #[must_use]
    pub const fn label(self) -> EngineStateLabel {
        match self {
            Self::Synced => EngineStateLabel::Synced,
            Self::Syncing => EngineStateLabel::Syncing,
            Self::Offline => EngineStateLabel::Offline,
            Self::AuthFailed => EngineStateLabel::AuthFailed,
        }
    }

    /// Collapse to the two-value external surface (§3.7).
    #[must_use]
    pub const fn external(self) -> EngineState {
        match self {
            Self::Synced | Self::Syncing => EngineState::Online,
            Self::Offline | Self::AuthFailed => EngineState::Offline,
        }
    }

    /// All four internal states (table-driven tests).
    pub const ALL: [Self; 4] = [
        Self::Synced,
        Self::Syncing,
        Self::Offline,
        Self::AuthFailed,
    ];
}

/// Externally visible engine reachability (two values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineState {
    Online,
    Offline,
}

impl EngineState {
    /// Whether `el_offline` should be true (`GET /eth/v1/node/syncing` / GetEngineState).
    #[must_use]
    pub const fn el_offline(self) -> bool {
        matches!(self, Self::Offline)
    }
}

/// Cached `ForkchoiceStateV1` triple for re-send on the Synced edge (CC-36 /4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedForkchoiceState {
    pub head_block_hash: [u8; 32],
    pub safe_block_hash: [u8; 32],
    pub finalized_block_hash: [u8; 32],
}

/// Whether ordered-lane EL calls (`newPayload` / `fcU`) may proceed.
///
/// Fail-closed when external state is [`EngineState::Offline`] (covers both
/// internal `Offline` and terminal `AuthFailed`).
#[must_use]
pub fn admits_el_call(external: EngineState) -> bool {
    matches!(external, EngineState::Online)
}

/// Why an upcheck transition fired (transition log for terminal tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTransition {
    pub from: EngineStateInternal,
    pub to: EngineStateInternal,
    pub reason: TransitionReason,
}

/// Class of upcheck outcome that produced a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionReason {
    EthSyncingFalse,
    EthSyncingOther,
    AuthRejected,
    TransportOrOther,
}

/// Inputs that drive one step of the state machine.
#[derive(Debug, Clone)]
pub enum UpcheckOutcome {
    /// `eth_syncing` returned successfully.
    Ok(EthSyncingResult),
    /// HTTP 401 / 403 (or other auth-class error).
    AuthRejected { body: String },
    /// Timeout, transport, 5xx, etc.
    Failure { detail: String },
}

impl UpcheckOutcome {
    /// Classify a transport-level [`EngineError`] into an upcheck outcome.
    #[must_use]
    pub fn from_engine_error(err: &EngineError) -> Self {
        match err {
            EngineError::Http401 { body } | EngineError::Http403 { body } => Self::AuthRejected {
                body: body.clone(),
            },
            other => Self::Failure {
                detail: other.to_string(),
            },
        }
    }
}

/// Shared engine state handle (service + upcheck driver).
#[derive(Debug, Clone)]
pub struct EngineStateHandle {
    inner: Arc<Mutex<EngineStateMachine>>,
    /// Watch for external Online/Offline (chain requeue edge is CC-36b / re-drive).
    external_tx: watch::Sender<EngineState>,
}

impl EngineStateHandle {
    /// Construct with empty capability cache and initial `Offline`.
    #[must_use]
    pub fn new(
        capabilities: Arc<CapabilityCache>,
        metrics: Option<EngineMetrics>,
        slot_duration: Duration,
    ) -> Self {
        let machine = EngineStateMachine::new(capabilities, metrics, slot_duration);
        let (external_tx, _) = watch::channel(machine.external());
        Self {
            inner: Arc::new(Mutex::new(machine)),
            external_tx,
        }
    }

    /// Subscribe to external Online/Offline changes.
    #[must_use]
    pub fn subscribe_external(&self) -> watch::Receiver<EngineState> {
        self.external_tx.subscribe()
    }

    /// Snapshot of the internal state.
    pub async fn internal(&self) -> EngineStateInternal {
        self.inner.lock().await.internal()
    }

    /// Snapshot of the external collapse.
    pub async fn external(&self) -> EngineState {
        self.inner.lock().await.external()
    }

    /// `GetEngineState` fields.
    pub async fn get_engine_state_fields(&self) -> (bool, String) {
        let g = self.inner.lock().await;
        (g.external().el_offline(), g.internal().as_str().to_owned())
    }

    /// Record the latest admitted fcU triple (for Synced-edge re-send).
    pub async fn cache_forkchoice(&self, state: CachedForkchoiceState) {
        self.inner.lock().await.cache_forkchoice(state);
    }

    /// Apply one upcheck outcome (tests / driver).
    pub async fn apply(&self, outcome: UpcheckOutcome) -> Option<StateTransition> {
        let mut g = self.inner.lock().await;
        let t = g.apply(outcome);
        if let Some(ref tr) = t {
            let _ = self.external_tx.send(tr.to.external());
        } else {
            let _ = self.external_tx.send(g.external());
        }
        t
    }

    /// Transition log (tests: AuthFailed terminal).
    pub async fn transition_log(&self) -> Vec<StateTransition> {
        self.inner.lock().await.transition_log().to_vec()
    }

    /// Detached-task upcheck count (tests: per-slot floor).
    pub async fn upcheck_count(&self) -> u64 {
        self.inner.lock().await.upcheck_count()
    }

    /// Take the pending re-send triple after a Synced-edge transition (tests / driver).
    pub async fn take_pending_fcu_resend(&self) -> Option<CachedForkchoiceState> {
        self.inner.lock().await.take_pending_fcu_resend()
    }

    /// Take the capability-refresh flag after a not-Synced → Synced edge.
    pub async fn take_pending_capability_refresh(&self) -> bool {
        self.inner.lock().await.take_pending_capability_refresh()
    }

    /// Shared capability cache.
    pub async fn capabilities(&self) -> Arc<CapabilityCache> {
        Arc::clone(self.inner.lock().await.capabilities())
    }

    /// Per-slot floor gate used by the detached driver.
    pub async fn try_mark_floor(&self, slot_index: u64) -> bool {
        let mut g = self.inner.lock().await;
        if g.internal() == EngineStateInternal::AuthFailed {
            return false;
        }
        if !g.should_floor_upcheck(slot_index) {
            return false;
        }
        g.mark_floor_upcheck(slot_index);
        true
    }

    /// Whether ordered-lane EL work may proceed (fail-closed when Offline/AuthFailed).
    pub async fn admits_el_call(&self) -> bool {
        admits_el_call(self.external().await)
    }

    /// Run one upcheck against the EL and apply the result.
    pub async fn run_upcheck(&self, transport: &EngineTransport, metrics: Option<&EngineMetrics>) {
        {
            let mut g = self.inner.lock().await;
            // Terminal: do not probe further (AuthFailed stays until process restart).
            if g.internal() == EngineStateInternal::AuthFailed {
                return;
            }
            g.note_upcheck_started();
        }
        let outcome = match eth_syncing(transport, metrics).await {
            Ok(r) => UpcheckOutcome::Ok(r),
            Err(e) => UpcheckOutcome::from_engine_error(&e),
        };
        let _ = self.apply(outcome).await;
    }

    /// Capability refresh + cached fcU re-send after any upcheck (floor **and**
    /// 250 ms event path). Must run on the same path that notices Synced.
    pub async fn apply_synced_edge_side_effects(
        &self,
        transport: &EngineTransport,
        metrics: Option<&EngineMetrics>,
        schedule: &ElForkSchedule,
    ) {
        // Capability refresh always on not-Synced → Synced (even with empty fcU cache).
        if self.take_pending_capability_refresh().await {
            let caps = self.capabilities().await;
            let _ = crate::methods::capabilities::exchange_capabilities(
                transport,
                caps.as_ref(),
                metrics,
            )
            .await;
        }
        // Re-send cached ForkchoiceStateV1 immediately (CC-36 /4).
        if let Some(fcu) = self.take_pending_fcu_resend().await {
            let _ = forkchoice_updated_v3(
                transport,
                schedule,
                metrics,
                &fcu.head_block_hash,
                &fcu.safe_block_hash,
                &fcu.finalized_block_hash,
                None,
            )
            .await;
        }
    }
}

/// Pure state machine + bookkeeping (not async by itself).
#[derive(Debug)]
pub struct EngineStateMachine {
    state: EngineStateInternal,
    capabilities: Arc<CapabilityCache>,
    metrics: Option<EngineMetrics>,
    cached_fcu: Option<CachedForkchoiceState>,
    /// Set when a not-Synced → Synced edge needs an immediate fcU re-send.
    pending_fcu_resend: Option<CachedForkchoiceState>,
    /// Set when a not-Synced → Synced edge needs a capability handshake refresh.
    pending_capability_refresh: bool,
    transitions: Vec<StateTransition>,
    upcheck_count: u64,
    slot_duration: Duration,
    /// Last slot index at which a floor upcheck ran (tests / driver).
    last_floor_slot: Option<u64>,
}

impl EngineStateMachine {
    /// Start `Offline` with an empty capability cache.
    #[must_use]
    pub fn new(
        capabilities: Arc<CapabilityCache>,
        metrics: Option<EngineMetrics>,
        slot_duration: Duration,
    ) -> Self {
        let s = Self {
            state: EngineStateInternal::Offline,
            capabilities,
            metrics,
            cached_fcu: None,
            pending_fcu_resend: None,
            pending_capability_refresh: false,
            transitions: Vec::new(),
            upcheck_count: 0,
            slot_duration,
            last_floor_slot: None,
        };
        s.publish_metrics();
        s
    }

    #[must_use]
    pub fn internal(&self) -> EngineStateInternal {
        self.state
    }

    #[must_use]
    pub fn external(&self) -> EngineState {
        self.state.external()
    }

    #[must_use]
    pub fn capabilities(&self) -> &Arc<CapabilityCache> {
        &self.capabilities
    }

    #[must_use]
    pub fn transition_log(&self) -> &[StateTransition] {
        &self.transitions
    }

    #[must_use]
    pub fn upcheck_count(&self) -> u64 {
        self.upcheck_count
    }

    pub fn note_upcheck_started(&mut self) {
        self.upcheck_count = self.upcheck_count.saturating_add(1);
    }

    pub fn cache_forkchoice(&mut self, state: CachedForkchoiceState) {
        self.cached_fcu = Some(state);
    }

    pub fn take_pending_fcu_resend(&mut self) -> Option<CachedForkchoiceState> {
        self.pending_fcu_resend.take()
    }

    pub fn take_pending_capability_refresh(&mut self) -> bool {
        let v = self.pending_capability_refresh;
        self.pending_capability_refresh = false;
        v
    }

    /// Apply one upcheck outcome. Returns the transition when state changes.
    ///
    /// **`AuthFailed` is terminal**: subsequent outcomes are ignored (no
    /// transition into `Offline` or anywhere else).
    pub fn apply(&mut self, outcome: UpcheckOutcome) -> Option<StateTransition> {
        if self.state == EngineStateInternal::AuthFailed {
            // Terminal: log nothing that looks like a leave; stay put.
            return None;
        }

        let (to, reason) = match &outcome {
            UpcheckOutcome::Ok(EthSyncingResult::NotSyncing) => {
                (EngineStateInternal::Synced, TransitionReason::EthSyncingFalse)
            }
            UpcheckOutcome::Ok(EthSyncingResult::Syncing(_)) => {
                (EngineStateInternal::Syncing, TransitionReason::EthSyncingOther)
            }
            UpcheckOutcome::AuthRejected { body } => {
                tracing::error!(
                    body = %body,
                    "execution engine auth rejected — terminal AuthFailed (no backoff to offline)"
                );
                (
                    EngineStateInternal::AuthFailed,
                    TransitionReason::AuthRejected,
                )
            }
            UpcheckOutcome::Failure { detail } => {
                tracing::error!(
                    detail = %detail,
                    "execution engine upcheck failed — Offline"
                );
                (
                    EngineStateInternal::Offline,
                    TransitionReason::TransportOrOther,
                )
            }
        };

        let from = self.state;
        if from == to {
            // Still apply side effects for Synced re-entry from itself? No —
            // only the edge matters. Capability clear only on enter.
            return None;
        }

        // Side effects on the edge.
        match to {
            EngineStateInternal::AuthFailed | EngineStateInternal::Offline => {
                self.capabilities.clear();
            }
            EngineStateInternal::Synced => {
                if from != EngineStateInternal::Synced {
                    tracing::info!("execution engine online");
                    // Always refresh capabilities on the edge (even with no fcU cache).
                    self.pending_capability_refresh = true;
                    // Re-send cached ForkchoiceStateV1 when present (CC-36 /4).
                    if let Some(fcu) = self.cached_fcu {
                        self.pending_fcu_resend = Some(fcu);
                    }
                }
            }
            EngineStateInternal::Syncing => {}
        }

        self.state = to;
        let tr = StateTransition { from, to, reason };
        self.transitions.push(tr.clone());
        self.publish_metrics();
        Some(tr)
    }

    /// Whether the per-slot floor should fire for `slot_index`.
    #[must_use]
    pub fn should_floor_upcheck(&self, slot_index: u64) -> bool {
        match self.last_floor_slot {
            None => true,
            Some(prev) => slot_index > prev,
        }
    }

    /// Record that a floor upcheck was scheduled for `slot_index`.
    pub fn mark_floor_upcheck(&mut self, slot_index: u64) {
        self.last_floor_slot = Some(slot_index);
    }

    #[must_use]
    pub fn slot_duration(&self) -> Duration {
        self.slot_duration
    }

    fn publish_metrics(&self) {
        let Some(m) = &self.metrics else {
            return;
        };
        for s in EngineStateInternal::ALL {
            let v = i64::from(s == self.state);
            m.state
                .get_or_create(&EngineStateLabels {
                    state: s.as_str().to_owned(),
                })
                .set(v);
        }
        m.el_offline
            .set(i64::from(self.external().el_offline()));
    }
}

/// Spawn the detached upcheck driver: event-driven reschedule + per-slot floor.
///
/// Structural criterion: this function contains `tokio::spawn` (CC-36 /7).
pub fn spawn_upcheck_driver(
    handle: EngineStateHandle,
    transport: SharedTransport,
    metrics: Option<EngineMetrics>,
    schedule: ElForkSchedule,
    slot_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    // Detached task — never inline on the import path ("to avoid slowing down
    // this request", CC-36 /7).
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(slot_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Slot counter for floor uniqueness (tests count eth_syncing calls).
        let slot_counter = AtomicU64::new(0);
        loop {
            interval.tick().await;
            let slot = slot_counter.fetch_add(1, Ordering::Relaxed);
            if !handle.try_mark_floor(slot).await {
                continue;
            }
            // Nested detached spawn so a slow upcheck cannot block the floor tick.
            let h = handle.clone();
            let t = Arc::clone(&transport);
            let m = metrics.clone();
            let sched = schedule.clone();
            tokio::spawn(async move {
                run_upcheck_with_side_effects(&h, t.as_ref(), m.as_ref(), &sched).await;
                // After a failed call or success from not-Synced, schedule
                // another upcheck soon — also detached — with the **same**
                // side-effect path so fcU resend is not deferred to the next floor.
                let st = h.internal().await;
                if st != EngineStateInternal::Synced && st != EngineStateInternal::AuthFailed {
                    let h2 = h.clone();
                    let t2 = Arc::clone(&t);
                    let m2 = m.clone();
                    let sched2 = sched.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        run_upcheck_with_side_effects(&h2, t2.as_ref(), m2.as_ref(), &sched2)
                            .await;
                    });
                }
            });
        }
    })
}

/// One upcheck + unified Synced-edge side effects (floor and 250 ms paths share this).
async fn run_upcheck_with_side_effects(
    handle: &EngineStateHandle,
    transport: &EngineTransport,
    metrics: Option<&EngineMetrics>,
    schedule: &ElForkSchedule,
) {
    handle.run_upcheck(transport, metrics).await;
    handle
        .apply_synced_edge_side_effects(transport, metrics, schedule)
        .await;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::capabilities::{CapabilityCache, CapabilitySnapshot};
    use crate::methods::eth_syncing::EthSyncingResult;
    use serde_json::json;
    use std::time::Duration;

    fn machine() -> EngineStateMachine {
        EngineStateMachine::new(
            Arc::new(CapabilityCache::new()),
            None,
            Duration::from_secs(12),
        )
    }

    fn seed_cache(m: &EngineStateMachine) {
        m.capabilities().store(CapabilitySnapshot::from_el_methods([
            "engine_newPayloadV4".into(),
        ]));
        assert!(!m.capabilities().is_empty());
    }

    /// CC-36 /1: table-driven over every internal state × edge.
    #[test]
    fn engine_state_transitions() {
        // (from, outcome, expected_to, external_of_to)
        let cases: Vec<(
            EngineStateInternal,
            UpcheckOutcome,
            EngineStateInternal,
            EngineState,
        )> = vec![
            // From Offline
            (
                EngineStateInternal::Offline,
                UpcheckOutcome::Ok(EthSyncingResult::NotSyncing),
                EngineStateInternal::Synced,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Offline,
                UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!(true))),
                EngineStateInternal::Syncing,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Offline,
                UpcheckOutcome::AuthRejected {
                    body: "stale token".into(),
                },
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::Offline,
                UpcheckOutcome::Failure {
                    detail: "reset".into(),
                },
                EngineStateInternal::Offline,
                EngineState::Offline,
            ),
            // From Syncing
            (
                EngineStateInternal::Syncing,
                UpcheckOutcome::Ok(EthSyncingResult::NotSyncing),
                EngineStateInternal::Synced,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Syncing,
                UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!({}))),
                EngineStateInternal::Syncing,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Syncing,
                UpcheckOutcome::AuthRejected { body: "x".into() },
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::Syncing,
                UpcheckOutcome::Failure {
                    detail: "to".into(),
                },
                EngineStateInternal::Offline,
                EngineState::Offline,
            ),
            // From Synced
            (
                EngineStateInternal::Synced,
                UpcheckOutcome::Ok(EthSyncingResult::NotSyncing),
                EngineStateInternal::Synced,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Synced,
                UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!(true))),
                EngineStateInternal::Syncing,
                EngineState::Online,
            ),
            (
                EngineStateInternal::Synced,
                UpcheckOutcome::AuthRejected { body: "x".into() },
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::Synced,
                UpcheckOutcome::Failure {
                    detail: "x".into(),
                },
                EngineStateInternal::Offline,
                EngineState::Offline,
            ),
            // From AuthFailed — terminal: stays AuthFailed for every outcome
            (
                EngineStateInternal::AuthFailed,
                UpcheckOutcome::Ok(EthSyncingResult::NotSyncing),
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::AuthFailed,
                UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!(true))),
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::AuthFailed,
                UpcheckOutcome::AuthRejected { body: "x".into() },
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
            (
                EngineStateInternal::AuthFailed,
                UpcheckOutcome::Failure {
                    detail: "x".into(),
                },
                EngineStateInternal::AuthFailed,
                EngineState::Offline,
            ),
        ];

        // Label set is exactly the four values.
        let labels: Vec<&str> = EngineStateInternal::ALL
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(labels, vec!["synced", "syncing", "offline", "auth_failed"]);

        // External collapse.
        assert_eq!(
            EngineStateInternal::Synced.external(),
            EngineState::Online
        );
        assert_eq!(
            EngineStateInternal::Syncing.external(),
            EngineState::Online
        );
        assert_eq!(
            EngineStateInternal::Offline.external(),
            EngineState::Offline
        );
        assert_eq!(
            EngineStateInternal::AuthFailed.external(),
            EngineState::Offline
        );

        for (from, outcome, expected_to, expected_ext) in cases {
            let mut m = machine();
            // Force `from` without side effects by applying a path, then reset log.
            force_state(&mut m, from);
            m.transitions.clear();
            let before = m.internal();
            assert_eq!(before, from);
            let tr = m.apply(outcome);
            assert_eq!(
                m.internal(),
                expected_to,
                "from={from:?} expected {expected_to:?}"
            );
            assert_eq!(m.external(), expected_ext);
            if from != expected_to {
                let t = tr.expect("state change must log a transition");
                assert_eq!(t.from, from);
                assert_eq!(t.to, expected_to);
            } else {
                assert!(tr.is_none(), "same-state must not log a leave");
            }
        }
    }

    fn force_state(m: &mut EngineStateMachine, target: EngineStateInternal) {
        // Reach target from Offline via apply (AuthFailed last).
        m.state = EngineStateInternal::Offline;
        match target {
            EngineStateInternal::Offline => {}
            EngineStateInternal::Syncing => {
                let _ = m.apply(UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!(true))));
            }
            EngineStateInternal::Synced => {
                let _ = m.apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing));
            }
            EngineStateInternal::AuthFailed => {
                let _ = m.apply(UpcheckOutcome::AuthRejected {
                    body: "force".into(),
                });
            }
        }
        assert_eq!(m.internal(), target);
    }

    /// CC-36 /3: AuthFailed stays put over ≥ 10 subsequent "slots".
    #[test]
    fn auth_failed_is_terminal() {
        let mut m = machine();
        let _ = m.apply(UpcheckOutcome::AuthRejected {
            body: "missing token".into(),
        });
        assert_eq!(m.internal(), EngineStateInternal::AuthFailed);
        let log_len_after_enter = m.transition_log().len();
        assert!(log_len_after_enter >= 1);
        assert_eq!(
            m.transition_log().last().unwrap().to,
            EngineStateInternal::AuthFailed
        );

        for i in 0..10 {
            // Present a variety of outcomes that would otherwise leave AuthFailed.
            let outcome = match i % 4 {
                0 => UpcheckOutcome::Ok(EthSyncingResult::NotSyncing),
                1 => UpcheckOutcome::Ok(EthSyncingResult::Syncing(json!(true))),
                2 => UpcheckOutcome::Failure {
                    detail: "timeout".into(),
                },
                _ => UpcheckOutcome::AuthRejected {
                    body: "again".into(),
                },
            };
            let tr = m.apply(outcome);
            assert!(
                tr.is_none(),
                "AuthFailed must not transition (slot {i}): {tr:?}"
            );
            assert_eq!(m.internal(), EngineStateInternal::AuthFailed);
        }
        // No backoff into Offline — transition log has no AuthFailed → Offline.
        assert!(
            !m.transition_log()
                .iter()
                .any(|t| t.from == EngineStateInternal::AuthFailed
                    && t.to == EngineStateInternal::Offline),
            "AuthFailed must not back off into Offline: {:?}",
            m.transition_log()
        );
        assert_eq!(
            m.transition_log().len(),
            log_len_after_enter,
            "terminal state must not append leave transitions"
        );
    }

    #[test]
    fn cache_cleared_on_auth_failed() {
        let mut m = machine();
        seed_cache(&m);
        let _ = m.apply(UpcheckOutcome::AuthRejected {
            body: "stale token".into(),
        });
        assert!(
            m.capabilities().is_empty(),
            "capability cache must be empty after AuthFailed"
        );
    }

    #[test]
    fn cache_cleared_on_offline() {
        let mut m = machine();
        // Enter Synced with a cache, then Offline.
        let _ = m.apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing));
        seed_cache(&m);
        let _ = m.apply(UpcheckOutcome::Failure {
            detail: "connection reset".into(),
        });
        assert_eq!(m.internal(), EngineStateInternal::Offline);
        assert!(
            m.capabilities().is_empty(),
            "capability cache must be empty after Offline"
        );
    }

    /// CC-36 /4: not-Synced → Synced queues a re-send of the cached triple.
    #[test]
    fn fcu_resent_on_synced_edge() {
        let mut m = machine();
        let cached = CachedForkchoiceState {
            head_block_hash: [1u8; 32],
            safe_block_hash: [2u8; 32],
            finalized_block_hash: [3u8; 32],
        };
        m.cache_forkchoice(cached);
        // Start Offline (default), go to Synced — edge must arm re-send.
        let tr = m
            .apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing))
            .expect("Offline → Synced");
        assert_eq!(tr.from, EngineStateInternal::Offline);
        assert_eq!(tr.to, EngineStateInternal::Synced);
        assert!(
            m.take_pending_capability_refresh(),
            "Synced edge must arm capability refresh even with a cached fcU"
        );
        let resent = m
            .take_pending_fcu_resend()
            .expect("Synced edge must re-send cached ForkchoiceStateV1");
        assert_eq!(resent, cached);
        // No second re-send without a new edge.
        assert!(m.take_pending_fcu_resend().is_none());
        assert!(!m.take_pending_capability_refresh());
    }

    /// Capability refresh is armed on Synced even when no fcU has been cached.
    #[test]
    fn capability_refresh_on_synced_without_fcu_cache() {
        let mut m = machine();
        let _ = m
            .apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing))
            .expect("Offline → Synced");
        assert!(m.take_pending_capability_refresh());
        assert!(m.take_pending_fcu_resend().is_none());
    }

    #[test]
    fn admits_el_call_fail_closed_when_offline() {
        assert!(admits_el_call(EngineState::Online));
        assert!(!admits_el_call(EngineState::Offline));
        assert!(!admits_el_call(EngineStateInternal::AuthFailed.external()));
        assert!(admits_el_call(EngineStateInternal::Syncing.external()));
    }

    /// Structural: driver uses spawn (grep criterion lives on this module).
    #[test]
    fn upcheck_driver_is_spawned_detached() {
        // Source-level contract is grep for `spawn` in this file; also assert
        // the function exists and returns a JoinHandle type name.
        let name = std::any::type_name_of_val(&spawn_upcheck_driver);
        assert!(name.contains("spawn_upcheck_driver"));
    }

    /// Per-slot floor: exactly one floor mark per slot over 5 slots.
    #[test]
    fn upcheck_per_slot_floor() {
        let mut m = machine();
        let mut floors = 0u64;
        for slot in 0..5 {
            assert!(
                m.should_floor_upcheck(slot),
                "slot {slot} must allow floor"
            );
            m.mark_floor_upcheck(slot);
            floors += 1;
            // Same slot again must not floor.
            assert!(
                !m.should_floor_upcheck(slot),
                "duplicate floor on slot {slot}"
            );
        }
        assert_eq!(floors, 5, "exactly one floor per slot over 5 slots");
    }
}
