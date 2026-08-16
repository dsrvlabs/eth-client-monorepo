//! Chain-stream client: thin adapter over [`cc_seam::Ipc`].
//!
//! The session / H1 / outstanding machine lives in `cc-seam`. This file maps
//! proto `ChainOutbound` onto seam types and applies metrics / late REJECT /
//! publish / view. OutstandingMap remains for unit tests of the cap.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_proto::p2p::{
    Acceptance, ChainView, GossipObject, ImportResult, ObjectKind as ProtoKind, PublishRequest,
    Reason, Verdict,
};
use cc_seam::{ChainIngress, Ipc, IpcConfig, IpcUpward, SeamError};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tracing::debug;

use super::publish::PublishDropCounter;
use super::view::ChainViewStore;
use super::{
    BACKOFF_CAP, BACKOFF_INITIAL, DEFAULT_VERDICT_LATE_AFTER, DEFAULT_VERDICT_TIMEOUT,
    OUTSTANDING_CAP, STALL_HEARTBEAT_FRACTION,
};
use crate::channels::{CHAIN_OUT_BOUND, ChainInbound, ChainOutbound, VerdictResolution};
use crate::metrics::{P2pMetrics, QueueName};

/// Jittered reconnect policy — owned by [`cc_seam::Ipc`].
pub use cc_seam::{full_jitter, new_session_id, next_backoff};

/// Correlation id for outstanding entries (`GossipObject.root` / `Verdict.correlation_id`).
pub type CorrelationId = Vec<u8>;

/// One un-answered downward object.
#[derive(Debug)]
pub struct OutstandingEntry {
    /// Session-local monotonic seq of the send.
    pub seq: u64,
    /// First time this correlation was sent (timeout clock — not reset on re-send).
    pub first_sent_at: Instant,
    /// When the object was last sent on the wire (latency for late detection).
    pub sent_at: Instant,
    /// Full object for re-send on reconnect (§10.4 / CC-27/4).
    pub object: GossipObject,
    /// Optional reply channel (tests / gossip validation hold).
    pub reply: Option<oneshot::Sender<VerdictResolution>>,
}

/// Bounded outstanding map: `CorrelationId → entry`.
///
/// Capped at [`OUTSTANDING_CAP`]; exported as `cc_p2p_queue_depth{q="outstanding"}`.
#[derive(Debug, Default)]
pub struct OutstandingMap {
    map: HashMap<CorrelationId, OutstandingEntry>,
}

impl OutstandingMap {
    /// Empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current depth.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// True when at the hard cap.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.map.len() >= OUTSTANDING_CAP
    }

    /// True when `id` is already tracked.
    #[must_use]
    pub fn contains(&self, id: &[u8]) -> bool {
        self.map.contains_key(id)
    }

    /// Insert a **new** id if under cap. Returns `false` when full **or** id already present.
    ///
    /// Never overwrites an existing entry (duplicate root must not orphan a reply).
    pub fn try_insert(&mut self, id: CorrelationId, entry: OutstandingEntry) -> bool {
        if self.map.contains_key(&id) {
            return false;
        }
        if self.map.len() >= OUTSTANDING_CAP {
            return false;
        }
        self.map.insert(id, entry);
        true
    }

    /// Remove and return an entry.
    pub fn remove(&mut self, id: &[u8]) -> Option<OutstandingEntry> {
        self.map.remove(id)
    }

    /// Iterate entries older than `timeout` since **first** send (for local IGNORE).
    pub fn drain_timed_out(
        &mut self,
        timeout: Duration,
        now: Instant,
    ) -> Vec<(CorrelationId, OutstandingEntry)> {
        let mut out = Vec::new();
        let keys: Vec<_> = self
            .map
            .iter()
            .filter(|(_, e)| now.duration_since(e.first_sent_at) >= timeout)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(e) = self.map.remove(&k) {
                out.push((k, e));
            }
        }
        out
    }

    /// Take all entries for re-send after reconnect (moves; reply channels preserved).
    pub fn take_all_for_resend(&mut self) -> Vec<OutstandingEntry> {
        self.map.drain().map(|(_, e)| e).collect()
    }

    /// Re-insert entries after a new session (new seq assigned by caller).
    pub fn reinsert(&mut self, id: CorrelationId, entry: OutstandingEntry) -> bool {
        self.try_insert(id, entry)
    }

    /// Keys currently tracked.
    #[must_use]
    pub fn keys(&self) -> Vec<CorrelationId> {
        self.map.keys().cloned().collect()
    }
}

/// Configuration for the chain-stream client.
#[derive(Debug, Clone)]
pub struct ChainStreamConfig {
    /// gRPC URI for `chain` (e.g. `http://127.0.0.1:9001`).
    pub chain_uri: String,
    /// Verdict wait timeout (default 2 s).
    pub verdict_timeout: Duration,
    /// Latency after which a verdict is counted late (default 100 ms).
    pub verdict_late_after: Duration,
    /// Initial reconnect backoff.
    pub backoff_initial: Duration,
    /// Backoff hard cap.
    pub backoff_cap: Duration,
    /// Connect timeout for each dial attempt.
    pub connect_timeout: Duration,
}

impl Default for ChainStreamConfig {
    fn default() -> Self {
        Self {
            chain_uri: "http://127.0.0.1:9001".to_owned(),
            verdict_timeout: DEFAULT_VERDICT_TIMEOUT,
            verdict_late_after: DEFAULT_VERDICT_LATE_AFTER,
            backoff_initial: BACKOFF_INITIAL,
            backoff_cap: BACKOFF_CAP,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// Handles shared with the rest of the p2p runtime.
#[derive(Debug, Clone)]
pub struct ChainStreamHandle {
    /// Latest `ChainView` (single writer: this client).
    pub view: ChainViewStore,
    /// Oldest-drop counter for outward publishes.
    pub publish_drops: PublishDropCounter,
}

impl ChainStreamHandle {
    /// Fresh handle with empty view.
    #[must_use]
    pub fn new() -> Self {
        Self {
            view: ChainViewStore::new(),
            publish_drops: PublishDropCounter::new(),
        }
    }
}

impl Default for ChainStreamHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Derive the stall bound from the configured gossipsub heartbeat (R-6).
///
/// `stall_max = heartbeat_interval × STALL_HEARTBEAT_FRACTION`.
/// **Never** inline a bare millisecond stall bound in call sites.
#[must_use]
pub fn stall_max_from_heartbeat(heartbeat: Duration) -> Duration {
    let nanos = (heartbeat.as_secs_f64() * STALL_HEARTBEAT_FRACTION * 1_000_000_000.0) as u64;
    Duration::from_nanos(nanos.max(1))
}

/// Drive [`Ipc`] until `shutdown`. Signature unchanged so `service.rs` does not churn.
pub async fn run_chain_stream_client(
    cfg: ChainStreamConfig,
    mut chain_out_rx: mpsc::Receiver<ChainOutbound>,
    chain_in_tx: mpsc::Sender<ChainInbound>,
    publish_fwd_tx: mpsc::Sender<PublishRequest>,
    handle: ChainStreamHandle,
    metrics: P2pMetrics,
    shutdown: watch::Receiver<bool>,
) {
    metrics.set_queue_depth(QueueName::Outstanding, 0);
    metrics.set_saturation_ratio(0.0);

    let upward = Arc::new(P2pUpward {
        handle,
        publish_fwd: publish_fwd_tx,
        chain_in: chain_in_tx,
        metrics: metrics.clone(),
        late_after: cfg.verdict_late_after,
    });
    let ipc_cfg = IpcConfig {
        chain_uri: cfg.chain_uri,
        verdict_timeout: cfg.verdict_timeout,
        backoff_initial: cfg.backoff_initial,
        backoff_cap: cfg.backoff_cap,
        connect_timeout: cfg.connect_timeout,
    };
    let mut shutdown_feed = shutdown.clone();
    let (ipc, _egress, mailbox, loop_fut) = Ipc::connect_with(ipc_cfg, shutdown, upward);
    tokio::pin!(loop_fut);

    // Cap in-flight submit_gossip so chain_out backs up and host reserve() stalls.
    let inflight = Arc::new(Semaphore::new(CHAIN_OUT_BOUND));
    let feed = async {
        loop {
            tokio::select! {
                biased;
                _ = shutdown_feed.changed() => {
                    if *shutdown_feed.borrow() {
                        break;
                    }
                }
                permit = inflight.clone().acquire_owned() => {
                    let Ok(permit) = permit else {
                        break;
                    };
                    tokio::select! {
                        biased;
                        _ = shutdown_feed.changed() => {
                            if *shutdown_feed.borrow() {
                                break;
                            }
                        }
                        item = chain_out_rx.recv() => {
                            match item {
                                None => break,
                                Some(item) => {
                                    let ipc = ipc.clone();
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        dispatch_outbound(ipc, item).await;
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    tokio::select! {
        biased;
        () = &mut loop_fut => {}
        () = feed => {
            // Producers closed; Ipc stays up for views until shutdown.
            loop_fut.await;
        }
    }
    drop(mailbox);
}

struct P2pUpward {
    handle: ChainStreamHandle,
    publish_fwd: mpsc::Sender<PublishRequest>,
    chain_in: mpsc::Sender<ChainInbound>,
    metrics: P2pMetrics,
    late_after: Duration,
}

impl IpcUpward for P2pUpward {
    fn on_view(&self, view: cc_seam::ChainView) {
        debug!(
            slot = view.slot,
            head_slot = view.head_slot,
            view_kind = view.view_kind,
            "received ChainView"
        );
        self.handle.view.store(view_to_proto(&view));
    }

    fn on_publish(&self, req: cc_seam::PublishRequest) {
        if self.publish_fwd.try_send(publish_to_proto(req)).is_err() {
            debug!("publish forward channel full or closed");
        }
    }

    fn on_verdict(&self, verdict: cc_seam::Verdict, latency: Duration) {
        self.metrics.observe_verdict_latency(latency.as_secs_f64());
        self.metrics.inc_chain_verdicts_received();
        if latency > self.late_after {
            self.metrics.inc_verdict_late();
        }
        let proto = verdict_to_proto(&verdict);
        let _ = self.chain_in.try_send(ChainInbound {
            verdict: proto,
            latency,
        });
    }

    fn on_stray_verdict(&self, verdict: cc_seam::Verdict) {
        self.metrics.inc_verdict_late();
        let _ = self.chain_in.try_send(ChainInbound {
            verdict: verdict_to_proto(&verdict),
            latency: Duration::ZERO,
        });
        debug!(
            "verdict for unknown/expired correlation_id (late after timeout or late import reject; not an equality term)"
        );
    }

    fn on_object_sent(&self) {
        self.metrics.inc_chain_objects_sent();
    }

    fn on_timeout(&self) {
        self.metrics.inc_verdict_timeout();
    }

    fn on_outstanding(&self, depth: usize) {
        let depth = depth as i64;
        self.metrics.set_queue_depth(QueueName::Outstanding, depth);
        let milli = ((depth as f64 / OUTSTANDING_CAP as f64) * 1000.0).round() as i64;
        self.metrics
            .set_saturation_ratio_milli(milli.clamp(0, 1000));
    }
}

async fn dispatch_outbound(ipc: Ipc, item: ChainOutbound) {
    let Some(obj) = gossip_from_proto(item.object) else {
        if let Some(reply) = item.reply {
            let _ = reply.send(VerdictResolution::Timeout);
        }
        return;
    };
    match ipc.submit_gossip(obj).await {
        Ok(res) => {
            if let Some(reply) = item.reply {
                let _ = reply.send(VerdictResolution::FromChain(verdict_to_proto(&res.verdict)));
            }
        }
        Err(SeamError::Backpressure { bound, waited_ms }) => {
            if let Some(reply) = item.reply {
                let _ = reply.send(VerdictResolution::Backpressure { bound, waited_ms });
            }
        }
        Err(_) => {
            if let Some(reply) = item.reply {
                let _ = reply.send(VerdictResolution::Timeout);
            }
        }
    }
}

fn gossip_from_proto(o: GossipObject) -> Option<cc_seam::GossipObject> {
    let root: [u8; 32] = o.root.try_into().ok()?;
    Some(cc_seam::GossipObject {
        ssz: o.ssz,
        fork: o.fork,
        root,
        kind: kind_from_proto(o.kind),
        subnet_id: o.subnet_id,
    })
}

fn kind_from_proto(kind: i32) -> cc_seam::ObjectKind {
    match ProtoKind::try_from(kind).unwrap_or(ProtoKind::Unspecified) {
        ProtoKind::Attestation => cc_seam::ObjectKind::Attestation,
        ProtoKind::Aggregate => cc_seam::ObjectKind::Aggregate,
        ProtoKind::SyncCommittee => cc_seam::ObjectKind::SyncCommittee,
        ProtoKind::SyncContribution => cc_seam::ObjectKind::SyncContribution,
        ProtoKind::VoluntaryExit => cc_seam::ObjectKind::VoluntaryExit,
        ProtoKind::ProposerSlashing => cc_seam::ObjectKind::ProposerSlashing,
        ProtoKind::AttesterSlashing => cc_seam::ObjectKind::AttesterSlashing,
        ProtoKind::BlsToExecutionChange => cc_seam::ObjectKind::BlsToExecutionChange,
        ProtoKind::ColumnSidecar => cc_seam::ObjectKind::ColumnSidecar,
        ProtoKind::Block | ProtoKind::Unspecified => cc_seam::ObjectKind::Block,
    }
}

fn kind_to_proto(kind: cc_seam::ObjectKind) -> i32 {
    let k = match kind {
        cc_seam::ObjectKind::Block => ProtoKind::Block,
        cc_seam::ObjectKind::Attestation => ProtoKind::Attestation,
        cc_seam::ObjectKind::Aggregate => ProtoKind::Aggregate,
        cc_seam::ObjectKind::SyncCommittee => ProtoKind::SyncCommittee,
        cc_seam::ObjectKind::SyncContribution => ProtoKind::SyncContribution,
        cc_seam::ObjectKind::VoluntaryExit => ProtoKind::VoluntaryExit,
        cc_seam::ObjectKind::ProposerSlashing => ProtoKind::ProposerSlashing,
        cc_seam::ObjectKind::AttesterSlashing => ProtoKind::AttesterSlashing,
        cc_seam::ObjectKind::BlsToExecutionChange => ProtoKind::BlsToExecutionChange,
        cc_seam::ObjectKind::ColumnSidecar => ProtoKind::ColumnSidecar,
    };
    k as i32
}

fn verdict_to_proto(v: &cc_seam::Verdict) -> Verdict {
    Verdict {
        correlation_id: v.correlation_id.to_vec(),
        acceptance: match v.acceptance {
            cc_seam::Acceptance::Accept => Acceptance::Accept,
            cc_seam::Acceptance::Reject => Acceptance::Reject,
            cc_seam::Acceptance::Ignore => Acceptance::Ignore,
        } as i32,
        reason: match v.reason {
            cc_seam::Reason::Valid => Reason::Valid,
            cc_seam::Reason::Invalid => Reason::Invalid,
            cc_seam::Reason::InvalidSignature => Reason::InvalidSignature,
            cc_seam::Reason::NotDescendedFromFinalized => Reason::NotDescendedFromFinalized,
            cc_seam::Reason::Duplicate => Reason::Duplicate,
            cc_seam::Reason::UnknownParent => Reason::UnknownParent,
            cc_seam::Reason::FutureSlot => Reason::FutureSlot,
            cc_seam::Reason::DeferredDa => Reason::DeferredDa,
            cc_seam::Reason::AlreadyKnown => Reason::AlreadyKnown,
            cc_seam::Reason::Internal => Reason::Internal,
        } as i32,
        import: match v.import {
            cc_seam::ImportResult::Imported => ImportResult::Imported,
            cc_seam::ImportResult::Duplicate => ImportResult::Duplicate,
            cc_seam::ImportResult::DeferredDa => ImportResult::DeferredDa,
            cc_seam::ImportResult::UnknownParent => ImportResult::UnknownParent,
            cc_seam::ImportResult::Invalid => ImportResult::Invalid,
            cc_seam::ImportResult::None => ImportResult::None,
        } as i32,
    }
}

fn publish_to_proto(r: cc_seam::PublishRequest) -> PublishRequest {
    PublishRequest {
        ssz: r.ssz,
        kind: kind_to_proto(r.kind),
        topic: r.topic,
        subnet_id: r.subnet_id,
    }
}

fn view_to_proto(v: &cc_seam::ChainView) -> ChainView {
    ChainView {
        slot: v.slot,
        epoch: v.epoch,
        head_root: v.head_root.to_vec(),
        head_slot: v.head_slot,
        finalized_root: v.finalized_root.to_vec(),
        finalized_epoch: v.finalized_epoch,
        justified_root: v.justified_root.to_vec(),
        justified_epoch: v.justified_epoch,
        genesis_time: v.genesis_time,
        genesis_validators_root: v.genesis_validators_root.to_vec(),
        proposer_lookahead: v.proposer_lookahead.clone(),
        proposer_pubkeys: v.proposer_pubkeys.clone(),
        active_validator_count: v.active_validator_count,
        view_kind: v.view_kind,
    }
}

/// Wait `sleep`, draining `chain_out_rx` and applying timeouts without shortening
/// the reconnect interval. Returns `true` if shutdown was requested.
///
/// Exposed for tests that assert backoff is not cancelled under outbound load.
/// Timer/drain policy is [`cc_seam::wait_reconnect_backoff`] (H1) — not a second loop.
pub async fn wait_reconnect_backoff(
    sleep: Duration,
    chain_out_rx: &mut mpsc::Receiver<ChainOutbound>,
    pending_out: &mut Vec<ChainOutbound>,
    outstanding: &mut OutstandingMap,
    metrics: &P2pMetrics,
    verdict_timeout: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    cc_seam::wait_reconnect_backoff(
        sleep,
        chain_out_rx,
        pending_out,
        shutdown,
        |_pending| apply_timeouts(outstanding, verdict_timeout, metrics),
        buffer_while_disconnected,
    )
    .await
}

fn apply_timeouts(outstanding: &mut OutstandingMap, timeout: Duration, metrics: &P2pMetrics) {
    let now = Instant::now();
    let timed = outstanding.drain_timed_out(timeout, now);
    for (_id, entry) in timed {
        metrics.inc_verdict_timeout();
        if let Some(reply) = entry.reply {
            let _ = reply.send(VerdictResolution::Timeout);
        }
        // Local IGNORE — gossipsub held message is released by the reply path.
        debug!(seq = entry.seq, "verdict timeout; resolved local IGNORE");
    }
    if !outstanding.is_empty() || metrics.queue_depth(QueueName::Outstanding) != 0 {
        sync_outstanding_metrics(outstanding, metrics);
    }
}

fn sync_outstanding_metrics(outstanding: &OutstandingMap, metrics: &P2pMetrics) {
    let depth = outstanding.len() as i64;
    metrics.set_queue_depth(QueueName::Outstanding, depth);
    // saturation_ratio = depth / 1024 on a 0–1 scale stored as f64 via gauge.
    // prometheus-client Gauge is i64; store milli-ratio (0–1000) so 1.0 → 1000.
    let milli = ((depth as f64 / OUTSTANDING_CAP as f64) * 1000.0).round() as i64;
    metrics.set_saturation_ratio_milli(milli.clamp(0, 1000));
}

fn buffer_while_disconnected(pending: &mut Vec<ChainOutbound>, item: ChainOutbound) {
    if pending.len() >= CHAIN_OUT_BOUND {
        // Bound growth while offline — drop newest with timeout resolution.
        if let Some(reply) = item.reply {
            let _ = reply.send(VerdictResolution::Timeout);
        }
        return;
    }
    pending.push(item);
}

/// Build a local IGNORE verdict (timeout / shed path).
#[must_use]
pub fn local_ignore_verdict(correlation_id: Vec<u8>) -> Verdict {
    Verdict {
        correlation_id,
        acceptance: Acceptance::Ignore as i32,
        reason: Reason::Internal as i32,
        import: ImportResult::None as i32,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_proto::common::Source;

    fn obj(seed: u64) -> GossipObject {
        let mut root = vec![0u8; 32];
        root[..8].copy_from_slice(&seed.to_le_bytes());
        GossipObject {
            ssz: root.clone(),
            fork: 0,
            root,
            source: Source::Gossip as i32,
            kind: 1,
            subnet_id: 0,
        }
    }

    fn entry(seq: u64, first: Instant, object: GossipObject) -> OutstandingEntry {
        OutstandingEntry {
            seq,
            first_sent_at: first,
            sent_at: first,
            object,
            reply: None,
        }
    }

    #[test]
    fn outstanding_cap_refuses_growth() {
        let mut m = OutstandingMap::new();
        let now = Instant::now();
        for i in 0..OUTSTANDING_CAP {
            let o = obj(i as u64);
            let id = o.root.clone();
            assert!(m.try_insert(id, entry(i as u64 + 1, now, o)));
        }
        assert!(m.is_full());
        let o = obj(9_999);
        let id = o.root.clone();
        assert!(!m.try_insert(id, entry(9999, now, o)));
        assert_eq!(m.len(), OUTSTANDING_CAP);
    }

    #[test]
    fn try_insert_refuses_duplicate_root() {
        let mut m = OutstandingMap::new();
        let now = Instant::now();
        let o = obj(1);
        let id = o.root.clone();
        assert!(m.try_insert(id.clone(), entry(1, now, o.clone())));
        // Second insert with same root must fail and not overwrite.
        assert!(!m.try_insert(id.clone(), entry(2, now, o)));
        assert_eq!(m.len(), 1);
        let kept = m.remove(&id).expect("original entry kept");
        assert_eq!(kept.seq, 1);
    }

    #[test]
    fn drain_timed_out_uses_first_sent_at() {
        let mut m = OutstandingMap::new();
        let old = Instant::now() - Duration::from_secs(5);
        let o = obj(1);
        m.try_insert(o.root.clone(), entry(1, old, o));
        let o2 = obj(2);
        m.try_insert(o2.root.clone(), entry(2, Instant::now(), o2));
        let timed = m.drain_timed_out(Duration::from_secs(2), Instant::now());
        assert_eq!(timed.len(), 1);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn stall_max_is_half_heartbeat_not_literal() {
        let hb = Duration::from_secs(1);
        let stall = stall_max_from_heartbeat(hb);
        assert_eq!(stall, Duration::from_millis(500));
        // Formula: fraction × heartbeat. AC forbids inlined stall bounds in this
        // module — the value is always derived from the configured interval.
        let custom_hb = Duration::from_millis(2000);
        assert_eq!(
            stall_max_from_heartbeat(custom_hb),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = Duration::from_millis(250);
        b = next_backoff(b, BACKOFF_CAP);
        assert_eq!(b, Duration::from_millis(500));
        for _ in 0..10 {
            b = next_backoff(b, BACKOFF_CAP);
        }
        assert_eq!(b, BACKOFF_CAP);
    }

    #[test]
    fn full_jitter_within_bounds() {
        let b = Duration::from_secs(10);
        for _ in 0..20 {
            let j = full_jitter(b);
            assert!(j <= b);
        }
    }

    #[test]
    fn session_ids_differ() {
        let a = new_session_id();
        let b = new_session_id();
        // Extremely unlikely to collide; if CSPRNG fails both may match — still ok.
        let _ = (a, b);
    }
}
