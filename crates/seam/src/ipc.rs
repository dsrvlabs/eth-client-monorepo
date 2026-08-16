//! Gatehouse & Keep transport: today's tonic-over-TCP edge (`[ARCH]` §2.5).
//!
//! [`Ipc`] is the **p2p-side client** ([`ChainIngress`]). [`IpcEgress`] is a
//! local mailbox for stream-upward publish/view — **not** a tonic write to
//! p2p. Do not give the p2p holder [`P2pEgress`]: view/publish stay
//! chain-owned (`p2p_stream.rs`). Auth is today's unauthenticated TCP;
//! `SO_PEERCRED` is an S3 option, not this wrap.
//!
//! This module is the single session + H1 + outstanding machine. Production
//! `run_chain_stream_client` is a thin adapter (proto ↔ seam) over [`Ipc`].

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use cc_proto::chain::chain_service_client::ChainServiceClient;
use cc_proto::common::Source;
use cc_proto::error_info_from_status;
use cc_proto::p2p::{
    Acceptance as ProtoAcceptance, ChainToP2p, ChainView as ProtoView,
    ColumnSidecar as ProtoColumn, DataAvailable, GossipObject as ProtoGossip,
    ImportResult as ProtoImport, ObjectKind as ProtoKind, P2pToChain,
    PublishRequest as ProtoPublish, Reason as ProtoReason, StreamHello, Verdict as ProtoVerdict,
    chain_to_p2p, p2p_to_chain,
};
use futures::StreamExt;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::transport::Endpoint;
use tracing::{debug, info, warn};

use crate::{
    Acceptance, ChainIngress, ChainView, ColumnSidecar, GossipObject, ImportResult, ObjectKind,
    P2pEgress, PublishRequest, Published, Reason, Root, SeamError, Verdict, VerdictResolution,
};

/// Live import-lane depth (`crates/scheduler/src/config.rs`). Cited for
/// [`SeamError::Backpressure`]; this handle does not stand up a second lane.
pub(crate) const IMPORT_LANE_DEPTH: usize = 64;

/// Live import send deadline (`services/chain/src/core.rs`).
pub(crate) const IMPORT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// SEC-44a-2 payload cap (`services/chain/src/events/mod.rs`).
pub(crate) const MAX_EVENT_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Publish queue (`services/p2p/src/channels.rs`).
pub(crate) const PUBLISH_BOUND: usize = 256;

/// Outbound stream / outstanding cap (`services/p2p/src/channels.rs` `CHAIN_OUT_BOUND`).
pub const CHAIN_OUT_BOUND: usize = 1024;

/// Reconnect backoff initial delay (`services/p2p/src/chain_stream/mod.rs`).
pub const BACKOFF_INITIAL: Duration = Duration::from_millis(250);

/// Reconnect backoff hard cap (`services/p2p/src/chain_stream/mod.rs`).
pub const BACKOFF_CAP: Duration = Duration::from_secs(10);

/// Local shed window inside this impl — not a [`VerdictResolution`].
pub const DEFAULT_VERDICT_TIMEOUT: Duration = Duration::from_secs(2);

/// Known `ErrorInfo.reason` for an un-bootstrapped core.
pub const REASON_NOT_BOOTSTRAPPED: &str = "NOT_BOOTSTRAPPED";

/// Dial / reconnect configuration. Defaults match `ChainStreamConfig`.
#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// gRPC URI for `chain` (e.g. `http://127.0.0.1:9001`).
    pub chain_uri: String,
    /// Outstanding shed window (stays inside this impl).
    pub verdict_timeout: Duration,
    /// Initial reconnect backoff.
    pub backoff_initial: Duration,
    /// Backoff hard cap.
    pub backoff_cap: Duration,
    /// Connect timeout for each dial attempt.
    pub connect_timeout: Duration,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            chain_uri: "http://127.0.0.1:9001".to_owned(),
            verdict_timeout: DEFAULT_VERDICT_TIMEOUT,
            backoff_initial: BACKOFF_INITIAL,
            backoff_cap: BACKOFF_CAP,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// Fresh random session id (reconnect must not reuse the previous incarnation).
#[must_use]
pub fn new_session_id() -> u64 {
    getrandom::u64().unwrap_or_else(|_| {
        Instant::now().elapsed().as_nanos() as u64 ^ u64::from(std::process::id())
    })
}

/// Full-jitter sleep duration in `[0, backoff]` (AWS full jitter).
#[must_use]
pub fn full_jitter(backoff: Duration) -> Duration {
    if backoff.is_zero() {
        return Duration::ZERO;
    }
    let max_ms = backoff.as_millis() as u64;
    let r = getrandom::u64().unwrap_or(0) % max_ms.saturating_add(1);
    Duration::from_millis(r)
}

/// Next backoff after a failed attempt: `min(prev × 2, cap)`.
#[must_use]
pub fn next_backoff(prev: Duration, cap: Duration) -> Duration {
    prev.saturating_mul(2).min(cap)
}

/// Map a tonic status onto [`SeamError`]. `RESOURCE_EXHAUSTED` is Policy A.
#[must_use]
pub fn map_tonic_status(status: &tonic::Status) -> SeamError {
    match status.code() {
        Code::ResourceExhausted => backpressure_import(),
        Code::InvalidArgument => SeamError::InvalidArgument(status.message().to_owned()),
        Code::FailedPrecondition => {
            let reason = error_info_from_status(status)
                .ok()
                .flatten()
                .map(|info| info.reason)
                .unwrap_or_default();
            if reason == REASON_NOT_BOOTSTRAPPED {
                SeamError::FailedPrecondition {
                    reason: crate::FailedPreconditionReason::NotBootstrapped,
                }
            } else {
                SeamError::Unavailable(status.message().to_owned())
            }
        }
        _ => SeamError::Unavailable(status.message().to_owned()),
    }
}

fn backpressure_at(bound: usize) -> SeamError {
    SeamError::Backpressure {
        bound,
        waited_ms: IMPORT_SEND_TIMEOUT.as_millis() as u64,
    }
}

fn backpressure_import() -> SeamError {
    backpressure_at(IMPORT_LANE_DEPTH)
}

fn backpressure_out() -> SeamError {
    backpressure_at(CHAIN_OUT_BOUND)
}

/// Upward / metric hooks for a production adapter. Defaults are no-ops.
pub trait IpcUpward: Send + Sync + 'static {
    fn on_view(&self, _view: ChainView) {}
    fn on_publish(&self, _req: PublishRequest) {}
    fn on_verdict(&self, _verdict: Verdict, _latency: Duration) {}
    fn on_stray_verdict(&self, _verdict: Verdict) {}
    fn on_object_sent(&self) {}
    fn on_timeout(&self) {}
    fn on_outstanding(&self, _depth: usize) {}
}

struct NoopUpward;

impl IpcUpward for NoopUpward {}

/// p2p-side client. [`ChainIngress`] only — never a [`P2pEgress`] write.
#[derive(Debug, Clone)]
pub struct Ipc {
    out_tx: mpsc::Sender<IpcOut>,
}

/// Local apply of stream-upward publish/view. Not the two-process egress
/// (`p2p_stream.rs`). Do not hand this to p2p as chain-owned [`P2pEgress`].
#[derive(Debug, Clone)]
pub struct IpcEgress {
    publish_tx: mpsc::Sender<PublishRequest>,
    view: Arc<ArcSwap<ChainView>>,
}

/// Receive side for stream-upward publish / the latest view.
#[derive(Debug)]
pub struct IpcMailbox {
    pub publish_rx: mpsc::Receiver<PublishRequest>,
    view: Arc<ArcSwap<ChainView>>,
}

impl IpcMailbox {
    /// Pointer load — never blocks.
    #[must_use]
    pub fn load_view(&self) -> Arc<ChainView> {
        self.view.load_full()
    }

    /// Shared store identity for extra readers (slot clock, Status).
    #[must_use]
    pub fn view_store(&self) -> Arc<ArcSwap<ChainView>> {
        Arc::clone(&self.view)
    }
}

impl Ipc {
    /// Client + local egress mailbox + reconnect task. Caller spawns the future.
    pub fn connect(
        cfg: IpcConfig,
        shutdown: watch::Receiver<bool>,
    ) -> (Self, IpcEgress, IpcMailbox, impl Future<Output = ()> + Send) {
        Self::connect_with(cfg, shutdown, Arc::new(NoopUpward))
    }

    /// Like [`Self::connect`], with production upward hooks (view / publish / metrics).
    pub fn connect_with(
        cfg: IpcConfig,
        shutdown: watch::Receiver<bool>,
        upward: Arc<dyn IpcUpward>,
    ) -> (Self, IpcEgress, IpcMailbox, impl Future<Output = ()> + Send) {
        let (out_tx, out_rx) = mpsc::channel(CHAIN_OUT_BOUND);
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_BOUND);
        let view = Arc::new(ArcSwap::from_pointee(ChainView::default()));
        let ipc = Self { out_tx };
        let egress = IpcEgress {
            publish_tx: publish_tx.clone(),
            view: Arc::clone(&view),
        };
        let mailbox = IpcMailbox {
            publish_rx,
            view: Arc::clone(&view),
        };
        let task = run_ipc_loop(cfg, out_rx, publish_tx, view, shutdown, upward);
        (ipc, egress, mailbox, task)
    }

    /// Remaining `out_tx` slots. Policy A occupancy is `capacity() == 0`
    /// at [`CHAIN_OUT_BOUND`] — Ipc's send-side bound, not Loop B's 64.
    #[cfg(test)]
    pub(crate) fn out_lane_capacity(&self) -> usize {
        self.out_tx.capacity()
    }

    /// Occupy every `out_tx` slot without parking a send-timeout waiter.
    /// Paused `submit_gossip` waiters sit on `IMPORT_SEND_TIMEOUT` and
    /// auto-advance the clock before overflow. `Full` means already full;
    /// `Closed` cannot satisfy occupancy.
    #[cfg(test)]
    pub(crate) fn fill_out_lane(&self) {
        for _ in 0..CHAIN_OUT_BOUND {
            let (reply, _rx) = oneshot::channel();
            match self.out_tx.try_send(IpcOut::Gossip {
                obj: GossipObject {
                    ssz: Vec::new(),
                    fork: 0,
                    root: [0; 32],
                    kind: ObjectKind::Block,
                    subnet_id: 0,
                },
                reply,
                enqueued_at: Instant::now(),
            }) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => {
                    assert!(
                        !self.out_tx.is_closed(),
                        "ipc out_tx closed; cannot occupy CHAIN_OUT_BOUND"
                    );
                }
            }
        }
        assert_eq!(
            self.out_tx.max_capacity(),
            CHAIN_OUT_BOUND,
            "Ipc Policy A send bound is CHAIN_OUT_BOUND"
        );
        assert_eq!(
            self.out_tx.capacity(),
            0,
            "Ipc send path must be full at CHAIN_OUT_BOUND before the deadline waiter"
        );
    }
}

enum IpcOut {
    Gossip {
        obj: GossipObject,
        reply: oneshot::Sender<Result<VerdictResolution, SeamError>>,
        enqueued_at: Instant,
    },
    DataAvailable {
        root: Root,
        slot: u64,
        reply: Option<oneshot::Sender<Result<(), SeamError>>>,
        enqueued_at: Instant,
    },
    Column {
        sidecar: ColumnSidecar,
        reply: oneshot::Sender<Result<(), SeamError>>,
        enqueued_at: Instant,
    },
}

fn enqueued_at(item: &IpcOut) -> Instant {
    match item {
        IpcOut::Gossip { enqueued_at, .. }
        | IpcOut::DataAvailable { enqueued_at, .. }
        | IpcOut::Column { enqueued_at, .. } => *enqueued_at,
    }
}

struct OutstandingEntry {
    first_sent_at: Instant,
    sent_at: Instant,
    object: GossipObject,
    reply: Option<oneshot::Sender<Result<VerdictResolution, SeamError>>>,
}

/// Gossip waiter. Column ACKs reuse the block root on the wire, so the key
/// must include kind — a column IGNORE must not complete a Block import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct GossipKey {
    kind: ObjectKind,
    root: Root,
}

impl GossipKey {
    fn of(obj: &GossipObject) -> Self {
        Self {
            kind: obj.kind,
            root: obj.root,
        }
    }
}

const NON_BLOCK_KINDS: [ObjectKind; 9] = [
    ObjectKind::Attestation,
    ObjectKind::Aggregate,
    ObjectKind::SyncCommittee,
    ObjectKind::SyncContribution,
    ObjectKind::VoluntaryExit,
    ObjectKind::ProposerSlashing,
    ObjectKind::AttesterSlashing,
    ObjectKind::BlsToExecutionChange,
    ObjectKind::ColumnSidecar,
];

#[derive(Default)]
struct OutstandingMap {
    map: HashMap<GossipKey, OutstandingEntry>,
}

impl OutstandingMap {
    fn len(&self) -> usize {
        self.map.len()
    }

    fn is_full(&self) -> bool {
        self.map.len() >= CHAIN_OUT_BOUND
    }

    fn contains(&self, id: &GossipKey) -> bool {
        self.map.contains_key(id)
    }

    fn try_insert(&mut self, id: GossipKey, entry: OutstandingEntry) -> bool {
        if self.map.contains_key(&id) || self.is_full() {
            return false;
        }
        self.map.insert(id, entry);
        true
    }

    fn drain_timed_out(&mut self, timeout: Duration, now: Instant) -> Vec<OutstandingEntry> {
        let keys: Vec<GossipKey> = self
            .map
            .iter()
            .filter(|(_, e)| now.duration_since(e.first_sent_at) >= timeout)
            .map(|(k, _)| *k)
            .collect();
        keys.into_iter()
            .filter_map(|k| self.map.remove(&k))
            .collect()
    }

    fn take_all_for_resend(&mut self) -> Vec<OutstandingEntry> {
        self.map.drain().map(|(_, e)| e).collect()
    }

    /// Column ACK (Ignore/AlreadyKnown/None) — never a Block import waiter.
    fn remove_non_block(&mut self, root: Root) -> Option<OutstandingEntry> {
        for kind in NON_BLOCK_KINDS {
            if let Some(entry) = self.map.remove(&GossipKey { kind, root }) {
                return Some(entry);
            }
        }
        None
    }

    fn remove_prefer_block(&mut self, root: Root) -> Option<OutstandingEntry> {
        if let Some(entry) = self.map.remove(&GossipKey {
            kind: ObjectKind::Block,
            root,
        }) {
            return Some(entry);
        }
        self.remove_non_block(root)
    }
}

enum SessionEnd {
    Shutdown,
    Disconnected,
}

async fn run_ipc_loop(
    cfg: IpcConfig,
    mut out_rx: mpsc::Receiver<IpcOut>,
    publish_tx: mpsc::Sender<PublishRequest>,
    view: Arc<ArcSwap<ChainView>>,
    mut shutdown: watch::Receiver<bool>,
    upward: Arc<dyn IpcUpward>,
) {
    let mut outstanding = OutstandingMap::default();
    let mut backoff = cfg.backoff_initial;
    let mut pending: Vec<IpcOut> = Vec::new();

    loop {
        if *shutdown.borrow() {
            fail_all(&mut outstanding, "shutdown", upward.as_ref());
            fail_pending(&mut pending, SeamError::Unavailable("shutdown".into()));
            return;
        }

        match connect_and_run_session(
            &cfg,
            &mut out_rx,
            &publish_tx,
            &view,
            &mut outstanding,
            &mut pending,
            &mut shutdown,
            &mut backoff,
            upward.as_ref(),
        )
        .await
        {
            SessionEnd::Shutdown => {
                fail_all(&mut outstanding, "shutdown", upward.as_ref());
                fail_pending(&mut pending, SeamError::Unavailable("shutdown".into()));
                return;
            }
            SessionEnd::Disconnected => {
                info!(
                    outstanding = outstanding.len(),
                    backoff_ms = backoff.as_millis() as u64,
                    "ipc chain stream disconnected; reconnecting with backoff"
                );
                let sleep = full_jitter(backoff);
                backoff = next_backoff(backoff, cfg.backoff_cap);
                if wait_reconnect_backoff(
                    sleep,
                    &mut out_rx,
                    &mut pending,
                    &mut shutdown,
                    |pending| {
                        apply_timeouts(&mut outstanding, cfg.verdict_timeout, upward.as_ref());
                        apply_pending_timeouts(pending, cfg.verdict_timeout);
                    },
                    buffer_while_disconnected,
                )
                .await
                {
                    fail_all(&mut outstanding, "shutdown", upward.as_ref());
                    fail_pending(&mut pending, SeamError::Unavailable("shutdown".into()));
                    return;
                }
            }
        }
    }
}

/// H1: wait `sleep` without cancelling under outbound load. Returns `true`
/// on shutdown. Live `client.rs` calls this — one timer policy, not two.
pub async fn wait_reconnect_backoff<T>(
    sleep: Duration,
    out_rx: &mut mpsc::Receiver<T>,
    pending: &mut Vec<T>,
    shutdown: &mut watch::Receiver<bool>,
    mut on_tick: impl FnMut(&mut Vec<T>),
    mut on_msg: impl FnMut(&mut Vec<T>, T),
) -> bool {
    let deadline = Instant::now() + sleep;
    let mut out_open = true;
    loop {
        on_tick(pending);
        if out_open {
            loop {
                match out_rx.try_recv() {
                    Ok(msg) => on_msg(pending, msg),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        out_open = false;
                        break;
                    }
                }
            }
        }
        if *shutdown.borrow() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline - now;
        if out_open {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return true;
                    }
                }
                () = tokio::time::sleep(remaining) => {
                    return false;
                }
                msg = out_rx.recv() => {
                    match msg {
                        Some(m) => on_msg(pending, m),
                        None => out_open = false,
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return true;
                    }
                }
                () = tokio::time::sleep(remaining) => {
                    return false;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_run_session(
    cfg: &IpcConfig,
    out_rx: &mut mpsc::Receiver<IpcOut>,
    publish_tx: &mpsc::Sender<PublishRequest>,
    view: &Arc<ArcSwap<ChainView>>,
    outstanding: &mut OutstandingMap,
    pending: &mut Vec<IpcOut>,
    shutdown: &mut watch::Receiver<bool>,
    backoff: &mut Duration,
    upward: &dyn IpcUpward,
) -> SessionEnd {
    let endpoint = match Endpoint::from_shared(cfg.chain_uri.clone()) {
        Ok(e) => e
            .connect_timeout(cfg.connect_timeout)
            .timeout(Duration::from_secs(30)),
        Err(e) => {
            warn!(error = %e, uri = %cfg.chain_uri, "invalid chain URI");
            return SessionEnd::Disconnected;
        }
    };

    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "ipc chain stream dial failed");
            return SessionEnd::Disconnected;
        }
    };

    let mut client = ChainServiceClient::new(channel);
    let (stream_tx, stream_rx) = mpsc::channel::<P2pToChain>(CHAIN_OUT_BOUND);
    let response = match client.p2p_stream(ReceiverStream::new(stream_rx)).await {
        Ok(r) => r,
        Err(status) => {
            warn!(error = %status, "P2pStream open failed");
            if status.code() == Code::ResourceExhausted {
                fail_all_err(outstanding, map_tonic_status(&status), upward);
            }
            return SessionEnd::Disconnected;
        }
    };
    let mut inbound = response.into_inner();

    let session_id = new_session_id();
    let mut out_seq: u64 = 0;
    out_seq = out_seq.saturating_add(1);
    if stream_tx
        .send(P2pToChain {
            seq: out_seq,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id,
                resume_seq: 0,
            })),
        })
        .await
        .is_err()
    {
        return SessionEnd::Disconnected;
    }
    *backoff = cfg.backoff_initial;
    info!(
        session_id,
        "ipc chain stream session opened (StreamHello sent)"
    );

    let mut to_resend = outstanding.take_all_for_resend().into_iter();
    while let Some(mut entry) = to_resend.next() {
        out_seq = out_seq.saturating_add(1);
        let id = GossipKey::of(&entry.object);
        let wire = P2pToChain {
            seq: out_seq,
            msg: Some(p2p_to_chain::Msg::Object(gossip_to_proto(&entry.object))),
        };
        if stream_tx.send(wire).await.is_err() {
            requeue_outstanding(outstanding, entry, to_resend);
            return SessionEnd::Disconnected;
        }
        entry.sent_at = Instant::now();
        let _ = outstanding.try_insert(id, entry);
    }
    upward.on_outstanding(outstanding.len());

    let mut buffered = std::mem::take(pending).into_iter();
    while let Some(item) = buffered.next() {
        if let Err(failed) = send_out(item, &mut out_seq, &stream_tx, outstanding, upward).await {
            pending.push(failed);
            pending.extend(buffered);
            return SessionEnd::Disconnected;
        }
    }

    let mut timeout_tick = tokio::time::interval(Duration::from_millis(100));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return SessionEnd::Shutdown;
                }
            }
            _ = timeout_tick.tick() => {
                apply_timeouts(outstanding, cfg.verdict_timeout, upward);
                apply_pending_timeouts(pending, cfg.verdict_timeout);
            }
            msg = inbound.next() => {
                match msg {
                    None => return SessionEnd::Disconnected,
                    Some(Err(status)) => {
                        warn!(error = %status, "ipc chain stream inbound error");
                        // Live import-lane overflow is RESOURCE_EXHAUSTED.
                        if status.code() == Code::ResourceExhausted {
                            fail_all_err(outstanding, map_tonic_status(&status), upward);
                        }
                        return SessionEnd::Disconnected;
                    }
                    Some(Ok(msg)) => {
                        handle_upward(msg, outstanding, view, publish_tx, upward);
                    }
                }
            }
            item = out_rx.recv() => {
                match item {
                    None => {
                        debug!("ipc out channel closed; session stays up for views");
                        return receive_only_loop(
                            cfg,
                            &mut inbound,
                            outstanding,
                            view,
                            publish_tx,
                            shutdown,
                            upward,
                        )
                        .await;
                    }
                    Some(item) => {
                        if let Err(failed) =
                            send_out(item, &mut out_seq, &stream_tx, outstanding, upward).await
                        {
                            pending.push(failed);
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
        }
    }
}

async fn receive_only_loop(
    cfg: &IpcConfig,
    inbound: &mut (impl StreamExt<Item = Result<ChainToP2p, tonic::Status>> + Unpin),
    outstanding: &mut OutstandingMap,
    view: &Arc<ArcSwap<ChainView>>,
    publish_tx: &mpsc::Sender<PublishRequest>,
    shutdown: &mut watch::Receiver<bool>,
    upward: &dyn IpcUpward,
) -> SessionEnd {
    let mut timeout_tick = tokio::time::interval(Duration::from_millis(100));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return SessionEnd::Shutdown;
                }
            }
            _ = timeout_tick.tick() => {
                apply_timeouts(outstanding, cfg.verdict_timeout, upward);
            }
            msg = inbound.next() => {
                match msg {
                    None => return SessionEnd::Disconnected,
                    Some(Err(status)) => {
                        if status.code() == Code::ResourceExhausted {
                            fail_all_err(outstanding, map_tonic_status(&status), upward);
                        }
                        return SessionEnd::Disconnected;
                    }
                    Some(Ok(msg)) => handle_upward(msg, outstanding, view, publish_tx, upward),
                }
            }
        }
    }
}

async fn send_out(
    item: IpcOut,
    out_seq: &mut u64,
    stream_tx: &mpsc::Sender<P2pToChain>,
    outstanding: &mut OutstandingMap,
    upward: &dyn IpcUpward,
) -> Result<(), IpcOut> {
    match item {
        IpcOut::Gossip {
            obj,
            reply,
            enqueued_at,
        } => {
            let id = GossipKey::of(&obj);
            if outstanding.contains(&id) || outstanding.is_full() {
                let _ = reply.send(Err(SeamError::Unavailable(
                    "duplicate or outstanding at cap; shed inside Ipc".into(),
                )));
                return Ok(());
            }
            *out_seq = out_seq.saturating_add(1);
            let wire = P2pToChain {
                seq: *out_seq,
                msg: Some(p2p_to_chain::Msg::Object(gossip_to_proto(&obj))),
            };
            if stream_tx.send(wire).await.is_err() {
                return Err(IpcOut::Gossip {
                    obj,
                    reply,
                    enqueued_at,
                });
            }
            upward.on_object_sent();
            let entry = OutstandingEntry {
                first_sent_at: enqueued_at,
                sent_at: Instant::now(),
                object: obj,
                reply: Some(reply),
            };
            let _ = outstanding.try_insert(id, entry);
            upward.on_outstanding(outstanding.len());
            Ok(())
        }
        IpcOut::DataAvailable {
            root,
            slot,
            reply,
            enqueued_at,
        } => {
            *out_seq = out_seq.saturating_add(1);
            let wire = P2pToChain {
                seq: *out_seq,
                msg: Some(p2p_to_chain::Msg::DataAvailable(DataAvailable {
                    root: root.to_vec(),
                    slot,
                })),
            };
            if stream_tx.send(wire).await.is_err() {
                return Err(IpcOut::DataAvailable {
                    root,
                    slot,
                    reply,
                    enqueued_at,
                });
            }
            if let Some(reply) = reply {
                let _ = reply.send(Ok(()));
            }
            Ok(())
        }
        IpcOut::Column {
            sidecar,
            reply,
            enqueued_at,
        } => {
            *out_seq = out_seq.saturating_add(1);
            let wire = P2pToChain {
                seq: *out_seq,
                msg: Some(p2p_to_chain::Msg::Column(column_to_proto(&sidecar))),
            };
            if stream_tx.send(wire).await.is_err() {
                return Err(IpcOut::Column {
                    sidecar,
                    reply,
                    enqueued_at,
                });
            }
            let _ = reply.send(Ok(()));
            Ok(())
        }
    }
}

fn handle_upward(
    msg: ChainToP2p,
    outstanding: &mut OutstandingMap,
    view: &Arc<ArcSwap<ChainView>>,
    publish_tx: &mpsc::Sender<PublishRequest>,
    upward: &dyn IpcUpward,
) {
    let Some(inner) = msg.msg else {
        return;
    };
    match inner {
        chain_to_p2p::Msg::Verdict(verdict) => apply_verdict(verdict, outstanding, upward),
        chain_to_p2p::Msg::Publish(req) => {
            let req = publish_from_proto(req);
            upward.on_publish(req.clone());
            // Policy C lives on [`IpcEgress::publish`]; full or closed: drop.
            let _ = publish_tx.try_send(req);
        }
        chain_to_p2p::Msg::View(proto) => {
            let v = view_from_proto(proto);
            view.store(Arc::new(v.clone()));
            upward.on_view(v);
        }
    }
}

fn is_column_ack(acceptance: ProtoAcceptance, reason: ProtoReason, import: ProtoImport) -> bool {
    matches!(
        (acceptance, reason, import),
        (
            ProtoAcceptance::Ignore,
            ProtoReason::AlreadyKnown,
            ProtoImport::None
        )
    )
}

fn apply_verdict(verdict: ProtoVerdict, outstanding: &mut OutstandingMap, upward: &dyn IpcUpward) {
    let Some(root) = root32(&verdict.correlation_id) else {
        return;
    };
    let acceptance =
        ProtoAcceptance::try_from(verdict.acceptance).unwrap_or(ProtoAcceptance::Unspecified);
    let reason = ProtoReason::try_from(verdict.reason).unwrap_or(ProtoReason::Unspecified);
    let import = ProtoImport::try_from(verdict.import).unwrap_or(ProtoImport::Unspecified);
    // Column ACK is Ignore/AlreadyKnown/None on the *block* root. Outstanding
    // is keyed by (kind, root); that ACK must not complete a Block waiter.
    let entry = if is_column_ack(acceptance, reason, import) {
        outstanding.remove_non_block(root)
    } else {
        outstanding.remove_prefer_block(root)
    };
    let Some(entry) = entry else {
        upward.on_stray_verdict(verdict_from_proto(verdict));
        return;
    };
    let latency = entry.sent_at.elapsed();
    upward.on_outstanding(outstanding.len());
    // Today's server maps import-lane RESOURCE_EXHAUSTED → this verdict shape.
    if matches!(
        (acceptance, reason, import),
        (
            ProtoAcceptance::Ignore,
            ProtoReason::Internal,
            ProtoImport::Invalid
        )
    ) {
        // Count as timeout so sent == verdicts + timeouts still holds.
        upward.on_timeout();
        if let Some(reply) = entry.reply {
            let _ = reply.send(Err(backpressure_import()));
        }
        return;
    }
    let seam_v = verdict_from_proto(verdict);
    upward.on_verdict(seam_v.clone(), latency);
    if let Some(reply) = entry.reply {
        let _ = reply.send(Ok(VerdictResolution { verdict: seam_v }));
    }
}

fn apply_timeouts(outstanding: &mut OutstandingMap, timeout: Duration, upward: &dyn IpcUpward) {
    for entry in outstanding.drain_timed_out(timeout, Instant::now()) {
        upward.on_timeout();
        if let Some(reply) = entry.reply {
            let _ = reply.send(Err(backpressure_out()));
        }
    }
    upward.on_outstanding(outstanding.len());
}

fn apply_pending_timeouts(pending: &mut Vec<IpcOut>, timeout: Duration) {
    let now = Instant::now();
    let mut i = 0;
    while i < pending.len() {
        if now.duration_since(enqueued_at(&pending[i])) >= timeout {
            fail_item(pending.remove(i), backpressure_out());
        } else {
            i += 1;
        }
    }
}

fn requeue_outstanding(
    outstanding: &mut OutstandingMap,
    entry: OutstandingEntry,
    tail: impl IntoIterator<Item = OutstandingEntry>,
) {
    let id = GossipKey::of(&entry.object);
    let _ = outstanding.try_insert(id, entry);
    for rest in tail {
        let id = GossipKey::of(&rest.object);
        let _ = outstanding.try_insert(id, rest);
    }
}

fn fail_item(item: IpcOut, err: SeamError) {
    match item {
        IpcOut::Gossip { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        IpcOut::Column { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        IpcOut::DataAvailable { reply, .. } => {
            if let Some(reply) = reply {
                let _ = reply.send(Err(err));
            }
        }
    }
}

fn fail_all(outstanding: &mut OutstandingMap, why: &str, upward: &dyn IpcUpward) {
    fail_all_err(outstanding, SeamError::Unavailable(why.into()), upward);
}

fn fail_all_err(outstanding: &mut OutstandingMap, err: SeamError, upward: &dyn IpcUpward) {
    let count_timeout = matches!(err, SeamError::Unavailable(_));
    for entry in outstanding.take_all_for_resend() {
        if count_timeout {
            upward.on_timeout();
        }
        if let Some(reply) = entry.reply {
            let _ = reply.send(Err(err.clone()));
        }
    }
    upward.on_outstanding(0);
}

fn fail_pending(pending: &mut Vec<IpcOut>, err: SeamError) {
    for item in pending.drain(..) {
        fail_item(item, err.clone());
    }
}

fn buffer_while_disconnected(pending: &mut Vec<IpcOut>, item: IpcOut) {
    if pending.len() >= CHAIN_OUT_BOUND {
        fail_item(item, backpressure_out());
        return;
    }
    pending.push(item);
}

fn gossip_to_proto(obj: &GossipObject) -> ProtoGossip {
    // Provenance is stamped here — the caller cannot smuggle API / trusted_local.
    ProtoGossip {
        ssz: obj.ssz.clone(),
        fork: obj.fork,
        root: obj.root.to_vec(),
        source: Source::Gossip as i32,
        kind: object_kind_to_proto(obj.kind) as i32,
        subnet_id: obj.subnet_id,
    }
}

fn column_to_proto(col: &ColumnSidecar) -> ProtoColumn {
    ProtoColumn {
        ssz: col.ssz.clone(),
        fork: col.fork,
        root: col.root.to_vec(),
        column_index: col.column_index,
        subnet_id: col.subnet_id,
    }
}

fn object_kind_to_proto(kind: ObjectKind) -> ProtoKind {
    match kind {
        ObjectKind::Block => ProtoKind::Block,
        ObjectKind::Attestation => ProtoKind::Attestation,
        ObjectKind::Aggregate => ProtoKind::Aggregate,
        ObjectKind::SyncCommittee => ProtoKind::SyncCommittee,
        ObjectKind::SyncContribution => ProtoKind::SyncContribution,
        ObjectKind::VoluntaryExit => ProtoKind::VoluntaryExit,
        ObjectKind::ProposerSlashing => ProtoKind::ProposerSlashing,
        ObjectKind::AttesterSlashing => ProtoKind::AttesterSlashing,
        ObjectKind::BlsToExecutionChange => ProtoKind::BlsToExecutionChange,
        ObjectKind::ColumnSidecar => ProtoKind::ColumnSidecar,
    }
}

fn object_kind_from_proto(kind: i32) -> ObjectKind {
    match ProtoKind::try_from(kind).unwrap_or(ProtoKind::Unspecified) {
        ProtoKind::Attestation => ObjectKind::Attestation,
        ProtoKind::Aggregate => ObjectKind::Aggregate,
        ProtoKind::SyncCommittee => ObjectKind::SyncCommittee,
        ProtoKind::SyncContribution => ObjectKind::SyncContribution,
        ProtoKind::VoluntaryExit => ObjectKind::VoluntaryExit,
        ProtoKind::ProposerSlashing => ObjectKind::ProposerSlashing,
        ProtoKind::AttesterSlashing => ObjectKind::AttesterSlashing,
        ProtoKind::BlsToExecutionChange => ObjectKind::BlsToExecutionChange,
        ProtoKind::ColumnSidecar => ObjectKind::ColumnSidecar,
        ProtoKind::Block | ProtoKind::Unspecified => ObjectKind::Block,
    }
}

fn verdict_from_proto(v: ProtoVerdict) -> Verdict {
    Verdict {
        correlation_id: root32(&v.correlation_id).unwrap_or([0; 32]),
        acceptance: match ProtoAcceptance::try_from(v.acceptance).unwrap_or(ProtoAcceptance::Ignore)
        {
            ProtoAcceptance::Accept => Acceptance::Accept,
            ProtoAcceptance::Reject => Acceptance::Reject,
            ProtoAcceptance::Ignore | ProtoAcceptance::Unspecified => Acceptance::Ignore,
        },
        reason: match ProtoReason::try_from(v.reason).unwrap_or(ProtoReason::Internal) {
            ProtoReason::Valid => Reason::Valid,
            ProtoReason::Invalid => Reason::Invalid,
            ProtoReason::InvalidSignature => Reason::InvalidSignature,
            ProtoReason::NotDescendedFromFinalized => Reason::NotDescendedFromFinalized,
            ProtoReason::Duplicate => Reason::Duplicate,
            ProtoReason::UnknownParent => Reason::UnknownParent,
            ProtoReason::FutureSlot => Reason::FutureSlot,
            ProtoReason::DeferredDa => Reason::DeferredDa,
            ProtoReason::AlreadyKnown => Reason::AlreadyKnown,
            ProtoReason::Internal | ProtoReason::Unspecified => Reason::Internal,
        },
        import: match ProtoImport::try_from(v.import).unwrap_or(ProtoImport::None) {
            ProtoImport::Imported => ImportResult::Imported,
            ProtoImport::Duplicate => ImportResult::Duplicate,
            ProtoImport::DeferredDa => ImportResult::DeferredDa,
            ProtoImport::UnknownParent => ImportResult::UnknownParent,
            ProtoImport::Invalid => ImportResult::Invalid,
            ProtoImport::None | ProtoImport::Unspecified => ImportResult::None,
        },
    }
}

fn publish_from_proto(req: ProtoPublish) -> PublishRequest {
    PublishRequest {
        ssz: req.ssz,
        kind: object_kind_from_proto(req.kind),
        topic: req.topic,
        subnet_id: req.subnet_id,
    }
}

fn view_from_proto(v: ProtoView) -> ChainView {
    ChainView {
        slot: v.slot,
        epoch: v.epoch,
        head_root: root32(&v.head_root).unwrap_or([0; 32]),
        head_slot: v.head_slot,
        finalized_root: root32(&v.finalized_root).unwrap_or([0; 32]),
        finalized_epoch: v.finalized_epoch,
        justified_root: root32(&v.justified_root).unwrap_or([0; 32]),
        justified_epoch: v.justified_epoch,
        genesis_time: v.genesis_time,
        genesis_validators_root: root32(&v.genesis_validators_root).unwrap_or([0; 32]),
        proposer_lookahead: v.proposer_lookahead,
        proposer_pubkeys: v.proposer_pubkeys,
        active_validator_count: v.active_validator_count,
        view_kind: v.view_kind,
    }
}

fn root32(bytes: &[u8]) -> Option<Root> {
    bytes.try_into().ok()
}

async fn enqueue(tx: &mpsc::Sender<IpcOut>, msg: IpcOut) -> Result<(), SeamError> {
    match tx.send_timeout(msg, IMPORT_SEND_TIMEOUT).await {
        Ok(()) => Ok(()),
        Err(SendTimeoutError::Timeout(_)) => Err(backpressure_out()),
        Err(SendTimeoutError::Closed(_)) => {
            Err(SeamError::Unavailable("ipc reconnect loop gone".into()))
        }
    }
}

/// Cap the oneshot so a stuck dial / `p2p_stream` / `send` cannot exceed the 2 s budget.
async fn await_until_deadline<T>(
    rx: oneshot::Receiver<Result<T, SeamError>>,
    enqueued_at: Instant,
) -> Result<T, SeamError> {
    let remaining = IMPORT_SEND_TIMEOUT.saturating_sub(enqueued_at.elapsed());
    match tokio::time::timeout(remaining, rx).await {
        Ok(Ok(inner)) => inner,
        Ok(Err(_)) => Err(SeamError::Unavailable("ipc reply dropped".into())),
        Err(_) => Err(backpressure_out()),
    }
}

#[async_trait]
impl ChainIngress for Ipc {
    async fn submit_gossip(&self, obj: GossipObject) -> Result<VerdictResolution, SeamError> {
        let (reply, rx) = oneshot::channel();
        let enqueued_at = Instant::now();
        enqueue(
            &self.out_tx,
            IpcOut::Gossip {
                obj,
                reply,
                enqueued_at,
            },
        )
        .await?;
        await_until_deadline(rx, enqueued_at).await
    }

    async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), SeamError> {
        let (reply, rx) = oneshot::channel();
        let enqueued_at = Instant::now();
        enqueue(
            &self.out_tx,
            IpcOut::DataAvailable {
                root,
                slot,
                reply: Some(reply),
                enqueued_at,
            },
        )
        .await?;
        await_until_deadline(rx, enqueued_at).await
    }

    async fn submit_column_sidecar(&self, sidecar: ColumnSidecar) -> Result<(), SeamError> {
        if sidecar.ssz.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Err(SeamError::InvalidArgument(format!(
                "column sidecar payload {} exceeds {MAX_EVENT_PAYLOAD_BYTES}",
                sidecar.ssz.len()
            )));
        }
        let (reply, rx) = oneshot::channel();
        let enqueued_at = Instant::now();
        enqueue(
            &self.out_tx,
            IpcOut::Column {
                sidecar,
                reply,
                enqueued_at,
            },
        )
        .await?;
        await_until_deadline(rx, enqueued_at).await
    }
}

#[async_trait]
impl P2pEgress for IpcEgress {
    async fn publish(&self, req: PublishRequest) -> Result<Published, SeamError> {
        match self.publish_tx.try_send(req) {
            Ok(()) => Ok(Published::Queued),
            Err(TrySendError::Full(_)) => Ok(Published::Dropped),
            Err(TrySendError::Closed(_)) => {
                Err(SeamError::Unavailable("publish queue closed".into()))
            }
        }
    }

    fn update_view(&self, view: ChainView) {
        self.view.store(Arc::new(view));
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::ObjectKind;
    use cc_proto::chain::chain_service_server::{ChainService, ChainServiceServer};
    use std::net::SocketAddr;
    use std::pin::Pin;
    use tokio::sync::mpsc as tokio_mpsc;
    use tonic::transport::Server;

    type BoxStreamChainToP2p =
        Pin<Box<dyn futures::Stream<Item = Result<ChainToP2p, tonic::Status>> + Send + 'static>>;

    fn gossip(root: Root) -> GossipObject {
        GossipObject {
            ssz: root.to_vec(),
            fork: 0,
            root,
            kind: ObjectKind::Block,
            subnet_id: 0,
        }
    }

    fn publish_req(topic: &str) -> PublishRequest {
        PublishRequest {
            ssz: Vec::new(),
            kind: ObjectKind::Block,
            topic: topic.to_owned(),
            subnet_id: 0,
        }
    }

    fn sidecar(ssz: Vec<u8>) -> ColumnSidecar {
        ColumnSidecar {
            ssz,
            fork: 0,
            root: [0; 32],
            column_index: 0,
            subnet_id: 0,
        }
    }

    #[derive(Debug, Default)]
    struct EchoChain;

    #[tonic::async_trait]
    impl ChainService for EchoChain {
        async fn p2p_stream(
            &self,
            request: tonic::Request<tonic::Streaming<P2pToChain>>,
        ) -> Result<tonic::Response<BoxStreamChainToP2p>, tonic::Status> {
            let mut inbound = request.into_inner();
            let (tx, rx) = tokio_mpsc::channel(16);
            tokio::spawn(async move {
                let mut seq = 0u64;
                while let Some(Ok(msg)) = inbound.next().await {
                    match msg.msg {
                        Some(p2p_to_chain::Msg::Hello(_)) => {
                            seq += 1;
                            let _ = tx
                                .send(Ok(ChainToP2p {
                                    seq,
                                    msg: Some(chain_to_p2p::Msg::View(ProtoView {
                                        slot: 1,
                                        view_kind: 4,
                                        genesis_time: 1,
                                        ..Default::default()
                                    })),
                                }))
                                .await;
                        }
                        Some(p2p_to_chain::Msg::Object(obj)) => {
                            seq += 1;
                            let _ = tx
                                .send(Ok(ChainToP2p {
                                    seq,
                                    msg: Some(chain_to_p2p::Msg::Verdict(ProtoVerdict {
                                        correlation_id: obj.root,
                                        acceptance: ProtoAcceptance::Accept as i32,
                                        reason: ProtoReason::Valid as i32,
                                        import: ProtoImport::Imported as i32,
                                    })),
                                }))
                                .await;
                        }
                        _ => {}
                    }
                }
            });
            Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
        }
    }

    async fn spawn_echo_grpc() -> (SocketAddr, oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = Server::builder()
                .add_service(ChainServiceServer::new(EchoChain))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        (addr, shutdown_tx)
    }

    #[test]
    fn map_resource_exhausted_is_backpressure() {
        let err = map_tonic_status(&tonic::Status::resource_exhausted("import lane full"));
        assert_eq!(err, backpressure_import());
    }

    #[test]
    fn map_invalid_argument() {
        let err = map_tonic_status(&tonic::Status::invalid_argument("bad root"));
        assert!(matches!(err, SeamError::InvalidArgument(_)));
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
    fn gossip_source_is_stamped_not_taken() {
        let proto = gossip_to_proto(&gossip([7; 32]));
        assert_eq!(proto.source, Source::Gossip as i32);
    }

    #[tokio::test]
    async fn publish_full_is_dropped() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_ipc, egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
        drop(task);
        for i in 0..PUBLISH_BOUND {
            let out = egress.publish(publish_req(&format!("t{i}"))).await.unwrap();
            assert_eq!(out, Published::Queued);
        }
        assert_eq!(
            egress.publish(publish_req("overflow")).await.unwrap(),
            Published::Dropped
        );
        drop(mailbox);
        let err = egress.publish(publish_req("closed")).await.unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
    }

    #[tokio::test]
    async fn update_view_never_fails() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_ipc, egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
        drop(task);
        let view = ChainView {
            slot: 9,
            head_root: [9; 32],
            view_kind: 3,
            ..ChainView::default()
        };
        egress.update_view(view.clone());
        assert_eq!(*mailbox.load_view(), view);
        drop(mailbox);
        egress.update_view(ChainView {
            slot: 10,
            ..ChainView::default()
        });
    }

    #[tokio::test]
    async fn oversize_column_is_invalid_argument() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ipc, _egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
        drop(task);
        let err = ipc
            .submit_column_sidecar(sidecar(vec![0; MAX_EVENT_PAYLOAD_BYTES + 1]))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        drop(mailbox);
    }

    #[test]
    fn offline_buffer_full_da_is_backpressure() {
        let mut pending = Vec::new();
        for i in 0..CHAIN_OUT_BOUND {
            let (tx, _rx) = oneshot::channel();
            pending.push(IpcOut::DataAvailable {
                root: [i as u8; 32],
                slot: i as u64,
                reply: Some(tx),
                enqueued_at: Instant::now(),
            });
        }
        let (tx, rx) = oneshot::channel();
        buffer_while_disconnected(
            &mut pending,
            IpcOut::DataAvailable {
                root: [0xff; 32],
                slot: 99,
                reply: Some(tx),
                enqueued_at: Instant::now(),
            },
        );
        assert_eq!(pending.len(), CHAIN_OUT_BOUND);
        assert_eq!(rx.blocking_recv().unwrap(), Err(backpressure_out()));
    }

    #[test]
    fn pending_older_than_deadline_is_backpressure() {
        let mut pending = Vec::new();
        let (tx, rx) = oneshot::channel();
        pending.push(IpcOut::Gossip {
            obj: gossip([1; 32]),
            reply: tx,
            enqueued_at: Instant::now() - Duration::from_secs(3),
        });
        apply_pending_timeouts(&mut pending, Duration::from_secs(2));
        assert!(pending.is_empty());
        assert_eq!(rx.blocking_recv().unwrap(), Err(backpressure_out()));
    }

    #[test]
    fn requeue_outstanding_keeps_failed_and_tail() {
        let mut outstanding = OutstandingMap::default();
        let e1 = OutstandingEntry {
            first_sent_at: Instant::now(),
            sent_at: Instant::now(),
            object: gossip([1; 32]),
            reply: None,
        };
        let e2 = OutstandingEntry {
            first_sent_at: Instant::now(),
            sent_at: Instant::now(),
            object: gossip([2; 32]),
            reply: None,
        };
        requeue_outstanding(&mut outstanding, e1, [e2]);
        assert!(outstanding.contains(&GossipKey {
            kind: ObjectKind::Block,
            root: [1; 32],
        }));
        assert!(outstanding.contains(&GossipKey {
            kind: ObjectKind::Block,
            root: [2; 32],
        }));
        assert_eq!(outstanding.len(), 2);
    }

    #[test]
    fn column_ack_does_not_complete_block_gossip() {
        let mut outstanding = OutstandingMap::default();
        let (tx, mut rx) = oneshot::channel();
        let key = GossipKey {
            kind: ObjectKind::Block,
            root: [1; 32],
        };
        assert!(outstanding.try_insert(
            key,
            OutstandingEntry {
                first_sent_at: Instant::now(),
                sent_at: Instant::now(),
                object: gossip([1; 32]),
                reply: Some(tx),
            }
        ));
        apply_verdict(
            ProtoVerdict {
                correlation_id: [1; 32].to_vec(),
                acceptance: ProtoAcceptance::Ignore as i32,
                reason: ProtoReason::AlreadyKnown as i32,
                import: ProtoImport::None as i32,
            },
            &mut outstanding,
            &NoopUpward,
        );
        assert!(outstanding.contains(&key));
        assert!(rx.try_recv().is_err());

        apply_verdict(
            ProtoVerdict {
                correlation_id: [1; 32].to_vec(),
                acceptance: ProtoAcceptance::Ignore as i32,
                reason: ProtoReason::Internal as i32,
                import: ProtoImport::Invalid as i32,
            },
            &mut outstanding,
            &NoopUpward,
        );
        assert_eq!(rx.blocking_recv().unwrap(), Err(backpressure_import()));
    }

    #[test]
    fn short_correlation_id_is_not_padded() {
        let mut outstanding = OutstandingMap::default();
        let (tx, mut rx) = oneshot::channel();
        let key = GossipKey {
            kind: ObjectKind::Block,
            root: [0; 32],
        };
        assert!(outstanding.try_insert(
            key,
            OutstandingEntry {
                first_sent_at: Instant::now(),
                sent_at: Instant::now(),
                object: gossip([0; 32]),
                reply: Some(tx),
            }
        ));
        apply_verdict(
            ProtoVerdict {
                correlation_id: vec![1, 2, 3],
                acceptance: ProtoAcceptance::Accept as i32,
                reason: ProtoReason::Valid as i32,
                import: ProtoImport::Imported as i32,
            },
            &mut outstanding,
            &NoopUpward,
        );
        assert!(outstanding.contains(&key));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn column_ack_completes_non_block_gossip_not_block() {
        let mut outstanding = OutstandingMap::default();
        let (block_tx, mut block_rx) = oneshot::channel();
        let (att_tx, att_rx) = oneshot::channel();
        let mut att = gossip([1; 32]);
        att.kind = ObjectKind::Attestation;
        assert!(outstanding.try_insert(
            GossipKey {
                kind: ObjectKind::Block,
                root: [1; 32],
            },
            OutstandingEntry {
                first_sent_at: Instant::now(),
                sent_at: Instant::now(),
                object: gossip([1; 32]),
                reply: Some(block_tx),
            }
        ));
        assert!(outstanding.try_insert(
            GossipKey {
                kind: ObjectKind::Attestation,
                root: [1; 32],
            },
            OutstandingEntry {
                first_sent_at: Instant::now(),
                sent_at: Instant::now(),
                object: att,
                reply: Some(att_tx),
            }
        ));
        apply_verdict(
            ProtoVerdict {
                correlation_id: [1; 32].to_vec(),
                acceptance: ProtoAcceptance::Ignore as i32,
                reason: ProtoReason::AlreadyKnown as i32,
                import: ProtoImport::None as i32,
            },
            &mut outstanding,
            &NoopUpward,
        );
        assert!(outstanding.contains(&GossipKey {
            kind: ObjectKind::Block,
            root: [1; 32],
        }));
        assert!(block_rx.try_recv().is_err());
        let got = att_rx.blocking_recv().unwrap().unwrap();
        assert_eq!(got.verdict.reason, Reason::AlreadyKnown);
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_timeout_is_backpressure() {
        let (tx, _rx) = mpsc::channel(1);
        let (reply, _rr) = oneshot::channel();
        tx.send(IpcOut::Gossip {
            obj: gossip([0; 32]),
            reply,
            enqueued_at: Instant::now(),
        })
        .await
        .unwrap();
        let (reply, _rr) = oneshot::channel();
        let err = enqueue(
            &tx,
            IpcOut::Gossip {
                obj: gossip([1; 32]),
                reply,
                enqueued_at: Instant::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err, backpressure_out());
    }

    #[tokio::test(start_paused = true)]
    async fn submit_gossip_oneshot_deadline_is_backpressure() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ipc, _egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
        // Keep `task` so `out_rx` stays open (item sits in `out_tx`, loop never ticks).
        let join = tokio::spawn(async move { ipc.submit_gossip(gossip([9; 32])).await });
        tokio::task::yield_now().await;
        tokio::time::advance(IMPORT_SEND_TIMEOUT).await;
        let err = join.await.unwrap().unwrap_err();
        assert_eq!(err, backpressure_out());
        drop((task, mailbox));
    }

    #[tokio::test]
    async fn submit_gossip_roundtrip_over_tonic() {
        let (addr, stop) = spawn_echo_grpc().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let cfg = IpcConfig {
            chain_uri: format!("http://{addr}"),
            ..IpcConfig::default()
        };
        let (ipc, _egress, mailbox, task) = Ipc::connect(cfg, shutdown_rx);
        let join = tokio::spawn(task);
        for _ in 0..100 {
            if mailbox.load_view().view_kind == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(mailbox.load_view().view_kind, 4);

        let got = ipc.submit_gossip(gossip([3; 32])).await.unwrap();
        assert_eq!(got.verdict.acceptance, Acceptance::Accept);
        assert_eq!(got.verdict.correlation_id, [3; 32]);
        assert_eq!(got.verdict.import, ImportResult::Imported);

        let _ = shutdown_tx.send(true);
        let _ = stop.send(());
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    }
}
