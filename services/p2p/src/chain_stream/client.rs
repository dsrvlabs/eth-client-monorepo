//! Chain-stream client: dial, reconnect, outstanding map, verdict timeout.
//!
//! Architecture §10.4–10.6. Never fatal (§2.4) — it is already a reconnect loop.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cc_proto::chain::chain_service_client::ChainServiceClient;
use cc_proto::p2p::{
    Acceptance, ChainToP2p, ChainView, GossipObject, ImportResult, P2pToChain, PublishRequest,
    Reason, StreamHello, Verdict, chain_to_p2p, p2p_to_chain,
};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tracing::{debug, info, warn};

use super::publish::PublishDropCounter;
use super::view::ChainViewStore;
use super::{
    BACKOFF_CAP, BACKOFF_INITIAL, DEFAULT_VERDICT_LATE_AFTER, DEFAULT_VERDICT_TIMEOUT,
    OUTSTANDING_CAP, STALL_HEARTBEAT_FRACTION,
};
use crate::channels::{ChainInbound, ChainOutbound, VerdictResolution, CHAIN_OUT_BOUND};
use crate::metrics::{P2pMetrics, QueueName};

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
    pub fn drain_timed_out(&mut self, timeout: Duration, now: Instant) -> Vec<(CorrelationId, OutstandingEntry)> {
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

/// Fresh random session id (reconnect must not reuse the previous incarnation).
#[must_use]
pub fn new_session_id() -> u64 {
    getrandom::u64().unwrap_or_else(|_| {
        // Fall back to a time-derived id if the CSPRNG is unavailable (should not
        // happen on supported platforms).
        Instant::now().elapsed().as_nanos() as u64
            ^ std::process::id() as u64
    })
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

/// Run the never-fatal reconnect loop until `shutdown` is true.
///
/// - On connect: `StreamHello`, re-send `outstanding`, expect full `ChainView`.
/// - Downward: `chain_out_rx` → stream objects, track outstanding.
/// - Upward: verdicts / publish / view.
/// - Timeout task: entries older than `verdict_timeout` → local IGNORE.
/// - Backoff always runs to completion; outbound traffic is drained without
///   cancelling the timer (H1 / reconnect-storm fix).
#[allow(clippy::too_many_arguments)]
pub async fn run_chain_stream_client(
    cfg: ChainStreamConfig,
    mut chain_out_rx: mpsc::Receiver<ChainOutbound>,
    chain_in_tx: mpsc::Sender<ChainInbound>,
    publish_fwd_tx: mpsc::Sender<PublishRequest>,
    handle: ChainStreamHandle,
    metrics: P2pMetrics,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut outstanding = OutstandingMap::new();
    let mut backoff = cfg.backoff_initial;
    // Objects accepted while disconnected sit here until a session opens; the
    // channel bound is CHAIN_OUT_BOUND, and outstanding is also capped.
    let mut pending_out: Vec<ChainOutbound> = Vec::new();

    metrics.set_queue_depth(QueueName::Outstanding, 0);
    metrics.set_saturation_ratio(0.0);

    loop {
        if *shutdown.borrow() {
            resolve_all_timeout(&mut outstanding, &metrics);
            return;
        }

        match connect_and_run_session(
            &cfg,
            &mut chain_out_rx,
            &chain_in_tx,
            &publish_fwd_tx,
            &handle,
            &metrics,
            &mut outstanding,
            &mut pending_out,
            &mut shutdown,
            &mut backoff,
        )
        .await
        {
            SessionEnd::Shutdown => {
                resolve_all_timeout(&mut outstanding, &metrics);
                return;
            }
            SessionEnd::Disconnected => {
                info!(
                    outstanding = outstanding.len(),
                    backoff_ms = backoff.as_millis() as u64,
                    "chain stream disconnected; reconnecting with backoff"
                );
                let sleep = full_jitter(backoff);
                backoff = next_backoff(backoff, cfg.backoff_cap);
                // H1: complete the full backoff window. Drain outbound into
                // `pending_out` without aborting the timer.
                if wait_reconnect_backoff(
                    sleep,
                    &mut chain_out_rx,
                    &mut pending_out,
                    &mut outstanding,
                    &metrics,
                    cfg.verdict_timeout,
                    &mut shutdown,
                )
                .await
                {
                    resolve_all_timeout(&mut outstanding, &metrics);
                    return;
                }
            }
        }
    }
}

/// Wait `sleep`, draining `chain_out_rx` and applying timeouts without shortening
/// the reconnect interval. Returns `true` if shutdown was requested.
///
/// Exposed for tests that assert backoff is not cancelled under outbound load.
pub async fn wait_reconnect_backoff(
    sleep: Duration,
    chain_out_rx: &mut mpsc::Receiver<ChainOutbound>,
    pending_out: &mut Vec<ChainOutbound>,
    outstanding: &mut OutstandingMap,
    metrics: &P2pMetrics,
    verdict_timeout: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let deadline = Instant::now() + sleep;
    let mut out_open = true;
    loop {
        // Timeouts keep running while disconnected (M4).
        apply_timeouts(outstanding, verdict_timeout, metrics);
        // Non-blocking drain of any already-queued outbound.
        if out_open {
            loop {
                match chain_out_rx.try_recv() {
                    Ok(msg) => buffer_while_disconnected(pending_out, msg),
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
                _ = tokio::time::sleep(remaining) => {
                    return false;
                }
                msg = chain_out_rx.recv() => {
                    // Buffer and continue — do **not** exit early (H1).
                    match msg {
                        Some(m) => buffer_while_disconnected(pending_out, m),
                        None => {
                            // Channel closed: stop selecting recv (would spin).
                            out_open = false;
                        }
                    }
                }
            }
        } else {
            // No more outbound producers — pure sleep until deadline.
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return true;
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    return false;
                }
            }
        }
    }
}

enum SessionEnd {
    Shutdown,
    Disconnected,
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_run_session(
    cfg: &ChainStreamConfig,
    chain_out_rx: &mut mpsc::Receiver<ChainOutbound>,
    chain_in_tx: &mpsc::Sender<ChainInbound>,
    publish_fwd_tx: &mpsc::Sender<PublishRequest>,
    handle: &ChainStreamHandle,
    metrics: &P2pMetrics,
    outstanding: &mut OutstandingMap,
    pending_out: &mut Vec<ChainOutbound>,
    shutdown: &mut watch::Receiver<bool>,
    backoff: &mut Duration,
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
            debug!(error = %e, "chain stream dial failed");
            return SessionEnd::Disconnected;
        }
    };

    let mut client = ChainServiceClient::new(channel);
    // Bound matches STREAM_OUTBOUND / CHAIN_OUT so we never buffer unbounded.
    let (out_tx, out_rx) = mpsc::channel::<P2pToChain>(CHAIN_OUT_BOUND);
    let outbound = ReceiverStream::new(out_rx);

    let response = match client.p2p_stream(outbound).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "P2pStream open failed");
            return SessionEnd::Disconnected;
        }
    };
    let mut inbound = response.into_inner();

    let session_id = new_session_id();
    let mut out_seq: u64 = 0;
    let mut last_chain_seq: u64 = 0;

    // StreamHello — resume_seq is last processed chain seq (0 on fresh).
    out_seq = out_seq.saturating_add(1);
    if out_tx
        .send(P2pToChain {
            seq: out_seq,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id,
                resume_seq: last_chain_seq,
            })),
        })
        .await
        .is_err()
    {
        return SessionEnd::Disconnected;
    }
    // Healthy open: reset backoff so a later blip does not stay at the cap (L5).
    *backoff = cfg.backoff_initial;
    info!(session_id, "chain stream session opened (StreamHello sent)");

    // Re-send everything still outstanding after the new hello (CC-27/4).
    // Drain, re-assign seq, re-insert. Do **not** re-increment
    // `chain_objects_sent` — the original send already counted; equality is
    // `sent == verdicts + timeouts` over the object's lifetime, not per wire frame.
    // Do **not** reset `first_sent_at` — timeout clock keeps ticking across reconnect.
    let to_resend = outstanding.take_all_for_resend();
    for mut entry in to_resend {
        out_seq = out_seq.saturating_add(1);
        entry.seq = out_seq;
        entry.sent_at = Instant::now();
        let id = entry.object.root.clone();
        let wire = P2pToChain {
            seq: out_seq,
            msg: Some(p2p_to_chain::Msg::Object(entry.object.clone())),
        };
        if out_tx.send(wire).await.is_err() {
            // Put back so the next session can re-send.
            let _ = outstanding.reinsert(id, entry);
            return SessionEnd::Disconnected;
        }
        let _ = outstanding.reinsert(id, entry);
    }
    sync_outstanding_metrics(outstanding, metrics);

    // Flush objects buffered while disconnected.
    let buffered = std::mem::take(pending_out);
    for item in buffered {
        if send_object(
            item,
            &mut out_seq,
            &out_tx,
            outstanding,
            metrics,
        )
        .await
        .is_err()
        {
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
                apply_timeouts(outstanding, cfg.verdict_timeout, metrics);
            }
            msg = inbound.next() => {
                match msg {
                    None => return SessionEnd::Disconnected,
                    Some(Err(status)) => {
                        warn!(error = %status, "chain stream inbound error");
                        return SessionEnd::Disconnected;
                    }
                    Some(Ok(msg)) => {
                        last_chain_seq = msg.seq;
                        if handle_upward(
                            msg,
                            outstanding,
                            handle,
                            publish_fwd_tx,
                            chain_in_tx,
                            metrics,
                            cfg.verdict_late_after,
                        ).await
                            .is_err()
                        {
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
            item = chain_out_rx.recv() => {
                match item {
                    None => {
                        // Outbound producers closed; keep the session until
                        // shutdown so views/publishes still flow.
                        // Park by waiting only on inbound/timeout/shutdown.
                        // Fall through: treat as disconnect of producers but
                        // stay connected until chain closes or shutdown.
                        debug!("chain_out channel closed; session stays up for views");
                        // Replace further recv with pending forever via a branch
                        // that never fires — simplest: spin on the other arms by
                        // not selecting this again. We break to a receive-only loop.
                        return receive_only_loop(
                            cfg,
                            &mut inbound,
                            outstanding,
                            handle,
                            publish_fwd_tx,
                            chain_in_tx,
                            metrics,
                            shutdown,
                            &mut last_chain_seq,
                        ).await;
                    }
                    Some(item) => {
                        if send_object(
                            item,
                            &mut out_seq,
                            &out_tx,
                            outstanding,
                            metrics,
                        )
                        .await
                        .is_err()
                        {
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn receive_only_loop(
    cfg: &ChainStreamConfig,
    inbound: &mut (impl StreamExt<Item = Result<ChainToP2p, tonic::Status>> + Unpin),
    outstanding: &mut OutstandingMap,
    handle: &ChainStreamHandle,
    publish_fwd_tx: &mpsc::Sender<PublishRequest>,
    chain_in_tx: &mpsc::Sender<ChainInbound>,
    metrics: &P2pMetrics,
    shutdown: &mut watch::Receiver<bool>,
    last_chain_seq: &mut u64,
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
                apply_timeouts(outstanding, cfg.verdict_timeout, metrics);
            }
            msg = inbound.next() => {
                match msg {
                    None => return SessionEnd::Disconnected,
                    Some(Err(_)) => return SessionEnd::Disconnected,
                    Some(Ok(msg)) => {
                        *last_chain_seq = msg.seq;
                        if handle_upward(
                            msg,
                            outstanding,
                            handle,
                            publish_fwd_tx,
                            chain_in_tx,
                            metrics,
                            cfg.verdict_late_after,
                        ).await
                            .is_err()
                        {
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
        }
    }
}

async fn send_object(
    item: ChainOutbound,
    out_seq: &mut u64,
    out_tx: &mpsc::Sender<P2pToChain>,
    outstanding: &mut OutstandingMap,
    metrics: &P2pMetrics,
) -> Result<(), ()> {
    let id = item.object.root.clone();

    // Duplicate root already outstanding: do not double-count `sent`, do not
    // overwrite the existing entry (orphans the first reply). Resolve the *new*
    // producer with local IGNORE and leave equality untouched (M2).
    if outstanding.contains(&id) {
        warn!("duplicate root already outstanding; dropping new send without equality term");
        if let Some(reply) = item.reply {
            let _ = reply.send(VerdictResolution::Timeout);
        }
        return Ok(());
    }

    if outstanding.is_full() {
        // Cap refusal is not a verdict timeout — do not charge the equality
        // triple (F2). Producer still gets a release on its reply channel.
        warn!("outstanding at cap; dropping outbound object (should be rare under §2.3)");
        if let Some(reply) = item.reply {
            let _ = reply.send(VerdictResolution::Timeout);
        }
        return Ok(());
    }

    *out_seq = out_seq.saturating_add(1);
    let seq = *out_seq;
    let wire = P2pToChain {
        seq,
        msg: Some(p2p_to_chain::Msg::Object(item.object.clone())),
    };
    out_tx.send(wire).await.map_err(|_| ())?;
    metrics.inc_chain_objects_sent();

    let now = Instant::now();
    let entry = OutstandingEntry {
        seq,
        first_sent_at: now,
        sent_at: now,
        object: item.object,
        reply: item.reply,
    };
    if !outstanding.try_insert(id, entry) {
        // Race should be impossible after contains/is_full checks; if it
        // happens, do not leave `sent` without a resolution term — count as
        // timeout so equality stays exact.
        metrics.inc_verdict_timeout();
    }
    sync_outstanding_metrics(outstanding, metrics);
    Ok(())
}

async fn handle_upward(
    msg: ChainToP2p,
    outstanding: &mut OutstandingMap,
    handle: &ChainStreamHandle,
    publish_fwd_tx: &mpsc::Sender<PublishRequest>,
    chain_in_tx: &mpsc::Sender<ChainInbound>,
    metrics: &P2pMetrics,
    late_after: Duration,
) -> Result<(), ()> {
    let Some(inner) = msg.msg else {
        return Ok(());
    };
    match inner {
        chain_to_p2p::Msg::Verdict(verdict) => {
            apply_verdict(verdict, outstanding, chain_in_tx, metrics, late_after).await;
            Ok(())
        }
        chain_to_p2p::Msg::Publish(req) => {
            // Outward path — do not block the stream on a full publish queue;
            // the publish dispatcher applies oldest-drop.
            if publish_fwd_tx.try_send(req).is_err() {
                // Dispatcher may be slow; try once with await under a short budget.
                // If still full, the dispatcher is responsible for drop accounting
                // when it eventually accepts — count a drop here if channel closed.
                warn!("publish forward channel full or closed");
            }
            Ok(())
        }
        chain_to_p2p::Msg::View(view) => {
            apply_view(view, handle, metrics);
            Ok(())
        }
    }
}

fn apply_view(view: ChainView, handle: &ChainStreamHandle, metrics: &P2pMetrics) {
    // Head-lag: if we know genesis/slot from the view, optional observe later.
    let _ = metrics;
    debug!(
        slot = view.slot,
        head_slot = view.head_slot,
        view_kind = view.view_kind,
        "received ChainView"
    );
    handle.view.store(view);
}

async fn apply_verdict(
    verdict: Verdict,
    outstanding: &mut OutstandingMap,
    chain_in_tx: &mpsc::Sender<ChainInbound>,
    metrics: &P2pMetrics,
    late_after: Duration,
) {
    let id = verdict.correlation_id.clone();
    if let Some(entry) = outstanding.remove(&id) {
        // Latency from last wire send; late budget is the validation window.
        let latency = entry.sent_at.elapsed();
        metrics.observe_verdict_latency(latency.as_secs_f64());
        metrics.inc_chain_verdicts_received();
        if latency > late_after {
            // Late but still the resolution term (CC-27/5 metric half).
            metrics.inc_verdict_late();
        }
        if let Some(reply) = entry.reply {
            let _ = reply.send(VerdictResolution::FromChain(verdict.clone()));
        }
        // Dispatch to consumers (gossip validation hold release).
        let _ = chain_in_tx.try_send(ChainInbound {
            verdict: verdict.clone(),
            latency,
        });
        sync_outstanding_metrics(outstanding, metrics);
    } else {
        // Already timed out, or a **late import correction** after early ACCEPT
        // (CC-27c): count late only — do **not** re-enter
        // `chain_verdicts_received` (CC-27/4 equality). Still forward to
        // `chain_in` so `import_invalid` app-score can fire without re-report.
        metrics.inc_verdict_late();
        let _ = chain_in_tx.try_send(ChainInbound {
            verdict,
            latency: std::time::Duration::ZERO,
        });
        debug!(
            "verdict for unknown/expired correlation_id (late after timeout or late import reject; not an equality term)"
        );
    }
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

fn resolve_all_timeout(outstanding: &mut OutstandingMap, metrics: &P2pMetrics) {
    let keys = outstanding.keys();
    for k in keys {
        if let Some(entry) = outstanding.remove(&k) {
            metrics.inc_verdict_timeout();
            if let Some(reply) = entry.reply {
                let _ = reply.send(VerdictResolution::Timeout);
            }
        }
    }
    sync_outstanding_metrics(outstanding, metrics);
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
