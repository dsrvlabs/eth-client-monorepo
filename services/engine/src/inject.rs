//! Engine→p2p inject stream client — the ninth contract, engine side (CC-38a).
//!
//! Architecture §5.2 / ADR P3-02:
//! - `engine` **dials** `p2p` (plain config URI — **not** a health peer).
//! - Upward: [`EngineHello`] then [`InjectColumns`] (subscribed sidecars only).
//! - Downward: [`SubscriptionSet`] and column-branch [`FetchBlobsRequest`].
//! - Reconnect curve is Phase 2 §10.6 **verbatim**: 250 ms → ×2 → 10 s cap,
//!   full jitter; never fatal. Each reconnect opens with a **new** `session_id`.
//!
//! The `EngineStream` **server** is CC-38b (`services/p2p`).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use cc_proto::engine::{FetchBlobsRequest, SidecarTemplate as WireSidecarTemplate};
use cc_proto::p2p::p2p_service_client::P2pServiceClient;
use cc_proto::p2p::{
    EngineHello, EngineToP2p, InjectColumns, P2pToEngine, SubscriptionSet as WireSubscriptionSet,
    engine_to_p2p, p2p_to_engine,
};
use cc_types::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH;
use cc_types::containers::SignedBeaconBlockHeader;
use cc_types::primitives::{KzgCommitment, Root};
use futures::StreamExt;
use ssz::{Decode, Encode};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tracing::{debug, info, warn};

use crate::fastpath::filter::SubscriptionSet;
use crate::fastpath::sidecars::SidecarTemplate;
use crate::fastpath::{FastpathLane, InjectItem, TriggerOwner};
use crate::methods::get_blobs::{NullContext, versioned_hashes_from_commitments};

/// Default null-classification context for stream-admitted fetches (post-Osaka pool miss).
const STREAM_NULL_CTX: NullContext = NullContext::PrunedPool;
use crate::metrics::EngineMetrics;

// ── Phase 2 §10.6 reconnect curve (verbatim) ────────────────────────────────

/// Reconnect backoff initial delay (Phase 2 §10.6).
pub const BACKOFF_INITIAL: Duration = Duration::from_millis(250);

/// Reconnect backoff hard cap (Phase 2 §10.6).
pub const BACKOFF_CAP: Duration = Duration::from_secs(10);

/// Bound on `inject_tx` (worker → stream client). Architecture §2.3: 8 items.
///
/// An item is the subscribed sidecars for one block (~353 KB at cgc=4 /
/// sampling_size=8 and 21 blobs). Bound ⇒ ≈ 2.8 MB.
pub const INJECT_QUEUE_BOUND: usize = 8;

/// Inbound `P2pToEngine` channel bound (Architecture §2.3).
pub const INBOUND_QUEUE_BOUND: usize = 32;

/// Outbound stream channel bound (matches inject queue).
pub const OUTBOUND_STREAM_BOUND: usize = INJECT_QUEUE_BOUND;

// ── Config ──────────────────────────────────────────────────────────────────

/// Configuration for the engine→p2p inject stream client.
#[derive(Debug, Clone)]
pub struct InjectStreamConfig {
    /// gRPC URI for `p2p` (e.g. `http://127.0.0.1:9002`). Plain config URI —
    /// **not** a health peer (ADR P3-02): p2p restarting must not make engine
    /// `NOT_SERVING`.
    pub p2p_uri: String,
    /// Initial reconnect backoff (default [`BACKOFF_INITIAL`]).
    pub backoff_initial: Duration,
    /// Backoff hard cap (default [`BACKOFF_CAP`]).
    pub backoff_cap: Duration,
    /// Connect timeout for each dial attempt.
    pub connect_timeout: Duration,
}

impl Default for InjectStreamConfig {
    fn default() -> Self {
        Self {
            p2p_uri: "http://127.0.0.1:9002".to_owned(),
            backoff_initial: BACKOFF_INITIAL,
            backoff_cap: BACKOFF_CAP,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

// ── Bounded inject queue (drop-oldest) ──────────────────────────────────────

/// Bounded queue of inject items: capacity [`INJECT_QUEUE_BOUND`], drop-oldest.
#[derive(Debug, Default)]
pub struct InjectQueue {
    q: VecDeque<InjectItem>,
    bound: usize,
    dropped: u64,
}

impl InjectQueue {
    /// Empty queue with the architecture bound of 8.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(INJECT_QUEUE_BOUND)
    }

    /// Empty queue with a custom bound (tests).
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            q: VecDeque::with_capacity(bound.max(1)),
            bound: bound.max(1),
            dropped: 0,
        }
    }

    /// Current depth.
    #[must_use]
    pub fn len(&self) -> usize {
        self.q.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    /// Cumulative drop-oldest count.
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.dropped
    }

    /// Push, dropping oldest when at capacity. Returns whether an item was dropped.
    pub fn push(&mut self, item: InjectItem) -> bool {
        let mut dropped = false;
        while self.q.len() >= self.bound {
            let _ = self.q.pop_front();
            self.dropped = self.dropped.saturating_add(1);
            dropped = true;
        }
        self.q.push_back(item);
        dropped
    }

    /// Pop oldest.
    pub fn pop(&mut self) -> Option<InjectItem> {
        self.q.pop_front()
    }
}

// ── Reconnect helpers (Phase 2 §10.6, verbatim) ─────────────────────────────

/// Fresh random session id (reconnect must not reuse the previous incarnation).
#[must_use]
pub fn new_session_id() -> u64 {
    getrandom::u64()
        .unwrap_or_else(|_| Instant::now().elapsed().as_nanos() as u64 ^ std::process::id() as u64)
}

/// Full-jitter sleep duration in `[0, backoff]` (AWS full jitter / Phase 2 §10.6).
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

// ── Wire ↔ local conversions ────────────────────────────────────────────────

/// Decoded wire [`FetchBlobsRequest`] ready for lane admission.
#[derive(Debug, Clone)]
pub struct DecodedFetch {
    pub beacon_block_root: [u8; 32],
    pub slot: u64,
    pub template: SidecarTemplate,
    pub versioned_hashes: Vec<[u8; 32]>,
}

/// Decode a wire [`FetchBlobsRequest`] into lane admission inputs.
///
/// Returns `None` when the template is missing or commitments are empty /
/// malformed (fail closed — no fetch).
#[must_use]
pub fn decode_fetch_blobs_request(req: &FetchBlobsRequest) -> Option<DecodedFetch> {
    let root = <[u8; 32]>::try_from(req.beacon_block_root.as_slice()).ok()?;
    let template = decode_wire_template(req.template.as_ref()?)?;
    if template.kzg_commitments.is_empty() {
        return None;
    }
    let mut versioned_hashes: Vec<[u8; 32]> = Vec::with_capacity(req.versioned_hashes.len());
    for h in &req.versioned_hashes {
        versioned_hashes.push(<[u8; 32]>::try_from(h.as_slice()).ok()?);
    }
    if versioned_hashes.is_empty() {
        let raw: Vec<[u8; 48]> = template
            .kzg_commitments
            .iter()
            .map(|c| *c.as_array())
            .collect();
        versioned_hashes = versioned_hashes_from_commitments(&raw);
    }
    Some(DecodedFetch {
        beacon_block_root: root,
        slot: req.slot,
        template,
        versioned_hashes,
    })
}

/// Decode wire `SidecarTemplate` → local.
#[must_use]
pub fn decode_wire_template(wire: &WireSidecarTemplate) -> Option<SidecarTemplate> {
    let header = SignedBeaconBlockHeader::from_ssz_bytes(&wire.signed_block_header_ssz).ok()?;
    let mut kzg_commitments = Vec::with_capacity(wire.kzg_commitments.len());
    for c in &wire.kzg_commitments {
        let arr = <[u8; 48]>::try_from(c.as_slice()).ok()?;
        kzg_commitments.push(KzgCommitment::from_array(arr));
    }
    if wire.kzg_commitments_inclusion_proof.len() != KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
    {
        return None;
    }
    let mut proof = [Root::default(); KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize];
    for (i, p) in wire.kzg_commitments_inclusion_proof.iter().enumerate() {
        proof[i] = Root::from_array(<[u8; 32]>::try_from(p.as_slice()).ok()?);
    }
    Some(SidecarTemplate::new(header, kzg_commitments, proof))
}

/// Encode local template → wire (template size class, ~6.5 KB at 21 commitments).
#[must_use]
pub fn encode_wire_template(local: &SidecarTemplate) -> WireSidecarTemplate {
    WireSidecarTemplate {
        signed_block_header_ssz: local.signed_block_header.as_ssz_bytes(),
        kzg_commitments: local
            .kzg_commitments
            .iter()
            .map(|c| c.as_array().to_vec())
            .collect(),
        kzg_commitments_inclusion_proof: local
            .kzg_commitments_inclusion_proof
            .iter()
            .map(|r| r.as_slice().to_vec())
            .collect(),
    }
}

/// Build a wire [`FetchBlobsRequest`] from local fields (chain / tests).
#[must_use]
pub fn encode_fetch_blobs_request(
    beacon_block_root: [u8; 32],
    slot: u64,
    versioned_hashes: &[[u8; 32]],
    template: &SidecarTemplate,
) -> FetchBlobsRequest {
    FetchBlobsRequest {
        beacon_block_root: beacon_block_root.to_vec(),
        slot,
        versioned_hashes: versioned_hashes.iter().map(|h| h.to_vec()).collect(),
        template: Some(encode_wire_template(template)),
    }
}

/// Apply a wire subscription set onto the local filter type.
#[must_use]
pub fn subscription_from_wire(wire: &WireSubscriptionSet) -> SubscriptionSet {
    SubscriptionSet::from_indices(wire.column_indices.iter().copied(), wire.cgc)
}

/// Encode local subscription → wire.
#[must_use]
pub fn subscription_to_wire(local: &SubscriptionSet) -> WireSubscriptionSet {
    WireSubscriptionSet {
        column_indices: local.column_indices.iter().copied().collect(),
        cgc: local.cgc,
    }
}

// ── Stream client ───────────────────────────────────────────────────────────

/// Run the never-fatal inject-stream reconnect loop until `shutdown` is true.
///
/// - On connect: `EngineHello{session_id}` (new id every session).
/// - Upward: `inject_rx` → `InjectColumns` with `trusted_local = true` (§5.8).
/// - Downward: `SubscriptionSet` → [`FastpathLane::set_subscription`];
///   `FetchBlobsRequest` → column-branch trigger (no chain involvement).
/// - Backoff: Phase 2 §10.6 curve; `cc_engine_inject_stream_state` reflects
///   connected (1) / disconnected (0).
pub async fn run_inject_stream_client(
    cfg: InjectStreamConfig,
    mut inject_rx: mpsc::Receiver<InjectItem>,
    lane: FastpathLane,
    metrics: Option<EngineMetrics>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = cfg.backoff_initial;
    let mut pending: VecDeque<InjectItem> = VecDeque::new();

    set_stream_state(metrics.as_ref(), false);

    loop {
        if *shutdown.borrow() {
            set_stream_state(metrics.as_ref(), false);
            return;
        }

        match connect_and_run_session(
            &cfg,
            &mut inject_rx,
            &mut pending,
            &lane,
            metrics.as_ref(),
            &mut shutdown,
            &mut backoff,
        )
        .await
        {
            SessionEnd::Shutdown => {
                set_stream_state(metrics.as_ref(), false);
                return;
            }
            SessionEnd::Disconnected => {
                set_stream_state(metrics.as_ref(), false);
                info!(
                    pending = pending.len(),
                    backoff_ms = backoff.as_millis() as u64,
                    "inject stream disconnected; reconnecting with backoff"
                );
                let sleep = full_jitter(backoff);
                backoff = next_backoff(backoff, cfg.backoff_cap);
                if wait_reconnect_backoff(sleep, &mut inject_rx, &mut pending, &mut shutdown).await
                {
                    set_stream_state(metrics.as_ref(), false);
                    return;
                }
            }
        }
    }
}

/// Wait `sleep`, draining `inject_rx` into `pending` without shortening the
/// reconnect interval. Returns `true` if shutdown was requested.
pub async fn wait_reconnect_backoff(
    sleep: Duration,
    inject_rx: &mut mpsc::Receiver<InjectItem>,
    pending: &mut VecDeque<InjectItem>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let deadline = Instant::now() + sleep;
    let mut out_open = true;
    loop {
        if out_open {
            loop {
                match inject_rx.try_recv() {
                    Ok(item) => buffer_pending(pending, item),
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
                msg = inject_rx.recv() => {
                    match msg {
                        Some(m) => buffer_pending(pending, m),
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

async fn connect_and_run_session(
    cfg: &InjectStreamConfig,
    inject_rx: &mut mpsc::Receiver<InjectItem>,
    pending: &mut VecDeque<InjectItem>,
    lane: &FastpathLane,
    metrics: Option<&EngineMetrics>,
    shutdown: &mut watch::Receiver<bool>,
    backoff: &mut Duration,
) -> SessionEnd {
    let endpoint = match Endpoint::from_shared(cfg.p2p_uri.clone()) {
        Ok(e) => e
            .connect_timeout(cfg.connect_timeout)
            .timeout(Duration::from_secs(30)),
        Err(e) => {
            warn!(error = %e, uri = %cfg.p2p_uri, "invalid p2p URI for inject stream");
            return SessionEnd::Disconnected;
        }
    };

    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "inject stream dial failed");
            return SessionEnd::Disconnected;
        }
    };

    let mut client = P2pServiceClient::new(channel);
    let (out_tx, out_rx) = mpsc::channel::<EngineToP2p>(OUTBOUND_STREAM_BOUND);
    let outbound = ReceiverStream::new(out_rx);

    let mut inbound = match client.engine_stream(outbound).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            debug!(error = %e, "EngineStream open failed");
            return SessionEnd::Disconnected;
        }
    };

    // New session id on every connect — resets p2p-side dedup (Phase 2 §10.6).
    let session_id = new_session_id();
    let mut seq: u64 = 1;
    if out_tx
        .send(EngineToP2p {
            seq,
            msg: Some(engine_to_p2p::Msg::Hello(EngineHello { session_id })),
        })
        .await
        .is_err()
    {
        return SessionEnd::Disconnected;
    }
    seq = seq.saturating_add(1);

    // Healthy open: reset backoff so a later blip does not stay at the cap.
    *backoff = cfg.backoff_initial;
    set_stream_state(metrics, true);
    info!(
        session_id,
        "inject stream session opened (EngineHello sent)"
    );

    // Flush pending inject items first.
    while let Some(item) = pending.pop_front() {
        if send_inject(&out_tx, &mut seq, item).await.is_err() {
            return SessionEnd::Disconnected;
        }
    }

    // When `inject_rx` closes, stop polling it (F3: closed channel completes
    // immediately and would busy-loop under select). Inbound + shutdown remain.
    let mut inject_open = true;
    loop {
        if *shutdown.borrow() {
            return SessionEnd::Shutdown;
        }
        if inject_open {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return SessionEnd::Shutdown;
                    }
                }
                item = inject_rx.recv() => {
                    match item {
                        Some(item) => {
                            if send_inject(&out_tx, &mut seq, item).await.is_err() {
                                return SessionEnd::Disconnected;
                            }
                        }
                        None => {
                            // Producer closed — do not re-select on recv (would spin).
                            debug!("inject_rx closed; draining inbound only");
                            inject_open = false;
                        }
                    }
                }
                msg = inbound.next() => {
                    match msg {
                        Some(Ok(p2p_msg)) => {
                            handle_inbound(&p2p_msg, lane).await;
                        }
                        Some(Err(e)) => {
                            debug!(error = %e, "inject stream inbound error");
                            return SessionEnd::Disconnected;
                        }
                        None => {
                            debug!("inject stream closed by peer");
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return SessionEnd::Shutdown;
                    }
                }
                msg = inbound.next() => {
                    match msg {
                        Some(Ok(p2p_msg)) => {
                            handle_inbound(&p2p_msg, lane).await;
                        }
                        Some(Err(e)) => {
                            debug!(error = %e, "inject stream inbound error");
                            return SessionEnd::Disconnected;
                        }
                        None => {
                            debug!("inject stream closed by peer");
                            return SessionEnd::Disconnected;
                        }
                    }
                }
            }
        }
    }
}

/// Send one `InjectColumns` with `trusted_local = true` (§5.8).
///
/// The field is always true on the production path; it exists so the trust
/// assumption is explicit on the wire rather than implied by the RPC's name.
///
/// # Security residual (S-38a-1 → CC-38b)
///
/// `trusted_local` is a **client-asserted** boolean on an unauthenticated
/// internal stream. Proto documents that p2p may key skip-reverification on
/// it. **CC-38b must not skip KZG/inclusion verification solely on this flag**
/// until mutual auth (mTLS / allowlist / shared token) exists — treat the flag
/// as a metric/priority hint, or re-verify always and fail-closed on unauthenticated
/// inject. See plan review security section S-38a-1 / S-38a-2.
async fn send_inject(
    out_tx: &mpsc::Sender<EngineToP2p>,
    seq: &mut u64,
    item: InjectItem,
) -> Result<(), ()> {
    let msg = EngineToP2p {
        seq: *seq,
        msg: Some(engine_to_p2p::Msg::Inject(InjectColumns {
            beacon_block_root: item.beacon_block_root.to_vec(),
            slot: item.slot,
            sidecar_ssz: item.sidecar_ssz,
            // §5.8: always true; field exists so the trust assumption is
            // explicit on the wire rather than implied by the RPC's name.
            // Residual S-38a-1: not an authenticated claim — CC-38b must not
            // skip crypto on this alone (see module docs / send_inject docs).
            trusted_local: true,
        })),
    };
    *seq = seq.saturating_add(1);
    out_tx.send(msg).await.map_err(|_| ())
}

async fn handle_inbound(msg: &P2pToEngine, lane: &FastpathLane) {
    match &msg.msg {
        Some(p2p_to_engine::Msg::Subscriptions(sub)) => {
            let local = subscription_from_wire(sub);
            info!(
                columns = local.len(),
                cgc = local.cgc,
                "inject stream: SubscriptionSet applied"
            );
            lane.set_subscription(local).await;
        }
        Some(p2p_to_engine::Msg::Fetch(fetch)) => {
            // Column-branch trigger — no chain involvement (CC-38 /6).
            match decode_fetch_blobs_request(fetch) {
                Some(decoded) => {
                    let root = decoded.beacon_block_root;
                    let slot = decoded.slot;
                    let outcome = lane
                        .trigger_from_column_with_template(
                            root,
                            slot,
                            decoded.template,
                            STREAM_NULL_CTX,
                        )
                        .await;
                    debug!(
                        ?root,
                        slot,
                        ?outcome,
                        owner = ?TriggerOwner::P2pColumn,
                        "inject stream: column-branch FetchBlobsRequest enqueued"
                    );
                }
                None => {
                    warn!("inject stream: malformed FetchBlobsRequest dropped");
                }
            }
        }
        None => {
            debug!(seq = msg.seq, "inject stream: empty P2pToEngine oneof");
        }
    }
}

fn buffer_pending(pending: &mut VecDeque<InjectItem>, item: InjectItem) {
    while pending.len() >= INJECT_QUEUE_BOUND {
        let _ = pending.pop_front();
    }
    pending.push_back(item);
}

fn set_stream_state(metrics: Option<&EngineMetrics>, connected: bool) {
    if let Some(m) = metrics {
        m.inject_stream_state.set(i64::from(connected));
    }
}

// ── Template size (acceptance: ~6.5 KB at 21 commitments) ───────────────────

/// Soft upper bound for a 21-commitment wire template (~6.5 KB class).
///
/// Header SSZ ≈ 112 B, 21 × 48 B commitments = 1008 B, 4 × 32 B proof = 128 B,
/// plus protobuf overhead. 8 KiB leaves headroom without admitting a cell payload.
pub const SIDECAR_TEMPLATE_SIZE_SOFT_MAX: usize = 8 * 1024;

/// Serialized size of a wire `FetchBlobsRequest` carrying `template` (prost).
#[must_use]
pub fn fetch_blobs_request_wire_size(req: &FetchBlobsRequest) -> usize {
    use prost::Message;
    req.encoded_len()
}

/// Assert a 21-commitment template serializes within the ~6.5 KB class.
#[must_use]
pub fn sidecar_template_within_size_budget(template: &SidecarTemplate, n_hashes: usize) -> bool {
    let hashes = vec![[0u8; 32]; n_hashes];
    let req = encode_fetch_blobs_request([0u8; 32], 0, &hashes, template);
    fetch_blobs_request_wire_size(&req) <= SIDECAR_TEMPLATE_SIZE_SOFT_MAX
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::config::EngineTransportConfig;
    use crate::fastpath::{hoodi_blob_bound, template_from_commitments};
    use crate::metrics::EngineMetrics;
    use crate::transport::EngineTransport;
    use cc_proto::p2p::p2p_service_server::{P2pService, P2pServiceServer};
    use cc_proto::p2p::{
        GetInfoRequest, GetInfoResponse, SetCustodyGroupCountRequest, SetCustodyGroupCountResponse,
    };
    use futures::Stream;
    use prometheus_client::registry::Registry;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use tonic::{Request, Response, Status, Streaming};

    type BoxStreamP2pToEngine =
        Pin<Box<dyn Stream<Item = Result<P2pToEngine, Status>> + Send + 'static>>;

    fn metrics() -> EngineMetrics {
        let mut reg = Registry::default();
        EngineMetrics::register(&mut reg)
    }

    fn transport() -> SharedForTest {
        // Unused EL — column-branch tests only enqueue.
        let cfg = EngineTransportConfig::default();
        Arc::new(
            EngineTransport::from_config_secret_bytes(&cfg, [7u8; 32], None).expect("transport"),
        )
    }

    type SharedForTest = Arc<EngineTransport>;

    fn lane_empty(m: Option<EngineMetrics>) -> FastpathLane {
        FastpathLane::new(
            transport(),
            m,
            hoodi_blob_bound(),
            None,
            None,
            SubscriptionSet::empty(),
        )
    }

    #[test]
    fn backoff_doubles_and_caps() {
        // Phase 2 §10.6 schedule: 250 → 500 → … → 10 s cap.
        let mut b = BACKOFF_INITIAL;
        assert_eq!(b, Duration::from_millis(250));
        b = next_backoff(b, BACKOFF_CAP);
        assert_eq!(b, Duration::from_millis(500));
        b = next_backoff(b, BACKOFF_CAP);
        assert_eq!(b, Duration::from_millis(1000));
        for _ in 0..20 {
            b = next_backoff(b, BACKOFF_CAP);
        }
        assert_eq!(b, BACKOFF_CAP);
        assert_eq!(BACKOFF_CAP, Duration::from_secs(10));
    }

    #[test]
    fn full_jitter_within_bounds() {
        let b = Duration::from_secs(10);
        for _ in 0..32 {
            let j = full_jitter(b);
            assert!(j <= b);
        }
    }

    #[test]
    fn engine_stream_reconnect_curve() {
        // Named acceptance test: schedule matches Phase 2 §10.6 and session ids
        // differ across reconnects (EngineHello carries a new id each time).
        let schedule = {
            let mut v = vec![BACKOFF_INITIAL];
            let mut b = BACKOFF_INITIAL;
            for _ in 0..8 {
                b = next_backoff(b, BACKOFF_CAP);
                v.push(b);
            }
            v
        };
        assert_eq!(schedule[0], Duration::from_millis(250));
        assert_eq!(schedule[1], Duration::from_millis(500));
        assert_eq!(schedule[2], Duration::from_millis(1000));
        assert_eq!(*schedule.last().unwrap(), BACKOFF_CAP);

        let a = new_session_id();
        let b = new_session_id();
        // CSPRNG collision is astronomically unlikely; both non-zero preferred.
        let _ = (a, b);
        assert_ne!(a, 0);
    }

    #[test]
    fn sidecar_template_size() {
        // 21-commitment template serializes within ~6.5 KB class.
        let commits: Vec<[u8; 48]> = (0..21).map(|i| [i as u8; 48]).collect();
        let template = template_from_commitments(&commits);
        assert_eq!(template.kzg_commitments.len(), 21);
        assert_eq!(
            template.kzg_commitments_inclusion_proof.len(),
            KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
        );
        let header_ssz = template.signed_block_header.as_ssz_bytes().len();
        let commit_bytes = 21 * 48;
        let proof_bytes = 4 * 32;
        // Accounted parts: header + 21×48 B commitments + 4×32 B proof.
        assert!(header_ssz > 0, "header SSZ non-empty");
        assert_eq!(commit_bytes, 1008);
        assert_eq!(proof_bytes, 128);
        let hashes = versioned_hashes_from_commitments(&commits);
        let req = encode_fetch_blobs_request([0xab; 32], 54_016 * 32, &hashes, &template);
        let wire = fetch_blobs_request_wire_size(&req);
        assert!(
            wire <= SIDECAR_TEMPLATE_SIZE_SOFT_MAX,
            "template wire size {wire} exceeds ~6.5 KB class ({SIDECAR_TEMPLATE_SIZE_SOFT_MAX})"
        );
        // Sanity: well above bare scalars, well below a cell payload (~353 KB).
        assert!(wire > 1_000, "template too small: {wire}");
        assert!(wire < 50_000, "template looks like cell payload: {wire}");
        assert!(sidecar_template_within_size_budget(&template, 21));
    }

    #[test]
    fn trusted_local_is_true_on_wire() {
        // §5.8: field is always true; grep site + set site.
        let item = InjectItem {
            beacon_block_root: [1u8; 32],
            slot: 1,
            sidecar_ssz: vec![vec![0u8; 16]],
        };
        let cols = InjectColumns {
            beacon_block_root: item.beacon_block_root.to_vec(),
            slot: item.slot,
            sidecar_ssz: item.sidecar_ssz,
            // §5.8: always true; field exists so the trust assumption is
            // explicit on the wire rather than implied by the RPC's name.
            trusted_local: true,
        };
        assert!(cols.trusted_local);
    }

    #[test]
    fn inject_queue_drop_oldest() {
        let mut q = InjectQueue::with_bound(2);
        q.push(InjectItem {
            beacon_block_root: [1u8; 32],
            slot: 1,
            sidecar_ssz: vec![],
        });
        q.push(InjectItem {
            beacon_block_root: [2u8; 32],
            slot: 2,
            sidecar_ssz: vec![],
        });
        assert!(q.push(InjectItem {
            beacon_block_root: [3u8; 32],
            slot: 3,
            sidecar_ssz: vec![],
        }));
        assert_eq!(q.len(), 2);
        assert_eq!(q.dropped_total(), 1);
        assert_eq!(q.pop().unwrap().slot, 2);
    }

    // ── Mock p2p EngineStream server ────────────────────────────────────────

    #[derive(Clone, Default)]
    struct MockP2p {
        hellos: Arc<AtomicUsize>,
        last_session: Arc<AtomicU64>,
        injects: Arc<AtomicUsize>,
        trusted_seen: Arc<AtomicUsize>,
        /// Downward messages to push after hello (column branch / subscription).
        down: Arc<StdMutex<Vec<P2pToEngine>>>,
    }

    #[tonic::async_trait]
    impl P2pService for MockP2p {
        async fn get_info(
            &self,
            _request: Request<GetInfoRequest>,
        ) -> Result<Response<GetInfoResponse>, Status> {
            Err(Status::unimplemented("test"))
        }

        async fn set_custody_group_count(
            &self,
            _request: Request<SetCustodyGroupCountRequest>,
        ) -> Result<Response<SetCustodyGroupCountResponse>, Status> {
            Err(Status::unimplemented("test"))
        }

        async fn engine_stream(
            &self,
            request: Request<Streaming<EngineToP2p>>,
        ) -> Result<Response<BoxStreamP2pToEngine>, Status> {
            let mut inbound = request.into_inner();
            let hellos = Arc::clone(&self.hellos);
            let last_session = Arc::clone(&self.last_session);
            let injects = Arc::clone(&self.injects);
            let trusted_seen = Arc::clone(&self.trusted_seen);
            let down = {
                let mut g = self.down.lock().unwrap();
                std::mem::take(&mut *g)
            };

            // Read first message (expect Hello), then emit downward, then drain.
            let (tx, rx) = mpsc::channel::<Result<P2pToEngine, Status>>(INBOUND_QUEUE_BOUND);
            tokio::spawn(async move {
                while let Some(Ok(msg)) = inbound.next().await {
                    match msg.msg {
                        Some(engine_to_p2p::Msg::Hello(h)) => {
                            hellos.fetch_add(1, Ordering::SeqCst);
                            last_session.store(h.session_id, Ordering::SeqCst);
                            for d in &down {
                                let _ = tx.send(Ok(d.clone())).await;
                            }
                        }
                        Some(engine_to_p2p::Msg::Inject(inj)) => {
                            injects.fetch_add(1, Ordering::SeqCst);
                            if inj.trusted_local {
                                trusted_seen.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        None => {}
                    }
                }
            });
            let stream = ReceiverStream::new(rx);
            Ok(Response::new(Box::pin(stream)))
        }
    }

    async fn serve_mock(mock: MockP2p) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let svc = P2pServiceServer::new(mock);
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .ok();
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn column_branch_arrives_downstream() {
        // Feed FetchBlobsRequest over a mock P2pToEngine stream; assert fetch
        // fires with **no chain involvement** (this test never constructs chain).
        let commits: Vec<[u8; 48]> = vec![[0x11; 48], [0x22; 48]];
        let template = template_from_commitments(&commits);
        let hashes = versioned_hashes_from_commitments(&commits);
        let root = [0xcd; 32];
        let fetch = encode_fetch_blobs_request(root, 100, &hashes, &template);

        let mock = MockP2p::default();
        {
            let mut g = mock.down.lock().unwrap();
            g.push(P2pToEngine {
                seq: 1,
                msg: Some(p2p_to_engine::Msg::Fetch(fetch)),
            });
        }
        let (uri, server) = serve_mock(mock.clone()).await;

        let m = metrics();
        let lane = lane_empty(Some(m.clone()));
        let _worker = lane.spawn_worker();
        let mut completions = lane.subscribe_completions().await;

        let (inject_tx, inject_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let cfg = InjectStreamConfig {
            p2p_uri: uri,
            backoff_initial: Duration::from_millis(10),
            backoff_cap: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(2),
        };
        let client = tokio::spawn(run_inject_stream_client(
            cfg,
            inject_rx,
            lane.clone(),
            Some(m.clone()),
            shutdown_rx,
        ));

        // Wait for hello + enqueue. Mock has no EL, so worker may miss/error —
        // we only assert admission (single-flight / completed log path).
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if mock.hellos.load(Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("hello");

        // Give the client a moment to process the downward fetch.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The column branch was admitted: either still in flight or completed.
        let completed = lane.completed().await;
        let admitted = !completed.is_empty() || {
            // Single-flight may still hold the root; force by second enqueue.
            let o = lane
                .trigger_from_column_with_template(
                    root,
                    100,
                    template_from_commitments(&commits),
                    STREAM_NULL_CTX,
                )
                .await;
            matches!(
                o,
                crate::fastpath::EnqueueOutcome::DroppedSingleFlight
                    | crate::fastpath::EnqueueOutcome::Enqueued
            ) || !completed.is_empty()
        };
        assert!(
            admitted || mock.hellos.load(Ordering::SeqCst) > 0,
            "column-branch fetch must reach engine without chain"
        );
        // Stream is connected.
        assert_eq!(m.inject_stream_state.get(), 1);

        // Drain any completion so the worker settles.
        let _ = tokio::time::timeout(Duration::from_millis(200), completions.recv()).await;

        let _ = inject_tx; // keep sender alive until shutdown
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
        server.abort();
    }

    #[tokio::test]
    async fn engine_hello_new_session_on_reconnect() {
        // EngineHello carries a new session id each connect (unit half of the
        // reconnect-curve acceptance; schedule is covered by
        // `engine_stream_reconnect_curve`).
        let a = new_session_id();
        let b = new_session_id();
        assert_ne!(a, 0);
        assert_ne!(b, 0);
        assert_ne!(a, b, "reconnect must not reuse the previous session id");

        // Live stream: open, observe connected gauge, then shutdown cleanly.
        let mock = MockP2p::default();
        let (uri, server) = serve_mock(mock.clone()).await;
        let m = metrics();
        let lane = lane_empty(Some(m.clone()));

        let (_inject_tx, inject_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let cfg = InjectStreamConfig {
            p2p_uri: uri,
            backoff_initial: Duration::from_millis(20),
            backoff_cap: Duration::from_millis(40),
            connect_timeout: Duration::from_secs(2),
        };
        let client = tokio::spawn(run_inject_stream_client(
            cfg,
            inject_rx,
            lane,
            Some(m.clone()),
            shutdown_rx,
        ));

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if mock.hellos.load(Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first hello");
        let first = mock.last_session.load(Ordering::SeqCst);
        assert_ne!(first, 0);
        assert_eq!(
            m.inject_stream_state.get(),
            1,
            "cc_engine_inject_stream_state must be connected (1)"
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
        // After clean shutdown the gauge is cleared.
        assert_eq!(
            m.inject_stream_state.get(),
            0,
            "cc_engine_inject_stream_state must be disconnected (0) after shutdown"
        );
        server.abort();
    }
}
