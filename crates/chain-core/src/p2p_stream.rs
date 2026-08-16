//! Chain-side `P2pStream` server and `ChainView` producer (CC-27a, Architecture §10).
//!
//! - Bidirectional gRPC stream: `p2p` dials, `chain` serves (§10.1).
//! - On `StreamHello` → full `ChainView` (fields 1–13).
//! - Cadence (§10.2): fields 1–8 on head change + slot tick; fields 11–13 only on
//!   epoch tick. Source: [`HeadSnapshotStore`] + [`EpochContextStore`] pointer
//!   loads — never the core command channel (Phase 1 §7.1 property restated).
//! - **Production drivers** (not test-only): epoch-sequence watcher + wall-clock
//!   slot ticker fan out [`ViewTick`] to all live sessions.
//! - Live sessions re-read `core` via shared `Arc<RwLock<…>>` so `install_core`
//!   is visible without reconnect.
//! - `GossipObject` of kind `BLOCK` → `ImportBlock` on the core; reply `Verdict`.
//! - Non-block kinds (attestation / sync / operations) are **discarded** with a
//!   counter (CC-2C/2D/2B Phase-5 seam — no pool in Phase 2).
//! - `PublishRequest` outward path: topic validation + enqueue onto live sessions
//!   (§10.5). Unknown topic → structured `INVALID_ARGUMENT` / `UNKNOWN_TOPIC`.
//! - `ColumnSidecar` is decoded into a typed `ColumnBatch` and ingested
//!   via `ArchiveWrite` (S2-A-05). Column bytes do not enter the ring.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
use cc_proto::p2p::{
    Acceptance, ChainToP2p, ChainView, GossipObject, ImportResult, ObjectKind, P2pToChain,
    PublishRequest, Reason, Verdict, chain_to_p2p, p2p_to_chain,
};
use cc_proto::status_with_error_info;
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status};

use crate::core::CoreHandle;
use crate::epoch_context::EpochContextStore;
use crate::events::EventInput;
use crate::head::HeadSnapshotStore;
use crate::service::ERROR_DOMAIN;
use crate::{ArchiveWriteHandle, decode_column_batch};

// ── view_kind constants (Architecture §10.2) ────────────────────────────────

/// Slot-tick `ChainView` (fields 1–8 only for epoch-scoped payload).
pub const VIEW_KIND_SLOT_TICK: u64 = 1;
/// Epoch-tick `ChainView` (fields 1–8 + 11–13).
pub const VIEW_KIND_EPOCH_TICK: u64 = 2;
/// Head-change `ChainView` (fields 1–8 only).
pub const VIEW_KIND_HEAD_CHANGE: u64 = 3;
/// Full snapshot on `StreamHello` / reconnect (fields 1–13).
pub const VIEW_KIND_FULL: u64 = 4;

/// Bound for outbound per-session channels (Architecture §2.2 / CC-27/3).
pub const STREAM_OUTBOUND_CAPACITY: usize = 1024;

/// Max concurrent `P2pStream` sessions (one honest p2p + headroom).
pub const MAX_P2P_STREAM_SESSIONS: u64 = 8;

/// gRPC `ErrorInfo.reason` for an unknown publish topic (§10.5).
pub const REASON_UNKNOWN_TOPIC: &str = "UNKNOWN_TOPIC";

/// gRPC `ErrorInfo.reason` when the concurrent session cap is hit.
pub const REASON_STREAM_SESSION_LIMIT: &str = "STREAM_SESSION_LIMIT";

/// Known Fulu topic path segments accepted by the publish handler.
///
/// Matches `services/p2p/src/gossip/topics.rs` family names (subnet id stripped).
const KNOWN_TOPIC_FAMILIES: &[&str] = &[
    "beacon_block",
    "beacon_aggregate_and_proof",
    "beacon_attestation",
    "data_column_sidecar",
    "sync_committee_contribution_and_proof",
    "sync_committee",
    "voluntary_exit",
    "proposer_slashing",
    "attester_slashing",
    "bls_to_execution_change",
];

/// Control events that drive the `ChainView` producer (production + tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewTick {
    /// Wall-clock / synthetic slot advance.
    Slot,
    /// Epoch boundary (lookahead payload included).
    Epoch,
    /// Head snapshot sequence advanced.
    HeadChange,
}

/// Live session state shared across all open streams and `install_core`.
///
/// - `head` / `epoch` are `ArcSwap` stores (clone shares identity).
/// - `core` is behind a shared lock so bootstrap install is visible to sessions
///   opened before the core was ready (F2).
#[derive(Clone)]
pub struct P2pStreamDeps {
    pub head: HeadSnapshotStore,
    pub epoch: EpochContextStore,
    /// Shared with [`crate::service::ChainServiceImpl`]; re-read on each import.
    pub core: Arc<RwLock<Option<CoreHandle>>>,
    /// Broadcast of [`ViewTick`] so sessions push `ChainView` on cadence.
    pub ticks: broadcast::Sender<ViewTick>,
    /// Outward publish fan-out: every live session listens.
    pub publish_tx: broadcast::Sender<PublishRequest>,
    /// Live session counter for the concurrent-session bound.
    session_count: Arc<AtomicU64>,
    /// Non-block gossip objects discarded (CC-2B/C/D — no pool in Phase 2).
    ///
    /// Proves validated sync / attestation / operation messages **arrive** on
    /// the stream even though chain retains nothing.
    pub gossip_discarded: Arc<AtomicU64>,
    /// Sync-committee family discards specifically (CC-2D counter).
    pub sync_discarded: Arc<AtomicU64>,
    /// Producer for observer events. Columns do not ride this lane (S2-A-05).
    pub event_tx: Option<mpsc::Sender<EventInput>>,
    /// Incremented on every column SSZ decode attempt (S2-A-05).
    pub column_decode_attempts: Arc<AtomicU64>,
    /// Typed archive ingest. `None` fail-closes the column path (J-01 injects).
    pub archive: Option<ArchiveWriteHandle>,
}

impl std::fmt::Debug for P2pStreamDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2pStreamDeps")
            .field("archive", &self.archive.as_ref().map(|_| "Some"))
            .field("column_decode_attempts", &self.column_decode_attempts())
            .finish_non_exhaustive()
    }
}

impl P2pStreamDeps {
    /// Build deps with a fresh tick/publish bus and start production cadence drivers.
    pub fn new(
        head: HeadSnapshotStore,
        epoch: EpochContextStore,
        core: Arc<RwLock<Option<CoreHandle>>>,
    ) -> Self {
        Self::with_events(head, epoch, core, None)
    }

    /// Like [`Self::new`], with an optional event-bus producer.
    pub fn with_events(
        head: HeadSnapshotStore,
        epoch: EpochContextStore,
        core: Arc<RwLock<Option<CoreHandle>>>,
        event_tx: Option<mpsc::Sender<EventInput>>,
    ) -> Self {
        Self::with_archive(head, epoch, core, event_tx, None)
    }

    /// Like [`Self::with_events`], with a typed archive ingest handle.
    pub fn with_archive(
        head: HeadSnapshotStore,
        epoch: EpochContextStore,
        core: Arc<RwLock<Option<CoreHandle>>>,
        event_tx: Option<mpsc::Sender<EventInput>>,
        archive: Option<ArchiveWriteHandle>,
    ) -> Self {
        let (ticks, _) = broadcast::channel(64);
        let (publish_tx, _) = broadcast::channel(64);
        // Process-wide drivers: epoch ArcSwap sequence + wall-clock slot ticks.
        spawn_epoch_sequence_driver(epoch.clone(), ticks.clone());
        spawn_slot_tick_driver(epoch.clone(), ticks.clone());
        Self {
            head,
            epoch,
            core,
            ticks,
            publish_tx,
            session_count: Arc::new(AtomicU64::new(0)),
            gossip_discarded: Arc::new(AtomicU64::new(0)),
            sync_discarded: Arc::new(AtomicU64::new(0)),
            event_tx,
            column_decode_attempts: Arc::new(AtomicU64::new(0)),
            archive,
        }
    }

    /// Column SSZ-decode attempts (S2-A-05: no longer 0-by-construction).
    #[must_use]
    pub fn column_decode_attempts(&self) -> u64 {
        self.column_decode_attempts.load(Ordering::Relaxed)
    }

    /// Total non-block gossip objects discarded (tests / CC-2D seam).
    #[must_use]
    pub fn gossip_discarded_count(&self) -> u64 {
        self.gossip_discarded.load(Ordering::Relaxed)
    }

    /// Sync-committee family discards (tests / CC-2D).
    #[must_use]
    pub fn sync_discarded_count(&self) -> u64 {
        self.sync_discarded.load(Ordering::Relaxed)
    }

    /// Notify all sessions of a view tick (tests + internal drivers).
    pub fn notify_tick(&self, tick: ViewTick) {
        let _ = self.ticks.send(tick);
    }

    /// Current core handle (install-visible).
    pub fn core_handle(&self) -> Option<CoreHandle> {
        self.core
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|p| p.into_inner().clone())
    }

    /// Install / replace the core visible to live sessions.
    pub fn set_core(&self, handle: Option<CoreHandle>) {
        match self.core.write() {
            Ok(mut g) => *g = handle,
            Err(p) => *p.into_inner() = handle,
        }
    }

    /// Validate topic and fan-out a `PublishRequest` to live stream sessions.
    ///
    /// Returns a structured `UNKNOWN_TOPIC` error for unrecognised topic families.
    /// End-to-end gossip exercise is CC-27b.
    pub fn request_publish(&self, req: PublishRequest) -> Result<(), Status> {
        validate_publish_topic(&req.topic)?;
        // Zero subscribers is fine: no connected p2p yet (Phase 2 / tests).
        let _ = self.publish_tx.send(req);
        Ok(())
    }

    fn try_acquire_session(&self) -> Result<SessionGuard, Status> {
        loop {
            let cur = self.session_count.load(Ordering::Relaxed);
            if cur >= MAX_P2P_STREAM_SESSIONS {
                return Err(status_with_error_info(
                    Code::ResourceExhausted,
                    format!(
                        "P2pStream concurrent session limit ({MAX_P2P_STREAM_SESSIONS}) reached"
                    ),
                    REASON_STREAM_SESSION_LIMIT,
                    ERROR_DOMAIN,
                ));
            }
            if self
                .session_count
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(SessionGuard {
                    count: Arc::clone(&self.session_count),
                });
            }
        }
    }
}

/// Decrements the session counter on drop.
struct SessionGuard {
    count: Arc<AtomicU64>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Watch [`EpochContextStore::sequence`] and fan out [`ViewTick::Epoch`].
///
/// Core publishes a new context at each epoch boundary; this makes the push
/// reach live sessions without a test calling [`P2pStreamDeps::notify_tick`].
fn spawn_epoch_sequence_driver(epoch: EpochContextStore, ticks: broadcast::Sender<ViewTick>) {
    tokio::spawn(async move {
        let mut last = epoch.load().sequence;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = epoch.load().sequence;
            if now != last {
                last = now;
                // sequence 0 is the empty default; only fire on real publishes.
                if now > 0 {
                    let _ = ticks.send(ViewTick::Epoch);
                }
            }
        }
    });
}

/// Wall-clock slot ticks from `genesis_time` / `seconds_per_slot` on the epoch store.
///
/// Silent while genesis is unset (pre-bootstrap). Aligns to the next slot boundary.
fn spawn_slot_tick_driver(epoch: EpochContextStore, ticks: broadcast::Sender<ViewTick>) {
    tokio::spawn(async move {
        let mut last_emitted_slot: Option<u64> = None;
        loop {
            let ctx = epoch.load();
            let genesis = ctx.genesis_time;
            let sps = ctx.seconds_per_slot;
            if genesis == 0 || sps == 0 {
                // Pre-bootstrap / unset clock — poll slowly for config appearance.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let now = unix_now_secs();
            let current_slot = if now <= genesis {
                0
            } else {
                (now - genesis) / sps
            };
            if last_emitted_slot != Some(current_slot) {
                // First observation or slot advanced.
                if last_emitted_slot.is_some() {
                    let _ = ticks.send(ViewTick::Slot);
                }
                last_emitted_slot = Some(current_slot);
            }
            // Sleep until next slot boundary (or 50 ms floor for short slots).
            let next_start =
                genesis.saturating_add(current_slot.saturating_add(1).saturating_mul(sps));
            let sleep_secs = next_start.saturating_sub(now).max(1);
            let sleep = Duration::from_secs(sleep_secs).min(Duration::from_secs(sps.max(1)));
            tokio::time::sleep(sleep.max(Duration::from_millis(50))).await;
        }
    });
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Reject unknown publish topics with `INVALID_ARGUMENT` + `UNKNOWN_TOPIC`.
pub fn validate_publish_topic(topic: &str) -> Result<(), Status> {
    let segment = topic_path_segment(topic);
    let family = topic_family(segment);
    if KNOWN_TOPIC_FAMILIES.contains(&family) {
        return Ok(());
    }
    Err(status_with_error_info(
        Code::InvalidArgument,
        format!("unknown publish topic: {topic}"),
        REASON_UNKNOWN_TOPIC,
        ERROR_DOMAIN,
    ))
}

/// Strip `/eth2/<digest>/` prefix and `/ssz_snappy` suffix if present.
fn topic_path_segment(topic: &str) -> &str {
    let t = topic.trim_start_matches('/');
    // Full form: eth2/<digest>/<name>/ssz_snappy
    if let Some(rest) = t.strip_prefix("eth2/") {
        let mut parts = rest.split('/');
        let _digest = parts.next();
        if let Some(name) = parts.next() {
            return name;
        }
    }
    // Bare path segment or name with trailing /ssz_snappy.
    t.trim_end_matches("/ssz_snappy")
        .rsplit('/')
        .next()
        .unwrap_or(t)
}

/// Map `beacon_attestation_7` → `beacon_attestation` (family check).
fn topic_family(segment: &str) -> &str {
    // Families with `_{id}` suffix: strip trailing digits after last underscore
    // only when the prefix is a known subnet family.
    for prefix in [
        "beacon_attestation_",
        "data_column_sidecar_",
        "sync_committee_",
    ] {
        if let Some(rest) = segment.strip_prefix(prefix)
            && rest.chars().all(|c| c.is_ascii_digit())
        {
            return prefix.trim_end_matches('_');
        }
    }
    // `sync_committee_contribution_and_proof` must not be stripped by the
    // `sync_committee_` rule above (no pure-digit suffix).
    segment
}

/// Build a `ChainView` from the two snapshot stores.
///
/// When `include_epoch_payload` is false, fields 11–13 are left empty (slot tick
/// / head change cadence).
pub fn build_chain_view(
    head: &HeadSnapshotStore,
    epoch: &EpochContextStore,
    view_kind: u64,
    include_epoch_payload: bool,
) -> ChainView {
    let snap = head.load();
    let ctx = epoch.load();
    let slots_per_epoch = ctx.slots_per_epoch.max(1);
    let head_slot = snap.head_slot.as_u64();
    let epoch_num = head_slot / slots_per_epoch;

    let mut view = ChainView {
        slot: head_slot,
        epoch: epoch_num,
        head_root: snap.head_root.as_slice().to_vec(),
        head_slot,
        finalized_root: snap.finalized.root.as_slice().to_vec(),
        finalized_epoch: snap.finalized.epoch.as_u64(),
        justified_root: snap.justified.root.as_slice().to_vec(),
        justified_epoch: snap.justified.epoch.as_u64(),
        genesis_time: ctx.genesis_time,
        genesis_validators_root: ctx.genesis_validators_root.as_slice().to_vec(),
        proposer_lookahead: Vec::new(),
        proposer_pubkeys: Vec::new(),
        active_validator_count: 0,
        view_kind,
    };
    if include_epoch_payload {
        view.proposer_lookahead = ctx.proposer_lookahead.clone();
        view.proposer_pubkeys = ctx.proposer_pubkeys.clone();
        view.active_validator_count = ctx.active_validator_count;
    }
    view
}

/// Outbound stream item type for tonic.
pub type BoxStreamChainToP2p =
    Pin<Box<dyn Stream<Item = Result<ChainToP2p, Status>> + Send + 'static>>;

/// Serve one `P2pStream` session.
pub async fn serve_p2p_stream(
    deps: P2pStreamDeps,
    inbound: impl Stream<Item = Result<P2pToChain, Status>> + Send + Unpin + 'static,
) -> Result<BoxStreamChainToP2p, Status> {
    let session_guard = deps.try_acquire_session()?;
    let (out_tx, out_rx) = mpsc::channel::<Result<ChainToP2p, Status>>(STREAM_OUTBOUND_CAPACITY);
    let seq = Arc::new(AtomicU64::new(0));

    // Per-session outbound seq allocator.
    let next_seq = {
        let seq = Arc::clone(&seq);
        move || seq.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    };

    // Drive the session on a task so we return the stream immediately.
    // SessionGuard moves into the task and drops when the session ends.
    tokio::spawn(async move {
        let _guard = session_guard;
        run_session(deps, inbound, out_tx, next_seq).await;
    });

    Ok(Box::pin(ReceiverStream::new(out_rx)) as BoxStreamChainToP2p)
}

async fn run_session<F>(
    deps: P2pStreamDeps,
    mut inbound: impl Stream<Item = Result<P2pToChain, Status>> + Unpin,
    out_tx: mpsc::Sender<Result<ChainToP2p, Status>>,
    mut next_seq: F,
) where
    F: FnMut() -> u64 + Send + 'static,
{
    let mut ticks = deps.ticks.subscribe();
    let mut publishes = deps.publish_tx.subscribe();
    // Watch head sequence so we can push HEAD_CHANGE without a poller thread.
    let mut last_head_seq = deps.head.load().sequence;
    let mut head_watch = spawn_head_watcher(deps.head.clone());

    loop {
        tokio::select! {
            biased;
            msg = inbound.next() => {
                match msg {
                    None => break,
                    Some(Err(status)) => {
                        let _ = out_tx.send(Err(status)).await;
                        break;
                    }
                    Some(Ok(msg)) => {
                        if handle_inbound(
                            &deps,
                            msg,
                            &out_tx,
                            &mut next_seq,
                        ).await.is_err() {
                            break;
                        }
                    }
                }
            }
            tick = ticks.recv() => {
                match tick {
                    Ok(t) => {
                        if send_view_for_tick(&deps, t, &out_tx, &mut next_seq).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            pub_req = publishes.recv() => {
                match pub_req {
                    Ok(req) => {
                        let seq = next_seq();
                        let msg = ChainToP2p {
                            seq,
                            msg: Some(chain_to_p2p::Msg::Publish(req)),
                        };
                        if out_tx.send(Ok(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            changed = head_watch.changed() => {
                if changed.is_err() {
                    break;
                }
                let seq_now = *head_watch.borrow();
                if seq_now != last_head_seq {
                    last_head_seq = seq_now;
                    if send_view_for_tick(
                        &deps,
                        ViewTick::HeadChange,
                        &out_tx,
                        &mut next_seq,
                    ).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

/// Poll head snapshot sequence and surface changes via a [`watch`] channel.
fn spawn_head_watcher(head: HeadSnapshotStore) -> watch::Receiver<u64> {
    let (tx, rx) = watch::channel(head.load().sequence);
    tokio::spawn(async move {
        let mut last = head.load().sequence;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = head.load().sequence;
            if now != last {
                last = now;
                if tx.send(now).is_err() {
                    break;
                }
            }
        }
    });
    rx
}

async fn send_view_for_tick<F>(
    deps: &P2pStreamDeps,
    tick: ViewTick,
    out_tx: &mpsc::Sender<Result<ChainToP2p, Status>>,
    next_seq: &mut F,
) -> Result<(), ()>
where
    F: FnMut() -> u64,
{
    let (view_kind, include_epoch) = match tick {
        ViewTick::Slot => (VIEW_KIND_SLOT_TICK, false),
        ViewTick::Epoch => (VIEW_KIND_EPOCH_TICK, true),
        ViewTick::HeadChange => (VIEW_KIND_HEAD_CHANGE, false),
    };
    // Always re-load ArcSwap stores (identity-shared; install does not orphan).
    let view = build_chain_view(&deps.head, &deps.epoch, view_kind, include_epoch);
    let seq = next_seq();
    let msg = ChainToP2p {
        seq,
        msg: Some(chain_to_p2p::Msg::View(view)),
    };
    out_tx.send(Ok(msg)).await.map_err(|_| ())
}

async fn handle_inbound<F>(
    deps: &P2pStreamDeps,
    msg: P2pToChain,
    out_tx: &mpsc::Sender<Result<ChainToP2p, Status>>,
    next_seq: &mut F,
) -> Result<(), ()>
where
    F: FnMut() -> u64,
{
    let Some(inner) = msg.msg else {
        return Ok(());
    };
    match inner {
        p2p_to_chain::Msg::Hello(hello) => {
            // Full ChainView on session open / reconnect (§10.6).
            // Re-load epoch store so install_core's publish is visible.
            let view = build_chain_view(&deps.head, &deps.epoch, VIEW_KIND_FULL, true);
            let seq = next_seq();
            let out = ChainToP2p {
                seq,
                msg: Some(chain_to_p2p::Msg::View(view)),
            };
            tracing::debug!(
                session_id = hello.session_id,
                resume_seq = hello.resume_seq,
                out_seq = seq,
                "p2p stream hello; sent full ChainView"
            );
            out_tx.send(Ok(out)).await.map_err(|_| ())?;
            Ok(())
        }
        p2p_to_chain::Msg::Object(obj) => {
            // CC-27c: may emit the gossip Verdict before the state transition.
            handle_gossip_object(deps, obj, out_tx, next_seq).await
        }
        p2p_to_chain::Msg::DataAvailable(da) => {
            // CC-24d: mark PeerDasAvailability + re-drive pending_da on the core.
            let Some(core) = deps.core_handle() else {
                tracing::debug!("DataAvailable while core absent; dropping");
                return Ok(());
            };
            let root = match crate::import::parse_root(&da.root) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "DataAvailable with invalid root");
                    return Ok(());
                }
            };
            if let Err(e) = core.notify_data_available(root, da.slot).await {
                tracing::warn!(error = %e, %root, slot = da.slot, "DataAvailable notify failed");
            }
            Ok(())
        }
        p2p_to_chain::Msg::Column(col) => {
            // S2-A-05: decode into a typed ColumnBatch and ingest. Column
            // bytes never enter the ring. Malformed SSZ is rejected.
            deps.column_decode_attempts.fetch_add(1, Ordering::Relaxed);
            let correlation_id = col.root.clone();
            let Ok(block_root) = <[u8; 32]>::try_from(col.root.as_slice()) else {
                tracing::error!(
                    root_len = col.root.len(),
                    "rejected column ingest with non-32-byte root"
                );
                return send_verdict(
                    out_tx,
                    next_seq,
                    Verdict {
                        correlation_id,
                        acceptance: Acceptance::Ignore as i32,
                        reason: Reason::Internal as i32,
                        import: ImportResult::None as i32,
                    },
                )
                .await;
            };
            if col.ssz.len() > crate::events::MAX_EVENT_PAYLOAD_BYTES {
                tracing::error!(
                    payload_len = col.ssz.len(),
                    cap = crate::events::MAX_EVENT_PAYLOAD_BYTES,
                    "rejected oversize column sidecar (SEC-44a-2)"
                );
                return send_verdict(
                    out_tx,
                    next_seq,
                    Verdict {
                        correlation_id,
                        acceptance: Acceptance::Ignore as i32,
                        reason: Reason::Internal as i32,
                        import: ImportResult::None as i32,
                    },
                )
                .await;
            }
            let batch = match decode_column_batch(bytes::Bytes::from(col.ssz), block_root) {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!(error = %e, "rejected malformed column sidecar");
                    return send_verdict(
                        out_tx,
                        next_seq,
                        Verdict {
                            correlation_id,
                            acceptance: Acceptance::Ignore as i32,
                            reason: Reason::Invalid as i32,
                            import: ImportResult::None as i32,
                        },
                    )
                    .await;
                }
            };
            let Some(archive) = deps.archive.as_ref() else {
                // J-01 wires the handle. Until then do not ACK AlreadyKnown
                // after a drop — the column was not stored.
                tracing::error!("column ingest unavailable: archive handle not injected");
                return send_verdict(
                    out_tx,
                    next_seq,
                    Verdict {
                        correlation_id,
                        acceptance: Acceptance::Ignore as i32,
                        reason: Reason::Internal as i32,
                        import: ImportResult::None as i32,
                    },
                )
                .await;
            };
            if let Err(e) = archive.ingest_columns(batch).await {
                tracing::error!(error = %e, "column ingest failed");
                return send_verdict(
                    out_tx,
                    next_seq,
                    Verdict {
                        correlation_id,
                        acceptance: Acceptance::Ignore as i32,
                        reason: Reason::Internal as i32,
                        import: ImportResult::None as i32,
                    },
                )
                .await;
            }
            send_verdict(
                out_tx,
                next_seq,
                Verdict {
                    correlation_id,
                    acceptance: Acceptance::Ignore as i32,
                    reason: Reason::AlreadyKnown as i32,
                    import: ImportResult::None as i32,
                },
            )
            .await
        }
    }
}

/// Handle one gossip object on the stream (CC-27c fast path).
///
/// When cheap gossip conditions pass, emits **exactly one** equality-term
/// `Verdict` (ACCEPT, `import=NONE`) **before** the state transition. A later
/// Reject-class import failure may send a **second** stream message that is
/// not an equality term — p2p applies `import_invalid` without re-reporting
/// gossip. `Internal` produces neither a second REJECT nor a penalty.
async fn handle_gossip_object<F>(
    deps: &P2pStreamDeps,
    obj: GossipObject,
    out_tx: &mpsc::Sender<Result<ChainToP2p, Status>>,
    next_seq: &mut F,
) -> Result<(), ()>
where
    F: FnMut() -> u64,
{
    let correlation_id = obj.root.clone();
    let kind = ObjectKind::try_from(obj.kind).unwrap_or(ObjectKind::Unspecified);

    // Only BLOCK is chain-authoritative in Phase 2 (ADR P2-04). Other kinds
    // travel up the stream so Phase 5/6 can later attach pools; until then
    // **discard** with a counter and reply IGNORE. No pool state is retained.
    if kind != ObjectKind::Block {
        note_non_block_discard(deps, kind);
        return send_verdict(
            out_tx,
            next_seq,
            Verdict {
                correlation_id,
                acceptance: Acceptance::Ignore as i32,
                reason: Reason::AlreadyKnown as i32,
                import: ImportResult::None as i32,
            },
        )
        .await;
    }

    // Re-read core on every object so install_core is visible to live sessions (F2).
    let Some(core) = deps.core_handle() else {
        return send_verdict(
            out_tx,
            next_seq,
            Verdict {
                correlation_id,
                acceptance: Acceptance::Ignore as i32,
                reason: Reason::Internal as i32,
                import: ImportResult::None as i32,
            },
        )
        .await;
    };

    let request = ImportBlockRequest {
        ssz: obj.ssz,
        fork: obj.fork,
        root: obj.root,
        source: obj.source,
    };

    let (early_tx, early_rx) = tokio::sync::oneshot::channel();
    let import_fut = core.import_block_for_gossip(request, Some(early_tx));
    tokio::pin!(import_fut);

    // Prefer early ACCEPT when both are ready (F4: deterministic, not random select).
    let mut early_emitted = false;
    tokio::select! {
        biased;
        early = early_rx => {
            if early.is_ok() {
                early_emitted = true;
                // Equality-term verdict: ACCEPT with import still open.
                send_verdict(
                    out_tx,
                    next_seq,
                    Verdict {
                        correlation_id: correlation_id.clone(),
                        acceptance: Acceptance::Accept as i32,
                        reason: Reason::Valid as i32,
                        import: ImportResult::None as i32,
                    },
                )
                .await?;
            }
        }
        outcome = &mut import_fut => {
            return emit_terminal_verdict(out_tx, next_seq, correlation_id, outcome).await;
        }
    }

    // Early ACCEPT was emitted; wait for import completion (no second equality verdict).
    let outcome = import_fut.await;
    match outcome {
        Ok(o) if early_emitted && o.late_import_reject => {
            // Late correction: stream message for app-score only (CC-27c / §5.3).
            // Not a gossip re-report; p2p equality ignores unknown correlation.
            send_verdict(
                out_tx,
                next_seq,
                Verdict {
                    correlation_id,
                    acceptance: Acceptance::Reject as i32,
                    reason: Reason::Invalid as i32,
                    import: ImportResult::Invalid as i32,
                },
            )
            .await
        }
        Ok(o) if early_emitted => {
            // Imported / deferred / internal — single equality verdict already sent.
            // Internal: explicitly no second REJECT and no penalty (Phase 1 §5.3).
            if o.late_import_internal {
                tracing::debug!("import failed Internal after early ACCEPT; no penalty, no REJECT");
            }
            Ok(())
        }
        other => emit_terminal_verdict(out_tx, next_seq, correlation_id, other).await,
    }
}

async fn emit_terminal_verdict<F>(
    out_tx: &mpsc::Sender<Result<ChainToP2p, Status>>,
    next_seq: &mut F,
    correlation_id: Vec<u8>,
    outcome: Result<crate::import::ImportOutcome, Status>,
) -> Result<(), ()>
where
    F: FnMut() -> u64,
{
    let verdict = match outcome {
        Ok(o) => map_import_verdict(correlation_id, o.response.verdict, &o.reason),
        Err(status) => {
            // Map gRPC failures onto Ignore/Internal so we never descore peers
            // for our own transport bugs (Phase 1 §5.3 Internal rule).
            let reason = if status.code() == Code::InvalidArgument {
                Reason::Invalid
            } else {
                Reason::Internal
            };
            let acceptance = if reason == Reason::Invalid {
                Acceptance::Reject
            } else {
                Acceptance::Ignore
            };
            Verdict {
                correlation_id,
                acceptance: acceptance as i32,
                reason: reason as i32,
                import: ImportResult::Invalid as i32,
            }
        }
    };
    send_verdict(out_tx, next_seq, verdict).await
}

async fn send_verdict<F>(
    out_tx: &mpsc::Sender<Result<ChainToP2p, Status>>,
    next_seq: &mut F,
    verdict: Verdict,
) -> Result<(), ()>
where
    F: FnMut() -> u64,
{
    let seq = next_seq();
    let out = ChainToP2p {
        seq,
        msg: Some(chain_to_p2p::Msg::Verdict(verdict)),
    };
    out_tx.send(Ok(out)).await.map_err(|_| ())
}

fn map_import_verdict(
    correlation_id: Vec<u8>,
    verdict: i32,
    detail: &crate::import::ImportReason,
) -> Verdict {
    // H3: typed reasons that are Ignore-class on the wire even when
    // the ImportBlockVerdict enum only has Invalid (proto gap for FUTURE_SLOT).
    match detail {
        crate::import::ImportReason::FutureSlot => {
            return Verdict {
                correlation_id,
                acceptance: Acceptance::Ignore as i32,
                reason: Reason::FutureSlot as i32,
                import: ImportResult::Invalid as i32,
            };
        }
        crate::import::ImportReason::TooOld => {
            return Verdict {
                correlation_id,
                acceptance: Acceptance::Ignore as i32,
                reason: Reason::AlreadyKnown as i32,
                import: ImportResult::Invalid as i32,
            };
        }
        crate::import::ImportReason::NotDescendedFromFinalized => {
            return Verdict {
                correlation_id,
                acceptance: Acceptance::Reject as i32,
                reason: Reason::NotDescendedFromFinalized as i32,
                import: ImportResult::Invalid as i32,
            };
        }
        crate::import::ImportReason::InternalProposerSig(_) => {
            return Verdict {
                correlation_id,
                acceptance: Acceptance::Ignore as i32,
                reason: Reason::Internal as i32,
                import: ImportResult::Invalid as i32,
            };
        }
        crate::import::ImportReason::None
        | crate::import::ImportReason::DataUnavailable
        | crate::import::ImportReason::UnknownParent
        | crate::import::ImportReason::ExecutionEngineUnavailable
        | crate::import::ImportReason::ProposerSigParentStateMissing
        | crate::import::ImportReason::Other(_) => {}
    }

    // ImportBlockVerdict → Acceptance / Reason / ImportResult.
    match ImportBlockVerdict::try_from(verdict).unwrap_or(ImportBlockVerdict::Unspecified) {
        ImportBlockVerdict::Imported => Verdict {
            correlation_id,
            acceptance: Acceptance::Accept as i32,
            reason: Reason::Valid as i32,
            import: ImportResult::Imported as i32,
        },
        ImportBlockVerdict::Duplicate => Verdict {
            correlation_id,
            acceptance: Acceptance::Ignore as i32,
            reason: Reason::Duplicate as i32,
            import: ImportResult::Duplicate as i32,
        },
        ImportBlockVerdict::DeferredDa => Verdict {
            correlation_id,
            acceptance: Acceptance::Ignore as i32,
            reason: Reason::DeferredDa as i32,
            import: ImportResult::DeferredDa as i32,
        },
        ImportBlockVerdict::UnknownParent => Verdict {
            correlation_id,
            acceptance: Acceptance::Ignore as i32,
            reason: Reason::UnknownParent as i32,
            import: ImportResult::UnknownParent as i32,
        },
        ImportBlockVerdict::Invalid => Verdict {
            correlation_id,
            acceptance: Acceptance::Reject as i32,
            reason: Reason::Invalid as i32,
            import: ImportResult::Invalid as i32,
        },
        ImportBlockVerdict::Unspecified => Verdict {
            correlation_id,
            acceptance: Acceptance::Ignore as i32,
            reason: Reason::Internal as i32,
            import: ImportResult::None as i32,
        },
    }
}

/// Count a non-block gossip object as discarded (CC-2B/C/D Phase-5 seam).
fn note_non_block_discard(deps: &P2pStreamDeps, kind: ObjectKind) {
    deps.gossip_discarded.fetch_add(1, Ordering::Relaxed);
    if matches!(
        kind,
        ObjectKind::SyncCommittee | ObjectKind::SyncContribution
    ) {
        deps.sync_discarded.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_proto::error_info_from_status;
    use cc_types::primitives::{Epoch, Root, Slot};

    use crate::epoch_context::EpochContext;
    use crate::head::HeadSnapshot;

    #[test]
    fn sync_discard_counter_proves_arrival_no_pool() {
        // CC-2D: validated sync messages travel up the stream; chain discards
        // them with a counter. No pool state exists.
        // Construct deps without `new()` so we do not spawn wall-clock drivers
        // (those require a tokio runtime).
        let (ticks, _) = broadcast::channel(4);
        let (publish_tx, _) = broadcast::channel(4);
        let deps = P2pStreamDeps {
            head: HeadSnapshotStore::default(),
            epoch: EpochContextStore::default(),
            core: Arc::new(RwLock::new(None)),
            ticks,
            publish_tx,
            session_count: Arc::new(AtomicU64::new(0)),
            gossip_discarded: Arc::new(AtomicU64::new(0)),
            sync_discarded: Arc::new(AtomicU64::new(0)),
            event_tx: None,
            column_decode_attempts: Arc::new(AtomicU64::new(0)),
            archive: None,
        };
        assert_eq!(deps.sync_discarded_count(), 0);
        assert_eq!(deps.gossip_discarded_count(), 0);

        note_non_block_discard(&deps, ObjectKind::SyncCommittee);
        note_non_block_discard(&deps, ObjectKind::SyncContribution);
        note_non_block_discard(&deps, ObjectKind::Attestation);

        assert_eq!(deps.sync_discarded_count(), 2);
        assert_eq!(deps.gossip_discarded_count(), 3);
        // Counter is the only retained state — deps hold no pool/queue for
        // these kinds.
    }

    #[test]
    fn validate_publish_topic_accepts_known_families() {
        validate_publish_topic("beacon_block").unwrap();
        validate_publish_topic("beacon_attestation_3").unwrap();
        validate_publish_topic("/eth2/c6ecb76c/voluntary_exit/ssz_snappy").unwrap();
        validate_publish_topic("data_column_sidecar_127").unwrap();
    }

    #[test]
    fn validate_publish_topic_rejects_unknown() {
        let err = validate_publish_topic("blob_sidecar_0").unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, REASON_UNKNOWN_TOPIC);
    }

    #[test]
    fn build_chain_view_cadence_strips_epoch_payload() {
        let head = HeadSnapshotStore::with_snapshot(HeadSnapshot {
            head_root: Root::from_array([1u8; 32]),
            head_slot: Slot::new(16),
            sequence: 1,
            finalized: cc_types::containers::Checkpoint {
                epoch: Epoch::new(1),
                root: Root::from_array([2u8; 32]),
            },
            justified: cc_types::containers::Checkpoint {
                epoch: Epoch::new(1),
                root: Root::from_array([3u8; 32]),
            },
            ..HeadSnapshot::default()
        });
        let epoch = EpochContextStore::with_context(EpochContext {
            epoch: Epoch::new(2),
            proposer_lookahead: vec![1, 2, 3],
            proposer_pubkeys: vec![vec![0xab; 48]],
            active_validator_count: 64,
            genesis_time: 1_000,
            genesis_validators_root: Root::from_array([9u8; 32]),
            seconds_per_slot: 6,
            slots_per_epoch: 8,
            sequence: 1,
        });

        let slot_view = build_chain_view(&head, &epoch, VIEW_KIND_SLOT_TICK, false);
        assert!(slot_view.proposer_lookahead.is_empty());
        assert!(slot_view.proposer_pubkeys.is_empty());
        assert_eq!(slot_view.active_validator_count, 0);
        assert_eq!(slot_view.head_slot, 16);
        assert_eq!(slot_view.epoch, 2); // 16 / 8
        assert_eq!(slot_view.view_kind, VIEW_KIND_SLOT_TICK);

        let epoch_view = build_chain_view(&head, &epoch, VIEW_KIND_EPOCH_TICK, true);
        assert_eq!(epoch_view.proposer_lookahead, vec![1, 2, 3]);
        assert_eq!(epoch_view.active_validator_count, 64);
        assert_eq!(epoch_view.view_kind, VIEW_KIND_EPOCH_TICK);
    }

    #[test]
    fn map_import_verdict_uses_typed_reason() {
        use crate::import::ImportReason;
        let id = vec![1u8; 32];
        let v = map_import_verdict(
            id.clone(),
            ImportBlockVerdict::Invalid as i32,
            &ImportReason::FutureSlot,
        );
        assert_eq!(v.reason, Reason::FutureSlot as i32);
        assert_eq!(v.acceptance, Acceptance::Ignore as i32);
        let v = map_import_verdict(
            id,
            ImportBlockVerdict::Invalid as i32,
            &ImportReason::TooOld,
        );
        assert_eq!(v.reason, Reason::AlreadyKnown as i32);
    }
}
