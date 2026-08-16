//! Resumable event bus: ring buffer, cursor, and per-subscriber fan-out
//! (CC-18c / §7.3; CC-44a data-bus payloads + byte ceiling).
//!
//! # Ownership and atomicity
//!
//! The **events task owns everything**: the ring, the sequence counter, and the
//! subscriber table. The core thread (or a synthetic test producer) is the single
//! producer and sends over a bounded `mpsc` using `send` / `blocking_send`, so a
//! wedged events task applies **backpressure to the producer** rather than silently
//! dropping events.
//!
//! Sequence numbers are assigned by the events task in receive order, which equals
//! producer send order because there is one producer.
//!
//! **Subscribe is atomic against publication** because both are handled by the same
//! single-threaded task: a `Subscribe { cursor, … }` command and an incoming event
//! are two arms of one `select!`, so the task copies the replay slice and inserts
//! the sender **without any window in between**. That — not the ring itself — is
//! what makes CC-18/1's "no gap, no duplicate" assertion structurally true rather
//! than empirically lucky. A later refactor that moves subscribe off this task
//! silently breaks that guarantee.
//!
//! # Cursor rejection
//!
//! Two distinct `FAILED_PRECONDITION` + `google.rpc.ErrorInfo` reasons (ADR-P1-11):
//!
//! | `reason`                 | Condition                                      |
//! |--------------------------|------------------------------------------------|
//! | `CURSOR_TOO_OLD`         | `cursor.seq + 1 < ring.front().seq` (evicted)  |
//! | `CURSOR_UNKNOWN_SESSION` | `cursor.session_id != ring.session_id`         |
//!
//! # Slow consumers
//!
//! Per-subscriber `mpsc` (default 256), **`try_send` only**. On `Full` the
//! subscriber is dropped and its stream terminated with `RESOURCE_EXHAUSTED`.
//! Conformance (`S1-A-11` / `[ARCH]` §2.2 policy B):
//! `events::slow_subscriber_is_terminated_not_stalled`.
//!
//! # Ring bounds (CC-44a / §4.3 — closes `OQ-P1-2`)
//!
//! Two bounds and the **byte ceiling binds first**: `chain.event_ring_events`
//! (default 4 096) and `chain.event_ring_bytes` (default 64 MiB). See
//! [`ring`] module docs for the 33–55 minute resume-window arithmetic.
//!
//! # Metrics
//!
//! [`Occupancy`] tracks ring length and the deepest subscriber queue depth for
//! `cc_chain_event_buffer_occupancy` (CC-1C registers the Prometheus family; this
//! module only maintains the live values). Bytes are **separate** gauges
//! (`cc_chain_event_buffer_bytes` / `_bytes_bound`) — not new label values on
//! the occupancy family.

mod cursor;
mod fanout;
mod ring;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_proto::chain::{Cursor, Event, EventKind};
use cc_proto::status_with_error_info;
use tokio::sync::{mpsc, oneshot};
use tonic::{Code, Status};

pub use cursor::{REASON_CURSOR_TOO_OLD, REASON_CURSOR_UNKNOWN_SESSION, validate_cursor};
pub use fanout::FanOut;
pub use ring::{EventRing, StoredEvent};

/// gRPC `ErrorInfo.domain` for chain cursor errors.
pub const ERROR_DOMAIN: &str = "eth.chain.v1";

/// Response metadata key for `SubscribeEvents` live `session_id` (CC-44b).
///
/// `Event` does not carry `session_id`; storage reads this on the initial
/// `SubscribeEvents` response so durable `WriteCursor` resume works after a
/// live-from-tip subscribe (without it, resume would stamp `session_id=0` and
/// re-attribute every reconnect as `CURSOR_UNKNOWN_SESSION`).
pub const SESSION_ID_METADATA_KEY: &str = "x-cc-chain-session-id";

/// Default event-ring **count** capacity (`chain.event_ring_events`; Architecture §4.3).
///
/// Raised from Phase 1's 1 024 so the **byte ceiling binds first** at every
/// plausible `cgc` (CC-44a / OQ-P1-2).
pub const DEFAULT_RING_CAPACITY: usize = 4096;

/// Default hard byte ceiling (`chain.event_ring_bytes` = 64 MiB; Architecture §4.3).
///
/// 33–55 minutes of resume window at `cgc = 8` (see [`ring`] module docs).
pub const DEFAULT_RING_BYTES: usize = 64 * 1024 * 1024;

/// Default per-subscriber live-queue capacity (Architecture §7.3).
pub const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

/// Hard cap on a single event's `payload` length before accept into the ring
/// (SEC-44a-2).
///
/// Matches the p2p req/resp uncompressed chunk ceiling (`MAX_PAYLOAD_SIZE` =
/// 10 MiB). Blocks already fail closed at 8 MiB on the checkpoint path; column
/// sidecars are far smaller. Anything larger is rejected **before** ring push
/// so one hostile frame cannot inflate occupancy by tens of megabytes.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Capacity of the command channel into the events task (subscribe / shutdown).
const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// Producer-side event before the task assigns a sequence number.
#[derive(Debug, Clone)]
pub struct EventInput {
    pub slot: u64,
    pub root: Bytes,
    pub kind: EventKind,
    pub payload: Bytes,
}

impl EventInput {
    /// Whether `payload` is within [`MAX_EVENT_PAYLOAD_BYTES`] (SEC-44a-2).
    #[must_use]
    pub fn payload_within_cap(&self) -> bool {
        self.payload.len() <= MAX_EVENT_PAYLOAD_BYTES
    }

    /// Convenience constructor with empty payload and `BLOCK_IMPORTED` kind.
    ///
    /// Production imports use [`Self::block_imported_with_payload`] so the SSZ
    /// body and verdict discriminator travel with the event (CC-44a / §4.2).
    pub fn block_imported(slot: u64, root: impl Into<Bytes>) -> Self {
        Self {
            slot,
            root: root.into(),
            kind: EventKind::BlockImported,
            payload: Bytes::new(),
        }
    }

    /// `BLOCK_IMPORTED` with payload = `[verdict_byte] ‖ SignedBeaconBlock SSZ`.
    pub fn block_imported_with_payload(
        slot: u64,
        root: impl Into<Bytes>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            slot,
            root: root.into(),
            kind: EventKind::BlockImported,
            payload: payload.into(),
        }
    }

    /// 32-byte root, or `None` — never pad/truncate a short or long slice.
    #[must_use]
    pub fn fixed_root(bytes: &[u8]) -> Option<Bytes> {
        <[u8; 32]>::try_from(bytes)
            .ok()
            .map(|r| Bytes::copy_from_slice(&r))
    }

    /// `HEAD` with payload = head slot as 8-byte little-endian.
    pub fn head(slot: u64, root: impl Into<Bytes>) -> Self {
        Self {
            slot,
            root: root.into(),
            kind: EventKind::Head,
            payload: Bytes::copy_from_slice(&slot.to_le_bytes()),
        }
    }

    /// `CHAIN_REORG` with payload = 32 B old head root ‖ 8 B common-ancestor slot LE.
    ///
    /// Short/long old-head roots are **not** padded or truncated to zeros.
    pub fn chain_reorg(
        new_head_slot: u64,
        new_head_root: impl Into<Bytes>,
        old_head_root: impl Into<Bytes>,
        common_ancestor_slot: u64,
    ) -> Self {
        let old = old_head_root.into();
        let payload = ChainReorgPayload::encode(old.as_ref(), common_ancestor_slot);
        Self {
            slot: new_head_slot,
            root: new_head_root.into(),
            kind: EventKind::ChainReorg,
            payload,
        }
    }

    /// `FINALIZED_CHECKPOINT` with payload = 8 B epoch LE ‖ 32 B state root ‖ SSZ scalars.
    ///
    /// Publishing this kind is the production trigger for
    /// [`cc_fork_choice::Store::prune_on_finalized`] (P0-10 / S0-A-21).
    pub fn finalized_checkpoint(
        epoch: u64,
        finalized_root: impl Into<Bytes>,
        state_root: impl Into<Bytes>,
        scalars_ssz: impl Into<Bytes>,
    ) -> Self {
        let state = state_root.into();
        let scalars = scalars_ssz.into();
        let payload = FinalizedCheckpointPayload::encode(epoch, state.as_ref(), scalars.as_ref());
        Self {
            // Slot is not load-bearing for this kind; use epoch start as a stable tag.
            slot: epoch.saturating_mul(32),
            root: finalized_root.into(),
            kind: EventKind::FinalizedCheckpoint,
            payload,
        }
    }

    /// `DATA_COLUMN` with payload = `DataColumnSidecar` SSZ, verbatim (no decode).
    pub fn data_column(
        slot: u64,
        block_root: impl Into<Bytes>,
        sidecar_ssz: impl Into<Bytes>,
    ) -> Self {
        Self {
            slot,
            root: block_root.into(),
            kind: EventKind::DataColumn,
            payload: sidecar_ssz.into(),
        }
    }
}

/// Typed `CHAIN_REORG` payload. Decode is fail-closed (no silent zero root).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainReorgPayload {
    pub old_head_root: [u8; 32],
    pub common_ancestor_slot: u64,
}

impl ChainReorgPayload {
    /// Encode. Non-32-byte roots are written as-is (never padded to `0`).
    #[must_use]
    pub fn encode(old_head_root: &[u8], common_ancestor_slot: u64) -> Bytes {
        let mut payload = Vec::with_capacity(old_head_root.len().saturating_add(8));
        payload.extend_from_slice(old_head_root);
        payload.extend_from_slice(&common_ancestor_slot.to_le_bytes());
        Bytes::from(payload)
    }

    /// Fail-closed: requires exactly 40 bytes.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 40 {
            return None;
        }
        let old_head_root = bytes.get(..32)?.try_into().ok()?;
        let slot: [u8; 8] = bytes.get(32..40)?.try_into().ok()?;
        Some(Self {
            old_head_root,
            common_ancestor_slot: u64::from_le_bytes(slot),
        })
    }
}

/// Typed `FINALIZED_CHECKPOINT` prefix: 8 B epoch ‖ 32 B state root ‖ scalars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedCheckpointPayload {
    pub epoch: u64,
    pub state_root: [u8; 32],
}

impl FinalizedCheckpointPayload {
    /// Encode. Non-32-byte state roots are written as-is (never padded to `0`).
    #[must_use]
    pub fn encode(epoch: u64, state_root: &[u8], scalars_ssz: &[u8]) -> Bytes {
        let mut payload = Vec::with_capacity(8 + state_root.len() + scalars_ssz.len());
        payload.extend_from_slice(&epoch.to_le_bytes());
        payload.extend_from_slice(state_root);
        payload.extend_from_slice(scalars_ssz);
        Bytes::from(payload)
    }

    /// Fail-closed prefix decode. Requires at least 40 bytes; scalars follow.
    #[must_use]
    pub fn decode_prefix(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 40 {
            return None;
        }
        let epoch: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
        let state_root = bytes.get(8..40)?.try_into().ok()?;
        Some(Self {
            epoch: u64::from_le_bytes(epoch),
            state_root,
        })
    }
}

/// Tunables for the events task. Count + byte bounds are config values.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct EventsConfig {
    /// Ring buffer entry capacity (`chain.event_ring_events`). Default **4096**.
    #[serde(default = "default_ring_capacity")]
    pub ring_capacity: usize,
    /// Hard byte ceiling (`chain.event_ring_bytes`). Default **64 MiB**.
    #[serde(default = "default_ring_bytes")]
    pub ring_bytes: usize,
    /// Per-subscriber live queue capacity. Default **256**.
    #[serde(default = "default_subscriber_queue_capacity")]
    pub subscriber_queue_capacity: usize,
    /// Override `session_id` (tests). `None` → random at spawn.
    #[serde(default, skip)]
    pub session_id: Option<u64>,
}

fn default_ring_capacity() -> usize {
    DEFAULT_RING_CAPACITY
}

fn default_ring_bytes() -> usize {
    DEFAULT_RING_BYTES
}

fn default_subscriber_queue_capacity() -> usize {
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            ring_capacity: DEFAULT_RING_CAPACITY,
            ring_bytes: DEFAULT_RING_BYTES,
            subscriber_queue_capacity: DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            session_id: None,
        }
    }
}

/// Live buffer-occupancy values for metrics (CC-1C / CC-44a).
///
/// Count gauges feed `cc_chain_event_buffer_occupancy{buffer=ring|subscriber}`;
/// byte gauges are **separate** series (`cc_chain_event_buffer_bytes` /
/// `cc_chain_event_buffer_bytes_bound`) — never labels on the occupancy family.
#[derive(Debug, Default)]
pub struct Occupancy {
    /// Current number of events retained in the ring.
    ring: AtomicUsize,
    /// Deepest per-subscriber live-queue depth (max across active subscribers).
    deepest_subscriber: AtomicUsize,
    /// Number of active subscribers (`cc_chain_subscribers`).
    subscribers: AtomicUsize,
    /// Accounted ring occupancy in bytes.
    bytes: AtomicUsize,
    /// Hard byte ceiling (constant after spawn).
    bytes_bound: AtomicUsize,
}

impl Occupancy {
    pub fn ring(&self) -> usize {
        self.ring.load(Ordering::Relaxed)
    }

    pub fn deepest_subscriber(&self) -> usize {
        self.deepest_subscriber.load(Ordering::Relaxed)
    }

    pub fn subscribers(&self) -> usize {
        self.subscribers.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn bytes_bound(&self) -> usize {
        self.bytes_bound.load(Ordering::Relaxed)
    }

    fn set_ring(&self, n: usize) {
        self.ring.store(n, Ordering::Relaxed);
    }

    fn set_deepest_subscriber(&self, n: usize) {
        self.deepest_subscriber.store(n, Ordering::Relaxed);
    }

    fn set_subscribers(&self, n: usize) {
        self.subscribers.store(n, Ordering::Relaxed);
    }

    fn set_bytes(&self, n: usize) {
        self.bytes.store(n, Ordering::Relaxed);
    }

    fn set_bytes_bound(&self, n: usize) {
        self.bytes_bound.store(n, Ordering::Relaxed);
    }
}

/// Handle to a running events task: publish synthetic/core events and subscribe.
#[derive(Debug, Clone)]
pub struct EventsHandle {
    cmd_tx: mpsc::Sender<Command>,
    event_tx: mpsc::Sender<EventInput>,
    occupancy: Arc<Occupancy>,
    session_id: u64,
    ring_capacity: usize,
    ring_bytes: usize,
    subscriber_queue_capacity: usize,
}

/// Live subscription: ordered replay of the ring slice, then live fan-out.
#[derive(Debug)]
pub struct EventSubscription {
    replay: std::vec::IntoIter<Event>,
    live: mpsc::Receiver<Event>,
    /// Set by the events task before dropping the live sender on slow-consumer kill.
    termination: Arc<Mutex<Option<Status>>>,
    session_id: u64,
}

impl EventSubscription {
    /// Process `session_id` — stamp into a resume [`Cursor`].
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Build a resume cursor from a delivered event.
    pub fn cursor_for(event: &Event, session_id: u64) -> Cursor {
        Cursor {
            session_id,
            seq: event.seq,
            slot: event.slot,
            root: event.root.clone(),
        }
    }

    /// Next event, or `Ok(None)` on clean end, or `Err(Status)` on slow-consumer kill.
    pub async fn recv(&mut self) -> Result<Option<Event>, Status> {
        if let Some(ev) = self.replay.next() {
            return Ok(Some(ev));
        }
        match self.live.recv().await {
            Some(ev) => Ok(Some(ev)),
            None => {
                let reason = self
                    .termination
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match reason {
                    Some(status) => Err(status),
                    None => Ok(None),
                }
            }
        }
    }
}

enum Command {
    Subscribe {
        cursor: Option<Cursor>,
        reply: oneshot::Sender<Result<EventSubscription, Status>>,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

impl EventsHandle {
    /// Spawn the single-threaded events task on the current tokio runtime.
    pub fn spawn(config: EventsConfig) -> Self {
        let ring_capacity = config.ring_capacity.max(1);
        let ring_bytes = config.ring_bytes.max(1);
        let subscriber_queue_capacity = config.subscriber_queue_capacity.max(1);
        let session_id = config.session_id.unwrap_or_else(random_session_id);
        let occupancy = Arc::new(Occupancy::default());
        occupancy.set_bytes_bound(ring_bytes);

        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        // Inbound producer channel sized to the ring count bound (Architecture §7.3).
        let (event_tx, event_rx) = mpsc::channel(ring_capacity);

        let occupancy_task = Arc::clone(&occupancy);
        tokio::spawn(async move {
            run_events_task(
                EventRing::new(ring_capacity, ring_bytes, session_id),
                FanOut::new(subscriber_queue_capacity),
                cmd_rx,
                event_rx,
                occupancy_task,
            )
            .await;
        });

        Self {
            cmd_tx,
            event_tx,
            occupancy,
            session_id,
            ring_capacity,
            ring_bytes,
            subscriber_queue_capacity,
        }
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn occupancy(&self) -> &Occupancy {
        &self.occupancy
    }

    pub fn ring_capacity(&self) -> usize {
        self.ring_capacity
    }

    pub fn ring_bytes(&self) -> usize {
        self.ring_bytes
    }

    pub fn subscriber_queue_capacity(&self) -> usize {
        self.subscriber_queue_capacity
    }

    /// Clone of the producer sender (core thread uses `blocking_send` on this).
    pub fn event_sender(&self) -> mpsc::Sender<EventInput> {
        self.event_tx.clone()
    }

    /// Async publish (tests / tokio producers).
    pub async fn publish(
        &self,
        input: EventInput,
    ) -> Result<(), mpsc::error::SendError<EventInput>> {
        self.event_tx.send(input).await
    }

    /// Non-blocking publish.
    pub fn try_publish(
        &self,
        input: EventInput,
    ) -> Result<(), mpsc::error::TrySendError<EventInput>> {
        self.event_tx.try_send(input)
    }

    /// Subscribe with an optional resume cursor. Atomic w.r.t. publication (see module doc).
    pub async fn subscribe(&self, cursor: Option<Cursor>) -> Result<EventSubscription, Status> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Subscribe { cursor, reply })
            .await
            .map_err(|_| Status::unavailable("events task is shut down"))?;
        rx.await
            .map_err(|_| Status::unavailable("events task dropped subscribe reply"))?
    }

    /// Ask the task to exit; waits until the loop breaks.
    pub async fn shutdown(&self) {
        let (done, rx) = oneshot::channel();
        if self.cmd_tx.send(Command::Shutdown { done }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

async fn run_events_task(
    mut ring: EventRing,
    mut fanout: FanOut,
    mut cmd_rx: mpsc::Receiver<Command>,
    mut event_rx: mpsc::Receiver<EventInput>,
    occupancy: Arc<Occupancy>,
) {
    // Once the producer channel closes, stop polling it so select! does not spin.
    let mut producer_open = true;

    loop {
        tokio::select! {
            // Prefer commands so a subscribe under load is not starved (still atomic
            // with publish: both arms run exclusively on this task).
            biased;

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Subscribe { cursor, reply }) => {
                        let result = handle_subscribe(&mut ring, &mut fanout, cursor);
                        occupancy.set_subscribers(fanout.len());
                        occupancy.set_ring(ring.len());
                        occupancy.set_bytes(ring.bytes());
                        occupancy.set_deepest_subscriber(fanout.deepest_depth());
                        let _ = reply.send(result);
                    }
                    Some(Command::Shutdown { done }) => {
                        fanout.shutdown_all();
                        occupancy.set_subscribers(0);
                        occupancy.set_deepest_subscriber(0);
                        let _ = done.send(());
                        break;
                    }
                    None => {
                        fanout.shutdown_all();
                        break;
                    }
                }
            }

            event = event_rx.recv(), if producer_open => {
                match event {
                    Some(input) => {
                        // SEC-44a-2: refuse oversize payloads at the ring edge
                        // even if a producer skipped the check.
                        if !input.payload_within_cap() {
                            tracing::error!(
                                kind = ?input.kind,
                                payload_len = input.payload.len(),
                                cap = MAX_EVENT_PAYLOAD_BYTES,
                                "rejected oversize event payload at ring (SEC-44a-2)"
                            );
                            continue;
                        }
                        let stored = ring.push(input);
                        occupancy.set_ring(ring.len());
                        occupancy.set_bytes(ring.bytes());
                        fanout.broadcast(&stored.to_event());
                        occupancy.set_subscribers(fanout.len());
                        occupancy.set_deepest_subscriber(fanout.deepest_depth());
                    }
                    None => {
                        producer_open = false;
                    }
                }
            }
        }
    }
}

fn handle_subscribe(
    ring: &mut EventRing,
    fanout: &mut FanOut,
    cursor: Option<Cursor>,
) -> Result<EventSubscription, Status> {
    let resume_from = match cursor {
        None => None,
        Some(ref c) => {
            validate_cursor(ring, c)?;
            Some(c.seq.saturating_add(1))
        }
    };

    let replay: Vec<Event> = match resume_from {
        None => Vec::new(), // live-only: no replay from the ring tip
        Some(from) => ring.iter_from(from).map(StoredEvent::to_event).collect(),
    };

    let (live_rx, termination) = fanout.insert();

    Ok(EventSubscription {
        replay: replay.into_iter(),
        live: live_rx,
        termination,
        session_id: ring.session_id(),
    })
}

fn random_session_id() -> u64 {
    // Extremely unlikely to fail; fall back to a non-zero constant rather than panic.
    getrandom::u64().unwrap_or(0xc0ff_ee00_d15e_a5e5)
}

/// Map a cursor validation failure into a tonic `Status` with `ErrorInfo`.
pub(crate) fn cursor_status(reason: &str, message: &str) -> Status {
    status_with_error_info(Code::FailedPrecondition, message, reason, ERROR_DOMAIN)
}

/// Build `RESOURCE_EXHAUSTED` for a slow consumer.
pub(crate) fn slow_consumer_status() -> Status {
    Status::resource_exhausted(
        "subscriber queue full: slow consumer terminated; reconnect with cursor to replay",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_proto::error_info_from_status;

    #[tokio::test]
    async fn live_subscribe_receives_subsequent_events() {
        let h = EventsHandle::spawn(EventsConfig {
            ring_capacity: 8,
            subscriber_queue_capacity: 8,
            session_id: Some(1),
            ring_bytes: usize::MAX,
        });
        let mut sub = h.subscribe(None).await.unwrap();
        h.publish(EventInput::block_imported(
            10,
            Bytes::from_static(b"root-a"),
        ))
        .await
        .unwrap();
        let ev = sub.recv().await.unwrap().unwrap();
        assert_eq!(ev.seq, 0);
        assert_eq!(ev.slot, 10);
        assert_eq!(ev.root.as_slice(), b"root-a");
        h.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_session_is_not_too_old() {
        let h = EventsHandle::spawn(EventsConfig {
            ring_capacity: 4,
            subscriber_queue_capacity: 4,
            session_id: Some(42),
            ring_bytes: usize::MAX,
        });
        h.publish(EventInput::block_imported(1, Bytes::from_static(b"r")))
            .await
            .unwrap();
        // Let the task process the event.
        tokio::task::yield_now().await;

        let err = h
            .subscribe(Some(Cursor {
                session_id: 99,
                seq: 0,
                slot: 1,
                root: b"r".to_vec(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, REASON_CURSOR_UNKNOWN_SESSION);
        h.shutdown().await;
    }

    #[test]
    fn chain_reorg_decode_is_fail_closed() {
        let root = [0x11u8; 32];
        let encoded = ChainReorgPayload::encode(&root, 7);
        let decoded = ChainReorgPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.old_head_root, root);
        assert_eq!(decoded.common_ancestor_slot, 7);
        assert!(ChainReorgPayload::decode(&[0u8; 32]).is_none());
        // Short root is not padded to a zero [u8; 32].
        let short = ChainReorgPayload::encode(&[0x22], 1);
        assert!(ChainReorgPayload::decode(&short).is_none());
        assert_ne!(short.len(), 40);
    }

    #[test]
    fn finalized_prefix_decode_is_fail_closed() {
        let sr = [0x33u8; 32];
        let encoded = FinalizedCheckpointPayload::encode(4, &sr, &[9, 9]);
        let decoded = FinalizedCheckpointPayload::decode_prefix(&encoded).unwrap();
        assert_eq!(decoded.epoch, 4);
        assert_eq!(decoded.state_root, sr);
        assert!(FinalizedCheckpointPayload::decode_prefix(&[0u8; 8]).is_none());
        let short = FinalizedCheckpointPayload::encode(1, &[0x01], &[]);
        assert!(FinalizedCheckpointPayload::decode_prefix(&short).is_none());
    }

    #[test]
    fn fixed_root_rejects_non_32() {
        assert!(EventInput::fixed_root(&[0u8; 32]).is_some());
        assert!(EventInput::fixed_root(&[0u8; 31]).is_none());
        assert!(EventInput::fixed_root(&[0u8; 33]).is_none());
    }
}
