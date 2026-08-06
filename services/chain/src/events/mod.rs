//! Resumable event bus: ring buffer, cursor, and per-subscriber fan-out (CC-18c / §7.3).
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
//!
//! # Metrics
//!
//! [`Occupancy`] tracks ring length and the deepest subscriber queue depth for
//! `cc_chain_event_buffer_occupancy` (CC-1C registers the Prometheus family; this
//! module only maintains the live values).

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

/// Default event-ring capacity (Architecture §7.3).
pub const DEFAULT_RING_CAPACITY: usize = 1024;

/// Default per-subscriber live-queue capacity (Architecture §7.3).
pub const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

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
    /// Convenience constructor with empty payload and `BLOCK_IMPORTED` kind.
    pub fn block_imported(slot: u64, root: impl Into<Bytes>) -> Self {
        Self {
            slot,
            root: root.into(),
            kind: EventKind::BlockImported,
            payload: Bytes::new(),
        }
    }
}

/// Tunables for the events task. Both bounds are config values (defaults as stated).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct EventsConfig {
    /// Ring buffer capacity. Default **1024**.
    #[serde(default = "default_ring_capacity")]
    pub ring_capacity: usize,
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

fn default_subscriber_queue_capacity() -> usize {
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            ring_capacity: DEFAULT_RING_CAPACITY,
            subscriber_queue_capacity: DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            session_id: None,
        }
    }
}

/// Live buffer-occupancy gauges for `cc_chain_event_buffer_occupancy` (CC-1C registers).
#[derive(Debug, Default)]
pub struct Occupancy {
    /// Current number of events retained in the ring.
    ring: AtomicUsize,
    /// Deepest per-subscriber live-queue depth (max across active subscribers).
    deepest_subscriber: AtomicUsize,
    /// Number of active subscribers (`cc_chain_subscribers`).
    subscribers: AtomicUsize,
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

    fn set_ring(&self, n: usize) {
        self.ring.store(n, Ordering::Relaxed);
    }

    fn set_deepest_subscriber(&self, n: usize) {
        self.deepest_subscriber.store(n, Ordering::Relaxed);
    }

    fn set_subscribers(&self, n: usize) {
        self.subscribers.store(n, Ordering::Relaxed);
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
        let subscriber_queue_capacity = config.subscriber_queue_capacity.max(1);
        let session_id = config.session_id.unwrap_or_else(random_session_id);
        let occupancy = Arc::new(Occupancy::default());

        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        // Inbound producer channel matches the ring default (Architecture §7.3 mpsc(1024)).
        let (event_tx, event_rx) = mpsc::channel(ring_capacity);

        let occupancy_task = Arc::clone(&occupancy);
        tokio::spawn(async move {
            run_events_task(
                EventRing::new(ring_capacity, session_id),
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
                        let stored = ring.push(input);
                        occupancy.set_ring(ring.len());
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
}
