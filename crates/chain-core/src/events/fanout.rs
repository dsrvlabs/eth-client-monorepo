//! Per-subscriber live queues with `try_send` only and slow-consumer termination.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cc_proto::chain::Event;
use tokio::sync::mpsc;
use tonic::Status;

use super::slow_consumer_status;

/// One live subscriber entry in the table owned by the events task.
struct Subscriber {
    tx: mpsc::Sender<Event>,
    /// Written before `tx` is dropped so the stream can surface `RESOURCE_EXHAUSTED`.
    termination: Arc<Mutex<Option<Status>>>,
    queue_capacity: usize,
}

/// Fan-out table: broadcast live events with non-blocking sends.
pub struct FanOut {
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    queue_capacity: usize,
}

impl std::fmt::Debug for FanOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOut")
            .field("subscribers", &self.subscribers.len())
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl FanOut {
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            subscribers: HashMap::new(),
            next_id: 1,
            queue_capacity: queue_capacity.max(1),
        }
    }

    pub fn len(&self) -> usize {
        self.subscribers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Insert a new subscriber; returns the consumer receiver and termination slot.
    pub fn insert(&mut self) -> (mpsc::Receiver<Event>, Arc<Mutex<Option<Status>>>) {
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        let termination = Arc::new(Mutex::new(None));
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.subscribers.insert(
            id,
            Subscriber {
                tx,
                termination: Arc::clone(&termination),
                queue_capacity: self.queue_capacity,
            },
        );
        (rx, termination)
    }

    /// Broadcast `event` to every subscriber.
    ///
    /// Full queues are terminated with `RESOURCE_EXHAUSTED` and removed; closed
    /// receivers are removed quietly.
    pub fn broadcast(&mut self, event: &Event) {
        let mut dead: Vec<u64> = Vec::new();
        for (id, sub) in &self.subscribers {
            match sub.tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if let Ok(mut guard) = sub.termination.lock() {
                        *guard = Some(slow_consumer_status());
                    }
                    dead.push(*id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    dead.push(*id);
                }
            }
        }
        for id in dead {
            self.subscribers.remove(&id);
        }
    }

    /// Max live-queue depth across subscribers (`cap - remaining capacity`).
    pub fn deepest_depth(&self) -> usize {
        self.subscribers
            .values()
            .map(|s| s.queue_capacity.saturating_sub(s.tx.capacity()))
            .max()
            .unwrap_or(0)
    }

    /// Drop every subscriber (clean end — no slow-consumer status).
    pub fn shutdown_all(&mut self) {
        self.subscribers.clear();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_proto::chain::EventKind;

    fn sample(seq: u64) -> Event {
        Event {
            seq,
            slot: seq,
            root: vec![seq as u8],
            kind: EventKind::Head as i32,
            payload: vec![],
        }
    }

    #[tokio::test]
    async fn full_queue_terminates_with_resource_exhausted() {
        let mut fan = FanOut::new(2);
        let (mut rx, term) = fan.insert();
        fan.broadcast(&sample(0));
        fan.broadcast(&sample(1));
        assert_eq!(fan.len(), 1);
        // Third send triggers Full (capacity 2).
        fan.broadcast(&sample(2));
        assert_eq!(fan.len(), 0);

        // Drain the two buffered events; then channel closes with termination reason.
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_none());
        let status = term.lock().unwrap().take().unwrap();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }
}
