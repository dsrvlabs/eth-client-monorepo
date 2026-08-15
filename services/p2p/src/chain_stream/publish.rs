//! Outward publish path (Architecture §10.5, CC-27/1 outward half).
//!
//! A `PublishRequest` from `chain` becomes a [`crate::channels::PublishRequest`]
//! on the bounded publish queue (256). When full, the **oldest** entry is dropped
//! and counted — a dropped local publish is loud.

use std::collections::VecDeque;

use cc_proto::p2p::PublishRequest as ProtoPublish;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::channels::{PUBLISH_BOUND, PublishRequest};
use crate::metrics::{P2pMetrics, QueueName};

/// Counts oldest-dropped local publishes (loud path).
#[derive(Debug, Default, Clone)]
pub struct PublishDropCounter {
    inner: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl PublishDropCounter {
    /// New zeroed counter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment and return the new total.
    pub fn inc(&self) -> u64 {
        self.inner
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1)
    }

    /// Current total.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.inner.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Convert a stream `PublishRequest` into the swarm-facing shape.
#[must_use]
pub fn to_swarm_publish(req: ProtoPublish) -> PublishRequest {
    // Prefer the topic path segment as given; subnet kinds may include id.
    let topic = if req.topic.is_empty() {
        format!("object_kind_{}", req.kind)
    } else {
        req.topic
    };
    PublishRequest {
        topic,
        data: req.ssz,
    }
}

/// Drain proto publishes into the swarm publish queue with oldest-drop policy.
///
/// Maintains a local deque so we can drop the **oldest** when the bound is hit
/// (tokio `mpsc` alone would drop newest).
pub async fn run_publish_dispatch(
    mut inbound: mpsc::Receiver<ProtoPublish>,
    publish_tx: mpsc::Sender<PublishRequest>,
    metrics: P2pMetrics,
    drops: PublishDropCounter,
) {
    let mut pending: VecDeque<PublishRequest> = VecDeque::with_capacity(PUBLISH_BOUND);
    loop {
        // Prefer draining capacity to the real channel first.
        while let Some(front) = pending.front() {
            match publish_tx.try_send(front.clone()) {
                Ok(()) => {
                    pending.pop_front();
                    let d = metrics.queue_depth(QueueName::Publish);
                    metrics.set_queue_depth(QueueName::Publish, (d + 1).min(PUBLISH_BOUND as i64));
                }
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!("publish channel closed; publish dispatch exiting");
                    return;
                }
            }
        }

        // Wait for a new publish, or for capacity if we are backlogged.
        if pending.is_empty() {
            match inbound.recv().await {
                Some(req) => enqueue(&mut pending, to_swarm_publish(req), &drops),
                None => {
                    // Flush remaining then exit.
                    flush_all(&mut pending, &publish_tx, &metrics).await;
                    return;
                }
            }
        } else {
            tokio::select! {
                msg = inbound.recv() => {
                    match msg {
                        Some(req) => enqueue(&mut pending, to_swarm_publish(req), &drops),
                        None => {
                            flush_all(&mut pending, &publish_tx, &metrics).await;
                            return;
                        }
                    }
                }
                permit = publish_tx.reserve() => {
                    match permit {
                        Ok(p) => {
                            if let Some(req) = pending.pop_front() {
                                p.send(req);
                                let d = metrics.queue_depth(QueueName::Publish);
                                metrics.set_queue_depth(
                                    QueueName::Publish,
                                    (d + 1).min(PUBLISH_BOUND as i64),
                                );
                            } else {
                                // Spurious: nothing to send; drop the permit.
                                drop(p);
                            }
                        }
                        Err(_) => {
                            error!("publish channel closed while pending");
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn enqueue(
    pending: &mut VecDeque<PublishRequest>,
    req: PublishRequest,
    drops: &PublishDropCounter,
) {
    if pending.len() >= PUBLISH_BOUND {
        let _ = pending.pop_front();
        let n = drops.inc();
        error!(
            dropped_total = n,
            "publish queue full; dropped oldest local publish (CC-27b)"
        );
    }
    pending.push_back(req);
}

async fn flush_all(
    pending: &mut VecDeque<PublishRequest>,
    publish_tx: &mpsc::Sender<PublishRequest>,
    metrics: &P2pMetrics,
) {
    while let Some(req) = pending.pop_front() {
        match publish_tx.send(req).await {
            Ok(()) => {
                let d = metrics.queue_depth(QueueName::Publish);
                metrics.set_queue_depth(QueueName::Publish, (d + 1).min(PUBLISH_BOUND as i64));
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn enqueue_drops_oldest_at_bound() {
        let drops = PublishDropCounter::new();
        let mut pending = std::collections::VecDeque::new();
        for i in 0..(PUBLISH_BOUND + 5) {
            enqueue(
                &mut pending,
                PublishRequest {
                    topic: format!("t{i}"),
                    data: vec![i as u8],
                },
                &drops,
            );
        }
        assert_eq!(pending.len(), PUBLISH_BOUND);
        assert_eq!(drops.get(), 5);
        // Oldest remaining should be the 5th inserted (indices 5..PUBLISH_BOUND+5).
        assert_eq!(pending.front().unwrap().topic, "t5");
    }
}
