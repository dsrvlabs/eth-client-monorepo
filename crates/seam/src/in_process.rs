//! Single Hull transport: bounded tokio `mpsc` + oneshot replies.
//!
//! These channels **are** the live queues under Single Hull (`[ARCH]` §2.1,
//! §2.5). They replace — they must not sit in front of — the scheduler
//! import lane, the events `event_tx` ring, or `publish_fwd`. Wrapping this
//! mailbox with those objects is a second bound (R-1).
//!
//! This crate is a leaf so the depths are copied here with their source
//! cited; the copies name those live depths, they do not add another queue.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::{mpsc, oneshot};

use crate::{
    ChainIngress, ChainView, ColumnSidecar, GossipObject, P2pEgress, PublishRequest, Published,
    Root, SeamError, VerdictResolution,
};

/// Live import-lane depth (`crates/scheduler/src/config.rs`).
pub const IMPORT_LANE_DEPTH: usize = 64;

/// Live import send deadline (`services/chain/src/core.rs`).
pub const IMPORT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Events producer ring (`services/chain/src/events/mod.rs`).
pub const DEFAULT_RING_CAPACITY: usize = 4096;

/// SEC-44a-2 payload cap (`services/chain/src/events/mod.rs`).
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Publish queue (`services/p2p/src/channels.rs`).
pub const PUBLISH_BOUND: usize = 256;

/// Work on the import lane. Columns ride [`InProcessMailbox::column_rx`].
#[derive(Debug)]
pub enum ImportMsg {
    Gossip {
        obj: GossipObject,
        reply: oneshot::Sender<VerdictResolution>,
    },
    DataAvailable {
        root: Root,
        slot: u64,
    },
}

/// Send side. Callers hold `Arc<dyn ChainIngress>` / `Arc<dyn P2pEgress>`.
///
/// [`P2pEgress::update_view`] is chain-owned (ADR-P2-05). Only the
/// consensus owner holds the write handle (this type or
/// [`InProcessMailbox::view_store`]).
#[derive(Debug, Clone)]
pub struct InProcess {
    import_tx: mpsc::Sender<ImportMsg>,
    column_tx: mpsc::Sender<ColumnSidecar>,
    publish_tx: mpsc::Sender<PublishRequest>,
    view: Arc<ArcSwap<ChainView>>,
}

/// Receive side. These receivers **are** the live Single Hull lanes:
/// `import_rx` replaces the Loop B import FIFO (no second `push_timeout`);
/// `column_rx` replaces `event_tx` (no second ring); `publish_rx` replaces
/// `publish_fwd` (no second `PUBLISH_BOUND`).
#[derive(Debug)]
pub struct InProcessMailbox {
    pub import_rx: mpsc::Receiver<ImportMsg>,
    pub column_rx: mpsc::Receiver<ColumnSidecar>,
    pub publish_rx: mpsc::Receiver<PublishRequest>,
    view: Arc<ArcSwap<ChainView>>,
}

impl InProcess {
    /// Directed channel pair. The returned queues **are** the Single Hull
    /// import / column / publish lanes. Drain [`InProcessMailbox`] in place
    /// of the scheduler import inbound, `event_tx`, and `publish_fwd`.
    #[must_use]
    pub fn pair() -> (Self, InProcessMailbox) {
        let (import_tx, import_rx) = mpsc::channel(IMPORT_LANE_DEPTH);
        let (column_tx, column_rx) = mpsc::channel(DEFAULT_RING_CAPACITY);
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_BOUND);
        let view = Arc::new(ArcSwap::from_pointee(ChainView::default()));
        (
            Self {
                import_tx,
                column_tx,
                publish_tx,
                view: Arc::clone(&view),
            },
            InProcessMailbox {
                import_rx,
                column_rx,
                publish_rx,
                view,
            },
        )
    }
}

impl InProcessMailbox {
    /// Pointer load — never blocks.
    #[must_use]
    pub fn load_view(&self) -> Arc<ChainView> {
        self.view.load_full()
    }

    /// Shared `ArcSwap` so [`P2pEgress::update_view`] never fails.
    /// `store` is public; only the consensus owner may hold this handle.
    /// Slot clock / Status readers use [`Self::load_view`].
    #[must_use]
    pub fn view_store(&self) -> Arc<ArcSwap<ChainView>> {
        Arc::clone(&self.view)
    }
}

async fn send_import(tx: &mpsc::Sender<ImportMsg>, msg: ImportMsg) -> Result<(), SeamError> {
    match tx.send_timeout(msg, IMPORT_SEND_TIMEOUT).await {
        Ok(()) => Ok(()),
        Err(SendTimeoutError::Timeout(_)) => Err(SeamError::Backpressure {
            bound: IMPORT_LANE_DEPTH,
            waited_ms: IMPORT_SEND_TIMEOUT.as_millis() as u64,
        }),
        Err(SendTimeoutError::Closed(_)) => {
            Err(SeamError::Unavailable("import lane closed".into()))
        }
    }
}

#[async_trait]
impl ChainIngress for InProcess {
    async fn submit_gossip(&self, obj: GossipObject) -> Result<VerdictResolution, SeamError> {
        let (reply, rx) = oneshot::channel();
        send_import(&self.import_tx, ImportMsg::Gossip { obj, reply }).await?;
        rx.await
            .map_err(|_| SeamError::Unavailable("import reply dropped".into()))
    }

    async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), SeamError> {
        send_import(&self.import_tx, ImportMsg::DataAvailable { root, slot }).await
    }

    async fn submit_column_sidecar(&self, sidecar: ColumnSidecar) -> Result<(), SeamError> {
        if sidecar.ssz.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Err(SeamError::InvalidArgument(format!(
                "column sidecar payload {} exceeds {MAX_EVENT_PAYLOAD_BYTES}",
                sidecar.ssz.len()
            )));
        }
        self.column_tx
            .send(sidecar)
            .await
            .map_err(|_| SeamError::Unavailable("column bus closed".into()))
    }
}

#[async_trait]
impl P2pEgress for InProcess {
    async fn publish(&self, req: PublishRequest) -> Result<Published, SeamError> {
        // Policy C on *this* queue. It is the live publish lane — do not
        // put `publish_fwd` or another `PUBLISH_BOUND` in front of it.
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
    use crate::{Acceptance, ImportResult, ObjectKind, Reason, Verdict};

    fn gossip(root: Root) -> GossipObject {
        GossipObject {
            ssz: Vec::new(),
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

    #[tokio::test]
    async fn submit_gossip_roundtrip_via_oneshot() {
        let (seam, mut mailbox) = InProcess::pair();
        let obj = gossip([1; 32]);
        let worker = tokio::spawn(async move {
            let msg = mailbox.import_rx.recv().await.expect("import");
            assert!(
                matches!(msg, ImportMsg::Gossip { .. }),
                "expected gossip, got {msg:?}"
            );
            let ImportMsg::Gossip { obj, reply } = msg else {
                return;
            };
            let _ = reply.send(VerdictResolution {
                verdict: Verdict {
                    correlation_id: obj.root,
                    acceptance: Acceptance::Accept,
                    reason: Reason::Valid,
                    import: ImportResult::Imported,
                },
            });
        });
        let got = seam.submit_gossip(obj).await.unwrap();
        worker.await.unwrap();
        assert_eq!(got.verdict.acceptance, Acceptance::Accept);
        assert_eq!(got.verdict.correlation_id, [1; 32]);
    }

    #[tokio::test(start_paused = true)]
    async fn import_full_is_backpressure() {
        let (seam, mailbox) = InProcess::pair();
        let mut waiters = Vec::with_capacity(IMPORT_LANE_DEPTH);
        for i in 0..IMPORT_LANE_DEPTH {
            let handle = seam.clone();
            let root = [i as u8; 32];
            waiters.push(tokio::spawn(async move {
                handle.submit_gossip(gossip(root)).await
            }));
        }
        while mailbox.import_rx.len() < IMPORT_LANE_DEPTH {
            tokio::task::yield_now().await;
        }

        let err = seam.submit_gossip(gossip([0xff; 32])).await.unwrap_err();
        assert_eq!(
            err,
            SeamError::Backpressure {
                bound: IMPORT_LANE_DEPTH,
                waited_ms: IMPORT_SEND_TIMEOUT.as_millis() as u64,
            }
        );
        let da = seam.notify_data_available([0xee; 32], 3).await.unwrap_err();
        assert!(matches!(
            da,
            SeamError::Backpressure {
                bound: IMPORT_LANE_DEPTH,
                ..
            }
        ));

        drop(mailbox);
        for waiter in waiters {
            let err = waiter.await.unwrap().unwrap_err();
            assert!(matches!(err, SeamError::Unavailable(_)));
        }
    }

    #[tokio::test]
    async fn closed_import_is_unavailable() {
        let (seam, mailbox) = InProcess::pair();
        drop(mailbox);
        let err = seam.submit_gossip(gossip([2; 32])).await.unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
        let err = seam.notify_data_available([2; 32], 1).await.unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
    }

    #[tokio::test]
    async fn publish_full_is_dropped() {
        let (seam, mailbox) = InProcess::pair();
        for i in 0..PUBLISH_BOUND {
            let out = seam.publish(publish_req(&format!("t{i}"))).await.unwrap();
            assert_eq!(out, Published::Queued);
        }
        assert_eq!(
            seam.publish(publish_req("overflow")).await.unwrap(),
            Published::Dropped
        );
        drop(mailbox);
        let err = seam.publish(publish_req("closed")).await.unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
    }

    #[tokio::test]
    async fn update_view_never_fails() {
        let (seam, mailbox) = InProcess::pair();
        let view = ChainView {
            slot: 9,
            head_root: [9; 32],
            view_kind: 3,
            ..ChainView::default()
        };
        seam.update_view(view.clone());
        assert_eq!(*mailbox.load_view(), view);
        drop(mailbox);
        seam.update_view(ChainView {
            slot: 10,
            ..ChainView::default()
        });
    }

    #[tokio::test]
    async fn oversize_column_is_invalid_argument() {
        let (seam, mailbox) = InProcess::pair();
        let err = seam
            .submit_column_sidecar(sidecar(vec![0; MAX_EVENT_PAYLOAD_BYTES + 1]))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(mailbox.column_rx.is_empty());

        seam.submit_column_sidecar(sidecar(vec![1, 2, 3]))
            .await
            .unwrap();
        drop(mailbox);
        let err = seam
            .submit_column_sidecar(sidecar(vec![4]))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
    }
}
