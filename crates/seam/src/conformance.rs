//! Policy A conformance: blocking with deadline (`[ARCH]` §2.2).
//!
//! `backpressure_surfaces_after_deadline` asserts [`SeamError::Backpressure`]
//! after at least [`IMPORT_SEND_TIMEOUT`] on a **full** send path. Wrappers
//! fill and check occupancy:
//!
//! - [`InProcess`]: live import lane [`IMPORT_LANE_DEPTH`]
//! - [`Ipc`]: handle send-side [`CHAIN_OUT_BOUND`] (not a second 64-deep
//!   queue in front of Loop B)

#![allow(clippy::unwrap_used, clippy::expect_used)]

use tokio::sync::watch;
use tokio::time::Instant;

use crate::in_process::{IMPORT_LANE_DEPTH, InProcess};
use crate::ipc::{CHAIN_OUT_BOUND, Ipc, IpcConfig};
use crate::{ChainIngress, GossipObject, IMPORT_SEND_TIMEOUT, ObjectKind, SeamError};

fn gossip(root: crate::Root) -> GossipObject {
    GossipObject {
        ssz: Vec::new(),
        fork: 0,
        root,
        kind: ObjectKind::Block,
        subnet_id: 0,
    }
}

/// Policy A: a full send path returns [`SeamError::Backpressure`] with the
/// impl's named `bound` only after blocking for [`IMPORT_SEND_TIMEOUT`].
async fn backpressure_surfaces_after_deadline(ingress: &dyn ChainIngress, bound: usize) {
    let start = Instant::now();
    let err = ingress.submit_gossip(gossip([0xff; 32])).await.unwrap_err();
    assert!(
        start.elapsed() >= IMPORT_SEND_TIMEOUT,
        "policy A must wait the deadline, elapsed {:?}",
        start.elapsed()
    );
    assert!(
        matches!(
            err,
            SeamError::Backpressure {
                bound: got,
                waited_ms
            } if got == bound && waited_ms >= IMPORT_SEND_TIMEOUT.as_millis() as u64
        ),
        "policy A must surface Backpressure {{ bound: {bound}, waited_ms ≥ {} }}, got {err:?}",
        IMPORT_SEND_TIMEOUT.as_millis()
    );
}

#[tokio::test(start_paused = true)]
async fn backpressure_surfaces_after_deadline_in_process() {
    let (seam, mailbox) = InProcess::pair();
    for slot in 0..IMPORT_LANE_DEPTH as u64 {
        seam.notify_data_available([0; 32], slot).await.unwrap();
    }
    assert_eq!(mailbox.import_rx.len(), IMPORT_LANE_DEPTH);
    backpressure_surfaces_after_deadline(&seam, IMPORT_LANE_DEPTH).await;
    drop(mailbox);
}

#[tokio::test(start_paused = true)]
async fn backpressure_surfaces_after_deadline_ipc() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    // Hold `task` unpolled so `out_rx` stays open and is not drained.
    let (ipc, _egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
    ipc.fill_out_lane();
    assert_eq!(ipc.out_lane_capacity(), 0);
    backpressure_surfaces_after_deadline(&ipc, CHAIN_OUT_BOUND).await;
    drop((task, mailbox));
}
