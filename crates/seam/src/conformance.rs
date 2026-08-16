//! Overflow-policy conformance (`[ARCH]` §2.2).
//!
//! Four distinct policies — do not collapse them.
//!
//! - **A** — [`backpressure_surfaces_after_deadline`] (this module). Full
//!   import send path → [`SeamError::Backpressure`] after
//!   [`IMPORT_SEND_TIMEOUT`].
//! - **B** — `events::slow_subscriber_is_terminated_not_stalled`
//!   (`services/chain/tests/events.rs`). Per-subscriber `try_send`; Full
//!   drops the *subscriber*. Not a [`ChainIngress`] / [`P2pEgress`] method.
//! - **C** — [`publish_drop_is_observable`] (this module). Full
//!   [`PUBLISH_BOUND`] → `Ok(Published::Dropped)` on both [`P2pEgress`]
//!   impls. Never [`SeamError::Backpressure`].
//! - **D** — `core::slot_tick_is_never_shed` (`services/chain/src/core.rs`,
//!   landed `S0-A-14`). Silent tick-drop is deleted; the never-shed lane
//!   is not a seam method.
//!
//! Policy A wrappers fill and check occupancy:
//!
//! - [`InProcess`]: live import lane [`IMPORT_LANE_DEPTH`]
//! - [`Ipc`]: handle send-side [`CHAIN_OUT_BOUND`] (not a second 64-deep
//!   queue in front of Loop B)
//!
//! Policy C wrappers fill [`PUBLISH_BOUND`] on [`InProcess`] and
//! `IpcEgress`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use tokio::sync::watch;
use tokio::time::Instant;

use crate::in_process::{IMPORT_LANE_DEPTH, InProcess, PUBLISH_BOUND};
use crate::ipc::{CHAIN_OUT_BOUND, Ipc, IpcConfig};
use crate::{
    ChainIngress, GossipObject, IMPORT_SEND_TIMEOUT, ObjectKind, P2pEgress, PublishRequest,
    Published, SeamError,
};

fn gossip(root: crate::Root) -> GossipObject {
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

/// Policy C: a full publish queue returns [`Published::Dropped`] as a
/// value, never [`SeamError::Backpressure`].
async fn publish_drop_is_observable(egress: &dyn P2pEgress) {
    let result = egress.publish(publish_req("overflow")).await;
    assert!(
        matches!(result, Ok(Published::Dropped)),
        "policy C must return Ok(Published::Dropped), got {result:?}"
    );
    let again = egress.publish(publish_req("overflow-2")).await;
    assert!(
        matches!(again, Ok(Published::Dropped)),
        "policy C drop must not become Backpressure, got {again:?}"
    );
}

async fn fill_publish_queue(egress: &dyn P2pEgress) {
    for i in 0..PUBLISH_BOUND {
        let out = egress.publish(publish_req(&format!("t{i}"))).await.unwrap();
        assert_eq!(
            out,
            Published::Queued,
            "slot {i} must enqueue before overflow"
        );
    }
}

#[tokio::test]
async fn publish_drop_is_observable_in_process() {
    let (seam, mailbox) = InProcess::pair();
    fill_publish_queue(&seam).await;
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    assert_eq!(mailbox.publish_rx.capacity(), 0);
    publish_drop_is_observable(&seam).await;
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    drop(mailbox);
}

#[tokio::test]
async fn publish_drop_is_observable_ipc() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    // Hold `task` unpolled so the reconnect loop cannot try_send onto this queue.
    let (_ipc, egress, mailbox, task) = Ipc::connect(IpcConfig::default(), shutdown_rx);
    fill_publish_queue(&egress).await;
    assert_eq!(mailbox.publish_rx.max_capacity(), PUBLISH_BOUND);
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    assert_eq!(mailbox.publish_rx.capacity(), 0);
    publish_drop_is_observable(&egress).await;
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    drop((task, mailbox));
}
