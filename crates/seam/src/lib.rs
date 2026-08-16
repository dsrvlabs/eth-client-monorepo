//! Typed handles and overflow contracts for internal service edges
//! (`[ARCH]` §2.1).
//!
//! Methods mirror today's proto `oneof` arms (gossip / DA / column sidecar
//! / publish / view) without taking `cc-proto` types.
//!
//! [`InProcess`] is the Single Hull transport: its lanes **are** the live
//! import / column / publish queues. Do not wrap them in front of the
//! scheduler import lane, the event ring, or `publish_fwd` — that is a
//! second bound (`[ARCH]` §2.1). `Ipc` is a later issue. Both stay
//! buildable permanently (`[ARCH]` §9.2).

mod in_process;

pub use in_process::{
    DEFAULT_RING_CAPACITY, IMPORT_LANE_DEPTH, IMPORT_SEND_TIMEOUT, ImportMsg, InProcess,
    InProcessMailbox, MAX_EVENT_PAYLOAD_BYTES, PUBLISH_BOUND,
};

use async_trait::async_trait;

/// Every internal edge fails in exactly these ways. Adding a variant is a
/// contract change and needs an ADR.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeamError {
    /// The receiver's bounded queue was full for the whole `send_timeout`.
    /// MUST map to gRPC `RESOURCE_EXHAUSTED` and back. Caller-visible; the
    /// caller is expected to shed, retry or descore — never to ignore.
    #[error("seam queue full after {waited_ms}ms (bound {bound})")]
    Backpressure { bound: usize, waited_ms: u64 },
    /// Receiver is gone (process exited, task aborted, stream torn down).
    #[error("seam peer unavailable: {0}")]
    Unavailable(String),
    /// The request was structurally rejected before any work was done.
    #[error("seam invalid argument: {0}")]
    InvalidArgument(String),
    /// Precondition not met (e.g. NOT_BOOTSTRAPPED).
    #[error("seam failed precondition: {reason}")]
    FailedPrecondition { reason: &'static str },
}

/// 32-byte consensus identity. Not `cc-types::Root` and not a proto `bytes`.
pub type Root = [u8; 32];

/// Gossip object family. Seam-owned; not an `eth.p2p.v1.ObjectKind` integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Block,
    Attestation,
    Aggregate,
    SyncCommittee,
    SyncContribution,
    VoluntaryExit,
    ProposerSlashing,
    AttesterSlashing,
    BlsToExecutionChange,
    ColumnSidecar,
}

/// GossipSub acceptance class. Seam-owned; not a proto discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Acceptance {
    Accept,
    Reject,
    Ignore,
}

/// Gossip / import reason. Seam-owned; [`Self::Internal`] must never become
/// a network-facing penalty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    Valid,
    Invalid,
    InvalidSignature,
    NotDescendedFromFinalized,
    Duplicate,
    UnknownParent,
    FutureSlot,
    DeferredDa,
    AlreadyKnown,
    /// Our bug / shed — never a peer penalty.
    Internal,
}

/// Import-path outcome. Seam-owned; not a proto discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImportResult {
    Imported,
    Duplicate,
    DeferredDa,
    UnknownParent,
    Invalid,
    None,
}

/// `P2pToChain.object` — SSZ bytes + identity.
///
/// Provenance is **stamped by the impl**, not taken from the caller: there
/// is no `source` / `API` / `trusted_local` field to smuggle privilege.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipObject {
    pub ssz: Vec<u8>,
    pub fork: u32,
    pub root: Root,
    pub kind: ObjectKind,
    pub subnet_id: u64,
}

/// `P2pToChain.column` — column sidecar SSZ. Relayed, not decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSidecar {
    pub ssz: Vec<u8>,
    pub fork: u32,
    pub root: Root,
    pub column_index: u64,
    pub subnet_id: u64,
}

/// `ChainToP2p.publish` — outward gossip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    pub ssz: Vec<u8>,
    pub kind: ObjectKind,
    pub topic: String,
    pub subnet_id: u64,
}

/// `ChainToP2p.view` — chain-owned consensus view (ADR-P2-05).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChainView {
    pub slot: u64,
    pub epoch: u64,
    pub head_root: Root,
    pub head_slot: u64,
    pub finalized_root: Root,
    pub finalized_epoch: u64,
    pub justified_root: Root,
    pub justified_epoch: u64,
    pub genesis_time: u64,
    pub genesis_validators_root: Root,
    pub proposer_lookahead: Vec<u64>,
    pub proposer_pubkeys: Vec<Vec<u8>>,
    pub active_validator_count: u64,
    pub view_kind: u64,
}

/// Exactly-one-verdict reply to [`GossipObject`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub correlation_id: Root,
    pub acceptance: Acceptance,
    pub reason: Reason,
    pub import: ImportResult,
}

/// Caller-visible resolution of [`ChainIngress::submit_gossip`].
///
/// A [`Verdict`] is the only success. Local IGNORE / timeout after 2 s is
/// **not** representable: stall-then-shed stays inside the Ipc impl.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictResolution {
    pub verdict: Verdict,
}

/// Outcome of [`P2pEgress::publish`]. Drop is a value, not a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// Enqueued on the existing publish queue (`PUBLISH_BOUND`).
    Queued,
    /// Queue was full — lossy by design (ADR-R-02).
    Dropped,
}

/// p2p → core. Gossip / DA / column-sidecar ingress (E1+E2).
///
/// # Overflow contract — `submit_gossip` / `notify_data_available`
///
/// Both MUST block up to `IMPORT_SEND_TIMEOUT` (2 s)
/// (`services/chain/src/core.rs`) and then return [`SeamError::Backpressure`].
/// They MUST NOT silently drop and MUST NOT return a success that means
/// IGNORE. Implementations that cannot block (e.g. a `try_send` fast path)
/// MUST still surface Backpressure.
///
/// The receiving bound is the live import lane:
/// `IMPORT_LANE_DEPTH = 64` (`crates/scheduler/src/config.rs`) and
/// `IMPORT_SEND_TIMEOUT = 2s` (`services/chain/src/core.rs`). This handle
/// does not introduce a second bound.
///
/// `notify_data_available` is a sampling-tracker signal, not a proof. Same
/// lane and deadline as `submit_gossip`; overflow MUST be
/// [`SeamError::Backpressure`], never swallowed as `Ok(())`.
///
/// # Overflow contract — `submit_column_sidecar`
///
/// Columns MUST NOT ride `IMPORT_LANE_DEPTH` (that HOL-blocks gossip).
/// The live path is the events producer `event_tx` (`p2p_stream.rs`):
/// depth `ring_capacity`, default `DEFAULT_RING_CAPACITY = 4096`
/// (`services/chain/src/events/mod.rs`); admit with `send().await` (no
/// import-lane deadline; F1: never silent `try_send`).
///
/// Oversize (`MAX_EVENT_PAYLOAD_BYTES` = 10 MiB, SEC-44a-2) MUST be
/// [`SeamError::InvalidArgument`]. A missing or closed bus MUST be
/// [`SeamError::Unavailable`]. Neither may be `Ok(())` after a drop.
#[async_trait]
pub trait ChainIngress: Send + Sync + 'static {
    async fn submit_gossip(&self, obj: GossipObject) -> Result<VerdictResolution, SeamError>;
    async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), SeamError>;
    async fn submit_column_sidecar(&self, sidecar: ColumnSidecar) -> Result<(), SeamError>;
}

/// core → p2p. Publish / view egress (E1).
///
/// # Overflow contract
///
/// `publish` is **lossy by design** and returns `Ok(Published::Dropped)`
/// rather than an error when the **publish** queue is full (policy C,
/// ADR-R-02). The bound this handle preserves is `PUBLISH_BOUND = 256`
/// (`services/p2p/src/channels.rs`), oldest-drop at
/// `services/p2p/src/chain_stream/publish.rs`. A full publish queue MUST
/// be `Ok(Published::Dropped)`, never [`SeamError::Backpressure`].
///
/// A later swarm hop may still drop on `CMD_BOUND` (`service.rs`:
/// "cmd queue full; dropping local publish"). That is not this handle's
/// overflow and is not a second bound here.
///
/// `update_view` is an `ArcSwap` store — never blocks, never fails. The
/// view is chain-owned (ADR-P2-05); this is not a p2p self-write.
#[async_trait]
pub trait P2pEgress: Send + Sync + 'static {
    async fn publish(&self, req: PublishRequest) -> Result<Published, SeamError>;
    fn update_view(&self, view: ChainView);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::Arc;

    #[derive(Debug)]
    struct Unused;

    #[async_trait]
    impl ChainIngress for Unused {
        async fn submit_gossip(&self, _obj: GossipObject) -> Result<VerdictResolution, SeamError> {
            Err(SeamError::Unavailable("unused".into()))
        }

        async fn notify_data_available(&self, _root: Root, _slot: u64) -> Result<(), SeamError> {
            Ok(())
        }

        async fn submit_column_sidecar(&self, _sidecar: ColumnSidecar) -> Result<(), SeamError> {
            Ok(())
        }
    }

    #[async_trait]
    impl P2pEgress for Unused {
        async fn publish(&self, _req: PublishRequest) -> Result<Published, SeamError> {
            Ok(Published::Dropped)
        }

        fn update_view(&self, _view: ChainView) {}
    }

    #[test]
    fn chain_ingress_is_dyn() {
        let _: Arc<dyn ChainIngress> = Arc::new(Unused);
    }

    #[test]
    fn p2p_egress_is_dyn() {
        let _: Arc<dyn P2pEgress> = Arc::new(Unused);
    }

    #[test]
    fn seam_error_has_exactly_four_variants() {
        fn classify(err: SeamError) -> u8 {
            match err {
                SeamError::Backpressure { .. } => 0,
                SeamError::Unavailable(_) => 1,
                SeamError::InvalidArgument(_) => 2,
                SeamError::FailedPrecondition { .. } => 3,
            }
        }
        assert_eq!(
            classify(SeamError::Backpressure {
                bound: 64,
                waited_ms: 2000
            }),
            0
        );
        assert_eq!(classify(SeamError::Unavailable("gone".into())), 1);
        assert_eq!(classify(SeamError::InvalidArgument("bad".into())), 2);
        assert_eq!(
            classify(SeamError::FailedPrecondition {
                reason: "NOT_BOOTSTRAPPED"
            }),
            3
        );
    }

    #[test]
    fn submit_gossip_success_is_only_a_verdict() {
        let resolution = VerdictResolution {
            verdict: Verdict {
                correlation_id: [0; 32],
                acceptance: Acceptance::Ignore,
                reason: Reason::Internal,
                import: ImportResult::None,
            },
        };
        assert_eq!(resolution.verdict.reason, Reason::Internal);
    }

    #[test]
    fn gossip_object_has_no_caller_source() {
        let obj = GossipObject {
            ssz: Vec::new(),
            fork: 0,
            root: [0; 32],
            kind: ObjectKind::Block,
            subnet_id: 0,
        };
        let _ = obj;
    }

    #[test]
    fn object_kind_has_no_api_or_trusted_local() {
        fn classify(k: ObjectKind) -> u8 {
            match k {
                ObjectKind::Block => 0,
                ObjectKind::Attestation => 1,
                ObjectKind::Aggregate => 2,
                ObjectKind::SyncCommittee => 3,
                ObjectKind::SyncContribution => 4,
                ObjectKind::VoluntaryExit => 5,
                ObjectKind::ProposerSlashing => 6,
                ObjectKind::AttesterSlashing => 7,
                ObjectKind::BlsToExecutionChange => 8,
                ObjectKind::ColumnSidecar => 9,
            }
        }
        assert_eq!(classify(ObjectKind::Block), 0);
    }
}
