//! Error taxonomy and Phase 2 gossip classification (Architecture §5.3, ADR-P1-07).
//!
//! `gossip_class()` is **exhaustive with no catch-all arm**: adding a
//! [`BlockError`] variant without classifying it is a compile failure.

use cc_types::primitives::{Root, Slot, ValidatorIndex};
use cc_types::state::StateAccessError;

/// Phase 2 (CC-22) maps this to the ACCEPT/REJECT/IGNORE verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GossipClass {
    /// Provably invalid; sender at fault (descore).
    Reject,
    /// Cannot judge yet, or already known; sender not at fault.
    Ignore,
    /// Our bug or resource limit — must never reach a network-facing verdict.
    Internal,
}

/// Which signature failed verification (batch failure re-attribution).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignatureKind {
    /// Proposer signature over the beacon block.
    BlockProposer,
    /// RANDAO reveal.
    Randao,
    /// Proposer slashing at operation index.
    ProposerSlashing(usize),
    /// Attester slashing at operation index.
    AttesterSlashing(usize),
    /// Attestation at operation index.
    Attestation(usize),
    /// Voluntary exit at operation index.
    VoluntaryExit(usize),
    /// Sync committee aggregate.
    SyncAggregate,
    /// BLS-to-execution change at operation index.
    BlsToExecutionChange(usize),
}

impl std::fmt::Display for SignatureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockProposer => write!(f, "block_proposer"),
            Self::Randao => write!(f, "randao"),
            Self::ProposerSlashing(i) => write!(f, "proposer_slashing[{i}]"),
            Self::AttesterSlashing(i) => write!(f, "attester_slashing[{i}]"),
            Self::Attestation(i) => write!(f, "attestation[{i}]"),
            Self::VoluntaryExit(i) => write!(f, "voluntary_exit[{i}]"),
            Self::SyncAggregate => write!(f, "sync_aggregate"),
            Self::BlsToExecutionChange(i) => write!(f, "bls_to_execution_change[{i}]"),
        }
    }
}

/// Epoch-processing errors (folded into [`BlockError`] at slot boundaries).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    /// Handler body not yet implemented (CC-13a–d).
    #[error("epoch handler not yet implemented: {0}")]
    NotYetImplemented(&'static str),
    /// Arithmetic overflow during epoch processing.
    #[error("epoch arithmetic overflow")]
    ArithmeticOverflow,
    /// State list/vector access failed.
    #[error("state access: {0}")]
    StateAccess(#[from] StateAccessError),
    /// Pubkey-cache invariant broken (from a shared helper).
    ///
    /// Distinct from [`Self::ArithmeticOverflow`] so P1-B/10 cannot relabel it.
    #[error("epoch cache poisoned")]
    CachePoisoned,
    /// Required state is not resident in the body/state cache.
    #[error("state not resident")]
    StateNotResident,
    /// State-resident BLS material failed deserialize/validate.
    #[error("state bls material invalid: {0}")]
    StateBlsMaterial(String),
    /// Typed operation failure leaked from a shared helper.
    #[error("invalid operation: {0}")]
    InvalidOperation(OperationError),
    /// Block-path error with no epoch analogue.
    ///
    /// Boxed to break the `BlockError`/`EpochError` cycle. Must not collapse
    /// to [`Self::ArithmeticOverflow`] (P1-B/10).
    #[error("block-path error during epoch processing: {0}")]
    BlockPath(Box<BlockError>),
}

/// Per-operation errors (folded into [`BlockError::InvalidOperation`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperationError {
    /// Operation failed a spec assertion.
    #[error("{op}: {detail}")]
    Invalid {
        /// Operation name.
        op: &'static str,
        /// Human-readable detail.
        detail: String,
    },
    /// Handler not yet implemented (CC-12b–d).
    #[error("operation not yet implemented: {0}")]
    NotYetImplemented(&'static str),
}

/// Engine-transport / payload-validation errors (CC-14 fills real variants).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// Transport or remote failure (classifies as [`GossipClass::Internal`]).
    #[error("engine transport: {0}")]
    Transport(String),
    /// EL rejected the payload (classifies as [`GossipClass::Reject`]).
    #[error("engine invalid payload")]
    InvalidPayload,
}

/// Top-level block / state-transition error (Architecture §5.3).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlockError {
    // ---- slot / header -------------------------------------------------------
    /// `process_slots` requires `state.slot < target`.
    #[error("slot not later than state: state={state_slot}, target={target}")]
    SlotNotLater {
        /// Current state slot.
        state_slot: Slot,
        /// Requested target slot.
        target: Slot,
    },
    /// Block slot does not equal state slot after `process_slots`.
    #[error("block slot mismatch: block={block_slot}, state={state_slot}")]
    BlockSlotMismatch {
        /// Block slot.
        block_slot: Slot,
        /// State slot.
        state_slot: Slot,
    },
    /// Block slot is not strictly greater than the latest block header slot.
    #[error("block slot not newer than parent header: block={block_slot}, parent={parent_slot}")]
    BlockSlotNotNewer {
        /// Block slot.
        block_slot: Slot,
        /// Latest header slot.
        parent_slot: Slot,
    },
    /// Proposer index does not match `get_beacon_proposer_index`.
    #[error("proposer mismatch: block={block}, expected={expected}")]
    ProposerMismatch {
        /// Index claimed by the block.
        block: ValidatorIndex,
        /// Expected proposer.
        expected: ValidatorIndex,
    },
    /// `block.parent_root` does not match `hash_tree_root(latest_block_header)`.
    #[error("parent root mismatch")]
    ParentRootMismatch {
        /// Expected parent root (from state).
        expected: Root,
        /// Parent root from the block.
        actual: Root,
    },
    /// Proposer is slashed.
    #[error("proposer {index} is slashed")]
    ProposerSlashed {
        /// Proposer index.
        index: ValidatorIndex,
    },
    /// Proposer index out of registry range.
    #[error("proposer index {index} out of registry (len={len})")]
    ProposerUnknown {
        /// Claimed index.
        index: ValidatorIndex,
        /// Registry length.
        len: usize,
    },
    /// Post-state root does not match `block.state_root`.
    #[error("state root mismatch")]
    StateRootMismatch {
        /// Root claimed by the block.
        expected: Root,
        /// Computed post-state root.
        actual: Root,
    },

    // ---- signatures ----------------------------------------------------------
    /// A block-level signature failed verification.
    #[error("invalid signature: {which}")]
    InvalidSignature {
        /// Which signature failed.
        which: SignatureKind,
    },
    /// BLS material **from the block** failed deserialize/validate (sig bytes,
    /// operation-carried pubkeys, …). Peer / message fault → [`GossipClass::Reject`].
    #[error("block bls material invalid: {0}")]
    BlsMaterial(String),
    /// BLS material **from local state** (registry pubkey, …) failed
    /// deserialize/validate. Our state / import invariant → [`GossipClass::Internal`].
    ///
    /// Must not descore an honest peer when our registry entry is unusable
    /// (SEC-12a-1).
    #[error("state bls material invalid: {0}")]
    StateBlsMaterial(String),

    // ---- operations / payload ------------------------------------------------
    /// Typed operation failure.
    #[error("invalid operation: {0}")]
    InvalidOperation(#[from] OperationError),
    /// Operation list length exceeds the preset max.
    #[error("operation count overflow: {op} count={count} max={max}")]
    OperationCountOverflow {
        /// Operation name.
        op: &'static str,
        /// Observed count.
        count: usize,
        /// Maximum allowed.
        max: u64,
    },
    /// Blob commitment count exceeds the runtime schedule bound.
    #[error("blob bound exceeded: count={count} max={max}")]
    BlobBoundExceeded {
        /// Commitment count.
        count: usize,
        /// Max from `get_blob_parameters`.
        max: u64,
    },
    /// Execution payload invalid per the engine (or local checks).
    #[error("invalid execution payload")]
    InvalidPayload,
    /// Engine seam error.
    #[error(transparent)]
    Engine(#[from] EngineError),

    // ---- gossip-facing ignore cases (fork-choice / import path) --------------
    /// Parent block root is unknown locally.
    #[error("unknown parent")]
    UnknownParent,
    /// Block slot is in the future relative to wall clock / store time.
    #[error("future slot: block={block_slot}, current={current_slot}")]
    FutureSlot {
        /// Block slot.
        block_slot: Slot,
        /// Current/store slot.
        current_slot: Slot,
    },
    /// Block is already in the store.
    #[error("block already known")]
    AlreadyKnown,
    /// Block does not descend from the finalized checkpoint.
    #[error("not descending from finalized")]
    NotDescendedFromFinalized,
    /// Data availability not yet satisfied (CC-17).
    #[error("data not available")]
    DataNotAvailable,

    // ---- internal ------------------------------------------------------------
    /// Cache invariant broken.
    #[error("cache poisoned")]
    CachePoisoned,
    /// Arithmetic overflow in transition helpers.
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
    /// Required state is not resident in the body/state cache.
    #[error("state not resident")]
    StateNotResident,
    /// State list/vector accessor failure.
    #[error("state access: {0}")]
    StateAccess(#[from] StateAccessError),
    /// Epoch processing failed (from `process_slots` epoch boundary).
    #[error(transparent)]
    Epoch(#[from] EpochError),
    /// Handler not yet implemented (CC-12b–d / CC-13).
    #[error("not yet implemented: {0}")]
    NotYetImplemented(&'static str),
}

impl BlockError {
    /// Phase 2 maps this to the p2p ACCEPT/REJECT/IGNORE verdict.
    ///
    /// Exhaustive: no `_ =>` arm. Adding a variant without a classification
    /// fails the build.
    pub fn gossip_class(&self) -> GossipClass {
        use BlockError::*;
        match self {
            // Reject — provably invalid, sender at fault.
            SlotNotLater { .. }
            | BlockSlotMismatch { .. }
            | BlockSlotNotNewer { .. }
            | ProposerMismatch { .. }
            | ParentRootMismatch { .. }
            | ProposerSlashed { .. }
            | ProposerUnknown { .. }
            | StateRootMismatch { .. }
            | InvalidSignature { .. }
            | BlsMaterial(_)
            | InvalidOperation(OperationError::Invalid { .. })
            | OperationCountOverflow { .. }
            | BlobBoundExceeded { .. }
            | InvalidPayload
            | Engine(EngineError::InvalidPayload) => GossipClass::Reject,

            // Ignore — cannot judge yet / already known.
            UnknownParent
            | FutureSlot { .. }
            | AlreadyKnown
            | NotDescendedFromFinalized
            | DataNotAvailable => GossipClass::Ignore,

            // Internal — our bug or limit; never a network-facing descore.
            // State-resident BLS failures and nested NYI must not look like peer fault
            // (SEC-12a-1, SEC-12a-2).
            CachePoisoned
            | ArithmeticOverflow
            | StateNotResident
            | StateAccess(_)
            | Epoch(_)
            | NotYetImplemented(_)
            | StateBlsMaterial(_)
            | InvalidOperation(OperationError::NotYetImplemented(_))
            | Engine(EngineError::Transport(_)) => GossipClass::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// Exhaustive match over every [`BlockError`] variant for `gossip_class`.
    /// Adding a variant without updating this test fails compilation.
    #[test]
    fn gossip_class_is_total_no_catchall() {
        let samples: &[BlockError] = &[
            BlockError::SlotNotLater {
                state_slot: Slot::new(1),
                target: Slot::new(1),
            },
            BlockError::BlockSlotMismatch {
                block_slot: Slot::new(1),
                state_slot: Slot::new(0),
            },
            BlockError::BlockSlotNotNewer {
                block_slot: Slot::new(1),
                parent_slot: Slot::new(1),
            },
            BlockError::ProposerMismatch {
                block: ValidatorIndex::new(0),
                expected: ValidatorIndex::new(1),
            },
            BlockError::ParentRootMismatch {
                expected: Root::ZERO,
                actual: Root::from_array([1u8; 32]),
            },
            BlockError::ProposerSlashed {
                index: ValidatorIndex::new(0),
            },
            BlockError::ProposerUnknown {
                index: ValidatorIndex::new(9),
                len: 1,
            },
            BlockError::StateRootMismatch {
                expected: Root::ZERO,
                actual: Root::from_array([2u8; 32]),
            },
            BlockError::InvalidSignature {
                which: SignatureKind::BlockProposer,
            },
            BlockError::BlsMaterial("block sig".into()),
            BlockError::StateBlsMaterial("registry pk".into()),
            BlockError::InvalidOperation(OperationError::Invalid {
                op: "attestation",
                detail: "bad".into(),
            }),
            BlockError::InvalidOperation(OperationError::NotYetImplemented("x")),
            BlockError::OperationCountOverflow {
                op: "attestations",
                count: 10,
                max: 1,
            },
            BlockError::BlobBoundExceeded { count: 20, max: 9 },
            BlockError::InvalidPayload,
            BlockError::Engine(EngineError::InvalidPayload),
            BlockError::Engine(EngineError::Transport("rpc".into())),
            BlockError::UnknownParent,
            BlockError::FutureSlot {
                block_slot: Slot::new(100),
                current_slot: Slot::new(1),
            },
            BlockError::AlreadyKnown,
            BlockError::NotDescendedFromFinalized,
            BlockError::DataNotAvailable,
            BlockError::CachePoisoned,
            BlockError::ArithmeticOverflow,
            BlockError::StateNotResident,
            BlockError::StateAccess(StateAccessError::OutOfBounds { index: 0, len: 0 }),
            BlockError::Epoch(EpochError::NotYetImplemented("process_epoch")),
            BlockError::NotYetImplemented("process_withdrawals"),
        ];

        for err in samples {
            // Explicit per-variant classification — no wildcards.
            let class = match err {
                BlockError::SlotNotLater { .. }
                | BlockError::BlockSlotMismatch { .. }
                | BlockError::BlockSlotNotNewer { .. }
                | BlockError::ProposerMismatch { .. }
                | BlockError::ParentRootMismatch { .. }
                | BlockError::ProposerSlashed { .. }
                | BlockError::ProposerUnknown { .. }
                | BlockError::StateRootMismatch { .. }
                | BlockError::InvalidSignature { .. }
                | BlockError::BlsMaterial(_)
                | BlockError::InvalidOperation(OperationError::Invalid { .. })
                | BlockError::OperationCountOverflow { .. }
                | BlockError::BlobBoundExceeded { .. }
                | BlockError::InvalidPayload
                | BlockError::Engine(EngineError::InvalidPayload) => GossipClass::Reject,

                BlockError::UnknownParent
                | BlockError::FutureSlot { .. }
                | BlockError::AlreadyKnown
                | BlockError::NotDescendedFromFinalized
                | BlockError::DataNotAvailable => GossipClass::Ignore,

                BlockError::CachePoisoned
                | BlockError::ArithmeticOverflow
                | BlockError::StateNotResident
                | BlockError::StateAccess(_)
                | BlockError::Epoch(_)
                | BlockError::NotYetImplemented(_)
                | BlockError::StateBlsMaterial(_)
                | BlockError::InvalidOperation(OperationError::NotYetImplemented(_))
                | BlockError::Engine(EngineError::Transport(_)) => GossipClass::Internal,
            };
            assert_eq!(err.gossip_class(), class, "mismatch for {err:?}");
        }
    }

    #[test]
    fn reject_and_ignore_variants_are_not_internal() {
        let reject_ignore: &[BlockError] = &[
            BlockError::InvalidSignature {
                which: SignatureKind::Randao,
            },
            BlockError::BlsMaterial("block-side".into()),
            BlockError::InvalidOperation(OperationError::Invalid {
                op: "deposit",
                detail: "bad amount".into(),
            }),
            BlockError::StateRootMismatch {
                expected: Root::ZERO,
                actual: Root::ZERO,
            },
            BlockError::ProposerMismatch {
                block: ValidatorIndex::new(0),
                expected: ValidatorIndex::new(1),
            },
            BlockError::BlobBoundExceeded { count: 1, max: 0 },
            BlockError::InvalidPayload,
            BlockError::UnknownParent,
            BlockError::FutureSlot {
                block_slot: Slot::new(2),
                current_slot: Slot::new(1),
            },
            BlockError::AlreadyKnown,
            BlockError::NotDescendedFromFinalized,
            BlockError::DataNotAvailable,
        ];
        for err in reject_ignore {
            assert_ne!(
                err.gossip_class(),
                GossipClass::Internal,
                "{err:?} must not be Internal"
            );
        }
    }

    #[test]
    fn internal_variants_named() {
        let internal: &[BlockError] = &[
            BlockError::CachePoisoned,
            BlockError::ArithmeticOverflow,
            BlockError::StateNotResident,
            BlockError::Engine(EngineError::Transport("x".into())),
            BlockError::NotYetImplemented("x"),
            BlockError::Epoch(EpochError::NotYetImplemented("y")),
            BlockError::StateBlsMaterial("registry".into()),
            BlockError::InvalidOperation(OperationError::NotYetImplemented("attestation")),
        ];
        for err in internal {
            assert_eq!(err.gossip_class(), GossipClass::Internal);
        }
    }

    /// SEC-12a-1: state-resident bad pubkey material is Internal, not Reject.
    #[test]
    fn state_bls_material_is_internal_block_is_reject() {
        assert_eq!(
            BlockError::StateBlsMaterial("pk".into()).gossip_class(),
            GossipClass::Internal
        );
        assert_eq!(
            BlockError::BlsMaterial("sig".into()).gossip_class(),
            GossipClass::Reject
        );
    }

    /// SEC-12a-2: nested operation NYI matches top-level NYI (Internal).
    #[test]
    fn operation_nyi_is_internal_invalid_is_reject() {
        assert_eq!(
            BlockError::InvalidOperation(OperationError::NotYetImplemented("x")).gossip_class(),
            GossipClass::Internal
        );
        assert_eq!(
            BlockError::InvalidOperation(OperationError::Invalid {
                op: "x",
                detail: "y".into(),
            })
            .gossip_class(),
            GossipClass::Reject
        );
    }
}
