//! Execution-engine seam (Architecture §5.5, CC-14).
//!
//! The whole Phase 1 EL-validation surface lives in this file so Phase 3 can
//! substitute a real Engine API client by changing one body, not `process_block`
//! or fork choice. Spec names and shapes are intentional.

use cc_types::execution::ExecutionPayload;
use cc_types::operations::ExecutionRequests;
use cc_types::preset::Preset;
use cc_types::primitives::{Hash256, Root};

use crate::error::EngineError;

/// Spec `VersionedHash` — versioned hash of a blob KZG commitment (`Bytes32`).
pub type VersionedHash = Hash256;

/// Spec `NewPayloadRequest` argument to `verify_and_notify_new_payload`.
///
/// The payload and execution requests are borrowed so the seam costs no clone
/// of multi-megabyte bodies.
#[derive(Debug)]
pub struct NewPayloadRequest<'a, P: Preset> {
    /// Execution payload from the beacon block body.
    pub execution_payload: &'a ExecutionPayload<P>,
    /// Versioned hashes derived from `blob_kzg_commitments` (filled by CC-12b).
    pub versioned_hashes: Vec<VersionedHash>,
    /// Parent beacon block root (`block.parent_root`).
    pub parent_beacon_block_root: Root,
    /// Electra execution requests from the block body.
    pub execution_requests: &'a ExecutionRequests<P>,
}

/// Engine response status (Engine API `PayloadStatusV1` — all five wire values).
///
/// Five variants match the five `PayloadStatusV1` statuses (ADR P3-04). Spec
/// aliases [`PayloadStatus::is_not_validated`] / [`PayloadStatus::is_invalidated`]
/// fold them for fork-choice bookkeeping; the metric surface needs all five.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadStatus {
    /// EL accepted the payload as valid.
    Valid,
    /// EL rejected the payload.
    Invalid {
        /// Latest valid execution block hash known to the EL, if any.
        latest_valid_hash: Option<Hash256>,
    },
    /// EL is still syncing — "requisite data missing". Spec `NOT_VALIDATED`.
    Syncing,
    /// Well-formed, not on the canonical chain, ancestors known. Spec `NOT_VALIDATED`.
    Accepted,
    /// EL rejected the block hash itself. Spec `INVALIDATED`; `latestValidHash` is always none.
    InvalidBlockHash,
}

impl PayloadStatus {
    /// Spec `NOT_VALIDATED` ≜ `SYNCING | ACCEPTED`.
    pub const fn is_not_validated(&self) -> bool {
        matches!(self, Self::Syncing | Self::Accepted)
    }

    /// Spec `INVALIDATED` ≜ `INVALID | INVALID_BLOCK_HASH`.
    pub const fn is_invalidated(&self) -> bool {
        matches!(self, Self::Invalid { .. } | Self::InvalidBlockHash)
    }
}

/// Spec-shaped execution-engine seam.
///
/// Object-safe: `P` is on the trait, so `dyn ExecutionEngine<Mainnet>` is fine
/// (required by the CC-14/3 substitution test).
pub trait ExecutionEngine<P: Preset>: Send + Sync {
    /// Spec `verify_and_notify_new_payload`.
    ///
    /// Exactly one call site exists in the tree — inside
    /// [`crate::block::process_execution_payload`] (CC-14/1).
    fn verify_and_notify_new_payload(
        &self,
        request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError>;
}

// Phase-1 optimistic always-Valid production stub **deleted** at CC-32b (not
// renamed, not feature-gated, not exported). Production uses
// `services/chain/src/engine_client.rs` (`EngineApiClient`). Unit tests that
// need always-Valid define a private harness type in their own module.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::block::{TransitionContext, process_execution_payload};
    use crate::error::{BlockError, GossipClass};
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::preset::Mainnet;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};

    /// Private always-Valid harness for this module only (not a library export).
    #[derive(Debug, Default, Clone, Copy)]
    struct AcceptEngine;

    impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, EngineError> {
            Ok(PayloadStatus::Valid)
        }
    }

    /// Test-only engine that always reports the payload invalid.
    ///
    /// Lives here so the substitution test is a pure trait-object swap
    /// (CC-14/3): no production branch on "is stubbed".
    #[derive(Debug, Default, Clone, Copy)]
    struct RejectAllEngine;

    impl<P: Preset> ExecutionEngine<P> for RejectAllEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, EngineError> {
            Ok(PayloadStatus::Invalid {
                latest_valid_hash: None,
            })
        }
    }

    /// Test-only engine that fails at the transport layer.
    #[derive(Debug, Default, Clone, Copy)]
    struct TransportFailEngine;

    impl<P: Preset> ExecutionEngine<P> for TransportFailEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, EngineError> {
            Err(EngineError::Transport("rpc down".into()))
        }
    }

    fn test_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "mainnet-test".into(),
            genesis_fork_version: ForkVersion::from_array([0; 4]),
            altair_fork_version: ForkVersion::from_array([1; 4]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2; 4]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3; 4]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4; 4]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5; 4]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6; 4]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 12,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 1,
            deposit_contract_address: ExecutionAddress::ZERO,
        }
    }

    /// Block + state aligned so local payload checks pass (engine is the variable).
    fn ready_state_and_block(config: &ChainConfig) -> (BeaconState<Mainnet>, BeaconBlock<Mainnet>) {
        let mut state = BeaconState::<Mainnet>::default();
        state.set_slot(Slot::new(1));
        state.set_genesis_time(0);
        let mut block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        // parent_hash == latest header block_hash (both zero by default).
        // prev_randao == randao mix at epoch 0 (zero by default).
        block.body.execution_payload.timestamp =
            state.genesis_time() + state.slot().as_u64() * config.seconds_per_slot;
        (state, block)
    }

    /// CC-14/3: swap `RejectAllEngine` in via `dyn ExecutionEngine<Mainnet>` and
    /// observe `process_execution_payload` return the typed invalid-payload error.
    /// No production code path branches on the engine identity.
    #[test]
    fn reject_all_engine_substitution_returns_invalid_payload() {
        let config = test_config();
        // Object-safety assertion: this type must compile (CC-14/3).
        let engine: &dyn ExecutionEngine<Mainnet> = &RejectAllEngine;
        let ctx = TransitionContext::<Mainnet>::new(&config, engine);

        let (mut state, block) = ready_state_and_block(&config);

        let err = process_execution_payload(&mut state, &block, &ctx).unwrap_err();
        assert!(
            matches!(err, BlockError::Engine(EngineError::InvalidPayload)),
            "expected Engine(InvalidPayload), got {err:?}"
        );
        assert_eq!(
            err.gossip_class(),
            GossipClass::Reject,
            "PayloadStatus::Invalid must classify as Reject"
        );
    }

    /// Transport failure through the seam classifies as [`GossipClass::Internal`].
    #[test]
    fn engine_transport_failure_is_gossip_internal() {
        let config = test_config();
        let engine: &dyn ExecutionEngine<Mainnet> = &TransportFailEngine;
        let ctx = TransitionContext::<Mainnet>::new(&config, engine);

        let (mut state, block) = ready_state_and_block(&config);

        let err = process_execution_payload(&mut state, &block, &ctx).unwrap_err();
        assert!(
            matches!(err, BlockError::Engine(EngineError::Transport(_))),
            "expected Engine(Transport(_)), got {err:?}"
        );
        assert_eq!(err.gossip_class(), GossipClass::Internal);
    }

    /// Accept engine returns Valid; `process_execution_payload` accepts when local checks pass.
    #[test]
    fn accept_engine_accepts() {
        let config = test_config();
        let engine: &dyn ExecutionEngine<Mainnet> = &AcceptEngine;
        let ctx = TransitionContext::<Mainnet>::new(&config, engine);

        let (mut state, block) = ready_state_and_block(&config);

        process_execution_payload(&mut state, &block, &ctx).expect("accept engine accepts");
    }

    /// Blob bound exceeded classifies as [`GossipClass::Reject`].
    #[test]
    fn blob_bound_exceeded_is_gossip_reject() {
        let config = test_config(); // max 9 at epoch 0
        let engine: &dyn ExecutionEngine<Mainnet> = &AcceptEngine;
        let ctx = TransitionContext::<Mainnet>::new(&config, engine);

        let (mut state, mut block) = ready_state_and_block(&config);
        // 10 commitments > max 9.
        for _ in 0..10 {
            block
                .body
                .blob_kzg_commitments
                .push(Default::default())
                .expect("10 < MAX_BLOB_COMMITMENTS");
        }

        let err = process_execution_payload(&mut state, &block, &ctx).unwrap_err();
        assert!(
            matches!(err, BlockError::BlobBoundExceeded { count: 10, max: 9 }),
            "expected BlobBoundExceeded, got {err:?}"
        );
        assert_eq!(err.gossip_class(), GossipClass::Reject);
    }

    /// Direct classification of constructed errors (mirrors §5.3 mapping).
    #[test]
    fn payload_status_invalid_and_transport_gossip_classes() {
        assert_eq!(
            BlockError::Engine(EngineError::InvalidPayload).gossip_class(),
            GossipClass::Reject
        );
        assert_eq!(
            BlockError::Engine(EngineError::Transport("x".into())).gossip_class(),
            GossipClass::Internal
        );
    }

    /// CC-34 /1 first half: all five `PayloadStatusV1` values map onto the two
    /// spec aliases — `is_not_validated` ≜ Syncing|Accepted,
    /// `is_invalidated` ≜ Invalid|InvalidBlockHash; Valid is neither.
    #[test]
    fn payload_status_aliases() {
        let cases: [(PayloadStatus, bool, bool); 5] = [
            (PayloadStatus::Valid, false, false),
            (
                PayloadStatus::Invalid {
                    latest_valid_hash: None,
                },
                false,
                true,
            ),
            (PayloadStatus::Syncing, true, false),
            (PayloadStatus::Accepted, true, false),
            (PayloadStatus::InvalidBlockHash, false, true),
        ];
        for (status, not_validated, invalidated) in cases {
            assert_eq!(
                status.is_not_validated(),
                not_validated,
                "{status:?}: is_not_validated"
            );
            assert_eq!(
                status.is_invalidated(),
                invalidated,
                "{status:?}: is_invalidated"
            );
            // The two aliases are mutually exclusive and Valid is in neither.
            assert!(
                !(status.is_not_validated() && status.is_invalidated()),
                "{status:?}: aliases must not overlap"
            );
        }
    }

    /// Engine that always returns a fixed status (test driver for outbox / NOT_VALIDATED).
    #[derive(Debug, Clone)]
    struct FixedStatusEngine(PayloadStatus);

    impl<P: Preset> ExecutionEngine<P> for FixedStatusEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, EngineError> {
            Ok(self.0.clone())
        }
    }

    /// Outbox records `Valid` on the success path.
    #[test]
    fn outbox_records_valid() {
        let config = test_config();
        let engine = FixedStatusEngine(PayloadStatus::Valid);
        let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
        let (mut state, block) = ready_state_and_block(&config);

        process_execution_payload(&mut state, &block, &ctx).expect("Valid completes");
        assert_eq!(
            ctx.take_payload_status(),
            Some(PayloadStatus::Valid),
            "outbox must hold Valid after the sole call site"
        );
    }

    /// Outbox records `Syncing` / `Accepted` (NOT_VALIDATED) on the success path.
    #[test]
    fn outbox_records_not_validated() {
        let config = test_config();
        for status in [PayloadStatus::Syncing, PayloadStatus::Accepted] {
            let engine = FixedStatusEngine(status.clone());
            let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
            let (mut state, block) = ready_state_and_block(&config);

            process_execution_payload(&mut state, &block, &ctx)
                .unwrap_or_else(|e| panic!("{status:?} must complete, got {e:?}"));
            assert_eq!(
                ctx.take_payload_status(),
                Some(status.clone()),
                "outbox must hold {status:?} after the sole call site"
            );
        }
    }

    /// Outbox is also written on the INVALIDATED path (carries latest_valid_hash for CC-35).
    #[test]
    fn outbox_records_invalidated() {
        let config = test_config();
        let status = PayloadStatus::Invalid {
            latest_valid_hash: Some(Hash256::repeat_byte(0xab)),
        };
        let engine = FixedStatusEngine(status.clone());
        let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
        let (mut state, block) = ready_state_and_block(&config);

        let err = process_execution_payload(&mut state, &block, &ctx).unwrap_err();
        assert!(matches!(
            err,
            BlockError::Engine(EngineError::InvalidPayload)
        ));
        assert_eq!(ctx.take_payload_status(), Some(status));

        // InvalidBlockHash likewise.
        let engine = FixedStatusEngine(PayloadStatus::InvalidBlockHash);
        let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
        let (mut state, block) = ready_state_and_block(&config);
        let err = process_execution_payload(&mut state, &block, &ctx).unwrap_err();
        assert!(matches!(
            err,
            BlockError::Engine(EngineError::InvalidPayload)
        ));
        assert_eq!(
            ctx.take_payload_status(),
            Some(PayloadStatus::InvalidBlockHash)
        );
    }

    /// CC-32 /5: NOT_VALIDATED (SYNCING and ACCEPTED) completes the state transition.
    #[test]
    fn not_validated_completes() {
        let config = test_config();
        for status in [PayloadStatus::Syncing, PayloadStatus::Accepted] {
            let engine = FixedStatusEngine(status.clone());
            let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
            let (mut state, block) = ready_state_and_block(&config);

            process_execution_payload(&mut state, &block, &ctx).unwrap_or_else(|e| {
                panic!("NOT_VALIDATED ({status:?}) must complete the transition, got {e:?}")
            });
            // Header is cached — transition completed, not short-circuited.
            assert_eq!(
                state.latest_execution_payload_header().timestamp,
                block.body.execution_payload.timestamp
            );
        }
    }
}
