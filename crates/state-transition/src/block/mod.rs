//! `state_transition` and `process_block` (Architecture §5.1–5.2).
//!
//! `process_block` is a **flat list of calls in spec order with no
//! conditionals** — the order *is* the spec. Handler bodies after
//! [`header::process_block_header`] land in CC-12b–d as stubs that return
//! [`BlockError::NotYetImplemented`] until those issues fill them.

pub mod header;

use std::marker::PhantomData;

use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};

use crate::engine_seam::{ExecutionEngine, NewPayloadRequest, PayloadStatus};
use crate::error::{BlockError, EngineError};
use crate::root_measure::measured_canonical_root;
use crate::signatures::verify_block_signatures;
use crate::slots::process_slots;
use crate::BlockSignatureStrategy;

pub use header::process_block_header;

// ---------------------------------------------------------------------------
// TransitionContext (engine trait lives in `engine_seam.rs`, CC-14)
// ---------------------------------------------------------------------------

/// Per-transition context (config + engine).
pub struct TransitionContext<'a, P: Preset> {
    /// Runtime chain config (blob schedule, forks, …).
    pub config: &'a ChainConfig,
    /// Execution-engine seam (CC-14).
    pub engine: &'a dyn ExecutionEngine<P>,
    _phantom: PhantomData<P>,
}

impl<'a, P: Preset> std::fmt::Debug for TransitionContext<'a, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionContext")
            .field("config_name", &self.config.config_name)
            .finish_non_exhaustive()
    }
}

impl<'a, P: Preset> TransitionContext<'a, P> {
    /// Construct a context.
    pub fn new(config: &'a ChainConfig, engine: &'a dyn ExecutionEngine<P>) -> Self {
        Self {
            config,
            engine,
            _phantom: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// state_transition / process_block
// ---------------------------------------------------------------------------

/// Spec `state_transition`.
///
/// 1. [`process_slots`] → pre-state root (threaded)
/// 2. signature verification per [`BlockSignatureStrategy`]
/// 3. [`process_block`]
/// 4. post-state root check (second `canonical_root` on a one-slot advance)
pub fn state_transition<P: Preset>(
    state: &mut BeaconState<P>,
    block: &SignedBeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    verify: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    let message = &block.message;

    // Process slots (including those with no blocks) since the previous block.
    let pre_state_root = process_slots(state, message.slot)?;

    // Verify signature(s).
    verify_block_signatures(state, block, verify)?;

    // Process block.
    process_block(state, message, ctx, pre_state_root)?;

    // Verify state root — second measured canonical_root on a block slot.
    let post_root = measured_canonical_root(state);
    if post_root != message.state_root {
        return Err(BlockError::StateRootMismatch {
            expected: message.state_root,
            actual: post_root,
        });
    }

    Ok(())
}

/// Spec `process_block` — flat call list in Fulu beacon-chain order.
///
/// Calls [`BeaconState::commit`] at the end (§3.4).
pub fn process_block<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    pre_state_root: Root,
) -> Result<(), BlockError> {
    process_block_header(state, block, pre_state_root)?;
    process_withdrawals(state, block)?;
    process_execution_payload(state, block, ctx)?;
    process_randao(state, block)?;
    process_eth1_data(state, block)?;
    process_operations(state, block, ctx)?;
    process_sync_aggregate(state, block)?;
    state.commit();
    Ok(())
}

// ---------------------------------------------------------------------------
// Handler stubs (CC-12b–d fill bodies; order is fixed here)
// ---------------------------------------------------------------------------

/// CC-12b — `process_withdrawals`.
pub fn process_withdrawals<P: Preset>(
    _state: &mut BeaconState<P>,
    _block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    Err(BlockError::NotYetImplemented("process_withdrawals"))
}

/// Spec `process_execution_payload` — sole call site of the CC-14 engine seam.
///
/// Local checks (payload header match, timestamp, blob bound, versioned-hash
/// derivation, `latest_execution_payload_header` update) land in CC-12b. This
/// issue only wires the engine call so the seam is load-bearing and greppable
/// (CC-14/1).
pub fn process_execution_payload<P: Preset>(
    _state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    // CC-12b fills versioned hashes from `body.blob_kzg_commitments`.
    let request = NewPayloadRequest {
        execution_payload: &block.body.execution_payload,
        versioned_hashes: Vec::new(),
        parent_beacon_block_root: block.parent_root,
        execution_requests: &block.body.execution_requests,
    };

    match ctx.engine.verify_and_notify_new_payload(request)? {
        PayloadStatus::Valid | PayloadStatus::Syncing => Ok(()),
        PayloadStatus::Invalid { .. } => Err(BlockError::Engine(EngineError::InvalidPayload)),
    }
}

/// CC-12b — `process_randao`.
pub fn process_randao<P: Preset>(
    _state: &mut BeaconState<P>,
    _block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    Err(BlockError::NotYetImplemented("process_randao"))
}

/// CC-12b — `process_eth1_data`.
pub fn process_eth1_data<P: Preset>(
    _state: &mut BeaconState<P>,
    _block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    Err(BlockError::NotYetImplemented("process_eth1_data"))
}

/// CC-12c / CC-12d — `process_operations` (slashings, attestations, deposits,
/// exits, BLS changes, execution requests).
pub fn process_operations<P: Preset>(
    _state: &mut BeaconState<P>,
    _block: &BeaconBlock<P>,
    _ctx: &TransitionContext<'_, P>,
) -> Result<(), BlockError> {
    Err(BlockError::NotYetImplemented("process_operations"))
}

/// CC-12d — `process_sync_aggregate`.
pub fn process_sync_aggregate<P: Preset>(
    _state: &mut BeaconState<P>,
    _block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    Err(BlockError::NotYetImplemented("process_sync_aggregate"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine_seam::StubOptimisticEngine;
    use crate::root_measure::{canonical_root_call_count, take_canonical_root_call_count};
    use crate::slots::{process_slot, process_slots};
    use cc_types::containers::{BeaconBlockHeader, Validator};
    use cc_types::primitives::{Gwei, Slot, ValidatorIndex};
    use cc_types::{BeaconState, Minimal};

    fn seed(state: &mut BeaconState<Minimal>) {
        for i in 0..state.proposer_lookahead_len() {
            state
                .proposer_lookahead_set(i, ValidatorIndex::new(0))
                .unwrap();
        }
        state
            .validators_push(Validator {
                pubkey: Default::default(),
                withdrawal_credentials: Root::ZERO,
                effective_balance: Gwei::new(32_000_000_000),
                slashed: false,
                activation_eligibility_epoch: Default::default(),
                activation_epoch: Default::default(),
                exit_epoch: cc_types::primitives::Epoch::new(u64::MAX),
                withdrawable_epoch: cc_types::primitives::Epoch::new(u64::MAX),
            })
            .unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();
        state.set_latest_block_header(BeaconBlockHeader {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body_root: Root::ZERO,
        });
        state.set_slot(Slot::new(0));
    }

    /// A block-slot advance of one empty slot performs exactly **one**
    /// `canonical_root` inside `process_slots`; the post-state check is the
    /// second. Together they are the §5.1 budget of two.
    #[test]
    fn block_slot_transition_two_canonical_roots() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);

        let _ = take_canonical_root_call_count();

        // Pre-state root via process_slots (one call).
        let pre_root = process_slots(&mut state, Slot::new(1)).unwrap();
        assert_eq!(canonical_root_call_count(), 1);
        assert_eq!(state.state_roots_get(0), Some(pre_root));
        assert_eq!(state.latest_block_header().state_root, pre_root);

        // Simulate the post-state check that state_transition performs.
        let post = measured_canonical_root(&mut state);
        assert_eq!(canonical_root_call_count(), 2);
        // post differs from pre once header was filled, but count is what matters.
        let _ = post;

        // Confirm process_slot itself is a single call.
        let mut s2 = BeaconState::<Minimal>::default();
        seed(&mut s2);
        s2.set_slot(Slot::new(2));
        let _ = take_canonical_root_call_count();
        let _ = process_slot(&mut s2).unwrap();
        assert_eq!(canonical_root_call_count(), 1);
    }

    #[test]
    fn process_block_stops_at_first_unimplemented_handler() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);
        let pre = process_slots(&mut state, Slot::new(1)).unwrap();
        let parent =
            Root::from_hash256(tree_hash::TreeHash::tree_hash_root(state.latest_block_header()));
        let block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: parent,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        // Minimal config for context.
        let config = minimal_test_config();
        let engine = StubOptimisticEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        let err = process_block(&mut state, &block, &ctx, pre).unwrap_err();
        assert!(matches!(
            err,
            BlockError::NotYetImplemented("process_withdrawals")
        ));
    }

    fn minimal_test_config() -> ChainConfig {
        use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
        use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion};
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
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
            seconds_per_slot: 6,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
        }
    }
}
