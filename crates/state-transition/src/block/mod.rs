//! `state_transition` and `process_block` (Architecture §5.1–5.2).
//!
//! `process_block` is a **flat list of calls in spec order with no
//! conditionals** — the order *is* the spec.

pub mod eth1_data;
pub mod execution_payload;
pub mod header;
pub mod operations;
pub mod randao;
pub mod sync_aggregate;
pub mod withdrawals;

use std::cell::RefCell;
use std::marker::PhantomData;

use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};

use crate::BlockSignatureStrategy;
use crate::engine_seam::{ExecutionEngine, PayloadStatus};
use crate::error::BlockError;
use crate::root_measure::measured_canonical_root;
use crate::signatures::verify_block_signatures;
use crate::slots::process_slots;

pub use eth1_data::process_eth1_data;
pub use execution_payload::process_execution_payload;
pub use header::process_block_header;
pub use operations::{
    ProcessAttestationOpts, process_attestation, process_attester_slashing,
    process_bls_to_execution_change, process_consolidation_request, process_deposit,
    process_deposit_request, process_operations, process_proposer_slashing, process_voluntary_exit,
    process_withdrawal_request,
};
pub use randao::process_randao;
pub use sync_aggregate::{process_sync_aggregate, process_sync_aggregate_with_opts};
pub use withdrawals::{get_expected_withdrawals, process_withdrawals};

// ---------------------------------------------------------------------------
// TransitionContext (engine trait lives in `engine_seam.rs`, CC-14)
// ---------------------------------------------------------------------------

/// Per-transition context (config + engine + payload-status outbox).
///
/// The outbox carries the EL's [`PayloadStatus`] out of the state transition
/// without a second `verify_and_notify_new_payload` call site (CC-14/1, ADR P3-03).
/// Written at the sole call site in [`super::execution_payload::process_execution_payload`];
/// read by `on_block` from `CC-34a` onward. This commit writes and leaves unread (D-4).
///
/// `RefCell` rather than `Mutex`: `TransitionContext` is stack-local per import and is
/// not required to be `Sync` (§12/8 compile check).
pub struct TransitionContext<'a, P: Preset> {
    /// Runtime chain config (blob schedule, forks, …).
    pub config: &'a ChainConfig,
    /// Execution-engine seam (CC-14).
    pub engine: &'a dyn ExecutionEngine<P>,
    /// CC-32/6 — payload status leaves the transition through here.
    ///
    /// Written exactly once at the sole engine call site (both Valid/NOT_VALIDATED
    /// and INVALIDATED paths). Not read in this commit (D-4).
    payload_status_outbox: RefCell<Option<PayloadStatus>>,
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
            payload_status_outbox: RefCell::new(None),
            _phantom: PhantomData,
        }
    }

    /// Record the payload status returned by the sole engine call site.
    ///
    /// `pub(crate)` so only this crate's `process_execution_payload` may write;
    /// external crates (fork-choice) consume via [`Self::take_payload_status`] only.
    /// Panics in debug builds if written twice (outbox is single-shot per transition).
    pub(crate) fn set_payload_status(&self, status: PayloadStatus) {
        let mut slot = self.payload_status_outbox.borrow_mut();
        debug_assert!(
            slot.is_none(),
            "payload_status_outbox written twice in one transition"
        );
        *slot = Some(status);
    }

    /// Take the recorded payload status (if any). Used by tests; production
    /// readers arrive in `CC-34a`.
    pub fn take_payload_status(&self) -> Option<PayloadStatus> {
        self.payload_status_outbox.borrow_mut().take()
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

    // Verify signature(s): proposer + RANDAO + CC-12c operation set (§5.2).
    // process_operations then runs with verify_signatures=false.
    verify_block_signatures(state, block, ctx.config, verify)?;

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
    // Signatures already verified in `state_transition` under the chosen strategy.
    process_operations(state, block, ctx, false)?;
    process_sync_aggregate(state, block)?;
    state.commit();
    Ok(())
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
    fn process_block_completes_with_empty_ops_and_empty_sync() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);
        // Map default (zero) sync-committee pubkeys to the seeded validator so
        // reward accounting can resolve indices without a registry scan.
        state
            .caches_mut()
            .pubkeys
            .insert(Default::default(), ValidatorIndex::new(0));
        // eth1 deposits disabled (unset start index) so empty deposits list is ok.
        state.set_deposit_requests_start_index(u64::MAX);

        let pre = process_slots(&mut state, Slot::new(1)).unwrap();
        let parent = Root::from_hash256(tree_hash::TreeHash::tree_hash_root(
            state.latest_block_header(),
        ));
        use crate::helpers::accessors::{get_current_epoch, get_randao_mix};
        let epoch = get_current_epoch(&state);
        let mix = get_randao_mix(&state, epoch).unwrap();
        let mut body = cc_types::BeaconBlockBody::<Minimal>::default();
        body.execution_payload.prev_randao = mix;
        body.execution_payload.timestamp = state.genesis_time() + state.slot().as_u64() * 6; // minimal seconds_per_slot
        body.execution_payload.parent_hash = state.latest_execution_payload_header().block_hash;
        // Empty participant set requires the infinity signature (eth_fast_aggregate_verify).
        body.sync_aggregate.sync_committee_signature =
            cc_types::primitives::BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE);
        let block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: parent,
            state_root: Root::ZERO,
            body,
        };
        let config = minimal_test_config();
        let engine = StubOptimisticEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        process_block(&mut state, &block, &ctx, pre).expect("full process_block should complete");
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
