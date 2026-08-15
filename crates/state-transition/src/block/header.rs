//! `process_block_header` (Architecture §5.1).

use cc_types::containers::BeaconBlockHeader;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{BeaconBlock, BeaconState};
use tree_hash::TreeHash;

use crate::error::BlockError;
use crate::helpers::accessors::get_beacon_proposer_index;

/// Spec `process_block_header`.
///
/// `pre_state_root` is the root returned by [`crate::slots::process_slots`] for
/// the last advanced slot — the value written into `state_roots` and consumed
/// here if `latest_block_header.state_root` is still zero (should already be
/// filled by `process_slot`; the parameter makes the §5.1 threading explicit
/// and testable).
pub fn process_block_header<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    pre_state_root: Root,
) -> Result<(), BlockError> {
    // Verify that the slots match.
    if block.slot != state.slot() {
        return Err(BlockError::BlockSlotMismatch {
            block_slot: block.slot,
            state_slot: state.slot(),
        });
    }

    // Verify that the block is newer than the latest block header.
    if block.slot <= state.latest_block_header().slot {
        return Err(BlockError::BlockSlotNotNewer {
            block_slot: block.slot,
            parent_slot: state.latest_block_header().slot,
        });
    }

    // Verify that proposer index is the correct index (EIP-7917 lookahead).
    let expected = get_beacon_proposer_index(state)?;
    if block.proposer_index != expected {
        return Err(BlockError::ProposerMismatch {
            block: block.proposer_index,
            expected,
        });
    }

    // Ensure parent header carries the pre-state root before we hash it.
    // `process_slot` normally fills this; consume the threaded root if not.
    if state.latest_block_header().state_root == Root::ZERO {
        let mut header = *state.latest_block_header();
        header.state_root = pre_state_root;
        state.set_latest_block_header(header);
    }

    // Verify that the parent matches.
    let parent_root = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));
    if block.parent_root != parent_root {
        return Err(BlockError::ParentRootMismatch {
            expected: parent_root,
            actual: block.parent_root,
        });
    }

    // Cache current block as the new latest block header.
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&block.body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        // Overwritten in the next `process_slot` call.
        state_root: Root::ZERO,
        body_root,
    });

    // Verify proposer is not slashed.
    let idx = block.proposer_index.as_u64() as usize;
    let proposer = state
        .validators_get(idx)
        .ok_or(BlockError::ProposerUnknown {
            index: block.proposer_index,
            len: state.validators_len(),
        })?;
    if proposer.slashed {
        return Err(BlockError::ProposerSlashed {
            index: block.proposer_index,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::root_measure::take_canonical_root_call_count;
    use crate::slots::process_slots;
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::containers::{BeaconBlockHeader, Validator};
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Gwei, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState, Minimal};

    fn test_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
            genesis_fork_version: ForkVersion::from_array([0, 0, 0, 1]),
            altair_fork_version: ForkVersion::from_array([1, 0, 0, 1]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2, 0, 0, 1]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3, 0, 0, 1]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4, 0, 0, 1]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5, 0, 0, 1]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6, 0, 0, 1]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 6,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
            churn_limit_quotient: 32,
            min_per_epoch_churn_limit_electra: 64_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
            shard_committee_period: Epoch::new(64),
            max_blobs_per_block_electra: 9,
        }
    }

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

    #[test]
    fn header_consumes_same_root_written_to_state_roots() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);

        let _ = take_canonical_root_call_count();
        let config = test_config();
        let pre_root = process_slots(&mut state, Slot::new(1), &config).unwrap();
        assert_eq!(state.state_roots_get(0), Some(pre_root));
        // process_slot already filled header.state_root with pre_root.
        assert_eq!(state.latest_block_header().state_root, pre_root);

        let parent_root = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));

        let block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root,
            state_root: Root::ZERO,
            body: Default::default(),
        };

        process_block_header(&mut state, &block, pre_root).unwrap();
        // New header has zero state_root (filled on next process_slot).
        assert_eq!(state.latest_block_header().state_root, Root::ZERO);
        assert_eq!(state.latest_block_header().parent_root, parent_root);
        assert_eq!(state.latest_block_header().slot, Slot::new(1));
    }
}
