//! `process_slots` / `process_slot` (Architecture §5.1).
//!
//! **Root threading:** `process_slot` computes the state root **once** via
//! [`measured_canonical_root`], writes it into `state_roots`, back-fills
//! `latest_block_header.state_root` when zero, and **returns** that root so
//! `state_transition` / `process_block_header` reuse it. A block slot that
//! advances one empty predecessor therefore pays exactly **two**
//! `canonical_root` calls across the full transition (pre + post).

use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use tree_hash::TreeHash;

use crate::epoch::process_epoch;
use crate::error::BlockError;
use crate::root_measure::measured_canonical_root;

/// Advance `state` through empty slots up to (but not past) `target`.
///
/// Returns the state root computed by the **last** [`process_slot`] call — the
/// pre-block state root when `target` is a block slot. Callers thread this into
/// [`crate::block::process_block_header`].
///
/// Calls [`BeaconState::commit`] at the end (§3.4).
pub fn process_slots<P: Preset>(
    state: &mut BeaconState<P>,
    target: Slot,
    config: &ChainConfig,
) -> Result<Root, BlockError> {
    if state.slot() >= target {
        return Err(BlockError::SlotNotLater {
            state_slot: state.slot(),
            target,
        });
    }

    let mut last_root = Root::ZERO;
    while state.slot() < target {
        last_root = process_slot(state)?;
        // Process epoch on the start slot of the next epoch.
        let next_slot = state
            .slot()
            .checked_add(1)
            .ok_or(BlockError::ArithmeticOverflow)?;
        if next_slot.as_u64() % P::SLOTS_PER_EPOCH == 0 {
            process_epoch(state, config)?;
        }
        state.set_slot(next_slot);
    }

    state.commit();
    Ok(last_root)
}

/// Spec `process_slot`: cache state root, back-fill header state root, cache
/// block root. Computes [`BeaconState::canonical_root`] **once**.
///
/// Returns the computed previous-state root (also written to `state_roots`).
pub fn process_slot<P: Preset>(state: &mut BeaconState<P>) -> Result<Root, BlockError> {
    // Cache state root — single measured hash.
    let previous_state_root = measured_canonical_root(state);
    let index = (state.slot().as_u64() % P::SLOTS_PER_HISTORICAL_ROOT) as usize;
    state.state_roots_set(index, previous_state_root)?;

    // Cache latest block header state root when still zero.
    let header = state.latest_block_header();
    if header.state_root == Root::ZERO {
        let mut filled = *header;
        filled.state_root = previous_state_root;
        state.set_latest_block_header(filled);
    }

    // Cache block root = hash_tree_root(latest_block_header).
    let previous_block_root =
        Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));
    state.block_roots_set(index, previous_block_root)?;

    Ok(previous_state_root)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::root_measure::{canonical_root_call_count, take_canonical_root_call_count};
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::containers::{BeaconBlockHeader, Validator};
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Gwei, ValidatorIndex};
    use cc_types::{BeaconState, Minimal};

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

    fn seed_proposer_lookahead(state: &mut BeaconState<Minimal>) {
        // Minimal PROPOSER_LOOKAHEAD_LEN = (1+1)*8 = 16.
        for i in 0..state.proposer_lookahead_len() {
            state
                .proposer_lookahead_set(i, ValidatorIndex::new(0))
                .unwrap();
        }
    }

    fn sample_validator() -> Validator {
        Validator {
            pubkey: Default::default(),
            withdrawal_credentials: Root::ZERO,
            effective_balance: Gwei::new(32_000_000_000),
            slashed: false,
            activation_eligibility_epoch: Default::default(),
            activation_epoch: Default::default(),
            exit_epoch: cc_types::primitives::Epoch::new(u64::MAX),
            withdrawable_epoch: cc_types::primitives::Epoch::new(u64::MAX),
        }
    }

    #[test]
    fn process_slot_single_root_writes_state_and_block_roots() {
        let mut state = BeaconState::<Minimal>::default();
        state.set_slot(Slot::new(0));
        state.set_latest_block_header(BeaconBlockHeader {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body_root: Root::ZERO,
        });
        seed_proposer_lookahead(&mut state);
        state.validators_push(sample_validator()).unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();

        let _ = take_canonical_root_call_count();
        let root = process_slot(&mut state).unwrap();
        assert_eq!(canonical_root_call_count(), 1);
        assert_eq!(state.state_roots_get(0), Some(root));
        assert_ne!(state.block_roots_get(0), Some(Root::ZERO));
        assert_eq!(state.latest_block_header().state_root, root);
    }

    #[test]
    fn process_slots_one_slot_returns_pre_root() {
        let mut state = BeaconState::<Minimal>::default();
        state.set_slot(Slot::new(0));
        state.set_latest_block_header(BeaconBlockHeader::default());
        seed_proposer_lookahead(&mut state);

        let _ = take_canonical_root_call_count();
        let config = test_config();
        let root = process_slots(&mut state, Slot::new(1), &config).unwrap();
        assert_eq!(state.slot(), Slot::new(1));
        assert_eq!(state.state_roots_get(0), Some(root));
        assert_eq!(canonical_root_call_count(), 1);
    }
}
