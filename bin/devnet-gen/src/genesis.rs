//! Build a Fulu-at-genesis `BeaconState` with real proposers and sync committees.

use anyhow::{Context, Result};
use cc_state_transition::{
    get_beacon_proposer_indices, get_next_sync_committee, rebuild_epoch_cache,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::containers::{BeaconBlockHeader, Eth1Data, Validator};
use cc_types::fork::Fork;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei, Root, Slot, ValidatorIndex};
use ssz::Encode;
use tree_hash::TreeHash;

use crate::keys::ValidatorKey;
use crate::params::DevnetParams;

/// 32 ETH in Gwei.
const ACTIVE_BALANCE: u64 = 32_000_000_000;

/// Construct genesis state for preset `P`.
pub fn build_genesis<P: Preset>(
    params: &DevnetParams,
    keys: &[ValidatorKey],
    config: &ChainConfig,
) -> Result<BeaconState<P>> {
    let mut state = BeaconState::<P>::default();

    state.set_genesis_time(params.genesis_time);
    state.set_slot(Slot::new(0));
    state.set_fork(Fork {
        previous_version: config.electra_fork_version,
        current_version: config.fulu_fork_version,
        epoch: Epoch::new(0),
    });

    // Eth1 / deposit path disabled (Electra deposit-requests era).
    state.set_eth1_data(Eth1Data {
        deposit_root: Root::ZERO,
        deposit_count: 0,
        block_hash: Root::from_array([0xee; 32]),
    });
    state.set_eth1_deposit_index(0);
    state.set_deposit_requests_start_index(u64::MAX);

    // Active validators from key material.
    for k in keys {
        let mut creds = [0u8; 32];
        creds[0] = 0x00; // BLS withdrawal credentials prefix
        creds[1..].copy_from_slice(&k.pubkey.as_slice()[..31]);
        // Use ETH1-style credentials (0x01) with zero address for empty withdrawals.
        let mut eth1_creds = [0u8; 32];
        eth1_creds[0] = 0x01;
        eth1_creds[12..].copy_from_slice(&[0u8; 20]);
        let _ = creds;
        state
            .validators_push(Validator {
                pubkey: k.pubkey,
                withdrawal_credentials: Root::from_array(eth1_creds),
                effective_balance: Gwei::new(ACTIVE_BALANCE),
                slashed: false,
                activation_eligibility_epoch: Epoch::new(0),
                activation_epoch: Epoch::new(0),
                exit_epoch: Epoch::new(u64::MAX),
                withdrawable_epoch: Epoch::new(u64::MAX),
            })
            .map_err(|e| anyhow::anyhow!("validators_push: {e}"))?;
        state
            .balances_push(Gwei::new(ACTIVE_BALANCE))
            .map_err(|e| anyhow::anyhow!("balances_push: {e}"))?;
        state
            .previous_epoch_participation_push(0)
            .map_err(|e| anyhow::anyhow!("prev participation: {e}"))?;
        state
            .current_epoch_participation_push(0)
            .map_err(|e| anyhow::anyhow!("curr participation: {e}"))?;
        state
            .inactivity_scores_push(0)
            .map_err(|e| anyhow::anyhow!("inactivity: {e}"))?;
    }

    // GVR = hash_tree_root(validators).
    // Access via tree_hash of the full state field by recomputing from SSZ list root:
    // BeaconState stores validators privately; use canonical approach:
    // hash_tree_root of each validator packed — state.canonical after fill is wrong
    // until GVR is set. Spec: genesis_validators_root = hash_tree_root(state.validators).
    let gvr = validators_tree_hash_root(&state);
    state.set_genesis_validators_root(gvr);

    // Seed RANDAO mixes with a deterministic non-zero pattern from the seed.
    let seed = params.seed_bytes()?;
    for i in 0..state.randao_mixes_len() {
        let mut mix = seed;
        mix[0] ^= (i as u8).wrapping_add(1);
        mix[1] ^= 0x5a;
        state
            .randao_mixes_set(i, Root::from_array(mix))
            .map_err(|e| anyhow::anyhow!("randao_mixes_set: {e}"))?;
    }

    // Pubkey cache (required by process_sync_aggregate).
    for (i, k) in keys.iter().enumerate() {
        state
            .caches_mut()
            .pubkeys
            .insert(k.pubkey, ValidatorIndex::new(i as u64));
    }

    // Sync committees from real selection.
    let committee = get_next_sync_committee(&state)
        .map_err(|e| anyhow::anyhow!("get_next_sync_committee: {e:?}"))?;
    state.set_current_sync_committee(committee.clone());
    state.set_next_sync_committee(committee);

    // Proposer lookahead: epochs 0 and 1 (PROPOSER_LOOKAHEAD_LEN = 2 * SPE).
    let ep0 = get_beacon_proposer_indices(&state, Epoch::new(0))
        .map_err(|e| anyhow::anyhow!("proposer indices epoch 0: {e:?}"))?;
    let ep1 = get_beacon_proposer_indices(&state, Epoch::new(1))
        .map_err(|e| anyhow::anyhow!("proposer indices epoch 1: {e:?}"))?;
    let mut all = ep0;
    all.extend(ep1);
    if all.len() != state.proposer_lookahead_len() {
        anyhow::bail!(
            "lookahead len mismatch: got {} expected {}",
            all.len(),
            state.proposer_lookahead_len()
        );
    }
    for (i, v) in all.into_iter().enumerate() {
        state
            .proposer_lookahead_set(i, v)
            .map_err(|e| anyhow::anyhow!("proposer_lookahead_set: {e}"))?;
    }

    // Empty genesis block header (body root of empty body).
    let empty_body = cc_types::BeaconBlockBody::<P>::default();
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&empty_body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO, // filled on first process_slot
        body_root,
    });

    rebuild_epoch_cache(&mut state).map_err(|e| anyhow::anyhow!("rebuild_epoch_cache: {e:?}"))?;
    state.commit();

    let _ = config; // timing/fork already applied
    Ok(state)
}

/// `hash_tree_root` of the validators list field.
fn validators_tree_hash_root<P: Preset>(state: &BeaconState<P>) -> Root {
    // Reconstruct the SSZ list root by hashing validators in order via a
    // temporary list of the same type capacity — TreeHash on VariableList.
    use ssz_types::VariableList;
    let mut vals = Vec::with_capacity(state.validators_len());
    for i in 0..state.validators_len() {
        if let Some(v) = state.validators_get(i) {
            vals.push(*v);
        }
    }
    let list: VariableList<Validator, P::ValidatorRegistryLimit> =
        VariableList::new(vals).unwrap_or_default();
    Root::from_hash256(list.tree_hash_root())
}

/// SSZ-encode genesis state to bytes.
pub fn encode_genesis_ssz<P: Preset>(state: &BeaconState<P>) -> Vec<u8> {
    state.as_ssz_bytes()
}

/// Write `genesis.ssz`.
pub fn write_genesis_ssz(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}
