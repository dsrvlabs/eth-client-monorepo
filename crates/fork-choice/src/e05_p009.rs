//! E0.5 (S0-A-32) — P0-09 falsifier.
//!
//! Decode the Hoodi anchor via the S0a-A-01 harness, grow the registry through
//! [`super::integrate_block`], and apply an attestation naming the new
//! validator. Skips when `HOODI_FIXTURES_CACHE` is unset (CC-10b).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "../../state-transition/tests/support/anchor.rs"]
mod support;

use std::sync::Arc;

use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_state_transition::{
    EngineError, ExecutionEngine, NewPayloadRequest, PayloadStatus, compute_epoch_at_slot,
};
use cc_types::containers::{AttestationData, Checkpoint, Validator};
use cc_types::operations::IndexedAttestation;
use cc_types::preset::Mainnet;
use cc_types::primitives::{BlsPublicKey, Epoch, Hash256, Root, Slot, ValidatorIndex};
use cc_types::{BeaconBlock, BeaconState};
use ssz_types::VariableList;

use super::{get_forkchoice_store, integrate_block};
use crate::da_seam::HarnessAvailability;
use crate::execution_status::ExecutionStatus;
use crate::head_cache::justified_balances_snapshot;
use crate::on_attestation::{OnAttestationError, on_attestation};
use crate::on_tick::on_tick;
use crate::store::LatestMessage;

use support::{CACHE_ENV, FETCH_HINT, cache_env_is_set, load_anchor, load_hoodi_config};

/// Always-Valid engine (same shape as the other `on_block` unit tests).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl ExecutionEngine<Mainnet> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, Mainnet>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

fn push_post_anchor_validator(state: &mut BeaconState<Mainnet>, index: u64, activation: Epoch) {
    let mut pubkey = [0u8; 48];
    pubkey[0] = 0xE5;
    pubkey[1..9].copy_from_slice(&index.to_le_bytes());
    pubkey[47] = 0x01;
    state
        .validators_push(Validator {
            pubkey: BlsPublicKey::from_array(pubkey),
            withdrawal_credentials: Root::from_array({
                let mut c = [0u8; 32];
                c[0] = 0x01;
                c
            }),
            effective_balance: MAX_EFFECTIVE_BALANCE,
            slashed: false,
            activation_eligibility_epoch: activation,
            activation_epoch: activation,
            exit_epoch: FAR_FUTURE_EPOCH,
            withdrawable_epoch: FAR_FUTURE_EPOCH,
        })
        .unwrap();
    state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    state.previous_epoch_participation_push(0).unwrap();
    state.current_epoch_participation_push(0).unwrap();
    state.inactivity_scores_push(0).unwrap();
}

/// An attestation naming a validator activated after the Hoodi anchor, evaluated
/// against a grown registry, must not hit `ValidatorIndexOutOfRange` or drop.
#[test]
fn e05_post_anchor_attestation_against_grown_hoodi_registry() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {CACHE_ENV} unset — Hoodi BeaconState SSZ is not in git; \
             {FETCH_HINT} (see crates/types/tests/fixtures/README.md)"
        );
        return;
    }

    let config = load_hoodi_config().unwrap_or_else(|e| panic!("{e}"));
    let loaded = load_anchor().unwrap_or_else(|e| panic!("{e}"));
    let anchor_n = loaded.validators_len();
    assert!(
        anchor_n > 10_000,
        "E0.5 must run against a real Hoodi registry, not a synthetic store; got {anchor_n}"
    );

    let anchor_slot = loaded.state.slot();
    let anchor_block = loaded.block.message.clone();
    let mut store = get_forkchoice_store(
        loaded.state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .expect("get_forkchoice_store from decoded Hoodi anchor");

    let anchor = store.justified_checkpoint().root;
    assert_eq!(
        store.vote_capacity(),
        anchor_n,
        "store must be sized to the decoded anchor registry"
    );

    // Slot S+1 attestation is in the past once the store is at S+2.
    let sps = store.seconds_per_slot();
    let tick = store.time().saturating_add(sps.saturating_mul(2));
    on_tick(&mut store, tick).unwrap();

    let child_slot = Slot::new(anchor_slot.as_u64().saturating_add(1));
    let mut child_bytes = [0u8; 32];
    child_bytes[0] = 0xE5;
    child_bytes[31] = 0x05;
    let child = Root::from_array(child_bytes);

    let mut post = store
        .block_state(&anchor)
        .expect("anchor post-state")
        .clone();
    post.set_slot(child_slot);
    let post_anchor_index = anchor_n as u64;
    let activation = Epoch::new(compute_epoch_at_slot::<Mainnet>(anchor_slot).as_u64() + 1);
    push_post_anchor_validator(&mut post, post_anchor_index, activation);
    let grown_n = post.validators_len();
    assert_eq!(grown_n, anchor_n + 1);

    let block = BeaconBlock {
        slot: child_slot,
        proposer_index: ValidatorIndex::new(0),
        parent_root: anchor,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    integrate_block(
        &mut store,
        child,
        &block,
        post,
        ExecutionStatus::Valid,
        Hash256::from([0xE5; 32]),
    )
    .expect("integrate_block must grow tables from the trusted post-state");

    assert_eq!(
        store.vote_capacity(),
        grown_n,
        "vote table must grow with the trusted post-state registry"
    );
    assert_eq!(store.justified_balances().len(), grown_n);

    let justified = store.justified_checkpoint();
    let snap = justified_balances_snapshot(&mut store, justified).unwrap();
    assert_eq!(
        snap.len(),
        grown_n,
        "justified_balances_snapshot must track the registry after growth"
    );

    let target_epoch = compute_epoch_at_slot::<Mainnet>(child_slot);
    let att = IndexedAttestation {
        attesting_indices: VariableList::new(vec![ValidatorIndex::new(post_anchor_index)]).unwrap(),
        data: AttestationData {
            slot: child_slot,
            index: Default::default(),
            beacon_block_root: child,
            source: store.justified_checkpoint(),
            target: Checkpoint {
                epoch: target_epoch,
                root: anchor,
            },
        },
        signature: Default::default(),
    };

    match on_attestation(&mut store, &att, false, &config) {
        Ok(()) => {}
        Err(OnAttestationError::ValidatorIndexOutOfRange { index, capacity }) => {
            panic!(
                "E0.5: post-anchor validator {index} hit ValidatorIndexOutOfRange \
                 (capacity {capacity})"
            );
        }
        Err(e) => panic!("E0.5: attestation dropped: {e}"),
    }

    assert_eq!(
        store.latest_message(ValidatorIndex::new(post_anchor_index)),
        Some(LatestMessage {
            epoch: target_epoch,
            root: child,
        }),
        "post-anchor vote must be recorded, not dropped"
    );
}
