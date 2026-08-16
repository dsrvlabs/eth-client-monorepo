//! CC-1F served-path tests: `GetCommitteeShuffling` + `GetValidatorPubkeys`.
//!
//! Both handlers go through the core thread's `Query` command against the head
//! state's shuffling cache / validator registry. Asserts:
//! - served current + next epoch; outside → FAILED_PRECONDITION
//! - dependent_root == block root at start_slot(epoch) − 1
//! - two branches → different dependent_root + different assignments (via RPC)
//! - GetValidatorPubkeys correct for a known range; oversize → INVALID_ARGUMENT

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use cc_chain::core::{CoreConfig, MAX_VALIDATOR_PUBKEYS_PER_REQUEST, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::ChainServiceImpl;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{GetCommitteeShufflingRequest, GetValidatorPubkeysRequest};
use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_state_transition::{
    compute_shuffled_active_indices, decision_root_for_epoch, get_beacon_committee,
    get_current_epoch,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::Validator;
use cc_types::preset::{Minimal, Preset};
use cc_types::primitives::{
    BlsPublicKey, CommitteeIndex, Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconState};
use prometheus_client::registry::Registry;
use tonic::{Code, Request};
use tree_hash::TreeHash;

/// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: cc_state_transition::NewPayloadRequest<'_, P>,
    ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
        Ok(cc_state_transition::PayloadStatus::Valid)
    }
}

const VALIDATORS: usize = 64;
/// Minimal SLOTS_PER_EPOCH = 8 → epoch 1 starts at slot 8.
const SLOT_EPOCH_1: u64 = 8;

fn minimal_config() -> ChainConfig {
    ChainConfig {
        preset_base: PresetName::Minimal,
        config_name: "minimal".into(),
        genesis_fork_version: ForkVersion::from_array([0x00, 0x00, 0x00, 0x01]),
        altair_fork_version: ForkVersion::from_array([0x01, 0x00, 0x00, 0x01]),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array([0x02, 0x00, 0x00, 0x01]),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array([0x03, 0x00, 0x00, 0x01]),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array([0x04, 0x00, 0x00, 0x01]),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array([0x05, 0x00, 0x00, 0x01]),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
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

fn active_validator(i: u64) -> Validator {
    Validator {
        pubkey: BlsPublicKey::from_array({
            let mut pk = [0u8; 48];
            pk[0..8].copy_from_slice(&i.to_le_bytes());
            pk[47] = 0x01;
            pk
        }),
        withdrawal_credentials: Root::from_array({
            let mut c = [0u8; 32];
            c[0] = 0x01;
            c
        }),
        effective_balance: MAX_EFFECTIVE_BALANCE,
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    }
}

/// Head state at `slot` with `n` active validators and seeded block/randao roots.
fn state_with_validators(n: usize, slot: Slot, decision_tag: u8) -> BeaconState<Minimal> {
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(slot);
    for i in 0..n {
        state.validators_push(active_validator(i as u64)).unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
    for i in 0..state.randao_mixes_len() {
        let mut mix = [0u8; 32];
        mix[0] = (i as u8).wrapping_add(1);
        mix[1] = decision_tag;
        mix[2] = 0xab;
        state.randao_mixes_set(i, Root::from_array(mix)).unwrap();
    }
    for i in 0..state
        .block_roots_len()
        .min(Minimal::SLOTS_PER_HISTORICAL_ROOT as usize)
    {
        let mut r = [0u8; 32];
        r[0] = decision_tag;
        r[1] = 0xad;
        r[8..16].copy_from_slice(&(i as u64).to_le_bytes());
        state.block_roots_set(i, Root::from_array(r)).unwrap();
    }
    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new((i as u64) % n as u64))
            .unwrap();
    }
    state
}

fn spawn_svc_with_state(
    state: BeaconState<Minimal>,
) -> (
    ChainServiceImpl,
    cc_chain::CoreThread,
    EventsHandle,
    BeaconState<Minimal>,
) {
    let config = minimal_config();
    let slot = state.slot();
    let anchor_block = BeaconBlock {
        slot,
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    // Clone state before store takes ownership (store keeps its own copy).
    let state_for_local = state.clone();
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let _anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 32,
        subscriber_queue_capacity: 16,
        session_id: Some(0x1F),
        ring_bytes: usize::MAX,
    });
    let head = HeadSnapshotStore::new();
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig::default(),
    );
    let svc = ChainServiceImpl::new(Some(core.handle.clone()), head, events.clone(), metrics);
    (svc, core, events, state_for_local)
}

async fn shutdown(core: cc_chain::CoreThread, events: EventsHandle) {
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── GetCommitteeShuffling: current + next served; outside fails ─────────────

#[tokio::test]
async fn committee_shuffling_current_and_next_match_local() {
    let state = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xaa);
    let (svc, core, events, local) = spawn_svc_with_state(state);

    let current = get_current_epoch(&local).as_u64();
    assert_eq!(current, 1, "fixture is epoch 1");
    let next = current + 1;

    // Current epoch: dependent_root is the true start_slot(epoch)−1 block root,
    // and the packed assignment matches get_beacon_committee.
    {
        let epoch = current;
        let resp = svc
            .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.epoch, epoch);
        let expected_dep = decision_root_for_epoch(&local, Epoch::new(epoch)).unwrap();
        assert_eq!(
            resp.dependent_root,
            expected_dep.as_slice(),
            "dependent_root must equal block root at start_slot(epoch)-1"
        );

        let local_shuffle = compute_shuffled_active_indices(&local, Epoch::new(epoch)).unwrap();
        let expected_indices: Vec<u64> =
            local_shuffle.shuffled.iter().map(|v| v.as_u64()).collect();
        assert_eq!(
            resp.shuffled_indices, expected_indices,
            "RPC shuffling must match compute_shuffled_active_indices on head state"
        );

        let start_slot = epoch * Minimal::SLOTS_PER_EPOCH;
        let local_c =
            get_beacon_committee(&local, Slot::new(start_slot), CommitteeIndex::new(0)).unwrap();
        let local_c_u64: Vec<u64> = local_c.iter().map(|v| v.as_u64()).collect();
        let count = resp
            .committees_per_slot
            .saturating_mul(Minimal::SLOTS_PER_EPOCH);
        let committee_index =
            (start_slot % Minimal::SLOTS_PER_EPOCH).saturating_mul(resp.committees_per_slot);
        let len = resp.shuffled_indices.len() as u64;
        let start = (len.saturating_mul(committee_index)) / count;
        let end = (len.saturating_mul(committee_index.saturating_add(1))) / count;
        let rpc_c = &resp.shuffled_indices[start as usize..end as usize];
        assert_eq!(
            rpc_c,
            local_c_u64.as_slice(),
            "packed RPC assignment must match get_beacon_committee locally"
        );
    }

    // Next epoch: seed is already fixed (RANDAO). Strict decision_root for next
    // is unavailable mid-epoch; served dependent_root is the **current** epoch's
    // true decision root (stable for the whole epoch — no per-slot thrash).
    {
        let epoch = next;
        let resp = svc
            .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.epoch, epoch);
        assert!(
            decision_root_for_epoch(&local, Epoch::new(epoch)).is_err(),
            "fixture is mid-epoch so strict decision_root for next is unavailable"
        );
        let expected_provisional = decision_root_for_epoch(&local, Epoch::new(current)).unwrap();
        assert_eq!(
            resp.dependent_root,
            expected_provisional.as_slice(),
            "next-epoch mid-window dependent_root must equal current epoch decision root"
        );

        let local_shuffle = compute_shuffled_active_indices(&local, Epoch::new(epoch)).unwrap();
        let expected_indices: Vec<u64> =
            local_shuffle.shuffled.iter().map(|v| v.as_u64()).collect();
        assert_eq!(
            resp.shuffled_indices, expected_indices,
            "next-epoch RPC shuffling must match compute_shuffled_active_indices"
        );
    }

    shutdown(core, events).await;
}

/// Same branch at two slots in the same epoch must return the **same** next-epoch
/// `dependent_root` (stable provisional = current decision root, not head-1).
#[tokio::test]
async fn next_epoch_dependent_root_stable_across_slots_same_branch() {
    let tag = 0xaa;
    let state_early = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), tag);
    // Later slot same epoch, same block_roots / RANDAO lineage (same tag seed).
    let state_late = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1 + 4), tag);
    let current = get_current_epoch(&state_early).as_u64();
    let next = current + 1;
    let expected = decision_root_for_epoch(&state_early, Epoch::new(current)).unwrap();
    assert_eq!(
        decision_root_for_epoch(&state_late, Epoch::new(current)).unwrap(),
        expected,
        "fixtures share current-epoch decision root"
    );

    let (svc_a, core_a, events_a, _) = spawn_svc_with_state(state_early);
    let resp_a = svc_a
        .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch: next }))
        .await
        .unwrap()
        .into_inner();
    shutdown(core_a, events_a).await;

    let (svc_b, core_b, events_b, _) = spawn_svc_with_state(state_late);
    let resp_b = svc_b
        .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch: next }))
        .await
        .unwrap()
        .into_inner();
    shutdown(core_b, events_b).await;

    assert_eq!(
        resp_a.dependent_root, resp_b.dependent_root,
        "next-epoch dependent_root must not thrash across slots on the same branch"
    );
    assert_eq!(resp_a.dependent_root, expected.as_slice());
    assert_eq!(
        resp_a.shuffled_indices, resp_b.shuffled_indices,
        "next-epoch assignment is seed-fixed within the epoch"
    );
}

#[tokio::test]
async fn committee_shuffling_outside_window_failed_precondition() {
    let state = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xaa);
    let (svc, core, events, local) = spawn_svc_with_state(state);
    let current = get_current_epoch(&local).as_u64();

    for bad in [0u64, current + 2, current + 99] {
        let err = svc
            .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch: bad }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "epoch {bad} should be FAILED_PRECONDITION"
        );
    }

    shutdown(core, events).await;
}

// ── Two branches → different dependent_root + assignments through RPC ───────

#[tokio::test]
async fn two_branches_different_dependent_root_via_rpc() {
    let mut state_a = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xaa);
    let mut state_b = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xbb);
    let epoch = Epoch::new(1);

    // Distinct decision roots at start_slot(1) − 1 = slot 7.
    let dep_slot = Slot::new(7);
    let idx = (dep_slot.as_u64() % Minimal::SLOTS_PER_HISTORICAL_ROOT) as usize;
    let root_a = Root::from_array([0xaa; 32]);
    let root_b = Root::from_array([0xbb; 32]);
    state_a.block_roots_set(idx, root_a).unwrap();
    state_b.block_roots_set(idx, root_b).unwrap();

    // Different RANDAO → different seeds → different shuffles.
    for i in 0..state_b.randao_mixes_len() {
        let mut mix = [0xff; 32];
        mix[0] = i as u8;
        state_b.randao_mixes_set(i, Root::from_array(mix)).unwrap();
    }

    assert_ne!(
        decision_root_for_epoch(&state_a, epoch).unwrap(),
        decision_root_for_epoch(&state_b, epoch).unwrap()
    );

    let (svc_a, core_a, events_a, _) = spawn_svc_with_state(state_a);
    let resp_a = svc_a
        .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest {
            epoch: epoch.as_u64(),
        }))
        .await
        .unwrap()
        .into_inner();
    shutdown(core_a, events_a).await;

    let (svc_b, core_b, events_b, _) = spawn_svc_with_state(state_b);
    let resp_b = svc_b
        .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest {
            epoch: epoch.as_u64(),
        }))
        .await
        .unwrap()
        .into_inner();
    shutdown(core_b, events_b).await;

    assert_eq!(resp_a.dependent_root, root_a.as_slice());
    assert_eq!(resp_b.dependent_root, root_b.as_slice());
    assert_ne!(
        resp_a.dependent_root, resp_b.dependent_root,
        "competing branches must yield different dependent_root values"
    );
    assert_ne!(
        resp_a.shuffled_indices, resp_b.shuffled_indices,
        "competing branches must yield different committee assignments"
    );
}

// ── GetValidatorPubkeys ─────────────────────────────────────────────────────

#[tokio::test]
async fn validator_pubkeys_range_matches_registry() {
    let state = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xaa);
    let (svc, core, events, local) = spawn_svc_with_state(state);

    let start = 3u64;
    let count = 8u64;
    let resp = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: start,
            count,
            indices: vec![],
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.indices.len(), count as usize);
    assert_eq!(resp.pubkeys.len(), count as usize);
    for (i, idx) in (start..start + count).enumerate() {
        assert_eq!(resp.indices[i], idx);
        let expected = local.validators_get(idx as usize).unwrap().pubkey;
        assert_eq!(resp.pubkeys[i], expected.as_slice());
    }

    // Explicit indices list path.
    let pick = vec![0u64, 10, 20, 63];
    let resp2 = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: 0,
            indices: pick.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp2.indices, pick);
    for (i, &idx) in pick.iter().enumerate() {
        let expected = local.validators_get(idx as usize).unwrap().pubkey;
        assert_eq!(resp2.pubkeys[i], expected.as_slice());
    }

    shutdown(core, events).await;
}

#[tokio::test]
async fn validator_pubkeys_oversize_is_invalid_argument() {
    let state = state_with_validators(VALIDATORS, Slot::new(SLOT_EPOCH_1), 0xaa);
    let (svc, core, events, _) = spawn_svc_with_state(state);

    // Range over the bound.
    let err = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: MAX_VALIDATOR_PUBKEYS_PER_REQUEST + 1,
            indices: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message()
            .contains(&MAX_VALIDATOR_PUBKEYS_PER_REQUEST.to_string()),
        "error should name the bound: {}",
        err.message()
    );

    // Explicit list over the bound.
    let big: Vec<u64> = (0..=MAX_VALIDATOR_PUBKEYS_PER_REQUEST).collect();
    assert_eq!(big.len() as u64, MAX_VALIDATOR_PUBKEYS_PER_REQUEST + 1);
    let err = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: 0,
            indices: big,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Empty / unbounded (count == 0, no indices).
    let err = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: 0,
            indices: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // At the bound succeeds (even if some indices are OOB for a small registry —
    // first check the bound, then OOB). Use 256 with a registry of 64 → OOB after
    // bound passes; use min(bound, registry).
    let ok = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: VALIDATORS as u64,
            indices: vec![],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ok.indices.len(), VALIDATORS);

    shutdown(core, events).await;
}

#[tokio::test]
async fn not_bootstrapped_without_core() {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::new(None, HeadSnapshotStore::new(), events.clone(), metrics);

    let err = svc
        .get_committee_shuffling(Request::new(GetCommitteeShufflingRequest { epoch: 0 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    let err = svc
        .get_validator_pubkeys(Request::new(GetValidatorPubkeysRequest {
            start_index: 0,
            count: 1,
            indices: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    events.shutdown().await;
}

/// Grep-level invariant: both handlers route through `Query` (no second state copy).
#[test]
fn handlers_use_query_command() {
    let src = include_str!("../../../crates/chain-core/src/service.rs");
    assert!(
        src.contains("QueryRequest::CommitteeShuffling"),
        "GetCommitteeShuffling must use CoreCommand::Query"
    );
    assert!(
        src.contains("QueryRequest::ValidatorPubkeys"),
        "GetValidatorPubkeys must use CoreCommand::Query"
    );
    let core_src = include_str!("../../../crates/chain-core/src/core.rs");
    assert!(
        core_src.contains("CoreCommand::Query"),
        "core must handle Query for CC-1F reads"
    );
}
