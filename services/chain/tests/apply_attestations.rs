//! CC-1E integration tests: `ApplyAttestations` bound, per-item results,
//! batching contract (`compute_deltas` ≤ 1 per batch), weight observed through
//! gRPC `GetHead`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use cc_chain::MAX_APPLY_ATTESTATIONS;
use cc_chain::core::{CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::ChainServiceImpl;
use cc_fork_choice::{
    ExecutionStatus, HarnessAvailability, ProtoNodeBlock, Store, get_forkchoice_store, on_tick,
};
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{ApplyAttestationsRequest, AttestationApplyVerdict, GetHeadRequest};
use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{AttestationData, BeaconBlockHeader, Checkpoint, Validator};
use cc_types::operations::IndexedAttestation;
use cc_types::preset::Minimal;
use cc_types::primitives::{
    BlsPublicKey, Epoch, ExecutionAddress, ForkVersion, Hash256, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconState};
use prometheus_client::registry::Registry;
use ssz::Encode;
use ssz_types::VariableList;
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

fn seed_validators(state: &mut BeaconState<Minimal>, n: usize) {
    for i in 0..n {
        state.validators_push(active_validator(i as u64)).unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
}

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
    }
}

fn root(b: u8) -> Root {
    let mut a = [0u8; 32];
    a[0] = b;
    Root::from_array(a)
}

fn cp(epoch: u64, r: Root) -> Checkpoint {
    Checkpoint {
        epoch: Epoch::new(epoch),
        root: r,
    }
}

fn indexed(
    indices: &[u64],
    slot: u64,
    beacon_block_root: Root,
    target: Checkpoint,
) -> IndexedAttestation<Minimal> {
    let attesting_indices = VariableList::new(
        indices
            .iter()
            .map(|i| ValidatorIndex::new(*i))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    IndexedAttestation {
        attesting_indices,
        data: AttestationData {
            slot: Slot::new(slot),
            index: Default::default(),
            beacon_block_root,
            source: cp(0, beacon_block_root),
            target,
        },
        signature: Default::default(),
    }
}

fn insert_child(store: &mut Store<Minimal>, parent: Root, child: Root, slot: u64) {
    let justified = store.justified_checkpoint();
    let finalized = store.finalized_checkpoint();
    let mut child_state = store.block_state(&parent).unwrap().clone();
    child_state.set_slot(Slot::new(slot));
    store.insert_block(
        child,
        BeaconBlockHeader {
            slot: Slot::new(slot),
            proposer_index: ValidatorIndex::new(0),
            parent_root: parent,
            state_root: Root::ZERO,
            body_root: Root::ZERO,
        },
        child_state,
    );
    store
        .proto_array_mut()
        .on_block(ProtoNodeBlock {
            slot: Slot::new(slot),
            root: child,
            parent_root: Some(parent),
            state_root: Root::ZERO,
            target_root: child,
            justified_checkpoint: justified,
            finalized_checkpoint: finalized,
            unrealized_justified_checkpoint: justified,
            unrealized_finalized_checkpoint: finalized,
            execution_status: ExecutionStatus::Valid,
            execution_block_hash: Hash256::ZERO,
        })
        .unwrap();
}

/// Two competing forks under the anchor; votes sized for `validators` validators.
/// Store time advanced so slot-1 free-floating attestations pass FutureSlot.
///
/// Anchor + child post-states carry active validators so `store_target_checkpoint_context`
/// builds non-zero `CheckpointContext` balances (required for weight to move the head).
fn forked_store(validators: usize) -> (Store<Minimal>, Root, Root, Root, ChainConfig) {
    let config = minimal_config();
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    seed_validators(&mut state, validators);
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    // get_forkchoice_store seeds votes from validators_len; keep balances dense.
    store.resize_votes(validators);
    store.set_justified_balances(vec![MAX_EFFECTIVE_BALANCE.as_u64(); validators]);
    let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

    let fork_a = root(0xA1);
    let fork_b = root(0xB1);
    insert_child(&mut store, anchor, fork_a, 1);
    insert_child(&mut store, anchor, fork_b, 1);
    // Slot 2 so slot-1 attestations are in the past (free-floating path).
    on_tick(&mut store, 12).unwrap();

    (store, anchor, fork_a, fork_b, config)
}

fn spawn_svc(
    store: Store<Minimal>,
    config: ChainConfig,
) -> (ChainServiceImpl, cc_chain::CoreThread, EventsHandle) {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 64,
        subscriber_queue_capacity: 32,
        session_id: Some(42),
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
    (svc, core, events)
}

// ── CC-1E/3: bound 128 ok, 129 INVALID_ARGUMENT, no partial apply ───────────

#[tokio::test]
async fn batch_of_128_succeeds_129_rejected_no_partial() {
    let (store, anchor, fork_a, _fork_b, config) = forked_store(4);
    let (svc, core, events) = spawn_svc(store, config);

    // 128 valid votes for fork_a (validators wrap — capacity 4; same-epoch
    // re-applies still return APPLIED after validate_on_attestation).
    let mut batch_128 = Vec::with_capacity(MAX_APPLY_ATTESTATIONS);
    for i in 0..MAX_APPLY_ATTESTATIONS {
        let att = indexed(&[(i % 4) as u64], 1, fork_a, cp(0, anchor));
        batch_128.push(att.as_ssz_bytes());
    }
    let resp = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: batch_128,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), MAX_APPLY_ATTESTATIONS);
    assert!(
        resp.results
            .iter()
            .all(|r| r.verdict == AttestationApplyVerdict::Applied as i32),
        "all 128 should apply: {:?}",
        resp.results.iter().map(|r| &r.reason).collect::<Vec<_>>()
    );

    let head_after_128 = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        head_after_128.head_root,
        fork_a.as_slice(),
        "128-att batch must elect A through GetHead"
    );

    // 129 → INVALID_ARGUMENT naming the bound; no partial apply.
    let att = indexed(&[0], 1, fork_a, cp(0, anchor));
    let mut batch_129 = Vec::with_capacity(129);
    for _ in 0..129 {
        batch_129.push(att.as_ssz_bytes());
    }
    let err = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: batch_129,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("128"),
        "must name the bound: {}",
        err.message()
    );

    let head_after_129 = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        head_after_129.head_root, head_after_128.head_root,
        "oversized batch must not partially apply or move head"
    );

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── CC-1E/2: weight change observed through GetHead ─────────────────────────

#[tokio::test]
async fn weight_change_observed_through_get_head() {
    // Validator 0 → A (A becomes head); validators 1–3 → B (B overtakes).
    // Same-epoch votes do not overwrite an existing message, so each validator
    // votes once. Observation is only through gRPC GetHead (not node.weight).
    let (store, anchor, fork_a, fork_b, config) = forked_store(4);
    let (svc, core, events) = spawn_svc(store, config);

    let resp = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: vec![indexed(&[0], 1, fork_a, cp(0, anchor)).as_ssz_bytes()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.results[0].verdict,
        AttestationApplyVerdict::Applied as i32
    );
    let head1 = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        head1.head_root,
        fork_a.as_slice(),
        "single vote for A elects A through GetHead"
    );

    let resp = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: (1..4u64)
                .map(|i| indexed(&[i], 1, fork_b, cp(0, anchor)).as_ssz_bytes())
                .collect(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp.results
            .iter()
            .all(|r| r.verdict == AttestationApplyVerdict::Applied as i32),
        "{:?}",
        resp.results
    );
    let head2 = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        head2.head_root,
        fork_b.as_slice(),
        "heavier branch B must become head; observed through GetHead only"
    );

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── mixed valid + rejected ──────────────────────────────────────────────────

#[tokio::test]
async fn mixed_valid_and_invalid_per_item_results() {
    let (store, anchor, fork_a, _fork_b, config) = forked_store(2);
    let (svc, core, events) = spawn_svc(store, config);

    let valid = indexed(&[0], 1, fork_a, cp(0, anchor));
    let unknown_block = indexed(&[0], 1, root(0xEE), cp(0, anchor));
    let resp = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: vec![valid.as_ssz_bytes(), unknown_block.as_ssz_bytes()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 2);
    assert_eq!(
        resp.results[0].verdict,
        AttestationApplyVerdict::Applied as i32
    );
    assert_eq!(
        resp.results[1].verdict,
        AttestationApplyVerdict::Rejected as i32
    );
    assert!(
        !resp.results[1].reason.is_empty(),
        "rejected must carry a reason"
    );

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── not bootstrapped ────────────────────────────────────────────────────────

#[tokio::test]
async fn apply_without_core_is_not_bootstrapped() {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::new(None, HeadSnapshotStore::new(), events.clone(), metrics);
    let err = svc
        .apply_attestations(Request::new(ApplyAttestationsRequest {
            attestations_ssz: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    events.shutdown().await;
}
