//! CC-44a /3: GetCanonicalRoots unary RPC.
//!
//! - One root per canonical slot in `[start, end]`
//! - Typed error below finalized retention / not bootstrapped

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use cc_chain::core::{CoreConfig, QueryReply, QueryRequest, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::{
    ChainServiceImpl, REASON_BELOW_FINALIZED_RETENTION, REASON_NOT_BOOTSTRAPPED,
};
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::GetCanonicalRootsRequest;
use cc_proto::error_info_from_status;
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::Minimal;
use cc_types::primitives::{
    Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconState};
use prometheus_client::registry::Registry;
use tonic::{Code, Request};

/// Private always-Valid test harness.
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

fn seeded_store() -> (cc_fork_choice::Store<Minimal>, ChainConfig) {
    let config = minimal_config();
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    (store, config)
}

fn spawn_svc(
    store: cc_fork_choice::Store<Minimal>,
    config: ChainConfig,
) -> (ChainServiceImpl, cc_chain::CoreThread, EventsHandle) {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 32,
        ring_bytes: usize::MAX,
        subscriber_queue_capacity: 16,
        session_id: Some(0x44),
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

#[tokio::test]
async fn get_canonical_roots_returns_one_root_per_slot() {
    let (store, config) = seeded_store();
    let (svc, core, events) = spawn_svc(store, config);

    // Inclusive single-slot range at the anchor.
    let resp = svc
        .get_canonical_roots(Request::new(GetCanonicalRootsRequest {
            start_slot: 0,
            end_slot: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.roots.len(), 1);
    assert_eq!(resp.roots[0].len(), 32);

    // Inclusive multi-slot range: one root per slot.
    let resp = svc
        .get_canonical_roots(Request::new(GetCanonicalRootsRequest {
            start_slot: 0,
            end_slot: 3,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.roots.len(), 4);
    for r in &resp.roots {
        assert_eq!(r.len(), 32);
    }

    let reply = core
        .handle
        .query(QueryRequest::CanonicalRoots {
            start_slot: 0,
            end_slot: 1,
        })
        .await
        .unwrap();
    match reply {
        QueryReply::CanonicalRoots { roots } => assert_eq!(roots.len(), 2),
        other => panic!("unexpected {other:?}"),
    }

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

#[tokio::test]
async fn get_canonical_roots_below_finalized_is_typed_error() {
    // Without core: NOT_BOOTSTRAPPED.
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::new(None, HeadSnapshotStore::new(), events.clone(), metrics);
    let err = svc
        .get_canonical_roots(Request::new(GetCanonicalRootsRequest {
            start_slot: 0,
            end_slot: 1,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&err).unwrap().unwrap();
    assert_eq!(info.reason, REASON_NOT_BOOTSTRAPPED);

    // Typed retention reason (helper used by the core path).
    let status = cc_chain::status_below_finalized(0, 32);
    assert_eq!(status.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&status).unwrap().unwrap();
    assert_eq!(info.reason, REASON_BELOW_FINALIZED_RETENTION);

    events.shutdown().await;
}

#[tokio::test]
async fn get_canonical_roots_rejects_inverted_range() {
    let (store, config) = seeded_store();
    let (svc, core, events) = spawn_svc(store, config);
    let err = svc
        .get_canonical_roots(Request::new(GetCanonicalRootsRequest {
            start_slot: 10,
            end_slot: 5,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}
