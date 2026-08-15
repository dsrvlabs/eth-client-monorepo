//! CC-3B — `IsOptimistic` surface (fork choice only; no Phase 3 production caller).
//!
//! - `known: false` for a root the store has never seen (Phase 6 must not invent
//!   `is_optimistic: false` for an unknown block).
//! - Synthetic Optimistic / Valid heads answer correctly with `known == true`.
//! - `el_offline: false` while `is_optimistic: true` — the two fields answer
//!   different questions (engine liveness vs payload validation).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cc_chain::core::{CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::ChainServiceImpl;
use cc_fork_choice::{
    ExecutionStatus, HarnessAvailability, ProtoNodeBlock, get_forkchoice_store, get_head,
};
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{IsOptimisticRequest, IsOptimisticResponse};
use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
use cc_proto::engine::{
    ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse, GetEngineStateRequest,
    GetEngineStateResponse, GetInfoRequest, GetInfoResponse, NewPayloadRequest, NewPayloadResponse,
    PayloadStatusV1,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::Minimal;
use cc_types::primitives::{
    Epoch, ExecutionAddress, ForkVersion, Hash256, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconState};
use prometheus_client::registry::Registry;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tree_hash::TreeHash;

/// Private always-Valid test harness (production stub deleted; not exported).
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

fn root_tag(b: u8) -> Root {
    let mut a = [0u8; 32];
    a[0] = b;
    Root::from_array(a)
}

fn hash_tag(b: u8) -> Hash256 {
    Hash256::from([b; 32])
}

/// Anchor store; optional Optimistic child inserted under the anchor.
fn seeded_store(
    optimistic_child: Option<(Root, ExecutionStatus)>,
) -> (cc_fork_choice::Store<Minimal>, Root, ChainConfig) {
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
    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

    if let Some((child, status)) = optimistic_child {
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: child,
                parent_root: Some(anchor),
                state_root: Root::ZERO,
                target_root: child,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: status,
                // H-3: Optimistic/Invalid must carry a non-zero execution hash.
                execution_block_hash: hash_tag(0x42),
            })
            .unwrap();
        // Make the child the head so node-level is_optimistic_node sees it.
        let _ = get_head(&mut store).unwrap();
    }

    (store, anchor, config)
}

fn spawn_svc(
    store: cc_fork_choice::Store<Minimal>,
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

async fn is_optimistic_rpc(svc: &ChainServiceImpl, root: Option<Root>) -> IsOptimisticResponse {
    let req = IsOptimisticRequest {
        root: root.map(|r| r.as_slice().to_vec()),
    };
    svc.is_optimistic(Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

/// CC-3B /2 — unknown root must not answer as a bare `false`.
///
/// A bare-bool response would make **Phase 6 report `is_optimistic: false` for
/// a block the node has never seen**, which is worse than an error.
#[tokio::test]
async fn is_optimistic_unknown_root() {
    let (store, _anchor, config) = seeded_store(None);
    let (svc, core, _events) = spawn_svc(store, config);

    let unknown = root_tag(0xFF);
    let resp = is_optimistic_rpc(&svc, Some(unknown)).await;
    assert!(
        !resp.known,
        "unknown root must set known=false so Phase 6 does not invent \
         is_optimistic:false for a never-seen block; got {resp:?}"
    );
    // is_optimistic is undefined when known=false; leave it alone.

    core.shutdown_and_join().await;
}

/// Synthetic head: Optimistic → true, Valid → false; both known.
#[tokio::test]
async fn is_optimistic_synthetic_head() {
    let optimistic = root_tag(0xA1);
    let (store_opt, anchor, config) = seeded_store(Some((optimistic, ExecutionStatus::Optimistic)));
    let (svc, core, _events) = spawn_svc(store_opt, config.clone());

    let opt_resp = is_optimistic_rpc(&svc, Some(optimistic)).await;
    assert!(opt_resp.known, "Optimistic head root must be known");
    assert!(
        opt_resp.is_optimistic,
        "ProtoNode.execution_status=Optimistic → is_optimistic=true; got {opt_resp:?}"
    );

    // Anchor is Valid (seeded by get_forkchoice_store).
    let valid_resp = is_optimistic_rpc(&svc, Some(anchor)).await;
    assert!(valid_resp.known, "Valid anchor root must be known");
    assert!(
        !valid_resp.is_optimistic,
        "ProtoNode.execution_status=Valid → is_optimistic=false; got {valid_resp:?}"
    );

    // Node-level (root-less) with Optimistic head → true.
    let node_resp = is_optimistic_rpc(&svc, None).await;
    assert!(node_resp.known);
    assert!(
        node_resp.is_optimistic,
        "node-level with Optimistic head must be true; got {node_resp:?}"
    );

    core.shutdown_and_join().await;

    // Valid-only tree: node-level false.
    let (store_valid, _a, config) = seeded_store(None);
    let (svc2, core2, _e2) = spawn_svc(store_valid, config);
    let node_valid = is_optimistic_rpc(&svc2, None).await;
    assert!(node_valid.known);
    assert!(
        !node_valid.is_optimistic,
        "Valid-only tree must not be node-level optimistic; got {node_valid:?}"
    );
    core2.shutdown_and_join().await;
}

/// Mock engine whose `GetEngineState` reports EL **up and answering SYNCING**
/// (`el_offline=false`, `internal_state=syncing`) — the real Phase A shape.
#[derive(Debug, Default)]
struct SyncingEngine;

#[tonic::async_trait]
impl EngineService for SyncingEngine {
    async fn get_info(
        &self,
        _: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse { build_info: None }))
    }

    async fn new_payload(
        &self,
        _: Request<NewPayloadRequest>,
    ) -> Result<Response<NewPayloadResponse>, Status> {
        Ok(Response::new(NewPayloadResponse {
            payload_status: Some(PayloadStatusV1 {
                status: "SYNCING".into(),
                latest_valid_hash: None,
                validation_error: None,
            }),
        }))
    }

    async fn forkchoice_updated(
        &self,
        _: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        Ok(Response::new(ForkchoiceUpdatedResponse {
            payload_status: Some(PayloadStatusV1 {
                status: "SYNCING".into(),
                latest_valid_hash: None,
                validation_error: None,
            }),
            payload_id: None,
        }))
    }

    async fn get_engine_state(
        &self,
        _: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        // EL up + answering SYNCING → external Online → el_offline=false.
        Ok(Response::new(GetEngineStateResponse {
            el_offline: false,
            internal_state: "syncing".into(),
        }))
    }
}

async fn spawn_syncing_engine() -> (SocketAddr, oneshot::Sender<()>) {
    let svc = EngineServiceServer::new(SyncingEngine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = rx.await;
                },
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, tx)
}

/// The combination a conflated implementation cannot produce: EL online
/// (`el_offline=false`, answering SYNCING) while fork choice is optimistic.
#[tokio::test]
async fn el_offline_false_while_optimistic_true() {
    // Chain: Optimistic head from fork choice (payload NOT_VALIDATED / SYNCING).
    let child = root_tag(0xB2);
    let (store, _anchor, config) = seeded_store(Some((child, ExecutionStatus::Optimistic)));
    let (svc, core, _events) = spawn_svc(store, config);

    let opt = is_optimistic_rpc(&svc, None).await;
    assert!(opt.known);
    assert!(
        opt.is_optimistic,
        "is_optimistic must come from fork choice (Optimistic head); got {opt:?}"
    );

    // Engine: EL up and answering SYNCING → el_offline=false.
    let (addr, shutdown) = spawn_syncing_engine().await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut engine = cc_proto::engine::engine_service_client::EngineServiceClient::new(channel);
    let engine_state = engine
        .get_engine_state(Request::new(GetEngineStateRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(
        !engine_state.el_offline,
        "EL answering SYNCING must have el_offline=false; got {engine_state:?}"
    );
    assert_eq!(engine_state.internal_state, "syncing");

    // Simultaneous: the combination a conflated implementation cannot produce.
    assert!(
        !engine_state.el_offline && opt.is_optimistic,
        "node can be el_offline:false AND is_optimistic:true \
         (engine Online/Syncing + fork-choice Optimistic head)"
    );

    let _ = shutdown.send(());
    core.shutdown_and_join().await;
}
