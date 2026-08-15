//! S0-A-27 / P0-15: a black-holed engine must *defer* the block (pending_engine),
//! not park the consensus core on an undeadlined `block_on`.
//!
//! S1-A-18 re-runs this assertion after the EL-bridge fold.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use cc_chain::ImportCounters;
use cc_chain::engine_client::{EngineApiClient, EngineRpcDeadlines};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::{encode_signed_block, import_block_with_early};
use cc_chain::metrics::{ChainMetrics, ImportResult};
use cc_chain::pending_engine::PendingEngine;
use cc_chain::residency::Residency;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_tick};
use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
use cc_proto::engine::{
    FetchBlobsRequest, FetchBlobsResponse, ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse,
    GetEngineStateRequest, GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
    NewPayloadRequest as ProtoNewPayloadRequest, NewPayloadResponse,
};
use cc_state_transition::BlockSignatureStrategy;
use cc_state_transition::helpers::accessors::{get_current_epoch, get_randao_mix};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, Validator};
use cc_types::preset::Minimal;
use cc_types::primitives::{
    Epoch, ExecutionAddress, ForkVersion, Gwei, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tree_hash::TreeHash;

/// Engine that accepts the RPC and never responds.
#[derive(Debug, Default)]
struct BlackHoleEngine;

#[tonic::async_trait]
impl EngineService for BlackHoleEngine {
    async fn get_info(
        &self,
        _: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        std::future::pending().await
    }

    async fn new_payload(
        &self,
        _: Request<ProtoNewPayloadRequest>,
    ) -> Result<Response<NewPayloadResponse>, Status> {
        std::future::pending().await
    }

    async fn forkchoice_updated(
        &self,
        _: Request<ForkchoiceUpdatedRequest>,
    ) -> Result<Response<ForkchoiceUpdatedResponse>, Status> {
        std::future::pending().await
    }

    async fn get_engine_state(
        &self,
        _: Request<GetEngineStateRequest>,
    ) -> Result<Response<GetEngineStateResponse>, Status> {
        std::future::pending().await
    }

    async fn fetch_blobs(
        &self,
        _: Request<FetchBlobsRequest>,
    ) -> Result<Response<FetchBlobsResponse>, Status> {
        std::future::pending().await
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
        churn_limit_quotient: 32,
        min_per_epoch_churn_limit_electra: 64_000_000_000,
        max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
        shard_committee_period: Epoch::new(64),
        max_blobs_per_block_electra: 9,
    }
}

/// Genesis whose slot-1 child reaches `process_execution_payload`.
fn seeded_store_with_engine(
    engine: Arc<EngineApiClient>,
) -> (
    cc_fork_choice::Store<Minimal>,
    Root,
    ChainConfig,
    SignedBeaconBlock<Minimal>,
) {
    let config = minimal_config();
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    state
        .validators_push(Validator {
            pubkey: Default::default(),
            withdrawal_credentials: Root::ZERO,
            effective_balance: Gwei::new(32_000_000_000),
            slashed: false,
            activation_eligibility_epoch: Default::default(),
            activation_epoch: Default::default(),
            exit_epoch: Epoch::new(u64::MAX),
            withdrawable_epoch: Epoch::new(u64::MAX),
        })
        .unwrap();
    state.balances_push(Gwei::new(32_000_000_000)).unwrap();
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body_root: Root::from_hash256(TreeHash::tree_hash_root(
            &BeaconBlockBody::<Minimal>::default(),
        )),
    });

    let state_root = Root::from_hash256(TreeHash::tree_hash_root(&state));
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root,
        body: BeaconBlockBody::default(),
    };
    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        engine,
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    // Slot 1 must not be future relative to store time.
    on_tick(&mut store, 12).unwrap();

    let parent = store.block_state(&Root::from_hash256(TreeHash::tree_hash_root(&anchor_block)));
    let parent = parent.expect("anchor state");
    let epoch = get_current_epoch::<Minimal>(parent);
    // Payload checks run after process_slots has advanced the cloned parent to slot 1.
    let mut body = BeaconBlockBody::<Minimal>::default();
    body.execution_payload.parent_hash = parent.latest_execution_payload_header().block_hash;
    body.execution_payload.prev_randao = get_randao_mix(parent, epoch).unwrap();
    body.execution_payload.timestamp = parent.genesis_time() + config.seconds_per_slot;

    let signed = SignedBeaconBlock {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::from_hash256(TreeHash::tree_hash_root(&anchor_block)),
            state_root: Root::ZERO,
            body,
        },
        signature: Default::default(),
    };
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    (store, anchor_root, config, signed)
}

async fn spawn_black_hole() -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let svc = EngineServiceServer::new(BlackHoleEngine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
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

/// Injected black-holed engine: import returns Deferred, parks in `pending_engine`,
/// does not write the block, and unparks within the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn black_holed_engine_defers_block_not_park() {
    let (addr, shutdown) = spawn_black_hole().await;
    let uri = format!("http://{addr}");
    let handle = Handle::current();
    let engine = Arc::new(EngineApiClient::from_handle_with_deadlines(
        handle,
        uri,
        EngineRpcDeadlines::for_test(),
    ));

    let start = std::time::Instant::now();
    let result = std::thread::Builder::new()
        .name("chain-core".into())
        .spawn(move || {
            let (mut store, _anchor, config, signed) = seeded_store_with_engine(engine);
            let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
            let before: Vec<Root> = store.blocks().keys().copied().collect();

            let mut registry = Registry::default();
            let metrics = ChainMetrics::register(&mut registry);
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
            let head = HeadSnapshotStore::new();
            let counters = ImportCounters::default();
            let mut snapshot_sequence = 0u64;
            let mut residency = Residency::<Minimal>::with_defaults();
            let mut pending_engine = PendingEngine::new();
            let ssz = encode_signed_block(&signed);
            let req = ImportBlockRequest {
                ssz,
                fork: 0,
                root: root.as_slice().to_vec(),
                source: 0,
            };
            let outcome = import_block_with_early(
                &mut store,
                &mut residency,
                &config,
                &head,
                &event_tx,
                &metrics,
                &counters,
                &mut snapshot_sequence,
                req,
                BlockSignatureStrategy::NoVerification,
                None,
                None,
                None,
                None,
                Some(&mut pending_engine),
                None,
            )
            .expect("import path must succeed as a deferral, not Status");

            let after: Vec<Root> = store.blocks().keys().copied().collect();
            (
                outcome.response.verdict,
                outcome.response.reason,
                pending_engine.contains(&root),
                pending_engine.len(),
                before,
                after,
                metrics.import_result_count(ImportResult::DeferredEngine),
            )
        })
        .expect("spawn chain-core")
        .join()
        .expect("chain-core join");
    let elapsed = start.elapsed();

    let (verdict, reason, parked, pending_len, before, after, deferred_metric) = result;
    assert_eq!(
        verdict,
        ImportBlockVerdict::DeferredDa as i32,
        "engine timeout must take the Ignore-class deferral verdict, reason={reason}"
    );
    assert_eq!(reason, "execution_engine_unavailable");
    assert!(parked, "block must be queued on pending_engine");
    assert_eq!(pending_len, 1);
    assert_eq!(
        before, after,
        "store.blocks must be unmutated (not imported)"
    );
    assert_eq!(deferred_metric, 1);
    assert!(
        elapsed < Duration::from_millis(800),
        "core parked on black-holed engine: {elapsed:?}"
    );

    let _ = shutdown.send(());
}
