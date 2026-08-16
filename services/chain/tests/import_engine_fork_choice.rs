//! S1-A-18 / [ARCH] §8.2: `import → engine → fork-choice` in one process.
//!
//! A `newPayload` timeout must **defer** into `pending_engine` (ADR-P3-05),
//! not park the consensus core. Re-runs S0-A-27 after S1-A-06 (direct
//! `EngineApi`, no `engine_client`). Fails if `pending_engine` is bypassed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use cc_chain::core::{CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::encode_signed_block;
use cc_chain::metrics::{ChainMetrics, ImportResult};
use cc_chain::{DirectEngine, QueryReply, QueryRequest};
use cc_engine_api::EngineApi;
use cc_engine_api::capabilities::CapabilityCache;
use cc_engine_api::config::{TimeoutKnobs, TransportTimeouts};
use cc_engine_api::fastpath::FastpathLane;
use cc_engine_api::fastpath::filter::SubscriptionSet;
use cc_engine_api::methods::eth_syncing::EthSyncingResult;
use cc_engine_api::state::{EngineStateHandle, UpcheckOutcome};
use cc_engine_api::transport::EngineTransport;
use cc_engine_api::version::ElForkSchedule;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_tick};
use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
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
use serde_json::json;
use tokio::runtime::Handle;
use tree_hash::TreeHash;
use wiremock::matchers::{body_string_contains, method as http_method};
use wiremock::{Mock, MockServer, ResponseTemplate};

const NEW_PAYLOAD_TIMEOUT: Duration = Duration::from_millis(80);
const PARK_BUDGET: Duration = Duration::from_millis(800);

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

fn seeded_store_with_engine(
    engine: Arc<DirectEngine>,
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
    on_tick(&mut store, 12).unwrap();

    let parent = store.block_state(&Root::from_hash256(TreeHash::tree_hash_root(&anchor_block)));
    let parent = parent.expect("anchor state");
    let epoch = get_current_epoch::<Minimal>(parent);
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

fn short_timeouts() -> TransportTimeouts {
    TransportTimeouts::from_knobs(&TimeoutKnobs {
        new_payload_ms: NEW_PAYLOAD_TIMEOUT.as_millis() as u64,
        forkchoice_updated_ms: NEW_PAYLOAD_TIMEOUT.as_millis() as u64,
        get_blobs_ms: NEW_PAYLOAD_TIMEOUT.as_millis() as u64,
        exchange_capabilities_ms: NEW_PAYLOAD_TIMEOUT.as_millis() as u64,
        eth_syncing_ms: NEW_PAYLOAD_TIMEOUT.as_millis() as u64,
        multiplier: 1.0,
    })
}

async fn mount_el_double(server: &MockServer) {
    Mock::given(http_method("POST"))
        .and(body_string_contains("eth_syncing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": false
        })))
        .mount(server)
        .await;
    Mock::given(http_method("POST"))
        .and(body_string_contains("engine_exchangeCapabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": ["engine_newPayloadV4", "engine_forkchoiceUpdatedV3"]
        })))
        .mount(server)
        .await;
    // Hang past the caller Duration so the deadline, not the EL body, unparks.
    Mock::given(http_method("POST"))
        .and(body_string_contains("engine_newPayloadV4"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(server)
        .await;
}

async fn direct_engine_on(uri: &str, timeouts: TransportTimeouts) -> Arc<DirectEngine> {
    let transport = Arc::new(EngineTransport::from_secret_bytes(
        uri.to_owned(),
        [1u8; 32],
        timeouts.clone(),
        NEW_PAYLOAD_TIMEOUT,
        None,
    ));
    let schedule = ElForkSchedule {
        osaka_time: 0,
        bpo1_time: None,
        bpo2_time: None,
        amsterdam_time: None,
    };
    let state = EngineStateHandle::new(Arc::new(CapabilityCache::new()), None, NEW_PAYLOAD_TIMEOUT);
    // Admit EL calls so the Duration cap (not the Offline gate) is the unpark.
    state
        .apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing))
        .await;
    let cfg = cc_types::ChainConfig::from_yaml_str(include_str!(
        "../../../crates/types/tests/fixtures/hoodi-config.yaml"
    ))
    .unwrap();
    let bound = cc_engine_api::fastpath::fetch::BlobBound::from_chain_config(&cfg).unwrap();
    let lane = FastpathLane::new(
        Arc::clone(&transport),
        None,
        bound,
        None,
        None,
        SubscriptionSet::empty(),
    );
    let api = EngineApi::from_parts(transport, schedule, Handle::current(), None, state, lane);
    Arc::new(DirectEngine::new(api, timeouts))
}

fn new_payload_hits(requests: &[wiremock::Request]) -> usize {
    requests
        .iter()
        .filter(|r| {
            std::str::from_utf8(&r.body)
                .map(|s| s.contains("engine_newPayloadV4"))
                .unwrap_or(false)
        })
        .count()
}

/// Wiremock EL: core-thread import times out `newPayload`, parks in
/// `pending_engine`, leaves fork-choice unmutated, and unparks inside the
/// explicit Duration. Occupancy stays 0 if `pending_engine` is bypassed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_payload_timeout_defers_via_pending_engine() {
    let server = MockServer::start().await;
    mount_el_double(&server).await;

    let timeouts = short_timeouts();
    let engine = direct_engine_on(&server.uri(), timeouts).await;
    let (store, _anchor, config, signed) = seeded_store_with_engine(Arc::clone(&engine));
    let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 64,
        subscriber_queue_capacity: 32,
        session_id: Some(18),
        ring_bytes: usize::MAX,
    });
    let head = HeadSnapshotStore::new();
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig {
            verify: BlockSignatureStrategy::NoVerification,
            engine: Some(engine),
            ..CoreConfig::default()
        },
    );

    let req = ImportBlockRequest {
        ssz: encode_signed_block(&signed),
        fork: 0,
        root: root.as_slice().to_vec(),
        source: 0,
    };

    let start = std::time::Instant::now();
    let resp = tokio::time::timeout(PARK_BUDGET, core.handle.import_block(req))
        .await
        .expect("core parked on black-holed newPayload")
        .expect("import path must succeed as a deferral, not Status");
    let elapsed = start.elapsed();

    assert_eq!(
        resp.verdict,
        ImportBlockVerdict::DeferredDa as i32,
        "engine timeout must take the Ignore-class deferral verdict, reason={}",
        resp.reason
    );
    assert_eq!(resp.reason, "execution_engine_unavailable");
    assert_eq!(metrics.import_result_count(ImportResult::DeferredEngine), 1);
    assert_eq!(
        metrics.pending_engine_occupancy.get(),
        1,
        "block must be queued on pending_engine; occupancy stays 0 if the map is bypassed"
    );

    let optimistic = core
        .handle
        .query(QueryRequest::IsOptimistic { root: Some(root) })
        .await
        .expect("is_optimistic query");
    match optimistic {
        QueryReply::IsOptimistic { known, .. } => {
            assert!(
                !known,
                "fork-choice store must stay unmutated (not imported)"
            );
        }
        other => panic!("expected IsOptimistic, got {other:?}"),
    }

    let received = server.received_requests().await.unwrap_or_default();
    assert!(
        new_payload_hits(&received) >= 1,
        "newPayload must be issued so the timeout is the trigger, not an Offline gate"
    );
    assert!(
        elapsed < PARK_BUDGET,
        "core parked on newPayload timeout: {elapsed:?}"
    );

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}
