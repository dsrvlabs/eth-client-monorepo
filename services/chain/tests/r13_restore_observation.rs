//! S0-A-31 / R-13 observation (b) — instrumented restore against the Hoodi pin.
//!
//! Requires `--features s0-a-31-observe` (see `Cargo.toml` `required-features`).
//! That feature is the only compile of the raw decode arm; production restore
//! stays on `from_ssz_bytes_hydrated`.
//!
//! Does **not** patch P1-A/22 or P1-A/23. It drives `handle_restore_accumulated`
//! (same gate + `apply_restore_set` + `end_stream` path as `RestoreFromStore`)
//! against the committed Hoodi snapshot and records which of the four sites
//! fire: decode, `on_block`, `end_stream`, DA-deferred drop.
//!
//! Replay set is one successor of the pin, streamed twice: `DEFERRED` first
//! (DA gate, no `process_block`) then `AVAILABLE` (`process_block`). That is
//! the restore/replay shape; it cannot be inferred from S0-A-30's standalone
//! `process_block` call.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "../../../crates/state-transition/tests/support/anchor.rs"]
mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cc_chain::core::CoreConfig;
use cc_chain::epoch_context::EpochContextStore;
use cc_chain::head::HeadSnapshotStore;
use cc_chain::metrics::ChainMetrics;
use cc_chain::restore::{
    AccumulatedRestore, RestoreHandlerDeps, RestoreTracePoint, handle_restore_accumulated,
    reset_restore_trace, restore_force_raw_decode, restore_trace,
};
use cc_proto::chain::{RestoreBlock, RestoreDaStatus, RestoreFooter, RestoreHeader};
use cc_proto::engine::engine_service_server::{EngineService, EngineServiceServer};
use cc_proto::engine::{
    ForkchoiceUpdatedRequest, ForkchoiceUpdatedResponse, GetEngineStateRequest,
    GetEngineStateResponse, GetInfoRequest, GetInfoResponse,
    NewPayloadRequest as ProtoNewPayloadRequest, NewPayloadResponse, PayloadStatusV1,
};
use cc_state_transition::{
    compute_time_at_slot, get_beacon_proposer_index, get_current_epoch, get_expected_withdrawals,
    get_randao_mix, process_slots,
};
use cc_types::config::ChainConfig;
use cc_types::containers::SyncAggregate;
use cc_types::execution::ExecutionPayload;
use cc_types::primitives::{BlsSignature, Root, Slot};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, Mainnet, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use ssz::Encode;
use ssz_types::VariableList;
use support::{
    CACHE_ENV, FETCH_HINT, cache_env_is_set, load_anchor, load_hoodi_config, resolve_anchor_paths,
};
use tokio::sync::oneshot;
use tonic::Status;
use tree_hash::TreeHash;

/// Always-VALID engine gRPC so restore's real `EngineApiClient` can complete
/// `newPayload` (AcceptEngine would hide the engine hop).
#[derive(Debug, Default)]
struct CountingEngine {
    calls: AtomicU64,
}

#[tonic::async_trait]
impl EngineService for CountingEngine {
    async fn get_info(
        &self,
        _: tonic::Request<GetInfoRequest>,
    ) -> Result<tonic::Response<GetInfoResponse>, Status> {
        Ok(tonic::Response::new(GetInfoResponse { build_info: None }))
    }

    async fn new_payload(
        &self,
        _: tonic::Request<ProtoNewPayloadRequest>,
    ) -> Result<tonic::Response<NewPayloadResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(NewPayloadResponse {
            payload_status: Some(PayloadStatusV1 {
                status: "VALID".into(),
                latest_valid_hash: None,
                validation_error: None,
            }),
        }))
    }

    async fn forkchoice_updated(
        &self,
        _: tonic::Request<ForkchoiceUpdatedRequest>,
    ) -> Result<tonic::Response<ForkchoiceUpdatedResponse>, Status> {
        Ok(tonic::Response::new(ForkchoiceUpdatedResponse {
            payload_status: Some(PayloadStatusV1 {
                status: "VALID".into(),
                latest_valid_hash: None,
                validation_error: None,
            }),
            payload_id: None,
        }))
    }

    async fn get_engine_state(
        &self,
        _: tonic::Request<GetEngineStateRequest>,
    ) -> Result<tonic::Response<GetEngineStateResponse>, Status> {
        Ok(tonic::Response::new(GetEngineStateResponse {
            el_offline: false,
            internal_state: "synced".into(),
        }))
    }
}

async fn spawn_counting_engine() -> (SocketAddr, oneshot::Sender<()>, Arc<CountingEngine>) {
    let mock = Arc::new(CountingEngine::default());
    let svc = EngineServiceServer::from_arc(Arc::clone(&mock));
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
    (addr, tx, mock)
}

fn matching_next_block(state: &BeaconState<Mainnet>, config: &ChainConfig) -> BeaconBlock<Mainnet> {
    let proposer = get_beacon_proposer_index(state).expect("proposer");
    let (withdrawals, _) = get_expected_withdrawals(state).expect("withdrawals");
    let epoch = get_current_epoch(state);
    let prev_randao = get_randao_mix(state, epoch).expect("randao");
    let timestamp =
        compute_time_at_slot(state.genesis_time(), state.slot(), config.seconds_per_slot);
    let parent_hash = state.latest_execution_payload_header().block_hash;
    let parent_root = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));

    let payload = ExecutionPayload::<Mainnet> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: state.latest_execution_payload_header().block_number + 1,
        gas_limit: state.latest_execution_payload_header().gas_limit,
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };

    let body = BeaconBlockBody::<Mainnet> {
        execution_payload: payload,
        eth1_data: state.eth1_data(),
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE),
        },
        ..Default::default()
    };

    BeaconBlock {
        slot: state.slot(),
        proposer_index: proposer,
        parent_root,
        state_root: Root::ZERO,
        body,
    }
}

struct RestoreSnapshot {
    state_ssz: Vec<u8>,
    anchor_block_ssz: Vec<u8>,
    successor: SignedBeaconBlock<Mainnet>,
    successor_root: Root,
    snapshot_slot: u64,
    config: ChainConfig,
}

fn load_restore_snapshot() -> RestoreSnapshot {
    let paths = resolve_anchor_paths().unwrap_or_else(|e| panic!("{e}"));
    let state_ssz = std::fs::read(&paths.state_ssz)
        .unwrap_or_else(|e| panic!("read {}: {e}", paths.state_ssz.display()));
    let anchor_block_ssz = std::fs::read(&paths.block_ssz)
        .unwrap_or_else(|e| panic!("read {}: {e}", paths.block_ssz.display()));
    let config = load_hoodi_config().unwrap_or_else(|e| panic!("{e}"));
    let loaded = load_anchor().unwrap_or_else(|e| panic!("{e}"));
    let snapshot_slot = loaded.state.slot().as_u64();
    let mut advanced = loaded.state;
    let next = Slot::new(snapshot_slot + 1);
    process_slots(&mut advanced, next, &config).expect("process_slots");
    let successor = SignedBeaconBlock {
        message: matching_next_block(&advanced, &config),
        signature: Default::default(),
    };
    drop(advanced);
    let successor_root = Root::from_hash256(TreeHash::tree_hash_root(&successor.message));
    RestoreSnapshot {
        state_ssz,
        anchor_block_ssz,
        successor,
        successor_root,
        snapshot_slot,
        config,
    }
}

fn accumulated(snap: &RestoreSnapshot) -> AccumulatedRestore {
    let ssz = snap.successor.as_ssz_bytes();
    let root = snap.successor_root.as_slice().to_vec();
    let deferred = RestoreBlock {
        ssz: ssz.clone(),
        fork: 0,
        root: root.clone(),
        da_status: RestoreDaStatus::Deferred as i32,
    };
    let available = RestoreBlock {
        ssz,
        fork: 0,
        root,
        da_status: RestoreDaStatus::Available as i32,
    };
    AccumulatedRestore {
        header: Some(RestoreHeader {
            schema_version: 1,
            config_digest: vec![0u8; 32],
            anchor_ssz: Vec::new(),
            split_ssz: Vec::new(),
            fork_choice_scalars_ssz: Vec::new(),
            snapshot_slot: snap.snapshot_slot,
            state_ssz_total_bytes: snap.state_ssz.len() as u64,
            anchor_block_ssz: snap.anchor_block_ssz.clone(),
            anchor_block_fork: 0,
        }),
        state_ssz: snap.state_ssz.clone(),
        blocks: vec![deferred, available],
        footer: Some(RestoreFooter {
            expected_head_root: snap.successor_root.as_slice().to_vec(),
            expected_head_slot: snap.successor.message.slot.as_u64(),
        }),
        empty: false,
    }
}

struct ArmTrace {
    name: &'static str,
    apply: String,
    points: Vec<RestoreTracePoint>,
}

impl ArmTrace {
    fn reached(&self, pred: impl Fn(&RestoreTracePoint) -> bool) -> bool {
        self.points.iter().any(pred)
    }

    fn dump(&self) {
        eprintln!("S0-A-31 arm `{}` apply={}", self.name, self.apply);
        for (i, p) in self.points.iter().enumerate() {
            eprintln!("  [{i}] {p}");
        }
    }
}

async fn run_arm(name: &'static str, snap: &RestoreSnapshot, raw: bool) -> ArmTrace {
    restore_force_raw_decode(raw);
    reset_restore_trace();

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
    let deps = RestoreHandlerDeps {
        gate: cc_chain::RestoreGate::new(Duration::from_secs(600)),
        head: HeadSnapshotStore::new(),
        epoch: EpochContextStore::new(),
        events: event_tx,
        metrics,
        chain_config: snap.config.clone(),
        core_cfg: CoreConfig {
            slot_tick_enabled: false,
            ..CoreConfig::default()
        },
        _marker: (),
    };

    let t0 = Instant::now();
    let apply = match handle_restore_accumulated::<Mainnet>(deps, accumulated(snap)).await {
        Ok(resp) => {
            let r = resp.into_inner();
            format!(
                "ok head_slot={} matched={}",
                r.head_slot, r.matched_expected
            )
        }
        Err(e) => format!("err: {e}"),
    };
    eprintln!(
        "S0-A-31: arm `{name}` finished in {:.1}s → {apply}",
        t0.elapsed().as_secs_f64()
    );

    restore_force_raw_decode(false);
    ArmTrace {
        name,
        apply,
        points: restore_trace(),
    }
}

fn is_decode(p: &RestoreTracePoint) -> bool {
    matches!(p, RestoreTracePoint::Decode { .. })
}

fn is_end_stream(p: &RestoreTracePoint) -> bool {
    matches!(p, RestoreTracePoint::EndStream)
}

fn is_da_drop(p: &RestoreTracePoint) -> bool {
    matches!(p, RestoreTracePoint::DaDeferredDrop { .. })
}

fn is_on_block(p: &RestoreTracePoint) -> bool {
    matches!(p, RestoreTracePoint::OnBlock { .. })
}

/// Reviewer grep target: do not hand-fill the cache this observation measures.
#[test]
fn test_body_contains_no_cache_population_call() {
    let src = include_str!("r13_restore_observation.rs");
    for needle in [
        concat!("rebuild_pubkey", "_cache"),
        concat!("top_up_pubkey", "_cache"),
        concat!("pubkeys", ".insert"),
        concat!("caches", "_mut"),
    ] {
        assert!(
            !src.contains(needle),
            "S0-A-31 test body must not contain `{needle}`"
        );
    }
    assert!(
        src.contains("handle_restore_accumulated"),
        "observation must drive the restore handler, not standalone process_block"
    );
}

/// Instrumented Hoodi restore: where does it fail?
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset (CC-10b).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hoodi_restore_observation_where_it_fails() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {CACHE_ENV} unset — Hoodi BeaconState SSZ is not in git; \
             {FETCH_HINT} (see crates/types/tests/fixtures/README.md)"
        );
        return;
    }

    let t_load = Instant::now();
    let snap = load_restore_snapshot();
    eprintln!(
        "S0-A-31: loaded Hoodi pin slot={} successor_slot={} state_ssz={} in {:.1}s",
        snap.snapshot_slot,
        snap.successor.message.slot.as_u64(),
        snap.state_ssz.len(),
        t_load.elapsed().as_secs_f64()
    );

    let hydrated = run_arm("hydrated", &snap, false).await;
    hydrated.dump();
    let raw = run_arm("raw", &snap, true).await;
    raw.dump();

    for arm in [&hydrated, &raw] {
        assert!(
            arm.reached(is_decode),
            "{} must reach restore decode",
            arm.name
        );
        assert!(
            arm.reached(is_on_block),
            "{} must reach restore on_block",
            arm.name
        );
        assert!(
            arm.reached(is_da_drop),
            "{} must reach the DA-deferred drop (Deferred block, DA gate first)",
            arm.name
        );
        assert!(
            arm.reached(is_end_stream),
            "{} must reach end_stream — this is the R-13 (b) observation",
            arm.name
        );
    }

    let RestoreTracePoint::Decode {
        hydrated: hyd_flag,
        pubkey_cache_len: hyd_cache,
        ..
    } = hydrated
        .points
        .iter()
        .find(|p| is_decode(p))
        .cloned()
        .expect("hydrated decode")
    else {
        panic!("decode variant");
    };
    assert!(hyd_flag, "production restore decode is hydrated");
    assert_eq!(
        hyd_cache, 0,
        "S2-A-10: decode does not own PubkeyIndexMap (TransitionContext)"
    );

    let RestoreTracePoint::Decode {
        hydrated: raw_flag,
        pubkey_cache_len: raw_cache,
        ..
    } = raw
        .points
        .iter()
        .find(|p| is_decode(p))
        .cloned()
        .expect("raw decode")
    else {
        panic!("decode variant");
    };
    assert!(!raw_flag, "raw arm must omit hydration");
    assert_eq!(
        raw_cache, 0,
        "S2-A-10: decode does not own PubkeyIndexMap (TransitionContext)"
    );

    eprintln!(
        "S0-A-31 conclusion: restore reaches end_stream (hydrated and raw). \
         Decode-time pubkey_cache_len is 0 on both arms (map is on TransitionContext). \
         P1-A/22 and P1-A/23 sites are reached — independent bugs."
    );
}
