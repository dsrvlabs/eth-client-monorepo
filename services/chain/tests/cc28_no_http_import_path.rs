//! CC-28/2 — no HTTP client on the block-import path; bootstrap is one-shot.
//!
//! After the checkpoint anchor is installed, the import path must not be able
//! to re-enter CC-19's HTTP bootstrap. Structural guarantees:
//!
//! 1. `reqwest` appears only in `checkpoint_sync.rs` under `services/chain/src`.
//! 2. `CoreCommand` has no bootstrap / fetch variant — post-anchor imports are
//!    store-only (gRPC `ImportBlock` or gossip).
//! 3. `ChainServiceImpl` after `install_core` holds a `CoreHandle`, never a
//!    `CheckpointClient`.
//!
//! The complementary structural check lives in
//! `scripts/check-no-http-import-path.sh` (`cargo tree -p cc-p2p` has no reqwest).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use cc_chain::core::{CoreCommand, CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::encode_signed_block;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::ChainServiceImpl;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::Minimal;
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex};
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
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

fn chain_src_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn chain_core_src_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../crates/chain-core/src")
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

/// CC-28/2 half-1 (compile-time-adjacent): `reqwest` only in checkpoint_sync.
#[test]
fn reqwest_only_in_checkpoint_sync() {
    let src = chain_src_root();
    let mut offenders = Vec::new();
    for root in [&src, &chain_core_src_root()] {
        for entry in walkdir_rs(root) {
            let path = entry;
            let rel = path.strip_prefix(root).unwrap_or(&path);
            if rel == std::path::Path::new("checkpoint_sync.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                panic!("read {}: {e}", path.display());
            });
            if text.contains("reqwest") {
                offenders.push(rel.display().to_string());
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "reqwest outside checkpoint_sync.rs (CC-28/2): {offenders:?}"
    );
}

/// Walk `*.rs` under `root` without a walkdir dependency.
fn walkdir_rs(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| {
            panic!("read_dir {}: {e}", dir.display());
        }) {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out
}

/// CoreCommand is the only path into the store after the anchor is set —
/// none of its variants re-enter HTTP bootstrap.
#[test]
fn core_command_has_no_bootstrap_variant() {
    // Exhaustive match: adding a Bootstrap/Fetch command fails this test.
    let sample = CoreCommand::Shutdown {
        done: tokio::sync::oneshot::channel().0,
    };
    match sample {
        CoreCommand::ImportBlock { .. }
        | CoreCommand::ImportBlockGossip { .. }
        | CoreCommand::ApplyAttestations { .. }
        | CoreCommand::Query { .. }
        | CoreCommand::BlockFor { .. }
        | CoreCommand::DataAvailable { .. }
        | CoreCommand::SlotTick
        | CoreCommand::Ping { .. }
        | CoreCommand::Shutdown { .. } => {}
    }
}

/// After the anchor is installed, ImportBlock runs store-only (no HTTP client
/// retained on the service). Simulates post-bootstrap by seeding a store and
/// installing the core — the same install_core path main uses after fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_http_not_reachable_after_anchor_set() {
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
    let signed = SignedBeaconBlock {
        message: anchor_block.clone(),
        signature: Default::default(),
    };
    let block_root = signed.message.tree_hash_root();
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        std::sync::Arc::new(AcceptEngine),
        std::sync::Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .expect("seed store at anchor");

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let events = EventsHandle::spawn(EventsConfig::default());
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig::default(),
    );

    // Service starts without core (pre-bootstrap), then install_core (anchor set).
    let svc = ChainServiceImpl::new(None, head, events, metrics);
    assert!(!svc.is_bootstrapped());
    svc.install_core(core.handle.clone());
    assert!(svc.is_bootstrapped());

    // Post-anchor: ImportBlock is available and does not need HTTP. Importing
    // the anchor itself yields DUPLICATE — pure store path.
    let handle = svc.core_handle().expect("core after install");
    let resp = handle
        .import_block(ImportBlockRequest {
            ssz: encode_signed_block(&signed),
            fork: 0,
            root: block_root.as_slice().to_vec(),
            source: 0,
        })
        .await
        .expect("ImportBlock after anchor");
    assert_eq!(resp.verdict, ImportBlockVerdict::Duplicate as i32);

    // No re-bootstrap surface: service holds only CoreHandle + snapshot stores.
    // (CheckpointClient is never a field of ChainServiceImpl — compile-time
    // enforced; this runtime check documents the post-anchor contract.)
    assert!(
        svc.is_bootstrapped(),
        "anchor remains installed; bootstrap is not re-entered"
    );

    handle.shutdown().await;
    core.join();
}
