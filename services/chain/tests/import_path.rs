//! CC-18b integration tests: core thread, import path, GetHead snapshot, backpressure.
//!
//! Entry-criterion note (R-11): `cargo nextest run -p cc-fork-choice --test fork_choice`
//! is green on develop tip 5f89686 before this suite runs against it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use cc_chain::core::{COMMAND_CHANNEL_CAPACITY, CoreCommand, CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::{encode_signed_block, publish_snapshot_then_events};
use cc_chain::metrics::{ChainMetrics, ImportResult};
use cc_chain::residency::{DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency};
use cc_chain::{BodyRingEntry, HeadSnapshot, ResidentRole, StateProvider};
use cc_fork_choice::{AlwaysAvailable, get_forkchoice_store};
use cc_proto::chain::{EventKind, ImportBlockRequest, ImportBlockVerdict};
use cc_state_transition::StubOptimisticEngine;
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::Minimal;
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex};
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use tokio::sync::oneshot;
use tree_hash::TreeHash;

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

fn seeded_store() -> (
    cc_fork_choice::Store<Minimal>,
    Root,
    ChainConfig,
    SignedBeaconBlock<Minimal>,
) {
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
    let signed_anchor = SignedBeaconBlock {
        message: anchor_block.clone(),
        signature: Default::default(),
    };
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(StubOptimisticEngine),
        Arc::new(AlwaysAvailable),
        config.seconds_per_slot,
    )
    .unwrap();
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    (store, anchor_root, config, signed_anchor)
}

fn spawn_test_core() -> (
    cc_chain::CoreThread,
    EventsHandle,
    Root,
    ChainMetrics,
    HeadSnapshotStore,
) {
    let (store, anchor, config, _) = seeded_store();
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 64,
        subscriber_queue_capacity: 32,
        session_id: Some(7),
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
    (core, events, anchor, metrics, head)
}

// ── CC-18/4 DUPLICATE without re-running transition ────────────────────────

#[tokio::test]
async fn duplicate_short_circuit_no_transition() {
    let (core, events, anchor, metrics, _head) = spawn_test_core();

    // Build a request whose probe root is already in store.blocks (the anchor).
    let ssz = encode_signed_block(&SignedBeaconBlock::<Minimal>::default());
    let req = ImportBlockRequest {
        ssz,
        fork: 0,
        root: anchor.as_slice().to_vec(),
        source: 0,
    };

    let before = core.handle.transition_count();
    let dup_before = metrics.import_result_count(ImportResult::Duplicate);

    let resp = core.handle.import_block(req).await.unwrap();
    assert_eq!(resp.verdict, ImportBlockVerdict::Duplicate as i32);
    assert_eq!(
        core.handle.transition_count(),
        before,
        "DUPLICATE must not invoke on_block / state transition"
    );
    assert_eq!(
        metrics.import_result_count(ImportResult::Duplicate),
        dup_before + 1
    );

    // Second time stays flat too.
    let ssz = encode_signed_block(&SignedBeaconBlock::<Minimal>::default());
    let req2 = ImportBlockRequest {
        ssz,
        fork: 0,
        root: anchor.as_slice().to_vec(),
        source: 0,
    };
    let before2 = core.handle.transition_count();
    let _ = core.handle.import_block(req2).await.unwrap();
    assert_eq!(core.handle.transition_count(), before2);

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── Root mismatch → INVALID_ARGUMENT, no import ────────────────────────────

#[tokio::test]
async fn root_mismatch_rejects_without_import() {
    let (core, events, anchor, metrics, head) = spawn_test_core();
    let head_before = head.load().sequence;

    let block = SignedBeaconBlock::<Minimal> {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: anchor,
            state_root: Root::ZERO,
            body: Default::default(),
        },
        signature: Default::default(),
    };
    let ssz = encode_signed_block(&block);
    // Lie about the root.
    let mut fake = [0u8; 32];
    fake[0] = 0xDE;
    fake[1] = 0xAD;
    let req = ImportBlockRequest {
        ssz,
        fork: 0,
        root: fake.to_vec(),
        source: 0,
    };

    let mismatch_before = metrics.import_root_mismatch_count();
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let err = core.handle.import_block(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(metrics.import_root_mismatch_count(), mismatch_before + 1);
    // Snapshot sequence unchanged — no import occurred.
    assert_eq!(head.load().sequence, head_before);
    // True root must not be treated as imported: re-import with correct probe
    // must not short-circuit as DUPLICATE (block was never integrated).
    let req_true = ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };
    let resp = core.handle.import_block(req_true).await.unwrap();
    assert_ne!(
        resp.verdict,
        ImportBlockVerdict::Duplicate as i32,
        "true root must not have been imported on mismatch path"
    );

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── GetHead from ArcSwap while core is blocked ─────────────────────────────

#[tokio::test]
async fn get_head_snapshot_while_core_blocked() {
    let (core, events, anchor, _metrics, head) = spawn_test_core();

    let h = core.handle.clone();
    let blocker = tokio::spawn(async move {
        h.block_for(Duration::from_secs(2)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = std::time::Instant::now();
    let snap = head.load();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "GetHead took {elapsed:?}"
    );
    assert_eq!(snap.head_root, anchor);

    blocker.await.unwrap();
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── Ordering: snapshot before head event ───────────────────────────────────

#[tokio::test]
async fn snapshot_published_before_head_event() {
    // Production path uses the same helper (`publish_snapshot_then_events` /
    // snapshot then try_send). Full ST happy-path import is CC-18d; this asserts
    // the publish helper's ordering contract used by `import_block`.
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 16,
        subscriber_queue_capacity: 8,
        session_id: Some(99),
    });
    let mut sub = events.subscribe(None).await.unwrap();

    let root = Root::from_array([0xAB; 32]);
    let snap = HeadSnapshot {
        head_root: root,
        head_slot: Slot::new(10),
        sequence: 1,
        ..HeadSnapshot::default()
    };
    publish_snapshot_then_events(&head, &events.event_sender(), &metrics, snap, 10, root);

    // Snapshot is observably updated before any event is consumed.
    assert_eq!(head.load().sequence, 1);
    assert_eq!(head.load().head_root, root);

    let mut saw_head = false;
    while let Ok(Some(ev)) = sub.recv().await {
        if ev.kind == EventKind::Head as i32 {
            saw_head = true;
            assert_eq!(head.load().sequence, 1, "snapshot must precede head event");
            break;
        }
    }
    assert!(saw_head, "expected Head event");

    events.shutdown().await;
}

// ── Backpressure: fill channel → RESOURCE_EXHAUSTED ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backpressure_returns_resource_exhausted() {
    let (core, events, anchor, metrics, _head) = spawn_test_core();

    // Block the core so the command channel fills.
    let h = core.handle.clone();
    let blocker = tokio::spawn(async move {
        h.block_for(Duration::from_secs(5)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fill the channel with Query commands that have no receivers waiting —
    // actually Query will queue behind BlockFor. Fill with ImportBlock that
    // holds reply channels we don't drop until after.
    let tx = core.handle.command_sender();
    let mut held_replies = Vec::new();
    for _ in 0..COMMAND_CHANNEL_CAPACITY {
        let (reply, rx) = oneshot::channel();
        held_replies.push(rx);
        let req = ImportBlockRequest {
            ssz: vec![],
            fork: 0,
            root: anchor.as_slice().to_vec(),
            source: 0,
        };
        // try_send so we don't wait — channel must fill.
        match tx.try_send(CoreCommand::ImportBlock {
            request: req,
            reply,
        }) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
            Err(e) => panic!("unexpected send error: {e}"),
        }
    }

    let before = metrics.import_rejected_backpressure_count();
    let req = ImportBlockRequest {
        ssz: encode_signed_block(&SignedBeaconBlock::<Minimal>::default()),
        fork: 0,
        root: anchor.as_slice().to_vec(),
        source: 0,
    };
    // send_timeout(2s) should fire.
    let err = core.handle.import_block(req).await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "expected RESOURCE_EXHAUSTED, got {err}"
    );
    assert!(
        metrics.import_rejected_backpressure_count() > before,
        "backpressure counter must increment"
    );

    // Drop held replies / unblock.
    drop(held_replies);
    // Unblock by letting BlockFor finish — but channel is full of imports that
    // will run after. Shutdown instead.
    // Note: BlockFor is first in queue; after 5s it completes. Force shutdown
    // by dropping the handle's ability... CoreHandle::shutdown also needs a
    // free slot. Use try_send path: wait for BlockFor then drain.
    let _ = blocker.await;
    // Drain queued imports.
    for _ in 0..COMMAND_CHANNEL_CAPACITY + 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── UNKNOWN_PARENT verdict ─────────────────────────────────────────────────

#[tokio::test]
async fn unknown_parent_verdict() {
    let (core, events, _anchor, metrics, _head) = spawn_test_core();

    let unknown_parent = Root::from_array([0xEE; 32]);
    let block = SignedBeaconBlock::<Minimal> {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: unknown_parent,
            state_root: Root::ZERO,
            body: Default::default(),
        },
        signature: Default::default(),
    };
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    // Advance store time so future-slot does not fire first — core's store is
    // at genesis time 0 / slot 0, so slot 1 is future. Use slot 0 with bad parent
    // instead? Slot must be > parent slot. Seed time via importing after we
    // cannot advance time from outside.
    //
    // Unknown parent is checked after DA and before future-slot in on_block.
    // Order: DA → parent → future slot. So unknown parent is returned even for
    // future slots? Looking at on_block: parent check is before future-slot.
    // Yes — parent is checked first.

    let req = ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };
    let resp = core.handle.import_block(req).await.unwrap();
    assert_eq!(
        resp.verdict,
        ImportBlockVerdict::UnknownParent as i32,
        "reason={}",
        resp.reason
    );
    assert!(metrics.import_result_count(ImportResult::UnknownParent) >= 1);

    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── Residency: max 4 states, body ring ≤ 64 ────────────────────────────────

#[test]
fn residency_metrics_bounds() {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let mut r = Residency::<Minimal>::new(DEFAULT_MAX_RESIDENT_STATES, DEFAULT_BODY_RING_CAPACITY);
    let state = BeaconState::<Minimal>::default();

    // Simulate 200 imports pinning rotating roles.
    let mut parent = Root::ZERO;
    for i in 1..=200u64 {
        let root = Root::from_array({
            let mut a = [0u8; 32];
            a[..8].copy_from_slice(&i.to_le_bytes());
            a
        });
        r.pin(ResidentRole::Head, root, Arc::new(state.clone()));
        if i % 8 == 0 {
            r.pin(ResidentRole::EpochBoundary, root, Arc::new(state.clone()));
        }
        r.pin(
            ResidentRole::Anchor,
            Root::from_array([1u8; 32]),
            Arc::new(state.clone()),
        );
        r.push_body(BodyRingEntry {
            root,
            parent_root: parent,
            slot: Slot::new(i),
            block: Arc::new(SignedBeaconBlock::default()),
        });
        parent = root;

        metrics.set_resident_states(r.resident_count() as u64);
        metrics.set_body_ring_len(r.body_ring_len() as u64);

        // AC: assert on metrics gauges, not only struct inspection.
        assert!(
            metrics.resident_states_value() <= DEFAULT_MAX_RESIDENT_STATES as i64,
            "gauge resident_states={} at import {i}",
            metrics.resident_states_value()
        );
        assert!(
            metrics.body_ring_len_value() <= DEFAULT_BODY_RING_CAPACITY as i64,
            "gauge body_ring={} at import {i}",
            metrics.body_ring_len_value()
        );
    }
    assert_eq!(
        metrics.body_ring_len_value(),
        DEFAULT_BODY_RING_CAPACITY as i64
    );
    assert!(metrics.resident_states_value() <= 4);
}

#[test]
fn deep_reorg_gap_no_panic() {
    let r = Residency::<Minimal>::new(4, 4);
    // Request a root with no body and no pin → gap.
    let missing = Root::from_array([0xFF; 32]);
    let err = r.get_state(missing);
    assert!(err.is_err());
}

/// H1 residual: end-to-end shallow reorg (≤64) with real ST + FC head is
/// deferred to **CC-18d** offline replay over the Hoodi sequence. Unit coverage
/// for pin-after-`get_head` and `ensure_in_store` gap lives in `residency` tests.
#[test]
fn h1_shallow_reorg_e2e_deferred_to_cc18d() {
    // Documentation pin — do not remove without adding the e2e test.
}

// ── KZG default wiring (compile-time) ──────────────────────────────────────

#[test]
fn kzg_default_is_cc11d_choice() {
    assert_eq!(
        cc_crypto::KzgBackendKind::default(),
        cc_crypto::KzgBackendKind::CKzg
    );
}
