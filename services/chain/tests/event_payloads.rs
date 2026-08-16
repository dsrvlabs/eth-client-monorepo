//! CC-44a integration tests: §4.2 payload shapes, DATA_COLUMN relay, byte ceiling.
//!
//! - Payload shape/length for BLOCK_IMPORTED / HEAD / CHAIN_REORG /
//!   FINALIZED_CHECKPOINT / DATA_COLUMN
//! - BLOCK_IMPORTED SSZ body is byte-identical to the imported block
//! - DEFERRED_DA vs IMPORTED share EventKind with different first payload bytes
//! - Byte ceiling binds before the count cap (~267 KB/slot and ~410 KB/slot rates)
//! - Config keys `event_ring_events` / `event_ring_bytes` load through cc-config

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use cc_chain::events::{
    EventInput, EventRing, EventsConfig, EventsHandle, MAX_EVENT_PAYLOAD_BYTES,
};
use cc_chain::{
    BLOCK_PAYLOAD_VERDICT_DEFERRED_DA, BLOCK_PAYLOAD_VERDICT_IMPORTED, BlockImportedPayloadVerdict,
    DEFAULT_RING_BYTES, DEFAULT_RING_CAPACITY, FORK_CHOICE_SCALARS_SSZ_LEN,
    ForkChoiceScalarsPayload, block_imported_payload,
};
use cc_proto::chain::EventKind;
use cc_proto::p2p::{ColumnSidecar, P2pToChain, p2p_to_chain};
use cc_types::containers::Checkpoint;
use cc_types::primitives::{Epoch, Root, Slot};
use serde::Deserialize;
use ssz::Encode;

// ── §4.2 payload shape helpers ──────────────────────────────────────────────

#[test]
fn block_imported_payload_shape_and_verdict_discriminator() {
    let body = b"signed-beacon-block-ssz-bytes";
    let imported = block_imported_payload(BlockImportedPayloadVerdict::Imported, body);
    let deferred = block_imported_payload(BlockImportedPayloadVerdict::DeferredDa, body);

    assert_eq!(imported[0], BLOCK_PAYLOAD_VERDICT_IMPORTED);
    assert_eq!(deferred[0], BLOCK_PAYLOAD_VERDICT_DEFERRED_DA);
    assert_ne!(imported[0], deferred[0]);
    // SSZ portion is byte-identical to what arrived.
    assert_eq!(&imported[1..], body.as_slice());
    assert_eq!(&deferred[1..], body.as_slice());
    // Same EventKind for both (constructed via EventInput helpers).
    let a = EventInput::block_imported_with_payload(1, Bytes::from(vec![1u8; 32]), imported);
    let b = EventInput::block_imported_with_payload(1, Bytes::from(vec![1u8; 32]), deferred);
    assert_eq!(a.kind, EventKind::BlockImported);
    assert_eq!(b.kind, EventKind::BlockImported);
    assert_eq!(a.kind, b.kind);
}

#[test]
fn head_payload_is_8_byte_slot_le() {
    let slot = 0x1122_3344_5566_7788u64;
    let input = EventInput::head(slot, Bytes::from(vec![0xabu8; 32]));
    assert_eq!(input.kind, EventKind::Head);
    assert_eq!(input.payload.len(), 8);
    assert_eq!(input.payload.as_ref(), &slot.to_le_bytes());
    assert_eq!(input.root.len(), 32);
}

#[test]
fn chain_reorg_payload_is_32_plus_8() {
    let old = [0x11u8; 32];
    let new = [0x22u8; 32];
    let ancestor = 42u64;
    let input = EventInput::chain_reorg(
        100,
        Bytes::copy_from_slice(&new),
        Bytes::copy_from_slice(&old),
        ancestor,
    );
    assert_eq!(input.kind, EventKind::ChainReorg);
    assert_eq!(input.payload.len(), 40);
    assert_eq!(&input.payload[..32], &old);
    assert_eq!(&input.payload[32..], &ancestor.to_le_bytes());
    assert_eq!(input.root.as_ref(), &new);
}

#[test]
fn fork_choice_scalars_fixed_len_is_240() {
    // F3: intentional mapping to `cc_store::meta::ForkChoiceScalars` — both
    // are 240-byte fixed SSZ containers with the same field order.
    assert_eq!(FORK_CHOICE_SCALARS_SSZ_LEN, 240);
    assert_eq!(
        ForkChoiceScalarsPayload::default().as_ssz_bytes().len(),
        FORK_CHOICE_SCALARS_SSZ_LEN
    );
}

#[test]
fn oversize_payload_not_accepted_by_cap_helper() {
    // SEC-44a-2
    let ok = EventInput::data_column(
        0,
        Bytes::from(vec![0u8; 32]),
        Bytes::from(vec![0u8; MAX_EVENT_PAYLOAD_BYTES]),
    );
    assert!(ok.payload_within_cap());
    let bad = EventInput::data_column(
        0,
        Bytes::from(vec![0u8; 32]),
        Bytes::from(vec![0u8; MAX_EVENT_PAYLOAD_BYTES + 1]),
    );
    assert!(!bad.payload_within_cap());
}

#[test]
fn finalized_checkpoint_payload_layout() {
    let epoch = 7u64;
    let state_root = [0x33u8; 32];
    let fin_root = [0x44u8; 32];
    let scalars = ForkChoiceScalarsPayload {
        time: 12,
        proposer_boost_root: Root::from_array([1u8; 32]),
        justified: Checkpoint {
            epoch: Epoch::new(6),
            root: Root::from_array([2u8; 32]),
        },
        finalized: Checkpoint {
            epoch: Epoch::new(epoch),
            root: Root::from_array(fin_root),
        },
        unrealized_justified: Checkpoint::default(),
        unrealized_finalized: Checkpoint::default(),
        head_root: Root::from_array([3u8; 32]),
        head_slot: Slot::new(99),
    };
    let scalars_ssz = scalars.as_ssz_bytes();
    let input = EventInput::finalized_checkpoint(
        epoch,
        Bytes::copy_from_slice(&fin_root),
        Bytes::copy_from_slice(&state_root),
        Bytes::from(scalars_ssz.clone()),
    );
    assert_eq!(input.kind, EventKind::FinalizedCheckpoint);
    assert_eq!(&input.payload[..8], &epoch.to_le_bytes());
    assert_eq!(&input.payload[8..40], &state_root);
    assert_eq!(&input.payload[40..], scalars_ssz.as_slice());
    assert_eq!(input.root.as_ref(), &fin_root);
}

#[test]
fn data_column_payload_is_verbatim_ssz() {
    let ssz = b"data-column-sidecar-ssz-opaque";
    let root = [0x55u8; 32];
    let input = EventInput::data_column(9, Bytes::copy_from_slice(&root), Bytes::from_static(ssz));
    assert_eq!(input.kind, EventKind::DataColumn);
    assert_eq!(input.payload.as_ref(), ssz.as_slice());
    assert_eq!(input.root.as_ref(), &root);
}

// ── Live bus: subscribe and assert shapes ───────────────────────────────────

#[tokio::test]
async fn live_subscribe_receives_populated_payloads() {
    let h = EventsHandle::spawn(EventsConfig {
        ring_capacity: 32,
        ring_bytes: usize::MAX,
        subscriber_queue_capacity: 16,
        session_id: Some(44),
    });
    let mut sub = h.subscribe(None).await.unwrap();

    let block_body = b"block-ssz-body-identical";
    let root = Bytes::from(vec![0x10u8; 32]);
    h.publish(EventInput::block_imported_with_payload(
        10,
        root.clone(),
        block_imported_payload(BlockImportedPayloadVerdict::Imported, block_body),
    ))
    .await
    .unwrap();
    h.publish(EventInput::head(10, root.clone())).await.unwrap();
    h.publish(EventInput::chain_reorg(
        10,
        root.clone(),
        Bytes::from(vec![0x20u8; 32]),
        5,
    ))
    .await
    .unwrap();
    h.publish(EventInput::finalized_checkpoint(
        1,
        root.clone(),
        Bytes::from(vec![0x30u8; 32]),
        Bytes::from(vec![0u8; 16]),
    ))
    .await
    .unwrap();
    h.publish(EventInput::data_column(
        10,
        root.clone(),
        Bytes::from_static(b"sidecar"),
    ))
    .await
    .unwrap();

    let kinds = [
        EventKind::BlockImported,
        EventKind::Head,
        EventKind::ChainReorg,
        EventKind::FinalizedCheckpoint,
        EventKind::DataColumn,
    ];
    for expected in kinds {
        let ev = sub.recv().await.unwrap().unwrap();
        assert_eq!(ev.kind, expected as i32, "kind {:?}", expected);
        assert!(!ev.payload.is_empty(), "payload empty for {:?}", expected);
        match expected {
            EventKind::BlockImported => {
                assert_eq!(ev.payload[0], BLOCK_PAYLOAD_VERDICT_IMPORTED);
                assert_eq!(&ev.payload[1..], block_body.as_slice());
            }
            EventKind::Head => assert_eq!(ev.payload.len(), 8),
            EventKind::ChainReorg => assert_eq!(ev.payload.len(), 40),
            EventKind::FinalizedCheckpoint => assert!(ev.payload.len() >= 40),
            EventKind::DataColumn => assert_eq!(ev.payload.as_slice(), b"sidecar"),
            _ => {}
        }
    }
    h.shutdown().await;
}

// ── Byte ceiling binds first ────────────────────────────────────────────────

/// Mean slot cost ≈ 267 KB (Architecture §4.3 arithmetic).
///
/// `floor(64 MiB / 267_000) = 251` slots. Architecture §4.3's table cites
/// **269** for a slightly lower measured mean (~249.5 KB/slot); both are far
/// below the 4 096 count cap, so the ceiling binds first either way.
const MEAN_SLOT_BYTES: usize = 267_000;
/// Headroom slot cost ≈ 410 KB → `floor(64 MiB / 410_000) = 163` slots.
const HEADROOM_SLOT_BYTES: usize = 410_000;
const CEILING: usize = 64 * 1024 * 1024;

#[test]
fn byte_ceiling_binds_first_at_mean_and_headroom_rates() {
    // Recorded for the commit description (CC-44a AC):
    //   mean ≈ 267 KB/slot → binds at floor(64 MiB / 267 KB) = **251** slots
    //   headroom ≈ 410 KB/slot → binds at floor(64 MiB / 410 KB) = **163** slots
    //   §4.3 table's 269 is the measured-mean equivalent (~249.5 KB/slot)
    //   count cap 4096 would allow ~409 slots of 10 events — **ceiling wins**.
    let mean_bind = CEILING / MEAN_SLOT_BYTES;
    let headroom_bind = CEILING / HEADROOM_SLOT_BYTES;
    assert_eq!(mean_bind, 251, "mean-rate bind slot count");
    assert_eq!(headroom_bind, 163, "headroom-rate bind slot count");
    // Architecture table 269 / 163 and the computed binds are all << 4096.
    assert!(mean_bind < DEFAULT_RING_CAPACITY);
    assert!(headroom_bind < DEFAULT_RING_CAPACITY);

    for (label, per_slot) in [("mean", MEAN_SLOT_BYTES), ("headroom", HEADROOM_SLOT_BYTES)] {
        // Simulate one large event per slot at the measured rate.
        let mut ring = EventRing::new(DEFAULT_RING_CAPACITY, CEILING, 1);
        let mut slots = 0u64;
        // Fill until the next push would require eviction by bytes.
        loop {
            let before = ring.bytes();
            if before.saturating_add(per_slot) > CEILING && !ring.is_empty() {
                // One more push forces byte eviction while count is still low.
                ring.push(EventInput {
                    slot: slots,
                    root: Bytes::from(vec![slots as u8; 32]),
                    kind: EventKind::BlockImported,
                    payload: Bytes::from(vec![0u8; per_slot.saturating_sub(32 + 20)]),
                });
                assert!(
                    ring.bytes() <= CEILING,
                    "{label}: over ceiling after eviction"
                );
                assert!(
                    ring.len() < DEFAULT_RING_CAPACITY,
                    "{label}: count still below 4096 when byte ceiling bound (len={})",
                    ring.len()
                );
                break;
            }
            ring.push(EventInput {
                slot: slots,
                root: Bytes::from(vec![slots as u8; 32]),
                kind: EventKind::BlockImported,
                payload: Bytes::from(vec![0u8; per_slot.saturating_sub(32 + 20)]),
            });
            slots += 1;
            assert!(
                slots < 5000,
                "{label}: failed to hit byte ceiling before absurd slot count"
            );
        }
        // Entry count at bind is well under 4096.
        assert!(
            ring.len() < DEFAULT_RING_CAPACITY,
            "{label}: ring.len()={} should be < 4096",
            ring.len()
        );
        // Slot count near the arithmetic (within a few for overhead).
        let expected = CEILING / per_slot;
        assert!(
            slots >= expected.saturating_sub(2) as u64 && slots <= expected as u64 + 2,
            "{label}: bound around {expected} slots, observed fill of {slots}"
        );
    }
}

// ── Config keys via cc-config ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ChainRingConfig {
    #[serde(default = "default_events")]
    event_ring_events: usize,
    #[serde(default = "default_bytes")]
    event_ring_bytes: usize,
}

fn default_events() -> usize {
    DEFAULT_RING_CAPACITY
}
fn default_bytes() -> usize {
    DEFAULT_RING_BYTES
}

#[test]
fn chain_toml_event_ring_keys_load() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/chain.toml");
    // Load only the chain-specific keys via figment-compatible toml parse.
    let text = std::fs::read_to_string(&path).expect("config/chain.toml");
    let cfg: ChainRingConfig = toml::from_str(&text).expect("parse chain.toml ring keys");
    assert_eq!(cfg.event_ring_events, 4096);
    assert_eq!(cfg.event_ring_bytes, 67_108_864);
}

// ── DATA_COLUMN producer (p2p stream relay, no decode) ──────────────────────

#[tokio::test]
async fn column_relay_publishes_data_column_without_decode() {
    use cc_chain::epoch_context::EpochContextStore;
    use cc_chain::head::HeadSnapshotStore;
    use cc_chain::p2p_stream::{P2pStreamDeps, serve_p2p_stream};
    use futures::StreamExt;
    use std::sync::{Arc, RwLock};
    use tokio_stream::wrappers::ReceiverStream;

    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 16,
        ring_bytes: usize::MAX,
        subscriber_queue_capacity: 8,
        session_id: Some(0x44a),
    });
    let mut sub = events.subscribe(None).await.unwrap();

    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::new();
    let core = Arc::new(RwLock::new(None));
    let deps = P2pStreamDeps::with_events(head, epoch, core, Some(events.event_sender()));

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(4);
    let inbound = ReceiverStream::new(in_rx).map(Ok);
    let mut outbound = serve_p2p_stream(deps.clone(), inbound).await.unwrap();

    // Open session so the inbound handler is live.
    in_tx
        .send(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(cc_proto::p2p::StreamHello {
                session_id: 1,
                resume_seq: 0,
            })),
        })
        .await
        .unwrap();
    // Drain the full ChainView.
    let _ = tokio::time::timeout(Duration::from_millis(200), outbound.next())
        .await
        .expect("view timeout")
        .expect("view end");

    let sidecar = b"opaque-sidecar-ssz-not-decoded";
    let block_root = vec![0x77u8; 32];
    in_tx
        .send(P2pToChain {
            seq: 2,
            msg: Some(p2p_to_chain::Msg::Column(ColumnSidecar {
                ssz: sidecar.to_vec(),
                fork: 0,
                root: block_root.clone(),
                column_index: 3,
                subnet_id: 3,
            })),
        })
        .await
        .unwrap();

    // Verdict ACK.
    let _ = tokio::time::timeout(Duration::from_millis(200), outbound.next())
        .await
        .expect("verdict timeout");

    // DATA_COLUMN appears on the bus with verbatim payload and block root.
    let ev = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event timeout")
        .unwrap()
        .expect("event");
    assert_eq!(ev.kind, EventKind::DataColumn as i32);
    assert_eq!(ev.payload.as_slice(), sidecar.as_slice());
    assert_eq!(ev.root, block_root);
    assert_eq!(
        deps.column_decode_attempts(),
        0,
        "chain must not decode the sidecar"
    );

    events.shutdown().await;
}
