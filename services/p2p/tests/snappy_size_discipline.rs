//! CC-22b — SnappyTransform bound + four-layer size discipline acceptance.
//!
//! `SnappyTransform` lives in the frozen `cc_libp2p` crate (CC-20a). This
//! integration test asserts the production wiring constants and the expansion
//! bomb property without reopening that crate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{self, Read};

use cc_libp2p::reexport::gossipsub::{DataTransform, TopicHash};
use cc_libp2p::{BehaviourConfig, GOSSIP_MAX_SIZE, SnappyTransform};
use cc_p2p::gossip::{
    DecodeCounter, TopicName, check_payload_len, check_payload_len_counted, max_container_bytes,
    message_id_valid_snappy, ssz_max,
};
use cc_types::{Mainnet, Minimal};

fn raw_message(data: Vec<u8>) -> cc_libp2p::reexport::gossipsub::RawMessage {
    cc_libp2p::reexport::gossipsub::RawMessage {
        source: None,
        data,
        sequence_number: Some(1),
        topic: TopicHash::from_raw("test"),
        signature: None,
        key: None,
        validated: false,
    }
}

#[test]
fn behaviour_defaults_read_gossip_max_from_config() {
    // max_transmit_size + max_uncompressed both come from GOSSIP_MAX_SIZE —
    // no inlined 10 MiB in the gossip service layer.
    let cfg = BehaviourConfig::default();
    assert_eq!(cfg.max_transmit_size, GOSSIP_MAX_SIZE);
    assert_eq!(cfg.max_uncompressed, GOSSIP_MAX_SIZE);
    assert_eq!(GOSSIP_MAX_SIZE, 10 * 1024 * 1024);
}

#[test]
fn sec_c1_message_id_fn_wired_and_content_addressed() {
    // SEC C1: ethereum_behaviour_config installs the p2p-owned message_id_fn;
    // ids are content-addressed (topic + decompressed payload), never seqno.
    use cc_libp2p::reexport::gossipsub::{Message, TopicHash};
    use cc_p2p::gossip::{ethereum_behaviour_config, message_id_valid_snappy};

    let cfg = ethereum_behaviour_config();
    let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
    let plain = b"ssz-body";
    let msg = Message {
        source: None,
        data: plain.to_vec(),
        sequence_number: Some(42),
        topic: TopicHash::from_raw(topic),
    };
    let id = (cfg.message_id_fn)(&msg);
    assert_eq!(id.0.as_slice(), message_id_valid_snappy(topic, plain));
    // Distinct payloads → distinct ids (no universal collision).
    let other = Message {
        data: b"other-body".to_vec(),
        ..msg.clone()
    };
    assert_ne!((cfg.message_id_fn)(&msg).0, (cfg.message_id_fn)(&other).0);
}

#[test]
fn expansion_past_ceiling_is_rejected_and_bounded() {
    // Crafted expansion bomb relative to a tight ceiling: a payload whose
    // snappy stream expands past `max` is rejected with InvalidData, and
    // `Read::take(max+1)` materialises at most max+1 bytes.
    //
    // Architecture §5.2's 200 KB → 80 MB narrative is the same property at
    // production scale (GOSSIP_MAX_SIZE); the unit form keeps CI light.
    let max = 64 * 1024;
    let transform = SnappyTransform::new(max);

    // ~compressible zeros: compressed stays small, decompressed exceeds max.
    let bomb_plain = vec![0u8; max + 1];
    // Compress with a high ceiling so outbound does not reject.
    let compressor = SnappyTransform::new(GOSSIP_MAX_SIZE);
    let compressed = compressor
        .outbound_transform(&TopicHash::from_raw("t"), bomb_plain)
        .expect("compress bomb");
    // Compressed input is far smaller than the claimed expansion.
    assert!(
        compressed.len() < max,
        "bomb compressed size {} should be << max {}",
        compressed.len(),
        max
    );

    let err = transform
        .inbound_transform(raw_message(compressed.clone()))
        .expect_err("must reject expansion past max");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("max_uncompressed"),
        "error should name the bound: {err}"
    );

    // take(max+1) sentinel: a direct read yields exactly max+1 for this stream.
    {
        let mut decoder = snap::read::FrameDecoder::new(compressed.as_slice());
        let mut limited = (&mut decoder).take((max as u64) + 1);
        let mut buf = Vec::new();
        limited.read_to_end(&mut buf).expect("take read");
        assert_eq!(
            buf.len(),
            max + 1,
            "decode through take must not exceed the sentinel bound"
        );
    }
}

#[test]
fn large_ceiling_expansion_bomb_rejected() {
    // Production-shaped: expand past GOSSIP_MAX_SIZE. Plaintext is max+1 zeros
    // (snappy-frame compressed input is tiny — the "declaring ~80 MB" form at
    // the 10 MiB ceiling).
    let transform = SnappyTransform::new(GOSSIP_MAX_SIZE);
    let plain = vec![0u8; GOSSIP_MAX_SIZE + 1];
    let compressed = transform
        .outbound_transform(&TopicHash::from_raw("t"), plain)
        .expect_err("outbound must refuse plaintexts above max_uncompressed");
    assert_eq!(compressed.kind(), io::ErrorKind::InvalidInput);

    // Build the bomb with an unbounded encoder so inbound sees over-size data.
    let mut compressed = Vec::new();
    {
        use std::io::Write;
        let mut encoder = snap::write::FrameEncoder::new(&mut compressed);
        encoder
            .write_all(&vec![0u8; GOSSIP_MAX_SIZE + 1])
            .expect("write");
        encoder.flush().expect("flush");
        let _ = encoder.into_inner().expect("finish");
    }
    // Zero-filled stream compresses well below the uncompressed ceiling
    // (architecture's ~200 KB → ~80 MB narrative at production scale).
    assert!(
        compressed.len() < GOSSIP_MAX_SIZE / 10,
        "zero-filled expansion bomb compressed to {} bytes (expected << GOSSIP_MAX_SIZE)",
        compressed.len()
    );

    let err = transform
        .inbound_transform(raw_message(compressed))
        .expect_err("inbound must reject expansion past GOSSIP_MAX_SIZE");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn message_id_matches_decompressed_payload_after_transform() {
    let topic = "/eth2/c6ecb76c/beacon_block/ssz_snappy";
    let plain = b"decompressed-ssz-body".to_vec();
    let transform = SnappyTransform::new(GOSSIP_MAX_SIZE);
    let compressed = transform
        .outbound_transform(&TopicHash::from_raw(topic), plain.clone())
        .expect("compress");
    assert_ne!(compressed, plain, "snappy must change the bytes");

    let msg = transform
        .inbound_transform(raw_message(compressed.clone()))
        .expect("decompress");
    assert_eq!(msg.data, plain);

    // Id over transform output (== decompressed) matches the valid-snappy id.
    let id = message_id_valid_snappy(topic, &msg.data);
    assert_eq!(id, message_id_valid_snappy(topic, &plain));
    assert_ne!(
        id,
        message_id_valid_snappy(topic, &compressed),
        "id must not be computed over compressed wire bytes"
    );
}

#[test]
fn per_container_table_covers_ten_families_and_preset_switch() {
    let families = [
        TopicName::BeaconBlock,
        TopicName::BeaconAggregateAndProof,
        TopicName::BeaconAttestation(0),
        TopicName::DataColumnSidecar(0),
        TopicName::SyncCommitteeContributionAndProof,
        TopicName::SyncCommittee(0),
        TopicName::VoluntaryExit,
        TopicName::ProposerSlashing,
        TopicName::AttesterSlashing,
        TopicName::BlsToExecutionChange,
    ];
    assert_eq!(families.len(), 10);
    for name in families {
        let m = max_container_bytes::<Mainnet>(name);
        assert!(m > 0 && m <= GOSSIP_MAX_SIZE);
    }
    assert_ne!(
        ssz_max::<Mainnet>(TopicName::BeaconAttestation(1)),
        ssz_max::<Minimal>(TopicName::BeaconAttestation(1))
    );
}

#[test]
fn over_bound_payload_never_reaches_ssz_decode() {
    let counter = DecodeCounter::new();
    let name = TopicName::ProposerSlashing;
    let max = max_container_bytes::<Mainnet>(name);

    for _ in 0..5 {
        let err = check_payload_len_counted::<Mainnet>(name, max + 100, &counter).unwrap_err();
        assert!(matches!(err, cc_p2p::gossip::SizeError::OverBound { .. }));
    }
    assert_eq!(
        counter.attempts(),
        0,
        "decode counter must stay flat across over-bound rejections"
    );

    check_payload_len::<Mainnet>(name, max).expect("exact bound ok");
}

#[test]
fn gossip_service_sources_have_no_inlined_ten_mib() {
    // Acceptance: grep -rn "10 * 1024 * 1024\|10485760" services/p2p/src/gossip/
    // must show no inlined ceiling — the transform/config own GOSSIP_MAX_SIZE.
    let roots = [
        include_str!("../src/gossip/mod.rs"),
        include_str!("../src/gossip/topics.rs"),
        include_str!("../src/gossip/registry.rs"),
        include_str!("../src/gossip/validate/mod.rs"),
    ];
    for (i, src) in roots.iter().enumerate() {
        assert!(
            !src.contains("10 * 1024 * 1024") && !src.contains("10485760"),
            "gossip source #{i} must not inline the 10 MiB ceiling"
        );
    }
}
