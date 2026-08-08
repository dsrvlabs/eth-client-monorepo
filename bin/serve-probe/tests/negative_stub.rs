//! CC-4B /3 — stub peer returns empty success below the window → probe fails
//! naming the slot.
//!
//! Also covers Status handshake + positive cooperating path against a local
//! dual-swarm peer (no external network).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::collapsible_match
)]

use std::time::Duration;

use cc_libp2p::reexport::{
    Multiaddr, PeerId, ProtocolSupport, StreamProtocol, Swarm, SwarmEvent, request_response,
};
use cc_libp2p::{
    BehaviourConfig, CcBehaviour, CcBehaviourEvent, Keypair, ReqRespRequest, ReqRespResponse,
    SwarmConfig, build_swarm,
};
use cc_serve_probe::client::{
    ProbeClient, encode_empty_success_framed, encode_resource_unavailable_framed,
};
use cc_serve_probe::codec::{ResponseChunk, encode_success_chunk};
use cc_serve_probe::probe::{ProbeConfig, negative_verdict, run_probe};
use cc_serve_probe::protocols::{BlocksByRangeRequest, Protocol, STATUS_V2_SSZ_LEN, StatusV2};
use cc_types::{Epoch, ForkDigest, Root, Slot};
use futures::StreamExt;
use tokio::time::timeout;

const FORK: [u8; 4] = [0xab, 0xcd, 0xef, 0x01];
/// Stub advertises eas=1000, head=1100 so negative samples land below 1000.
const EAS: u64 = 1000;
const HEAD: u64 = 1100;

fn probe_protocols() -> Vec<(StreamProtocol, ProtocolSupport)> {
    Protocol::ALL
        .into_iter()
        .map(|p| (StreamProtocol::new(p.protocol_id()), ProtocolSupport::Full))
        .collect()
}

fn make_swarm() -> (PeerId, Swarm<CcBehaviour>) {
    let keypair = Keypair::generate_secp256k1();
    let peer_id = PeerId::from_public_key(&keypair.public());
    let cfg = BehaviourConfig::default()
        .with_reqresp_protocols(probe_protocols())
        .with_reqresp_request_timeout(Duration::from_secs(15));
    let behaviour = CcBehaviour::new(&keypair, cfg).expect("behaviour");
    let swarm = build_swarm(keypair, behaviour, &SwarmConfig::default()).expect("swarm");
    (peer_id, swarm)
}

fn stub_status() -> StatusV2 {
    StatusV2 {
        fork_digest: ForkDigest::from_array(FORK),
        finalized_root: Root::ZERO,
        finalized_epoch: Epoch::new(0),
        head_root: Root::from_array([0x22; 32]),
        head_slot: Slot::new(HEAD),
        earliest_available_slot: Slot::new(EAS),
    }
}

fn encode_status_framed(status: &StatusV2) -> Vec<u8> {
    let ssz = status.to_ssz_bytes();
    encode_success_chunk(&ssz, false, None, Protocol::StatusV2.response_limits())
        .expect("status frame")
}

/// Drive a stub peer that returns **empty success** for every block/column
/// request (the dishonest shape below the window).
async fn run_empty_success_stub(mut swarm: Swarm<CcBehaviour>) {
    let status = stub_status();
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(
                request_response::Event::Message { message, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    assert_eq!(request.ssz.len(), STATUS_V2_SSZ_LEN);
                    let framed = encode_status_framed(&status);
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
                request_response::Message::Response { .. } => {}
            },
            SwarmEvent::Behaviour(CcBehaviourEvent::Reqresp(
                request_response::Event::Message { message, .. },
            )) => {
                if let request_response::Message::Request { channel, .. } = message {
                    // Empty success stream — zero chunks, no result byte 3.
                    let framed = encode_empty_success_framed();
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
            }
            _ => {}
        }
    }
}

/// Drive a cooperating stub: Status + ResourceUnavailable below eas, block
/// success (tiny SSZ) above eas.
async fn run_cooperating_stub(mut swarm: Swarm<CcBehaviour>) {
    let status = stub_status();
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(
                request_response::Event::Message { message, .. },
            )) => match message {
                request_response::Message::Request { channel, .. } => {
                    let framed = encode_status_framed(&status);
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
                request_response::Message::Response { .. } => {}
            },
            SwarmEvent::Behaviour(CcBehaviourEvent::Reqresp(
                request_response::Event::Message { message, .. },
            )) => {
                if let request_response::Message::Request {
                    request, channel, ..
                } = message
                {
                    let framed = serve_data_request(&request);
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                }
            }
            _ => {}
        }
    }
}

fn serve_data_request(request: &ReqRespRequest) -> Vec<u8> {
    let proto = request.protocol.as_ref();
    if proto.contains("beacon_blocks_by_range") {
        let req = BlocksByRangeRequest::from_ssz_bytes(&request.ssz).expect("by_range");
        let slot = req.start_slot.as_u64();
        if slot < EAS {
            return encode_resource_unavailable_framed();
        }
        // Tiny non-empty "block" payload with context.
        return encode_success_chunk(
            b"block-ssz",
            true,
            Some(FORK),
            Protocol::BeaconBlocksByRangeV2.response_limits(),
        )
        .expect("success chunk");
    }
    if proto.contains("data_column_sidecars_by_range") {
        // Decode start_slot from the fixed prefix.
        if request.ssz.len() >= 8 {
            let slot = u64::from_le_bytes(request.ssz[0..8].try_into().unwrap());
            if slot < EAS {
                return encode_resource_unavailable_framed();
            }
            return encode_success_chunk(
                b"column-ssz",
                true,
                Some(FORK),
                Protocol::DataColumnSidecarsByRangeV1.response_limits(),
            )
            .expect("column chunk");
        }
    }
    encode_resource_unavailable_framed()
}

async fn listen(swarm: &mut Swarm<CcBehaviour>) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                break address;
            }
        }
    })
    .await
    .expect("listen")
}

#[tokio::test]
async fn stub_empty_success_below_window_fails_naming_slot() {
    let (_stub_id, mut stub) = make_swarm();
    let listen_addr = listen(&mut stub).await;
    let addr_with_peer = format!("{listen_addr}/p2p/{_stub_id}");

    let stub_task = tokio::spawn(async move {
        run_empty_success_stub(stub).await;
    });

    let cfg = ProbeConfig {
        peer: addr_with_peer,
        fork_digest: ForkDigest::from_array(FORK),
        slots: 4,
        full_window: false,
        below: 8,
        columns: vec![],
    };

    let outcome = timeout(Duration::from_secs(60), run_probe(&cfg))
        .await
        .expect("probe timeout")
        .expect("probe dial/status");

    assert_eq!(outcome.earliest_available_slot, EAS);
    assert_eq!(outcome.head_slot, HEAD);

    // Negative side must fail and name exact slots.
    assert!(
        !outcome.negative.pass,
        "negative side must fail on empty success"
    );
    let failing = outcome.negative.failing_slots();
    assert!(!failing.is_empty(), "must name at least one failing slot");
    for slot in &failing {
        assert!(*slot < EAS, "failing slot {slot} must be below eas={EAS}");
        let reason = outcome.negative.failures.get(slot).unwrap();
        assert!(
            reason.contains(&format!("slot {slot}")),
            "reason must name slot: {reason}"
        );
        assert!(
            reason.contains("empty success") || reason.contains("ResourceUnavailable"),
            "reason must describe empty success: {reason}"
        );
    }

    // JSON-shaped report fields for jq acceptance.
    let report = cc_serve_probe::ProbeReport::from_outcome(
        &cfg.peer,
        &outcome.agent_version,
        "0xabcdef01",
        &outcome,
    );
    assert!(!report.negative.pass);
    assert!(!report.negative.failing_slots.is_empty());
    let json = serde_json::to_value(&report).unwrap();
    let n = json["negative"]["failing_slots"].as_array().unwrap().len();
    assert!(
        n > 0,
        "jq '.negative.failing_slots | length' must be non-zero"
    );

    stub_task.abort();
}

#[tokio::test]
async fn cooperating_stub_passes_both_sides() {
    let (_stub_id, mut stub) = make_swarm();
    let listen_addr = listen(&mut stub).await;
    let addr_with_peer = format!("{listen_addr}/p2p/{_stub_id}");

    let stub_task = tokio::spawn(async move {
        run_cooperating_stub(stub).await;
    });

    let cfg = ProbeConfig {
        peer: addr_with_peer,
        fork_digest: ForkDigest::from_array(FORK),
        slots: 8,
        full_window: false,
        below: 8,
        columns: vec![0, 1],
    };

    let outcome = timeout(Duration::from_secs(60), run_probe(&cfg))
        .await
        .expect("probe timeout")
        .expect("probe dial/status");

    assert!(
        outcome.positive.pass,
        "positive failures: {:?}",
        outcome.positive.failures
    );
    assert!(
        outcome.negative.pass,
        "negative failures: {:?}",
        outcome.negative.failures
    );
    assert!(outcome.pass());

    stub_task.abort();
}

#[tokio::test]
async fn unreachable_multiaddr_is_named_transport_error() {
    // High port unlikely to accept connections on loopback.
    let cfg = ProbeConfig {
        peer: "/ip4/127.0.0.1/tcp/1".to_owned(),
        fork_digest: ForkDigest::from_array(FORK),
        slots: 1,
        full_window: false,
        below: 1,
        columns: vec![],
    };
    let err = timeout(Duration::from_secs(30), run_probe(&cfg))
        .await
        .expect("outer timeout")
        .expect_err("must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("transport") || msg.contains("unreachable") || msg.contains("dial"),
        "must name transport error: {msg}"
    );
}

#[test]
fn negative_verdict_unit_names_slot() {
    let reason = negative_verdict(&[], 12345, "blocks").expect("fail");
    assert!(reason.contains("12345"));
    assert!(reason.contains("empty success"));

    // ResourceUnavailable alone is pass.
    let framed = encode_resource_unavailable_framed();
    let chunks = cc_serve_probe::decode_response_chunks(
        &framed,
        true,
        Protocol::BeaconBlocksByRangeV2.response_limits(),
    )
    .unwrap();
    assert!(negative_verdict(&chunks, 1, "blocks").is_none());
    assert!(chunks.iter().all(ResponseChunk::is_resource_unavailable));
}

#[tokio::test]
async fn dial_prints_eas_and_head_via_peer_info() {
    let (_stub_id, mut stub) = make_swarm();
    let listen_addr = listen(&mut stub).await;
    let addr = format!("{listen_addr}/p2p/{_stub_id}");
    let stub_task = tokio::spawn(async move {
        run_cooperating_stub(stub).await;
    });

    let client = timeout(
        Duration::from_secs(20),
        ProbeClient::dial(&addr, ForkDigest::from_array(FORK)),
    )
    .await
    .expect("timeout")
    .expect("dial");
    let info = client.peer_info();
    assert_eq!(info.status.earliest_available_slot.as_u64(), EAS);
    assert_eq!(info.status.head_slot.as_u64(), HEAD);

    stub_task.abort();
}
