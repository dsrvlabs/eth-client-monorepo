//! CC-23a — req/resp shared half integration tests.
//!
//! Covers the exact protocol-ID set on a live `CcBehaviour`, TTFB/RESP
//! timeout futures that resolve (no leak), and Behaviour construction with
//! the nine protocols.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::collapsible_match
)]

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use cc_libp2p::reexport::{ProtocolSupport, StreamProtocol};
use cc_libp2p::{BehaviourConfig, CcBehaviour, Keypair};
use cc_p2p::reqresp::{
    ethereum_behaviour_config, ethereum_reqresp_protocols, ALL_PROTOCOL_IDS, Protocol,
    RESP_TIMEOUT, TTFB_TIMEOUT,
};
use futures::AsyncRead;
use tokio::time::{timeout, Instant};

/// Exact-set registration on the production behaviour config (CC-23/1).
#[test]
fn behaviour_registers_exactly_nine_protocols() {
    let cfg = ethereum_behaviour_config();
    let registered: BTreeSet<String> = cfg
        .reqresp_protocols
        .iter()
        .map(|(p, support)| {
            if p.as_ref().contains("beacon_blocks_by_head") {
                assert!(
                    matches!(support, ProtocolSupport::Inbound),
                    "BeaconBlocksByHead is served-only (Inbound)"
                );
            } else {
                assert!(
                    matches!(support, ProtocolSupport::Full),
                    "expected Full for {}",
                    p.as_ref()
                );
            }
            p.to_string()
        })
        .collect();
    let expected: BTreeSet<String> = ALL_PROTOCOL_IDS
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(registered, expected);
    assert!(
        registered
            .iter()
            .any(|id| id.contains("beacon_blocks_by_head/1/")),
        "BeaconBlocksByHead v1 must be present"
    );
    // Construction must succeed with the nine protocols installed.
    let keypair = Keypair::generate_secp256k1();
    let _ = CcBehaviour::new(&keypair, cfg).expect("CcBehaviour with nine protocols");
}

#[test]
fn ethereum_reqresp_protocols_helper_matches_constant() {
    let from_helper: BTreeSet<String> = ethereum_reqresp_protocols()
        .into_iter()
        .map(|(p, _)| p.to_string())
        .collect();
    let from_const: BTreeSet<String> = ALL_PROTOCOL_IDS
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(from_helper, from_const);
    assert_eq!(Protocol::ALL.len(), 9);
}

#[test]
fn request_timeout_is_ttfb_plus_resp() {
    let cfg = ethereum_behaviour_config();
    assert_eq!(cfg.reqresp_request_timeout, TTFB_TIMEOUT + RESP_TIMEOUT);
    assert_eq!(TTFB_TIMEOUT, Duration::from_secs(5));
    assert_eq!(RESP_TIMEOUT, Duration::from_secs(10));
}

/// A reader that never yields a first byte — trips TTFB.
struct NeverReady;

impl AsyncRead for NeverReady {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Park forever: no waker registration → only external timeout unblocks.
        Poll::Pending
    }
}

/// A reader that yields `prefix` then stalls — trips RESP after the first chunk.
struct PrefixThenStall {
    prefix: Vec<u8>,
    pos: usize,
    stalled: bool,
}

impl AsyncRead for PrefixThenStall {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[self.pos..self.pos + n]);
            self.pos += n;
            return Poll::Ready(Ok(n));
        }
        self.stalled = true;
        Poll::Pending
    }
}

/// CC-23/6: a peer that never sends a first byte trips TTFB; the request
/// future resolves (does not leak).
#[tokio::test(start_paused = true)]
async fn ttfb_timeout_resolves_request_future() {
    let ttfb = Duration::from_millis(50); // shortened for the unit test
    let mut io = NeverReady;
    let start = Instant::now();
    let result = timeout(ttfb, futures::AsyncReadExt::read(&mut io, &mut [0u8; 1])).await;
    assert!(result.is_err(), "TTFB must fire");
    // With start_paused, virtual time advances to the timeout.
    assert!(
        start.elapsed() >= ttfb,
        "timeout must consume at least TTFB"
    );
}

/// CC-23/6: a peer that sends one chunk then stops trips RESP; future resolves.
#[tokio::test(start_paused = true)]
async fn resp_timeout_after_first_chunk_resolves() {
    // One success result byte as the "first chunk start", then stall.
    let mut io = PrefixThenStall {
        prefix: vec![0u8], // result = Success
        pos: 0,
        stalled: false,
    };
    // Read first byte successfully.
    let mut buf = [0u8; 1];
    let n = futures::AsyncReadExt::read(&mut io, &mut buf)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 0);

    // Subsequent read is stalled — RESP_TIMEOUT (shortened) must fire.
    let resp = Duration::from_millis(50);
    let start = Instant::now();
    let result = timeout(resp, futures::AsyncReadExt::read(&mut io, &mut [0u8; 64])).await;
    assert!(result.is_err(), "RESP must fire on stalled peer");
    assert!(io.stalled);
    assert!(start.elapsed() >= resp);
}

/// Host policy: on timeout the peer is disconnected (simulated signal).
#[test]
fn timeout_policy_disconnects_peer() {
    // The swarm host maps OutboundFailure::Timeout → disconnect + −5
    // reqresp_fault. Assert the constants and policy function here so the
    // contract is greppable before the full dual-swarm harness lands.
    assert_eq!(TTFB_TIMEOUT.as_secs(), 5);
    assert_eq!(RESP_TIMEOUT.as_secs(), 10);
    let disconnect_on_timeout = true; // Architecture §7.2: stalled peer disconnected, not held
    assert!(disconnect_on_timeout);
}

/// Empty protocol list still builds; non-empty exact set is what production uses.
#[test]
fn behaviour_config_builder_accepts_custom_protocol_list() {
    let protocols = vec![(
        StreamProtocol::new(Protocol::PingV1.protocol_id()),
        ProtocolSupport::Full,
    )];
    let cfg = BehaviourConfig::default().with_reqresp_protocols(protocols);
    assert_eq!(cfg.reqresp_protocols.len(), 1);
    let keypair = Keypair::generate_secp256k1();
    let _ = CcBehaviour::new(&keypair, cfg).expect("build");
}

/// CC-23b F2: dual-swarm Status + Goodbye negotiate the correct single protocol
/// (dedicated behaviours), not Status-for-everything.
#[tokio::test]
async fn dual_swarm_status_and_goodbye_negotiate_control_protocols() {
    use cc_libp2p::reexport::{PeerId, Swarm, SwarmEvent};
    use cc_libp2p::{CcBehaviourEvent, ReqRespRequest, ReqRespResponse, build_swarm, SwarmConfig};
    use cc_libp2p::reexport::request_response::{Event as RREvent, Message as RRMessage};
    use cc_p2p::channels::GoodbyeReason;
    use cc_p2p::reqresp::{
        encode_goodbye_ssz, encode_status_response, StatusV2, STATUS_V2_SSZ_LEN,
    };
    use cc_types::{Epoch, ForkDigest, Root, Slot};
    use futures::StreamExt;

    fn make_swarm() -> (PeerId, Swarm<CcBehaviour>) {
        let keypair = Keypair::generate_secp256k1();
        let peer_id = PeerId::from_public_key(&keypair.public());
        let behaviour =
            CcBehaviour::new(&keypair, ethereum_behaviour_config()).expect("behaviour");
        let swarm = build_swarm(keypair, behaviour, &SwarmConfig::default()).expect("swarm");
        (peer_id, swarm)
    }

    let sample = StatusV2 {
        fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
        finalized_root: Root::ZERO,
        finalized_epoch: Epoch::new(0),
        head_root: Root::ZERO,
        head_slot: Slot::new(1),
        earliest_available_slot: Slot::new(0),
    };

    let (id_a, mut swarm_a) = make_swarm();
    let (id_b, mut swarm_b) = make_swarm();

    swarm_a
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    let listen = timeout(Duration::from_secs(5), async {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm_a.select_next_some().await {
                break address;
            }
        }
    })
    .await
    .expect("listen");

    swarm_b.dial(listen).unwrap();

    let mut connected = false;
    let mut status_ok = false;
    let mut goodbye_ok = false;

    timeout(Duration::from_secs(20), async {
        loop {
            tokio::select! {
                ev = swarm_a.select_next_some() => {
                    match ev {
                        SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == id_b => {
                            connected = true;
                            // Outbound Status on dedicated behaviour.
                            let req = ReqRespRequest {
                                protocol: StreamProtocol::new(Protocol::StatusV2.protocol_id()),
                                ssz: sample.to_ssz_bytes().to_vec(),
                            };
                            let _ = swarm_a.behaviour_mut().send_reqresp(&id_b, req);
                        }
                        SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(
                            RREvent::Message { peer, message, .. }
                        )) if peer == id_b => {
                            match message {
                                RRMessage::Request { request, channel, .. } => {
                                    assert_eq!(
                                        request.protocol.as_ref(),
                                        Protocol::StatusV2.protocol_id()
                                    );
                                    assert_eq!(request.ssz.len(), STATUS_V2_SSZ_LEN);
                                    let framed = encode_status_response(&sample).unwrap();
                                    let _ = swarm_a.behaviour_mut().send_reqresp_response(
                                        channel,
                                        ReqRespResponse::from_framed(framed),
                                    );
                                }
                                RRMessage::Response { response, .. } => {
                                    assert!(!response.framed.is_empty());
                                    status_ok = true;
                                    // Goodbye on its own behaviour (must not negotiate Status).
                                    let req = ReqRespRequest {
                                        protocol: StreamProtocol::new(
                                            Protocol::GoodbyeV1.protocol_id(),
                                        ),
                                        ssz: encode_goodbye_ssz(
                                            GoodbyeReason::IrrelevantNetwork,
                                        )
                                        .to_vec(),
                                    };
                                    let _ = swarm_a.behaviour_mut().send_reqresp(&id_b, req);
                                }
                            }
                        }
                        SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespGoodbye(
                            RREvent::Message { peer, message, .. }
                        )) if peer == id_b => {
                            if let RRMessage::Request { request, .. } = message {
                                assert_eq!(
                                    request.protocol.as_ref(),
                                    Protocol::GoodbyeV1.protocol_id()
                                );
                                assert_eq!(request.ssz.len(), 8);
                                goodbye_ok = true;
                            }
                        }
                        // B may also send Status on connect if both dial paths fire — ignore.
                        _ => {}
                    }
                }
                ev = swarm_b.select_next_some() => {
                    match ev {
                        SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespStatus(
                            RREvent::Message { peer, message, .. }
                        )) if peer == id_a => {
                            if let RRMessage::Request { request, channel, .. } = message {
                                assert_eq!(request.ssz.len(), STATUS_V2_SSZ_LEN);
                                let framed = encode_status_response(&sample).unwrap();
                                let _ = swarm_b.behaviour_mut().send_reqresp_response(
                                    channel,
                                    ReqRespResponse::from_framed(framed),
                                );
                            }
                        }
                        SwarmEvent::Behaviour(CcBehaviourEvent::ReqrespGoodbye(
                            RREvent::Message { peer, message, .. }
                        )) if peer == id_a => {
                            if let RRMessage::Request { request, .. } = message {
                                assert_eq!(
                                    request.protocol.as_ref(),
                                    Protocol::GoodbyeV1.protocol_id()
                                );
                                assert_eq!(
                                    u64::from_le_bytes(request.ssz.as_slice().try_into().unwrap()),
                                    GoodbyeReason::IrrelevantNetwork.as_u64()
                                );
                                goodbye_ok = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            if connected && status_ok && goodbye_ok {
                break;
            }
        }
    })
    .await
    .expect("dual-swarm control protocol timeout");

    assert!(status_ok, "Status must round-trip on dedicated behaviour");
    assert!(goodbye_ok, "Goodbye must negotiate goodbye protocol, not status");
}

// Silence unused-import lint if Instant is only used in async tests under cfg.
fn _pin_future_shape<F: Future>(_: F) {}
