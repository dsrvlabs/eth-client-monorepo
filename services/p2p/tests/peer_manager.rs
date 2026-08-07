//! CC-20c acceptance: connection_limits hard cap, independent in/out caps,
//! allow_block_list ban enforcement — real in-process swarms (no docker).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use cc_libp2p::reexport::{ConnectionLimits, DialOpts, Multiaddr, PeerId, Swarm, SwarmEvent};
use cc_libp2p::{CcBehaviour, Keypair, SwarmConfig, build_swarm, connection_limits};
use cc_p2p::gossip::ethereum_behaviour_config;
use futures::StreamExt;
use tokio::time::timeout;

fn make_swarm(limits: ConnectionLimits) -> (PeerId, Swarm<CcBehaviour>) {
    let keypair = Keypair::generate_secp256k1();
    let peer_id = PeerId::from_public_key(&keypair.public());
    let mut cfg = ethereum_behaviour_config();
    cfg.connection_limits = limits;
    let behaviour = CcBehaviour::new(&keypair, cfg).expect("behaviour");
    let swarm = build_swarm(keypair, behaviour, &SwarmConfig::default()).expect("swarm");
    (peer_id, swarm)
}

async fn listen(swarm: &mut Swarm<CcBehaviour>) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("listen");
    timeout(Duration::from_secs(5), async {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                break address;
            }
        }
    })
    .await
    .expect("listen addr")
}

#[tokio::test]
async fn connection_limits_rejects_past_max_established() {
    // Hard cap independent of the peer-manager scheduler: max_established = 100.
    // Spin 101 dialers; assert the 101st is refused at the swarm level.
    let limits = connection_limits(Some(100), Some(100), Some(100));
    let (_lid, mut listener) = make_swarm(limits);
    let addr = listen(&mut listener).await;

    let mut dialers: Vec<Swarm<CcBehaviour>> = Vec::new();
    for _ in 0..101 {
        let (_id, mut d) = make_swarm(connection_limits(Some(100), Some(100), Some(100)));
        d.dial(DialOpts::unknown_peer_id().address(addr.clone()).build())
            .expect("dial");
        dialers.push(d);
    }

    let (n, denied) = timeout(Duration::from_secs(60), async {
        let mut n = 0usize;
        let mut denied = 0usize;
        loop {
            tokio::select! {
                ev = listener.select_next_some() => {
                    match ev {
                        SwarmEvent::ConnectionEstablished { .. } => n += 1,
                        SwarmEvent::IncomingConnectionError { .. } => denied += 1,
                        _ => {}
                    }
                }
                ev = futures::future::poll_fn(|cx| {
                    use std::task::Poll;
                    for d in dialers.iter_mut() {
                        if let Poll::Ready(e) = d.poll_next_unpin(cx) {
                            return Poll::Ready(e);
                        }
                    }
                    Poll::Pending
                }) => {
                    let _ = ev;
                }
            }
            if n >= 100 && denied >= 1 {
                break (n, denied);
            }
            if n + denied >= 101 && n >= 100 {
                break (n, denied);
            }
        }
    })
    .await
    .expect("timed out waiting for 101st denial under max=100");

    assert!(
        n <= 100,
        "connection_limits must cap established at 100, got {n}"
    );
    assert_eq!(n, 100, "101st must be refused at the swarm level");
    assert!(denied >= 1, "expected at least one denial, got {denied}");
}

#[tokio::test]
async fn inbound_and_outbound_caps_independent() {
    // Listener: max_established_incoming = 2, max_established_outgoing = 2,
    // max_established = 10 so the combined cap is not the bottleneck.
    let limits = connection_limits(Some(10), Some(2), Some(2));
    let (_lid, mut a) = make_swarm(limits);
    let addr_a = listen(&mut a).await;

    let mut inbound_hosts = Vec::new();
    for _ in 0..2 {
        let (_id, mut d) = make_swarm(connection_limits(Some(10), Some(10), Some(10)));
        d.dial(DialOpts::unknown_peer_id().address(addr_a.clone()).build())
            .expect("dial");
        inbound_hosts.push(d);
    }

    let mut got_in = 0usize;
    timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                ev = a.select_next_some() => {
                    if matches!(ev, SwarmEvent::ConnectionEstablished { .. }) {
                        got_in += 1;
                        if got_in >= 2 { break; }
                    }
                }
                _ = async {
                    for d in inbound_hosts.iter_mut() {
                        let _ = d.select_next_some().await;
                    }
                } => {}
            }
        }
    })
    .await
    .expect("two inbound");

    // A can still dial outbound even with inbound full.
    let (_id3, mut third) = make_swarm(connection_limits(Some(10), Some(10), Some(10)));
    let addr_third = listen(&mut third).await;
    a.dial(DialOpts::unknown_peer_id().address(addr_third.clone()).build())
        .expect("a dials third");

    let mut a_outbound_ok = false;
    timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                ev = a.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { endpoint, .. } = ev
                        && endpoint.is_dialer()
                    {
                        a_outbound_ok = true;
                        break;
                    }
                }
                _ = third.select_next_some() => {}
                _ = async {
                    for d in inbound_hosts.iter_mut() {
                        let _ = d.select_next_some().await;
                    }
                } => {}
            }
        }
    })
    .await
    .expect("outbound still permitted under full inbound");

    assert!(
        a_outbound_ok,
        "outbound dial must succeed even with inbound cap full"
    );
}

#[tokio::test]
async fn block_list_refuses_banned_peer() {
    let (_id_a, mut a) = make_swarm(connection_limits(Some(10), Some(10), Some(10)));
    let (id_b, mut b) = make_swarm(connection_limits(Some(10), Some(10), Some(10)));
    let addr_a = listen(&mut a).await;

    // Ban B on A before dial.
    a.behaviour_mut().allow_block.block_peer(id_b);

    b.dial(DialOpts::unknown_peer_id().address(addr_a).build())
        .expect("dial");

    let mut denied = false;
    let mut established = false;
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                ev = a.select_next_some() => {
                    match ev {
                        SwarmEvent::IncomingConnectionError { .. } => {
                            denied = true;
                            break;
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == id_b => {
                            established = true;
                            break;
                        }
                        _ => {}
                    }
                }
                _ = b.select_next_some() => {}
            }
        }
    })
    .await
    .expect("timed out waiting for ban enforcement");

    assert!(denied, "banned peer must be refused at swarm level");
    assert!(!established, "banned peer must not establish");
}
