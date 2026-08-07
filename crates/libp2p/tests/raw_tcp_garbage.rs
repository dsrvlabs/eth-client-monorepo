//! CC-20/5 — garbage before or during the Noise handshake must not take the host down.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::single_match,
    clippy::collapsible_match,
    clippy::collapsible_if
)]

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use cc_libp2p::reexport::{Multiaddr, PeerId, SwarmEvent};
use cc_libp2p::{BehaviourConfig, CcBehaviour, SwarmConfig, build_swarm};
use futures::StreamExt;
use libp2p::identity::Keypair;
use tokio::time::timeout;

fn make_swarm() -> (PeerId, libp2p::swarm::Swarm<CcBehaviour>) {
    let keypair = Keypair::generate_secp256k1();
    let peer_id = PeerId::from_public_key(&keypair.public());
    let behaviour = CcBehaviour::new(&keypair, BehaviourConfig::default()).expect("behaviour");
    let swarm = build_swarm(keypair, behaviour, &SwarmConfig::default()).expect("swarm");
    (peer_id, swarm)
}

async fn listen_loopback(swarm: &mut libp2p::swarm::Swarm<CcBehaviour>) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().expect("addr"))
        .expect("listen");
    timeout(Duration::from_secs(5), async {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    // Prefer plain /ip4/…/tcp/… without p2p suffix for raw TcpStream.
                    if address
                        .iter()
                        .any(|p| matches!(p, libp2p::multiaddr::Protocol::Tcp(_)))
                    {
                        return address;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("listen timeout")
}

fn multiaddr_to_socket(addr: &Multiaddr) -> std::net::SocketAddr {
    let mut ip = None;
    let mut port = None;
    for p in addr.iter() {
        match p {
            libp2p::multiaddr::Protocol::Ip4(v) => ip = Some(std::net::IpAddr::V4(v)),
            libp2p::multiaddr::Protocol::Ip6(v) => ip = Some(std::net::IpAddr::V6(v)),
            libp2p::multiaddr::Protocol::Tcp(p) => port = Some(p),
            _ => {}
        }
    }
    std::net::SocketAddr::new(ip.expect("ip"), port.expect("port"))
}

async fn assert_incoming_error_then_legit_dial(
    mut host: libp2p::swarm::Swarm<CcBehaviour>,
    listen: Multiaddr,
    write_garbage: impl FnOnce(std::net::SocketAddr) + Send + 'static,
) {
    let socket = multiaddr_to_socket(&listen);
    let garbage = tokio::task::spawn_blocking(move || write_garbage(socket));

    let mut saw_incoming_error = false;
    timeout(Duration::from_secs(10), async {
        while !saw_incoming_error {
            match host.select_next_some().await {
                SwarmEvent::IncomingConnectionError { .. } => {
                    saw_incoming_error = true;
                }
                SwarmEvent::IncomingConnection { .. } => {
                    // Handshake still in progress / about to fail.
                }
                _ => {}
            }
        }
    })
    .await
    .expect("expected IncomingConnectionError");

    garbage.await.expect("garbage task");

    // Second half of CC-20/5: a subsequent legitimate dial from a second host succeeds.
    let (peer_b, mut guest) = make_swarm();
    guest.dial(listen).expect("legit dial");

    let mut connected = false;
    timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                ev = host.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = ev {
                        if peer_id == peer_b {
                            connected = true;
                            break;
                        }
                    }
                }
                ev = guest.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = ev {
                        connected = true;
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("legitimate dial must succeed after garbage");

    assert!(
        connected,
        "host must accept a legitimate dial after garbage"
    );
    assert!(saw_incoming_error);
}

#[tokio::test]
async fn garbage_before_noise_handshake_host_stays_up() {
    let (_id, mut host) = make_swarm();
    let listen = listen_loopback(&mut host).await;

    assert_incoming_error_then_legit_dial(host, listen, |socket| {
        let mut stream = TcpStream::connect(socket).expect("raw connect");
        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
        // Random bytes before any multistream/Noise negotiation.
        let junk = [0xdeu8, 0xad, 0xbe, 0xef]
            .iter()
            .cycle()
            .take(256)
            .copied()
            .collect::<Vec<_>>();
        let _ = stream.write_all(&junk);
        let _ = stream.flush();
        // Keep the socket open briefly so the host sees the bad handshake.
        std::thread::sleep(Duration::from_millis(200));
    })
    .await;
}

#[tokio::test]
async fn garbage_during_noise_handshake_host_stays_up() {
    let (_id, mut host) = make_swarm();
    let listen = listen_loopback(&mut host).await;

    assert_incoming_error_then_legit_dial(host, listen, |socket| {
        let mut stream = TcpStream::connect(socket).expect("raw connect");
        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .ok();

        // Multistream-select prologue for /noise, then garbage instead of a real handshake.
        // libp2p multistream-select: length-prefixed protocol strings.
        // A minimal corrupted negotiation is enough: write a plausible header then junk.
        let noise_proto = b"\x13/noise\n";
        let _ = stream.write_all(noise_proto);
        let _ = stream.flush();
        // Drain a little of the peer's multistream greeting if any.
        let mut buf = [0u8; 64];
        let _ = std::io::Read::read(&mut stream, &mut buf);
        // Garbage mid-handshake.
        let junk = (0u8..128).collect::<Vec<_>>();
        let _ = stream.write_all(&junk);
        let _ = stream.flush();
        std::thread::sleep(Duration::from_millis(200));
    })
    .await;
}
