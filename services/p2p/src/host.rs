//! Swarm task — sole owner of `Swarm<CcBehaviour>` (Architecture §2.1, ADR P2-02).
//!
//! **The swarm task never does work.** Events are routed onto bounded channels
//! within microseconds; mutations arrive only as [`SwarmCommand`]s on `cmd_rx`.
//! There is no `Mutex<Swarm>` / `RwLock<Swarm>` anywhere in `services/p2p`.

use cc_libp2p::reexport::futures::StreamExt;
use cc_libp2p::reexport::{Multiaddr, Swarm, SwarmEvent};
use cc_libp2p::{CcBehaviour, CcBehaviourEvent};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::channels::{
    ConnEvent, GossipWork, PublishRequest, ReqRespInbound, SwarmCommand,
};
use crate::metrics::{P2pMetrics, QueueName};

/// Inputs owned exclusively by the swarm task after spawn.
#[allow(missing_debug_implementations)] // `Swarm` is not Debug
pub struct SwarmTask {
    /// The only `Swarm<CcBehaviour>` in the process.
    pub swarm: Swarm<CcBehaviour>,
    /// peer-manager / publish path → swarm.
    pub cmd_rx: mpsc::Receiver<SwarmCommand>,
    /// swarm → gossip validation.
    pub gossip_tx: mpsc::Sender<GossipWork>,
    /// swarm → req/resp server.
    pub reqresp_in_tx: mpsc::Sender<ReqRespInbound>,
    /// swarm → peer manager.
    pub conn_tx: mpsc::Sender<ConnEvent>,
    /// Metric handles for queue depth.
    pub metrics: P2pMetrics,
}

/// Build + listen, then run the event loop until `cmd_rx` closes.
///
/// Listen only — **no dialling** in this issue (CC-21c owns the first bootnode dial).
pub async fn run_swarm_task(mut task: SwarmTask, listen_addr: Multiaddr) {
    match task.swarm.listen_on(listen_addr.clone()) {
        Ok(id) => info!(%listen_addr, ?id, "swarm listening"),
        Err(e) => {
            error!(%listen_addr, error = %e, "swarm listen_on failed");
            return;
        }
    }

    loop {
        tokio::select! {
            event = task.swarm.select_next_some() => {
                route_swarm_event(&mut task, event).await;
            }
            cmd = task.cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => handle_command(&mut task, cmd).await,
                    None => {
                        info!("swarm cmd channel closed; swarm task exiting");
                        break;
                    }
                }
            }
        }
    }
}

async fn route_swarm_event(task: &mut SwarmTask, event: SwarmEvent<CcBehaviourEvent>) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "new listen address");
            let _ = task.conn_tx.try_send(ConnEvent {
                tag: "new_listen_addr",
            });
            bump_depth(&task.metrics, QueueName::Conn, task.conn_tx.max_capacity());
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            debug!(%peer_id, "connection established");
            let _ = task.conn_tx.try_send(ConnEvent {
                tag: "connection_established",
            });
            bump_depth(&task.metrics, QueueName::Conn, task.conn_tx.max_capacity());
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            debug!(%peer_id, "connection closed");
            let _ = task.conn_tx.try_send(ConnEvent {
                tag: "connection_closed",
            });
            bump_depth(&task.metrics, QueueName::Conn, task.conn_tx.max_capacity());
        }
        SwarmEvent::Behaviour(bev) => route_behaviour(task, bev).await,
        SwarmEvent::IncomingConnectionError { error, .. } => {
            debug!(error = %error, "incoming connection error");
        }
        SwarmEvent::OutgoingConnectionError { error, .. } => {
            debug!(error = %error, "outgoing connection error");
        }
        other => {
            debug!(event = ?other, "swarm event");
        }
    }
}

async fn route_behaviour(task: &mut SwarmTask, bev: CcBehaviourEvent) {
    match bev {
        CcBehaviourEvent::Gossipsub(ev) => {
            // Real validation pool is CC-22*; route a stub work item so the edge exists.
            if let cc_libp2p::reexport::gossipsub::Event::Message { message, .. } = ev {
                let work = GossipWork {
                    bytes: message.data,
                };
                match task.gossip_tx.try_send(work) {
                    Ok(()) => {
                        bump_depth(
                            &task.metrics,
                            QueueName::Gossip,
                            task.gossip_tx.max_capacity(),
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // §2.3 stall-then-shed is CC-27b; for now drop and count later.
                        warn!("gossip validation queue full; dropping (shed path is CC-27b)");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!("gossip validation queue closed");
                    }
                }
            }
        }
        CcBehaviourEvent::Reqresp(ev) => {
            // Codec body is CC-23a; still exercise the reqresp_in edge shape.
            let _ = ev;
            let _ = task.reqresp_in_tx.try_send(ReqRespInbound { bytes: Vec::new() });
            bump_depth(
                &task.metrics,
                QueueName::ReqrespIn,
                task.reqresp_in_tx.max_capacity(),
            );
        }
        CcBehaviourEvent::Identify(_)
        | CcBehaviourEvent::Ping(_)
        | CcBehaviourEvent::Limits(_)
        | CcBehaviourEvent::AllowBlock(_) => {
            // Peer-manager scoring / ban policy is CC-20c.
        }
    }
}

async fn handle_command(task: &mut SwarmTask, cmd: SwarmCommand) {
    // Depth drops when we dequeue a command.
    let depth = task.metrics.queue_depth(QueueName::Cmd);
    if depth > 0 {
        task.metrics.set_queue_depth(QueueName::Cmd, depth - 1);
    }

    match cmd {
        SwarmCommand::Noop => {}
        SwarmCommand::Publish(PublishRequest { topic, data }) => {
            // Real publish uses IdentTopic + gossipsub; topic registry is CC-22a.
            // Keep the command path hot without depending on topic validation here.
            debug!(%topic, len = data.len(), "publish command (stub until topic wiring)");
            let _ = (topic, data);
        }
        SwarmCommand::Subscribe { topic } => {
            debug!(%topic, "subscribe command (stub until topic wiring)");
            let _ = topic;
        }
        SwarmCommand::Disconnect { peer_id } => {
            debug!(%peer_id, "disconnect command (typed PeerId in CC-20c)");
            let _ = peer_id;
        }
    }
}

fn bump_depth(metrics: &P2pMetrics, q: QueueName, max_capacity: usize) {
    // tokio mpsc: max_capacity() is remaining capacity; depth ≈ bound - remaining
    // is not exposed directly. Approximate by incrementing and clamping to bound.
    let bound = match q {
        QueueName::Gossip => crate::channels::GOSSIP_BOUND,
        QueueName::ReqrespIn => crate::channels::REQRESP_IN_BOUND,
        QueueName::Conn => crate::channels::CONN_BOUND,
        QueueName::Kzg => crate::channels::KZG_BOUND,
        QueueName::Publish => crate::channels::PUBLISH_BOUND,
        QueueName::Cmd => crate::channels::CMD_BOUND,
        _ => max_capacity.saturating_add(1),
    };
    let next = (metrics.queue_depth(q) + 1).min(bound as i64);
    metrics.set_queue_depth(q, next);
}

/// Construct a swarm from identity material (helper for [`crate::service`]).
pub fn build_host_swarm(
    keypair: cc_libp2p::Keypair,
) -> Result<Swarm<CcBehaviour>, HostBuildError> {
    let behaviour = CcBehaviour::new(&keypair, cc_libp2p::BehaviourConfig::default())
        .map_err(|e| HostBuildError::Behaviour(e.to_string()))?;
    cc_libp2p::build_swarm(keypair, behaviour, &cc_libp2p::SwarmConfig::default())
        .map_err(|e| HostBuildError::Swarm(e.to_string()))
}

/// Errors from swarm construction.
#[derive(Debug, thiserror::Error)]
pub enum HostBuildError {
    /// Behaviour composition failed.
    #[error("cc behaviour: {0}")]
    Behaviour(String),
    /// Swarm build failed.
    #[error("swarm: {0}")]
    Swarm(String),
}
