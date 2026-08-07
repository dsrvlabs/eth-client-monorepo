//! Swarm task — sole owner of `Swarm<CcBehaviour>` (Architecture §2.1, ADR P2-02).
//!
//! **The swarm task never does work.** Events are routed onto bounded channels
//! within microseconds; mutations arrive only as [`SwarmCommand`]s on `cmd_rx`.
//! There is no `Mutex<Swarm>` / `RwLock<Swarm>` anywhere in `services/p2p`.
//!
//! **H1 (lifecycle delivery):** `ConnectionEstablished` / `ConnectionClosed` /
//! `DialFailure` are never silently dropped. If the conn queue is full they
//! enter a durable pending buffer and the task stops reading new swarm events
//! until capacity returns, while still draining `cmd_rx` (avoids deadlock with
//! peer-manager `send().await` on policy cmds).
//!
//! **§2.3 stall-then-shed (CC-27b):** before polling `swarm.next()` the task
//! acquires a permit on the chain-stream outbound channel. The stall is bounded
//! by `stall_max_from_heartbeat(heartbeat_interval)` — derived from config, not
//! an inlined millisecond constant. On expiry the loop sheds new gossip with
//! `IGNORE` and increments `cc_p2p_gossip_shed_total{topic}`. Shed messages
//! never enter the chain stream, so the CC-27/4 equality is untouched.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use cc_libp2p::reexport::futures::StreamExt;
use cc_libp2p::reexport::{DialOpts, IdentTopic, MessageAcceptance, Multiaddr, Swarm, SwarmEvent};
use cc_libp2p::{CcBehaviour, CcBehaviourEvent};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::chain_stream::stall_max_from_heartbeat;
use crate::channels::{
    ChainOutbound, ConnEvent, ConnectionDirection, GossipWork, PublishRequest, ReqRespInbound,
    SwarmCommand,
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
    /// any → chain-stream outbound (backpressure permit source, §2.3).
    pub chain_out_tx: mpsc::Sender<ChainOutbound>,
    /// Gossipsub heartbeat interval (stall bound is derived from this).
    pub heartbeat_interval: Duration,
    /// Metric handles for queue depth.
    pub metrics: P2pMetrics,
    /// Goodbye commands observed before wire handler (CC-23b) exists.
    pub goodbye_dropped: u64,
    /// Durable buffer for lifecycle conn events that could not enter `conn_tx`
    /// without dropping (H1). Never holds `NewListenAddr` only.
    pending_conn: VecDeque<ConnEvent>,
    /// Cumulative stall time observed (for tests / soak).
    pub stall_fired: bool,
}

impl SwarmTask {
    /// Construct with empty pending buffer.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // Channel fan-in is intentional (sole Swarm owner).
    pub fn new(
        swarm: Swarm<CcBehaviour>,
        cmd_rx: mpsc::Receiver<SwarmCommand>,
        gossip_tx: mpsc::Sender<GossipWork>,
        reqresp_in_tx: mpsc::Sender<ReqRespInbound>,
        conn_tx: mpsc::Sender<ConnEvent>,
        chain_out_tx: mpsc::Sender<ChainOutbound>,
        heartbeat_interval: Duration,
        metrics: P2pMetrics,
    ) -> Self {
        Self {
            swarm,
            cmd_rx,
            gossip_tx,
            reqresp_in_tx,
            conn_tx,
            chain_out_tx,
            heartbeat_interval,
            metrics,
            goodbye_dropped: 0,
            pending_conn: VecDeque::new(),
            stall_fired: false,
        }
    }
}

/// Build + listen, then run the event loop until `cmd_rx` closes.
///
/// Listens always; dials only when the peer manager (or a test) sends
/// [`SwarmCommand::Dial`]. Discovery-driven dials are CC-21c.
pub async fn run_swarm_task(mut task: SwarmTask, listen_addr: Multiaddr) {
    match task.swarm.listen_on(listen_addr.clone()) {
        Ok(id) => info!(%listen_addr, ?id, "swarm listening"),
        Err(e) => {
            error!(%listen_addr, error = %e, "swarm listen_on failed");
            return;
        }
    }

    // Stall bound from configured heartbeat (R-6) — never an inlined constant.
    let stall_max = stall_max_from_heartbeat(task.heartbeat_interval);
    info!(
        heartbeat_ms = task.heartbeat_interval.as_millis() as u64,
        stall_max_ms = stall_max.as_millis() as u64,
        "swarm stall-then-shed bound derived from heartbeat"
    );

    loop {
        // Always push pending lifecycle events as soon as capacity exists.
        flush_pending_conn(&mut task);

        let pending = !task.pending_conn.is_empty();
        if pending {
            // H1 fail-closed: do not read new swarm events while lifecycle
            // delivery is backlogged. Still drain cmds so policy closes can
            // free peers and PM can make progress (avoids H2 deadlock).
            enum PendingWait {
                Cmd(Option<SwarmCommand>),
                Capacity,
                ConnClosed,
            }
            let wait = {
                let cmd_rx = &mut task.cmd_rx;
                let conn_tx = &task.conn_tx;
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => PendingWait::Cmd(cmd),
                    permit = conn_tx.reserve() => match permit {
                        Ok(p) => {
                            drop(p);
                            PendingWait::Capacity
                        }
                        Err(_) => PendingWait::ConnClosed,
                    },
                }
            };
            match wait {
                PendingWait::Cmd(Some(cmd)) => handle_command(&mut task, cmd).await,
                PendingWait::Cmd(None) => {
                    info!("swarm cmd channel closed; swarm task exiting");
                    break;
                }
                PendingWait::Capacity => {
                    flush_pending_conn(&mut task);
                }
                PendingWait::ConnClosed => {
                    error!("conn channel closed while pending lifecycle events");
                    break;
                }
            }
        } else {
            // §2.3: acquire a chain_out permit before polling the mesh.
            // On stall expiry, resume polling and shed gossip (IGNORE).
            enum IdleWait {
                Event {
                    event: Box<SwarmEvent<CcBehaviourEvent>>,
                    shed: bool,
                },
                Cmd(Option<SwarmCommand>),
            }

            let stall_start = Instant::now();
            let wait = {
                let swarm = &mut task.swarm;
                let cmd_rx = &mut task.cmd_rx;
                let chain_out_tx = &task.chain_out_tx;
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => IdleWait::Cmd(cmd),
                    permit = chain_out_tx.reserve() => {
                        match permit {
                            Ok(p) => {
                                // Capacity is available — drop the permit (we
                                // re-acquire via try_send on the producer path).
                                // Holding it would reserve a slot forever.
                                drop(p);
                                let elapsed = stall_start.elapsed();
                                if elapsed > Duration::from_millis(1) {
                                    task.metrics
                                        .set_swarm_stall_seconds(elapsed.as_secs_f64());
                                }
                                tokio::select! {
                                    event = swarm.select_next_some() => IdleWait::Event {
                                        event: Box::new(event),
                                        shed: false,
                                    },
                                    cmd = cmd_rx.recv() => IdleWait::Cmd(cmd),
                                }
                            }
                            Err(_) => {
                                // Chain-out channel closed: still drain swarm/cmd.
                                tokio::select! {
                                    event = swarm.select_next_some() => IdleWait::Event {
                                        event: Box::new(event),
                                        shed: false,
                                    },
                                    cmd = cmd_rx.recv() => IdleWait::Cmd(cmd),
                                }
                            }
                        }
                    }
                    _ = tokio::time::sleep(stall_max) => {
                        // Stall bound expired — shed mode for this iteration.
                        let elapsed = stall_start.elapsed();
                        task.metrics.set_swarm_stall_seconds(elapsed.as_secs_f64());
                        task.stall_fired = true;
                        debug!(
                            stall_ms = elapsed.as_millis() as u64,
                            "chain_out stall expired; shedding gossip this tick"
                        );
                        tokio::select! {
                            event = swarm.select_next_some() => IdleWait::Event {
                                event: Box::new(event),
                                shed: true,
                            },
                            cmd = cmd_rx.recv() => IdleWait::Cmd(cmd),
                        }
                    }
                }
            };
            match wait {
                IdleWait::Event { event, shed } => {
                    route_swarm_event(&mut task, *event, shed).await;
                }
                IdleWait::Cmd(Some(cmd)) => handle_command(&mut task, cmd).await,
                IdleWait::Cmd(None) => {
                    info!("swarm cmd channel closed; swarm task exiting");
                    break;
                }
            }
        }
    }
}

fn flush_pending_conn(task: &mut SwarmTask) {
    while let Some(front) = task.pending_conn.front() {
        match task.conn_tx.try_send(front.clone()) {
            Ok(()) => {
                task.pending_conn.pop_front();
                bump_depth(
                    &task.metrics,
                    QueueName::Conn,
                    task.conn_tx.max_capacity(),
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => break,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("conn channel closed; discarding pending lifecycle buffer");
                task.pending_conn.clear();
                break;
            }
        }
    }
}

/// Deliver a **lifecycle** conn event without silent drop (H1).
fn deliver_lifecycle(task: &mut SwarmTask, event: ConnEvent) {
    // Drain any older pending first so order is preserved.
    flush_pending_conn(task);
    match task.conn_tx.try_send(event) {
        Ok(()) => {
            bump_depth(
                &task.metrics,
                QueueName::Conn,
                task.conn_tx.max_capacity(),
            );
        }
        Err(mpsc::error::TrySendError::Full(ev)) => {
            // Durable buffer — never drop ConnectionClosed / Established / DialFailure.
            error!(
                pending = task.pending_conn.len() + 1,
                "conn queue full; buffering lifecycle event (H1; overflow is a bug)"
            );
            task.pending_conn.push_back(ev);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            error!("conn channel closed; lifecycle event lost");
        }
    }
}

async fn route_swarm_event(
    task: &mut SwarmTask,
    event: SwarmEvent<CcBehaviourEvent>,
    shed: bool,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "new listen address");
            // Informational — may drop under pressure (not a table lifecycle event).
            if task
                .conn_tx
                .try_send(ConnEvent::NewListenAddr { address })
                .is_ok()
            {
                bump_depth(
                    &task.metrics,
                    QueueName::Conn,
                    task.conn_tx.max_capacity(),
                );
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            let direction = if endpoint.is_dialer() {
                ConnectionDirection::Outbound
            } else {
                ConnectionDirection::Inbound
            };
            let remote = endpoint.get_remote_address().clone();
            debug!(%peer_id, ?direction, "connection established");
            deliver_lifecycle(
                task,
                ConnEvent::ConnectionEstablished {
                    peer_id,
                    direction,
                    endpoint: remote,
                },
            );
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            debug!(%peer_id, "connection closed");
            deliver_lifecycle(task, ConnEvent::ConnectionClosed { peer_id });
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            debug!(?peer_id, error = %error, "outgoing connection error");
            deliver_lifecycle(
                task,
                ConnEvent::DialFailure {
                    peer_id,
                    error: error.to_string(),
                },
            );
        }
        SwarmEvent::Behaviour(bev) => route_behaviour(task, bev, shed).await,
        SwarmEvent::IncomingConnectionError { error, .. } => {
            debug!(error = %error, "incoming connection error");
        }
        other => {
            debug!(event = ?other, "swarm event");
        }
    }
}

async fn route_behaviour(task: &mut SwarmTask, bev: CcBehaviourEvent, shed: bool) {
    match bev {
        CcBehaviourEvent::Gossipsub(ev) => {
            if let cc_libp2p::reexport::gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } = ev
            {
                let topic = message.topic.to_string();
                if shed {
                    // §2.3: IGNORE without entering the pipeline. Sender did
                    // nothing wrong — must not be descored. Shed never touches
                    // the chain stream, so CC-27/4 equality is preserved.
                    task.metrics.inc_gossip_shed(&topic);
                    let _ = task.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                        &message_id,
                        &propagation_source,
                        MessageAcceptance::Ignore,
                    );
                    debug!(%topic, "shed gossip message (IGNORE; not sent down stream)");
                    return;
                }
                // Real validation pool is CC-22*; route a stub work item so the edge exists.
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
                        // H2: same shed semantics as stall — release gossipsub
                        // with IGNORE so the message is not held pending forever.
                        task.metrics.inc_gossip_shed(&topic);
                        let _ = task.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                            &message_id,
                            &propagation_source,
                            MessageAcceptance::Ignore,
                        );
                        warn!(%topic, "gossip validation queue full; shed IGNORE");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Channel gone: still release the hold.
                        let _ = task.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                            &message_id,
                            &propagation_source,
                            MessageAcceptance::Ignore,
                        );
                        warn!("gossip validation queue closed; reported IGNORE");
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
            // Peer-manager scoring / ban policy is CC-20c (events observed here).
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
            let ident = IdentTopic::new(topic.clone());
            match task.swarm.behaviour_mut().gossipsub.publish(ident, data) {
                Ok(id) => debug!(%topic, ?id, "published to gossipsub"),
                Err(e) => warn!(%topic, error = %e, "gossipsub publish failed"),
            }
        }
        SwarmCommand::Subscribe { topic } => {
            let ident = IdentTopic::new(topic.clone());
            match task.swarm.behaviour_mut().gossipsub.subscribe(&ident) {
                Ok(true) => debug!(%topic, "subscribed"),
                Ok(false) => debug!(%topic, "already subscribed"),
                Err(e) => warn!(%topic, error = %e, "subscribe failed"),
            }
        }
        SwarmCommand::Dial { peer_id, addr } => {
            let opts = DialOpts::peer_id(peer_id)
                .addresses(vec![addr.clone()])
                .build();
            match task.swarm.dial(opts) {
                Ok(()) => debug!(%peer_id, %addr, "dialing"),
                Err(e) => {
                    debug!(%peer_id, error = %e, "dial rejected");
                    deliver_lifecycle(
                        task,
                        ConnEvent::DialFailure {
                            peer_id: Some(peer_id),
                            error: e.to_string(),
                        },
                    );
                }
            }
        }
        SwarmCommand::Goodbye { peer_id, reason } => {
            record_goodbye(task, peer_id, reason);
        }
        SwarmCommand::Disconnect { peer_id } => {
            let _ = task.swarm.disconnect_peer_id(peer_id);
            debug!(%peer_id, "disconnect command");
        }
        SwarmCommand::ClosePeer {
            peer_id,
            reason,
            ban,
        } => {
            // Atomic policy close (H2): Goodbye bookkeeping → disconnect → optional ban.
            record_goodbye(task, peer_id, reason);
            let _ = task.swarm.disconnect_peer_id(peer_id);
            if ban {
                task.swarm.behaviour_mut().allow_block.block_peer(peer_id);
                debug!(%peer_id, "close+ban peer");
            } else {
                debug!(%peer_id, "close peer");
            }
        }
        SwarmCommand::BlockPeer { peer_id } => {
            task.swarm.behaviour_mut().allow_block.block_peer(peer_id);
            debug!(%peer_id, "block peer (allow_block_list)");
        }
        SwarmCommand::UnblockPeer { peer_id } => {
            task.swarm.behaviour_mut().allow_block.unblock_peer(peer_id);
            debug!(%peer_id, "unblock peer");
        }
    }
}

fn record_goodbye(
    task: &mut SwarmTask,
    peer_id: cc_libp2p::PeerId,
    reason: crate::channels::GoodbyeReason,
) {
    // Wire format is CC-23b; assert the command path and count drops.
    task.goodbye_dropped = task.goodbye_dropped.saturating_add(1);
    debug!(
        %peer_id,
        reason = reason.as_u64(),
        dropped = task.goodbye_dropped,
        "goodbye command (wire handler CC-23b; dropped with counter)"
    );
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
///
/// Installs the eth2 Altair+ `message_id_fn` (SEC C1 / CC-22b) via
/// [`crate::gossip::ethereum_behaviour_config`].
pub fn build_host_swarm(
    keypair: cc_libp2p::Keypair,
) -> Result<Swarm<CcBehaviour>, HostBuildError> {
    build_host_swarm_with_config(keypair, crate::gossip::ethereum_behaviour_config())
}

/// Construct a swarm with an explicit [`cc_libp2p::BehaviourConfig`] (limits tests).
///
/// Callers that build their own config should still install an eth2 message-id
/// (see [`crate::gossip::ethereum_behaviour_config`]); bare
/// [`BehaviourConfig::default`](cc_libp2p::BehaviourConfig::default) already
/// embeds a secure eth2 default inside `cc_libp2p`, but the p2p-owned function
/// is the fixture source of truth.
pub fn build_host_swarm_with_config(
    keypair: cc_libp2p::Keypair,
    behaviour_cfg: cc_libp2p::BehaviourConfig,
) -> Result<Swarm<CcBehaviour>, HostBuildError> {
    let behaviour = CcBehaviour::new(&keypair, behaviour_cfg)
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
