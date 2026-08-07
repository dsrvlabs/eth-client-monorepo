//! Swarm construction over [`crate::build_transport`] + [`crate::CcBehaviour`].

use std::time::Duration;

use libp2p::PeerId;
use libp2p::identity::Keypair;
use libp2p::swarm::{self, Swarm};

use crate::behaviour::CcBehaviour;
use crate::limits::DEFAULT_IDLE_CONNECTION_TIMEOUT;
use crate::transport::{TransportBuildError, TransportConfig, build_transport};

/// Swarm-level config: transport knobs + idle timeout.
#[derive(Debug, Clone)]
pub struct SwarmConfig {
    pub transport: TransportConfig,
    /// How long to keep idle connections (default 30 s).
    pub idle_connection_timeout: Duration,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            transport: TransportConfig::default(),
            idle_connection_timeout: DEFAULT_IDLE_CONNECTION_TIMEOUT,
        }
    }
}

/// Errors from [`build_swarm`].
#[derive(Debug)]
pub enum SwarmBuildError {
    Transport(TransportBuildError),
}

impl std::fmt::Display for SwarmBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

impl std::error::Error for SwarmBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
        }
    }
}

impl From<TransportBuildError> for SwarmBuildError {
    fn from(value: TransportBuildError) -> Self {
        Self::Transport(value)
    }
}

/// Build a tokio-executor [`Swarm`] with the given behaviour and config.
pub fn build_swarm(
    keypair: Keypair,
    behaviour: CcBehaviour,
    cfg: &SwarmConfig,
) -> Result<Swarm<CcBehaviour>, SwarmBuildError> {
    let peer_id = PeerId::from_public_key(&keypair.public());
    let transport = build_transport(&keypair, &cfg.transport)?;
    let swarm_cfg = swarm::Config::with_tokio_executor()
        .with_idle_connection_timeout(cfg.idle_connection_timeout);
    Ok(Swarm::new(transport, behaviour, peer_id, swarm_cfg))
}
