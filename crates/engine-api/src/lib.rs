//! Engine API client: three-lane transport, JWT signer, health machine,
//! method adapters, getBlobsV2 fastpath, and the S1-A-06 in-process host.
//!
//! [`jwt.rs`](jwt.rs) stays crate-private so `JwtSecret` is not a crate-public
//! item (ADR-R-03).

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

pub mod api;
pub mod capabilities;
pub mod config;
pub mod errors;
pub mod fastpath;
pub mod methods;
pub mod metrics;
pub mod network_config;
pub mod state;
pub mod transport;
pub mod version;

// Signer lives here; module stays private so no crate-public `JwtSecret`.
#[allow(dead_code, unreachable_pub)]
mod jwt;

pub use api::{EngineApi, EngineBuildError, PreparedEngine};
pub use network_config::{
    NETWORK_CONFIG_MAX_FILE_BYTES, NetworkConfigError, load_network_chain_config,
    validate_network_config_path,
};

impl transport::EngineTransport {
    /// Load the JWT secret from [`config::EngineTransportConfig::jwt_secret_path`]
    /// and build the transport. `JwtSecret` stays crate-private (ADR-R-03).
    pub fn from_secret_path(
        cfg: &config::EngineTransportConfig,
        metrics: Option<metrics::EngineMetrics>,
    ) -> Result<Self, errors::EngineError> {
        let jwt = jwt::JwtSecret::load(&cfg.jwt_secret_path).map_err(|e| {
            errors::EngineError::Transport {
                detail: format!("JWT secret: {e}"),
            }
        })?;
        Self::new(cfg, jwt, metrics)
    }

    /// Test/constructor helper: 32-byte secret, no crate-public `JwtSecret`.
    #[must_use]
    pub fn from_secret_bytes(
        endpoint: impl Into<String>,
        secret: [u8; 32],
        timeouts: config::TransportTimeouts,
        soft_deadline: std::time::Duration,
        metrics: Option<metrics::EngineMetrics>,
    ) -> Self {
        Self::from_parts(
            endpoint,
            jwt::JwtSecret::from_bytes(secret),
            timeouts,
            soft_deadline,
            metrics,
        )
    }

    /// Like [`Self::new`], taking raw secret bytes instead of `JwtSecret`.
    pub fn from_config_secret_bytes(
        cfg: &config::EngineTransportConfig,
        secret: [u8; 32],
        metrics: Option<metrics::EngineMetrics>,
    ) -> Result<Self, errors::EngineError> {
        Self::new(cfg, jwt::JwtSecret::from_bytes(secret), metrics)
    }
}
