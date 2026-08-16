//! Engine API client: three-lane transport, JWT signer, health machine,
//! method adapters, and getBlobsV2 fastpath.
//!
//! S1-A-05: [`methods`] and [`fastpath`] live here (verbatim). `cc-engine`
//! re-exports them via `pub use`.
//! S1-A-03: [`jwt.rs`](jwt.rs) stays crate-private so `JwtSecret` is not a
//! crate-public item (ADR-R-03).

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

pub mod capabilities;
pub mod config;
pub mod errors;
pub mod fastpath;
pub mod methods;
pub mod state;
pub mod transport;
pub mod version;

// Signer lives here; module stays private so no crate-public `JwtSecret`.
#[allow(dead_code, unreachable_pub)]
mod jwt;

// Metric types stay in the service crate until A-06 deletes them; compiled
// here so methods/fastpath/transport can be real (not test-only) modules.
#[path = "../../../services/engine/src/metrics.rs"]
pub mod metrics;

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
