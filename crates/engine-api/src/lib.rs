//! Engine API client: three-lane transport, JWT signer, and health machine.
//!
//! S1-A-03: [`jwt.rs`](jwt.rs), [`version.rs`](version.rs), [`errors.rs`](errors.rs),
//! and [`capabilities.rs`](capabilities.rs) live here (verbatim). `JwtSecret`
//! is not a crate-public item (ADR-R-03).
//! S1-A-02/A-04: [`config.rs`](config.rs), [`transport.rs`](transport.rs), and
//! [`state.rs`](state.rs) live here; `cc-engine` still `#[path]`s them.

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

pub mod capabilities;

// Signer lives here; module stays private so no crate-public `JwtSecret`.
#[allow(dead_code, unreachable_pub)]
mod jwt;

#[cfg(test)]
#[path = "test_methods.rs"]
mod methods;
#[cfg(test)]
#[path = "test_metrics.rs"]
mod metrics;

#[cfg(test)]
mod errors;
#[cfg(test)]
mod version;

#[cfg(test)]
#[path = "config.rs"]
mod config;
#[cfg(test)]
#[path = "state.rs"]
mod state;
#[cfg(test)]
#[path = "transport.rs"]
mod transport;
