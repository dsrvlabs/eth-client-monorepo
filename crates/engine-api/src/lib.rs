//! Engine API client: three-lane transport, JWT signer, and health machine.
//!
//! S1-A-02: [`config.rs`](config.rs) and [`transport.rs`](transport.rs) live here
//! (verbatim). `cc-engine` compiles them via `#[path]` so JWT / errors / metrics /
//! version / methods stay owned by `cc-engine` until S1-A-03 / A-05.
//!
//! Unit tests under this crate compile those two files against private
//! test-only companions (not `jwt.rs` and not a public `JwtSecret`).

#![cfg_attr(test, allow(dead_code, unreachable_pub))]

#[cfg(test)]
#[path = "test_errors.rs"]
mod errors;
#[cfg(test)]
#[path = "test_jwt.rs"]
mod jwt;
#[cfg(test)]
#[path = "test_methods.rs"]
mod methods;
#[cfg(test)]
#[path = "test_metrics.rs"]
mod metrics;
#[cfg(test)]
#[path = "test_version.rs"]
mod version;

#[cfg(test)]
#[path = "config.rs"]
mod config;
#[cfg(test)]
#[path = "transport.rs"]
mod transport;
