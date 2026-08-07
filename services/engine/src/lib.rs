//! `cc-engine` library surface.
//!
//! - **CC-3Aa**: Phase 3 metric family declarations (binary registers them;
//!   observations land in the requirements that own each family)
//! - **CC-30a**: Engine API transport (three lanes, JWT, error taxonomy,
//!   soft deadline, secret loader) — offline half
//! - **CC-30b**: Container auth against real geth — `iat` skew pair, 403-vs-401
//!   typed errors, geth-format `crc32` line (`tests/auth_container.rs`)

#![allow(missing_docs)]

pub mod config;
pub mod errors;
pub mod jwt;
pub mod methods;
pub mod metrics;
pub mod transport;
