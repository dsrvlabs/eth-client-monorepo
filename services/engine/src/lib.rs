//! `cc-engine` library surface.
//!
//! - **CC-3Aa**: Phase 3 metric family declarations (binary registers them;
//!   observations land in the requirements that own each family)
//! - **CC-30a**: Engine API transport (three lanes, JWT, error taxonomy,
//!   soft deadline, secret loader) — offline half; no container tests

#![allow(missing_docs)]

pub mod config;
pub mod errors;
pub mod jwt;
pub mod methods;
pub mod metrics;
pub mod transport;
