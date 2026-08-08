//! Persistent store library (Phase 4 store DAG).
//!
//! Engine, codecs, and schema land in later issues. This crate depends only on
//! `cc-types` in the workspace DAG permanently. Consensus container types must
//! not appear here; the store holds opaque bytes under typed keys.
//!
//! [`buckets`] is Stream V's one file inside `crates/store` (CC-4Ca / Amendment 7):
//! histogram boundary arrays shared by `services/storage` and `bin/store-bench`.

#![allow(missing_docs)]

/// Histogram bucket boundaries for the storage metric surface (§10.2 / CC-4Ca).
pub mod buckets;

/// Placeholder so dependents can touch the crate until the real API lands.
pub const STUB_VERSION: u32 = 0;
