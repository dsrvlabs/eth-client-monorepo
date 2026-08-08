//! Persistent store library stub (Phase 4 store DAG).
//!
//! Engine, codecs, and schema land in later issues. This crate depends only on
//! `cc-types` in the workspace DAG permanently. Consensus container types must
//! not appear here; the store holds opaque bytes under typed keys.

#![allow(missing_docs)]

/// Placeholder so dependents can touch the crate until the real API lands.
pub const STUB_VERSION: u32 = 0;
