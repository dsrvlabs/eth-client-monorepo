//! `cc-p2p` library surface.
//!
//! - **CC-21a**: A-P2-4 ENR sequence probe + `EnrManager::apply` batching skeleton
//! - **CC-21b**: Epoch-aware fork digest (`fork_digest`) — no libp2p/discv5/I/O
//! - **CC-22a**: Topic registry, Fulu topic strings, Steady→Overlap→Drain skeleton
//! - **CC-29a**: Phase 2 metric family declarations (binary registers them)

#![allow(missing_docs)]

pub mod discovery;
pub mod fork_digest;
pub mod gossip;
pub mod metrics;
