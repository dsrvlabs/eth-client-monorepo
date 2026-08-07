//! `cc-p2p` library surface.
//!
//! - **CC-20b**: runtime skeleton — swarm task sole owner, persisted identity,
//!   supervisor, slot clock, §2.2 channel map
//! - **CC-20c**: peer manager — `PeerTable`, dial scheduler, backoff/ban,
//!   eviction, `Goodbye`-on-disconnect
//! - **CC-21a**: A-P2-4 ENR sequence probe + `EnrManager::apply` batching skeleton
//! - **CC-21b**: Epoch-aware fork digest (`fork_digest`) — no libp2p/discv5/I/O
//! - **CC-22a**: Topic registry, Fulu topic strings, Steady→Overlap→Drain skeleton
//! - **CC-29a**: Phase 2 metric family declarations (binary registers them)

#![allow(missing_docs)]

pub mod channels;
pub mod clock;
pub mod discovery;
pub mod fault_mode;
pub mod fork_digest;
pub mod gossip;
pub mod host;
pub mod identity;
pub mod metrics;
pub mod peer_manager;
pub mod service;
pub mod supervisor;
