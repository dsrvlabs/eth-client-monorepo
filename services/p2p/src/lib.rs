//! `cc-p2p` library surface.
//!
//! - **CC-20b**: runtime skeleton — swarm task sole owner, persisted identity,
//!   supervisor, slot clock, §2.2 channel map
//! - **CC-20c**: peer manager — `PeerTable`, dial scheduler, backoff/ban,
//!   eviction, `Goodbye`-on-disconnect
//! - **CC-21a**: A-P2-4 ENR sequence probe + `EnrManager::apply` batching skeleton
//! - **CC-21b**: Epoch-aware fork digest (`fork_digest`) — no libp2p/discv5/I/O
//! - **CC-21c**: discv5 discovery task, ENR field encoders, predicates, dial queue
//! - **CC-22a**: Topic registry, Fulu topic strings, Steady→Overlap→Drain skeleton
//! - **CC-22b**: Message-id preimage + fixture, per-container SSZ max table, pre-decode check
//! - **CC-22c**: GossipSub `ScoringConfig`, two score spaces, IDONTWANT, penalty table
//! - **CC-22d**: block/column validators, seen/pending, one Verdict, single report site
//! - **CC-22e**: hostile-input harness (tests/hostile_input.rs) — panic-is-failure + counting alloc
//! - **CC-24a**: custody + sampling sets (`das::custody`), eight column subscriptions, custody-compatible metric
//! - **CC-23a**: req/resp shared half — SszSnappy framing, nine protocols, rate limits, scheduler
//! - **CC-24b**: KZG verify pool (`das::verify_pool`) — dedicated OS threads, §8.2 three steps, ADR P2-08 batching
//! - **CC-26a**: bounded backfill cache (1 GiB ceiling) + `earliest_available_slot` AtomicU64
//! - **CC-24c**: sampling tracker (`das::sampling`) — all-or-nothing `BTreeSet` equality, 64-task bound, `DataAvailable`
//! - **CC-27b**: chain-stream client, outstanding map, stall-then-shed, ChainView store
//! - **CC-29a**: Phase 2 metric family declarations (binary registers them)

#![allow(missing_docs)]

pub mod backfill;
pub mod chain_stream;
pub mod channels;
pub mod clock;
pub mod das;
pub mod discovery;
pub mod fault_mode;
pub mod fork_digest;
pub mod gossip;
pub mod host;
pub mod identity;
pub mod metrics;
pub mod peer_manager;
pub mod reqresp;
pub mod service;
pub mod supervisor;
pub mod verdict;

pub use verdict::{
    gossip_class_for_reason, is_late_import_reject, to_message_acceptance, Verdict,
};
