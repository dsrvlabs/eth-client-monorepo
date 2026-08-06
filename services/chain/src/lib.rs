//! `cc-chain` library surface.
//!
//! - **CC-18c**: resumable event bus (ring, cursor, fan-out)
//! - **CC-1C**: timing metrics (budgeted histograms, gauges, counters)
//! - **CC-18b**: dedicated core thread, import path, `ArcSwap<HeadSnapshot>`, residency
//!
//! The binary (`main.rs`) serves gRPC; until CC-19 bootstrap the core is absent
//! and `ImportBlock` returns `NOT_BOOTSTRAPPED`. Tests construct a store and
//! spawn the core directly via [`core::spawn_core_thread`].

#![allow(missing_docs)]

pub mod core;
pub mod events;
pub mod head;
pub mod import;
pub mod metrics;
pub mod residency;
pub mod service;

pub use core::{
    COMMAND_CHANNEL_CAPACITY, CoreCommand, CoreConfig, CoreHandle, CoreThread, IMPORT_SEND_TIMEOUT,
    QueryReply, SHUTDOWN_JOIN_TIMEOUT, spawn_core_thread,
};
pub use events::{
    DEFAULT_RING_CAPACITY, DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, ERROR_DOMAIN, EventInput,
    EventSubscription, EventsConfig, EventsHandle, Occupancy, REASON_CURSOR_TOO_OLD,
    REASON_CURSOR_UNKNOWN_SESSION,
};
pub use head::{HeadSnapshot, HeadSnapshotStore};
pub use import::{
    ImportCounters, ImportOutcome, decode_signed_block, encode_signed_block, parse_root,
};
pub use metrics::{
    AUX_DURATION_BUCKETS, BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BudgetOp,
    CI_BLOCK_CEILING_SECS, CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS, HashPath,
    ImportResult, ImportStage, PROCESS_BLOCK_BUCKETS, PROCESS_EPOCH_BUCKETS,
};
pub use residency::{
    BodyRingEntry, DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency,
    ResidencyError, ResidentRole, StateProvider,
};
pub use service::{ChainServiceImpl, REASON_NOT_BOOTSTRAPPED};
