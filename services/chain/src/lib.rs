//! `cc-chain` library surface.
//!
//! Phase 1 lands the events bus (CC-18c) and timing metrics (CC-1C) here so integration
//! tests can drive them without linking `cc-fork-choice` or full import (R-11).
//! The binary (`main.rs`) remains the Phase 0 gRPC stub until CC-18b wires the core;
//! it already registers [`metrics::ChainMetrics`] into the bootstrap registry.

#![allow(missing_docs)]

pub mod events;
pub mod metrics;

pub use events::{
    DEFAULT_RING_CAPACITY, DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, ERROR_DOMAIN, EventInput,
    EventSubscription, EventsConfig, EventsHandle, Occupancy, REASON_CURSOR_TOO_OLD,
    REASON_CURSOR_UNKNOWN_SESSION,
};
pub use metrics::{
    AUX_DURATION_BUCKETS, BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BudgetOp,
    CI_BLOCK_CEILING_SECS, CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS, HashPath,
    ImportResult, ImportStage, PROCESS_BLOCK_BUCKETS, PROCESS_EPOCH_BUCKETS,
};
