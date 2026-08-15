//! Shared work-scheduler substrate ([ARCH] §3.1, S0-A-13).
//!
//! Typed FIFO/LIFO queues, a lane registry, queue-type and depth configuration,
//! and a manager whose first-match-wins selection chain is data — the order of
//! that chain is the policy. No producers or consumers are wired here.
//!
//! `max_workers` is a conceptual cap (Loop B stays 1: one `Store`, one thread).
//! This crate never starts worker threads.

mod chain;
mod config;
mod manager;
mod queue;

pub use chain::{ChainLane, ChainWork, LOOP_B_LANES};
pub use config::{
    DEFAULT_MAX_WORKERS, Depth, IMPORT_LANE_DEPTH, INBOUND_POLL_CHAIN, InboundClass, LaneKey,
    LaneSpec, MIN_QUEUE_LEN, OVERPROVISION_PCT, QUERY_P0_LANE_DEPTH, QUERY_P1_LANE_DEPTH,
    QueueKind, QueueSizes, TICK_LANE_DEPTH, sized_from_validators,
};
pub use manager::{
    ConfigError, Enqueue, LaneSnapshot, Manager, ManagerConfig, Selected, WorkSource,
};
pub use queue::{FifoQueue, LifoQueue};
