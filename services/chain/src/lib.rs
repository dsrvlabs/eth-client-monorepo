//! `cc-chain` library surface.
//!
//! Phase 1 lands the events bus (CC-18c) here so integration tests can drive it with
//! synthetic events without linking `cc-fork-choice` or `cc-state-transition` (R-11).
//! The binary (`main.rs`) remains the Phase 0 gRPC stub until CC-18b wires the core.

#![allow(missing_docs)]

pub mod events;

pub use events::{
    EventInput, EventSubscription, EventsConfig, EventsHandle, Occupancy, DEFAULT_RING_CAPACITY,
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, ERROR_DOMAIN, REASON_CURSOR_TOO_OLD,
    REASON_CURSOR_UNKNOWN_SESSION,
};
