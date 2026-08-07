//! Backfill cache and serve window — Architecture §9.5 / §9.6 / CC-26a.
//!
//! | Module | Role |
//! |--------|------|
//! | [`cache`] | Two-map bounded cache, 1 GiB ceiling, oldest-first eviction |
//! | [`window`] | `earliest_available_slot` as one `AtomicU64` (ADR P2-14) |
//!
//! Gap detection and the range planner are **CC-26b** (out of scope here).

pub mod cache;
pub mod window;

pub use cache::{
    BackfillCache, InsertOutcome, CACHE_BLOCK_COUNT_BOUND, CACHE_BOUND_BYTES,
    CACHE_COLUMN_COUNT_BOUND, CACHE_EMPTY_SLOT_BOUND, CACHE_WINDOW_WALK_DEPTH, MAX_SAMPLED,
};
pub use window::{compute_earliest_available_slot, ServeWindow, EMPTY_WINDOW_SLOT};
