//! Backfill cache, serve window, and range planner — Architecture §9 / CC-26.
//!
//! | Module | Role |
//! |--------|------|
//! | [`cache`] | Two-map bounded cache, 1 GiB ceiling, oldest-first eviction (CC-26a) |
//! | [`window`] | `earliest_available_slot` as one `AtomicU64` (ADR P2-14 / CC-48 one-writer) |
//! | [`planner`] | Gap detection, ≤64-slot batches, oldest-first import, no DA bypass (CC-26b) |

pub mod cache;
pub mod planner;
pub mod window;

pub use cache::{
    BackfillCache, InsertOutcome, CACHE_BLOCK_COUNT_BOUND, CACHE_BOUND_BYTES,
    CACHE_COLUMN_COUNT_BOUND, CACHE_EMPTY_SLOT_BOUND, CACHE_WINDOW_WALK_DEPTH, MAX_SAMPLED,
};
pub use planner::{
    batch_timeout, da_deferred_count, eligible_peers, expected_chunks, feed_backfill_to_sampling,
    parent_linkage_walk, plan_batches, BackfillDaResult, BackfillPeer, BackfillPlanner, BatchFetch,
    BatchPlan, BatchState, BatchStatus, CompletionStatus, FetchedBlock, FetchedColumn, GapDetected,
    GapDetector, GapTrigger, ImportReady, RecordedGap, BATCH_MAX_PEER_ATTEMPTS, BATCH_SLOT_LIMIT,
    BATCH_TIMEOUT_CAP, CLOCK_STALL_THRESHOLD, HEAD_JUMP_THRESHOLD, MAX_CONCURRENT_BATCHES,
    PEER_STATUS_GAP_THRESHOLD,
};
pub use window::{compute_earliest_available_slot, ServeWindow, EMPTY_WINDOW_SLOT};
