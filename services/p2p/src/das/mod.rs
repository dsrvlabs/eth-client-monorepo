//! PeerDAS custody / sampling surface — Architecture §8 / CC-24.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`custody`] | CC-24a | sampled + custodied sets, column-subnet subscription, custody-compatible peers |
//! | [`verify_pool`] | CC-24b | dedicated OS-thread KZG pool, §8.2 three steps, ADR P2-08 batching |
//! | [`sampling`] | CC-24c | per-root sampling tracker, all-or-nothing `BTreeSet` equality, DA emit |
//!
//! DA seam substitution (**CC-24d**) lands in a sibling module later. No req/resp
//! code lives here.

pub mod custody;
pub mod sampling;
pub mod verify_pool;

pub use custody::{
    count_custody_compatible_peers, is_peer_custody_compatible, column_subnets_for_groups,
    CustodiedGroups, CustodyManager, SampledGroups,
};
pub use sampling::{
    DeadlineClock, FixedDeadline, SamplingHandle, SamplingTask, SamplingTracker, TaskState,
    SAMPLING_TASK_BOUND,
};
pub use verify_pool::{
    pool_worker_count, run_verify_pool_bridge, verify_sidecar_three_steps, BatchMode,
    VerifyFailReason, VerifyJob, VerifyOutcome, VerifyPool, VerifyStep, VerifyStepCounters,
    CROSS_SIDECAR_BATCH_MIN, SAMPLING_P95_BUDGET_SECS, VERIFY_QUEUE_BOUND,
};
