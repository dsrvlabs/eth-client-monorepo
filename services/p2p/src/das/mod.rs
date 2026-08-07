//! PeerDAS custody / sampling / recovery surface — Architecture §8 / CC-24–25.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | [`custody`] | CC-24a | sampled + custodied sets, column-subnet subscription, custody-compatible peers |
//! | [`verify_pool`] | CC-24b | dedicated OS-thread KZG pool, §8.2 three steps, ADR P2-08 batching |
//! | [`sampling`] | CC-24c | per-root sampling tracker, all-or-nothing `BTreeSet` equality, DA emit |
//! | [`recovery`] | CC-25 | by-root recovery ladder at end of slot *N*, `custody_unserved` |
//!
//! DA seam substitution (**CC-24d**) lives in `services/chain`. Req/resp wire
//! handlers for columns are CC-23d; recovery consumes the protocol ID + scheduler.

pub mod custody;
pub mod recovery;
pub mod sampling;
pub mod verify_pool;

pub use custody::{
    count_custody_compatible_peers, is_peer_custody_compatible, column_subnets_for_groups,
    CustodiedGroups, CustodyManager, SampledGroups,
};
pub use recovery::{
    apply_custody_unserved, batch_for_peer, candidates_for_column, candidates_for_missing,
    chain_timeout_outlasts_ladder, default_ladder_under_chain_timeout, encode_single_root_request,
    log_abandoned, peer_custody_columns, peer_custodies_column, plan_batched_requests, recover,
    recovery_ladder_worst_case_secs, recovery_request_spec, PeerQueryResult, PlannedRequest,
    RecoveryInput, RecoveryOutcome, RecoveryPeer, RecoveryPenalty, CHAIN_PENDING_DA_TIMEOUT_SLOTS,
    RECOVERY_MAX_ATTEMPTS, RECOVERY_MAX_PEERS, RECOVERY_RESP_SECS, RECOVERY_TTFB_SECS,
};
pub use sampling::{
    DeadlineClock, FixedDeadline, RecoveryTrigger, SamplingHandle, SamplingTask, SamplingTracker,
    TaskState, SAMPLING_TASK_BOUND,
};
pub use verify_pool::{
    pool_worker_count, run_verify_pool_bridge, verify_sidecar_three_steps, BatchMode,
    VerifyFailReason, VerifyJob, VerifyOutcome, VerifyPool, VerifyStep, VerifyStepCounters,
    CROSS_SIDECAR_BATCH_MIN, SAMPLING_P95_BUDGET_SECS, VERIFY_QUEUE_BOUND,
};
