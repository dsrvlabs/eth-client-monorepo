//! `cc-chain` library surface.
//!
//! - **CC-18c**: resumable event bus (ring, cursor, fan-out)
//! - **CC-1C**: timing metrics (budgeted histograms, gauges, counters)
//! - **CC-18b**: dedicated core thread, import path, `ArcSwap<HeadSnapshot>`, residency
//! - **CC-1E**: batched `ApplyAttestations` (weight observed through `GetHead`)
//! - **CC-19a**: checkpoint fetch, provider fallback, verification
//!
//! The binary (`main.rs`) serves gRPC. When `checkpoint_providers` is configured
//! the core is spawned from a verified anchor (CC-19a); full lifecycle/health
//! is CC-19b. Without providers the core is absent and `ImportBlock` /
//! `ApplyAttestations` return `NOT_BOOTSTRAPPED`. Tests construct a store and
//! spawn the core directly via [`core::spawn_core_thread`].

#![allow(missing_docs)]

pub mod apply_attestations;
pub mod checkpoint_sync;
pub mod core;
pub mod events;
pub mod head;
pub mod import;
pub mod metrics;
pub mod residency;
pub mod service;

pub use apply_attestations::MAX_APPLY_ATTESTATIONS;
pub use checkpoint_sync::{
    BootstrapSummary, CheckpointBootstrapConfig, CheckpointClient, CheckpointError,
    FetchedCheckpoint, GenesisInfo, MAX_BLOCK_BYTES, MAX_JSON_BYTES, MAX_STATE_BYTES,
    NETWORK_RETRIES, PROVIDER_CONNECT_TIMEOUT, PROVIDER_TOTAL_TIMEOUT, REQUIRED_CONSENSUS_VERSION,
    TRIPLE_ATTEMPTS, bootstrap_core_from_providers, cross_check_spec, fetch_checkpoint,
    parse_optional_root, spawn_core_from_checkpoint, validate_provider_base, verify_checkpoint,
};
pub use core::{
    COMMAND_CHANNEL_CAPACITY, CoreCommand, CoreConfig, CoreHandle, CoreThread, IMPORT_SEND_TIMEOUT,
    MAX_VALIDATOR_PUBKEYS_PER_REQUEST, QueryReply, QueryRequest, SHUTDOWN_JOIN_TIMEOUT,
    spawn_core_thread,
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
    AUX_DURATION_BUCKETS, BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BootstrapResult,
    BudgetOp, CI_BLOCK_CEILING_SECS, CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS,
    HashPath, ImportResult, ImportStage, PROCESS_BLOCK_BUCKETS, PROCESS_EPOCH_BUCKETS,
};
pub use residency::{
    BodyRingEntry, DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency,
    ResidencyError, ResidentRole, StateProvider,
};
pub use service::{ChainServiceImpl, REASON_NOT_BOOTSTRAPPED};
