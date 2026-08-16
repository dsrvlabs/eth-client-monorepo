//! `cc-chain` library surface.
//!
//! - **CC-18c**: resumable event bus (ring, cursor, fan-out)
//! - **CC-1C**: timing metrics (budgeted histograms, gauges, counters)
//! - **CC-18b**: dedicated core thread, import path, `ArcSwap<HeadSnapshot>`, residency
//! - **CC-1E**: batched `ApplyAttestations` (weight observed through `GetHead`)
//! - **CC-19a**: checkpoint fetch, provider fallback, verification
//! - **CC-1G**: `/eth/v1/config/spec` as a `BLOB_SCHEDULE` source
//! - **CC-27a**: `P2pStream` server, `ChainView` producer, `ArcSwap<EpochContext>`,
//!   `GetValidatorRecords`
//! - **CC-27c**: gossip-verify fast path — block acceptance before state transition
//! - **CC-24d**: DA seam substitution — [`da`] (`pending_da`) +
//!   [`cc_fork_choice::PeerDasAvailability`]; Phase-1 optimistic DA stub deleted
//! - **CC-36a**: third deferral outcome — [`pending_engine`] (64 / 8 slots),
//!   separate from `pending_da`
//! - **CC-38a**: block-branch fast-path trigger in [`da`] (template-sized
//!   only; no cell payload through chain)
//! - **S1-A-06**: E3 is a direct [`engine::DirectEngine`] call (no gRPC bridge)
//!
//! The binary (`main.rs`) binds first, then awaits local restore
//! ([`restore`] / CC-45b) for `restore_grace_seconds` before falling back to
//! checkpoint bootstrap when `checkpoint_providers` is configured (CC-19
//! demoted to fallback): self health SERVING while aggregate `""` stays
//! NOT_SERVING until the core is installed. Without providers and without a
//! restore the core is absent and fork-choice RPCs return `NOT_BOOTSTRAPPED`.
//! Tests construct a store and spawn the core via [`core::spawn_core_thread`].

#![allow(missing_docs)]

#[path = "../../../crates/chain-core/src/apply_attestations.rs"]
pub mod apply_attestations;
pub mod checkpoint_sync;
#[path = "../../../crates/chain-core/src/core.rs"]
pub mod core;
#[path = "../../../crates/chain-core/src/da.rs"]
pub mod da;
pub mod engine;
pub mod epoch_context;
#[path = "../../../crates/chain-core/src/events/mod.rs"]
pub mod events;
pub mod fcu_driver;
pub mod head;
#[path = "../../../crates/chain-core/src/import.rs"]
pub mod import;
pub mod invalidation;
pub mod liveness;
pub mod metrics;
pub mod p2p_stream;
#[path = "../../../crates/chain-core/src/pending_engine.rs"]
pub mod pending_engine;
#[path = "../../../crates/chain-core/src/residency.rs"]
pub mod residency;
pub mod restore;
pub mod service;
pub mod tick;

pub use apply_attestations::MAX_APPLY_ATTESTATIONS;
pub use checkpoint_sync::{
    BlobScheduleFromSpecError, BootstrapSummary, CheckpointBootstrapConfig, CheckpointClient,
    CheckpointError, FetchedCheckpoint, GenesisInfo, MAX_BLOCK_BYTES, MAX_JSON_BYTES,
    MAX_STATE_BYTES, NETWORK_RETRIES, PROVIDER_CONNECT_TIMEOUT, PROVIDER_TOTAL_TIMEOUT,
    REQUIRED_CONSENSUS_VERSION, TRIPLE_ATTEMPTS, blob_schedule_from_spec,
    blob_schedule_from_spec_map, bootstrap_core_from_providers,
    bootstrap_core_from_providers_with_epoch, cross_check_spec, fetch_checkpoint,
    parse_optional_root, spawn_core_from_checkpoint, spawn_core_from_checkpoint_with_epoch,
    validate_provider_base, verify_checkpoint, warm_canonical_root,
};
pub use core::{
    AttestationEnqueue, AttestationSender, AttestationWork, CoreCommand, CoreConfig, CoreHandle,
    CoreThread, IMPORT_SEND_TIMEOUT, ImportWork, MAX_VALIDATOR_PUBKEYS_PER_REQUEST,
    MAX_VALIDATOR_RECORDS_PER_REQUEST, QueryP0Work, QueryP1Work, QueryReply, QueryRequest,
    SHUTDOWN_JOIN_TIMEOUT, WakingSender, spawn_core_thread, spawn_core_thread_with_epoch,
};
pub use da::{
    BlockBranchTrigger, CELL_PAYLOAD_SOFT_MIN, DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
    DEFAULT_RECOVERY_MAX_ATTEMPTS, DEFAULT_RECOVERY_MAX_PEERS, DEFAULT_SECONDS_PER_SLOT,
    OutboundTriggerBytes, PENDING_DA_BOUND, PendingDa, PendingDaEntry, RESP_TIMEOUT_SECS,
    TEMPLATE_WIRE_SOFT_MAX, TTFB_TIMEOUT_SECS, assert_timeout_outlasts_recovery,
    block_branch_trigger_from_signed, chain_pending_timeout_secs, default_timeout_ordering_ok,
    kzg_commitment_to_versioned_hash, recovery_ladder_worst_case_secs,
    versioned_hashes_from_commitments,
};
pub use engine::{
    DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT, DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT,
    DEFAULT_ENGINE_GET_STATE_TIMEOUT, DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT, DirectEngine,
    SharedEngine,
};
pub use epoch_context::{EpochContext, EpochContextStore};
pub use events::{
    ChainReorgPayload, DEFAULT_RING_BYTES, DEFAULT_RING_CAPACITY,
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, ERROR_DOMAIN, EventInput, EventSubscription, EventsConfig,
    EventsHandle, FinalizedCheckpointPayload, MAX_EVENT_PAYLOAD_BYTES, Occupancy,
    REASON_CURSOR_TOO_OLD, REASON_CURSOR_UNKNOWN_SESSION, SESSION_ID_METADATA_KEY,
};
pub use fcu_driver::{
    FcuBuildError, FcuDriver, FcuSink, FcuSkip, ForkchoiceState, RecordingFcuSink,
    build_forkchoice_state, safe_is_ancestor_of_head,
};
pub use head::{HeadSnapshot, HeadSnapshotStore};
pub use import::{
    BLOCK_PAYLOAD_VERDICT_DEFERRED_DA, BLOCK_PAYLOAD_VERDICT_IMPORTED, BlockImportedPayloadVerdict,
    FORK_CHOICE_SCALARS_SSZ_LEN, ForkChoiceScalarsPayload, ImportCounters, ImportOutcome,
    ImportReason, block_imported_payload, common_ancestor_slot, decode_signed_block,
    encode_signed_block, fork_choice_scalars_ssz, import_block_with_early, late_import_flags,
    on_block_error_gossip_class, parse_root, split_block_imported_payload,
};
pub use invalidation::{ExitFn, handle_justified_checkpoint_invalidated, process_exit};
pub use liveness::{
    CONSECUTIVE_MISS_THRESHOLD, CONSECUTIVE_SUCCESS_THRESHOLD, ConsecutiveMissPolicy, CoreLiveness,
    DEFAULT_ATTESTATION_DUE_BPS, DEFAULT_SLOT_DURATION_MS, LivenessError, LivenessSink,
    SAMPLES_PER_SLOT, default_liveness_deadline, liveness_deadline, probe_core_liveness,
    run_core_liveness_loop, sample_interval, soft_deadline_ms,
};
pub use metrics::{
    AUX_DURATION_BUCKETS, BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BootstrapResult,
    BudgetOp, CI_BLOCK_CEILING_SECS, CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS,
    HashPath, ImportResult, ImportStage, OptimisticDirection, PROCESS_BLOCK_BUCKETS,
    PROCESS_EPOCH_BUCKETS,
};
pub use p2p_stream::{
    MAX_P2P_STREAM_SESSIONS, P2pStreamDeps, REASON_STREAM_SESSION_LIMIT, REASON_UNKNOWN_TOPIC,
    STREAM_OUTBOUND_CAPACITY, VIEW_KIND_EPOCH_TICK, VIEW_KIND_FULL, VIEW_KIND_HEAD_CHANGE,
    VIEW_KIND_SLOT_TICK, ViewTick, build_chain_view, validate_publish_topic,
};
pub use pending_engine::{
    DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, PENDING_ENGINE_BOUND, PendingEngine, PendingEngineEntry,
};
pub use residency::{
    BodyRingEntry, DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency,
    ResidencyError, ResidentRole, StateProvider,
};
pub use restore::{
    DEFAULT_RESTORE_GRACE_SECONDS, RestoreApplyInput, RestoreApplyResult, RestoreGate,
    RestoreGateOutcome, RestoreHandlerDeps, RestoreInstall, apply_restore_set,
    handle_restore_accumulated, handle_restore_from_store, reset_restore_da_gate_invocations,
    restore_da_gate_invocations, spawn_core_from_restore,
};
#[cfg(feature = "s0-a-31-observe")]
pub use restore::{
    RestoreTracePoint, reset_restore_trace, restore_force_raw_decode, restore_trace,
};
pub use service::{
    ChainServiceImpl, REASON_BELOW_FINALIZED_RETENTION, REASON_NOT_BOOTSTRAPPED,
    status_below_finalized,
};
pub use tick::{DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY, GossipClock};

/// p2p → chain handle. Named here so this edge is not a transport type.
pub type ChainIngressHandle = std::sync::Arc<dyn cc_seam::ChainIngress>;
/// chain → p2p handle. Named here so this edge is not a transport type.
pub type P2pEgressHandle = std::sync::Arc<dyn cc_seam::P2pEgress>;
/// Archive ingest handle. Named in `cc-chain-core`; never a storage type.
pub use cc_chain_core::ArchiveWriteHandle;
