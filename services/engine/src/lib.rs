//! `cc-engine` library surface — thin host over [`cc_engine_api`].
//!
//! S1-A-06: transport, JWT, health, methods, fastpath, and the production
//! constructor live in `cc-engine-api`. This crate stays a workspace member
//! so the 4-container topology can still run for A/B (`[ARCH]` §9.1).

#![allow(missing_docs)]

pub use cc_engine_api::api::{EngineApi, EngineBuildError, PreparedEngine};
pub use cc_engine_api::capabilities;
pub use cc_engine_api::config;
pub use cc_engine_api::errors;
pub use cc_engine_api::fastpath;
pub use cc_engine_api::methods;
pub use cc_engine_api::metrics;
pub use cc_engine_api::network_config::{
    NETWORK_CONFIG_MAX_FILE_BYTES, NetworkConfigError, load_network_chain_config,
    validate_network_config_path,
};
pub use cc_engine_api::state;
pub use cc_engine_api::transport;
pub use cc_engine_api::version;

// Private on cc-engine-api (ADR-R-03); same file so container ITs can load a
// hex secret without making `JwtSecret` crate-public on the API crate.
#[path = "../../../crates/engine-api/src/jwt.rs"]
pub mod jwt;

pub use cc_proto::EngineRpcReason;
pub use fastpath::cells::{
    CellsError, ZippedBlobMaterial, compute_cells_zipped_with_el_proofs, parse_el_proofs,
};
pub use fastpath::fetch::{
    BlobBound, CountingSamplingTracker, FetchRequest, FetchResult, NullSamplingTracker,
    SamplingTrackerProbe, assert_request_length_within_bound, epoch_at_slot, fetch_blobs,
};
pub use fastpath::filter::{
    FILTERED_PAYLOAD_SOFT_MAX_BYTES, FilterOutcome, SubscriptionSet,
    UNFILTERED_PAYLOAD_ORDER_BYTES, filter_subscribed,
};
pub use fastpath::sidecars::{AssembleError, SidecarTemplate, transpose_to_sidecars};
pub use fastpath::{
    COMPLETED_LOG_BOUND, EnqueueOutcome, FASTPATH_QUEUE_BOUND, FastpathLane, InjectItem, Trigger,
    TriggerOwner, production_cell_kzg, reconstruct_and_filter, template_from_commitments,
};
pub use methods::eth_syncing::{EthSyncingResult, eth_syncing};
pub use methods::fcu::{
    FcuDroppedStale, FcuGatedError, FcuSequenceGate, build_fcu_params, decode_fcu_payload_status,
    decode_fcu_result, forkchoice_updated_v3, forkchoice_updated_v3_gated,
};
pub use methods::get_blobs::{
    BlobAndProofV2, GET_BLOBS_V2_MAX_HASHES, GetBlobsOutcome, NullCause, NullContext,
    VERSIONED_HASH_VERSION_KZG, build_get_blobs_v2_params, classify_null, get_blobs_v2,
    kzg_commitment_to_versioned_hash, versioned_hashes_from_commitments,
};
pub use methods::new_payload::DecodedPayloadStatus;
pub use state::{
    CachedForkchoiceState, EngineState, EngineStateHandle, EngineStateInternal, EngineStateMachine,
    StateTransition, TransitionReason, UpcheckOutcome, admits_el_call, spawn_upcheck_driver,
};
