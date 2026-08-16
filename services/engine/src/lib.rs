//! `cc-engine` library surface.
//!
//! - **CC-3Aa**: Phase 3 metric family declarations (binary registers them;
//!   observations land in the requirements that own each family)
//! - **CC-30a**: Engine API transport (three lanes, JWT, error taxonomy,
//!   soft deadline, secret loader) — offline half
//! - **CC-30b**: Container auth against real geth — `iat` skew pair, 403-vs-401
//!   typed errors, geth-format `crc32` line (`tests/auth_container.rs`)
//! - **CC-31**: Method set, fork gate (`method_for` on payload timestamp),
//!   `ADVERTISED_CAPABILITIES`, capability cache clear edges
//! - **CC-32b**: `NewPayload` / `ForkchoiceUpdated` / `GetEngineState` server,
//!   SSZ→JSON encode, `cc_engine_payload_status_total` observations
//! - **CC-33**: `forkchoiceUpdatedV3` adapter, sequence high-water (resets on
//!   reconnect), three-value payloadStatus decoder, `-38002`/`-38006` handling
//! - **CC-36a**: four-state engine machine, `eth_syncing` upcheck on the upcheck
//!   lane, detached+floored drive, fcU re-send on Synced edge
//! - **CC-37a**: `getBlobsV2` on the fastpath lane — two triggers (chain block /
//!   p2p column), single-flight by `beacon_block_root`, null-is-miss classification,
//!   runtime `max_blobs_per_block` bound (CC-1G), 1 s timeout off the ST thread
//! - **CC-37b**: `CellKzg::compute_cells` on `spawn_blocking`, zip with EL proofs,
//!   128-way transpose, inclusion proof from the block, subscribe-only filter
//!   before the process boundary (inject is CC-38)
//! - **CC-38a**: ninth contract engine side — `EngineStream` client (`inject`),
//!   Phase 2 §10.6 reconnect curve, `FetchBlobs` unary for the chain block branch,
//!   column branch down the reverse stream direction (p2p server is CC-38b)

#![allow(missing_docs)]

pub use cc_engine_api::capabilities;
pub use cc_engine_api::config;
pub use cc_engine_api::errors;
pub use cc_engine_api::fastpath;
pub use cc_engine_api::methods;
pub use cc_engine_api::metrics;
pub use cc_engine_api::state;
pub use cc_engine_api::transport;
pub use cc_engine_api::version;

pub mod inject;
// Private on cc-engine-api (ADR-R-03); same file so the type stays one impl.
#[path = "../../../crates/engine-api/src/jwt.rs"]
pub mod jwt;
pub mod service;

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
    TriggerOwner, hoodi_blob_bound, production_cell_kzg, reconstruct_and_filter,
    template_from_commitments,
};
pub use inject::{
    BACKOFF_CAP, BACKOFF_INITIAL, DecodedFetch, INBOUND_QUEUE_BOUND, INJECT_QUEUE_BOUND,
    InjectQueue, InjectStreamConfig, SIDECAR_TEMPLATE_SIZE_SOFT_MAX, decode_fetch_blobs_request,
    decode_wire_template, encode_fetch_blobs_request, encode_wire_template,
    fetch_blobs_request_wire_size, full_jitter, new_session_id, next_backoff,
    run_inject_stream_client, sidecar_template_within_size_budget, subscription_from_wire,
    subscription_to_wire, wait_reconnect_backoff,
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
pub use state::{
    CachedForkchoiceState, EngineState, EngineStateHandle, EngineStateInternal, EngineStateMachine,
    StateTransition, TransitionReason, UpcheckOutcome, admits_el_call, spawn_upcheck_driver,
};
