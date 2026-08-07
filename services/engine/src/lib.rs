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

#![allow(missing_docs)]

pub mod capabilities;
pub mod config;
pub mod errors;
pub mod fastpath;
pub mod jwt;
pub mod methods;
pub mod metrics;
pub mod service;
pub mod state;
pub mod transport;
pub mod version;

pub use fastpath::{
    COMPLETED_LOG_BOUND, EnqueueOutcome, FASTPATH_QUEUE_BOUND, FastpathLane, Trigger, TriggerOwner,
    hoodi_blob_bound,
};
pub use fastpath::fetch::{
    BlobBound, CountingSamplingTracker, FetchRequest, FetchResult, NullSamplingTracker,
    SamplingTrackerProbe, assert_request_length_within_bound, epoch_at_slot, fetch_blobs,
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
pub use service::REASON_FCU_DROPPED_STALE;
pub use state::{
    CachedForkchoiceState, EngineState, EngineStateHandle, EngineStateInternal, EngineStateMachine,
    StateTransition, TransitionReason, UpcheckOutcome, admits_el_call, spawn_upcheck_driver,
};
