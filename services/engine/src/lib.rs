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

#![allow(missing_docs)]

pub mod capabilities;
pub mod config;
pub mod errors;
pub mod jwt;
pub mod methods;
pub mod metrics;
pub mod service;
pub mod state;
pub mod transport;
pub mod version;

pub use methods::eth_syncing::{EthSyncingResult, eth_syncing};
pub use methods::fcu::{
    FcuDroppedStale, FcuGatedError, FcuSequenceGate, build_fcu_params, decode_fcu_payload_status,
    decode_fcu_result, forkchoice_updated_v3, forkchoice_updated_v3_gated,
};
pub use service::REASON_FCU_DROPPED_STALE;
pub use state::{
    CachedForkchoiceState, EngineState, EngineStateHandle, EngineStateInternal, EngineStateMachine,
    StateTransition, TransitionReason, UpcheckOutcome, admits_el_call, spawn_upcheck_driver,
};
