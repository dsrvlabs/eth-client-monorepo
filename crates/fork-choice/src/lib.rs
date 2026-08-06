//! Fork choice (Architecture §6, CC-15–CC-17).
//!
//! - CC-15a: proto-array, [`Store`] skeleton, [`on_tick`]
//! - CC-15b: [`on_block`], [`compute_pulled_up_tip`], [`CheckpointContext`] LRU
//! - CC-17: data-availability seam in [`da_seam`]
//!
//! Attestation handlers (CC-16), proposer boost / `get_head`, and the vector
//! runner / green declaration (CC-15c) are successor issues.

#![allow(missing_docs)]

pub mod checkpoint_context;
pub mod da_seam;
pub mod on_block;
pub mod on_tick;
pub mod proto_array;
pub mod store;

pub use checkpoint_context::{
    CheckpointContext, CheckpointContextKey, CommitteeCache, checkpoint_context_key,
};
pub use da_seam::{AlwaysAvailable, BlockImport, DataAvailability, DeferralReason, ImportedBlock};
pub use on_block::{
    OnBlockError, compute_pulled_up_tip, get_checkpoint_block, get_forkchoice_store, on_block,
};
pub use on_tick::on_tick;
pub use proto_array::{ProtoArray, ProtoArrayError, ProtoNode, ProtoNodeBlock};
pub use store::{
    CachedHead, DEFAULT_CHECKPOINT_CONTEXT_CAPACITY, LatestMessage, Store, StoreError,
};
