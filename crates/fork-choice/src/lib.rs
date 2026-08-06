//! Fork choice (Architecture §6, CC-15–CC-17).
//!
//! - CC-15a: proto-array, [`Store`] skeleton, [`on_tick`]
//! - CC-17: data-availability seam in [`da_seam`]
//!
//! `on_block` (CC-15b), attestation handlers (CC-16), and the vector runner /
//! green declaration (CC-15c) are successor issues.

#![allow(missing_docs)]

pub mod da_seam;
pub mod on_tick;
pub mod proto_array;
pub mod store;

pub use da_seam::{
    AlwaysAvailable, BlockImport, DataAvailability, DeferralReason, ImportedBlock,
};
pub use on_tick::on_tick;
pub use proto_array::{ProtoArray, ProtoArrayError, ProtoNode, ProtoNodeBlock};
pub use store::{
    CachedHead, CheckpointContext, LatestMessage, Store, StoreError,
    DEFAULT_CHECKPOINT_CONTEXT_CAPACITY,
};
