//! Fork choice (Architecture §6, CC-15–CC-17).
//!
//! - CC-15a: proto-array, [`Store`] skeleton, [`on_tick`]
//! - CC-15b: [`on_block`], [`compute_pulled_up_tip`], [`CheckpointContext`] LRU
//! - CC-15c: [`get_head`], proposer boost as delta, head cache, reorg detection
//! - CC-16: [`on_attestation`], [`on_attester_slashing`], [`compute_deltas`]
//! - CC-17 / CC-24d: data-availability seam in [`da_seam`]
//!   ([`PeerDasAvailability`] substitutes the deleted Phase-1 optimistic stub)

#![allow(missing_docs)]

pub mod checkpoint_context;
pub mod da_seam;
pub mod head_cache;
pub mod on_attestation;
pub mod on_block;
pub mod on_tick;
pub mod proto_array;
pub mod store;

pub use checkpoint_context::{
    CheckpointContext, CheckpointContextKey, CommitteeCache, checkpoint_context_key,
};
pub use da_seam::{
    AVAILABLE_ROOTS_BOUND, BlockImport, DataAvailability, DeferralReason, HarnessAvailability,
    ImportedBlock, PeerDasAvailability,
};
pub use head_cache::{
    ChainReorg, GetHeadError, PROPOSER_SCORE_BOOST, compute_proposer_boost_score, get_head,
    get_proposer_head,
};
pub use on_attestation::{
    OnAttestationError, apply_attestation_deltas, compute_deltas, compute_deltas_call_count,
    on_attestation, on_attester_slashing, store_target_checkpoint_context, validate_on_attestation,
};
pub use on_block::{
    OnBlockError, compute_pulled_up_tip, get_checkpoint_block, get_forkchoice_store, on_block,
    record_block_timeliness, update_proposer_boost_root,
};
pub use on_tick::on_tick;
pub use proto_array::{
    PreviousProposerBoost, ProtoArray, ProtoArrayError, ProtoNode, ProtoNodeBlock,
};
pub use store::{
    CachedHead, DEFAULT_CHECKPOINT_CONTEXT_CAPACITY, LatestMessage, Store, StoreError, VoteTracker,
};
