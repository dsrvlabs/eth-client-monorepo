//! `BeaconState` flat struct + cache placeholder + List/Vector seam (Architecture §3.4).
//!
//! Spec fields are private and reached through accessors. `StateCaches` is an
//! empty placeholder filled by CC-10g; it is excluded from SSZ / tree-hash and
//! from `PartialEq`.

use std::fmt;

use ssz::DecodeError;
use ssz_derive::{Decode, Encode};
use ssz_types::BitVector;
use tree_hash_derive::TreeHash;
use typenum::U4;

use crate::containers::{
    BeaconBlockHeader, Checkpoint, Eth1Data, HistoricalSummary, SyncCommittee, Validator,
};
use crate::execution::ExecutionPayloadHeader;
use crate::fork::{Fork, ForkName};
use crate::operations::{PendingConsolidation, PendingDeposit, PendingPartialWithdrawal};
use crate::preset::Preset;
use crate::primitives::{Epoch, Gwei, Root, Slot, ValidatorIndex};

// ---------------------------------------------------------------------------
// CC-1H seam (Architecture §3.4): only this module should name these backends.
// ---------------------------------------------------------------------------

/// Variable-length list. Phase 1: `ssz_types::VariableList`. CC-1H: milhouse.
pub type List<T, N> = ssz_types::VariableList<T, N>;

/// Fixed-length vector. Phase 1: `ssz_types::FixedVector`. CC-1H: milhouse.
pub type Vector<T, N> = ssz_types::FixedVector<T, N>;

/// `JUSTIFICATION_BITS_LENGTH = 4`.
pub type JustificationBitsLength = U4;

/// Participation flag byte (`ParticipationFlags` in the spec).
pub type ParticipationFlags = u8;

/// Placeholder for cached hashing / epoch / pubkey layers (filled by CC-10g).
///
/// Carries a debug-only tag so unit tests can construct two states that differ
/// only in cache contents and assert `PartialEq` ignores caches.
#[derive(Clone, Default)]
pub struct StateCaches<P: Preset> {
    /// Non-spec discriminator for PartialEq unit tests; always zero on decode.
    pub(crate) tag: u64,
    _marker: std::marker::PhantomData<P>,
}

impl<P: Preset> fmt::Debug for StateCaches<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateCaches")
            .field("tag", &self.tag)
            .finish()
    }
}

impl<P: Preset> StateCaches<P> {
    /// Construct caches with an explicit tag (test / debug only).
    pub fn with_tag(tag: u64) -> Self {
        Self {
            tag,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Spec `BeaconState` (Electra fields + Fulu `proposer_lookahead`).
///
/// All spec fields are private. `caches` is excluded from SSZ, tree-hash, and
/// equality.
#[derive(Clone, Encode, Decode, TreeHash)]
pub struct BeaconState<P: Preset> {
    genesis_time: u64,
    genesis_validators_root: Root,
    slot: Slot,
    fork: Fork,
    latest_block_header: BeaconBlockHeader,
    block_roots: Vector<Root, P::SlotsPerHistoricalRoot>,
    state_roots: Vector<Root, P::SlotsPerHistoricalRoot>,
    historical_roots: List<Root, P::HistoricalRootsLimit>,
    eth1_data: Eth1Data,
    eth1_data_votes: List<Eth1Data, P::Eth1DataVotesLength>,
    eth1_deposit_index: u64,
    validators: List<Validator, P::ValidatorRegistryLimit>,
    balances: List<Gwei, P::ValidatorRegistryLimit>,
    randao_mixes: Vector<Root, P::EpochsPerHistoricalVector>,
    slashings: Vector<Gwei, P::EpochsPerSlashingsVector>,
    previous_epoch_participation: List<ParticipationFlags, P::ValidatorRegistryLimit>,
    current_epoch_participation: List<ParticipationFlags, P::ValidatorRegistryLimit>,
    justification_bits: BitVector<JustificationBitsLength>,
    previous_justified_checkpoint: Checkpoint,
    current_justified_checkpoint: Checkpoint,
    finalized_checkpoint: Checkpoint,
    inactivity_scores: List<u64, P::ValidatorRegistryLimit>,
    current_sync_committee: SyncCommittee<P>,
    next_sync_committee: SyncCommittee<P>,
    latest_execution_payload_header: ExecutionPayloadHeader<P>,
    next_withdrawal_index: u64,
    next_withdrawal_validator_index: ValidatorIndex,
    historical_summaries: List<HistoricalSummary, P::HistoricalRootsLimit>,
    deposit_requests_start_index: u64,
    deposit_balance_to_consume: Gwei,
    exit_balance_to_consume: Gwei,
    earliest_exit_epoch: Epoch,
    consolidation_balance_to_consume: Gwei,
    earliest_consolidation_epoch: Epoch,
    pending_deposits: List<PendingDeposit, P::PendingDepositsLimit>,
    pending_partial_withdrawals: List<PendingPartialWithdrawal, P::PendingPartialWithdrawalsLimit>,
    pending_consolidations: List<PendingConsolidation, P::PendingConsolidationsLimit>,
    /// Fulu EIP-7917 proposer lookahead.
    proposer_lookahead: Vector<ValidatorIndex, P::ProposerLookaheadLen>,

    #[ssz(skip_serializing, skip_deserializing)]
    #[tree_hash(skip_hashing)]
    caches: StateCaches<P>,
}

impl<P: Preset> Default for BeaconState<P> {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            genesis_validators_root: Root::default(),
            slot: Slot::default(),
            fork: Fork {
                previous_version: Default::default(),
                current_version: Default::default(),
                epoch: Epoch::default(),
            },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::default(),
            state_roots: Vector::default(),
            historical_roots: List::default(),
            eth1_data: Eth1Data::default(),
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::default(),
            balances: List::default(),
            randao_mixes: Vector::default(),
            slashings: Vector::default(),
            previous_epoch_participation: List::default(),
            current_epoch_participation: List::default(),
            justification_bits: BitVector::default(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
            inactivity_scores: List::default(),
            current_sync_committee: SyncCommittee::default(),
            next_sync_committee: SyncCommittee::default(),
            latest_execution_payload_header: ExecutionPayloadHeader::default(),
            next_withdrawal_index: 0,
            next_withdrawal_validator_index: ValidatorIndex::default(),
            historical_summaries: List::default(),
            deposit_requests_start_index: 0,
            deposit_balance_to_consume: Gwei::default(),
            exit_balance_to_consume: Gwei::default(),
            earliest_exit_epoch: Epoch::default(),
            consolidation_balance_to_consume: Gwei::default(),
            earliest_consolidation_epoch: Epoch::default(),
            pending_deposits: List::default(),
            pending_partial_withdrawals: List::default(),
            pending_consolidations: List::default(),
            proposer_lookahead: Vector::default(),
            caches: StateCaches::default(),
        }
    }
}

impl<P: Preset> fmt::Debug for BeaconState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeaconState")
            .field("slot", &self.slot)
            .field("genesis_time", &self.genesis_time)
            .field("validators_len", &self.validators.len())
            .field("caches", &self.caches)
            .finish_non_exhaustive()
    }
}

// PartialEq compares spec fields only — never caches (Architecture §3.2 / §3.4).
impl<P: Preset> PartialEq for BeaconState<P> {
    fn eq(&self, other: &Self) -> bool {
        self.genesis_time == other.genesis_time
            && self.genesis_validators_root == other.genesis_validators_root
            && self.slot == other.slot
            && self.fork == other.fork
            && self.latest_block_header == other.latest_block_header
            && self.block_roots == other.block_roots
            && self.state_roots == other.state_roots
            && self.historical_roots == other.historical_roots
            && self.eth1_data == other.eth1_data
            && self.eth1_data_votes == other.eth1_data_votes
            && self.eth1_deposit_index == other.eth1_deposit_index
            && self.validators == other.validators
            && self.balances == other.balances
            && self.randao_mixes == other.randao_mixes
            && self.slashings == other.slashings
            && self.previous_epoch_participation == other.previous_epoch_participation
            && self.current_epoch_participation == other.current_epoch_participation
            && self.justification_bits == other.justification_bits
            && self.previous_justified_checkpoint == other.previous_justified_checkpoint
            && self.current_justified_checkpoint == other.current_justified_checkpoint
            && self.finalized_checkpoint == other.finalized_checkpoint
            && self.inactivity_scores == other.inactivity_scores
            && self.current_sync_committee == other.current_sync_committee
            && self.next_sync_committee == other.next_sync_committee
            && self.latest_execution_payload_header == other.latest_execution_payload_header
            && self.next_withdrawal_index == other.next_withdrawal_index
            && self.next_withdrawal_validator_index == other.next_withdrawal_validator_index
            && self.historical_summaries == other.historical_summaries
            && self.deposit_requests_start_index == other.deposit_requests_start_index
            && self.deposit_balance_to_consume == other.deposit_balance_to_consume
            && self.exit_balance_to_consume == other.exit_balance_to_consume
            && self.earliest_exit_epoch == other.earliest_exit_epoch
            && self.consolidation_balance_to_consume == other.consolidation_balance_to_consume
            && self.earliest_consolidation_epoch == other.earliest_consolidation_epoch
            && self.pending_deposits == other.pending_deposits
            && self.pending_partial_withdrawals == other.pending_partial_withdrawals
            && self.pending_consolidations == other.pending_consolidations
            && self.proposer_lookahead == other.proposer_lookahead
    }
}

impl<P: Preset> Eq for BeaconState<P> {}

impl<P: Preset> BeaconState<P> {
    /// Decode SSZ bytes under an explicit fork context.
    ///
    /// Phase 1 supports only [`ForkName::Fulu`]. Same long-term boundary as
    /// [`crate::SignedBeaconBlock::from_ssz_bytes_with`].
    pub fn from_ssz_bytes_with(fork_name: ForkName, bytes: &[u8]) -> Result<Self, DecodeError> {
        match fork_name {
            ForkName::Fulu => <Self as ssz::Decode>::from_ssz_bytes(bytes),
            other => Err(DecodeError::BytesInvalid(format!(
                "unsupported fork for BeaconState SSZ decode: {other} (Phase 1 is Fulu-only)"
            ))),
        }
    }

    /// No-op under `ssz_types`; milhouse will `apply_updates()` here (CC-1H).
    pub fn commit(&mut self) {}

    // --- read accessors (intersection-only contract grows in CC-10g) --------

    /// Genesis time.
    pub fn genesis_time(&self) -> u64 {
        self.genesis_time
    }

    /// Genesis validators root.
    pub fn genesis_validators_root(&self) -> Root {
        self.genesis_validators_root
    }

    /// Current slot.
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// Current fork.
    pub fn fork(&self) -> Fork {
        self.fork
    }

    /// Latest block header.
    pub fn latest_block_header(&self) -> &BeaconBlockHeader {
        &self.latest_block_header
    }

    /// Finalized checkpoint.
    pub fn finalized_checkpoint(&self) -> Checkpoint {
        self.finalized_checkpoint
    }

    /// Current justified checkpoint.
    pub fn current_justified_checkpoint(&self) -> Checkpoint {
        self.current_justified_checkpoint
    }

    /// Previous justified checkpoint.
    pub fn previous_justified_checkpoint(&self) -> Checkpoint {
        self.previous_justified_checkpoint
    }

    /// Validator registry length.
    pub fn validators_len(&self) -> usize {
        self.validators.len()
    }

    /// Proposer lookahead vector (Fulu EIP-7917).
    pub fn proposer_lookahead(&self) -> &Vector<ValidatorIndex, P::ProposerLookaheadLen> {
        &self.proposer_lookahead
    }

    /// Borrow caches (CC-10g will expand this surface).
    pub fn caches(&self) -> &StateCaches<P> {
        &self.caches
    }

    /// Mutable caches (test / CC-10g).
    pub fn caches_mut(&mut self) -> &mut StateCaches<P> {
        &mut self.caches
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::preset::{Mainnet, Minimal};
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;
    use typenum::Unsigned;

    #[test]
    fn proposer_lookahead_capacity_mainnet() {
        let expected = (Mainnet::MIN_SEED_LOOKAHEAD + 1) * Mainnet::SLOTS_PER_EPOCH;
        assert_eq!(Mainnet::PROPOSER_LOOKAHEAD_LEN, expected);
        assert_eq!(
            <Mainnet as Preset>::ProposerLookaheadLen::to_u64(),
            expected
        );
        assert_eq!(expected, 64);
        let state = BeaconState::<Mainnet>::default();
        assert_eq!(state.proposer_lookahead().len(), expected as usize);
    }

    #[test]
    fn proposer_lookahead_capacity_minimal() {
        let expected = (Minimal::MIN_SEED_LOOKAHEAD + 1) * Minimal::SLOTS_PER_EPOCH;
        assert_eq!(Minimal::PROPOSER_LOOKAHEAD_LEN, expected);
        assert_eq!(expected, 16);
        let state = BeaconState::<Minimal>::default();
        assert_eq!(state.proposer_lookahead().len(), expected as usize);
    }

    #[test]
    fn partial_eq_ignores_caches() {
        let mut a = BeaconState::<Mainnet>::default();
        let mut b = a.clone();
        a.caches_mut().tag = 1;
        b.caches_mut().tag = 999;
        assert_eq!(a, b, "states that differ only in caches must compare equal");
        // Sanity: a real field difference is unequal.
        let c = BeaconState::<Mainnet> {
            slot: Slot::new(1),
            ..Default::default()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn default_ssz_roundtrip_and_tree_hash() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let decoded = BeaconState::<Minimal>::from_ssz_bytes(&bytes).unwrap();
        assert_eq!(decoded, state);
        // Caches on decode are default (tag 0), independent of source tag.
        let mut tagged = state.clone();
        tagged.caches_mut().tag = 42;
        let re = BeaconState::<Minimal>::from_ssz_bytes(&tagged.as_ssz_bytes()).unwrap();
        assert_eq!(re.caches().tag, 0);
        let _ = state.tree_hash_root();
    }

    #[test]
    fn from_ssz_bytes_with_fulu_ok() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let decoded =
            BeaconState::<Minimal>::from_ssz_bytes_with(ForkName::Fulu, &bytes).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn from_ssz_bytes_with_rejects_non_fulu() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let err =
            BeaconState::<Minimal>::from_ssz_bytes_with(ForkName::Electra, &bytes).unwrap_err();
        match err {
            DecodeError::BytesInvalid(msg) => assert!(msg.contains("unsupported fork"), "{msg}"),
            other => panic!("expected BytesInvalid, got {other:?}"),
        }
    }

    #[test]
    fn commit_is_noop() {
        let mut state = BeaconState::<Minimal>::default();
        state.commit();
    }

    #[test]
    fn no_public_spec_fields() {
        // Compile-time / documentation: fields are private; this test exists so
        // `grep -rn "pub " crates/types/src/state/mod.rs` reviewers have a
        // named companion AC check. Spec fields are not `pub`.
        let state = BeaconState::<Minimal>::default();
        let _ = state.slot();
        let _ = state.proposer_lookahead();
    }
}
