//! Intersection-only accessors for `BeaconState` (Architecture §3.4).
//!
//! Allowed surface: `len`, `is_empty`, `get`, `iter`, `get_mut`, `push`, `set`, `commit`.
//! Mutating accessors record dirty leaf indices into `StateCaches`.
//!
//! Callers outside `crates/types` must not name `ssz_types::{VariableList, FixedVector}` nor
//! rely on `Deref`/`as_slice`/`iter_mut` on state lists.

use ssz_types::BitVector;
use tree_hash::{Hash256, TreeHash};
use typenum::Unsigned;

use super::caches::{StateField, container_leaf, list_id, packed_basic_leaf};
use super::{BeaconState, JustificationBitsLength, List, ParticipationFlags, StateCaches, Vector};
use crate::containers::{
    BeaconBlockHeader, Checkpoint, Eth1Data, HistoricalSummary, SyncCommittee, Validator,
};
use crate::execution::ExecutionPayloadHeader;
use crate::fork::Fork;
use crate::operations::{PendingConsolidation, PendingDeposit, PendingPartialWithdrawal};
use crate::preset::Preset;
use crate::primitives::{Epoch, Gwei, Root, Slot, ValidatorIndex};

/// Error from a bounds-checked list mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateAccessError {
    /// Index past the current list length.
    OutOfBounds {
        /// Requested index.
        index: usize,
        /// Current length.
        len: usize,
    },
    /// Push would exceed the SSZ max length.
    Full {
        /// Maximum length.
        max: usize,
    },
}

impl std::fmt::Display for StateAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfBounds { index, len } => {
                write!(f, "index {index} out of bounds for length {len}")
            }
            Self::Full { max } => write!(f, "list is full (max {max})"),
        }
    }
}

impl std::error::Error for StateAccessError {}

impl From<ssz_types::Error> for StateAccessError {
    fn from(err: ssz_types::Error) -> Self {
        match err {
            ssz_types::Error::OutOfBounds { i, len } => {
                // push path reports would-be length as `i`.
                if i > len {
                    Self::Full { max: len }
                } else {
                    Self::OutOfBounds { index: i, len }
                }
            }
            _ => Self::OutOfBounds { index: 0, len: 0 },
        }
    }
}

// ---------------------------------------------------------------------------
// Read accessors
// ---------------------------------------------------------------------------

impl<P: Preset> BeaconState<P> {
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

    /// Justification bits.
    pub fn justification_bits(&self) -> &BitVector<JustificationBitsLength> {
        &self.justification_bits
    }

    /// Eth1 data.
    pub fn eth1_data(&self) -> Eth1Data {
        self.eth1_data
    }

    /// Eth1 deposit index.
    pub fn eth1_deposit_index(&self) -> u64 {
        self.eth1_deposit_index
    }

    /// Next withdrawal index.
    pub fn next_withdrawal_index(&self) -> u64 {
        self.next_withdrawal_index
    }

    /// Next withdrawal validator index.
    pub fn next_withdrawal_validator_index(&self) -> ValidatorIndex {
        self.next_withdrawal_validator_index
    }

    /// Current sync committee.
    pub fn current_sync_committee(&self) -> &SyncCommittee<P> {
        &self.current_sync_committee
    }

    /// Next sync committee.
    pub fn next_sync_committee(&self) -> &SyncCommittee<P> {
        &self.next_sync_committee
    }

    /// Latest execution payload header.
    pub fn latest_execution_payload_header(&self) -> &ExecutionPayloadHeader<P> {
        &self.latest_execution_payload_header
    }

    /// Proposer lookahead length (Fulu EIP-7917).
    pub fn proposer_lookahead_len(&self) -> usize {
        self.proposer_lookahead.len()
    }

    /// Proposer lookahead entry.
    pub fn proposer_lookahead_get(&self, i: usize) -> Option<ValidatorIndex> {
        self.proposer_lookahead.get(i).copied()
    }

    /// Proposer lookahead iterator.
    pub fn proposer_lookahead_iter(&self) -> impl Iterator<Item = &ValidatorIndex> {
        self.proposer_lookahead.iter()
    }

    /// Borrow proposer lookahead as the seam vector type (read-only intersection use).
    pub fn proposer_lookahead(&self) -> &Vector<ValidatorIndex, P::ProposerLookaheadLen> {
        &self.proposer_lookahead
    }

    /// Validator registry length.
    pub fn validators_len(&self) -> usize {
        self.validators.len()
    }

    /// Whether the validator registry is empty.
    pub fn validators_is_empty(&self) -> bool {
        self.validators.is_empty()
    }

    /// Get validator by index.
    pub fn validators_get(&self, i: usize) -> Option<&Validator> {
        self.validators.get(i)
    }

    /// Iterate validators.
    pub fn validators_iter(&self) -> impl Iterator<Item = &Validator> {
        self.validators.iter()
    }

    /// Balances length.
    pub fn balances_len(&self) -> usize {
        self.balances.len()
    }

    /// Get balance by index.
    pub fn balances_get(&self, i: usize) -> Option<Gwei> {
        self.balances.get(i).copied()
    }

    /// Iterate balances.
    pub fn balances_iter(&self) -> impl Iterator<Item = &Gwei> {
        self.balances.iter()
    }

    /// Previous epoch participation length.
    pub fn previous_epoch_participation_len(&self) -> usize {
        self.previous_epoch_participation.len()
    }

    /// Get previous-epoch participation flags.
    pub fn previous_epoch_participation_get(&self, i: usize) -> Option<ParticipationFlags> {
        self.previous_epoch_participation.get(i).copied()
    }

    /// Current epoch participation length.
    pub fn current_epoch_participation_len(&self) -> usize {
        self.current_epoch_participation.len()
    }

    /// Get current-epoch participation flags.
    pub fn current_epoch_participation_get(&self, i: usize) -> Option<ParticipationFlags> {
        self.current_epoch_participation.get(i).copied()
    }

    /// Inactivity scores length.
    pub fn inactivity_scores_len(&self) -> usize {
        self.inactivity_scores.len()
    }

    /// Get inactivity score.
    pub fn inactivity_scores_get(&self, i: usize) -> Option<u64> {
        self.inactivity_scores.get(i).copied()
    }

    /// RANDAO mixes length (fixed).
    pub fn randao_mixes_len(&self) -> usize {
        self.randao_mixes.len()
    }

    /// Get a RANDAO mix.
    pub fn randao_mixes_get(&self, i: usize) -> Option<Root> {
        self.randao_mixes.get(i).copied()
    }

    /// Block roots length (fixed).
    pub fn block_roots_len(&self) -> usize {
        self.block_roots.len()
    }

    /// Get a block root.
    pub fn block_roots_get(&self, i: usize) -> Option<Root> {
        self.block_roots.get(i).copied()
    }

    /// State roots length (fixed).
    pub fn state_roots_len(&self) -> usize {
        self.state_roots.len()
    }

    /// Get a state root.
    pub fn state_roots_get(&self, i: usize) -> Option<Root> {
        self.state_roots.get(i).copied()
    }

    /// Historical roots length.
    pub fn historical_roots_len(&self) -> usize {
        self.historical_roots.len()
    }

    /// Pending deposits length.
    pub fn pending_deposits_len(&self) -> usize {
        self.pending_deposits.len()
    }

    /// Pending partial withdrawals length.
    pub fn pending_partial_withdrawals_len(&self) -> usize {
        self.pending_partial_withdrawals.len()
    }

    /// Get a pending partial withdrawal by index.
    pub fn pending_partial_withdrawals_get(&self, i: usize) -> Option<&PendingPartialWithdrawal> {
        self.pending_partial_withdrawals.get(i)
    }

    /// Iterate pending partial withdrawals.
    pub fn pending_partial_withdrawals_iter(
        &self,
    ) -> impl Iterator<Item = &PendingPartialWithdrawal> {
        self.pending_partial_withdrawals.iter()
    }

    /// Eth1 data votes length.
    pub fn eth1_data_votes_len(&self) -> usize {
        self.eth1_data_votes.len()
    }

    /// Get an eth1 data vote by index.
    pub fn eth1_data_votes_get(&self, i: usize) -> Option<Eth1Data> {
        self.eth1_data_votes.get(i).copied()
    }

    /// Iterate eth1 data votes.
    pub fn eth1_data_votes_iter(&self) -> impl Iterator<Item = &Eth1Data> {
        self.eth1_data_votes.iter()
    }

    /// Count of votes equal to `vote` in `eth1_data_votes`.
    pub fn eth1_data_votes_count(&self, vote: &Eth1Data) -> usize {
        self.eth1_data_votes.iter().filter(|v| *v == vote).count()
    }

    /// Pending consolidations length.
    pub fn pending_consolidations_len(&self) -> usize {
        self.pending_consolidations.len()
    }

    /// Borrow caches.
    pub fn caches(&self) -> &StateCaches<P> {
        &self.caches
    }

    /// Mutable caches (test / transition fill hooks).
    pub fn caches_mut(&mut self) -> &mut StateCaches<P> {
        &mut self.caches
    }
}

// ---------------------------------------------------------------------------
// Scalar / small-field setters
// ---------------------------------------------------------------------------

impl<P: Preset> BeaconState<P> {
    /// Set genesis time.
    pub fn set_genesis_time(&mut self, v: u64) {
        self.genesis_time = v;
        self.caches.field_roots.mark_dirty(StateField::GenesisTime);
    }

    /// Set genesis validators root.
    pub fn set_genesis_validators_root(&mut self, v: Root) {
        self.genesis_validators_root = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::GenesisValidatorsRoot);
    }

    /// Set slot.
    pub fn set_slot(&mut self, v: Slot) {
        self.slot = v;
        self.caches.field_roots.mark_dirty(StateField::Slot);
    }

    /// Set fork.
    pub fn set_fork(&mut self, v: Fork) {
        self.fork = v;
        self.caches.field_roots.mark_dirty(StateField::Fork);
    }

    /// Set latest block header.
    pub fn set_latest_block_header(&mut self, v: BeaconBlockHeader) {
        self.latest_block_header = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::LatestBlockHeader);
    }

    /// Set finalized checkpoint.
    pub fn set_finalized_checkpoint(&mut self, v: Checkpoint) {
        self.finalized_checkpoint = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::FinalizedCheckpoint);
    }

    /// Set current justified checkpoint.
    pub fn set_current_justified_checkpoint(&mut self, v: Checkpoint) {
        self.current_justified_checkpoint = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::CurrentJustifiedCheckpoint);
    }

    /// Set previous justified checkpoint.
    pub fn set_previous_justified_checkpoint(&mut self, v: Checkpoint) {
        self.previous_justified_checkpoint = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::PreviousJustifiedCheckpoint);
    }

    /// Set eth1 data.
    pub fn set_eth1_data(&mut self, v: Eth1Data) {
        self.eth1_data = v;
        self.caches.field_roots.mark_dirty(StateField::Eth1Data);
    }

    /// Set eth1 deposit index.
    pub fn set_eth1_deposit_index(&mut self, v: u64) {
        self.eth1_deposit_index = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::Eth1DepositIndex);
    }

    /// Append an eth1 data vote.
    pub fn eth1_data_votes_push(&mut self, v: Eth1Data) -> Result<(), StateAccessError> {
        self.eth1_data_votes
            .push(v)
            .map_err(StateAccessError::from)?;
        self.caches.field_roots.mark_dirty(StateField::Eth1DataVotes);
        Ok(())
    }

    /// Replace eth1 data votes (e.g. epoch reset).
    pub fn eth1_data_votes_clear(&mut self) {
        self.eth1_data_votes = List::default();
        self.caches.field_roots.mark_dirty(StateField::Eth1DataVotes);
    }

    /// Set next withdrawal index.
    pub fn set_next_withdrawal_index(&mut self, v: u64) {
        self.next_withdrawal_index = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::NextWithdrawalIndex);
    }

    /// Set next withdrawal validator index.
    pub fn set_next_withdrawal_validator_index(&mut self, v: ValidatorIndex) {
        self.next_withdrawal_validator_index = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::NextWithdrawalValidatorIndex);
    }

    /// Set latest execution payload header.
    pub fn set_latest_execution_payload_header(&mut self, v: ExecutionPayloadHeader<P>) {
        self.latest_execution_payload_header = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::LatestExecutionPayloadHeader);
    }

    /// Drop the first `n` pending partial withdrawals (Electra queue advance).
    pub fn pending_partial_withdrawals_drain_prefix(
        &mut self,
        n: usize,
    ) -> Result<(), StateAccessError> {
        let len = self.pending_partial_withdrawals.len();
        if n > len {
            return Err(StateAccessError::OutOfBounds { index: n, len });
        }
        if n == 0 {
            return Ok(());
        }
        // Rebuild from the remaining tail — VariableList has no drain API.
        let old = std::mem::take(&mut self.pending_partial_withdrawals);
        let mut remaining: Vec<PendingPartialWithdrawal> = old.to_vec();
        remaining.drain(..n);
        self.pending_partial_withdrawals =
            List::new(remaining).map_err(StateAccessError::from)?;
        self.caches
            .field_roots
            .mark_dirty(StateField::PendingPartialWithdrawals);
        Ok(())
    }

    /// Set a RANDAO mix (fixed vector).
    pub fn randao_mixes_set(&mut self, i: usize, v: Root) -> Result<(), StateAccessError> {
        let len = self.randao_mixes.len();
        let slot = self
            .randao_mixes
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.field_roots.mark_dirty(StateField::RandaoMixes);
        Ok(())
    }

    /// Set a block root.
    pub fn block_roots_set(&mut self, i: usize, v: Root) -> Result<(), StateAccessError> {
        let len = self.block_roots.len();
        let slot = self
            .block_roots
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.field_roots.mark_dirty(StateField::BlockRoots);
        Ok(())
    }

    /// Set a state root.
    pub fn state_roots_set(&mut self, i: usize, v: Root) -> Result<(), StateAccessError> {
        let len = self.state_roots.len();
        let slot = self
            .state_roots
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.field_roots.mark_dirty(StateField::StateRoots);
        Ok(())
    }

    /// Set a proposer lookahead entry.
    pub fn proposer_lookahead_set(
        &mut self,
        i: usize,
        v: ValidatorIndex,
    ) -> Result<(), StateAccessError> {
        let len = self.proposer_lookahead.len();
        let slot = self
            .proposer_lookahead
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches
            .field_roots
            .mark_dirty(StateField::ProposerLookahead);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Large-list mutators (dirty-tracked)
// ---------------------------------------------------------------------------

impl<P: Preset> BeaconState<P> {
    /// Mutable validator access; marks the leaf dirty (conservative).
    pub fn validators_get_mut(&mut self, i: usize) -> Option<&mut Validator> {
        let len = self.validators.len();
        if i >= len {
            return None;
        }
        self.caches
            .mark_list_element_dirty(list_id::VALIDATORS, StateField::Validators, i);
        self.validators.get_mut(i)
    }

    /// Replace a validator at `i`.
    pub fn validators_set(&mut self, i: usize, v: Validator) -> Result<(), StateAccessError> {
        let len = self.validators.len();
        let slot = self
            .validators
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches
            .mark_list_element_dirty(list_id::VALIDATORS, StateField::Validators, i);
        Ok(())
    }

    /// Append a validator.
    pub fn validators_push(&mut self, v: Validator) -> Result<(), StateAccessError> {
        self.validators.push(v).map_err(StateAccessError::from)?;
        let new_len = self.validators.len();
        self.caches
            .note_list_length(list_id::VALIDATORS, StateField::Validators, new_len);
        Ok(())
    }

    /// Set a balance.
    pub fn balances_set(&mut self, i: usize, v: Gwei) -> Result<(), StateAccessError> {
        let len = self.balances.len();
        let slot = self
            .balances
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches
            .mark_list_element_dirty(list_id::BALANCES, StateField::Balances, i);
        Ok(())
    }

    /// Append a balance.
    pub fn balances_push(&mut self, v: Gwei) -> Result<(), StateAccessError> {
        self.balances.push(v).map_err(StateAccessError::from)?;
        let new_len = self.balances.len();
        self.caches
            .note_list_length(list_id::BALANCES, StateField::Balances, new_len);
        Ok(())
    }

    /// Mutable balance access.
    pub fn balances_get_mut(&mut self, i: usize) -> Option<&mut Gwei> {
        if i >= self.balances.len() {
            return None;
        }
        self.caches
            .mark_list_element_dirty(list_id::BALANCES, StateField::Balances, i);
        self.balances.get_mut(i)
    }

    /// Set previous-epoch participation flags.
    pub fn previous_epoch_participation_set(
        &mut self,
        i: usize,
        v: ParticipationFlags,
    ) -> Result<(), StateAccessError> {
        let len = self.previous_epoch_participation.len();
        let slot = self
            .previous_epoch_participation
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.mark_list_element_dirty(
            list_id::PREV_PARTICIPATION,
            StateField::PreviousEpochParticipation,
            i,
        );
        Ok(())
    }

    /// Append previous-epoch participation flags.
    pub fn previous_epoch_participation_push(
        &mut self,
        v: ParticipationFlags,
    ) -> Result<(), StateAccessError> {
        self.previous_epoch_participation
            .push(v)
            .map_err(StateAccessError::from)?;
        let new_len = self.previous_epoch_participation.len();
        self.caches.note_list_length(
            list_id::PREV_PARTICIPATION,
            StateField::PreviousEpochParticipation,
            new_len,
        );
        Ok(())
    }

    /// Set current-epoch participation flags.
    pub fn current_epoch_participation_set(
        &mut self,
        i: usize,
        v: ParticipationFlags,
    ) -> Result<(), StateAccessError> {
        let len = self.current_epoch_participation.len();
        let slot = self
            .current_epoch_participation
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.mark_list_element_dirty(
            list_id::CURR_PARTICIPATION,
            StateField::CurrentEpochParticipation,
            i,
        );
        Ok(())
    }

    /// Append current-epoch participation flags.
    pub fn current_epoch_participation_push(
        &mut self,
        v: ParticipationFlags,
    ) -> Result<(), StateAccessError> {
        self.current_epoch_participation
            .push(v)
            .map_err(StateAccessError::from)?;
        let new_len = self.current_epoch_participation.len();
        self.caches.note_list_length(
            list_id::CURR_PARTICIPATION,
            StateField::CurrentEpochParticipation,
            new_len,
        );
        Ok(())
    }

    /// Set an inactivity score.
    pub fn inactivity_scores_set(&mut self, i: usize, v: u64) -> Result<(), StateAccessError> {
        let len = self.inactivity_scores.len();
        let slot = self
            .inactivity_scores
            .get_mut(i)
            .ok_or(StateAccessError::OutOfBounds { index: i, len })?;
        *slot = v;
        self.caches.mark_list_element_dirty(
            list_id::INACTIVITY_SCORES,
            StateField::InactivityScores,
            i,
        );
        Ok(())
    }

    /// Append an inactivity score.
    pub fn inactivity_scores_push(&mut self, v: u64) -> Result<(), StateAccessError> {
        self.inactivity_scores
            .push(v)
            .map_err(StateAccessError::from)?;
        let new_len = self.inactivity_scores.len();
        self.caches.note_list_length(
            list_id::INACTIVITY_SCORES,
            StateField::InactivityScores,
            new_len,
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// commit + canonical_root
// ---------------------------------------------------------------------------

impl<P: Preset> BeaconState<P> {
    /// Flush pending list backend updates.
    ///
    /// No-op under `ssz_types`. Under milhouse (CC-1H) this will call `apply_updates()`
    /// on every list. Always invoked at the top of [`Self::canonical_root`].
    pub fn commit(&mut self) {
        // ssz_types: nothing to flush.
    }

    /// Cached state root path (Architecture §3.4 / ADR-P1-04).
    ///
    /// Commits list backends, updates list-hash and field-root caches, and returns the
    /// container root. Independent of [`TreeHash::tree_hash_root`], which remains the
    /// cold uncached path used by `ssz_static`.
    pub fn canonical_root(&mut self) -> Root {
        self.commit();
        self.recompute_caches();
        let hash = self.caches.field_roots.cached_root.unwrap_or_else(|| {
            // recompute_caches always sets this; fall back to cold path if not.
            TreeHash::tree_hash_root(self)
        });
        Root::from_hash256(hash)
    }

    /// Rebuild dirty list and field caches; leave `field_roots.cached_root` set.
    fn recompute_caches(&mut self) {
        let registry_limit = P::ValidatorRegistryLimit::to_usize();

        // Temporarily take caches so list fields and caches can be borrowed together.
        let mut caches = std::mem::take(&mut self.caches);

        let validators_root = {
            let _ = caches.ensure_list_cache(list_id::VALIDATORS, registry_limit, 1);
            let list = &self.validators;
            let list_len = list.len();
            match caches.list_hashes[list_id::VALIDATORS].as_mut() {
                Some(c) => c.recompute_with(list_len, |leaf| container_leaf(&list[..], leaf)),
                None => list.tree_hash_root(),
            }
        };

        let balances_root = {
            let packing = Gwei::tree_hash_packing_factor();
            let _ = caches.ensure_list_cache(list_id::BALANCES, registry_limit, packing);
            let list = &self.balances;
            let list_len = list.len();
            match caches.list_hashes[list_id::BALANCES].as_mut() {
                Some(c) => {
                    c.recompute_with(list_len, |leaf| packed_basic_leaf(&list[..], packing, leaf))
                }
                None => list.tree_hash_root(),
            }
        };

        let prev_part_root = {
            let packing = u8::tree_hash_packing_factor();
            let _ = caches.ensure_list_cache(list_id::PREV_PARTICIPATION, registry_limit, packing);
            let list = &self.previous_epoch_participation;
            let list_len = list.len();
            match caches.list_hashes[list_id::PREV_PARTICIPATION].as_mut() {
                Some(c) => {
                    c.recompute_with(list_len, |leaf| packed_basic_leaf(&list[..], packing, leaf))
                }
                None => list.tree_hash_root(),
            }
        };

        let curr_part_root = {
            let packing = u8::tree_hash_packing_factor();
            let _ = caches.ensure_list_cache(list_id::CURR_PARTICIPATION, registry_limit, packing);
            let list = &self.current_epoch_participation;
            let list_len = list.len();
            match caches.list_hashes[list_id::CURR_PARTICIPATION].as_mut() {
                Some(c) => {
                    c.recompute_with(list_len, |leaf| packed_basic_leaf(&list[..], packing, leaf))
                }
                None => list.tree_hash_root(),
            }
        };

        let inactivity_root = {
            let packing = u64::tree_hash_packing_factor();
            let _ = caches.ensure_list_cache(list_id::INACTIVITY_SCORES, registry_limit, packing);
            let list = &self.inactivity_scores;
            let list_len = list.len();
            match caches.list_hashes[list_id::INACTIVITY_SCORES].as_mut() {
                Some(c) => {
                    c.recompute_with(list_len, |leaf| packed_basic_leaf(&list[..], packing, leaf))
                }
                None => list.tree_hash_root(),
            }
        };

        caches
            .field_roots
            .set_field_root(StateField::Validators, validators_root);
        caches
            .field_roots
            .set_field_root(StateField::Balances, balances_root);
        caches
            .field_roots
            .set_field_root(StateField::PreviousEpochParticipation, prev_part_root);
        caches
            .field_roots
            .set_field_root(StateField::CurrentEpochParticipation, curr_part_root);
        caches
            .field_roots
            .set_field_root(StateField::InactivityScores, inactivity_root);

        let dirty: Vec<usize> = (0..super::caches::BEACON_STATE_FIELD_COUNT)
            .filter(|&i| caches.field_roots.is_dirty(i))
            .collect();

        for i in dirty {
            if let Some(root) = self.field_tree_hash(i) {
                caches.field_roots.roots[i] = root;
                caches.field_roots.dirty.set(i, false);
            }
        }

        let _ = caches.field_roots.container_root_from_leaves();
        self.caches = caches;
    }

    /// Cold tree-hash of a single top-level field by index.
    fn field_tree_hash(&self, index: usize) -> Option<Hash256> {
        Some(match index {
            0 => self.genesis_time.tree_hash_root(),
            1 => self.genesis_validators_root.tree_hash_root(),
            2 => self.slot.tree_hash_root(),
            3 => self.fork.tree_hash_root(),
            4 => self.latest_block_header.tree_hash_root(),
            5 => self.block_roots.tree_hash_root(),
            6 => self.state_roots.tree_hash_root(),
            7 => self.historical_roots.tree_hash_root(),
            8 => self.eth1_data.tree_hash_root(),
            9 => self.eth1_data_votes.tree_hash_root(),
            10 => self.eth1_deposit_index.tree_hash_root(),
            11 => self.validators.tree_hash_root(),
            12 => self.balances.tree_hash_root(),
            13 => self.randao_mixes.tree_hash_root(),
            14 => self.slashings.tree_hash_root(),
            15 => self.previous_epoch_participation.tree_hash_root(),
            16 => self.current_epoch_participation.tree_hash_root(),
            17 => self.justification_bits.tree_hash_root(),
            18 => self.previous_justified_checkpoint.tree_hash_root(),
            19 => self.current_justified_checkpoint.tree_hash_root(),
            20 => self.finalized_checkpoint.tree_hash_root(),
            21 => self.inactivity_scores.tree_hash_root(),
            22 => self.current_sync_committee.tree_hash_root(),
            23 => self.next_sync_committee.tree_hash_root(),
            24 => self.latest_execution_payload_header.tree_hash_root(),
            25 => self.next_withdrawal_index.tree_hash_root(),
            26 => self.next_withdrawal_validator_index.tree_hash_root(),
            27 => self.historical_summaries.tree_hash_root(),
            28 => self.deposit_requests_start_index.tree_hash_root(),
            29 => self.deposit_balance_to_consume.tree_hash_root(),
            30 => self.exit_balance_to_consume.tree_hash_root(),
            31 => self.earliest_exit_epoch.tree_hash_root(),
            32 => self.consolidation_balance_to_consume.tree_hash_root(),
            33 => self.earliest_consolidation_epoch.tree_hash_root(),
            34 => self.pending_deposits.tree_hash_root(),
            35 => self.pending_partial_withdrawals.tree_hash_root(),
            36 => self.pending_consolidations.tree_hash_root(),
            37 => self.proposer_lookahead.tree_hash_root(),
            _ => return None,
        })
    }
}

// Silence unused import warnings for types referenced only in docs / future accessors.
#[allow(dead_code)]
type _ListProof<T, N> = List<T, N>;
#[allow(dead_code)]
type _Pending = (
    PendingDeposit,
    PendingPartialWithdrawal,
    PendingConsolidation,
    HistoricalSummary,
    Epoch,
);
