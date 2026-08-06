//! [`CheckpointContext`] — derived checkpoint data (Architecture §6.6, ADR-P1-08).
//!
//! # Spec deviation
//!
//! The consensus-spec stores a full `BeaconState` per checkpoint (the map the
//! architecture replaces under ADR-P1-08). With a flat 150–200 MB state that map
//! is an OOM with a spec citation. Phase 1 stores only the fields `on_attestation`
//! (CC-16) actually reads:
//!
//! - committee / shuffling shell → `get_beacon_committee` / attesting indices
//! - effective balances + total active balance → `compute_deltas` at justification change
//! - `fork` + `genesis_validators_root` → signature domain (carried even though Phase 1
//!   does not verify store-level attestation signatures — eight bytes now, not a
//!   two-crate change later)
//!
//! **If a future phase finds a read this struct cannot serve, the honest fix is to
//! add the field, not to reintroduce the full state.**
//!
//! Bounded at [`crate::store::DEFAULT_CHECKPOINT_CONTEXT_CAPACITY`] (8) LRU entries
//! (~64 MB worst case).

use cc_state_transition::compute_epoch_at_slot;
use cc_state_transition::helpers::accessors::{get_active_validator_indices, get_total_balance};
use cc_types::BeaconState;
use cc_types::containers::Checkpoint;
use cc_types::fork::Fork;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Gwei, Root, ValidatorIndex};

/// Shell holding epoch committee / shuffling data for a checkpoint.
///
/// Phase 1 fills a minimal snapshot of shuffled active indices when available;
/// CC-16 is the primary reader (`get_beacon_committee` resolution). Empty is a
/// valid shell — later fills expand fields rather than swapping the type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitteeCache {
    /// Active validators in shuffled order for the checkpoint epoch, if known.
    pub shuffled_active_indices: Vec<ValidatorIndex>,
}

/// Derived data for one checkpoint (Architecture §6.6 / ADR-P1-08).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointContext {
    /// Checkpoint epoch.
    pub epoch: Epoch,
    /// Committee / shuffling shell for `on_attestation`.
    pub committee_cache: CommitteeCache,
    /// Effective balances snapshot for `compute_deltas` (~8 MB at 10^6 validators).
    pub effective_balances: Vec<Gwei>,
    /// Total active balance at the checkpoint.
    pub total_active_balance: Gwei,
    /// Fork for attestation signature domain.
    pub fork: Fork,
    /// Genesis validators root for attestation signature domain.
    pub genesis_validators_root: Root,
}

impl CheckpointContext {
    /// Build context from a post-state at `checkpoint.epoch`.
    ///
    /// Balances and fork metadata are taken from `state` as-of now. The
    /// committee shell is left empty unless `shuffled_active_indices` is
    /// supplied by the caller (CC-16 may fill on first use).
    pub fn from_state<P: Preset>(state: &BeaconState<P>, checkpoint: Checkpoint) -> Self {
        let epoch = checkpoint.epoch;
        let effective_balances: Vec<Gwei> = state
            .validators_iter()
            .map(|v| v.effective_balance)
            .collect();

        // Prefer the checkpoint epoch's active set; fall back to a zero total on
        // empty registries (tests / pre-genesis shells).
        let active = get_active_validator_indices(state, epoch);
        let total_active_balance =
            get_total_balance(state, &active).unwrap_or_else(|_| Gwei::new(0));

        Self {
            epoch,
            committee_cache: CommitteeCache::default(),
            effective_balances,
            total_active_balance,
            fork: state.fork(),
            genesis_validators_root: state.genesis_validators_root(),
        }
    }

    /// Build from a post-state using the state's current epoch as the checkpoint epoch.
    pub fn from_post_state<P: Preset>(state: &BeaconState<P>) -> Self {
        let epoch = compute_epoch_at_slot::<P>(state.slot());
        let checkpoint = Checkpoint {
            epoch,
            root: Root::ZERO,
        };
        Self::from_state(state, checkpoint)
    }
}

/// LRU key for [`crate::store::Store`]'s checkpoint-context map: `(epoch, root)`.
pub type CheckpointContextKey = (Epoch, Root);

/// Build the map key for a checkpoint.
#[inline]
pub fn checkpoint_context_key(checkpoint: Checkpoint) -> CheckpointContextKey {
    (checkpoint.epoch, checkpoint.root)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::preset::Minimal;
    use cc_types::primitives::Epoch;

    #[test]
    fn from_default_state_populates_fork_and_balances() {
        let state = BeaconState::<Minimal>::default();
        let cp = Checkpoint {
            epoch: Epoch::new(0),
            root: Root::from_array([1u8; 32]),
        };
        let ctx = CheckpointContext::from_state(&state, cp);
        assert_eq!(ctx.epoch, Epoch::new(0));
        assert_eq!(ctx.fork, state.fork());
        assert_eq!(ctx.genesis_validators_root, state.genesis_validators_root());
        assert!(ctx.effective_balances.is_empty());
        assert!(ctx.committee_cache.shuffled_active_indices.is_empty());
    }
}
