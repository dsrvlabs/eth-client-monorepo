//! Spec `on_tick` / `on_tick_per_slot` (Architecture §6.2, CC-15a).
//!
//! Advances store time slot-by-slot so every intermediate slot boundary is
//! observed: clears `proposer_boost_root` on each new slot, and on an epoch
//! boundary promotes unrealized checkpoints via `update_checkpoints`.

use cc_types::preset::Preset;

use crate::store::{Store, StoreError};

/// Spec `on_tick(store, time)`.
///
/// If the store has fallen behind, each intermediate slot is processed with
/// `on_tick_per_slot` so proposer-boost clears and epoch-boundary checkpoint
/// updates are not skipped.
pub fn on_tick<P: Preset>(store: &mut Store<P>, time: u64) -> Result<(), StoreError> {
    if time < store.time() {
        return Err(StoreError::TimeWentBackwards {
            time,
            store_time: store.time(),
        });
    }

    let tick_slot = time.saturating_sub(store.genesis_time()) / store.seconds_per_slot();

    while store.get_current_slot().as_u64() < tick_slot {
        let previous_time = store.genesis_time().saturating_add(
            store
                .get_current_slot()
                .as_u64()
                .saturating_add(1)
                .saturating_mul(store.seconds_per_slot()),
        );
        on_tick_per_slot(store, previous_time);
    }
    on_tick_per_slot(store, time);
    Ok(())
}

/// Spec `on_tick_per_slot(store, time)`.
fn on_tick_per_slot<P: Preset>(store: &mut Store<P>, time: u64) {
    let previous_slot = store.get_current_slot();

    // Update store time (mid-slot time alone does not move the head).
    store.set_time(time);

    let current_slot = store.get_current_slot();

    // If this is a new slot, reset store.proposer_boost_root.
    if current_slot.as_u64() > previous_slot.as_u64() {
        store.clear_proposer_boost_root();
    }

    // If a new epoch, pull-up justification and finalization from previous epoch.
    if current_slot.as_u64() > previous_slot.as_u64()
        && Store::<P>::compute_slots_since_epoch_start(current_slot) == 0
    {
        store.update_checkpoints(
            store.unrealized_justified_checkpoint(),
            store.unrealized_finalized_checkpoint(),
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

/// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: cc_state_transition::NewPayloadRequest<'_, P>,
    ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
        Ok(cc_state_transition::PayloadStatus::Valid)
    }
}

    use std::sync::Arc;

        use cc_types::containers::Checkpoint;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Root};

    use super::*;
    use crate::da_seam::HarnessAvailability;
    use crate::store::Store;

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn cp(epoch: u64, r: Root) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(epoch),
            root: r,
        }
    }

    /// Minimal preset: `SLOTS_PER_EPOCH = 8`, use 6 s/slot like the vector suite.
    fn new_store(genesis_time: u64, time: u64) -> Store<Minimal> {
        let anchor = cp(0, root(1));
        Store::new(
            time,
            genesis_time,
            6,
            anchor,
            anchor,
            16,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
        )
    }

    #[test]
    fn on_tick_slot_boundary_clears_proposer_boost_and_bumps_counter() {
        let genesis = 0_u64;
        let mut store = new_store(genesis, 0);
        store.set_proposer_boost_root_for_test(root(0xAB));
        let before = store.mutation_counter();

        on_tick(&mut store, 6).unwrap();

        assert_eq!(store.time(), 6);
        assert_eq!(store.get_current_slot().as_u64(), 1);
        assert_eq!(store.proposer_boost_root(), Root::ZERO);
        assert!(
            store.mutation_counter() > before,
            "slot boundary must bump mutation_counter"
        );
    }

    #[test]
    fn on_tick_epoch_boundary_updates_justified_from_unrealized() {
        let genesis = 0_u64;
        // Minimal: 8 slots/epoch × 6 s = 48 s per epoch.
        // Start at slot 7 (time 42).
        let mut store = new_store(genesis, 42);
        assert_eq!(store.get_current_slot().as_u64(), 7);
        assert_eq!(store.get_current_store_epoch().as_u64(), 0);

        let new_justified = cp(1, root(0x11));
        let new_finalized = cp(0, root(1));
        store.seed_unrealized_for_test(new_justified, new_finalized);
        store.set_proposer_boost_root_for_test(root(0xCD));

        // Cross into slot 8 = start of epoch 1 (time 48).
        on_tick(&mut store, 48).unwrap();

        assert_eq!(store.get_current_slot().as_u64(), 8);
        assert_eq!(store.get_current_store_epoch().as_u64(), 1);
        assert_eq!(store.proposer_boost_root(), Root::ZERO);
        assert_eq!(store.justified_checkpoint(), new_justified);
        assert_eq!(store.finalized_checkpoint().epoch.as_u64(), 0);
    }

    #[test]
    fn on_tick_same_slot_does_not_clear_boost() {
        let mut store = new_store(0, 0);
        store.set_proposer_boost_root_for_test(root(0xEE));
        let before = store.mutation_counter();

        on_tick(&mut store, 5).unwrap();

        assert_eq!(store.proposer_boost_root(), root(0xEE));
        assert_eq!(
            store.mutation_counter(),
            before,
            "same-slot tick must not bump counter"
        );
        assert_eq!(store.time(), 5);
    }

    #[test]
    fn on_tick_rejects_backwards_time() {
        let mut store = new_store(0, 10);
        let err = on_tick(&mut store, 5).unwrap_err();
        assert!(matches!(
            err,
            StoreError::TimeWentBackwards {
                time: 5,
                store_time: 10
            }
        ));
    }

    #[test]
    fn on_tick_catches_up_slot_by_slot() {
        let mut store = new_store(0, 0);
        store.set_proposer_boost_root_for_test(root(0x01));
        let before = store.mutation_counter();

        on_tick(&mut store, 18).unwrap(); // slot 3

        assert_eq!(store.get_current_slot().as_u64(), 3);
        assert_eq!(store.proposer_boost_root(), Root::ZERO);
        assert_eq!(store.mutation_counter(), before + 3);
    }
}
