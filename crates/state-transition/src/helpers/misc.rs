//! Miscellaneous pure helpers (`compute_*`).

use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Slot};

/// `compute_epoch_at_slot(slot) = slot // SLOTS_PER_EPOCH`.
#[inline]
pub fn compute_epoch_at_slot<P: Preset>(slot: Slot) -> Epoch {
    slot.epoch(P::SLOTS_PER_EPOCH)
}
