//! Spec `process_randao` (phase0, unchanged through Fulu).
//!
//! Signature verification is collected into the block-level [`crate::signatures::BlockSignatureSet`]
//! (VerifyBatch contribution / VerifyIndividual direct verify). This handler only mixes the
//! reveal into `randao_mixes` after signatures have already been checked by
//! [`crate::signatures::verify_block_signatures`] — matching Architecture §5.2.
//!
//! When called standalone (unit tests), callers must verify the reveal first or accept an
//! unverified mix update (spec vectors for this handler are not emitted under Fulu operations).

use cc_types::preset::Preset;
use cc_types::{BeaconBlock, BeaconState};

use crate::error::BlockError;
use crate::helpers::accessors::{get_current_epoch, get_randao_mix};
use crate::helpers::misc::{hash_signature_root, xor_bytes32};

/// Spec `process_randao` — mix update only (signature verified elsewhere).
pub fn process_randao<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
) -> Result<(), BlockError> {
    let epoch = get_current_epoch(state);
    let reveal_hash = hash_signature_root(&block.body.randao_reveal);
    let mix = xor_bytes32(get_randao_mix(state, epoch)?, reveal_hash);
    let i = (epoch.as_u64() % P::EPOCHS_PER_HISTORICAL_VECTOR) as usize;
    state.randao_mixes_set(i, mix)?;
    Ok(())
}
