//! R-10 compile-only guard: every type/constant `cc-crypto` reads from `cc-types`
//! is public at this commit (CC-10c). Stream B must never need to edit this crate
//! for a missing primitive.

#![allow(dead_code, unused_imports, clippy::unwrap_used)]

use cc_types::{
    BlsPublicKey, BlsSignature, Cell, Domain, DomainType, Epoch, ExecutionAddress, Fork,
    ForkData, ForkDigest, ForkName, ForkVersion, Hash256, KzgCommitment, KzgProof, Root, Slot,
    ValidatorIndex, BYTES_PER_CELL, CELLS_PER_EXT_BLOB, CUSTODY_REQUIREMENT,
    FIELD_ELEMENTS_PER_CELL, FIELD_ELEMENTS_PER_EXT_BLOB, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH,
    NUMBER_OF_COLUMNS, NUMBER_OF_CUSTODY_GROUPS, SAMPLES_PER_SLOT,
};

#[test]
fn crypto_surface_types_and_constants_are_public() {
    // Types Stream B constructs or names.
    let _root: Root = Root::ZERO;
    let _fv: ForkVersion = ForkVersion::ZERO;
    let _commit: KzgCommitment = KzgCommitment::ZERO;
    let _proof: KzgProof = KzgProof::ZERO;
    let _cell: Cell = Cell::ZERO;
    let _hash: Hash256 = _root.to_hash256();
    let _pk: BlsPublicKey = BlsPublicKey::ZERO;
    let _sig: BlsSignature = BlsSignature::ZERO;
    let _domain: Domain = Domain::ZERO;
    let _dt: DomainType = DomainType::ZERO;
    let _slot: Slot = Slot::new(0);
    let _epoch: Epoch = Epoch::new(0);
    let _vi: ValidatorIndex = ValidatorIndex::new(0);
    let _addr: ExecutionAddress = ExecutionAddress::ZERO;
    let _fork = Fork {
        previous_version: ForkVersion::ZERO,
        current_version: ForkVersion::ZERO,
        epoch: Epoch::new(0),
    };
    let _fd = ForkData {
        current_version: ForkVersion::ZERO,
        genesis_validators_root: Root::ZERO,
    };
    let _digest = ForkDigest::ZERO;
    let _name = ForkName::Fulu;

    // KZG / DAS constants.
    assert_eq!(CELLS_PER_EXT_BLOB, 128);
    assert_eq!(FIELD_ELEMENTS_PER_CELL, 64);
    assert_eq!(FIELD_ELEMENTS_PER_EXT_BLOB, 8192);
    assert_eq!(NUMBER_OF_COLUMNS, 128);
    assert_eq!(NUMBER_OF_CUSTODY_GROUPS, 128);
    assert_eq!(CUSTODY_REQUIREMENT, 4);
    assert_eq!(SAMPLES_PER_SLOT, 8);
    assert_eq!(KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, 4);
    assert_eq!(BYTES_PER_CELL, 2048);
    assert_eq!(Cell::LEN, BYTES_PER_CELL);

    cc_types::__crypto_surface_markers();
}
