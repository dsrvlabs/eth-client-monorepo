//! Cryptography primitives for the consensus client (CC-11a / CC-11b).
//!
//! - [`bls`]: thin owned wrapper over `blst` 0.3.17 `min_pk` (Architecture §4.1)
//! - [`kzg`]: cell-KZG trait + backends (Architecture §4.2–4.3)
//! - [`domain`]: fork-aware signing domains (Architecture §4.5)
//! - [`hash`]: SHA-256 helpers
//!
//! This crate depends only on [`cc_types`] in the workspace DAG — never on
//! state-transition types above the crypto layer.

#![allow(missing_docs)]

pub mod bls;
pub mod domain;
pub mod hash;
pub mod kzg;

pub use bls::{
    aggregate_public_keys, aggregate_signatures, aggregate_verify, bls_verify_count,
    eth_fast_aggregate_verify, fast_aggregate_verify, take_bls_verify_count, verify,
    AggregatePublicKey, AggregateSignature, BlsError, PublicKey, Signature, SignatureSet,
    BLS_SIGNATURE_DST, INFINITY_SIGNATURE,
};

#[cfg(any(test, feature = "signing"))]
pub use bls::SecretKey;
pub use domain::{
    compute_domain, compute_fork_data_root, compute_signing_root, get_domain, SigningData,
    DOMAIN_AGGREGATE_AND_PROOF, DOMAIN_BEACON_ATTESTER, DOMAIN_BEACON_BUILDER,
    DOMAIN_BEACON_PROPOSER, DOMAIN_BLS_TO_EXECUTION_CHANGE, DOMAIN_BUILDER_DEPOSIT,
    DOMAIN_CONTRIBUTION_AND_PROOF, DOMAIN_DEPOSIT, DOMAIN_PROPOSER_PREFERENCES,
    DOMAIN_PTC_ATTESTER, DOMAIN_RANDAO, DOMAIN_SELECTION_PROOF, DOMAIN_SYNC_COMMITTEE,
    DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF, DOMAIN_VOLUNTARY_EXIT,
};
pub use hash::{hash32_concat, hash_fixed};
pub use kzg::{
    Blob, CellKzg, CellProofs, Cells, CellsAndProofs, KzgBackendKind, KzgError, BYTES_PER_BLOB,
};

#[cfg(feature = "kzg-c-kzg")]
pub use kzg::CKzgBackend;

#[cfg(feature = "kzg-rust-eth-kzg")]
pub use kzg::{RustEthKzgBackend, UsePrecomp, DEFAULT_USE_PRECOMP};

#[cfg(any(
    all(feature = "kzg-c-kzg", not(feature = "kzg-rust-eth-kzg")),
    all(feature = "kzg-rust-eth-kzg", not(feature = "kzg-c-kzg")),
))]
pub use kzg::DefaultKzg;
