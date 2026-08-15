//! Domain and signing-root computation (Architecture §4.5).
//!
//! Operates on types from `cc-types` only — never on full beacon-state
//! containers. Callers in `cc-state-transition` pass `state.fork()` and
//! `state.genesis_validators_root()`.

use cc_types::{Domain, DomainType, Epoch, Fork, ForkData, ForkVersion, Root};
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

// ---------------------------------------------------------------------------
// Fulu DOMAIN_* constants (little-endian DomainType bytes)
// ---------------------------------------------------------------------------

/// `DOMAIN_BEACON_PROPOSER = 0x00000000`
pub const DOMAIN_BEACON_PROPOSER: DomainType = DomainType::from_array([0x00, 0x00, 0x00, 0x00]);
/// `DOMAIN_BEACON_ATTESTER = 0x01000000`
pub const DOMAIN_BEACON_ATTESTER: DomainType = DomainType::from_array([0x01, 0x00, 0x00, 0x00]);
/// `DOMAIN_RANDAO = 0x02000000`
pub const DOMAIN_RANDAO: DomainType = DomainType::from_array([0x02, 0x00, 0x00, 0x00]);
/// `DOMAIN_DEPOSIT = 0x03000000`
pub const DOMAIN_DEPOSIT: DomainType = DomainType::from_array([0x03, 0x00, 0x00, 0x00]);
/// `DOMAIN_VOLUNTARY_EXIT = 0x04000000`
pub const DOMAIN_VOLUNTARY_EXIT: DomainType = DomainType::from_array([0x04, 0x00, 0x00, 0x00]);
/// `DOMAIN_SELECTION_PROOF = 0x05000000`
pub const DOMAIN_SELECTION_PROOF: DomainType = DomainType::from_array([0x05, 0x00, 0x00, 0x00]);
/// `DOMAIN_AGGREGATE_AND_PROOF = 0x06000000`
pub const DOMAIN_AGGREGATE_AND_PROOF: DomainType = DomainType::from_array([0x06, 0x00, 0x00, 0x00]);
/// `DOMAIN_SYNC_COMMITTEE = 0x07000000`
pub const DOMAIN_SYNC_COMMITTEE: DomainType = DomainType::from_array([0x07, 0x00, 0x00, 0x00]);
/// `DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF = 0x08000000`
pub const DOMAIN_SYNC_COMMITTEE_SELECTION_PROOF: DomainType =
    DomainType::from_array([0x08, 0x00, 0x00, 0x00]);
/// `DOMAIN_CONTRIBUTION_AND_PROOF = 0x09000000`
pub const DOMAIN_CONTRIBUTION_AND_PROOF: DomainType =
    DomainType::from_array([0x09, 0x00, 0x00, 0x00]);
/// `DOMAIN_BLS_TO_EXECUTION_CHANGE = 0x0A000000`
pub const DOMAIN_BLS_TO_EXECUTION_CHANGE: DomainType =
    DomainType::from_array([0x0a, 0x00, 0x00, 0x00]);
/// `DOMAIN_BEACON_BUILDER = 0x0B000000` (Gloas / builder; **not** `0x1B`)
pub const DOMAIN_BEACON_BUILDER: DomainType = DomainType::from_array([0x0b, 0x00, 0x00, 0x00]);
/// `DOMAIN_PTC_ATTESTER = 0x0C000000`
pub const DOMAIN_PTC_ATTESTER: DomainType = DomainType::from_array([0x0c, 0x00, 0x00, 0x00]);
/// `DOMAIN_PROPOSER_PREFERENCES = 0x0D000000`
pub const DOMAIN_PROPOSER_PREFERENCES: DomainType =
    DomainType::from_array([0x0d, 0x00, 0x00, 0x00]);
/// `DOMAIN_BUILDER_DEPOSIT = 0x0E000000`
pub const DOMAIN_BUILDER_DEPOSIT: DomainType = DomainType::from_array([0x0e, 0x00, 0x00, 0x00]);

/// Spec `SigningData` container: `object_root` + `domain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, TreeHash)]
pub struct SigningData {
    /// Root of the signed object (or epoch / slot as basic type root).
    pub object_root: Root,
    /// 32-byte domain from [`compute_domain`].
    pub domain: Domain,
}

/// `compute_fork_data_root(current_version, genesis_validators_root)`.
///
/// `genesis_validators_root` is taken by value with no default — callers must
/// pass the fetched value. Writing a zero root is greppable at the call site.
pub fn compute_fork_data_root(current_version: ForkVersion, genesis_validators_root: Root) -> Root {
    let fork_data = ForkData {
        current_version,
        genesis_validators_root,
    };
    Root::from_hash256(fork_data.tree_hash_root())
}

/// `compute_domain(domain_type, fork_version, genesis_validators_root)`.
///
/// When `fork_version` or `gvr` is `None`, the all-zero value is substituted
/// (deposit-style domains). Network genesis fork versions must be passed
/// explicitly — this crate does not embed a chain config.
pub fn compute_domain(
    domain_type: DomainType,
    fork_version: Option<ForkVersion>,
    gvr: Option<Root>,
) -> Domain {
    // Zero defaults via from_array so the greppable zero-root constructor stays
    // confined to fixtures and explicit call sites (Cross-Requirement Dependency 1).
    let fork_version = fork_version.unwrap_or_else(|| ForkVersion::from_array([0u8; 4]));
    let gvr = gvr.unwrap_or_else(|| Root::from_array([0u8; 32]));
    let fork_data_root = compute_fork_data_root(fork_version, gvr);
    let mut domain = [0u8; 32];
    domain[0..4].copy_from_slice(domain_type.as_slice());
    domain[4..32].copy_from_slice(&fork_data_root.as_slice()[0..28]);
    Domain::from_array(domain)
}

/// `compute_signing_root(object, domain) = hash_tree_root(SigningData)`.
pub fn compute_signing_root<T: TreeHash>(object: &T, domain: Domain) -> Root {
    let signing_data = SigningData {
        object_root: Root::from_hash256(object.tree_hash_root()),
        domain,
    };
    Root::from_hash256(signing_data.tree_hash_root())
}

/// `get_domain(fork, domain_type, epoch, genesis_validators_root)`.
///
/// Takes [`Fork`] rather than a full beacon-state container so `cc-crypto`
/// stays below the state-transition layer. When `epoch` is `None`, the current
/// fork version is used; when `Some(e)` and `e < fork.epoch`, the previous
/// version is used.
pub fn get_domain(
    fork: &Fork,
    domain_type: DomainType,
    epoch: Option<Epoch>,
    genesis_validators_root: Root,
) -> Domain {
    let fork_version = match epoch {
        Some(e) if e < fork.epoch => fork.previous_version,
        _ => fork.current_version,
    };
    compute_domain(
        domain_type,
        Some(fork_version),
        Some(genesis_validators_root),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use cc_types::Epoch;

    #[test]
    fn domain_constants_match_spec_bytes() {
        assert_eq!(DOMAIN_BEACON_PROPOSER.as_slice(), &[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(DOMAIN_BEACON_ATTESTER.as_slice(), &[0x01, 0x00, 0x00, 0x00]);
        assert_eq!(DOMAIN_RANDAO.as_slice(), &[0x02, 0x00, 0x00, 0x00]);
        assert_eq!(DOMAIN_SYNC_COMMITTEE.as_slice(), &[0x07, 0x00, 0x00, 0x00]);
        assert_eq!(
            DOMAIN_BLS_TO_EXECUTION_CHANGE.as_slice(),
            &[0x0a, 0x00, 0x00, 0x00]
        );
        assert_eq!(DOMAIN_BEACON_BUILDER.as_slice(), &[0x0b, 0x00, 0x00, 0x00]);
        assert_eq!(DOMAIN_PTC_ATTESTER.as_slice(), &[0x0c, 0x00, 0x00, 0x00]);
        assert_eq!(
            DOMAIN_PROPOSER_PREFERENCES.as_slice(),
            &[0x0d, 0x00, 0x00, 0x00]
        );
        assert_eq!(DOMAIN_BUILDER_DEPOSIT.as_slice(), &[0x0e, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn get_domain_selects_previous_version_before_fork_epoch() {
        let fork = Fork {
            previous_version: ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            current_version: ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            epoch: Epoch::new(50688),
        };
        let gvr = Root::from_array([0x11; 32]);
        let before = get_domain(&fork, DOMAIN_BEACON_PROPOSER, Some(Epoch::new(50687)), gvr);
        let after = get_domain(&fork, DOMAIN_BEACON_PROPOSER, Some(Epoch::new(50688)), gvr);
        let current = get_domain(&fork, DOMAIN_BEACON_PROPOSER, None, gvr);
        assert_ne!(before, after);
        assert_eq!(after, current);
        assert_eq!(
            before,
            compute_domain(
                DOMAIN_BEACON_PROPOSER,
                Some(fork.previous_version),
                Some(gvr)
            )
        );
    }

    #[test]
    fn compute_signing_root_of_root_is_stable() {
        let domain = compute_domain(
            DOMAIN_BEACON_PROPOSER,
            Some(ForkVersion::from_array([0x70, 0x00, 0x09, 0x10])),
            Some(Root::from_array([0x21; 32])),
        );
        let object = Root::from_array([0xab; 32]);
        let a = compute_signing_root(&object, domain);
        let b = compute_signing_root(&object, domain);
        assert_eq!(a, b);
        assert_ne!(a.as_slice(), &[0u8; 32]);
    }
}
