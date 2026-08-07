//! Subnet and custody predicates for discv5 queries — Architecture §6.4, CC-21c.
//!
//! Predicates are closures over `&Enr` evaluated by discv5 during
//! `find_node_predicate`. Each factory returns `Box<dyn Fn(&Enr) -> bool + Send>`
//! matching the discv5 0.11 signature recorded in `docs/p2p-dependencies.md` §14/5.

use cc_types::ForkDigest;
use discv5::Enr;

use super::enr::{
    attnets_has, enr_custody_groups, read_eth2, syncnets_has,
};

/// Extract the advertised fork digest from an ENR's `eth2` field, if present.
#[must_use]
pub fn enr_fork_digest(enr: &Enr) -> Option<ForkDigest> {
    read_eth2(enr).map(|id| id.fork_digest)
}

/// True when the ENR's `eth2.fork_digest` matches `current`.
///
/// Inside an Overlap window the caller may also accept the next digest by
/// composing predicates; this helper is the exact-match arm.
#[must_use]
pub fn digest_matches(enr: &Enr, current: ForkDigest) -> bool {
    enr_fork_digest(enr) == Some(current)
}

/// Generic peer predicate: digest match (current, or next inside Overlap).
///
/// `allowed` is the set of digests we accept (usually `{current}` or
/// `{current, next}` during Overlap — built by
/// [`crate::fork_digest::discovery_allowed_digests`] / CC-2A).
#[must_use]
pub fn generic_peer_predicate(
    allowed: Vec<ForkDigest>,
) -> Box<dyn Fn(&Enr) -> bool + Send> {
    Box::new(move |enr: &Enr| {
        enr_fork_digest(enr).is_some_and(|d| allowed.contains(&d))
    })
}

/// Attestation subnet *s*: digest match and `enr.attnets()[s]`.
#[must_use]
pub fn attestation_subnet_predicate(
    allowed: Vec<ForkDigest>,
    subnet: u8,
) -> Box<dyn Fn(&Enr) -> bool + Send> {
    Box::new(move |enr: &Enr| {
        enr_fork_digest(enr).is_some_and(|d| allowed.contains(&d)) && attnets_has(enr, subnet)
    })
}

/// Sync subnet *s*: digest match and `enr.syncnets()[s]`.
#[must_use]
pub fn sync_subnet_predicate(
    allowed: Vec<ForkDigest>,
    subnet: u8,
) -> Box<dyn Fn(&Enr) -> bool + Send> {
    Box::new(move |enr: &Enr| {
        enr_fork_digest(enr).is_some_and(|d| allowed.contains(&d)) && syncnets_has(enr, subnet)
    })
}

/// Column *c*: digest match and custody set of the ENR covers column `c`.
///
/// Custody is **computable from the ENR alone** via
/// `get_custody_groups(node_id, cgc.unwrap_or(CUSTODY_REQUIREMENT))` — no
/// handshake required (Architecture §6.4).
#[must_use]
pub fn column_predicate(
    allowed: Vec<ForkDigest>,
    column: u64,
) -> Box<dyn Fn(&Enr) -> bool + Send> {
    Box::new(move |enr: &Enr| {
        if !enr_fork_digest(enr).is_some_and(|d| allowed.contains(&d)) {
            return false;
        }
        enr_custody_groups(enr).contains(&column)
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::discovery::enr::{
        ENR_KEY_ATTNETS, ENR_KEY_CGC, ENR_KEY_ETH2, ENR_KEY_SYNCNETS, EnrFieldChange, EnrManager,
        EnrSeqStrategy, encode_attnets, encode_cgc, encode_eth2, encode_syncnets, enr_custody_groups,
        node_id_as_u256,
    };
    use crate::fork_digest::EnrForkId;
    use cc_types::{
        CUSTODY_REQUIREMENT, Epoch, ForkVersion, NUMBER_OF_CUSTODY_GROUPS, get_custody_groups,
    };
    use discv5::enr::CombinedKey;

    fn digest_a() -> ForkDigest {
        ForkDigest::from_array([0xaa, 0xbb, 0xcc, 0xdd])
    }
    fn digest_b() -> ForkDigest {
        ForkDigest::from_array([0x11, 0x22, 0x33, 0x44])
    }

    fn enr_with(
        eth2_digest: ForkDigest,
        attnets: u64,
        syncnets: u8,
        cgc: Option<u64>,
    ) -> discv5::Enr {
        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let eth2 = EnrForkId {
            fork_digest: eth2_digest,
            next_fork_version: ForkVersion::ZERO,
            next_fork_epoch: Epoch::new(u64::MAX),
        };
        let mut changes = vec![
            EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(eth2)),
            EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(attnets)),
            EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(syncnets)),
        ];
        if let Some(c) = cgc {
            changes.push(EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(c)));
        }
        manager.apply(changes).unwrap();
        manager.local_enr()
    }

    #[test]
    fn generic_accepts_matching_digest_rejects_wrong() {
        let good = enr_with(digest_a(), 0, 0, None);
        let bad = enr_with(digest_b(), 0, 0, None);
        let pred = generic_peer_predicate(vec![digest_a()]);
        assert!(pred(&good));
        assert!(!pred(&bad));
    }

    #[test]
    fn attnets_requires_bit_and_digest() {
        let set = enr_with(digest_a(), 1u64 << 5, 0, None);
        let clear = enr_with(digest_a(), 0, 0, None);
        let wrong_digest = enr_with(digest_b(), 1u64 << 5, 0, None);
        let pred = attestation_subnet_predicate(vec![digest_a()], 5);
        assert!(pred(&set));
        assert!(!pred(&clear));
        assert!(!pred(&wrong_digest));
    }

    #[test]
    fn syncnets_requires_bit_and_digest() {
        let set = enr_with(digest_a(), 0, 1u8 << 2, None);
        let clear = enr_with(digest_a(), 0, 0, None);
        let pred = sync_subnet_predicate(vec![digest_a()], 2);
        assert!(pred(&set));
        assert!(!pred(&clear));
    }

    #[test]
    fn column_predicate_uses_cgc_and_default() {
        // Build an ENR with explicit cgc = NUMBER_OF_CUSTODY_GROUPS so every
        // column is covered; and one with cgc=0 groups empty.
        let full = enr_with(digest_a(), 0, 0, Some(NUMBER_OF_CUSTODY_GROUPS));
        let empty = enr_with(digest_a(), 0, 0, Some(0));
        // cgc missing → CUSTODY_REQUIREMENT default.
        let defaulted = enr_with(digest_a(), 0, 0, None);

        let col = *enr_custody_groups(&defaulted)
            .iter()
            .next()
            .expect("default custody non-empty");
        let pred_hit = column_predicate(vec![digest_a()], col);
        let pred_miss = column_predicate(vec![digest_a()], 9_999); // out of range never covered

        assert!(pred_hit(&full), "full custody covers any real column");
        assert!(pred_hit(&defaulted), "default cgc covers its own groups");
        assert!(!pred_hit(&empty), "cgc=0 covers nothing");
        assert!(!pred_miss(&full));

        // Sanity: defaulted groups match get_custody_groups(node_id, CUSTODY_REQUIREMENT).
        let expected = get_custody_groups(node_id_as_u256(defaulted.node_id()), CUSTODY_REQUIREMENT);
        assert_eq!(enr_custody_groups(&defaulted), expected);

        // Keep CombinedKey import honest (identity coupling).
        let _ = CombinedKey::generate_secp256k1();
    }
}
