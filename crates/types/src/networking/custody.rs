//! Fulu DAS custody helpers (`fulu/das-core.md`, `fulu/p2p-interface.md`).
//!
//! Four pure functions for custody-group selection, column mapping, subnet
//! mapping, and sampling size. Uses `ethereum_hashing` directly (Architecture
//! §3.6) — no workspace edge added.

use std::collections::BTreeSet;

use alloy_primitives::U256;

use crate::{
    DATA_COLUMN_SIDECAR_SUBNET_COUNT, NUMBER_OF_COLUMNS, NUMBER_OF_CUSTODY_GROUPS, SAMPLES_PER_SLOT,
};

/// Custody group identifier (`CustodyIndex` = `Uint64`).
pub type CustodyIndex = u64;

/// Column identifier in the extended data matrix (`ColumnIndex` = `Uint64`).
pub type ColumnIndex = u64;

/// Gossip subnet identifier (`SubnetID` = `Uint64`).
pub type SubnetId = u64;

/// Select the custody groups a node with `node_id` must custody.
///
/// Spec: `fulu/das-core.md#get_custody_groups`. Returns a sorted,
/// duplicate-free set (enforced by [`BTreeSet`]).
///
/// # Panics
///
/// Panics if `custody_group_count > NUMBER_OF_CUSTODY_GROUPS` (spec assert).
#[allow(clippy::panic)] // Spec asserts on invalid custody_group_count
pub fn get_custody_groups(node_id: U256, custody_group_count: u64) -> BTreeSet<CustodyIndex> {
    assert!(
        custody_group_count <= NUMBER_OF_CUSTODY_GROUPS,
        "custody_group_count ({custody_group_count}) > NUMBER_OF_CUSTODY_GROUPS ({NUMBER_OF_CUSTODY_GROUPS})"
    );

    // Skip computation if all groups are custodied.
    if custody_group_count == NUMBER_OF_CUSTODY_GROUPS {
        return (0..NUMBER_OF_CUSTODY_GROUPS).collect();
    }

    let mut current_id = node_id;
    let mut custody_groups = BTreeSet::new();

    while (custody_groups.len() as u64) < custody_group_count {
        // hash(uint_to_bytes(current_id)) — SHA-256 over little-endian 32 bytes.
        let hash = ethereum_hashing::hash_fixed(&current_id.to_le_bytes::<32>());
        // bytes_to_uint64(hash[0:8]) % NUMBER_OF_CUSTODY_GROUPS
        let mut le = [0u8; 8];
        le.copy_from_slice(&hash[0..8]);
        let custody_group = u64::from_le_bytes(le) % NUMBER_OF_CUSTODY_GROUPS;
        custody_groups.insert(custody_group);

        // Overflow prevention: wrap UINT256_MAX → 0.
        if current_id == U256::MAX {
            current_id = U256::ZERO;
        } else {
            current_id += U256::from(1u64);
        }
    }

    custody_groups
}

/// Columns belonging to a single custody group.
///
/// Spec: `fulu/das-core.md#compute_columns_for_custody_group`.
///
/// # Panics
///
/// Panics if `custody_group >= NUMBER_OF_CUSTODY_GROUPS` (spec assert).
#[allow(clippy::panic)] // Spec asserts on invalid custody_group
pub fn compute_columns_for_custody_group(custody_group: CustodyIndex) -> Vec<ColumnIndex> {
    assert!(
        custody_group < NUMBER_OF_CUSTODY_GROUPS,
        "custody_group ({custody_group}) >= NUMBER_OF_CUSTODY_GROUPS ({NUMBER_OF_CUSTODY_GROUPS})"
    );
    let columns_per_group = NUMBER_OF_COLUMNS / NUMBER_OF_CUSTODY_GROUPS;
    (0..columns_per_group)
        .map(|i| NUMBER_OF_CUSTODY_GROUPS * i + custody_group)
        .collect()
}

/// Subnet for a data-column sidecar gossip topic.
///
/// Spec: `fulu/p2p-interface.md#compute_subnet_for_data_column_sidecar`.
#[must_use]
pub fn compute_subnet_for_data_column_sidecar(column_index: ColumnIndex) -> SubnetId {
    column_index % DATA_COLUMN_SIDECAR_SUBNET_COUNT
}

/// Number of custody groups to sample given an advertised custody count.
///
/// Spec: `fulu/das-core.md#custody-sampling` —
/// `sampling_size = max(SAMPLES_PER_SLOT, custody_group_count)`.
#[must_use]
pub fn sampling_size(custody_group_count: u64) -> u64 {
    SAMPLES_PER_SLOT.max(custody_group_count)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::CUSTODY_REQUIREMENT;

    #[test]
    fn sampling_size_at_minimum_custody_exceeds_custody() {
        // CC-1B/2: at minimum custody, sampling exceeds custody.
        assert_eq!(sampling_size(CUSTODY_REQUIREMENT), SAMPLES_PER_SLOT);
        assert_eq!(sampling_size(4), 8);
        assert_eq!(sampling_size(12), 12);
        assert_eq!(sampling_size(0), SAMPLES_PER_SLOT);
        assert_eq!(sampling_size(SAMPLES_PER_SLOT), SAMPLES_PER_SLOT);
    }

    #[test]
    fn get_custody_groups_size_stable_and_deterministic() {
        // CC-1B/3: fixed node_id → set of exact size, stable across calls.
        let node_id = U256::from(0xdead_beef_u64);
        let count = CUSTODY_REQUIREMENT;
        let a = get_custody_groups(node_id, count);
        let b = get_custody_groups(node_id, count);
        assert_eq!(a.len() as u64, count);
        assert_eq!(a, b);
        // BTreeSet is sorted + duplicate-free by construction.
        let as_vec: Vec<_> = a.iter().copied().collect();
        let mut sorted = as_vec.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(as_vec, sorted);
    }

    #[test]
    fn columns_partition_all_groups() {
        // Over all 128 groups, each of the 128 column indices appears exactly
        // `NUMBER_OF_COLUMNS / NUMBER_OF_CUSTODY_GROUPS` times (once when equal).
        let expected_hits = NUMBER_OF_COLUMNS / NUMBER_OF_CUSTODY_GROUPS;
        let mut hits = vec![0u64; NUMBER_OF_COLUMNS as usize];
        for group in 0..NUMBER_OF_CUSTODY_GROUPS {
            for col in compute_columns_for_custody_group(group) {
                assert!(
                    col < NUMBER_OF_COLUMNS,
                    "column {col} out of range for group {group}"
                );
                hits[col as usize] += 1;
            }
        }
        for (col, n) in hits.iter().enumerate() {
            assert_eq!(
                *n, expected_hits,
                "column {col} hit {n} times, expected {expected_hits}"
            );
        }
    }

    #[test]
    fn subnet_is_column_mod_subnet_count() {
        assert_eq!(compute_subnet_for_data_column_sidecar(0), 0);
        assert_eq!(compute_subnet_for_data_column_sidecar(127), 127);
        assert_eq!(
            compute_subnet_for_data_column_sidecar(128),
            0 % DATA_COLUMN_SIDECAR_SUBNET_COUNT
        );
        assert_eq!(
            compute_subnet_for_data_column_sidecar(129),
            129 % DATA_COLUMN_SIDECAR_SUBNET_COUNT
        );
    }

    #[test]
    fn get_custody_groups_full_set() {
        let groups = get_custody_groups(U256::ZERO, NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(groups.len() as u64, NUMBER_OF_CUSTODY_GROUPS);
        for i in 0..NUMBER_OF_CUSTODY_GROUPS {
            assert!(groups.contains(&i));
        }
    }

    #[test]
    fn get_custody_groups_empty() {
        let groups = get_custody_groups(U256::MAX, 0);
        assert!(groups.is_empty());
    }

    #[test]
    fn compute_columns_identity_when_one_per_group() {
        // Mainnet: NUMBER_OF_COLUMNS == NUMBER_OF_CUSTODY_GROUPS ⇒ one column each.
        assert_eq!(NUMBER_OF_COLUMNS, NUMBER_OF_CUSTODY_GROUPS);
        assert_eq!(compute_columns_for_custody_group(0), vec![0]);
        assert_eq!(compute_columns_for_custody_group(55), vec![55]);
        assert_eq!(compute_columns_for_custody_group(127), vec![127]);
    }
}
