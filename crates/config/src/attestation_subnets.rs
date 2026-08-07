//! Attestation-subnet backbone configuration (phase0 p2p-interface / CC-2C).
//!
//! These are **runtime config values**, not compile-time preset constants. Hoodi
//! and mainnet share the defaults below; a self-devnet overrides them so
//! rotation periods and subnet counts match the generated network.
//!
//! Spec source: `specs/phase0/p2p-interface.md` configuration table +
//! `compute_subscribed_subnet`. The five values consumed by
//! [`crate::AttestationSubnetConfig`] and `services/p2p` SubnetManager are:
//!
//! | Field | Hoodi / mainnet default |
//! |-------|-------------------------|
//! | `subnets_per_node` | 2 |
//! | `epochs_per_subnet_subscription` | 256 (also the node-offset modulus) |
//! | `attestation_subnet_count` | 64 |
//! | `attestation_subnet_prefix_bits` | 6 |
//! | `node_id_bits` | 256 |

use serde::{Deserialize, Serialize};

// ── Named defaults (sole literal home for the five values) ──────────────────

/// Number of long-lived attestation subnets a node should subscribe to.
pub const SUBNETS_PER_NODE: u64 = 2;

/// Number of epochs a long-lived subnet subscription lasts.
///
/// Also the modulus for `node_offset = node_id % EPOCHS_PER_SUBNET_SUBSCRIPTION`.
pub const EPOCHS_PER_SUBNET_SUBSCRIPTION: u64 = 256;

/// Number of attestation subnets in the gossipsub protocol.
pub const ATTESTATION_SUBNET_COUNT: u64 = 64;

/// Extra NodeId bits for subnet mapping (spec `ATTESTATION_SUBNET_EXTRA_BITS`).
///
/// Mainnet / Hoodi keep this at 0; prefix bits are then `ceillog2(count)`.
pub const ATTESTATION_SUBNET_EXTRA_BITS: u64 = 0;

/// NodeId bit-width used when extracting the subnet prefix (`NODE_ID_BITS`).
pub const NODE_ID_BITS: u64 = 256;

/// `ceillog2(ATTESTATION_SUBNET_COUNT) + ATTESTATION_SUBNET_EXTRA_BITS` for the
/// Hoodi / mainnet defaults (`= 6`). Stored explicitly so a non-power-of-two
/// count can be overridden without silent recomputation.
pub const ATTESTATION_SUBNET_PREFIX_BITS: u64 = 6;

// ── Runtime config ──────────────────────────────────────────────────────────

/// Runtime attestation-subnet parameters for the long-lived backbone.
///
/// Constructed from service config / network profile. [`Self::hoodi`] (alias
/// [`Self::mainnet`]) returns the phase0 defaults; tests and devnets pass a
/// custom value so rotation periods shrink to something exercisable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttestationSubnetConfig {
    /// `SUBNETS_PER_NODE` — long-lived backbone size.
    pub subnets_per_node: u64,
    /// `EPOCHS_PER_SUBNET_SUBSCRIPTION` — rotation period and node-offset modulus.
    pub epochs_per_subnet_subscription: u64,
    /// `ATTESTATION_SUBNET_COUNT` — gossip topic / ENR bitvector length.
    pub attestation_subnet_count: u64,
    /// `ATTESTATION_SUBNET_PREFIX_BITS` — bits of NodeId used as the shuffle index.
    pub attestation_subnet_prefix_bits: u64,
    /// `NODE_ID_BITS` — NodeId width (always 256 on discv5).
    pub node_id_bits: u64,
}

impl AttestationSubnetConfig {
    /// Hoodi / mainnet phase0 networking defaults.
    #[must_use]
    pub const fn hoodi() -> Self {
        Self {
            subnets_per_node: SUBNETS_PER_NODE,
            epochs_per_subnet_subscription: EPOCHS_PER_SUBNET_SUBSCRIPTION,
            attestation_subnet_count: ATTESTATION_SUBNET_COUNT,
            attestation_subnet_prefix_bits: ATTESTATION_SUBNET_PREFIX_BITS,
            node_id_bits: NODE_ID_BITS,
        }
    }

    /// Alias of [`Self::hoodi`] (same preset base).
    #[must_use]
    pub const fn mainnet() -> Self {
        Self::hoodi()
    }

    /// Whether the config is usable (non-zero counts, prefix fits in a `u64` shift).
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.subnets_per_node > 0
            && self.epochs_per_subnet_subscription > 0
            && self.attestation_subnet_count > 0
            && self.attestation_subnet_prefix_bits > 0
            && self.attestation_subnet_prefix_bits < 64
            && self.node_id_bits > 0
            && self.node_id_bits >= self.attestation_subnet_prefix_bits
            && self.subnets_per_node <= self.attestation_subnet_count
    }

    /// Shuffle domain size: `1 << attestation_subnet_prefix_bits`.
    #[must_use]
    pub const fn shuffle_index_count(self) -> u64 {
        1u64 << self.attestation_subnet_prefix_bits
    }
}

impl Default for AttestationSubnetConfig {
    fn default() -> Self {
        Self::hoodi()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn hoodi_matches_phase0_table() {
        let c = AttestationSubnetConfig::hoodi();
        assert_eq!(c.subnets_per_node, 2);
        assert_eq!(c.epochs_per_subnet_subscription, 256);
        assert_eq!(c.attestation_subnet_count, 64);
        assert_eq!(c.attestation_subnet_prefix_bits, 6);
        assert_eq!(c.node_id_bits, 256);
        assert_eq!(c.shuffle_index_count(), 64);
        assert!(c.is_valid());
    }

    #[test]
    fn named_defaults_match_struct() {
        let c = AttestationSubnetConfig::hoodi();
        assert_eq!(c.subnets_per_node, SUBNETS_PER_NODE);
        assert_eq!(
            c.epochs_per_subnet_subscription,
            EPOCHS_PER_SUBNET_SUBSCRIPTION
        );
        assert_eq!(c.attestation_subnet_count, ATTESTATION_SUBNET_COUNT);
        assert_eq!(
            c.attestation_subnet_prefix_bits,
            ATTESTATION_SUBNET_PREFIX_BITS
        );
        assert_eq!(c.node_id_bits, NODE_ID_BITS);
        let _ = ATTESTATION_SUBNET_EXTRA_BITS;
    }
}
