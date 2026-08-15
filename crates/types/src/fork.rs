//! Fork types (Architecture §3.1).
//!
//! `ForkName` is activation-ordered. Runtime schedule walks live on
//! [`crate::config::ChainConfig`] (`fork_name_at_epoch` and siblings).
//! `ForkName::Fulu` is the only active Phase 1 variant (D1); the enum exists so
//! `context_deserialize` (CC-10d) can take a fork context.

use std::fmt;
use std::str::FromStr;

use ssz_derive::{Decode, Encode};
use tree_hash::{PackedEncoding, TreeHash, TreeHashType};
use tree_hash_derive::TreeHash;

use crate::primitives::{Epoch, ForkVersion, Root};

/// Spec `FAR_FUTURE_EPOCH = 2**64 - 1`.
///
/// Unscheduled forks use this activation epoch and are skipped by
/// [`crate::config::ChainConfig`] schedule accessors.
pub const FAR_FUTURE_EPOCH: Epoch = Epoch::new(u64::MAX);

/// Named consensus fork. Phase 1 activates only [`ForkName::Fulu`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ForkName {
    /// Phase 0 / genesis.
    Base,
    /// Altair.
    Altair,
    /// Bellatrix (The Merge).
    Bellatrix,
    /// Capella.
    Capella,
    /// Deneb.
    Deneb,
    /// Electra.
    Electra,
    /// Fulu (Fusaka / PeerDAS) — the Phase 1 active fork.
    Fulu,
}

impl ForkName {
    /// Lowercase fork name as used in Eth-Consensus-Version headers and paths.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "phase0",
            Self::Altair => "altair",
            Self::Bellatrix => "bellatrix",
            Self::Capella => "capella",
            Self::Deneb => "deneb",
            Self::Electra => "electra",
            Self::Fulu => "fulu",
        }
    }

    /// Every variant in activation order.
    pub const fn all() -> [Self; 7] {
        [
            Self::Base,
            Self::Altair,
            Self::Bellatrix,
            Self::Capella,
            Self::Deneb,
            Self::Electra,
            Self::Fulu,
        ]
    }

    /// Every variant newest-first — the `ChainConfig` schedule-table order.
    pub const fn all_descending() -> [Self; 7] {
        [
            Self::Fulu,
            Self::Electra,
            Self::Deneb,
            Self::Capella,
            Self::Bellatrix,
            Self::Altair,
            Self::Base,
        ]
    }
}

impl fmt::Display for ForkName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error parsing a fork name string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown fork name: {0}")]
pub struct UnknownForkName(pub String);

impl FromStr for ForkName {
    type Err = UnknownForkName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "phase0" | "base" | "genesis" => Ok(Self::Base),
            "altair" => Ok(Self::Altair),
            "bellatrix" => Ok(Self::Bellatrix),
            "capella" => Ok(Self::Capella),
            "deneb" => Ok(Self::Deneb),
            "electra" => Ok(Self::Electra),
            "fulu" => Ok(Self::Fulu),
            other => Err(UnknownForkName(other.to_string())),
        }
    }
}

/// Spec `Fork` container: previous/current version + epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct Fork {
    /// Previous fork version.
    pub previous_version: ForkVersion,
    /// Current fork version.
    pub current_version: ForkVersion,
    /// Epoch at which `current_version` activates.
    pub epoch: Epoch,
}

/// Spec `ForkData` container used in domain computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct ForkData {
    /// Current fork version.
    pub current_version: ForkVersion,
    /// Genesis validators root.
    pub genesis_validators_root: Root,
}

/// 4-byte fork digest (first 4 bytes of `hash_tree_root(ForkData)` or BPO-extended form).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[ssz(struct_behaviour = "transparent")]
pub struct ForkDigest(pub [u8; 4]);

impl ForkDigest {
    /// All-zero digest.
    pub const ZERO: Self = Self([0u8; 4]);

    /// Construct from a fixed array.
    pub const fn from_array(bytes: [u8; 4]) -> Self {
        Self(bytes)
    }

    /// Borrow as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Default for ForkDigest {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<[u8; 4]> for ForkDigest {
    fn from(bytes: [u8; 4]) -> Self {
        Self(bytes)
    }
}

impl TreeHash for ForkDigest {
    fn tree_hash_type() -> TreeHashType {
        <[u8; 4] as TreeHash>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> PackedEncoding {
        self.0.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <[u8; 4] as TreeHash>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> tree_hash::Hash256 {
        self.0.tree_hash_root()
    }
}

impl fmt::Debug for ForkDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ForkDigest(0x{:02x}{:02x}{:02x}{:02x})",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

impl fmt::Display for ForkDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "0x{:02x}{:02x}{:02x}{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn all_descending_is_reverse_of_all() {
        let all = ForkName::all();
        let descending = ForkName::all_descending();
        for (i, name) in all.iter().enumerate() {
            assert_eq!(descending[all.len() - 1 - i], *name);
        }
        assert_eq!(FAR_FUTURE_EPOCH.as_u64(), u64::MAX);
    }

    #[test]
    fn fork_name_roundtrip_str() {
        for name in ForkName::all() {
            assert_eq!(
                ForkName::from_str(name.as_str()).unwrap_or_else(|e| panic!("{e}")),
                name
            );
        }
        assert_eq!(
            ForkName::from_str("fulu").unwrap_or_else(|e| panic!("{e}")),
            ForkName::Fulu
        );
    }

    #[test]
    fn fork_ssz_and_tree_hash() {
        let fork = Fork {
            previous_version: ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            current_version: ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            epoch: Epoch::new(50688),
        };
        let bytes = fork.as_ssz_bytes();
        assert_eq!(
            Fork::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}")),
            fork
        );
        // Container root is non-zero for non-default content.
        assert_ne!(fork.tree_hash_root(), tree_hash::Hash256::ZERO);
    }

    #[test]
    fn fork_data_ssz_roundtrip() {
        let fd = ForkData {
            current_version: ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            genesis_validators_root: Root::ZERO,
        };
        let bytes = fd.as_ssz_bytes();
        assert_eq!(
            ForkData::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}")),
            fd
        );
    }

    #[test]
    fn fork_digest_ssz() {
        let d = ForkDigest::from_array([0xaa, 0xbb, 0xcc, 0xdd]);
        assert_eq!(d.as_ssz_bytes(), [0xaa, 0xbb, 0xcc, 0xdd]);
    }
}
