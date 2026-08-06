//! Consensus primitive newtypes (Architecture §3.1).
//!
//! Scalar identifiers and fixed-byte types with SSZ + TreeHash as their inner
//! representation (transparent encoding) and checked arithmetic helpers.

use std::fmt;
use std::ops::{Add, Sub};

use ssz_derive::{Decode, Encode};
use tree_hash::{Hash256 as ThHash256, PackedEncoding, TreeHash, TreeHashType};

/// Tree-hash / merkle leaf type (alloy `B256` via `tree_hash`).
pub type Hash256 = ThHash256;

// ---------------------------------------------------------------------------
// u64 newtypes
// ---------------------------------------------------------------------------

macro_rules! u64_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
        #[ssz(struct_behaviour = "transparent")]
        pub struct $name(u64);

        impl $name {
            /// Construct from a raw `u64`.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Zero value.
            pub const ZERO: Self = Self(0);

            /// Borrow the inner `u64`.
            pub const fn as_u64(self) -> u64 {
                self.0
            }

            /// Consume into the inner `u64`.
            pub const fn into_u64(self) -> u64 {
                self.0
            }

            /// Checked addition; `None` on overflow.
            pub const fn checked_add(self, rhs: u64) -> Option<Self> {
                match self.0.checked_add(rhs) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }

            /// Checked subtraction; `None` on underflow.
            pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
                match self.0.checked_sub(rhs) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }

            /// Saturating addition.
            pub const fn saturating_add(self, rhs: u64) -> Self {
                Self(self.0.saturating_add(rhs))
            }

            /// Saturating subtraction.
            pub const fn saturating_sub(self, rhs: u64) -> Self {
                Self(self.0.saturating_sub(rhs))
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> u64 {
                value.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        /// Unchecked `+`. Panics on overflow when `overflow-checks` is on
        /// (workspace release profile). Prefer [`Self::checked_add`] for
        /// input-derived values in state transition.
        impl Add<u64> for $name {
            type Output = Self;

            fn add(self, rhs: u64) -> Self::Output {
                Self(self.0 + rhs)
            }
        }

        /// Unchecked `-`. Panics on underflow when `overflow-checks` is on
        /// (workspace release profile). Prefer [`Self::checked_sub`] for
        /// input-derived values in state transition.
        impl Sub<u64> for $name {
            type Output = Self;

            fn sub(self, rhs: u64) -> Self::Output {
                Self(self.0 - rhs)
            }
        }

        impl TreeHash for $name {
            fn tree_hash_type() -> TreeHashType {
                TreeHashType::Basic
            }

            fn tree_hash_packed_encoding(&self) -> PackedEncoding {
                self.0.tree_hash_packed_encoding()
            }

            fn tree_hash_packing_factor() -> usize {
                u64::tree_hash_packing_factor()
            }

            fn tree_hash_root(&self) -> ThHash256 {
                self.0.tree_hash_root()
            }
        }
    };
}

u64_newtype!(
    /// Slot number.
    Slot
);
u64_newtype!(
    /// Epoch number.
    Epoch
);
u64_newtype!(
    /// Validator registry index.
    ValidatorIndex
);
u64_newtype!(
    /// Committee index within a slot.
    CommitteeIndex
);
u64_newtype!(
    /// Gwei denomination.
    Gwei
);

impl Slot {
    /// Epoch containing this slot for the given `slots_per_epoch`.
    pub const fn epoch(self, slots_per_epoch: u64) -> Epoch {
        Epoch::new(self.0 / slots_per_epoch)
    }
}

impl Epoch {
    /// First slot of this epoch for the given `slots_per_epoch`.
    pub const fn start_slot(self, slots_per_epoch: u64) -> Slot {
        Slot::new(self.0.saturating_mul(slots_per_epoch))
    }
}

// ---------------------------------------------------------------------------
// Fixed-byte newtypes
// ---------------------------------------------------------------------------

macro_rules! bytes_newtype {
    ($(#[$meta:meta])* $name:ident, $n:expr) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
        #[ssz(struct_behaviour = "transparent")]
        pub struct $name(pub [u8; $n]);

        impl $name {
            /// All-zero bytes.
            pub const ZERO: Self = Self([0u8; $n]);

            /// Byte length.
            pub const LEN: usize = $n;

            /// Construct from a fixed array.
            pub const fn from_array(bytes: [u8; $n]) -> Self {
                Self(bytes)
            }

            /// Borrow as a byte slice.
            pub fn as_slice(&self) -> &[u8] {
                &self.0
            }

            /// Borrow the fixed array.
            pub const fn as_array(&self) -> &[u8; $n] {
                &self.0
            }

            /// Consume into the fixed array.
            pub const fn into_array(self) -> [u8; $n] {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::ZERO
            }
        }

        impl From<[u8; $n]> for $name {
            fn from(bytes: [u8; $n]) -> Self {
                Self(bytes)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}(0x", stringify!($name))?;
                for b in &self.0 {
                    write!(f, "{b:02x}")?;
                }
                write!(f, ")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "0x")?;
                for b in &self.0 {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }

        // Manual TreeHash: derive only supports named fields; delegate to the array.
        impl TreeHash for $name {
            fn tree_hash_type() -> TreeHashType {
                <[u8; $n] as TreeHash>::tree_hash_type()
            }

            fn tree_hash_packed_encoding(&self) -> PackedEncoding {
                self.0.tree_hash_packed_encoding()
            }

            fn tree_hash_packing_factor() -> usize {
                <[u8; $n] as TreeHash>::tree_hash_packing_factor()
            }

            fn tree_hash_root(&self) -> ThHash256 {
                self.0.tree_hash_root()
            }
        }
    };
}

bytes_newtype!(
    /// 32-byte Merkle root / state root / block root.
    Root,
    32
);
bytes_newtype!(
    /// 4-byte fork version.
    ForkVersion,
    4
);
bytes_newtype!(
    /// 4-byte domain type (e.g. `DOMAIN_BEACON_PROPOSER`).
    DomainType,
    4
);
bytes_newtype!(
    /// 32-byte signing domain.
    Domain,
    32
);
bytes_newtype!(
    /// 48-byte BLS public key.
    BlsPublicKey,
    48
);
bytes_newtype!(
    /// 96-byte BLS signature.
    BlsSignature,
    96
);
bytes_newtype!(
    /// 20-byte execution-layer address.
    ExecutionAddress,
    20
);
bytes_newtype!(
    /// 48-byte KZG commitment.
    KzgCommitment,
    48
);
bytes_newtype!(
    /// 48-byte KZG proof.
    KzgProof,
    48
);

impl Root {
    /// View as a `tree_hash::Hash256`.
    pub fn to_hash256(self) -> Hash256 {
        Hash256::from(self.0)
    }

    /// Construct from a `tree_hash::Hash256`.
    pub fn from_hash256(hash: Hash256) -> Self {
        Self(hash.into())
    }
}

impl From<Hash256> for Root {
    fn from(hash: Hash256) -> Self {
        Self::from_hash256(hash)
    }
}

impl From<Root> for Hash256 {
    fn from(root: Root) -> Self {
        root.to_hash256()
    }
}

/// A DAS cell: `BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_CELL` = 2048 bytes.
///
/// Manual `Debug` so a failing assertion does not dump 2 KB of hex (Architecture §3.2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[ssz(struct_behaviour = "transparent")]
pub struct Cell(pub [u8; crate::BYTES_PER_CELL]);

impl Cell {
    /// All-zero cell.
    pub const ZERO: Self = Self([0u8; crate::BYTES_PER_CELL]);

    /// Byte length of a cell.
    pub const LEN: usize = crate::BYTES_PER_CELL;

    /// Construct from a fixed array.
    pub const fn from_array(bytes: [u8; crate::BYTES_PER_CELL]) -> Self {
        Self(bytes)
    }

    /// Borrow as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Default for Cell {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<[u8; crate::BYTES_PER_CELL]> for Cell {
    fn from(bytes: [u8; crate::BYTES_PER_CELL]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for Cell {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TreeHash for Cell {
    fn tree_hash_type() -> TreeHashType {
        <[u8; crate::BYTES_PER_CELL] as TreeHash>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> PackedEncoding {
        self.0.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <[u8; crate::BYTES_PER_CELL] as TreeHash>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> ThHash256 {
        self.0.tree_hash_root()
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Compact: length + first/last 4 bytes.
        write!(
            f,
            "Cell(len={}, head={:02x}{:02x}{:02x}{:02x}…tail={:02x}{:02x}{:02x}{:02x})",
            Self::LEN,
            self.0[0],
            self.0[1],
            self.0[2],
            self.0[3],
            self.0[Self::LEN - 4],
            self.0[Self::LEN - 3],
            self.0[Self::LEN - 2],
            self.0[Self::LEN - 1],
        )
    }
}

// ---------------------------------------------------------------------------
// Hex helpers for config parsing
// ---------------------------------------------------------------------------

/// Parse a `0x`-prefixed hex string into a fixed-size byte array.
pub fn parse_hex_bytes<const N: usize>(s: &str) -> Result<[u8; N], HexParseError> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != N * 2 {
        return Err(HexParseError::Length {
            expected: N * 2,
            actual: hex.len(),
        });
    }
    let mut out = [0u8; N];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, HexParseError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(HexParseError::InvalidNibble(b)),
    }
}

/// Error parsing a hex string into fixed bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HexParseError {
    /// Wrong number of hex characters.
    #[error("hex length {actual}, expected {expected}")]
    Length {
        /// Expected character count (without `0x`).
        expected: usize,
        /// Actual character count (without `0x`).
        actual: usize,
    },
    /// Non-hex character.
    #[error("invalid hex nibble: {0}")]
    InvalidNibble(u8),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use ssz::{Decode, Encode};
    use tree_hash::TreeHash;

    #[test]
    fn slot_ssz_roundtrip() {
        let slot = Slot::new(42);
        let bytes = slot.as_ssz_bytes();
        assert_eq!(bytes, 42u64.to_le_bytes());
        let decoded = Slot::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(decoded, slot);
    }

    #[test]
    fn slot_tree_hash_matches_u64() {
        let slot = Slot::new(42);
        assert_eq!(slot.tree_hash_root(), 42u64.tree_hash_root());
    }

    #[test]
    fn root_ssz_roundtrip() {
        let mut arr = [0u8; 32];
        arr[0] = 0xde;
        arr[1] = 0xad;
        let root = Root::from_array(arr);
        let bytes = root.as_ssz_bytes();
        assert_eq!(bytes, arr);
        let decoded = Root::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(decoded, root);
    }

    #[test]
    fn root_tree_hash_matches_array() {
        let arr = [0xabu8; 32];
        let root = Root::from_array(arr);
        assert_eq!(root.tree_hash_root(), arr.tree_hash_root());
    }

    #[test]
    fn fork_version_ssz_roundtrip() {
        let fv = ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]);
        let bytes = fv.as_ssz_bytes();
        assert_eq!(bytes, [0x70, 0x00, 0x09, 0x10]);
        assert_eq!(
            ForkVersion::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}")),
            fv
        );
    }

    #[test]
    fn bls_pubkey_ssz_len() {
        let pk = BlsPublicKey::ZERO;
        assert_eq!(pk.as_ssz_bytes().len(), 48);
        assert_eq!(
            BlsPublicKey::from_ssz_bytes(&pk.as_ssz_bytes()).unwrap_or_else(|e| panic!("{e:?}")),
            pk
        );
    }

    #[test]
    fn bls_signature_ssz_len() {
        let sig = BlsSignature::ZERO;
        assert_eq!(sig.as_ssz_bytes().len(), 96);
    }

    #[test]
    fn kzg_commitment_ssz_len() {
        let c = KzgCommitment::ZERO;
        assert_eq!(c.as_ssz_bytes().len(), 48);
        assert_eq!(c.tree_hash_root(), [0u8; 48].tree_hash_root());
    }

    #[test]
    fn cell_ssz_len_and_debug() {
        let cell = Cell::ZERO;
        assert_eq!(cell.as_ssz_bytes().len(), crate::BYTES_PER_CELL);
        let dbg = format!("{cell:?}");
        assert!(dbg.contains("Cell(len=2048"));
        assert!(!dbg.contains(&"00".repeat(100)));
    }

    #[test]
    fn checked_add_overflow_returns_none() {
        let slot = Slot::new(u64::MAX);
        assert_eq!(slot.checked_add(1), None);
        assert_eq!(slot.checked_add(0), Some(Slot::new(u64::MAX)));
    }

    #[test]
    fn checked_sub_underflow_returns_none() {
        let epoch = Epoch::new(0);
        assert_eq!(epoch.checked_sub(1), None);
        assert_eq!(Epoch::new(5).checked_sub(3), Some(Epoch::new(2)));
    }

    #[test]
    fn gwei_checked_at_u64_max_boundary() {
        let g = Gwei::new(u64::MAX);
        assert!(g.checked_add(1).is_none());
        assert!(Gwei::new(0).checked_sub(1).is_none());
    }

    #[test]
    fn parse_hex_bytes_fork_version() {
        let bytes = parse_hex_bytes::<4>("0x70000910").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(bytes, [0x70, 0x00, 0x09, 0x10]);
    }
}
