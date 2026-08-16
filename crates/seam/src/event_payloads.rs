//! Typed `SubscribeEvents` payload layouts (P1-D/11 / S2-A-08).
//!
//! The ring still carries opaque `bytes`. Consumers must decode through
//! these structs. A short or unknown payload is `None` — never a silent
//! zero root, epoch, slot, or verdict.

use crate::Bytes;

/// Wire first byte of `BLOCK_IMPORTED`. Matches `ImportBlockVerdict`
/// (`IMPORTED = 1`, `DEFERRED_DA = 3`). Unknown values fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockImportedVerdict {
    Imported = 1,
    DeferredDa = 3,
}

impl BlockImportedVerdict {
    /// Wire first-byte value.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Fail-closed decode. `None` for an unknown discriminant (never default).
    #[must_use]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Imported),
            3 => Some(Self::DeferredDa),
            _ => None,
        }
    }
}

/// `BLOCK_IMPORTED` payload = `[verdict_byte] ‖ SignedBeaconBlock SSZ`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockImportedPayload<'a> {
    pub verdict: BlockImportedVerdict,
    pub block_ssz: &'a [u8],
}

impl<'a> BlockImportedPayload<'a> {
    /// Encode. The SSZ tail is written as-is.
    #[must_use]
    pub fn encode(verdict: BlockImportedVerdict, block_ssz: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(1 + block_ssz.len());
        out.push(verdict.as_u8());
        out.extend_from_slice(block_ssz);
        Bytes::from(out)
    }

    /// Fail-closed: first byte must be a known verdict.
    #[must_use]
    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        let (disc, rest) = bytes.split_first()?;
        Some(Self {
            verdict: BlockImportedVerdict::from_u8(*disc)?,
            block_ssz: rest,
        })
    }
}

/// `HEAD` payload = head slot as 8-byte little-endian.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadPayload {
    pub slot: u64,
}

impl HeadPayload {
    /// Encode.
    #[must_use]
    pub fn encode(slot: u64) -> Bytes {
        Bytes::copy_from_slice(&slot.to_le_bytes())
    }

    /// Fail-closed: requires exactly 8 bytes.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let slot: [u8; 8] = bytes.try_into().ok()?;
        Some(Self {
            slot: u64::from_le_bytes(slot),
        })
    }
}

/// Typed `CHAIN_REORG` payload. Decode is fail-closed (no silent zero root).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainReorgPayload {
    pub old_head_root: [u8; 32],
    pub common_ancestor_slot: u64,
}

impl ChainReorgPayload {
    /// Encode. Non-32-byte roots are written as-is (never padded to `0`).
    #[must_use]
    pub fn encode(old_head_root: &[u8], common_ancestor_slot: u64) -> Bytes {
        let mut payload = Vec::with_capacity(old_head_root.len().saturating_add(8));
        payload.extend_from_slice(old_head_root);
        payload.extend_from_slice(&common_ancestor_slot.to_le_bytes());
        Bytes::from(payload)
    }

    /// Fail-closed: requires exactly 40 bytes.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        const LEN: usize = 32 + 8;
        if bytes.len() != LEN {
            return None;
        }
        let (root, slot) = bytes.split_at(32);
        let old_head_root: [u8; 32] = root.try_into().ok()?;
        let slot: [u8; 8] = slot.try_into().ok()?;
        Some(Self {
            old_head_root,
            common_ancestor_slot: u64::from_le_bytes(slot),
        })
    }
}

/// Typed `FINALIZED_CHECKPOINT` prefix: 8 B epoch ‖ 32 B state root ‖ scalars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedCheckpointPayload {
    pub epoch: u64,
    pub state_root: [u8; 32],
}

impl FinalizedCheckpointPayload {
    /// Prefix length: epoch + state root.
    pub const PREFIX_LEN: usize = 8 + 32;

    /// Encode. Non-32-byte state roots are written as-is (never padded to `0`).
    #[must_use]
    pub fn encode(epoch: u64, state_root: &[u8], scalars_ssz: &[u8]) -> Bytes {
        let mut payload = Vec::with_capacity(8 + state_root.len() + scalars_ssz.len());
        payload.extend_from_slice(&epoch.to_le_bytes());
        payload.extend_from_slice(state_root);
        payload.extend_from_slice(scalars_ssz);
        Bytes::from(payload)
    }

    /// Fail-closed prefix decode. Requires at least [`Self::PREFIX_LEN`] bytes.
    #[must_use]
    pub fn decode_prefix(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::PREFIX_LEN {
            return None;
        }
        let (prefix, _) = bytes.split_at(Self::PREFIX_LEN);
        let (epoch, root) = prefix.split_at(8);
        let epoch: [u8; 8] = epoch.try_into().ok()?;
        let state_root: [u8; 32] = root.try_into().ok()?;
        Some(Self {
            epoch: u64::from_le_bytes(epoch),
            state_root,
        })
    }

    /// Fail-closed prefix + remaining scalars (empty tail is allowed).
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<(Self, &[u8])> {
        let prefix = Self::decode_prefix(bytes)?;
        let (_, scalars) = bytes.split_at(Self::PREFIX_LEN);
        Some((prefix, scalars))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn block_imported_unknown_verdict_is_none() {
        let body = b"ssz";
        let imported = BlockImportedPayload::encode(BlockImportedVerdict::Imported, body);
        let decoded = BlockImportedPayload::decode(&imported).unwrap();
        assert_eq!(decoded.verdict, BlockImportedVerdict::Imported);
        assert_eq!(decoded.block_ssz, body.as_slice());
        assert!(BlockImportedPayload::decode(&[]).is_none());
        assert!(BlockImportedPayload::decode(&[0]).is_none());
        assert!(BlockImportedPayload::decode(&[2, 1, 2, 3]).is_none());
    }

    #[test]
    fn head_decode_is_fail_closed() {
        let slot = 0x1122_3344_5566_7788u64;
        let encoded = HeadPayload::encode(slot);
        assert_eq!(HeadPayload::decode(&encoded).unwrap().slot, slot);
        assert!(HeadPayload::decode(&[0u8; 7]).is_none());
        assert!(HeadPayload::decode(&[0u8; 9]).is_none());
    }

    #[test]
    fn chain_reorg_decode_is_fail_closed() {
        let root = [0x11u8; 32];
        let encoded = ChainReorgPayload::encode(&root, 7);
        let decoded = ChainReorgPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.old_head_root, root);
        assert_eq!(decoded.common_ancestor_slot, 7);
        assert!(ChainReorgPayload::decode(&[0u8; 32]).is_none());
        let short = ChainReorgPayload::encode(&[0x22], 1);
        assert!(ChainReorgPayload::decode(&short).is_none());
        assert_ne!(short.len(), 40);
    }

    #[test]
    fn finalized_prefix_decode_is_fail_closed() {
        let sr = [0x33u8; 32];
        let encoded = FinalizedCheckpointPayload::encode(4, &sr, &[9, 9]);
        let decoded = FinalizedCheckpointPayload::decode_prefix(&encoded).unwrap();
        assert_eq!(decoded.epoch, 4);
        assert_eq!(decoded.state_root, sr);
        let (prefix, scalars) = FinalizedCheckpointPayload::decode(&encoded).unwrap();
        assert_eq!(prefix, decoded);
        assert_eq!(scalars, &[9, 9]);
        assert!(FinalizedCheckpointPayload::decode_prefix(&[0u8; 8]).is_none());
        let short = FinalizedCheckpointPayload::encode(1, &[0x01], &[]);
        assert!(FinalizedCheckpointPayload::decode_prefix(&short).is_none());
    }
}
