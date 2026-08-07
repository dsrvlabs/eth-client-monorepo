//! `MetaData v3` — Architecture §7.4 / CC-23b.
//!
//! Four fields: `seq_number`, `attnets` (`BitVector[64]`), `syncnets`
//! (`BitVector[4]`), `custody_group_count`. `seq_number` bumps on **any**
//! change to the other three. Phase 2 keeps attnets/syncnets empty until
//! CC-2C / CC-2D; `custody_group_count` is mutable at CC-21d.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use cc_types::CUSTODY_REQUIREMENT;

use crate::channels::GoodbyeReason;
use crate::discovery::enr::{ATTNETS_BIT_LEN, SYNCNETS_BIT_LEN};
use crate::reqresp::codec::{ResponseChunk, SszSnappyFraming};
use crate::reqresp::Protocol;

/// Fixed SSZ length of `MetaData v3` (8+8+1+8).
pub const METADATA_V3_SSZ_LEN: usize = 25;

/// Ethereum `MetaData v3` response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetaDataV3 {
    /// Sequence number; bumps when any of the other three change.
    pub seq_number: u64,
    /// Attestation subnet bitfield (`BitVector[64]` packed LE into `u64`).
    pub attnets: u64,
    /// Sync-committee subnet bitfield (`BitVector[4]` in the low nibble).
    pub syncnets: u8,
    /// Custody group count advertised to peers.
    pub custody_group_count: u64,
}

impl MetaDataV3 {
    /// Field count for fixtures / acceptance (always four).
    #[must_use]
    pub const fn field_count() -> usize {
        4
    }

    /// SSZ-encode (fixed 25 bytes).
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; METADATA_V3_SSZ_LEN] {
        let mut out = [0u8; METADATA_V3_SSZ_LEN];
        out[0..8].copy_from_slice(&self.seq_number.to_le_bytes());
        out[8..16].copy_from_slice(&self.attnets.to_le_bytes());
        out[16] = self.syncnets & 0x0f;
        out[17..25].copy_from_slice(&self.custody_group_count.to_le_bytes());
        out
    }

    /// SSZ-decode; rejects wrong lengths (a three-field body fails).
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != METADATA_V3_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "MetaData v3 SSZ length {} != {METADATA_V3_SSZ_LEN} (must be four fields)",
                    bytes.len()
                ),
            ));
        }
        Ok(Self {
            seq_number: u64::from_le_bytes(bytes[0..8].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "seq_number")
            })?),
            attnets: u64::from_le_bytes(bytes[8..16].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "attnets")
            })?),
            syncnets: bytes[16] & 0x0f,
            custody_group_count: u64::from_le_bytes(bytes[17..25].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "custody_group_count")
            })?),
        })
    }
}

impl Default for MetaDataV3 {
    fn default() -> Self {
        Self {
            seq_number: 0,
            attnets: 0,
            syncnets: 0,
            custody_group_count: CUSTODY_REQUIREMENT,
        }
    }
}

/// Authoritative local MetaData with atomic seq bumps on mutation.
///
/// Subnet managers (CC-2C/2D) and the custody hook (CC-21d) call the setters;
/// Ping/Status handlers only **read**.
#[derive(Debug)]
pub struct LocalMetaData {
    inner: Mutex<MetaDataV3>,
    /// Mirror of `seq_number` for lock-free Ping construction.
    seq: AtomicU64,
}

impl Default for LocalMetaData {
    fn default() -> Self {
        Self::new(CUSTODY_REQUIREMENT)
    }
}

impl LocalMetaData {
    /// Construct with empty bitvectors and the given custody count (`seq = 0`).
    #[must_use]
    pub fn new(custody_group_count: u64) -> Self {
        let md = MetaDataV3 {
            seq_number: 0,
            attnets: 0,
            syncnets: 0,
            custody_group_count,
        };
        Self {
            inner: Mutex::new(md),
            seq: AtomicU64::new(0),
        }
    }

    /// Snapshot current MetaData.
    #[must_use]
    pub fn load(&self) -> MetaDataV3 {
        *self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Current sequence number (lock-free).
    #[must_use]
    pub fn seq_number(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Replace attnets; bumps `seq_number` when the value changes.
    pub fn set_attnets(&self, attnets: u64) -> u64 {
        self.mutate(|md| {
            if md.attnets != attnets {
                md.attnets = attnets;
                true
            } else {
                false
            }
        })
    }

    /// Replace syncnets (low 4 bits); bumps on change.
    pub fn set_syncnets(&self, syncnets: u8) -> u64 {
        let syncnets = syncnets & 0x0f;
        self.mutate(|md| {
            if md.syncnets != syncnets {
                md.syncnets = syncnets;
                true
            } else {
                false
            }
        })
    }

    /// Replace custody group count; bumps on change.
    pub fn set_custody_group_count(&self, cgc: u64) -> u64 {
        self.mutate(|md| {
            if md.custody_group_count != cgc {
                md.custody_group_count = cgc;
                true
            } else {
                false
            }
        })
    }

    fn mutate(&self, f: impl FnOnce(&mut MetaDataV3) -> bool) -> u64 {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if f(&mut guard) {
            guard.seq_number = guard.seq_number.saturating_add(1);
            self.seq.store(guard.seq_number, Ordering::Release);
        }
        guard.seq_number
    }
}

/// Config knob: reject peers advertising `cgc < CUSTODY_REQUIREMENT`.
///
/// Default **false** (accept) — rejecting shrinks an already-thin
/// custody-compatible peer set; clause 1 needs ≥ 8 of them (CC-23/2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CgcPolicy {
    /// When true, peers with `cgc < CUSTODY_REQUIREMENT` are Goodbye'd.
    pub reject_low_cgc_peers: bool,
}

impl CgcPolicy {
    /// Default accept policy.
    #[must_use]
    pub const fn accept_low_cgc() -> Self {
        Self {
            reject_low_cgc_peers: false,
        }
    }

    /// Strict reject policy.
    #[must_use]
    pub const fn reject_low_cgc() -> Self {
        Self {
            reject_low_cgc_peers: true,
        }
    }
}

/// Outcome of applying the low-cgc policy to a peer's advertised count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgcEval {
    /// Keep the peer.
    Accept,
    /// Disconnect with Goodbye.
    Reject {
        /// Wire reason (fault / error — peer failed a local policy check).
        reason: GoodbyeReason,
    },
}

/// Evaluate peer `custody_group_count` under [`CgcPolicy`].
#[must_use]
pub fn evaluate_peer_cgc(policy: CgcPolicy, peer_cgc: u64) -> CgcEval {
    if policy.reject_low_cgc_peers && peer_cgc < CUSTODY_REQUIREMENT {
        CgcEval::Reject {
            reason: GoodbyeReason::FaultOrError,
        }
    } else {
        CgcEval::Accept
    }
}

/// Encode MetaData as a framed success response (empty request side).
pub fn encode_metadata_response(md: &MetaDataV3) -> io::Result<Vec<u8>> {
    let chunk = ResponseChunk::success(md.to_ssz_bytes().to_vec());
    SszSnappyFraming::encode_response_chunk(&chunk, Protocol::MetaDataV3)
}

/// Encode empty MetaData request.
pub fn encode_metadata_request() -> io::Result<Vec<u8>> {
    SszSnappyFraming::encode_request(&[], Protocol::MetaDataV3)
}

/// Decode MetaData SSZ body.
pub fn decode_metadata_ssz(ssz: &[u8]) -> Result<MetaDataV3, io::Error> {
    MetaDataV3::from_ssz_bytes(ssz)
}

/// Decode framed MetaData response.
pub fn decode_metadata_response_framed(framed: &[u8]) -> Result<MetaDataV3, io::Error> {
    let chunks = SszSnappyFraming::decode_response(framed, Protocol::MetaDataV3)?;
    let Some(ResponseChunk::Success { ssz, .. }) = chunks.into_iter().next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata response missing success chunk",
        ));
    };
    MetaDataV3::from_ssz_bytes(&ssz)
}

/// Compile-time-ish documentation of bitfield widths (ENR / MetaData agree).
pub const fn attnets_bit_len() -> usize {
    ATTNETS_BIT_LEN
}

/// Syncnets bit length (must be 4).
pub const fn syncnets_bit_len() -> usize {
    SYNCNETS_BIT_LEN
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn field_count_is_four() {
        assert_eq!(MetaDataV3::field_count(), 4);
        assert_eq!(METADATA_V3_SSZ_LEN, 25);
        assert_eq!(attnets_bit_len(), 64);
        assert_eq!(syncnets_bit_len(), 4);
    }

    #[test]
    fn ssz_roundtrip_four_fields() {
        let md = MetaDataV3 {
            seq_number: 9,
            attnets: 0x8000_0000_0000_0001,
            syncnets: 0b0101,
            custody_group_count: 8,
        };
        let bytes = md.to_ssz_bytes();
        assert_eq!(bytes.len(), 25);
        assert_eq!(MetaDataV3::from_ssz_bytes(&bytes).unwrap(), md);
    }

    #[test]
    fn three_field_metadata_fails_decode() {
        // 17 bytes = seq + attnets + syncnets without cgc.
        let three = vec![0u8; 17];
        let err = MetaDataV3::from_ssz_bytes(&three).unwrap_err();
        assert!(
            err.to_string().contains("four fields") || err.to_string().contains("25"),
            "{err}"
        );
    }

    #[test]
    fn framing_roundtrip() {
        let md = MetaDataV3::default();
        let framed = encode_metadata_response(&md).unwrap();
        assert_eq!(decode_metadata_response_framed(&framed).unwrap(), md);
        let req = encode_metadata_request().unwrap();
        assert!(req.is_empty());
    }

    #[test]
    fn seq_bumps_on_each_of_three_fields() {
        let local = LocalMetaData::new(CUSTODY_REQUIREMENT);
        assert_eq!(local.seq_number(), 0);

        let s1 = local.set_attnets(1);
        assert_eq!(s1, 1);
        assert_eq!(local.load().attnets, 1);

        let s2 = local.set_syncnets(0b0011);
        assert_eq!(s2, 2);
        assert_eq!(local.load().syncnets, 0b0011);

        let s3 = local.set_custody_group_count(8);
        assert_eq!(s3, 3);
        assert_eq!(local.load().custody_group_count, 8);

        // No-op mutations do not bump.
        assert_eq!(local.set_attnets(1), 3);
        assert_eq!(local.set_syncnets(0b0011), 3);
        assert_eq!(local.set_custody_group_count(8), 3);
    }

    #[test]
    fn cgc_policy_both_ways() {
        // Default: accept cgc = 1.
        assert_eq!(
            evaluate_peer_cgc(CgcPolicy::default(), 1),
            CgcEval::Accept
        );
        assert_eq!(
            evaluate_peer_cgc(CgcPolicy::accept_low_cgc(), 1),
            CgcEval::Accept
        );
        // Strict: reject cgc = 1 with Goodbye.
        match evaluate_peer_cgc(CgcPolicy::reject_low_cgc(), 1) {
            CgcEval::Reject { reason } => {
                assert_eq!(reason, GoodbyeReason::FaultOrError);
            }
            CgcEval::Accept => panic!("must reject low cgc when policy true"),
        }
        // cgc >= CUSTODY_REQUIREMENT always accepted.
        assert_eq!(
            evaluate_peer_cgc(CgcPolicy::reject_low_cgc(), CUSTODY_REQUIREMENT),
            CgcEval::Accept
        );
    }
}
