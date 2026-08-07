//! `Status v2` — Architecture §7.4 / CC-23b.
//!
//! Six fixed SSZ fields. Handlers **read** only:
//! - `fork_digest` ← [`crate::fork_digest::ForkContext`]
//! - `finalized_root` / `finalized_epoch` / `head_root` / `head_slot` ← [`ChainView`]
//! - `earliest_available_slot` ← CC-26a [`ServeWindow`] atomic (never recomputed here)

use std::io;

use cc_proto::p2p::ChainView;
use cc_types::primitives::{Epoch, Root, Slot};
use cc_types::ForkDigest;

use crate::channels::GoodbyeReason;
use crate::fork_digest::ForkContext;
use crate::reqresp::codec::{ResponseChunk, SszSnappyFraming};
use crate::reqresp::Protocol;

/// Fixed SSZ length of `Status v2` (4+32+8+32+8+8).
pub const STATUS_V2_SSZ_LEN: usize = 92;

/// Ethereum `Status v2` (Fulu) request **and** response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusV2 {
    /// Current fork digest.
    pub fork_digest: ForkDigest,
    /// Finalized checkpoint root.
    pub finalized_root: Root,
    /// Finalized checkpoint epoch.
    pub finalized_epoch: Epoch,
    /// Head block root.
    pub head_root: Root,
    /// Head slot.
    pub head_slot: Slot,
    /// Earliest slot this node can honestly serve (CC-26a atomic read).
    pub earliest_available_slot: Slot,
}

impl StatusV2 {
    /// SSZ-encode the six fields (fixed 92 bytes, little-endian integers).
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; STATUS_V2_SSZ_LEN] {
        let mut out = [0u8; STATUS_V2_SSZ_LEN];
        out[0..4].copy_from_slice(self.fork_digest.as_slice());
        out[4..36].copy_from_slice(self.finalized_root.as_slice());
        out[36..44].copy_from_slice(&self.finalized_epoch.as_u64().to_le_bytes());
        out[44..76].copy_from_slice(self.head_root.as_slice());
        out[76..84].copy_from_slice(&self.head_slot.as_u64().to_le_bytes());
        out[84..92].copy_from_slice(&self.earliest_available_slot.as_u64().to_le_bytes());
        out
    }

    /// SSZ-decode; rejects anything but exactly 92 bytes.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != STATUS_V2_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Status v2 SSZ length {} != {STATUS_V2_SSZ_LEN} (must be six fields)",
                    bytes.len()
                ),
            ));
        }
        let mut fd = [0u8; 4];
        fd.copy_from_slice(&bytes[0..4]);
        let mut fr = [0u8; 32];
        fr.copy_from_slice(&bytes[4..36]);
        let mut hr = [0u8; 32];
        hr.copy_from_slice(&bytes[44..76]);
        Ok(Self {
            fork_digest: ForkDigest::from_array(fd),
            finalized_root: Root::from_array(fr),
            finalized_epoch: Epoch::new(u64::from_le_bytes(bytes[36..44].try_into().map_err(
                |_| io::Error::new(io::ErrorKind::InvalidData, "finalized_epoch"),
            )?)),
            head_root: Root::from_array(hr),
            head_slot: Slot::new(u64::from_le_bytes(bytes[76..84].try_into().map_err(
                |_| io::Error::new(io::ErrorKind::InvalidData, "head_slot"),
            )?)),
            earliest_available_slot: Slot::new(u64::from_le_bytes(
                bytes[84..92].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "earliest_available_slot")
                })?,
            )),
        })
    }

    /// Field count for fixtures / acceptance (always six).
    #[must_use]
    pub const fn field_count() -> usize {
        6
    }
}

/// Build our local `Status v2` from the three sole sources (Architecture §7.4).
///
/// - digest from [`ForkContext`]
/// - chain fields from [`ChainView`] only (never a second head poller)
/// - window from the CC-26a atomic **value** already loaded by the caller
#[must_use]
pub fn build_local_status(
    fork_ctx: &ForkContext,
    view: &ChainView,
    earliest_available_slot: Slot,
) -> StatusV2 {
    StatusV2 {
        fork_digest: fork_ctx.current_digest(),
        finalized_root: root_from_bytes(&view.finalized_root),
        finalized_epoch: Epoch::new(view.finalized_epoch),
        head_root: root_from_bytes(&view.head_root),
        head_slot: Slot::new(view.head_slot),
        earliest_available_slot,
    }
}

fn root_from_bytes(b: &[u8]) -> Root {
    if b.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(b);
        Root::from_array(arr)
    } else {
        Root::ZERO
    }
}

/// Outcome of inspecting a peer's `Status v2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusEval {
    /// Digest matches; keep the peer and record status.
    Accept,
    /// Different network — emit Goodbye + disconnect.
    RejectIrrelevantNetwork {
        /// Wire reason code (spec `Irrelevant network`).
        reason: GoodbyeReason,
    },
}

/// Evaluate a peer Status against our local fork digest.
///
/// `nfd` is **not** an input — an `nfd` mismatch MUST NOT disconnect (CC-21/4).
#[must_use]
pub fn evaluate_peer_status(local_digest: ForkDigest, peer: &StatusV2) -> StatusEval {
    if peer.fork_digest == local_digest {
        StatusEval::Accept
    } else {
        StatusEval::RejectIrrelevantNetwork {
            // Spec code 2 = Irrelevant network. Architecture prose sometimes
            // writes "3"; the wire enum / eth2 p2p-interface is authoritative.
            reason: GoodbyeReason::IrrelevantNetwork,
        }
    }
}

/// Encode our Status as a framed success response (no context bytes).
pub fn encode_status_response(status: &StatusV2) -> io::Result<Vec<u8>> {
    let ssz = status.to_ssz_bytes();
    let chunk = ResponseChunk::success(ssz.to_vec());
    SszSnappyFraming::encode_response_chunk(&chunk, Protocol::StatusV2)
}

/// Encode Status as an outbound request body (snappy-framed SSZ).
pub fn encode_status_request(status: &StatusV2) -> io::Result<Vec<u8>> {
    let ssz = status.to_ssz_bytes();
    SszSnappyFraming::encode_request(&ssz, Protocol::StatusV2)
}

/// Decode an inbound Status request (uncompressed SSZ already extracted by codec).
pub fn decode_status_ssz(ssz: &[u8]) -> Result<StatusV2, io::Error> {
    StatusV2::from_ssz_bytes(ssz)
}

/// Decode a success response chunk's SSZ into [`StatusV2`].
pub fn decode_status_response_framed(framed: &[u8]) -> Result<StatusV2, io::Error> {
    let chunks = SszSnappyFraming::decode_response(framed, Protocol::StatusV2)?;
    let Some(ResponseChunk::Success { ssz, .. }) = chunks.into_iter().next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "status response missing success chunk",
        ));
    };
    StatusV2::from_ssz_bytes(&ssz)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_types::{ChainConfig, Epoch, Root};
    use crate::fork_digest::ForkContext;

    fn hoodi_fork_ctx() -> ForkContext {
        const YAML: &str =
            include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");
        let cfg = ChainConfig::from_yaml_str(YAML).expect("hoodi config");
        let gvr = Root::from_array([0xAB; 32]);
        ForkContext::new(cfg, gvr, Epoch::new(0))
    }

    fn sample_status() -> StatusV2 {
        StatusV2 {
            fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
            finalized_root: Root::from_array([0x11; 32]),
            finalized_epoch: Epoch::new(10),
            head_root: Root::from_array([0x22; 32]),
            head_slot: Slot::new(320),
            earliest_available_slot: Slot::new(300),
        }
    }

    #[test]
    fn field_count_is_six() {
        assert_eq!(StatusV2::field_count(), 6);
        assert_eq!(STATUS_V2_SSZ_LEN, 92);
    }

    #[test]
    fn ssz_roundtrip_six_fields() {
        let s = sample_status();
        let bytes = s.to_ssz_bytes();
        assert_eq!(bytes.len(), 92);
        let dec = StatusV2::from_ssz_bytes(&bytes).unwrap();
        assert_eq!(dec, s);
    }

    #[test]
    fn five_field_status_fails_decode() {
        // 84 bytes would be five fields without earliest_available_slot.
        let mut five = vec![0u8; 84];
        five[0..4].copy_from_slice(&[1, 2, 3, 4]);
        let err = StatusV2::from_ssz_bytes(&five).unwrap_err();
        assert!(
            err.to_string().contains("six fields") || err.to_string().contains("92"),
            "{err}"
        );
    }

    #[test]
    fn fixture_roundtrip_through_framing() {
        let s = sample_status();
        let req = encode_status_request(&s).unwrap();
        let ssz = SszSnappyFraming::decode_request(&req, Protocol::StatusV2).unwrap();
        assert_eq!(StatusV2::from_ssz_bytes(&ssz).unwrap(), s);

        let resp = encode_status_response(&s).unwrap();
        assert_eq!(decode_status_response_framed(&resp).unwrap(), s);
    }

    #[test]
    fn build_local_reads_chain_view_and_window() {
        let fork_ctx = hoodi_fork_ctx();
        let view = ChainView {
            head_slot: 99,
            head_root: vec![0xAA; 32],
            finalized_epoch: 3,
            finalized_root: vec![0xBB; 32],
            ..ChainView::default()
        };
        let window = Slot::new(50);
        let s = build_local_status(&fork_ctx, &view, window);
        assert_eq!(s.head_slot, Slot::new(99));
        assert_eq!(s.head_root, Root::from_array([0xAA; 32]));
        assert_eq!(s.finalized_epoch, Epoch::new(3));
        assert_eq!(s.finalized_root, Root::from_array([0xBB; 32]));
        assert_eq!(s.earliest_available_slot, window);
        assert_eq!(s.fork_digest, fork_ctx.current_digest());
    }

    #[test]
    fn fork_digest_mismatch_is_goodbye_disconnect() {
        let local = ForkDigest::from_array([9, 9, 9, 9]);
        let mut peer = sample_status();
        peer.fork_digest = ForkDigest::from_array([1, 1, 1, 1]);
        match evaluate_peer_status(local, &peer) {
            StatusEval::RejectIrrelevantNetwork { reason } => {
                assert_eq!(reason, GoodbyeReason::IrrelevantNetwork);
                assert_eq!(reason.as_u64(), 2);
            }
            StatusEval::Accept => panic!("must reject mismatched digest"),
        }
    }

    #[test]
    fn matching_digest_accepts() {
        let d = ForkDigest::from_array([1, 2, 3, 4]);
        let peer = sample_status();
        assert_eq!(evaluate_peer_status(d, &peer), StatusEval::Accept);
    }

    /// Acceptance: reqresp/ only **reads** earliest_available_slot (no recompute).
    #[test]
    fn earliest_available_slot_sites_are_reads_only() {
        let roots = [
            include_str!("status.rs"),
            include_str!("handshake.rs"),
            include_str!("ping.rs"),
            include_str!("metadata.rs"),
            include_str!("mod.rs"),
        ];
        for src in roots {
            for line in src.lines() {
                let t = line.trim_start();
                if t.starts_with("//") || t.starts_with("//!") || t.starts_with("///") {
                    continue;
                }
                if line.contains("earliest_available_slot") {
                    // Allowed: field name, load path, Slot::new assignment from load.
                    assert!(
                        !line.contains("compute_earliest")
                            && !line.contains("store_recomputed")
                            && !line.contains(".store("),
                        "reqresp must not compute/write earliest_available_slot: {line}"
                    );
                }
            }
        }
    }

    /// Grep-guard companion: `nfd` is not a parameter of status evaluation.
    #[test]
    fn nfd_mismatch_is_not_a_status_disconnect_input() {
        // Status evaluation only sees fork_digest. A peer ENR with a different
        // nfd still Accepts when Status.fork_digest matches (CC-21/4 re-run).
        let local = ForkDigest::from_array([1, 2, 3, 4]);
        let peer = sample_status();
        let _peer_nfd = ForkDigest::from_array([0xFF; 4]); // deliberately unused
        assert_eq!(evaluate_peer_status(local, &peer), StatusEval::Accept);
        // The evaluate function signature has no nfd parameter — compile-time
        // proof that nfd cannot disconnect through this path.
        let src = include_str!("status.rs");
        assert!(
            !src.contains("fn evaluate_peer_status") || {
                let f = src
                    .split("fn evaluate_peer_status")
                    .nth(1)
                    .unwrap_or("");
                let sig = f.split('{').next().unwrap_or("");
                !sig.contains("nfd")
            },
            "evaluate_peer_status must not take nfd"
        );
    }
}
