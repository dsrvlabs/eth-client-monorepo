//! Ethereum `/eth2/beacon_chain/req/ping/1/` — Architecture §7.4 / CC-23b.
//!
//! Carries the local [`MetaData v3`](super::metadata::MetaDataV3) `seq_number`.
//! Distinct from libp2p's `ping` behaviour (liveness/RTT) — both stay
//! (`docs/p2p-dependencies.md` §Deviations).

use std::io;

use crate::reqresp::codec::{ResponseChunk, SszSnappyFraming};
use crate::reqresp::Protocol;

/// Fixed SSZ length of a `Ping` body (`uint64`).
pub const PING_SSZ_LEN: usize = 8;

/// Ethereum req/resp `Ping` (MetaData sequence exchange).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Ping {
    /// Peer's MetaData `seq_number` (or our reply of the same).
    pub seq_number: u64,
}

impl Ping {
    /// Wrap a sequence number.
    #[must_use]
    pub const fn new(seq_number: u64) -> Self {
        Self { seq_number }
    }

    /// SSZ-encode as little-endian `uint64`.
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; PING_SSZ_LEN] {
        self.seq_number.to_le_bytes()
    }

    /// SSZ-decode; rejects non-8-byte payloads.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != PING_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Ping SSZ length {} != {PING_SSZ_LEN}", bytes.len()),
            ));
        }
        let arr: [u8; 8] = bytes.try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "ping seq_number")
        })?;
        Ok(Self {
            seq_number: u64::from_le_bytes(arr),
        })
    }
}

/// Whether a peer's ping seq disagrees with the last MetaData we cached for them.
#[must_use]
pub fn seq_mismatch(cached_seq: Option<u64>, peer_seq: u64) -> bool {
    match cached_seq {
        None => true, // never fetched → treat as mismatch so we fetch once
        Some(s) => s != peer_seq,
    }
}

/// Encode Ping as a framed success response.
pub fn encode_ping_response(ping: Ping) -> io::Result<Vec<u8>> {
    let chunk = ResponseChunk::success(ping.to_ssz_bytes().to_vec());
    SszSnappyFraming::encode_response_chunk(&chunk, Protocol::PingV1)
}

/// Encode Ping as an outbound request body.
pub fn encode_ping_request(ping: Ping) -> io::Result<Vec<u8>> {
    SszSnappyFraming::encode_request(&ping.to_ssz_bytes(), Protocol::PingV1)
}

/// Decode uncompressed Ping SSZ.
pub fn decode_ping_ssz(ssz: &[u8]) -> Result<Ping, io::Error> {
    Ping::from_ssz_bytes(ssz)
}

/// Decode a framed Ping response.
pub fn decode_ping_response_framed(framed: &[u8]) -> Result<Ping, io::Error> {
    let chunks = SszSnappyFraming::decode_response(framed, Protocol::PingV1)?;
    let Some(ResponseChunk::Success { ssz, .. }) = chunks.into_iter().next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ping response missing success chunk",
        ));
    };
    Ping::from_ssz_bytes(&ssz)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn ssz_roundtrip() {
        let p = Ping::new(42);
        assert_eq!(Ping::from_ssz_bytes(&p.to_ssz_bytes()).unwrap(), p);
    }

    #[test]
    fn framing_roundtrip() {
        let p = Ping::new(7);
        let framed = encode_ping_response(p).unwrap();
        assert_eq!(decode_ping_response_framed(&framed).unwrap(), p);
        let req = encode_ping_request(p).unwrap();
        let ssz = SszSnappyFraming::decode_request(&req, Protocol::PingV1).unwrap();
        assert_eq!(Ping::from_ssz_bytes(&ssz).unwrap(), p);
    }

    #[test]
    fn seq_mismatch_triggers_when_unknown_or_different() {
        assert!(seq_mismatch(None, 0));
        assert!(seq_mismatch(Some(1), 2));
        assert!(!seq_mismatch(Some(3), 3));
    }
}
