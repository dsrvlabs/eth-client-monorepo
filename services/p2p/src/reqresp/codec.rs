//! SSZ+snappy framing for Ethereum req/resp (Architecture §7.2 / CC-23a).
//!
//! ```text
//! request  : <varint uncompressed_length> <snappy-framed SSZ>
//! response : repeat { <u8 result> <ForkDigest ctx, 4B, if result==0 && has_ctx>
//!                     <varint uncompressed_length> <snappy-framed payload> }
//! error    : <u8 result != 0> <varint len> <snappy-framed ErrorMessage>
//! ```
//!
//! Four rules before any proportional allocation:
//! 1. Varint length checked against protocol min/max **before** `FrameDecoder`
//! 2. Snappy **frame** format (`snap::read::FrameDecoder`)
//! 3. Per-chunk `ForkDigest` from that chunk's own slot ([`context_for_slot`])
//! 4. Every decode is fallible in both directions

use std::io::{self, Cursor, Read, Write};
use std::time::Duration;

use cc_types::{Epoch, ForkDigest, Slot};

use crate::fork_digest::ForkContext;
use crate::reqresp::Protocol;

/// Spec `MAX_PAYLOAD_SIZE` — global ceiling for one uncompressed req/resp chunk (10 MiB).
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// `ErrorMessage : List[byte, 256]` max SSZ length.
pub const MAX_ERROR_MESSAGE: usize = 256;

/// ForkDigest context width (Altair+).
pub const CONTEXT_BYTES_LEN: usize = 4;

/// Time-to-first-byte timeout for the first response chunk (CC-23/6).
pub const TTFB_TIMEOUT: Duration = Duration::from_secs(5);

/// Per subsequent response-chunk timeout (CC-23/6).
pub const RESP_TIMEOUT: Duration = Duration::from_secs(10);

/// Protocol-declared SSZ size bounds (checked before decompression).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SszLimits {
    /// Inclusive minimum uncompressed length.
    pub min: usize,
    /// Inclusive maximum uncompressed length (also capped by [`MAX_PAYLOAD_SIZE`]).
    pub max: usize,
}

impl SszLimits {
    /// Effective max after the global payload ceiling.
    #[must_use]
    pub const fn effective_max(self) -> usize {
        if self.max < MAX_PAYLOAD_SIZE {
            self.max
        } else {
            MAX_PAYLOAD_SIZE
        }
    }

    /// Whether `len` is outside `[min, effective_max]`.
    #[must_use]
    pub const fn is_out_of_bounds(self, len: usize) -> bool {
        len < self.min || len > self.effective_max()
    }
}

/// Single-byte response code (spec phase0/altair).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ResponseCode {
    /// Successful chunk; payload matches the protocol schema.
    Success = 0,
    /// Semantically invalid / malformed request.
    InvalidRequest = 1,
    /// Responder-side error (includes rate-limit).
    ServerError = 2,
    /// Requested resource not available.
    ResourceUnavailable = 3,
}

impl ResponseCode {
    /// Parse a wire byte; unknown codes in `[4,127]` are treated as errors.
    #[must_use]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Success),
            1 => Some(Self::InvalidRequest),
            2 => Some(Self::ServerError),
            3 => Some(Self::ResourceUnavailable),
            _ => None,
        }
    }

    /// Wire byte.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Whether this is a success code.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }
}

/// One decoded response chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseChunk {
    /// `result == 0` with optional Altair+ context bytes and SSZ payload.
    Success {
        /// `ForkDigest` when the protocol carries context bytes.
        context: Option<[u8; CONTEXT_BYTES_LEN]>,
        /// Uncompressed SSZ payload.
        ssz: Vec<u8>,
    },
    /// `result != 0` with error message (≤ 256 bytes).
    Error {
        /// Wire result code.
        code: u8,
        /// Uncompressed error message bytes (UTF-8 optional).
        message: Vec<u8>,
    },
}

impl ResponseChunk {
    /// Build a success chunk with an explicit fork-digest context.
    #[must_use]
    pub fn success_with_context(context: [u8; CONTEXT_BYTES_LEN], ssz: Vec<u8>) -> Self {
        Self::Success {
            context: Some(context),
            ssz,
        }
    }

    /// Build a success chunk without context bytes (status/ping/metadata).
    #[must_use]
    pub fn success(ssz: Vec<u8>) -> Self {
        Self::Success {
            context: None,
            ssz,
        }
    }

    /// Build a rate-limit / server-error chunk (code 2).
    #[must_use]
    pub fn server_error(message: impl Into<Vec<u8>>) -> Self {
        let mut message = message.into();
        if message.len() > MAX_ERROR_MESSAGE {
            message.truncate(MAX_ERROR_MESSAGE);
        }
        Self::Error {
            code: ResponseCode::ServerError.as_u8(),
            message,
        }
    }
}

/// Framing helpers: varint + snappy-frame encode/decode with pre-allocation bounds.
#[derive(Debug, Clone, Copy, Default)]
pub struct SszSnappyFraming;

impl SszSnappyFraming {
    /// Encode a protobuf-style unsigned varint.
    #[must_use]
    pub fn encode_varint(mut n: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            out.push(b);
            if n == 0 {
                break;
            }
        }
        out
    }

    /// Decode a protobuf-style unsigned varint from the front of `buf`.
    ///
    /// Returns `(value, bytes_consumed)`. Rejects overlong encodings (>10 bytes)
    /// and values that do not fit `u64`.
    pub fn decode_varint(buf: &[u8]) -> io::Result<(u64, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        for (i, &b) in buf.iter().enumerate() {
            if i >= 10 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint longer than 10 bytes",
                ));
            }
            let body = u64::from(b & 0x7f);
            if shift >= 64 || (body << shift) >> shift != body {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint overflow",
                ));
            }
            result |= body << shift;
            if b & 0x80 == 0 {
                return Ok((result, i + 1));
            }
            shift += 7;
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete varint",
        ))
    }

    /// Read a varint from a `Read` source (byte-at-a-time).
    pub fn read_varint<R: Read>(r: &mut R) -> io::Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        for i in 0..10 {
            let mut byte = [0u8; 1];
            r.read_exact(&mut byte)?;
            let b = byte[0];
            let body = u64::from(b & 0x7f);
            if shift >= 64 || (body << shift) >> shift != body {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint overflow",
                ));
            }
            result |= body << shift;
            if b & 0x80 == 0 {
                let _ = i;
                return Ok(result);
            }
            shift += 7;
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "varint longer than 10 bytes",
        ))
    }

    /// Snappy-frame compress `ssz` and prepend the uncompressed-length varint.
    ///
    /// Returns the full framing payload: `<varint len> <snappy frames>`.
    pub fn encode_payload(ssz: &[u8], limits: SszLimits) -> io::Result<Vec<u8>> {
        if limits.is_out_of_bounds(ssz.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "ssz length {} out of bounds [{}, {}]",
                    ssz.len(),
                    limits.min,
                    limits.effective_max()
                ),
            ));
        }
        let mut out = Self::encode_varint(ssz.len() as u64);
        {
            let mut encoder = snap::write::FrameEncoder::new(&mut out);
            encoder.write_all(ssz)?;
            encoder.flush()?;
            encoder
                .into_inner()
                .map_err(|e| io::Error::other(format!("snappy frame finish: {e}")))?;
        }
        Ok(out)
    }

    /// Decode `<varint len> <snappy frames>` into uncompressed SSZ.
    ///
    /// **Rule 1:** the varint is checked against `limits` **before** the
    /// decompressor is constructed. A 2 GiB claim costs one varint read.
    ///
    /// **Rule 2:** snappy **frame** format only.
    ///
    /// Returns `(ssz, bytes_consumed)` where `bytes_consumed` is the exact
    /// number of input bytes read (varint + snappy frames).
    pub fn decode_payload(buf: &[u8], limits: SszLimits) -> io::Result<(Vec<u8>, usize)> {
        let mut cursor = Cursor::new(buf);
        let ssz = Self::read_payload(&mut cursor, limits)?;
        let consumed = usize::try_from(cursor.position()).unwrap_or(0);
        Ok((ssz, consumed))
    }

    /// Streaming decode of one payload from a `Read`.
    ///
    /// Checks `limits` before constructing `FrameDecoder`, then `take`-bounds
    /// the reader to the claimed length. Advances `r` only by the snappy
    /// frames actually consumed (via a counting reader under the decoder).
    pub fn read_payload<R: Read>(r: &mut R, limits: SszLimits) -> io::Result<Vec<u8>> {
        let len_u64 = Self::read_varint(r)?;
        let len = usize::try_from(len_u64).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "varint length does not fit usize",
            )
        })?;
        if limits.is_out_of_bounds(len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "claimed ssz length {len} out of bounds [{}, {}] (no decompress)",
                    limits.min,
                    limits.effective_max()
                ),
            ));
        }
        // Bound compressed reads to the snappy worst-case for `len` so a
        // hostile stream cannot force unbounded work.
        let max_compressed = snap::raw::max_compress_len(len);
        let mut limited_src = r.take(u64::try_from(max_compressed).unwrap_or(u64::MAX));
        // Construct decompressor only after the bound check.
        let mut decoder = snap::read::FrameDecoder::new(&mut limited_src);
        let mut limited = (&mut decoder).take(u64::try_from(len).unwrap_or(u64::MAX));
        let mut ssz = vec![0u8; len];
        limited.read_exact(&mut ssz)?;
        // FrameDecoder reads frame-by-frame and stops after producing `len`
        // uncompressed bytes, leaving the underlying reader position right
        // after the last consumed frame (needed for multi-chunk streams).
        Ok(ssz)
    }

    /// Encode a full request body (no result byte).
    pub fn encode_request(ssz: &[u8], protocol: Protocol) -> io::Result<Vec<u8>> {
        // MetaData has an empty request — still valid.
        if ssz.is_empty() && protocol.request_limits().max == 0 {
            return Ok(Vec::new());
        }
        Self::encode_payload(ssz, protocol.request_limits())
    }

    /// Decode a full request body.
    pub fn decode_request(buf: &[u8], protocol: Protocol) -> io::Result<Vec<u8>> {
        let limits = protocol.request_limits();
        if buf.is_empty() && limits.max == 0 {
            return Ok(Vec::new());
        }
        let (ssz, _) = Self::decode_payload(buf, limits)?;
        Ok(ssz)
    }

    /// Encode one response chunk (result + optional context + payload).
    pub fn encode_response_chunk(
        chunk: &ResponseChunk,
        protocol: Protocol,
    ) -> io::Result<Vec<u8>> {
        match chunk {
            ResponseChunk::Success { context, ssz } => {
                let mut out = vec![ResponseCode::Success.as_u8()];
                if protocol.has_context_bytes() {
                    let ctx = context.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "success chunk missing ForkDigest context for protocol that requires it",
                        )
                    })?;
                    out.extend_from_slice(&ctx);
                }
                out.extend(Self::encode_payload(ssz, protocol.response_limits())?);
                Ok(out)
            }
            ResponseChunk::Error { code, message } => {
                if *code == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "error chunk must have non-zero result code",
                    ));
                }
                let mut out = vec![*code];
                // Errors never carry context bytes (Altair).
                let limits = SszLimits {
                    min: 0,
                    max: MAX_ERROR_MESSAGE,
                };
                out.extend(Self::encode_payload(message, limits)?);
                Ok(out)
            }
        }
    }

    /// Decode one response chunk from a buffer; returns `(chunk, bytes_consumed)`.
    pub fn decode_response_chunk(
        buf: &[u8],
        protocol: Protocol,
    ) -> io::Result<(ResponseChunk, usize)> {
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "empty response chunk",
            ));
        }
        let result = buf[0];
        let mut offset = 1usize;

        if result == ResponseCode::Success.as_u8() {
            let context = if protocol.has_context_bytes() {
                if buf.len() < offset + CONTEXT_BYTES_LEN {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "missing ForkDigest context bytes",
                    ));
                }
                let mut ctx = [0u8; CONTEXT_BYTES_LEN];
                ctx.copy_from_slice(&buf[offset..offset + CONTEXT_BYTES_LEN]);
                offset += CONTEXT_BYTES_LEN;
                Some(ctx)
            } else {
                None
            };
            let (ssz, consumed) = Self::decode_payload(&buf[offset..], protocol.response_limits())?;
            offset += consumed;
            Ok((ResponseChunk::Success { context, ssz }, offset))
        } else {
            let limits = SszLimits {
                min: 0,
                max: MAX_ERROR_MESSAGE,
            };
            let (message, consumed) = Self::decode_payload(&buf[offset..], limits)?;
            offset += consumed;
            Ok((
                ResponseChunk::Error {
                    code: result,
                    message,
                },
                offset,
            ))
        }
    }

    /// Encode zero or more response chunks into a single stream body.
    pub fn encode_response(
        chunks: &[ResponseChunk],
        protocol: Protocol,
    ) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        for c in chunks {
            out.extend(Self::encode_response_chunk(c, protocol)?);
        }
        Ok(out)
    }

    /// Decode a multi-chunk response stream (until buffer exhausted).
    pub fn decode_response(
        buf: &[u8],
        protocol: Protocol,
    ) -> io::Result<Vec<ResponseChunk>> {
        let mut offset = 0;
        let mut chunks = Vec::new();
        while offset < buf.len() {
            let (chunk, n) = Self::decode_response_chunk(&buf[offset..], protocol)?;
            offset += n;
            let is_error = matches!(chunk, ResponseChunk::Error { .. });
            chunks.push(chunk);
            if is_error {
                break;
            }
        }
        Ok(chunks)
    }
}

/// Compute the per-chunk `ForkDigest` context from a chunk's own slot (CC-23/7).
///
/// Uses [`ForkContext`]'s per-epoch cache so a range spanning a BPO boundary
/// carries the correct digest per chunk.
pub fn context_for_slot(
    fork_ctx: &mut ForkContext,
    slot: Slot,
    slots_per_epoch: u64,
) -> [u8; CONTEXT_BYTES_LEN] {
    let epoch: Epoch = slot.epoch(slots_per_epoch);
    let digest: ForkDigest = fork_ctx.digest_at(epoch);
    let mut out = [0u8; CONTEXT_BYTES_LEN];
    out.copy_from_slice(digest.as_slice());
    out
}

/// Build a success response chunk tagged with the digest for `slot`.
pub fn success_chunk_for_slot(
    fork_ctx: &mut ForkContext,
    slot: Slot,
    slots_per_epoch: u64,
    ssz: Vec<u8>,
) -> ResponseChunk {
    let context = context_for_slot(fork_ctx, slot, slots_per_epoch);
    ResponseChunk::success_with_context(context, ssz)
}

// ── libp2p request_response::Codec surface (used by dual-swarm / host) ───────
//
// The Behaviour-typed codec lives in `cc_libp2p::SszSnappyCodec` and uses the
// same framing rules. Helpers above are the single source of truth for tests
// and handler bodies (CC-23b+).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_types::{ChainConfig, Mainnet, Preset, Root};
    use std::io::Cursor;

    fn roundtrip_payload(ssz: &[u8], limits: SszLimits) {
        let encoded = SszSnappyFraming::encode_payload(ssz, limits).unwrap();
        let (decoded, _) = SszSnappyFraming::decode_payload(&encoded, limits).unwrap();
        assert_eq!(decoded, ssz);
    }

    #[test]
    fn varint_roundtrip() {
        for n in [0u64, 1, 127, 128, 255, 300, 16_384, u32::MAX as u64, u64::MAX] {
            let enc = SszSnappyFraming::encode_varint(n);
            let (dec, consumed) = SszSnappyFraming::decode_varint(&enc).unwrap();
            assert_eq!(dec, n);
            assert_eq!(consumed, enc.len());
        }
    }

    #[test]
    fn frame_format_roundtrips() {
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        roundtrip_payload(b"hello-ssz", limits);
        roundtrip_payload(&vec![0xABu8; 64 * 1024], limits);
        roundtrip_payload(&[], SszLimits { min: 0, max: 0 });
    }

    #[test]
    fn block_format_payload_fails_to_decode() {
        // Snappy *block* format is the Phase 1 trap — FrameDecoder must reject it.
        let ssz = b"not-a-frame";
        let block = snap::raw::Encoder::new()
            .compress_vec(ssz)
            .expect("block compress");
        let mut framed = SszSnappyFraming::encode_varint(ssz.len() as u64);
        framed.extend_from_slice(&block);
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        assert!(
            SszSnappyFraming::decode_payload(&framed, limits).is_err(),
            "block-format payload must not round-trip through FrameDecoder"
        );
        // Frame format of the same payload must succeed (trap closed both ways).
        roundtrip_payload(ssz, limits);
    }

    #[test]
    fn claimed_2_gib_rejected_before_decompressor() {
        // A response claiming 2 GiB costs one varint read and no proportional allocation.
        let two_gib: u64 = 2 * 1024 * 1024 * 1024;
        let bomb = SszSnappyFraming::encode_varint(two_gib);
        // No compressed payload follows — we must fail on the bound check alone.
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let err = SszSnappyFraming::decode_payload(&bomb, limits).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("no decompress"),
            "error must name the pre-decompress reject: {err}"
        );

        // Streaming path: same bound check before FrameDecoder.
        let mut cursor = Cursor::new(bomb);
        let err = SszSnappyFraming::read_payload(&mut cursor, limits).unwrap_err();
        assert!(err.to_string().contains("no decompress"), "{err}");
    }

    #[test]
    fn request_roundtrip_status() {
        let ssz = vec![0u8; 92];
        let enc = SszSnappyFraming::encode_request(&ssz, Protocol::StatusV2).unwrap();
        let dec = SszSnappyFraming::decode_request(&enc, Protocol::StatusV2).unwrap();
        assert_eq!(dec, ssz);
    }

    #[test]
    fn empty_metadata_request() {
        let enc = SszSnappyFraming::encode_request(&[], Protocol::MetaDataV3).unwrap();
        assert!(enc.is_empty());
        let dec = SszSnappyFraming::decode_request(&enc, Protocol::MetaDataV3).unwrap();
        assert!(dec.is_empty());
    }

    #[test]
    fn response_chunk_with_context_roundtrip() {
        let ctx = [0x11, 0x22, 0x33, 0x44];
        let chunk = ResponseChunk::success_with_context(ctx, b"block-ssz".to_vec());
        let enc =
            SszSnappyFraming::encode_response_chunk(&chunk, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        let (dec, n) =
            SszSnappyFraming::decode_response_chunk(&enc, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        assert_eq!(n, enc.len());
        match dec {
            ResponseChunk::Success { context, ssz } => {
                assert_eq!(context, Some(ctx));
                assert_eq!(ssz, b"block-ssz");
            }
            ResponseChunk::Error { .. } => panic!("expected success"),
        }
    }

    #[test]
    fn error_chunk_no_context() {
        let chunk = ResponseChunk::server_error(b"rate limited".as_slice());
        let enc =
            SszSnappyFraming::encode_response_chunk(&chunk, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        // result byte + varint + frame — no 4-byte context after result.
        assert_eq!(enc[0], ResponseCode::ServerError.as_u8());
        let (dec, _) =
            SszSnappyFraming::decode_response_chunk(&enc, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        match dec {
            ResponseChunk::Error { code, message } => {
                assert_eq!(code, 2);
                assert_eq!(message, b"rate limited");
            }
            ResponseChunk::Success { .. } => panic!("expected error"),
        }
    }

    #[test]
    fn multi_chunk_response_roundtrip() {
        let chunks = vec![
            ResponseChunk::success_with_context([1, 0, 0, 0], b"a".to_vec()),
            ResponseChunk::success_with_context([2, 0, 0, 0], b"b".to_vec()),
        ];
        let enc =
            SszSnappyFraming::encode_response(&chunks, Protocol::BeaconBlocksByRootV2).unwrap();
        let dec =
            SszSnappyFraming::decode_response(&enc, Protocol::BeaconBlocksByRootV2).unwrap();
        assert_eq!(dec.len(), 2);
        assert_eq!(dec, chunks);
    }

    #[test]
    fn fork_digest_differs_across_epochs() {
        // Hoodi config: Fulu + BPO boundaries yield distinct digests per epoch.
        const YAML: &str =
            include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");
        let cfg = ChainConfig::from_yaml_str(YAML).expect("hoodi config");
        let gvr = Root::from_array([0xAB; 32]);
        let mut fork_ctx = ForkContext::new(cfg, gvr, Epoch::new(0));
        let slots_per_epoch = Mainnet::SLOTS_PER_EPOCH;

        // Epoch 0 (pre-Fulu) vs epoch 54016 (BPO 2 on Hoodi) — digests differ.
        let slot_a = Slot::new(0);
        let slot_b = Slot::new(slots_per_epoch * 54_016);
        let ctx_a = context_for_slot(&mut fork_ctx, slot_a, slots_per_epoch);
        let ctx_b = context_for_slot(&mut fork_ctx, slot_b, slots_per_epoch);
        assert_ne!(
            ctx_a, ctx_b,
            "synthetic chunks across epochs must have different digests"
        );

        let c1 = success_chunk_for_slot(
            &mut fork_ctx,
            slot_a,
            slots_per_epoch,
            b"chunk-a".to_vec(),
        );
        let c2 = success_chunk_for_slot(
            &mut fork_ctx,
            slot_b,
            slots_per_epoch,
            b"chunk-b".to_vec(),
        );
        let encoded =
            SszSnappyFraming::encode_response(&[c1, c2], Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        let decoded =
            SszSnappyFraming::decode_response(&encoded, Protocol::BeaconBlocksByRangeV2).unwrap();
        assert_eq!(decoded.len(), 2);
        match (&decoded[0], &decoded[1]) {
            (
                ResponseChunk::Success {
                    context: Some(a), ..
                },
                ResponseChunk::Success {
                    context: Some(b), ..
                },
            ) => {
                assert_eq!(*a, ctx_a);
                assert_eq!(*b, ctx_b);
                assert_ne!(
                    a, b,
                    "response spanning two epochs must carry different ForkDigest per chunk"
                );
            }
            _ => panic!("expected two success chunks with context"),
        }
    }

    #[test]
    fn timeouts_constants() {
        assert_eq!(TTFB_TIMEOUT, Duration::from_secs(5));
        assert_eq!(RESP_TIMEOUT, Duration::from_secs(10));
    }
}
