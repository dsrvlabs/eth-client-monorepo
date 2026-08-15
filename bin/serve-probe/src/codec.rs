//! Own SSZ+snappy framing for the serve probe (CC-4B / ADR P4-12).
//!
//! Intentionally independent of `services/p2p::reqresp::codec` so a shared bug
//! cannot make the probe pass against our own node.
//!
//! ```text
//! request  : <varint uncompressed_length> <snappy-framed SSZ>
//! response : repeat { <u8 result> <ForkDigest ctx, 4B, if result==0 && has_ctx>
//!                     <varint uncompressed_length> <snappy-framed payload> }
//! ```

use std::io::{self, Cursor, Read, Write};

/// Spec `MAX_PAYLOAD_SIZE` — global ceiling for one uncompressed chunk (10 MiB).
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// `ErrorMessage : List[byte, 256]` max SSZ length.
pub const MAX_ERROR_MESSAGE: usize = 256;

/// ForkDigest context width (Altair+).
pub const CONTEXT_BYTES_LEN: usize = 4;

/// Max success/error chunks decoded from one framed response (SEC-4B-1).
///
/// Probe requests use `count = 1` (and a small column set); a hostile peer must
/// not force hundreds of per-chunk allocations under the transport stream cap.
pub const MAX_RESPONSE_CHUNKS: usize = 16;

/// Cumulative uncompressed payload budget across all chunks in one response
/// (SEC-4B-1). Matches the transport multi-chunk framed ceiling order of
/// magnitude; fail closed before allocating beyond this.
pub const MAX_RESPONSE_UNCOMPRESSED: usize = 32 * 1024 * 1024;

/// Protocol-declared SSZ size bounds (checked before decompression).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SszLimits {
    /// Inclusive minimum uncompressed length.
    pub min: usize,
    /// Inclusive maximum uncompressed length.
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
    /// Parse a wire byte; unknown codes in `[4,127]` map to `None`.
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
        /// Uncompressed error message bytes.
        message: Vec<u8>,
    },
}

impl ResponseChunk {
    /// Whether this is a success chunk.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Whether this is result-byte `3: ResourceUnavailable`.
    #[must_use]
    pub fn is_resource_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Error {
                code: c,
                ..
            } if *c == ResponseCode::ResourceUnavailable.as_u8()
        )
    }
}

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
/// Returns `(value, bytes_consumed)`.
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
    for _ in 0..10 {
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
    let mut out = encode_varint(ssz.len() as u64);
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
/// Returns `(ssz, bytes_consumed)`. Bound check runs **before** decompress.
pub fn decode_payload(buf: &[u8], limits: SszLimits) -> io::Result<(Vec<u8>, usize)> {
    let mut cursor = Cursor::new(buf);
    let ssz = read_payload(&mut cursor, limits)?;
    let consumed = usize::try_from(cursor.position()).unwrap_or(0);
    Ok((ssz, consumed))
}

/// Streaming decode of one payload from a `Read`.
pub fn read_payload<R: Read>(r: &mut R, limits: SszLimits) -> io::Result<Vec<u8>> {
    let len_u64 = read_varint(r)?;
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
    let max_compressed = snap::raw::max_compress_len(len);
    let mut limited_src = r.take(u64::try_from(max_compressed).unwrap_or(u64::MAX));
    let mut decoder = snap::read::FrameDecoder::new(&mut limited_src);
    let mut limited = (&mut decoder).take(u64::try_from(len).unwrap_or(u64::MAX));
    let mut ssz = vec![0u8; len];
    limited.read_exact(&mut ssz)?;
    Ok(ssz)
}

/// Encode a full request body (no result byte).
pub fn encode_request(ssz: &[u8], limits: SszLimits) -> io::Result<Vec<u8>> {
    if ssz.is_empty() && limits.max == 0 {
        return Ok(Vec::new());
    }
    encode_payload(ssz, limits)
}

/// Encode one success response chunk.
pub fn encode_success_chunk(
    ssz: &[u8],
    has_context: bool,
    context: Option<[u8; CONTEXT_BYTES_LEN]>,
    response_limits: SszLimits,
) -> io::Result<Vec<u8>> {
    let mut out = vec![ResponseCode::Success.as_u8()];
    if has_context {
        let ctx = context.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "success chunk missing ForkDigest context for protocol that requires it",
            )
        })?;
        out.extend_from_slice(&ctx);
    }
    out.extend(encode_payload(ssz, response_limits)?);
    Ok(out)
}

/// Encode one error response chunk (no context bytes).
pub fn encode_error_chunk(code: u8, message: &[u8]) -> io::Result<Vec<u8>> {
    if code == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "error chunk must have non-zero result code",
        ));
    }
    let mut msg = message.to_vec();
    if msg.len() > MAX_ERROR_MESSAGE {
        msg.truncate(MAX_ERROR_MESSAGE);
    }
    let mut out = vec![code];
    let limits = SszLimits {
        min: 0,
        max: MAX_ERROR_MESSAGE,
    };
    out.extend(encode_payload(&msg, limits)?);
    Ok(out)
}

/// Decode one response chunk; returns `(chunk, bytes_consumed)`.
pub fn decode_response_chunk(
    buf: &[u8],
    has_context: bool,
    response_limits: SszLimits,
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
        let context = if has_context {
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
        let (ssz, consumed) = decode_payload(&buf[offset..], response_limits)?;
        offset += consumed;
        Ok((ResponseChunk::Success { context, ssz }, offset))
    } else {
        let limits = SszLimits {
            min: 0,
            max: MAX_ERROR_MESSAGE,
        };
        let (message, consumed) = decode_payload(&buf[offset..], limits)?;
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

/// Decode a multi-chunk response stream (until buffer exhausted).
///
/// An empty buffer is a valid empty success stream (zero chunks).
///
/// **SEC-4B-1:** fails closed if more than [`MAX_RESPONSE_CHUNKS`] chunks appear
/// or cumulative uncompressed SSZ/error bytes exceed
/// [`MAX_RESPONSE_UNCOMPRESSED`].
pub fn decode_response_chunks(
    buf: &[u8],
    has_context: bool,
    response_limits: SszLimits,
) -> io::Result<Vec<ResponseChunk>> {
    decode_response_chunks_bounded(
        buf,
        has_context,
        response_limits,
        MAX_RESPONSE_CHUNKS,
        MAX_RESPONSE_UNCOMPRESSED,
    )
}

/// Bounded multi-chunk decode (SEC-4B-1).
pub fn decode_response_chunks_bounded(
    buf: &[u8],
    has_context: bool,
    response_limits: SszLimits,
    max_chunks: usize,
    max_uncompressed: usize,
) -> io::Result<Vec<ResponseChunk>> {
    let mut offset = 0;
    let mut chunks = Vec::new();
    let mut total_uncompressed = 0usize;
    while offset < buf.len() {
        if chunks.len() >= max_chunks {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("response exceeds max chunk count {max_chunks} (fail closed, SEC-4B-1)"),
            ));
        }
        let (chunk, n) = decode_response_chunk(&buf[offset..], has_context, response_limits)?;
        offset += n;
        let piece_len = match &chunk {
            ResponseChunk::Success { ssz, .. } => ssz.len(),
            ResponseChunk::Error { message, .. } => message.len(),
        };
        total_uncompressed = total_uncompressed.saturating_add(piece_len);
        if total_uncompressed > max_uncompressed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "response exceeds max uncompressed budget {max_uncompressed} bytes (fail closed, SEC-4B-1)"
                ),
            ));
        }
        let is_error = matches!(chunk, ResponseChunk::Error { .. });
        chunks.push(chunk);
        if is_error {
            break;
        }
    }
    Ok(chunks)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for n in [0u64, 1, 127, 128, 255, 300, 16_384, u32::MAX as u64] {
            let enc = encode_varint(n);
            let (dec, consumed) = decode_varint(&enc).unwrap();
            assert_eq!(dec, n);
            assert_eq!(consumed, enc.len());
        }
    }

    #[test]
    fn payload_roundtrip() {
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let ssz = b"hello-ssz";
        let enc = encode_payload(ssz, limits).unwrap();
        let (dec, _) = decode_payload(&enc, limits).unwrap();
        assert_eq!(dec, ssz);
    }

    #[test]
    fn claimed_2_gib_rejected_before_decompress() {
        let two_gib: u64 = 2 * 1024 * 1024 * 1024;
        let bomb = encode_varint(two_gib);
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let err = decode_payload(&bomb, limits).unwrap_err();
        assert!(err.to_string().contains("no decompress"), "{err}");
    }

    #[test]
    fn success_with_context_roundtrip() {
        let ctx = [0x11, 0x22, 0x33, 0x44];
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let enc = encode_success_chunk(b"block", true, Some(ctx), limits).unwrap();
        let (chunk, n) = decode_response_chunk(&enc, true, limits).unwrap();
        assert_eq!(n, enc.len());
        match chunk {
            ResponseChunk::Success { context, ssz } => {
                assert_eq!(context, Some(ctx));
                assert_eq!(ssz, b"block");
            }
            ResponseChunk::Error { .. } => panic!("expected success"),
        }
    }

    #[test]
    fn resource_unavailable_chunk() {
        let enc = encode_error_chunk(3, b"out of window").unwrap();
        assert_eq!(enc[0], 3);
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let (chunk, _) = decode_response_chunk(&enc, true, limits).unwrap();
        assert!(chunk.is_resource_unavailable());
    }

    #[test]
    fn empty_buffer_is_zero_chunks() {
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let chunks = decode_response_chunks(&[], true, limits).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn multi_chunk_count_cap_fails_closed() {
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        // Three tiny success chunks; cap at 2 → fail closed.
        let mut framed = Vec::new();
        for payload in [b"a".as_slice(), b"b", b"c"] {
            framed.extend(encode_success_chunk(payload, false, None, limits).unwrap());
        }
        let err =
            decode_response_chunks_bounded(&framed, false, limits, 2, MAX_RESPONSE_UNCOMPRESSED)
                .unwrap_err();
        assert!(err.to_string().contains("max chunk count"), "{err}");
    }

    #[test]
    fn multi_chunk_uncompressed_budget_fails_closed() {
        let limits = SszLimits {
            min: 0,
            max: MAX_PAYLOAD_SIZE,
        };
        let a = encode_success_chunk(b"hello", false, None, limits).unwrap();
        let b = encode_success_chunk(b"world!!", false, None, limits).unwrap();
        let mut framed = a;
        framed.extend(b);
        // Budget smaller than combined payloads (5+7).
        let err = decode_response_chunks_bounded(&framed, false, limits, 16, 10).unwrap_err();
        assert!(err.to_string().contains("uncompressed budget"), "{err}");
    }
}
