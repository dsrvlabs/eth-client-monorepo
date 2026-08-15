//! Live `request_response` SSZ+snappy codec (Architecture §7.2 / CC-23a).
//!
//! **Streaming, protocol-aware, timeout-bounded** — never unbounded `read_to_end`
//! of an untrusted stream (security H1/H3/M2):
//! 1. Varint length checked against protocol min/max **before** allocating the
//!    uncompressed buffer or constructing `FrameDecoder`.
//! 2. Compressed reads are `take`-bounded to `max_compress_len(claimed)`.
//! 3. Response streams are `take`-bounded to a protocol-aware absolute ceiling.
//! 4. TTFB 5 s to first response byte; RESP 10 s idle between subsequent reads.

use std::io::{self, Read as StdRead, Write as StdWrite};
use std::time::Duration;

use futures::AsyncRead as FuturesAsyncRead;
use futures::AsyncReadExt as _;
use futures::AsyncWrite as FuturesAsyncWrite;
use futures::AsyncWriteExt as _;
use libp2p::request_response::Codec;
use libp2p::swarm::StreamProtocol;

/// Spec `MAX_PAYLOAD_SIZE` (10 MiB) — global uncompressed chunk ceiling.
pub const REQRESP_MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// Time-to-first-byte for the first response byte (Architecture §7.2).
pub const TTFB_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle timeout between subsequent response reads (Architecture §7.2).
pub const RESP_TIMEOUT: Duration = Duration::from_secs(10);

/// Absolute ceiling on a multi-chunk response stream (DoS bound).
///
/// Covers worst-case control + multi-chunk body while remaining far below
/// unbounded `read_to_end`. Chunk-level SSZ still capped at
/// [`REQRESP_MAX_PAYLOAD_SIZE`] when handlers decode.
pub const MAX_RESPONSE_STREAM_BYTES: usize = 32 * 1024 * 1024;

/// Inbound / outbound request: negotiated protocol + uncompressed SSZ body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReqRespRequest {
    /// Negotiated protocol ID string.
    pub protocol: StreamProtocol,
    /// Uncompressed SSZ (empty for MetaData).
    pub ssz: Vec<u8>,
}

/// Response: pre-framed multi-chunk body (handlers / fail-closed stub build it).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReqRespResponse {
    /// Wire bytes: `repeat { result [context] varint snappy-frames }`.
    pub framed: Vec<u8>,
}

impl ReqRespResponse {
    /// Empty response (goodbye / no body).
    #[must_use]
    pub fn empty() -> Self {
        Self { framed: Vec::new() }
    }

    /// Wrap already-framed bytes.
    #[must_use]
    pub fn from_framed(framed: Vec<u8>) -> Self {
        Self { framed }
    }
}

/// SSZ+snappy framing codec for the multi-protocol `request_response` behaviour.
#[derive(Debug, Clone)]
pub struct SszSnappyCodec {
    /// Fallback uncompressed ceiling when protocol is unknown.
    pub max_payload: usize,
    /// TTFB for first response byte.
    pub ttfb_timeout: Duration,
    /// Idle timeout for subsequent response reads.
    pub resp_timeout: Duration,
    /// Absolute multi-chunk response stream ceiling.
    pub max_response_stream: usize,
}

impl Default for SszSnappyCodec {
    fn default() -> Self {
        Self {
            max_payload: REQRESP_MAX_PAYLOAD_SIZE,
            ttfb_timeout: TTFB_TIMEOUT,
            resp_timeout: RESP_TIMEOUT,
            max_response_stream: MAX_RESPONSE_STREAM_BYTES,
        }
    }
}

impl SszSnappyCodec {
    /// Build with an explicit uncompressed ceiling (tests may shrink it).
    #[must_use]
    pub const fn new(max_payload: usize) -> Self {
        Self {
            max_payload,
            ttfb_timeout: TTFB_TIMEOUT,
            resp_timeout: RESP_TIMEOUT,
            max_response_stream: MAX_RESPONSE_STREAM_BYTES,
        }
    }
}

impl Codec for SszSnappyCodec {
    type Protocol = StreamProtocol;
    type Request = ReqRespRequest;
    type Response = ReqRespResponse;

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: FuturesAsyncRead + Unpin + Send,
    {
        let proto = protocol.clone();
        let (min, max) = request_limits(protocol.as_ref(), self.max_payload);
        if max == 0 {
            // MetaData: empty body; peer half-closes. Bound any garbage to 1 byte.
            let mut take = io.take(1);
            let mut probe = [0u8; 1];
            let n = take.read(&mut probe).await?;
            if n == 0 {
                return Ok(ReqRespRequest {
                    protocol: proto,
                    ssz: Vec::new(),
                });
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metadata request must be empty",
            ));
        }
        let ssz = read_ssz_snappy_streaming(io, min, max).await?;
        Ok(ReqRespRequest {
            protocol: proto,
            ssz,
        })
    }

    async fn read_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: FuturesAsyncRead + Unpin + Send,
    {
        let cap = response_stream_cap(protocol.as_ref(), self.max_response_stream);
        let framed = read_response_stream(io, cap, self.ttfb_timeout, self.resp_timeout).await?;
        Ok(ReqRespResponse { framed })
    }

    async fn write_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: FuturesAsyncWrite + Unpin + Send,
    {
        let (min, max) = request_limits(protocol.as_ref(), self.max_payload);
        if max == 0 || req.ssz.is_empty() {
            io.close().await?;
            return Ok(());
        }
        let framed = encode_ssz_snappy_payload(&req.ssz, min, max)?;
        io.write_all(&framed).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: FuturesAsyncWrite + Unpin + Send,
    {
        if res.framed.len() > self.max_response_stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "response exceeds max_response_stream",
            ));
        }
        if !res.framed.is_empty() {
            io.write_all(&res.framed).await?;
        }
        io.close().await?;
        Ok(())
    }
}

// ── protocol-aware limits (mirror services/p2p::reqresp::Protocol; no cc-* deps) ─
// Keep this table equal to `Protocol::request_limits` and
// `bin/serve-probe` `Protocol::request_limits` (S0-B-06). Dedup is S3a-A-03.

/// Request `(min, max)` SSZ bounds for a negotiated protocol ID.
#[must_use]
pub fn request_limits(protocol: &str, global_max: usize) -> (usize, usize) {
    match protocol {
        "/eth2/beacon_chain/req/status/2/ssz_snappy" => (92, 92),
        "/eth2/beacon_chain/req/goodbye/1/ssz_snappy"
        | "/eth2/beacon_chain/req/ping/1/ssz_snappy" => (8, 8),
        "/eth2/beacon_chain/req/metadata/3/ssz_snappy" => (0, 0),
        // (start_slot, count, step) — three uint64s.
        "/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy" => (24, 24),
        // List[Root, 1024] of fixed-size elements: bare 32*n = 32768.
        "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy" => (0, 1024 * 32),
        "/eth2/beacon_chain/req/beacon_blocks_by_head/1/ssz_snappy" => (40, 40),
        // (start_slot, count, columns offset) + up to 128×u64 column indices.
        "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy" => (20, 20 + 128 * 8),
        // List[DataColumnsByRootIdentifier, 1024] framing max; semantic bound is 128.
        "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy" => {
            (0, REQRESP_MAX_PAYLOAD_SIZE)
        }
        _ => (0, global_max.min(REQRESP_MAX_PAYLOAD_SIZE)),
    }
}

/// Response stream absolute byte ceiling for a protocol.
#[must_use]
pub fn response_stream_cap(protocol: &str, absolute: usize) -> usize {
    // Single-chunk control protocols: tiny.
    let control = matches!(
        protocol,
        "/eth2/beacon_chain/req/status/2/ssz_snappy"
            | "/eth2/beacon_chain/req/ping/1/ssz_snappy"
            | "/eth2/beacon_chain/req/metadata/3/ssz_snappy"
            | "/eth2/beacon_chain/req/goodbye/1/ssz_snappy"
    );
    if control {
        // One chunk: result + varint + max_compress_len(256) + margin.
        return absolute.min(64 * 1024);
    }
    absolute.min(MAX_RESPONSE_STREAM_BYTES)
}

// ── streaming encode/decode ─────────────────────────────────────────────────

/// Encode `<varint len><snappy-framed SSZ>`.
pub fn encode_ssz_snappy_payload(ssz: &[u8], min: usize, max: usize) -> io::Result<Vec<u8>> {
    if ssz.len() < min || ssz.len() > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("ssz length {} out of bounds [{min}, {max}]", ssz.len()),
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

/// Buffer-based decode (tests / helpers). Bounds checked **before** decompress.
pub fn decode_ssz_snappy_payload(buf: &[u8], min: usize, max: usize) -> io::Result<Vec<u8>> {
    let (len_u64, varint_len) = decode_varint(buf)?;
    let len = usize::try_from(len_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "varint length does not fit usize",
        )
    })?;
    if len < min || len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("claimed ssz length {len} out of bounds [{min}, {max}] (no decompress)"),
        ));
    }
    let compressed = &buf[varint_len..];
    let max_compressed = snap::raw::max_compress_len(len);
    let limited_src =
        std::io::Cursor::new(compressed).take(u64::try_from(max_compressed).unwrap_or(u64::MAX));
    let mut decoder = snap::read::FrameDecoder::new(limited_src);
    let mut limited = (&mut decoder).take(u64::try_from(len).unwrap_or(u64::MAX));
    let mut ssz = vec![0u8; len];
    limited.read_exact(&mut ssz)?;
    Ok(ssz)
}

/// Streaming request body: varint → bound check → take-bound compressed → frame decode.
async fn read_ssz_snappy_streaming<T>(io: &mut T, min: usize, max: usize) -> io::Result<Vec<u8>>
where
    T: FuturesAsyncRead + Unpin + Send,
{
    let len_u64 = read_varint_async(io).await?;
    let len = usize::try_from(len_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "varint length does not fit usize",
        )
    })?;
    // Bound check BEFORE any proportional allocation / FrameDecoder.
    if len < min || len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("claimed ssz length {len} out of bounds [{min}, {max}] (no decompress)"),
        ));
    }
    let max_compressed = snap::raw::max_compress_len(len);
    // Compressed side is take-bounded — never unbounded read_to_end.
    let mut take = io.take(u64::try_from(max_compressed).unwrap_or(u64::MAX));
    let mut compressed = Vec::new();
    take.read_to_end(&mut compressed).await?;
    if compressed.len() > max_compressed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compressed payload exceeds max_compress_len",
        ));
    }
    let mut decoder = snap::read::FrameDecoder::new(compressed.as_slice());
    let mut limited = (&mut decoder).take(u64::try_from(len).unwrap_or(u64::MAX));
    let mut ssz = vec![0u8; len];
    limited.read_exact(&mut ssz)?;
    Ok(ssz)
}

/// Multi-chunk response stream with TTFB + per-read RESP + absolute take cap.
async fn read_response_stream<T>(
    io: &mut T,
    cap: usize,
    ttfb: Duration,
    resp: Duration,
) -> io::Result<Vec<u8>>
where
    T: FuturesAsyncRead + Unpin + Send,
{
    let cap_u64 = u64::try_from(cap).unwrap_or(u64::MAX);
    let mut limited = io.take(cap_u64);
    let mut buf = Vec::new();

    // TTFB: first byte.
    let mut first = [0u8; 1];
    match tokio::time::timeout(ttfb, limited.read(&mut first)).await {
        Ok(Ok(0)) => return Ok(Vec::new()),
        Ok(Ok(1)) => buf.push(first[0]),
        Ok(Ok(_)) => unreachable!("1-byte buffer"),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TTFB_TIMEOUT: no first response byte",
            ));
        }
    }

    // Subsequent reads: RESP idle timeout each; hard-capped by take(cap).
    let mut tmp = [0u8; 8 * 1024];
    loop {
        match tokio::time::timeout(resp, limited.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                if buf.len().saturating_add(n) > cap {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "response stream exceeds protocol/absolute ceiling",
                    ));
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "RESP_TIMEOUT: stalled response stream",
                ));
            }
        }
    }
    Ok(buf)
}

async fn read_varint_async<T>(io: &mut T) -> io::Result<u64>
where
    T: FuturesAsyncRead + Unpin + Send,
{
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        let mut byte = [0u8; 1];
        let n = io.read(&mut byte).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete varint",
            ));
        }
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

fn encode_varint(mut n: u64) -> Vec<u8> {
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

fn decode_varint(buf: &[u8]) -> io::Result<(u64, usize)> {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn status_request_limits_are_tight() {
        let (min, max) = request_limits("/eth2/beacon_chain/req/status/2/ssz_snappy", 10_000_000);
        assert_eq!((min, max), (92, 92));
    }

    #[test]
    fn ping_cannot_claim_10_mib() {
        let (min, max) = request_limits("/eth2/beacon_chain/req/ping/1/ssz_snappy", 10_000_000);
        assert_eq!((min, max), (8, 8));
    }

    #[test]
    fn by_root_max_is_list_bound_not_10_mib() {
        let (_, max) = request_limits(
            "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy",
            10_000_000,
        );
        assert_eq!(max, 1024 * 32);
        assert!(max < 100_000);
    }

    #[test]
    fn column_request_limits_match_protocol_table() {
        assert_eq!(
            request_limits(
                "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy",
                REQRESP_MAX_PAYLOAD_SIZE,
            ),
            (20, 20 + 128 * 8)
        );
        assert_eq!(
            request_limits(
                "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy",
                REQRESP_MAX_PAYLOAD_SIZE,
            ),
            (0, REQRESP_MAX_PAYLOAD_SIZE)
        );
    }

    #[test]
    fn claimed_2_gib_rejected_before_decompress() {
        let two_gib: u64 = 2 * 1024 * 1024 * 1024;
        let bomb = encode_varint(two_gib);
        let err = decode_ssz_snappy_payload(&bomb, 0, REQRESP_MAX_PAYLOAD_SIZE).unwrap_err();
        assert!(err.to_string().contains("no decompress"), "{err}");
    }

    #[test]
    fn frame_roundtrip() {
        let ssz = b"hello-ssz";
        let enc = encode_ssz_snappy_payload(ssz, 0, REQRESP_MAX_PAYLOAD_SIZE).unwrap();
        let dec = decode_ssz_snappy_payload(&enc, 0, REQRESP_MAX_PAYLOAD_SIZE).unwrap();
        assert_eq!(dec, ssz);
    }

    #[test]
    fn control_response_stream_cap_is_small() {
        let cap = response_stream_cap(
            "/eth2/beacon_chain/req/status/2/ssz_snappy",
            MAX_RESPONSE_STREAM_BYTES,
        );
        assert!(cap <= 64 * 1024);
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_request_respects_take_bound() {
        let ssz = vec![7u8; 92];
        let framed = encode_ssz_snappy_payload(&ssz, 92, 92).unwrap();
        let mut cursor = futures::io::Cursor::new(framed);
        let out = read_ssz_snappy_streaming(&mut cursor, 92, 92)
            .await
            .unwrap();
        assert_eq!(out, ssz);
    }

    #[tokio::test(start_paused = true)]
    async fn ttfb_timeout_on_silent_peer() {
        // Never-ready reader.
        struct Pending;
        impl FuturesAsyncRead for Pending {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &mut [u8],
            ) -> std::task::Poll<io::Result<usize>> {
                std::task::Poll::Pending
            }
        }
        let mut io = Pending;
        let err = read_response_stream(
            &mut io,
            1024,
            Duration::from_millis(50),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(err.to_string().contains("TTFB"), "{err}");
    }
}
