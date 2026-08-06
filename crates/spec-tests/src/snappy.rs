//! Snappy decompression for `.ssz_snappy` vector payloads.
//!
//! Consensus-spec **test vectors** use snappy **block** (raw) compression — see
//! `tests/formats/README.md` in ethereum/consensus-specs ("Snappy block
//! compression"). That is distinct from the snappy **framing** stream format
//! used on the p2p wire (`snap::read::FrameDecoder`).
//!
//! This module therefore decompresses with [`snap::raw::Decoder`]. Using the
//! frame decoder on vector files fails with a corrupt-input error; a unit test
//! locks that distinction in so it is not rediscovered as an SSZ bug.
//!
//! **Size cap:** claimed uncompressed length is checked via
//! [`snap::raw::decompress_len`] **before** allocation. Claims above
//! [`MAX_DECOMPRESSED_BYTES`] are rejected to prevent snappy-bomb DoS from a
//! poisoned cache tree (marker digests alone do not re-hash case files).

use std::path::Path;

use crate::error::Error;

#[cfg(test)]
use std::io::Read;

/// Hard maximum uncompressed size for a single vector payload (64 MiB).
///
/// Larger than any consensus-spec case on the current pin; small enough to
/// fail closed on a malicious snappy length header without multi-GiB OOM.
pub const MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// Decompress snappy **block**-format bytes (vector `.ssz_snappy` payloads).
///
/// Rejects claimed lengths above [`MAX_DECOMPRESSED_BYTES`] **before**
/// allocating the output buffer.
pub fn decompress_block(compressed: &[u8], path_for_err: &Path) -> Result<Vec<u8>, Error> {
    let claimed = snap::raw::decompress_len(compressed).map_err(|e| Error::Snappy {
        path: path_for_err.to_path_buf(),
        detail: e.to_string(),
    })?;
    if claimed > MAX_DECOMPRESSED_BYTES {
        return Err(Error::Snappy {
            path: path_for_err.to_path_buf(),
            detail: format!(
                "claimed uncompressed length {claimed} exceeds hard max {MAX_DECOMPRESSED_BYTES}"
            ),
        });
    }
    // Allocate only after the cap check (Decoder::decompress_vec would otherwise
    // honour the full claimed length up to ~4 GiB).
    let mut out = vec![0u8; claimed];
    let n = snap::raw::Decoder::new()
        .decompress(compressed, &mut out)
        .map_err(|e| Error::Snappy {
            path: path_for_err.to_path_buf(),
            detail: e.to_string(),
        })?;
    out.truncate(n);
    Ok(out)
}

/// Attempt snappy **frame** decode (test / format-proof helper only).
///
/// Production vector loading does **not** use this path. Output is capped at
/// [`MAX_DECOMPRESSED_BYTES`] via [`Read::take`].
#[cfg(test)]
pub(crate) fn try_decompress_frame(compressed: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = snap::read::FrameDecoder::new(compressed)
        .take(MAX_DECOMPRESSED_BYTES as u64 + 1);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| e.to_string())?;
    if out.len() > MAX_DECOMPRESSED_BYTES {
        return Err(format!(
            "frame stream exceeded hard max {MAX_DECOMPRESSED_BYTES}"
        ));
    }
    Ok(out)
}

/// Encode a snappy-block length prefix (varint) for tests.
#[cfg(test)]
fn encode_snappy_len_varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::io::Write;

    #[test]
    fn frame_decoder_rejects_block_format_without_panic() {
        let block = snap::raw::Encoder::new()
            .compress_vec(b"hello vectors")
            .expect("compress");
        let err = try_decompress_frame(&block).expect_err("frame must reject block");
        assert!(
            !err.is_empty(),
            "decode error should be non-empty, got {err:?}"
        );
    }

    #[test]
    fn frame_decoder_accepts_frame_roundtrip() {
        let payload = b"frame payload for unit test";
        let mut framed = Vec::new();
        {
            let mut enc = snap::write::FrameEncoder::new(&mut framed);
            enc.write_all(payload).expect("write");
            enc.flush().expect("flush");
        }
        let out = try_decompress_frame(&framed).expect("frame decode");
        assert_eq!(out, payload);
    }

    #[test]
    fn block_decompress_roundtrip() {
        let payload = b"block payload";
        let compressed = snap::raw::Encoder::new()
            .compress_vec(payload)
            .expect("compress");
        let out = decompress_block(&compressed, Path::new("mem")).expect("decompress");
        assert_eq!(out, payload);
    }

    #[test]
    fn malicious_claimed_length_rejected_without_huge_alloc() {
        // Claim ~1 GiB uncompressed (well under snap's ~4 GiB max, over our 64 MiB cap).
        let claimed: u64 = 1024 * 1024 * 1024;
        assert!(claimed as usize > MAX_DECOMPRESSED_BYTES);
        let mut bomb = encode_snappy_len_varint(claimed);
        // Trailing junk — we must fail on the length check before allocating.
        bomb.extend_from_slice(&[0u8; 16]);

        let err = decompress_block(&bomb, Path::new("bomb.ssz_snappy")).expect_err("cap");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds hard max") || msg.contains(&MAX_DECOMPRESSED_BYTES.to_string()),
            "expected size-cap error, got {msg}"
        );
        assert!(msg.contains(&claimed.to_string()) || msg.contains("hard max"), "{msg}");
    }

    #[test]
    fn claimed_length_at_max_is_allowed_to_attempt_decode() {
        // Exactly MAX is not rejected by the cap check (decode may still fail).
        let mut buf = encode_snappy_len_varint(MAX_DECOMPRESSED_BYTES as u64);
        buf.push(0);
        // Must not return the "exceeds hard max" error.
        match decompress_block(&buf, Path::new("edge")) {
            Ok(_) => {}
            Err(Error::Snappy { detail, .. }) => {
                assert!(
                    !detail.contains("exceeds hard max"),
                    "exact max should not trip cap: {detail}"
                );
            }
            Err(e) => panic!("unexpected {e}"),
        }
    }
}
