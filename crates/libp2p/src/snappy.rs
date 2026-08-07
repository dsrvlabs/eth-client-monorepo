//! Snappy framing transform for gossipsub ([`DataTransform`]) — Architecture §5.2 / ADR P2-06.
//!
//! Decompression runs inside the swarm task, so it must be bounded:
//! `snap::read::FrameDecoder` wrapped in [`std::io::Read::take`] so an
//! 80 MB-expanding payload costs at most `max_uncompressed` work/output.

use std::io::{self, Read, Write};

use libp2p::gossipsub::{DataTransform, Message, RawMessage, TopicHash};

/// Spec `GOSSIP_MAX_SIZE`: global ceiling for compressed transmit and
/// uncompressed transform output (10 MiB). Per-container SSZ bounds land in CC-22b.
pub const GOSSIP_MAX_SIZE: usize = 10 * 1024 * 1024;

/// Gossipsub [`DataTransform`] that snappy-frame-compresses outbound payloads
/// and decompresses inbound ones under a hard uncompressed-size ceiling.
#[derive(Debug, Clone)]
pub struct SnappyTransform {
    max_uncompressed: usize,
}

impl SnappyTransform {
    /// Build with the given uncompressed ceiling (bytes). Production uses
    /// [`GOSSIP_MAX_SIZE`]; tests may use a smaller bound.
    pub const fn new(max_uncompressed: usize) -> Self {
        Self { max_uncompressed }
    }

    /// Uncompressed-size ceiling applied via [`Read::take`].
    pub const fn max_uncompressed(&self) -> usize {
        self.max_uncompressed
    }
}

impl Default for SnappyTransform {
    fn default() -> Self {
        Self::new(GOSSIP_MAX_SIZE)
    }
}

impl DataTransform for SnappyTransform {
    fn inbound_transform(&self, raw_message: RawMessage) -> Result<Message, io::Error> {
        let max = self.max_uncompressed;
        // take(max + 1) so we can detect overflow without a counting allocator.
        let mut decoder = snap::read::FrameDecoder::new(raw_message.data.as_slice());
        let mut limited =
            (&mut decoder).take(u64::try_from(max.saturating_add(1)).unwrap_or(u64::MAX));
        let mut data = Vec::new();
        limited.read_to_end(&mut data)?;
        if data.len() > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snappy decompressed size exceeds max_uncompressed",
            ));
        }
        Ok(Message {
            source: raw_message.source,
            data,
            sequence_number: raw_message.sequence_number,
            topic: raw_message.topic,
        })
    }

    fn outbound_transform(&self, _topic: &TopicHash, data: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        if data.len() > self.max_uncompressed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "outbound payload exceeds max_uncompressed before snappy",
            ));
        }
        let mut compressed = Vec::new();
        {
            let mut encoder = snap::write::FrameEncoder::new(&mut compressed);
            encoder.write_all(&data)?;
            encoder.flush()?;
            // Ensure the encoder finishes the frame before `compressed` is used.
            encoder
                .into_inner()
                .map_err(|e| io::Error::other(format!("snappy frame encoder finish: {e}")))?;
        }
        Ok(compressed)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use libp2p::gossipsub::TopicHash;

    fn raw(data: Vec<u8>) -> RawMessage {
        RawMessage {
            source: None,
            data,
            sequence_number: Some(1),
            topic: TopicHash::from_raw("test"),
            signature: None,
            key: None,
            validated: false,
        }
    }

    #[test]
    fn round_trip_1_mib() {
        let transform = SnappyTransform::new(GOSSIP_MAX_SIZE);
        let payload = vec![0xABu8; 1024 * 1024];
        let compressed = transform
            .outbound_transform(&TopicHash::from_raw("t"), payload.clone())
            .expect("compress");
        let msg = transform
            .inbound_transform(raw(compressed))
            .expect("decompress");
        assert_eq!(msg.data, payload);
    }

    #[test]
    fn expansion_past_max_is_rejected() {
        // Small max so the test stays light; the production bound is 10 MiB.
        let max = 64 * 1024;
        let transform = SnappyTransform::new(max);
        let big = vec![0u8; max + 1];
        let mut compressed = Vec::new();
        {
            let mut encoder = snap::write::FrameEncoder::new(&mut compressed);
            encoder.write_all(&big).expect("write");
            encoder.flush().expect("flush");
            let _ = encoder.into_inner().expect("finish");
        }
        let err = transform
            .inbound_transform(raw(compressed))
            .expect_err("must reject expansion past max");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // take(max+1) sentinel: overflow path materialises max+1 bytes then rejects
        // (counting-allocator-free bound assertion).
        assert!(
            err.to_string().contains("max_uncompressed"),
            "reject message should name the bound: {err}"
        );

        // take-bound unit assertion: a successful decode never exceeds max.
        let ok_payload = vec![1u8; max];
        let compressed_ok = transform
            .outbound_transform(&TopicHash::from_raw("t"), ok_payload.clone())
            .expect("compress ok");
        let msg = transform
            .inbound_transform(raw(compressed_ok))
            .expect("decompress ok");
        assert!(msg.data.len() <= max);
        assert_eq!(msg.data, ok_payload);

        // Direct take-bound probe: reading through take(max+1) yields exactly max+1
        // for a payload of max+1 zeros (frame-compressed), proving the ceiling is hit.
        {
            let mut compressed2 = Vec::new();
            {
                let mut encoder = snap::write::FrameEncoder::new(&mut compressed2);
                encoder.write_all(&vec![0u8; max + 1]).expect("write");
                encoder.flush().expect("flush");
                let _ = encoder.into_inner().expect("finish");
            }
            let mut decoder = snap::read::FrameDecoder::new(compressed2.as_slice());
            let mut limited = (&mut decoder).take((max as u64) + 1);
            let mut buf = Vec::new();
            limited.read_to_end(&mut buf).expect("take read");
            assert_eq!(
                buf.len(),
                max + 1,
                "take(max+1) must surface the overflow sentinel length"
            );
        }
    }
}
