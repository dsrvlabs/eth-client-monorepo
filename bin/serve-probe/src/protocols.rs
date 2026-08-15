//! Request/response SSZ bodies and protocol IDs for the five probe protocols.
//!
//! Independent of `services/p2p` — field layouts match the eth2 p2p-interface.

use std::io;

use cc_types::{Epoch, ForkDigest, Root, Slot};

use crate::codec::SszLimits;

/// Fixed SSZ length of `Status v2` (4+32+8+32+8+8).
pub const STATUS_V2_SSZ_LEN: usize = 92;

/// SSZ length of `BeaconBlocksByRange` request: two `uint64`.
pub const BY_RANGE_BLOCKS_SSZ_LEN: usize = 16;

/// Fixed prefix of a column by-range SSZ container before the columns list body.
pub const COLUMNS_BY_RANGE_FIXED_PREFIX: usize = 20;

/// The five protocols this probe speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// `/eth2/beacon_chain/req/status/2/`
    StatusV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_range/2/`
    BeaconBlocksByRangeV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_root/2/`
    BeaconBlocksByRootV2,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_range/1/`
    DataColumnSidecarsByRangeV1,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_root/1/`
    DataColumnSidecarsByRootV1,
}

impl Protocol {
    /// Full libp2p protocol ID including the `ssz_snappy` encoding suffix.
    #[must_use]
    pub const fn protocol_id(self) -> &'static str {
        match self {
            Self::StatusV2 => "/eth2/beacon_chain/req/status/2/ssz_snappy",
            Self::BeaconBlocksByRangeV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy"
            }
            Self::BeaconBlocksByRootV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy"
            }
            Self::DataColumnSidecarsByRangeV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy"
            }
            Self::DataColumnSidecarsByRootV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy"
            }
        }
    }

    /// Whether successful response chunks carry a 4-byte `ForkDigest` context.
    #[must_use]
    pub const fn has_context_bytes(self) -> bool {
        matches!(
            self,
            Self::BeaconBlocksByRangeV2
                | Self::BeaconBlocksByRootV2
                | Self::DataColumnSidecarsByRangeV1
                | Self::DataColumnSidecarsByRootV1
        )
    }

    /// Protocol-declared SSZ size bounds for requests.
    ///
    /// Mirrored in `cc_p2p::reqresp::Protocol::request_limits` and
    /// `cc_libp2p::request_limits` (S0-B-06). Values are copied, not shared —
    /// the encoder below stays independent (S0-B-07 / ADR-P4-12).
    #[must_use]
    pub const fn request_limits(self) -> SszLimits {
        match self {
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            // (start_slot, count, step) — three uint64s.
            Self::BeaconBlocksByRangeV2 => SszLimits { min: 24, max: 24 },
            // List[Root, 1024] of fixed-size elements: bare 32×n (not 10 MiB).
            Self::BeaconBlocksByRootV2 => SszLimits {
                min: 0,
                max: 1024 * 32,
            },
            // (start_slot, count, columns offset) + up to 128×u64 column indices.
            Self::DataColumnSidecarsByRangeV1 => SszLimits {
                min: 20,
                max: 20 + 128 * 8,
            },
            // List[DataColumnsByRootIdentifier, 1024] framing max; semantic bound is 128.
            Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 0,
                max: crate::codec::MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// Protocol-declared SSZ size bounds for a single success response chunk.
    #[must_use]
    pub const fn response_limits(self) -> SszLimits {
        match self {
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            Self::BeaconBlocksByRangeV2
            | Self::BeaconBlocksByRootV2
            | Self::DataColumnSidecarsByRangeV1
            | Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 0,
                max: crate::codec::MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// All five probe protocols.
    pub const ALL: [Self; 5] = [
        Self::StatusV2,
        Self::BeaconBlocksByRangeV2,
        Self::BeaconBlocksByRootV2,
        Self::DataColumnSidecarsByRangeV1,
        Self::DataColumnSidecarsByRootV1,
    ];
}

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
    /// Earliest slot this node can honestly serve.
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
            finalized_epoch: Epoch::new(u64::from_le_bytes(
                bytes[36..44]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "finalized_epoch"))?,
            )),
            head_root: Root::from_array(hr),
            head_slot: Slot::new(u64::from_le_bytes(
                bytes[76..84]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "head_slot"))?,
            )),
            earliest_available_slot: Slot::new(u64::from_le_bytes(
                bytes[84..92].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "earliest_available_slot")
                })?,
            )),
        })
    }

    /// Build a probe-side Status with the CLI fork digest and zero chain fields.
    #[must_use]
    pub fn for_probe(fork_digest: ForkDigest) -> Self {
        Self {
            fork_digest,
            finalized_root: Root::ZERO,
            finalized_epoch: Epoch::ZERO,
            head_root: Root::ZERO,
            head_slot: Slot::ZERO,
            earliest_available_slot: Slot::ZERO,
        }
    }
}

/// `BeaconBlocksByRange v2` request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlocksByRangeRequest {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots to cover.
    pub count: u64,
}

impl BlocksByRangeRequest {
    /// SSZ-encode `(start_slot, count)`.
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; BY_RANGE_BLOCKS_SSZ_LEN] {
        let mut out = [0u8; BY_RANGE_BLOCKS_SSZ_LEN];
        out[0..8].copy_from_slice(&self.start_slot.as_u64().to_le_bytes());
        out[8..16].copy_from_slice(&self.count.to_le_bytes());
        out
    }

    /// SSZ-decode; length must be exactly 16.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != BY_RANGE_BLOCKS_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "by_range SSZ length {} != {BY_RANGE_BLOCKS_SSZ_LEN}",
                    bytes.len()
                ),
            ));
        }
        let start = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "start_slot"))?,
        );
        let count = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "count"))?,
        );
        Ok(Self {
            start_slot: Slot::new(start),
            count,
        })
    }
}

/// `BeaconBlocksByRoot v2` request body: ordered list of roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlocksByRootRequest {
    /// Roots in request order.
    pub roots: Vec<Root>,
}

impl BlocksByRootRequest {
    /// SSZ-encode as `List[Root, 1024]` (offset + packed roots).
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        // Variable list: 4-byte offset to elements (= 4), then 32-byte roots.
        let mut out = Vec::with_capacity(4 + self.roots.len() * 32);
        out.extend_from_slice(&4u32.to_le_bytes());
        for r in &self.roots {
            out.extend_from_slice(r.as_slice());
        }
        out
    }

    /// SSZ-decode `List[Root, 1024]`.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root list too short",
            ));
        }
        let offset = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "by_root offset"))?,
        ) as usize;
        if offset != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_root unexpected offset {offset}"),
            ));
        }
        let rest = &bytes[4..];
        if !rest.len().is_multiple_of(32) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root roots not multiple of 32",
            ));
        }
        let n = rest.len() / 32;
        if n > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root list exceeds 1024",
            ));
        }
        let mut roots = Vec::with_capacity(n);
        for i in 0..n {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&rest[i * 32..(i + 1) * 32]);
            roots.push(Root::from_array(arr));
        }
        Ok(Self { roots })
    }
}

/// `DataColumnSidecarsByRange v1` request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnsByRangeRequest {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots to cover.
    pub count: u64,
    /// Requested column indices.
    pub columns: Vec<u64>,
}

impl ColumnsByRangeRequest {
    /// SSZ-encode `(start_slot, count, columns)` as a container.
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COLUMNS_BY_RANGE_FIXED_PREFIX + self.columns.len() * 8);
        out.extend_from_slice(&self.start_slot.as_u64().to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&(COLUMNS_BY_RANGE_FIXED_PREFIX as u32).to_le_bytes());
        for c in &self.columns {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// SSZ-decode.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < COLUMNS_BY_RANGE_FIXED_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "by_range column SSZ length {} < {COLUMNS_BY_RANGE_FIXED_PREFIX}",
                    bytes.len()
                ),
            ));
        }
        let start = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "start_slot"))?,
        );
        let count = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "count"))?,
        );
        let offset = u32::from_le_bytes(
            bytes[16..20]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "columns offset"))?,
        ) as usize;
        if offset != COLUMNS_BY_RANGE_FIXED_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_range unexpected columns offset {offset}"),
            ));
        }
        let rest = &bytes[COLUMNS_BY_RANGE_FIXED_PREFIX..];
        if !rest.len().is_multiple_of(8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_range columns not a multiple of 8",
            ));
        }
        let n = rest.len() / 8;
        if n > 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_range columns list exceeds 128",
            ));
        }
        let mut columns = Vec::with_capacity(n);
        for i in 0..n {
            let c = u64::from_le_bytes(
                rest[i * 8..(i + 1) * 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "column index"))?,
            );
            columns.push(c);
        }
        Ok(Self {
            start_slot: Slot::new(start),
            count,
            columns,
        })
    }
}

/// `DataColumnSidecarsByRoot v1` request body: ordered list of identifiers.
///
/// Each identifier is encoded as `Root (32) + offset (4) + column indices (8 each)`.
/// For the probe we only need encode of a single-identifier list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnsByRootRequest {
    /// `(block_root, column_indices)` pairs.
    pub identifiers: Vec<(Root, Vec<u64>)>,
}

impl ColumnsByRootRequest {
    /// SSZ-encode as `List[DataColumnsByRootIdentifier, 128]`.
    ///
    /// Outer list is offset-based; each element is a container with fixed root
    /// + offset to its column-index list.
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        // Outer list: offset (=4) then concatenated element encodings.
        let mut elements = Vec::new();
        for (root, cols) in &self.identifiers {
            // Container: root(32) + offset(4) + columns body.
            let mut el = Vec::with_capacity(36 + cols.len() * 8);
            el.extend_from_slice(root.as_slice());
            el.extend_from_slice(&36u32.to_le_bytes());
            for c in cols {
                el.extend_from_slice(&c.to_le_bytes());
            }
            elements.push(el);
        }
        // SSZ List of variable-size elements: offset table then bodies.
        let n = elements.len();
        let mut out = Vec::new();
        // Single outer offset to first element of the list body = 4.
        out.extend_from_slice(&4u32.to_le_bytes());
        // Element offsets relative to start of list body (after the 4-byte outer offset).
        // Actually for List[Container]: encoding is offset-to-elements (=4) then
        // for variable elements, an offset table of n u32s then bodies.
        // Simpler fixed path for empty list:
        if n == 0 {
            return out;
        }
        // Re-encode properly: List[T] for variable T uses offsets relative to
        // the start of the list serialization.
        // Layout: [offset_0, offset_1, ..., offset_{n-1}, body_0, body_1, ...]
        // where offset_0 = 4*n.
        let mut bodies = Vec::new();
        let mut offsets = Vec::with_capacity(n);
        let mut cursor = (4 * n) as u32;
        for el in &elements {
            offsets.push(cursor);
            cursor = cursor.saturating_add(el.len() as u32);
            bodies.extend_from_slice(el);
        }
        out.clear();
        for o in offsets {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&bodies);
        out
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn status_ssz_roundtrip() {
        let s = StatusV2 {
            fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
            finalized_root: Root::from_array([0x11; 32]),
            finalized_epoch: Epoch::new(10),
            head_root: Root::from_array([0x22; 32]),
            head_slot: Slot::new(320),
            earliest_available_slot: Slot::new(300),
        };
        let bytes = s.to_ssz_bytes();
        assert_eq!(bytes.len(), 92);
        assert_eq!(StatusV2::from_ssz_bytes(&bytes).unwrap(), s);
    }

    #[test]
    fn blocks_by_range_ssz_roundtrip() {
        let r = BlocksByRangeRequest {
            start_slot: Slot::new(99),
            count: 1,
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(BlocksByRangeRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    #[test]
    fn columns_by_range_ssz_roundtrip() {
        let r = ColumnsByRangeRequest {
            start_slot: Slot::new(50),
            count: 1,
            columns: vec![0, 1, 2, 3],
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(ColumnsByRangeRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    #[test]
    fn protocol_ids_end_with_ssz_snappy() {
        for p in Protocol::ALL {
            assert!(
                p.protocol_id().ends_with("/ssz_snappy"),
                "{}",
                p.protocol_id()
            );
        }
    }

    /// S0-B-06: probe request-limit table equals the live libp2p codec table
    /// on the five overlapping protocol IDs.
    #[test]
    fn request_limits_match_libp2p_codec() {
        const GLOBAL: usize = cc_libp2p::REQRESP_MAX_PAYLOAD_SIZE;
        for p in Protocol::ALL {
            let proto = p.request_limits();
            let codec = cc_libp2p::request_limits(p.protocol_id(), GLOBAL);
            assert_eq!(
                (proto.min, proto.max),
                codec,
                "{}: probe Protocol::request_limits != cc_libp2p::request_limits",
                p.protocol_id()
            );
        }
    }
}
