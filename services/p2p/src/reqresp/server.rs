//! Block- and column-protocol serve dispatcher — Architecture §7.1 / §7.3 / CC-23c+d.
//!
//! Turns a planned response into a framed wire body, applying chunk-level
//! inbound rate limiting (truncate, never error mid-stream).
//!
//! - **CC-23c:** three block protocols (Blocks rate-limit kind)
//! - **CC-23d:** two column protocols (Columns rate-limit kind)

use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_libp2p::PeerId;
use cc_types::{Epoch, Mainnet, Preset};

use crate::backfill::BackfillCache;
use crate::fork_digest::ForkContext;
use crate::metrics::P2pMetrics;
use crate::reqresp::Protocol;
use crate::reqresp::blocks::{BlockServeCtx, BlockServeError, PlannedBlocks, plan_block_response};
use crate::reqresp::codec::{ResponseChunk, ResponseCode, SszSnappyFraming};
use crate::reqresp::columns::{ColumnServeCtx, plan_column_response};
use crate::reqresp::limits::{
    ChunkBudgetResult, InboundRateLimiter, RATE_LIMIT_ERROR_MESSAGE, RateLimitKind,
    RateLimitOutcome,
};

/// Shared block-serve state owned by the swarm task (optional until cache is
/// attached at runtime).
#[derive(Debug)]
pub struct BlockServeState<P: Preset = Mainnet> {
    /// Backfill cache — sole block source for all three protocols.
    pub cache: Arc<Mutex<BackfillCache<P>>>,
    /// Slots per epoch (mainnet 32).
    pub slots_per_epoch: u64,
    /// `FULU_FORK_EPOCH` from the loaded network config.
    pub fulu_fork_epoch: Epoch,
}

impl<P: Preset> BlockServeState<P> {
    /// Wrap a cache for host attachment.
    #[must_use]
    pub fn new(cache: BackfillCache<P>, slots_per_epoch: u64, fulu_fork_epoch: Epoch) -> Self {
        Self {
            cache: Arc::new(Mutex::new(cache)),
            slots_per_epoch,
            fulu_fork_epoch,
        }
    }

    /// Construct from an already-shared cache.
    #[must_use]
    pub fn from_shared(
        cache: Arc<Mutex<BackfillCache<P>>>,
        slots_per_epoch: u64,
        fulu_fork_epoch: Epoch,
    ) -> Self {
        Self {
            cache,
            slots_per_epoch,
            fulu_fork_epoch,
        }
    }
}

/// Outcome of serving one inbound block request (for metrics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeResultLabel {
    /// At least one success chunk returned.
    Ok,
    /// Code 1 invalid request.
    InvalidRequest,
    /// Code 3 resource unavailable.
    ResourceUnavailable,
    /// Code 2 rate limited (admission or empty-bucket error).
    RateLimited,
    /// Response channel / encode failure.
    Failure,
}

impl ServeResultLabel {
    /// Prometheus `result` label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::InvalidRequest => "invalid_request",
            Self::ResourceUnavailable => "resource_unavailable",
            Self::RateLimited => "rate_limited",
            Self::Failure => "failure",
        }
    }
}

/// Fully framed response body + metric label.
#[derive(Debug, Clone)]
pub struct FramedServe {
    /// Snappy-framed multi-chunk (or single error) body.
    pub framed: Vec<u8>,
    /// Metric result label.
    pub label: ServeResultLabel,
    /// Rate-limit outcome when the bucket fired (for score penalty).
    pub rate_limited: Option<RateLimitOutcome>,
}

/// Plan + rate-limit + frame a block-protocol response.
///
/// Caller supplies the unlocked cache borrow, fork context, limiter, and
/// current epoch. Metrics are optional (unit tests may pass `None`).
#[allow(clippy::too_many_arguments)] // Serve path deliberately threads every dep explicitly.
pub fn serve_block_protocol<P: Preset>(
    protocol: Protocol,
    request_ssz: &[u8],
    peer: PeerId,
    cache: &BackfillCache<P>,
    fork_ctx: &mut ForkContext,
    slots_per_epoch: u64,
    fulu_fork_epoch: Epoch,
    current_epoch: Epoch,
    limiter: &mut InboundRateLimiter,
    now: Instant,
    metrics: Option<&P2pMetrics>,
) -> FramedServe {
    debug_assert!(protocol.is_block_protocol());

    let mut ctx = BlockServeCtx {
        cache,
        fork_ctx,
        slots_per_epoch,
        current_epoch,
        fulu_fork_epoch,
    };

    let planned = match plan_block_response(protocol, request_ssz, &mut ctx) {
        Ok(p) => p,
        Err(e) => {
            let label = match e {
                BlockServeError::InvalidRequest(_) => ServeResultLabel::InvalidRequest,
                BlockServeError::ResourceUnavailable(_) => ServeResultLabel::ResourceUnavailable,
            };
            return frame_error(e.to_chunk(), protocol, label);
        }
    };

    apply_budget_and_frame_kind(
        protocol,
        peer,
        planned,
        RateLimitKind::Blocks,
        limiter,
        now,
        metrics,
    )
}

/// Apply chunk budget then frame. Mid-stream empty bucket → truncate
/// (valid short success). Empty start → rate-limit error response.
fn apply_budget_and_frame_kind(
    protocol: Protocol,
    peer: PeerId,
    planned: PlannedBlocks,
    kind: RateLimitKind,
    limiter: &mut InboundRateLimiter,
    now: Instant,
    metrics: Option<&P2pMetrics>,
) -> FramedServe {
    match limiter.apply_chunk_budget(peer, kind, planned.chunks, now) {
        ChunkBudgetResult::Serve { chunks, limited } => {
            if let Some(ref outcome) = limited
                && let Some(m) = metrics
            {
                InboundRateLimiter::record_violation(
                    m, &mut 0.0, // score applied via host PeerPenalty path
                    *outcome, protocol,
                );
            }
            // Truncated success still counts as ok for the requester;
            // ratelimit series captures the pressure.
            frame_success(&chunks, protocol, ServeResultLabel::Ok, limited)
        }
        ChunkBudgetResult::Error { outcome, chunk } => {
            if let Some(m) = metrics {
                InboundRateLimiter::record_violation(m, &mut 0.0, outcome, protocol);
            }
            FramedServe {
                framed: encode_chunks(std::slice::from_ref(&chunk), protocol),
                label: ServeResultLabel::RateLimited,
                rate_limited: Some(outcome),
            }
        }
    }
}

fn frame_success(
    chunks: &[ResponseChunk],
    protocol: Protocol,
    label: ServeResultLabel,
    rate_limited: Option<RateLimitOutcome>,
) -> FramedServe {
    FramedServe {
        framed: encode_chunks(chunks, protocol),
        label,
        rate_limited,
    }
}

fn frame_error(chunk: ResponseChunk, protocol: Protocol, label: ServeResultLabel) -> FramedServe {
    FramedServe {
        framed: encode_chunks(std::slice::from_ref(&chunk), protocol),
        label,
        rate_limited: None,
    }
}

fn encode_chunks(chunks: &[ResponseChunk], protocol: Protocol) -> Vec<u8> {
    match SszSnappyFraming::encode_response(chunks, protocol) {
        Ok(bytes) => bytes,
        Err(_) => {
            // Minimal code-2 fallback — should not happen with well-formed chunks.
            vec![ResponseCode::ServerError.as_u8()]
        }
    }
}

/// Whether `protocol` is one of the three block serve paths.
#[must_use]
pub fn is_block_serve_protocol(protocol: Protocol) -> bool {
    protocol.is_block_protocol()
}

/// Whether `protocol` is one of the two column serve paths (CC-23d).
#[must_use]
pub fn is_column_serve_protocol(protocol: Protocol) -> bool {
    protocol.is_column_protocol()
}

/// Plan + rate-limit + frame a column-protocol response (CC-23d).
#[allow(clippy::too_many_arguments)]
pub fn serve_column_protocol<P: Preset>(
    protocol: Protocol,
    request_ssz: &[u8],
    peer: PeerId,
    cache: &BackfillCache<P>,
    fork_ctx: &mut ForkContext,
    slots_per_epoch: u64,
    fulu_fork_epoch: Epoch,
    current_epoch: Epoch,
    limiter: &mut InboundRateLimiter,
    now: Instant,
    metrics: Option<&P2pMetrics>,
) -> FramedServe {
    debug_assert!(protocol.is_column_protocol());

    let mut ctx = ColumnServeCtx {
        cache,
        fork_ctx,
        slots_per_epoch,
        current_epoch,
        fulu_fork_epoch,
        by_root_fault: crate::reqresp::columns::ByRootFaultPolicy::Honest,
    };

    let planned = match plan_column_response(protocol, request_ssz, &mut ctx) {
        Ok(p) => p,
        Err(e) => {
            let label = match e {
                BlockServeError::InvalidRequest(_) => ServeResultLabel::InvalidRequest,
                BlockServeError::ResourceUnavailable(_) => ServeResultLabel::ResourceUnavailable,
            };
            return frame_error(e.to_chunk(), protocol, label);
        }
    };

    apply_budget_and_frame_kind(
        protocol,
        peer,
        planned,
        RateLimitKind::Columns,
        limiter,
        now,
        metrics,
    )
}

/// Rate-limit error chunk (shared with host admission path).
#[must_use]
pub fn rate_limit_error_chunk() -> ResponseChunk {
    ResponseChunk::Error {
        code: ResponseCode::ServerError.as_u8(),
        message: RATE_LIMIT_ERROR_MESSAGE.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::backfill::BackfillCache;
    use crate::fork_digest::ForkContext;
    use crate::reqresp::blocks::{BlocksByRangeRequest, MAX_REQUEST_BLOCKS_DENEB};
    use crate::reqresp::codec::SszSnappyFraming;
    use cc_types::primitives::{Root, Slot};
    use cc_types::{ChainConfig, Mainnet, Preset, SignedBeaconBlock};
    use prometheus_client::registry::Registry;
    use std::sync::Arc;
    use std::time::Instant;

    const HOODI: &str = include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");

    fn hoodi() -> ChainConfig {
        ChainConfig::from_yaml_str(HOODI).unwrap()
    }

    fn block_at(slot: u64, parent: Root) -> Arc<SignedBeaconBlock<Mainnet>> {
        let mut b = SignedBeaconBlock::<Mainnet>::default();
        b.message.slot = Slot::new(slot);
        b.message.parent_root = parent;
        let mut sr = [0u8; 32];
        sr[0..8].copy_from_slice(&slot.to_le_bytes());
        sr[8] = 0x7;
        b.message.state_root = Root::from_array(sr);
        Arc::new(b)
    }

    fn filled(lo: u64, hi: u64) -> BackfillCache<Mainnet> {
        let mut cache = BackfillCache::with_bounds(
            Slot::new(0),
            0u64..8,
            std::iter::empty(),
            1 << 30,
            2048,
            2048 * 8,
        );
        let mut parent = Root::ZERO;
        for s in lo..=hi {
            let block = block_at(s, parent);
            let root = Root::from(block.canonical_root());
            cache.insert_block(Slot::new(s), root, block);
            parent = root;
        }
        cache.set_head_slot(Slot::new(hi));
        // CC-48: seed advertised window for serve-handler unit tests.
        cache.seed_advertised_from_floor();
        cache
    }

    fn fork_ctx() -> ForkContext {
        ForkContext::new(hoodi(), Root::from_array([0xAB; 32]), Epoch::new(60_000))
    }

    /// Floor at genesis so low test slots are inside the historical window.
    fn test_epochs() -> (Epoch, Epoch) {
        (Epoch::new(0), Epoch::new(0))
    }

    #[test]
    fn serve_by_range_ok_increments_path() {
        let cache = filled(200, 210);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);

        let req = BlocksByRangeRequest {
            start_slot: Slot::new(200),
            count: 3,
        };
        let (cur, fulu) = test_epochs();
        let framed = serve_block_protocol(
            Protocol::BeaconBlocksByRangeV2,
            &req.to_ssz_bytes(),
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            Some(&metrics),
        );
        assert_eq!(framed.label, ServeResultLabel::Ok);
        let chunks =
            SszSnappyFraming::decode_response(&framed.framed, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        assert_eq!(chunks.len(), 3);
        metrics.inc_reqresp_inbound(
            Protocol::BeaconBlocksByRangeV2.as_str(),
            framed.label.as_str(),
        );
        assert_eq!(
            metrics.reqresp_inbound_count("beacon_blocks_by_range", "ok"),
            1
        );
    }

    #[test]
    fn serve_oversized_is_invalid_request() {
        let cache = filled(200, 210);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let req = BlocksByRangeRequest {
            start_slot: Slot::new(200),
            count: MAX_REQUEST_BLOCKS_DENEB + 1,
        };
        let (cur, fulu) = test_epochs();
        let framed = serve_block_protocol(
            Protocol::BeaconBlocksByRangeV2,
            &req.to_ssz_bytes(),
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            None,
        );
        assert_eq!(framed.label, ServeResultLabel::InvalidRequest);
        let chunks =
            SszSnappyFraming::decode_response(&framed.framed, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            ResponseChunk::Error { code, .. } => {
                assert_eq!(*code, ResponseCode::InvalidRequest.as_u8());
            }
            ResponseChunk::Success { .. } => panic!("must be error"),
        }
    }

    #[test]
    fn rate_limit_truncates_to_valid_short_response() {
        let cache = filled(200, 200 + 50);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let peer = PeerId::random();
        let now = Instant::now();
        // Leave exactly 2 tokens.
        for _ in 0..126 {
            assert_eq!(
                lim.check_chunk(peer, RateLimitKind::Blocks, now),
                RateLimitOutcome::Allow
            );
        }
        let req = BlocksByRangeRequest {
            start_slot: Slot::new(200),
            count: 50,
        };
        let (cur, fulu) = test_epochs();
        let framed = serve_block_protocol(
            Protocol::BeaconBlocksByRangeV2,
            &req.to_ssz_bytes(),
            peer,
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            now,
            None,
        );
        assert_eq!(framed.label, ServeResultLabel::Ok);
        assert!(framed.rate_limited.is_some());
        let chunks =
            SszSnappyFraming::decode_response(&framed.framed, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        assert_eq!(chunks.len(), 2, "must truncate to remaining tokens");
        // Truncated response still decodes as a complete multi-chunk success stream.
        for c in &chunks {
            assert!(matches!(c, ResponseChunk::Success { .. }));
        }
    }

    #[test]
    fn metrics_cover_all_three_protocols() {
        let cache = filled(300, 310);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);

        let (cur, fulu) = test_epochs();

        // by_range
        let r = BlocksByRangeRequest {
            start_slot: Slot::new(300),
            count: 1,
        };
        let f = serve_block_protocol(
            Protocol::BeaconBlocksByRangeV2,
            &r.to_ssz_bytes(),
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            Some(&metrics),
        );
        metrics.inc_reqresp_inbound("beacon_blocks_by_range", f.label.as_str());

        // by_root
        let (root, _, _) = cache.block_at_slot(Slot::new(301)).unwrap();
        let root_ssz =
            crate::reqresp::blocks::BlocksByRootRequest { roots: vec![root] }.to_ssz_bytes();
        let f = serve_block_protocol(
            Protocol::BeaconBlocksByRootV2,
            &root_ssz,
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            Some(&metrics),
        );
        metrics.inc_reqresp_inbound("beacon_blocks_by_root", f.label.as_str());

        // by_head
        let (head_root, _, _) = cache.block_at_slot(Slot::new(310)).unwrap();
        let head_ssz = crate::reqresp::blocks::BlocksByHeadRequest {
            beacon_root: head_root,
            count: 2,
        }
        .to_ssz_bytes();
        let f = serve_block_protocol(
            Protocol::BeaconBlocksByHeadV1,
            &head_ssz,
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            Some(&metrics),
        );
        metrics.inc_reqresp_inbound("beacon_blocks_by_head", f.label.as_str());

        assert_eq!(
            metrics.reqresp_inbound_count("beacon_blocks_by_range", "ok"),
            1
        );
        assert_eq!(
            metrics.reqresp_inbound_count("beacon_blocks_by_root", "ok"),
            1
        );
        assert_eq!(
            metrics.reqresp_inbound_count("beacon_blocks_by_head", "ok"),
            1
        );
    }

    fn filled_cols(lo: u64, hi: u64) -> BackfillCache<Mainnet> {
        use cc_types::sidecar::DataColumnSidecar;
        let mut cache =
            BackfillCache::with_bounds(Slot::new(0), 0u64..8, 0u64..4, 1 << 30, 2048, 2048 * 8);
        let mut parent = Root::ZERO;
        for s in lo..=hi {
            let block = block_at(s, parent);
            let root = Root::from(block.canonical_root());
            cache.insert_block(Slot::new(s), root, Arc::clone(&block));
            for c in 0..4u64 {
                let mut sc = DataColumnSidecar::<Mainnet> {
                    index: c,
                    ..Default::default()
                };
                sc.signed_block_header.message.slot = Slot::new(s);
                cache.insert_column(Slot::new(s), root, c, Arc::new(sc));
            }
            parent = root;
        }
        cache.set_head_slot(Slot::new(hi));
        // CC-48: seed advertised window for serve-handler unit tests.
        cache.seed_advertised_from_floor();
        cache
    }

    #[test]
    fn serve_column_by_range_ok_increments_path() {
        let cache = filled_cols(200, 210);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let mut registry = Registry::default();
        let metrics = P2pMetrics::register(&mut registry);

        let req = crate::reqresp::columns::ColumnsByRangeRequest {
            start_slot: Slot::new(200),
            count: 2,
            columns: vec![0, 1],
        };
        let (cur, fulu) = test_epochs();
        let framed = serve_column_protocol(
            Protocol::DataColumnSidecarsByRangeV1,
            &req.to_ssz_bytes(),
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            Some(&metrics),
        );
        assert_eq!(framed.label, ServeResultLabel::Ok);
        metrics.inc_reqresp_inbound(
            Protocol::DataColumnSidecarsByRangeV1.as_str(),
            framed.label.as_str(),
        );
        assert_eq!(
            metrics.reqresp_inbound_count("data_column_sidecars_by_range", "ok"),
            1
        );
        // Outbound mirror (client path metric surface).
        metrics.inc_reqresp_outbound("data_column_sidecars_by_range", "ok");
        metrics.inc_reqresp_outbound("data_column_sidecars_by_root", "ok");
        assert_eq!(
            metrics.reqresp_outbound_count("data_column_sidecars_by_range", "ok"),
            1
        );
        assert_eq!(
            metrics.reqresp_outbound_count("data_column_sidecars_by_root", "ok"),
            1
        );
    }

    #[test]
    fn serve_column_by_root_ok() {
        let cache = filled_cols(300, 305);
        let mut fork = fork_ctx();
        let mut lim = InboundRateLimiter::new();
        let (root, _, _) = cache.block_at_slot(Slot::new(301)).unwrap();
        let req = crate::reqresp::columns::ColumnsByRootRequest {
            identifiers: vec![crate::reqresp::columns::make_by_root_identifier(
                root,
                &[0, 1],
            )],
        };
        let (cur, fulu) = test_epochs();
        let framed = serve_column_protocol(
            Protocol::DataColumnSidecarsByRootV1,
            &req.to_ssz_bytes(),
            PeerId::random(),
            &cache,
            &mut fork,
            Mainnet::SLOTS_PER_EPOCH,
            fulu,
            cur,
            &mut lim,
            Instant::now(),
            None,
        );
        assert_eq!(framed.label, ServeResultLabel::Ok);
        let chunks =
            SszSnappyFraming::decode_response(&framed.framed, Protocol::DataColumnSidecarsByRootV1)
                .unwrap();
        assert_eq!(chunks.len(), 2);
    }
}
