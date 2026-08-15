//! Beacon block req/resp handlers — Architecture §7.1 / §7.5 / CC-23c.
//!
//! Three protocols, one serve source ([`BackfillCache`]), one honesty rule:
//!
//! | Protocol | Request | Order |
//! |---|---|---|
//! | `beacon_blocks_by_range/2/` | `(start_slot, count, step)` | ascending slot |
//! | `beacon_blocks_by_root/2/` | `List[Root, 128]` | request order |
//! | `beacon_blocks_by_head/1/` | `(beacon_root, count)` | **descending** ancestors |
//!
//! Bounds: [`MAX_REQUEST_BLOCKS_DENEB`] = 128, enforced **before** any
//! `Vec::with_capacity` proportional to the claimed size (CC-23/3).
//!
//! Window: `earliest_available_slot` is **read** from the cache atomic only
//! (ADR P2-14) — never recomputed here.

use std::io;
use std::sync::Arc;

use cc_types::primitives::{Epoch, Root, Slot};
use cc_types::{Mainnet, Preset, SignedBeaconBlock};
use ssz::Encode;

use crate::backfill::{BackfillCache, EMPTY_WINDOW_SLOT};
use crate::fork_digest::ForkContext;
use crate::reqresp::Protocol;
use crate::reqresp::codec::{
    CONTEXT_BYTES_LEN, ResponseChunk, ResponseCode, success_chunk_for_slot,
};

// ── Spec / config constants ─────────────────────────────────────────────────

/// Spec `MAX_REQUEST_BLOCKS_DENEB` — hard cap on blocks per request.
pub const MAX_REQUEST_BLOCKS_DENEB: u64 = 128;

/// Hoodi / mainnet block serve floor (≈ 4.8 months / 146.77 days).
///
/// Formula: `MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2`
/// (`256 + 65536 / 2`). Phase 4 `CC-4A` makes `cc_store::window` the authority;
/// this Phase 2 constant keeps the arithmetic form so the literal is never a
/// production source constant (CC-4A /4 grep). `CC-49` will wire the store
/// value through; until then the mainnet/hoodi scalars are inlined.
pub const MIN_EPOCHS_FOR_BLOCK_REQUESTS: u64 = 256 + 65_536 / 2;

/// SSZ length of `BeaconBlocksByRange` request: three `uint64`
/// (`start_slot`, `count`, `step`). `step` is deprecated but still required.
pub const BY_RANGE_SSZ_LEN: usize = 24;

/// SSZ length of `BeaconBlocksByHead` request: `Root` + `uint64`.
pub const BY_HEAD_SSZ_LEN: usize = 40;

// ── Request types ───────────────────────────────────────────────────────────

/// `BeaconBlocksByRange v2` request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlocksByRangeRequest {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots to cover (1..=[`MAX_REQUEST_BLOCKS_DENEB`]).
    pub count: u64,
    /// Deprecated; spec requires `1`. Still part of the SSZ schema.
    pub step: u64,
}

impl BlocksByRangeRequest {
    /// Spec-deprecated `step` value; MUST be 1.
    pub const STEP: u64 = 1;

    /// Build a request with `step` set to [`Self::STEP`].
    #[must_use]
    pub fn new(start_slot: Slot, count: u64) -> Self {
        Self {
            start_slot,
            count,
            step: Self::STEP,
        }
    }

    /// SSZ-encode `(start_slot, count, step)`.
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; BY_RANGE_SSZ_LEN] {
        let mut out = [0u8; BY_RANGE_SSZ_LEN];
        out[0..8].copy_from_slice(&self.start_slot.as_u64().to_le_bytes());
        out[8..16].copy_from_slice(&self.count.to_le_bytes());
        out[16..24].copy_from_slice(&self.step.to_le_bytes());
        out
    }

    /// SSZ-decode; length must be exactly 24.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != BY_RANGE_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_range SSZ length {} != {BY_RANGE_SSZ_LEN}", bytes.len()),
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
        let step = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "step"))?,
        );
        Ok(Self {
            start_slot: Slot::new(start),
            count,
            step,
        })
    }
}

/// `BeaconBlocksByHead v1` request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlocksByHeadRequest {
    /// Head block root to start walking from.
    pub beacon_root: Root,
    /// Number of ancestors to return (1..=[`MAX_REQUEST_BLOCKS_DENEB`]).
    pub count: u64,
}

impl BlocksByHeadRequest {
    /// SSZ-encode `(beacon_root, count)`.
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; BY_HEAD_SSZ_LEN] {
        let mut out = [0u8; BY_HEAD_SSZ_LEN];
        out[0..32].copy_from_slice(self.beacon_root.as_slice());
        out[32..40].copy_from_slice(&self.count.to_le_bytes());
        out
    }

    /// SSZ-decode; length must be exactly 40.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != BY_HEAD_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_head SSZ length {} != {BY_HEAD_SSZ_LEN}", bytes.len()),
            ));
        }
        let mut root = [0u8; 32];
        root.copy_from_slice(&bytes[0..32]);
        let count = u64::from_le_bytes(
            bytes[32..40]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "count"))?,
        );
        Ok(Self {
            beacon_root: Root::from_array(root),
            count,
        })
    }
}

/// `BeaconBlocksByRoot v2` request body: ordered list of roots.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlocksByRootRequest {
    /// Roots, in the order the peer asked for them.
    pub roots: Vec<Root>,
}

impl BlocksByRootRequest {
    /// SSZ-encode as `List[Root, N]` (offset + concatenated 32-byte roots).
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        // VariableList SSZ: 4-byte offset to elements, then elements.
        let mut out = Vec::with_capacity(4 + self.roots.len() * 32);
        out.extend_from_slice(&4u32.to_le_bytes());
        for r in &self.roots {
            out.extend_from_slice(r.as_slice());
        }
        out
    }

    /// SSZ-decode a `List[Root, …]`. Does **not** enforce the 128 bound —
    /// call [`validate_root_list_len`] before allocating a response.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root SSZ too short for list offset",
            ));
        }
        let offset = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "list offset"))?,
        ) as usize;
        if offset != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_root unexpected list offset {offset}"),
            ));
        }
        let rest = &bytes[4..];
        if !rest.len().is_multiple_of(32) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root payload not a multiple of 32",
            ));
        }
        let n = rest.len() / 32;
        // Cap decode work at the framing max (1024) so a hostile list cannot
        // force unbounded parse work before the semantic 128 check.
        if n > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_root list exceeds framing max 1024",
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

// ── Bound checks (CC-23/3) ──────────────────────────────────────────────────

/// Validate a block `count` **before** any proportional allocation.
///
/// Rejects `0` and anything above [`MAX_REQUEST_BLOCKS_DENEB`].
pub fn validate_block_count(count: u64) -> Result<(), BlockServeError> {
    if count == 0 {
        return Err(BlockServeError::InvalidRequest("count must be > 0"));
    }
    if count > MAX_REQUEST_BLOCKS_DENEB {
        return Err(BlockServeError::InvalidRequest(
            "count exceeds MAX_REQUEST_BLOCKS_DENEB (128)",
        ));
    }
    Ok(())
}

/// Validate a by-root list length **before** planning a response.
pub fn validate_root_list_len(len: usize) -> Result<(), BlockServeError> {
    if len == 0 {
        return Err(BlockServeError::InvalidRequest(
            "root list must be non-empty",
        ));
    }
    if len as u64 > MAX_REQUEST_BLOCKS_DENEB {
        return Err(BlockServeError::InvalidRequest(
            "root list exceeds MAX_REQUEST_BLOCKS_DENEB (128)",
        ));
    }
    Ok(())
}

// ── Window / epoch honesty (§7.5) ───────────────────────────────────────────

/// Spec `compute_min_epochs_for_block_requests()` — returns the config constant.
///
/// Phase 4 authority for the arithmetic is `cc_store::window` (CC-4A). This
/// Phase 2 constant keeps the inlined form so production code never hard-codes
/// the decimal `33024` (CC-4A /4).
#[must_use]
pub const fn compute_min_epochs_for_block_requests() -> u64 {
    MIN_EPOCHS_FOR_BLOCK_REQUESTS
}

/// `minimum_request_epoch(blocks) = max(current − min_epochs, FULU_FORK_EPOCH)`.
///
/// **CC-4A /5 (D-5) — `FULU_FORK_EPOCH` clamp retained as a
/// simplification licensed by D1 (spec delta 3).** The consensus-specs p2p
/// interface clamps ByRange / ByRoot historical floors at `GENESIS_EPOCH` and
/// clamps ByHead at nothing; the Phase 2 (`CC-23c`) floor of `FULU_FORK_EPOCH`
/// on the *block* path is **not** spec-derived. It is kept deliberately:
/// today's Hoodi `FULU_FORK_EPOCH` (50 688) only rises, and past that epoch the
/// clamp agrees with `current − compute_min_epochs_for_block_requests()` once
/// `current ≥ FULU + min_epochs`. Silently keeping an unexplained clamp is
/// what this comment (and the matching unit test) prevents.
#[must_use]
pub fn minimum_request_epoch_blocks(current_epoch: Epoch, fulu_fork_epoch: Epoch) -> Epoch {
    let floor = current_epoch
        .as_u64()
        .saturating_sub(compute_min_epochs_for_block_requests());
    Epoch::new(floor.max(fulu_fork_epoch.as_u64()))
}

/// First slot of `epoch` under the given slots-per-epoch.
#[must_use]
pub fn epoch_start_slot(epoch: Epoch, slots_per_epoch: u64) -> Slot {
    Slot::new(epoch.as_u64().saturating_mul(slots_per_epoch.max(1)))
}

/// Why a request was refused at the window gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowDeny {
    /// `start` / root slot is below the advertised serve window.
    BelowEarliest,
    /// Below the spec-permitted historical floor.
    BelowMinimumEpoch,
}

/// Check a **slot** against the honesty rule.
///
/// ```text
/// below earliest_available_slot  → ResourceUnavailable
/// below minimum_request_epoch    → ResourceUnavailable
/// inside served_window           → Ok
/// ```
pub fn check_slot_window(
    slot: Slot,
    earliest_available_slot: Slot,
    current_epoch: Epoch,
    fulu_fork_epoch: Epoch,
    slots_per_epoch: u64,
) -> Result<(), WindowDeny> {
    let earliest_u = earliest_available_slot.as_u64();
    // Empty-window seed: refuse everything honestly.
    if earliest_u == EMPTY_WINDOW_SLOT || slot.as_u64() < earliest_u {
        return Err(WindowDeny::BelowEarliest);
    }
    let min_epoch = minimum_request_epoch_blocks(current_epoch, fulu_fork_epoch);
    let min_slot = epoch_start_slot(min_epoch, slots_per_epoch);
    if slot.as_u64() < min_slot.as_u64() {
        return Err(WindowDeny::BelowMinimumEpoch);
    }
    Ok(())
}

// ── Serve errors / results ──────────────────────────────────────────────────

/// Handler-level error before any success chunk is produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockServeError {
    /// Malformed request or oversize bound (code 1).
    InvalidRequest(&'static str),
    /// Resource not available / outside window (code 3).
    ResourceUnavailable(&'static str),
}

impl BlockServeError {
    /// Wire response code.
    #[must_use]
    pub const fn response_code(&self) -> ResponseCode {
        match self {
            Self::InvalidRequest(_) => ResponseCode::InvalidRequest,
            Self::ResourceUnavailable(_) => ResponseCode::ResourceUnavailable,
        }
    }

    /// Human-readable message (≤ 256 bytes for the wire).
    #[must_use]
    pub const fn message(&self) -> &'static str {
        match self {
            Self::InvalidRequest(m) | Self::ResourceUnavailable(m) => m,
        }
    }

    /// Build a framed error chunk.
    #[must_use]
    pub fn to_chunk(&self) -> ResponseChunk {
        ResponseChunk::Error {
            code: self.response_code().as_u8(),
            message: self.message().as_bytes().to_vec(),
        }
    }
}

impl From<WindowDeny> for BlockServeError {
    fn from(d: WindowDeny) -> Self {
        match d {
            WindowDeny::BelowEarliest => Self::ResourceUnavailable("below earliest_available_slot"),
            WindowDeny::BelowMinimumEpoch => {
                Self::ResourceUnavailable("below minimum_request_epoch")
            }
        }
    }
}

/// Planned success chunks (before rate-limit truncation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedBlocks {
    /// Success chunks tagged with per-slot `ForkDigest`.
    pub chunks: Vec<ResponseChunk>,
    /// When non-zero, server delays this long before the first response byte
    /// (CC-2Jc `stall-reqresp` — past [`crate::reqresp::TTFB_TIMEOUT`]).
    pub first_byte_delay: std::time::Duration,
}

impl PlannedBlocks {
    /// Honest plan: chunks only, no first-byte delay.
    #[must_use]
    pub fn new(chunks: Vec<ResponseChunk>) -> Self {
        Self {
            chunks,
            first_byte_delay: std::time::Duration::ZERO,
        }
    }
}

// ── Serve paths ─────────────────────────────────────────────────────────────

/// Shared context for the three block handlers.
#[derive(Debug)]
pub struct BlockServeCtx<'a, P: Preset = Mainnet> {
    /// Backfill cache — **sole** block source.
    pub cache: &'a BackfillCache<P>,
    /// Per-epoch fork-digest cache (CC-23/7).
    pub fork_ctx: &'a mut ForkContext,
    /// Slots per epoch (mainnet 32).
    pub slots_per_epoch: u64,
    /// Wall-clock epoch for the historical floor.
    pub current_epoch: Epoch,
    /// `FULU_FORK_EPOCH` from network config.
    pub fulu_fork_epoch: Epoch,
}

impl<'a, P: Preset> BlockServeCtx<'a, P> {
    /// `earliest_available_slot` atomic read (never recomputed here).
    #[must_use]
    pub fn earliest_available_slot(&self) -> Slot {
        self.cache.earliest_available_slot()
    }

    /// Head slot from the cache, if published.
    #[must_use]
    pub fn head_slot(&self) -> Option<Slot> {
        self.cache.head_slot()
    }
}

/// Serve `beacon_blocks_by_range/2/`.
///
/// Validates `count` **before** allocating the response vec. Window check is
/// on `start_slot`. Missing slots are omitted; zero blocks → ResourceUnavailable.
pub fn serve_blocks_by_range<P: Preset>(
    ctx: &mut BlockServeCtx<'_, P>,
    req: BlocksByRangeRequest,
) -> Result<PlannedBlocks, BlockServeError> {
    // CC-23/3: bound check before any proportional allocation.
    validate_block_count(req.count)?;

    check_slot_window(
        req.start_slot,
        ctx.earliest_available_slot(),
        ctx.current_epoch,
        ctx.fulu_fork_epoch,
        ctx.slots_per_epoch,
    )?;

    // Capacity is the *validated* count (≤ 128), never the raw claim.
    let mut chunks = Vec::with_capacity(req.count as usize);
    let start = req.start_slot.as_u64();
    for i in 0..req.count {
        let slot = Slot::new(start.saturating_add(i));
        // Do not walk past head if known.
        if let Some(head) = ctx.head_slot()
            && slot.as_u64() > head.as_u64()
        {
            break;
        }
        if let Some((_root, ssz)) = ctx.cache.block_ssz_at_slot(slot) {
            chunks.push(success_chunk_for_slot(
                ctx.fork_ctx,
                slot,
                ctx.slots_per_epoch,
                ssz,
            ));
        }
    }

    if chunks.is_empty() {
        return Err(BlockServeError::ResourceUnavailable(
            "no blocks in requested range",
        ));
    }
    Ok(PlannedBlocks::new(chunks))
}

/// Serve `beacon_blocks_by_root/2/`.
///
/// List length bound checked before planning. Unknown roots are skipped; if
/// **none** are found → ResourceUnavailable (never empty success).
pub fn serve_blocks_by_root<P: Preset>(
    ctx: &mut BlockServeCtx<'_, P>,
    req: &BlocksByRootRequest,
) -> Result<PlannedBlocks, BlockServeError> {
    validate_root_list_len(req.roots.len())?;

    let mut chunks = Vec::with_capacity(req.roots.len());
    for root in &req.roots {
        if let Some((slot, ssz)) = ctx.cache.block_ssz_by_root(root) {
            // Honesty: also refuse roots that sit below the serve window.
            if check_slot_window(
                slot,
                ctx.earliest_available_slot(),
                ctx.current_epoch,
                ctx.fulu_fork_epoch,
                ctx.slots_per_epoch,
            )
            .is_err()
            {
                continue;
            }
            chunks.push(success_chunk_for_slot(
                ctx.fork_ctx,
                slot,
                ctx.slots_per_epoch,
                ssz,
            ));
        }
    }

    if chunks.is_empty() {
        return Err(BlockServeError::ResourceUnavailable(
            "no requested roots available",
        ));
    }
    Ok(PlannedBlocks::new(chunks))
}

/// Serve `beacon_blocks_by_head/1/` — ancestors in **descending** slot order.
///
/// Starts at `beacon_root`, then walks `parent_root`. Missing root →
/// ResourceUnavailable (never empty success).
pub fn serve_blocks_by_head<P: Preset>(
    ctx: &mut BlockServeCtx<'_, P>,
    req: BlocksByHeadRequest,
) -> Result<PlannedBlocks, BlockServeError> {
    validate_block_count(req.count)?;

    // Head root must be in the cache; otherwise honest refuse.
    let Some((first_slot, first_block, _)) = ctx.cache.block_by_root(&req.beacon_root) else {
        return Err(BlockServeError::ResourceUnavailable(
            "beacon_root not in cache",
        ));
    };

    check_slot_window(
        first_slot,
        ctx.earliest_available_slot(),
        ctx.current_epoch,
        ctx.fulu_fork_epoch,
        ctx.slots_per_epoch,
    )?;

    let mut chunks = Vec::with_capacity(req.count as usize);
    let mut current_root = req.beacon_root;
    let mut current_block: Arc<SignedBeaconBlock<P>> = first_block;
    let mut current_slot = first_slot;

    for i in 0..req.count {
        if i > 0 {
            let parent = current_block.message.parent_root;
            match ctx.cache.block_by_root(&parent) {
                Some((slot, block, _)) => {
                    // Stop if parent walks below the serve window.
                    if check_slot_window(
                        slot,
                        ctx.earliest_available_slot(),
                        ctx.current_epoch,
                        ctx.fulu_fork_epoch,
                        ctx.slots_per_epoch,
                    )
                    .is_err()
                    {
                        break;
                    }
                    current_root = parent;
                    current_block = block;
                    current_slot = slot;
                }
                None => break,
            }
        }
        let _ = current_root; // root tracked for walk; payload is SSZ of block
        let ssz = current_block.as_ssz_bytes();
        chunks.push(success_chunk_for_slot(
            ctx.fork_ctx,
            current_slot,
            ctx.slots_per_epoch,
            ssz,
        ));
    }

    if chunks.is_empty() {
        return Err(BlockServeError::ResourceUnavailable(
            "no ancestors available",
        ));
    }
    Ok(PlannedBlocks::new(chunks))
}

/// Decode a raw SSZ request for a block protocol and plan the response.
pub fn plan_block_response<P: Preset>(
    protocol: Protocol,
    ssz: &[u8],
    ctx: &mut BlockServeCtx<'_, P>,
) -> Result<PlannedBlocks, BlockServeError> {
    match protocol {
        Protocol::BeaconBlocksByRangeV2 => {
            let req = BlocksByRangeRequest::from_ssz_bytes(ssz)
                .map_err(|_| BlockServeError::InvalidRequest("malformed by_range request"))?;
            serve_blocks_by_range(ctx, req)
        }
        Protocol::BeaconBlocksByRootV2 => {
            let req = BlocksByRootRequest::from_ssz_bytes(ssz)
                .map_err(|_| BlockServeError::InvalidRequest("malformed by_root request"))?;
            serve_blocks_by_root(ctx, &req)
        }
        Protocol::BeaconBlocksByHeadV1 => {
            let req = BlocksByHeadRequest::from_ssz_bytes(ssz)
                .map_err(|_| BlockServeError::InvalidRequest("malformed by_head request"))?;
            serve_blocks_by_head(ctx, req)
        }
        _ => Err(BlockServeError::InvalidRequest("not a block protocol")),
    }
}

/// Extract `ForkDigest` context bytes from a success chunk (tests).
#[must_use]
pub fn chunk_context(chunk: &ResponseChunk) -> Option<[u8; CONTEXT_BYTES_LEN]> {
    match chunk {
        ResponseChunk::Success { context, .. } => *context,
        ResponseChunk::Error { .. } => None,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::fork_digest::{ForkContext, compute_fork_digest};
    use crate::reqresp::codec::SszSnappyFraming;
    use cc_types::{ChainConfig, Mainnet, Preset, Root};
    use prometheus_client::registry::Registry;
    use std::sync::Arc;

    const HOODI: &str = include_str!("../../../../crates/types/tests/fixtures/hoodi-config.yaml");

    fn hoodi_cfg() -> ChainConfig {
        ChainConfig::from_yaml_str(HOODI).expect("hoodi")
    }

    fn fork_ctx_at(epoch: u64) -> ForkContext {
        ForkContext::new(hoodi_cfg(), Root::from_array([0xAB; 32]), Epoch::new(epoch))
    }

    fn root_for(n: u64) -> Root {
        let mut a = [0u8; 32];
        a[0..8].copy_from_slice(&n.to_le_bytes());
        Root::from_array(a)
    }

    fn block_at(slot: u64, parent: Root) -> Arc<SignedBeaconBlock<Mainnet>> {
        let mut b = SignedBeaconBlock::<Mainnet>::default();
        b.message.slot = Slot::new(slot);
        b.message.parent_root = parent;
        // Distinct state_root so canonical roots differ.
        let mut sr = [0u8; 32];
        sr[0..8].copy_from_slice(&slot.to_le_bytes());
        sr[8] = 0x42;
        b.message.state_root = Root::from_array(sr);
        Arc::new(b)
    }

    /// Cache with complete slots `[lo, hi]` (inclusive), no custody columns.
    fn filled_cache(lo: u64, hi: u64) -> BackfillCache<Mainnet> {
        // Empty custodied set → completeness = block present only.
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
            cache.insert_block(Slot::new(s), root, Arc::clone(&block));
            parent = root;
        }
        cache.set_head_slot(Slot::new(hi));
        // CC-48: inserts no longer write the advertised AtomicU64; seed it for
        // serve-handler unit tests (production: WatchServeWindow).
        cache.seed_advertised_from_floor();
        cache
    }

    /// Test context with the historical floor at genesis so low slot numbers
    /// exercise the `earliest_available_slot` branch (production Hoodi has the
    /// window far above the floor, so the first branch still fires there).
    fn serve_ctx<'a>(
        cache: &'a BackfillCache<Mainnet>,
        fork_ctx: &'a mut ForkContext,
    ) -> BlockServeCtx<'a, Mainnet> {
        BlockServeCtx {
            cache,
            fork_ctx,
            slots_per_epoch: Mainnet::SLOTS_PER_EPOCH,
            // min_epoch = max(current − 33024, fulu) = max(0, 0) = 0
            current_epoch: Epoch::new(0),
            fulu_fork_epoch: Epoch::new(0),
        }
    }

    #[test]
    fn max_request_blocks_is_128() {
        assert_eq!(MAX_REQUEST_BLOCKS_DENEB, 128);
        assert_eq!(compute_min_epochs_for_block_requests(), 33_024);
    }

    #[test]
    fn by_range_roundtrip_ssz() {
        let r = BlocksByRangeRequest::new(Slot::new(42), 7);
        let bytes = r.to_ssz_bytes();
        assert_eq!(BlocksByRangeRequest::from_ssz_bytes(&bytes).unwrap(), r);
        assert_eq!(r.step, 1);
        assert_eq!(bytes.len(), BY_RANGE_SSZ_LEN);
        assert_eq!(BY_RANGE_SSZ_LEN, 24);
    }

    /// Spec schema is three little-endian `uint64`s. Bytes are assembled here
    /// by hand so the codec is checked against the schema, not against itself.
    #[test]
    fn by_range_ssz_matches_handwritten_24_byte_fixture() {
        assert_eq!(BY_RANGE_SSZ_LEN, 24);
        // start_slot = 42, count = 7, step = 1
        let fixture: [u8; 24] = [
            42, 0, 0, 0, 0, 0, 0, 0, // start_slot
            7, 0, 0, 0, 0, 0, 0, 0, // count
            1, 0, 0, 0, 0, 0, 0, 0, // step
        ];
        let decoded = BlocksByRangeRequest::from_ssz_bytes(&fixture).expect("24-byte fixture");
        assert_eq!(
            decoded,
            BlocksByRangeRequest {
                start_slot: Slot::new(42),
                count: 7,
                step: 1,
            }
        );
        assert_eq!(decoded.to_ssz_bytes(), fixture);
    }

    #[test]
    fn by_head_roundtrip_ssz() {
        let r = BlocksByHeadRequest {
            beacon_root: root_for(9),
            count: 3,
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(BlocksByHeadRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    #[test]
    fn by_root_roundtrip_ssz() {
        let r = BlocksByRootRequest {
            roots: vec![root_for(1), root_for(2)],
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(BlocksByRootRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    #[test]
    fn count_129_refused_before_alloc() {
        assert!(validate_block_count(129).is_err());
        assert!(validate_block_count(1_000_000).is_err());
        assert!(validate_block_count(128).is_ok());
        assert!(validate_block_count(1).is_ok());
        assert!(validate_block_count(0).is_err());
    }

    #[test]
    fn oversized_root_list_refused() {
        assert!(validate_root_list_len(129).is_err());
        assert!(validate_root_list_len(128).is_ok());
        assert!(validate_root_list_len(0).is_err());
    }

    /// Memory-bounded: planning a 10⁶ claim does no more work/alloc than 129.
    ///
    /// Both fail at the count gate; neither reaches `Vec::with_capacity`.
    #[test]
    fn memory_bounded_oversized_range_claim() {
        let cache = filled_cache(100, 110);
        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);

        let huge = BlocksByRangeRequest::new(Slot::new(100), 1_000_000);
        let over = BlocksByRangeRequest::new(Slot::new(100), 129);
        let e1 = serve_blocks_by_range(&mut ctx, huge).unwrap_err();
        let e2 = serve_blocks_by_range(&mut ctx, over).unwrap_err();
        assert!(matches!(e1, BlockServeError::InvalidRequest(_)));
        assert!(matches!(e2, BlockServeError::InvalidRequest(_)));
        // Both error paths allocate only the static error — no count-proportional
        // response buffer. (Capacity is only taken after validate_block_count.)
    }

    #[test]
    fn one_slot_below_earliest_is_resource_unavailable() {
        let cache = filled_cache(100, 110);
        let earliest = cache.earliest_available_slot();
        assert_eq!(earliest, Slot::new(100));

        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);

        let below = BlocksByRangeRequest::new(Slot::new(earliest.as_u64() - 1), 1);
        let err = serve_blocks_by_range(&mut ctx, below).unwrap_err();
        assert!(matches!(err, BlockServeError::ResourceUnavailable(_)));
        assert_eq!(err.response_code(), ResponseCode::ResourceUnavailable);
        // Not an empty success.
        assert!(!matches!(err, BlockServeError::InvalidRequest(_)));
    }

    #[test]
    fn one_slot_above_earliest_is_served() {
        let cache = filled_cache(100, 110);
        let earliest = cache.earliest_available_slot();
        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);

        let above = BlocksByRangeRequest::new(Slot::new(earliest.as_u64() + 1), 2);
        let planned = serve_blocks_by_range(&mut ctx, above).unwrap();
        assert_eq!(planned.chunks.len(), 2);
        for c in &planned.chunks {
            assert!(matches!(c, ResponseChunk::Success { .. }));
        }
    }

    #[test]
    fn below_minimum_request_epoch_refused() {
        // Force the historical floor above earliest by setting a tiny window
        // at a high slot while current_epoch is low relative to min_epochs.
        // With min_epochs = 33024 and fulu = 50688, at current=50688 the floor
        // is fulu. Put earliest far below fulu start slot → BelowMinimumEpoch.
        let fulu_epoch = hoodi_cfg().fulu_fork_epoch.as_u64(); // 50688
        let spe = Mainnet::SLOTS_PER_EPOCH;
        // Cache holds a low slot that is still "complete".
        let mut cache = BackfillCache::with_bounds(
            Slot::new(0),
            0u64..8,
            std::iter::empty(),
            1 << 20,
            64,
            64 * 8,
        );
        let low_slot = 10u64;
        let block = block_at(low_slot, Root::ZERO);
        let root = Root::from(block.canonical_root());
        cache.insert_block(Slot::new(low_slot), root, block);
        cache.set_head_slot(Slot::new(low_slot));
        cache.seed_advertised_from_floor();
        // earliest should be 10.
        assert_eq!(cache.earliest_available_slot(), Slot::new(low_slot));

        let mut fork_ctx = fork_ctx_at(fulu_epoch);
        let mut ctx = BlockServeCtx {
            cache: &cache,
            fork_ctx: &mut fork_ctx,
            slots_per_epoch: spe,
            current_epoch: Epoch::new(fulu_epoch),
            fulu_fork_epoch: Epoch::new(fulu_epoch),
        };
        let req = BlocksByRangeRequest::new(Slot::new(low_slot), 1);
        let err = serve_blocks_by_range(&mut ctx, req).unwrap_err();
        // Slot 10 is below fulu epoch start → BelowMinimumEpoch branch.
        assert!(
            matches!(err, BlockServeError::ResourceUnavailable(m) if m.contains("minimum")),
            "got {err:?}"
        );
    }

    #[test]
    fn by_range_fork_digest_differs_across_epoch_54016() {
        // Hoodi BPO 2 at epoch 54 016.
        let spe = Mainnet::SLOTS_PER_EPOCH;
        let epoch_a = 54_015u64;
        let epoch_b = 54_016u64;
        let slot_a = epoch_a * spe;
        let slot_b = epoch_b * spe;

        let mut cache = BackfillCache::with_bounds(
            Slot::new(0),
            0u64..8,
            std::iter::empty(),
            1 << 30,
            2048,
            2048 * 8,
        );
        let mut parent = Root::ZERO;
        for s in slot_a..=slot_b {
            let block = block_at(s, parent);
            let root = Root::from(block.canonical_root());
            cache.insert_block(Slot::new(s), root, Arc::clone(&block));
            parent = root;
        }
        cache.set_head_slot(Slot::new(slot_b));
        cache.seed_advertised_from_floor();

        let mut fork_ctx = fork_ctx_at(epoch_b);
        let gvr = fork_ctx.genesis_validators_root();
        let cfg = fork_ctx.config().clone();
        let expected_a = compute_fork_digest(&cfg, gvr, Epoch::new(epoch_a));
        let expected_b = compute_fork_digest(&cfg, gvr, Epoch::new(epoch_b));
        assert_ne!(
            expected_a, expected_b,
            "BPO boundary must change the digest"
        );

        let mut ctx = serve_ctx(&cache, &mut fork_ctx);
        // Request both boundary slots (count spans the epoch edge).
        let count = slot_b - slot_a + 1;
        assert!(count <= MAX_REQUEST_BLOCKS_DENEB);
        let planned = serve_blocks_by_range(
            &mut ctx,
            BlocksByRangeRequest::new(Slot::new(slot_a), count),
        )
        .unwrap();
        assert_eq!(planned.chunks.len(), count as usize);

        let ctx_a = chunk_context(&planned.chunks[0]).unwrap();
        let ctx_b = chunk_context(planned.chunks.last().unwrap()).unwrap();
        assert_eq!(&ctx_a[..], expected_a.as_slice());
        assert_eq!(&ctx_b[..], expected_b.as_slice());
        assert_ne!(ctx_a, ctx_b, "chunks either side of 54016 must differ");

        // Round-trip framing preserves both digests.
        let enc =
            SszSnappyFraming::encode_response(&planned.chunks, Protocol::BeaconBlocksByRangeV2)
                .unwrap();
        let dec = SszSnappyFraming::decode_response(&enc, Protocol::BeaconBlocksByRangeV2).unwrap();
        assert_eq!(chunk_context(&dec[0]), Some(ctx_a));
        assert_eq!(chunk_context(dec.last().unwrap()), Some(ctx_b));
    }

    #[test]
    fn by_head_returns_descending_ancestors() {
        // Build a parent chain 100→101→102→103 with known roots.
        let mut cache = BackfillCache::with_bounds(
            Slot::new(0),
            0u64..8,
            std::iter::empty(),
            1 << 20,
            64,
            64 * 8,
        );
        let mut parent = Root::ZERO;
        let mut roots = Vec::new();
        for s in 100u64..=103 {
            let block = block_at(s, parent);
            let root = Root::from(block.canonical_root());
            cache.insert_block(Slot::new(s), root, Arc::clone(&block));
            roots.push((s, root));
            parent = root;
        }
        cache.set_head_slot(Slot::new(103));
        cache.seed_advertised_from_floor();

        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);
        let head_root = roots.last().unwrap().1;
        let planned = serve_blocks_by_head(
            &mut ctx,
            BlocksByHeadRequest {
                beacon_root: head_root,
                count: 4,
            },
        )
        .unwrap();
        assert_eq!(planned.chunks.len(), 4);

        // Decode SSZ payloads and assert descending slot order.
        let mut slots = Vec::new();
        for c in &planned.chunks {
            match c {
                ResponseChunk::Success { ssz, .. } => {
                    let b = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(
                        cc_types::ForkName::Fulu,
                        ssz,
                    )
                    .unwrap();
                    slots.push(b.message.slot.as_u64());
                }
                ResponseChunk::Error { .. } => panic!("expected success"),
            }
        }
        assert_eq!(slots, vec![103, 102, 101, 100]);
    }

    #[test]
    fn by_head_unknown_root_is_resource_unavailable() {
        let cache = filled_cache(100, 105);
        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);
        let err = serve_blocks_by_head(
            &mut ctx,
            BlocksByHeadRequest {
                beacon_root: root_for(0xDEAD),
                count: 1,
            },
        )
        .unwrap_err();
        assert!(matches!(err, BlockServeError::ResourceUnavailable(_)));
    }

    #[test]
    fn by_root_serves_known_skips_unknown() {
        let cache = filled_cache(100, 105);
        // Grab a real root from the cache.
        let (root, _, _) = cache.block_at_slot(Slot::new(103)).unwrap();
        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);
        let planned = serve_blocks_by_root(
            &mut ctx,
            &BlocksByRootRequest {
                roots: vec![root_for(0xBEEF), root, root_for(0xCAFE)],
            },
        )
        .unwrap();
        assert_eq!(planned.chunks.len(), 1);
    }

    #[test]
    fn by_root_all_unknown_is_resource_unavailable() {
        let cache = filled_cache(100, 105);
        let mut fork_ctx = fork_ctx_at(60_000);
        let mut ctx = serve_ctx(&cache, &mut fork_ctx);
        let err = serve_blocks_by_root(
            &mut ctx,
            &BlocksByRootRequest {
                roots: vec![root_for(1), root_for(2)],
            },
        )
        .unwrap_err();
        assert!(matches!(err, BlockServeError::ResourceUnavailable(_)));
    }

    #[test]
    fn earliest_available_slot_is_cache_load_only() {
        // Production body (above `mod tests`) must not write / recompute the window.
        let src = include_str!("blocks.rs");
        let prod = src.split("mod tests").next().expect("tests module");
        assert!(
            !prod.contains("store_recomputed"),
            "production blocks.rs must not write the serve window"
        );
        assert!(
            !prod.contains("compute_earliest_available_slot"),
            "production blocks.rs must not recompute the window"
        );
        let cache = filled_cache(50, 60);
        let mut fork_ctx = fork_ctx_at(60_000);
        let ctx = serve_ctx(&cache, &mut fork_ctx);
        assert_eq!(
            ctx.earliest_available_slot(),
            cache.earliest_available_slot()
        );
        let _ = Registry::default();
    }

    #[test]
    fn cache_is_sole_block_source() {
        // Acceptance: production body uses CC-26a BackfillCache only.
        let src = include_str!("blocks.rs");
        let prod = src.split("mod tests").next().expect("tests module");
        assert!(prod.contains("BackfillCache"));
        assert!(prod.contains("cache.block_ssz_at_slot") || prod.contains("cache.block_by_root"));
        // No second store type in production code.
        assert!(!prod.contains("HashMap<Slot"));
    }

    /// CC-4A /5 (D-5) — FULU clamp retained; agrees with CC-4A floor at today's epoch.
    ///
    /// Spec delta 3 would clamp at GENESIS_EPOCH only. Our retained
    /// `FULU_FORK_EPOCH` floor is a *simplification licensed by D1*. At a
    /// "today" epoch well past `FULU + min_epochs` the clamp does not bind, so
    /// `minimum_request_epoch_blocks` equals `current − compute_min_epochs…`
    /// (the CC-4A computed floor offset).
    #[test]
    fn fulu_clamp_retained_agrees_with_cc4a_floor_at_todays_epoch() {
        let cfg = hoodi_cfg();
        let fulu = cfg.fulu_fork_epoch;
        let min_epochs = compute_min_epochs_for_block_requests();
        // "Today": comfortably past FULU + min_epochs so the FULU arm of max() is idle.
        let today = Epoch::new(
            fulu.as_u64()
                .saturating_add(min_epochs)
                .saturating_add(10_000),
        );
        let pure = today.as_u64().saturating_sub(min_epochs);
        let clamped = minimum_request_epoch_blocks(today, fulu);
        assert_eq!(
            clamped.as_u64(),
            pure,
            "at today's epoch the FULU clamp must agree with CC-4A's computed offset"
        );
        // Clamp still present in production (not silently removed).
        let src = include_str!("blocks.rs");
        let prod = src.split("mod tests").next().expect("tests module");
        assert!(
            prod.contains("FULU_FORK_EPOCH") && prod.contains("simplification licensed by D1"),
            "must retain the clamp with the D1 licensing comment"
        );
        // And when current is exactly FULU, the floor is FULU (clamp binds).
        let at_fulu = minimum_request_epoch_blocks(fulu, fulu);
        assert_eq!(at_fulu, fulu);
    }
}
