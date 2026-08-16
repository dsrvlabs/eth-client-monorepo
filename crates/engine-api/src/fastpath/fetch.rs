//! Fast-path fetch orchestration (CC-37a / Architecture §5.3).
//!
//! Owns:
//! - runtime `max_blobs_per_block` gate via CC-1G [`BlobParameters`]
//! - the 128-hash EL bound asserted **unreachable** (one request, no re-shape)
//! - null-cause classification (three named arms, no catch-all)
//! - observation of `cc_engine_fastpath_seconds{stage="fetch"}` and
//!   `cc_engine_getblobs_total`
//!
//! On Complete, the lane worker composes CC-37b reconstruction (cells →
//! transpose → filter). This module stays wire-only; inject is **CC-38**.

use std::sync::Arc;

use cc_types::config::{BlobParameters, BlobSchedule};
use cc_types::preset::{Mainnet, Preset};
use cc_types::primitives::Epoch;
use cc_types::sidecar::DataColumnSidecar;

use crate::errors::EngineError;
use crate::methods::get_blobs::{
    GET_BLOBS_V2_MAX_HASHES, GetBlobsOutcome, NullCause, NullContext, get_blobs_v2,
};
use crate::metrics::EngineMetrics;
use crate::transport::EngineTransport;

/// Sampling-tracker interaction surface used to prove a miss never touches DA.
///
/// Production injection is CC-38; this trait exists so CC-37a tests can assert
/// the sampling path is **untouched** on `null` / miss.
pub trait SamplingTrackerProbe: Send + Sync {
    /// Record that the tracker was consulted or mutated.
    fn note_interaction(&self);
    /// Number of interactions (tests).
    fn interactions(&self) -> u64;
}

/// No-op probe (production placeholder until CC-38 wires the real tracker).
#[derive(Debug, Default)]
pub struct NullSamplingTracker;

impl SamplingTrackerProbe for NullSamplingTracker {
    fn note_interaction(&self) {}
    fn interactions(&self) -> u64 {
        0
    }
}

/// In-memory probe for unit tests.
#[derive(Debug, Default)]
pub struct CountingSamplingTracker {
    hits: std::sync::atomic::AtomicU64,
}

impl SamplingTrackerProbe for CountingSamplingTracker {
    fn note_interaction(&self) {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn interactions(&self) -> u64 {
        self.hits.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Blob-bound provider: CC-1G `get_blob_parameters(epoch)` over a schedule.
///
/// Pre-schedule Electra max comes from the constructor (config
/// `max_blobs_per_block_electra`). Schedule entries come from config
/// (production) or test fixtures constructed outside this module so
/// blob-count maxima never appear as literals here.
#[derive(Debug, Clone)]
pub struct BlobBound {
    schedule: BlobSchedule,
    /// Electra fork epoch used as the pre-schedule fallback base epoch.
    electra_fork_epoch: Epoch,
    /// `MAX_BLOBS_PER_BLOCK_ELECTRA` from the loaded chain config.
    max_blobs_per_block_electra: u64,
}

impl BlobBound {
    /// Construct from a validated schedule + Electra base epoch + Electra max.
    #[must_use]
    pub fn new(
        schedule: BlobSchedule,
        electra_fork_epoch: Epoch,
        max_blobs_per_block_electra: u64,
    ) -> Self {
        Self {
            schedule,
            electra_fork_epoch,
            max_blobs_per_block_electra,
        }
    }

    /// CC-1G `get_blob_parameters(epoch)`.
    #[must_use]
    pub fn get_blob_parameters(&self, epoch: Epoch) -> BlobParameters {
        self.schedule.get_blob_parameters(
            epoch,
            self.electra_fork_epoch,
            self.max_blobs_per_block_electra,
        )
    }
}

/// One fetch request after single-flight admission.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    /// Beacon block root (single-flight key; already reserved by the lane).
    pub beacon_block_root: [u8; 32],
    /// Slot of the block / column (for epoch → blob bound).
    pub slot: u64,
    /// Versioned hashes from `blob_kzg_commitments` (never empty — zero-commit
    /// blocks are filtered at the trigger).
    pub versioned_hashes: Vec<[u8; 32]>,
    /// Context for null classification (pre-Osaka vs pool miss vs partial).
    pub null_ctx: NullContext,
}

/// Result of a fetch attempt (miss is success-shaped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchResult {
    /// Wire hit only (lane has no KZG backend — tests / fetch-only mode).
    Complete(GetBlobsOutcome),
    /// Wire hit + bind + `compute_cells` + transpose + subscribe filter
    /// (CC-37b live path). Inject of `published` is **CC-38**.
    Assembled {
        n_blobs: usize,
        published: Vec<DataColumnSidecar<Mainnet>>,
        dropped: usize,
        published_bytes: usize,
    },
    Miss {
        cause: NullCause,
    },
    /// Transport / timeout / hard decode / reconstruction failure.
    Error(String),
    /// Request rejected before the wire (bound / empty) — no EL call.
    Skipped {
        reason: &'static str,
    },
}

/// Assert the request length is within the runtime blob bound and the EL 128
/// ceiling. Bound overflow is a programming error (CC-37 /8): production callers
/// gate on CC-1G `max_blobs_per_block`, so `-38004` is unreachable. We refuse
/// the call rather than re-shape the request into multiple smaller ones.
pub fn assert_request_length_within_bound(
    versioned_hashes: &[[u8; 32]],
    bound: &BlobBound,
    epoch: Epoch,
) -> Result<BlobParameters, &'static str> {
    if versioned_hashes.is_empty() {
        return Err("empty versioned_hashes");
    }
    let params = bound.get_blob_parameters(epoch);
    // Soft gate first so unit tests can exercise the overflow path without
    // panicking under `debug_assert` (debug builds enable it).
    if (versioned_hashes.len() as u64) > params.max_blobs_per_block {
        return Err("exceeds runtime max_blobs_per_block");
    }
    if versioned_hashes.len() > GET_BLOBS_V2_MAX_HASHES {
        return Err("exceeds EL 128-hash ceiling");
    }
    // CC-37 /8: once past the soft gate, both bounds hold — `-38004` unreachable.
    debug_assert!((versioned_hashes.len() as u64) <= params.max_blobs_per_block);
    debug_assert!(versioned_hashes.len() <= GET_BLOBS_V2_MAX_HASHES);
    Ok(params)
}

/// Compute the epoch for a slot under Mainnet `SLOTS_PER_EPOCH`.
#[must_use]
pub fn epoch_at_slot(slot: u64) -> Epoch {
    Epoch::new(slot / Mainnet::SLOTS_PER_EPOCH)
}

/// Run one `getBlobsV2` fetch: bound check → fastpath call → null-cause match.
///
/// On miss, the sampling tracker is **not** touched. On complete, the **lane
/// worker** composes CC-37b reconstruction (bind → cells → transpose → filter);
/// this function returns the wire outcome only.
pub async fn fetch_blobs(
    transport: &EngineTransport,
    metrics: Option<&EngineMetrics>,
    bound: &BlobBound,
    tracker: Option<&dyn SamplingTrackerProbe>,
    request: &FetchRequest,
) -> FetchResult {
    let epoch = epoch_at_slot(request.slot);
    let params = match assert_request_length_within_bound(&request.versioned_hashes, bound, epoch) {
        Ok(p) => p,
        Err("empty versioned_hashes") => {
            return FetchResult::Skipped {
                reason: "empty versioned_hashes",
            };
        }
        Err(reason) => {
            tracing::warn!(
                reason,
                n = request.versioned_hashes.len(),
                epoch = epoch.as_u64(),
                "getBlobsV2 request rejected by bound gate"
            );
            return FetchResult::Skipped { reason };
        }
    };
    // Silence unused in release when debug_assert is stripped — params used for logs.
    let _ = params;

    match get_blobs_v2(
        transport,
        metrics,
        &request.versioned_hashes,
        request.null_ctx,
    )
    .await
    {
        Ok(GetBlobsOutcome::Complete(blobs)) => {
            // Worker composes reconstruction when a KZG backend is configured.
            // Tracker interaction stays closed here (CC-38 inject owns DA).
            let _ = tracker;
            FetchResult::Complete(GetBlobsOutcome::Complete(blobs))
        }
        Ok(GetBlobsOutcome::Miss(cause)) => {
            // Exhaustive three-way match on NullCause (CC-37a: no wildcard arm).
            match cause {
                NullCause::PartialHit => {
                    // Miss is not an engine fault; sampling path untouched.
                    FetchResult::Miss {
                        cause: NullCause::PartialHit,
                    }
                }
                NullCause::PrunedPool => FetchResult::Miss {
                    cause: NullCause::PrunedPool,
                },
                NullCause::ElHeadPreOsaka => FetchResult::Miss {
                    cause: NullCause::ElHeadPreOsaka,
                },
            }
        }
        Err(e) => {
            // -38004 is asserted unreachable; if it appears, surface as error.
            if matches!(e, EngineError::TooLargeRequest { .. }) {
                debug_assert!(
                    false,
                    "-38004 TooLargeRequest is unreachable when gated by max_blobs_per_block"
                );
            }
            FetchResult::Error(e.to_string())
        }
    }
}

/// Shared handle pieces for the worker.
pub type SharedBlobBound = Arc<BlobBound>;
