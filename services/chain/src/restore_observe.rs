//! S0-A-31 observation surface. Compiled only with `feature = "s0-a-31-observe"`.
//!
//! Production restore (`apply_restore_set` without this feature) never names
//! [`BeaconState::from_ssz_bytes_with`] and never records the in-process trace.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use cc_types::BeaconState;
use cc_types::fork::ForkName;
use cc_types::preset::Preset;
use ssz::DecodeError;
use tracing::info;

/// One site the S0-A-31 restore observation can hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreTracePoint {
    /// Snapshot `BeaconState` SSZ decode (`apply_restore_set`).
    Decode {
        /// `false` when [`restore_force_raw_decode`] omitted the top-up.
        hydrated: bool,
        pubkey_cache_len: usize,
        validators_len: usize,
    },
    /// One `on_block` in the replay loop.
    OnBlock {
        index: usize,
        slot: u64,
        da_status: i32,
        outcome: String,
    },
    /// Stored-`DEFERRED` block accepted and left unmarked (P1-A/23 site).
    DaDeferredDrop { index: usize, root: String },
    /// [`super::RestoreGate::end_stream`] (P1-A/22 site).
    EndStream,
}

impl std::fmt::Display for RestoreTracePoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode {
                hydrated,
                pubkey_cache_len,
                validators_len,
            } => write!(
                f,
                "decode hydrated={hydrated} pubkey_cache={pubkey_cache_len} validators={validators_len}"
            ),
            Self::OnBlock {
                index,
                slot,
                da_status,
                outcome,
            } => write!(
                f,
                "on_block[{index}] slot={slot} da={da_status} outcome={outcome}"
            ),
            Self::DaDeferredDrop { index, root } => {
                write!(f, "da_deferred_drop[{index}] root={root}")
            }
            Self::EndStream => write!(f, "end_stream"),
        }
    }
}

static RESTORE_TRACE: Mutex<Vec<RestoreTracePoint>> = Mutex::new(Vec::new());
static RESTORE_FORCE_RAW_DECODE: AtomicBool = AtomicBool::new(false);

pub(super) fn record_restore_trace(point: RestoreTracePoint) {
    info!(target: "s0_a_31", %point, "restore observation");
    RESTORE_TRACE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(point);
}

/// Clear the S0-A-31 trace (observation runs).
pub fn reset_restore_trace() {
    RESTORE_TRACE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
}

/// Snapshot of sites reached since the last [`reset_restore_trace`].
#[must_use]
pub fn restore_trace() -> Vec<RestoreTracePoint> {
    RESTORE_TRACE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Omit pubkey-cache hydration on the next restore decode.
///
/// Exists only under `s0-a-31-observe`. The production binary does not compile
/// this symbol.
pub fn restore_force_raw_decode(raw: bool) {
    RESTORE_FORCE_RAW_DECODE.store(raw, Ordering::SeqCst);
}

pub(super) fn raw_decode_enabled() -> bool {
    RESTORE_FORCE_RAW_DECODE.load(Ordering::SeqCst)
}

/// Decode the restore snapshot, optionally skipping the S0-A-02 top-up.
pub(super) fn decode_snapshot<P: Preset>(bytes: &[u8]) -> Result<BeaconState<P>, DecodeError> {
    if raw_decode_enabled() {
        BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, bytes)
    } else {
        BeaconState::<P>::from_ssz_bytes_hydrated(ForkName::Fulu, bytes)
    }
}
