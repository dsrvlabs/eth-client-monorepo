//! P2P-side chain stream client (CC-27b / Architecture §10.4–10.6, §2.3).
//!
//! - [`client`]: dials `chain`, reconnects with bounded exponential backoff,
//!   tracks `outstanding`, re-sends on reconnect, resolves verdict timeouts as
//!   local IGNORE.
//! - [`view`]: single-writer [`ArcSwap`] of the latest [`ChainView`].
//! - [`publish`]: outward `PublishRequest` → bounded publish queue (oldest-
//!   dropped, counted).
//!
//! Stall-then-shed lives in the swarm task (`host.rs`); this module owns the
//! stream edge and the no-lost-verdict map.

pub mod client;
pub mod publish;
pub mod records;
pub mod view;

pub use client::{
    ChainStreamConfig, ChainStreamHandle, OutstandingEntry, OutstandingMap, new_session_id,
    run_chain_stream_client, stall_max_from_heartbeat, wait_reconnect_backoff,
};
pub use crate::channels::VerdictResolution;
pub use publish::{PublishDropCounter, run_publish_dispatch};
pub use records::{
    check_record_request_bound, MapValidatorRecordSource, RecordsError, RpcValidatorRecordSource,
    ValidatorRecordCache, ValidatorRecordSource, FetchedRecords, MAX_VALIDATOR_RECORDS_PER_REQUEST,
    VALIDATOR_RECORD_CACHE_BOUND,
};
pub use view::{ChainViewStore, VIEW_KIND_EPOCH_TICK, VIEW_KIND_FULL, VIEW_KIND_HEAD_CHANGE, VIEW_KIND_SLOT_TICK};

use std::time::Duration;

use crate::channels::CHAIN_OUT_BOUND;

/// Cap for `outstanding` — same as the outbound channel bound (Architecture §10.4).
pub const OUTSTANDING_CAP: usize = CHAIN_OUT_BOUND;

/// Local IGNORE when chain does not answer within this window (§10.4).
pub const DEFAULT_VERDICT_TIMEOUT: Duration = Duration::from_secs(2);

/// Latency budget after which a verdict is counted late (CC-27/5 metric half).
///
/// The p95 criterion is asserted in CC-27c; this issue only exports the counter.
pub const DEFAULT_VERDICT_LATE_AFTER: Duration = Duration::from_millis(100);

/// Reconnect backoff initial delay (§10.6).
pub const BACKOFF_INITIAL: Duration = Duration::from_millis(250);

/// Reconnect backoff hard cap (§10.6).
pub const BACKOFF_CAP: Duration = Duration::from_secs(10);

/// Stall bound multiplier: `stall_max = heartbeat_interval × STALL_HEARTBEAT_FRACTION`.
///
/// **Not** an inlined millisecond stall bound — derived from the configured
/// heartbeat (R-6 / CC-27/3). Never hard-code a stall duration here.
pub const STALL_HEARTBEAT_FRACTION: f64 = 0.5;
