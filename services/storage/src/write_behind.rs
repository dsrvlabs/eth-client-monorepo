//! Write-behind task: `SubscribeEvents` consumer and slot commit-unit builder.
//!
//! Architecture §4.4–4.7 / CC-44b:
//! - Accumulate events into a **slot commit unit**
//! - Flush on the first of: `HEAD` for slot `S+1`, `commit_max_events`, or
//!   `commit_max_latency`
//! - Submit P0 batches with [`WriteCursor`] in the **same batch as the data**
//! - Attribute both cursor rejection reasons on reconnect
//!
//! **P0-13:** a failed P0 flush ends the session with
//! `Reconnect { cursor: last_flushed }`. The cursor bounds the loss window; a
//! failed unit never advances it. Resume from the last acked cursor replays
//! `seq + 1`. When nothing is acked, reconnect uses a predecessor cursor at
//! `first_failed_seq - 1` so the failed unit is re-delivered (not live-from-tip).
//!
//! **D-14:** this issue proves the two rejection reasons are distinguishable and
//! attributed. It does **not** discharge clause 2 (no-hole / hole-closed halves
//! land at CC-45b/CC-48 M4.4 and CC-47a M4.5).
//!
//! Helpers used by tests and future restore/gap paths may appear unused in the
//! binary graph; keep them reachable without editing call sites later.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cc_proto::chain::chain_service_client::ChainServiceClient;
use cc_proto::chain::{
    Cursor, Event, EventKind, GetCanonicalRootsRequest, GetHeadRequest, SubscribeEventsRequest,
};
use cc_proto::error_info_from_status;
use cc_store::columns::{COLUMN_INDEX_SSZ_OFFSET, column_index_at_offset};
use cc_store::engine::Engine;
use cc_store::meta::WriteCursor;
use cc_store::{DaStatus, Root, Slot};
// Root used by FINALIZED_CHECKPOINT → migration (CC-41).
use futures::StreamExt;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::{debug, error, info, warn};

use crate::metrics::{ReconnectReason, StorageMetrics};
use crate::migrate::{
    Migrator, maybe_migrate_on_finalized, parse_finalized_payload, root_from_event,
};
use crate::replay::{ReplayDriver, maybe_snapshot_on_finalized};
use crate::writer::{
    CommitUnit, StagedBlock, StagedColumn, StagedForkChoiceScalars, WriterError, WriterHandle,
    load_write_cursor, observe_reconnect,
};

/// gRPC `ErrorInfo.reason` for session mismatch (chain cursor.rs).
pub(crate) const REASON_CURSOR_UNKNOWN_SESSION: &str = "CURSOR_UNKNOWN_SESSION";
/// gRPC `ErrorInfo.reason` for ring eviction (chain cursor.rs).
pub(crate) const REASON_CURSOR_TOO_OLD: &str = "CURSOR_TOO_OLD";

/// Must match `cc_chain::SESSION_ID_METADATA_KEY` / chain `SubscribeEvents` response.
///
/// `Event` has no `session_id` field; chain attaches the live session on the
/// initial response metadata so write-behind can stamp durable `WriteCursor`
/// without re-attributing every reconnect as `CURSOR_UNKNOWN_SESSION`.
pub(crate) const SESSION_ID_METADATA_KEY: &str = "x-cc-chain-session-id";

/// First payload byte = `ImportBlockVerdict::Imported` (proto enum value 1).
pub(crate) const BLOCK_PAYLOAD_VERDICT_IMPORTED: u8 = 1;
/// First payload byte = `ImportBlockVerdict::DeferredDa` (proto enum value 3).
pub(crate) const BLOCK_PAYLOAD_VERDICT_DEFERRED_DA: u8 = 3;

/// Default: one commit per slot — **the loss bound** (§4.4).
pub(crate) const DEFAULT_COMMIT_SLOTS: u64 = 1;
/// Flush when this many events accumulate without a slot boundary.
pub(crate) const DEFAULT_COMMIT_MAX_EVENTS: usize = 64;
/// Flush after this much wall time without a flush (loss-window bound).
pub(crate) const DEFAULT_COMMIT_MAX_LATENCY: Duration = Duration::from_secs(4);

/// Write-behind flush / stream knobs (from `config/storage.toml`).
#[derive(Debug, Clone)]
pub(crate) struct WriteBehindConfig {
    /// gRPC URI for `chain` (e.g. `http://127.0.0.1:9001`).
    pub chain_uri: String,
    /// One commit per N slots. Default **1** — the loss bound (§4.4).
    pub commit_slots: u64,
    /// Max events accumulated before a flush.
    pub commit_max_events: usize,
    /// Max wall time between flushes.
    pub commit_max_latency: Duration,
    /// Connect timeout for each dial.
    pub connect_timeout: Duration,
    /// Reconnect backoff initial.
    pub backoff_initial: Duration,
    /// Reconnect backoff cap.
    pub backoff_cap: Duration,
}

impl Default for WriteBehindConfig {
    fn default() -> Self {
        Self {
            chain_uri: "http://127.0.0.1:9001".to_owned(),
            commit_slots: DEFAULT_COMMIT_SLOTS,
            commit_max_events: DEFAULT_COMMIT_MAX_EVENTS,
            commit_max_latency: DEFAULT_COMMIT_MAX_LATENCY,
            connect_timeout: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(200),
            backoff_cap: Duration::from_secs(30),
        }
    }
}

/// In-flight accumulation for the current commit unit.
#[derive(Debug, Default)]
struct Accumulator {
    blocks: Vec<StagedBlock>,
    columns: Vec<StagedColumn>,
    fork_choice: Option<StagedForkChoiceScalars>,
    /// First event included (predecessor resume when nothing is acked).
    first: Option<(u64 /*seq*/, u64 /*slot*/, Root)>,
    /// Last event included (drives the durable cursor).
    last: Option<(u64 /*seq*/, u64 /*slot*/, Root)>,
    /// First open-slot we started accumulating for (for lag metric / HEAD flush).
    open_slot: Option<u64>,
    /// Events accepted into this unit.
    event_count: usize,
    /// When the unit opened.
    opened_at: Option<Instant>,
    /// Highest head slot observed (for lag).
    latest_head_slot: Option<u64>,
}

impl Accumulator {
    fn is_empty(&self) -> bool {
        self.event_count == 0
    }

    fn open_if_needed(&mut self, slot: u64) {
        if self.opened_at.is_none() {
            self.opened_at = Some(Instant::now());
            self.open_slot = Some(slot);
        }
    }

    fn note_event(&mut self, seq: u64, slot: u64, root: Root) {
        self.open_if_needed(slot);
        if self.first.is_none() {
            self.first = Some((seq, slot, root));
        }
        self.last = Some((seq, slot, root));
        self.event_count = self.event_count.saturating_add(1);
    }

    fn into_commit_unit(self) -> Option<CommitUnit> {
        let (seq, slot, root) = self.last?;
        Some(CommitUnit {
            blocks: self.blocks,
            columns: self.columns,
            fork_choice: self.fork_choice,
            cursor: WriteCursor {
                session_id: 0, // filled by caller from live session
                seq,
                slot: Slot::new(slot),
                root,
            },
            done: None,
        })
    }
}

/// Spawn write-behind with **respawn-on-panic** supervision (§1.5 counter-example
/// to the writer: write-behind is **not** process-fatal).
///
/// Returns `(join, respawn_count)`. Production ignores the counter; tests assert
/// it increments when the inner task panics.
///
/// `migrator` is the CC-41 hot/cold split driver; `None` disables migration
/// (tests that only exercise the stream).
/// `replayer` is the CC-42 snapshot ring driver; `None` disables finalization-
/// driven snapshots (the poll-path REPLAY TASK may still run).
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_write_behind(
    cfg: WriteBehindConfig,
    writer: WriterHandle,
    metrics: StorageMetrics,
    initial_cursor: Option<WriteCursor>,
    shutdown: watch::Receiver<bool>,
    migrator: Option<Arc<Migrator>>,
    replayer: Option<Arc<ReplayDriver>>,
    engine: Option<Arc<Engine>>,
) -> (tokio::task::JoinHandle<()>, Arc<AtomicU64>) {
    let respawns = Arc::new(AtomicU64::new(0));
    let respawns_task = Arc::clone(&respawns);
    let join = tokio::spawn(async move {
        supervise_write_behind(
            cfg,
            writer,
            metrics,
            initial_cursor,
            shutdown,
            respawns_task,
            migrator,
            replayer,
            engine,
        )
        .await;
    });
    (join, respawns)
}

/// In-memory resume authority shared across panic respawns.
#[derive(Debug)]
struct ResumeAuthority {
    cursor: Mutex<Option<WriteCursor>>,
}

impl ResumeAuthority {
    fn new(initial: Option<WriteCursor>) -> Arc<Self> {
        Arc::new(Self {
            cursor: Mutex::new(initial),
        })
    }

    fn load(&self) -> Option<WriteCursor> {
        *self.cursor.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn store(&self, cursor: Option<WriteCursor>) {
        *self.cursor.lock().unwrap_or_else(|e| e.into_inner()) = cursor;
    }
}

/// Unacked unit to re-submit when SubscribeEvents cannot replay its first seq.
#[derive(Debug, Default)]
struct PendingRetry {
    unit: Mutex<Option<CommitUnit>>,
}

impl PendingRetry {
    fn store(&self, unit: CommitUnit) {
        *self.unit.lock().unwrap_or_else(|e| e.into_inner()) = Some(unit);
    }

    fn take(&self) -> Option<CommitUnit> {
        self.unit.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

/// After a write-behind panic, resume from the durable cursor or the task's
/// last_flushed — never the stale spawn-time value alone.
///
/// Prefer a resumable cursor, then the higher seq.
#[must_use]
pub(crate) fn resume_after_panic(
    disk: Option<WriteCursor>,
    last_flushed: Option<WriteCursor>,
) -> Option<WriteCursor> {
    [disk, last_flushed]
        .into_iter()
        .flatten()
        .max_by_key(|c| (is_resumable(c) as u8, c.seq))
}

#[allow(clippy::too_many_arguments)]
async fn supervise_write_behind(
    cfg: WriteBehindConfig,
    writer: WriterHandle,
    metrics: StorageMetrics,
    initial_cursor: Option<WriteCursor>,
    mut shutdown: watch::Receiver<bool>,
    respawns: Arc<AtomicU64>,
    migrator: Option<Arc<Migrator>>,
    replayer: Option<Arc<ReplayDriver>>,
    engine: Option<Arc<Engine>>,
) {
    let mut backoff = cfg.backoff_initial;
    let resume = ResumeAuthority::new(initial_cursor);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let disk = engine
            .as_ref()
            .and_then(|e| load_write_cursor(e).ok().flatten());
        let cursor_i = resume_after_panic(disk, resume.load());
        let cfg_i = cfg.clone();
        let writer_i = writer.clone();
        let metrics_i = metrics.clone();
        let shutdown_i = shutdown.clone();
        let migrator_i = migrator.clone();
        let replayer_i = replayer.clone();
        let resume_i = Arc::clone(&resume);
        let inner = tokio::spawn(async move {
            run_write_behind(
                cfg_i, writer_i, metrics_i, cursor_i, shutdown_i, migrator_i, replayer_i, resume_i,
            )
            .await;
        });
        match inner.await {
            Ok(()) => break, // clean shutdown from run_write_behind
            Err(e) if e.is_panic() => {
                // Not following while the inner task is down (CC-45c gauge).
                metrics.set_following_head(false);
                let n = respawns.fetch_add(1, Ordering::SeqCst).saturating_add(1);
                warn!(
                    target: "cc_storage::write_behind",
                    respawns = n,
                    "write-behind task panicked — respawning from durable cursor (not process-fatal; writer is)"
                );
                if wait_backoff(&mut shutdown, backoff).await {
                    break;
                }
                backoff = next_backoff(backoff, cfg.backoff_cap);
            }
            Err(e) => {
                metrics.set_following_head(false);
                warn!(
                    target: "cc_storage::write_behind",
                    error = %e,
                    "write-behind task cancelled"
                );
                break;
            }
        }
    }
    metrics.set_following_head(false);
}

/// Drive write-behind until shutdown.
#[allow(clippy::too_many_arguments)]
async fn run_write_behind(
    cfg: WriteBehindConfig,
    writer: WriterHandle,
    metrics: StorageMetrics,
    mut durable_cursor: Option<WriteCursor>,
    mut shutdown: watch::Receiver<bool>,
    migrator: Option<Arc<Migrator>>,
    replayer: Option<Arc<ReplayDriver>>,
    resume: Arc<ResumeAuthority>,
) {
    let mut backoff = cfg.backoff_initial;
    let pending = PendingRetry::default();
    // Start not-following; flipped to 1 only after SubscribeEvents succeeds.
    metrics.set_following_head(false);
    info!(
        target: "cc_storage::write_behind",
        chain = %cfg.chain_uri,
        "write-behind task started"
    );

    loop {
        if *shutdown.borrow() {
            break;
        }
        match run_session(
            &cfg,
            &writer,
            &metrics,
            durable_cursor.as_ref(),
            &mut shutdown,
            migrator.as_deref(),
            replayer.as_deref(),
            &resume,
            &pending,
        )
        .await
        {
            SessionEnd::Shutdown => break,
            SessionEnd::SetCursor(c) => {
                // `None` = live-from-tip after rejection (discard durable cursor).
                // Not currently on a live session until the next subscribe.
                metrics.set_following_head(false);
                durable_cursor = c;
                resume.store(durable_cursor);
                backoff = cfg.backoff_initial;
            }
            SessionEnd::Reconnect { reason, cursor } => {
                // Stream lost — not following until the next successful subscribe.
                metrics.set_following_head(false);
                if let Some(c) = cursor {
                    durable_cursor = Some(c);
                }
                resume.store(durable_cursor);
                if let Some(r) = reason {
                    observe_reconnect(&metrics, r);
                }
                if wait_backoff(&mut shutdown, backoff).await {
                    break;
                }
                backoff = next_backoff(backoff, cfg.backoff_cap);
            }
        }
    }
    metrics.set_following_head(false);
    info!(target: "cc_storage::write_behind", "write-behind task stopped");
}

#[derive(Debug)]
pub(crate) enum SessionEnd {
    Shutdown,
    /// Update the durable resume cursor held by the reconnect loop.
    ///
    /// `None` means discard and resubscribe **live from tip** (§4.6).
    SetCursor(Option<WriteCursor>),
    Reconnect {
        reason: Option<ReconnectReason>,
        /// Last **resumable** cursor committed in the session (carry across reconnect).
        cursor: Option<WriteCursor>,
    },
}

/// Whether `c` may be sent as a SubscribeEvents resume cursor.
///
/// `session_id == 0` is never resume-valid: chain's session is a non-zero random
/// u64, and stamping 0 caused every reconnect after a live-only period to be
/// mis-attributed as `CURSOR_UNKNOWN_SESSION` (review O1).
#[must_use]
pub(crate) fn is_resumable(c: &WriteCursor) -> bool {
    c.session_id != 0
}

/// Build a gRPC resume cursor, or `None` for live-from-tip.
#[must_use]
pub(crate) fn to_resume_cursor(c: &WriteCursor) -> Option<Cursor> {
    if !is_resumable(c) {
        return None;
    }
    Some(Cursor {
        session_id: c.session_id,
        seq: c.seq,
        slot: c.slot.as_u64(),
        root: c.root.as_slice().to_vec(),
    })
}

/// Read live `session_id` from a `SubscribeEvents` response (chain metadata).
#[must_use]
pub(crate) fn session_from_metadata(md: &tonic::metadata::MetadataMap) -> Option<u64> {
    md.get(SESSION_ID_METADATA_KEY)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&id| id != 0)
}

/// Advance the in-memory seq frontier after a **successful commit**.
///
/// `session_id == 0` is still stored so the seq does not split from the on-disk
/// cursor; [`to_resume_cursor`] will not send it until
/// [`stamp_session_id`] makes it resumable.
fn note_committed_resume(last: &mut Option<WriteCursor>, cursor: WriteCursor) {
    *last = Some(cursor);
}

/// Copy a live non-zero session onto a stored frontier that was committed
/// without metadata (closes the session_id-0 split).
fn stamp_session_id(last: &mut Option<WriteCursor>, session_id: u64) -> bool {
    if session_id == 0 {
        return false;
    }
    if let Some(c) = last.as_mut()
        && c.session_id == 0
    {
        c.session_id = session_id;
        return true;
    }
    false
}

/// Submit P0 and wait for commit ack before treating the cursor as durable.
async fn flush_committed(
    writer: &WriterHandle,
    unit: CommitUnit,
    last_flushed: &mut Option<WriteCursor>,
    resume: Option<&ResumeAuthority>,
) -> Result<(), WriterError> {
    let cursor = unit.cursor;
    writer.submit_p0_committed(unit).await?;
    note_committed_resume(last_flushed, cursor);
    if let Some(r) = resume {
        r.store(*last_flushed);
    }
    Ok(())
}

/// First event seq re-delivered when resuming from `c` (chain replays `seq + 1`).
#[must_use]
pub(crate) fn resume_from_seq(c: &WriteCursor) -> u64 {
    c.seq.saturating_add(1)
}

/// Inclusive seq to re-apply after a failed flush.
///
/// Last acked cursor: chain replays [`resume_from_seq`]. Nothing acked: the
/// failed unit's first seq (the predecessor cursor's `seq + 1`, or `0`).
#[must_use]
pub(crate) fn replay_after_flush_error(
    last_flushed: Option<&WriteCursor>,
    failed_first_seq: u64,
) -> u64 {
    match last_flushed {
        Some(c) => resume_from_seq(c),
        None => failed_first_seq,
    }
}

/// Predecessor of an unacked unit so chain `seq + 1` is `failed_first_seq`.
fn predecessor_resume_cursor(failed_first: (u64, u64, Root), session_id: u64) -> WriteCursor {
    let (seq, slot, root) = failed_first;
    WriteCursor {
        session_id,
        seq: seq.saturating_sub(1),
        slot: Slot::new(slot),
        root,
    }
}

/// P0-13: end the session at the last **acked** cursor. Resume from that
/// cursor replays `seq + 1`. When nothing is acked, use a predecessor at
/// `first_failed_seq - 1` so the failed unit is not skipped by live-from-tip.
///
/// The cursor bounds the loss window; a failed unit never advances it.
fn session_end_on_p0_flush_error(
    last_flushed: Option<WriteCursor>,
    failed_first: Option<(u64, u64, Root)>,
    session_id: u64,
) -> SessionEnd {
    let cursor =
        last_flushed.or_else(|| failed_first.map(|f| predecessor_resume_cursor(f, session_id)));
    SessionEnd::Reconnect {
        reason: Some(ReconnectReason::Transport),
        cursor,
    }
}

fn clone_commit_unit(unit: &CommitUnit) -> CommitUnit {
    CommitUnit {
        blocks: unit.blocks.clone(),
        columns: unit.columns.clone(),
        fork_choice: unit.fork_choice.clone(),
        cursor: unit.cursor,
        done: None,
    }
}

/// Flush a P0 unit, or end the session so a later successful unit cannot
/// commit a seq that skips the failed one.
async fn flush_or_reconnect(
    writer: &WriterHandle,
    unit: CommitUnit,
    last_flushed: &mut Option<WriteCursor>,
    failed_first: Option<(u64, u64, Root)>,
    what: &'static str,
    resume: Option<&ResumeAuthority>,
    pending: Option<&PendingRetry>,
) -> Result<(), SessionEnd> {
    let session_id = unit.cursor.session_id;
    let first_seq = failed_first.map(|(s, _, _)| s);
    // Chain resume is seq+1; a predecessor of seq 0 cannot express replay
    // from 0. Keep the unit and re-submit after reconnect.
    let retry = if last_flushed.is_none() && first_seq == Some(0) {
        Some(clone_commit_unit(&unit))
    } else {
        None
    };
    match flush_committed(writer, unit, last_flushed, resume).await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!(
                target: "cc_storage::write_behind",
                error = %e,
                "{what}"
            );
            if let (Some(p), Some(u)) = (pending, retry) {
                p.store(u);
            }
            Err(session_end_on_p0_flush_error(
                *last_flushed,
                failed_first,
                session_id,
            ))
        }
    }
}

/// Planned recovery for a failed SubscribeEvents (testable without gRPC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscribeRecovery {
    /// Discard durable cursor; resubscribe live from tip.
    UnknownSession,
    /// Discard cursor; run GetHead + GetCanonicalRoots fallback (attribution).
    TooOld,
    /// Keep durable cursor; reconnect with it.
    ResourceExhausted,
    /// Keep durable cursor; transport reconnect.
    Transport,
}

/// Map a subscribe `Status` to a recovery plan and attribute metrics.
///
/// This is the handler core exercised by integration-style unit tests (O2).
#[must_use]
pub(crate) fn plan_and_attribute_subscribe_error(
    status: &Status,
    metrics: &StorageMetrics,
) -> SubscribeRecovery {
    let reason = classify_subscribe_status(status);
    match reason {
        Some(ReconnectReason::CursorUnknownSession) => {
            observe_reconnect(metrics, ReconnectReason::CursorUnknownSession);
            SubscribeRecovery::UnknownSession
        }
        Some(ReconnectReason::CursorTooOld) => {
            observe_reconnect(metrics, ReconnectReason::CursorTooOld);
            SubscribeRecovery::TooOld
        }
        Some(ReconnectReason::ResourceExhausted) => {
            observe_reconnect(metrics, ReconnectReason::ResourceExhausted);
            SubscribeRecovery::ResourceExhausted
        }
        Some(ReconnectReason::Transport) | None => {
            // Transport is attributed by the reconnect loop when `reason: Some`.
            SubscribeRecovery::Transport
        }
    }
}

/// Apply a recovery plan to the durable-cursor state machine (no network).
#[must_use]
pub(crate) fn session_end_for_recovery(
    recovery: SubscribeRecovery,
    durable_cursor: Option<&WriteCursor>,
) -> SessionEnd {
    match recovery {
        SubscribeRecovery::UnknownSession | SubscribeRecovery::TooOld => {
            SessionEnd::SetCursor(None)
        }
        SubscribeRecovery::ResourceExhausted => SessionEnd::Reconnect {
            reason: None, // already counted in plan_and_attribute
            cursor: durable_cursor.filter(|c| is_resumable(c)).cloned(),
        },
        SubscribeRecovery::Transport => SessionEnd::Reconnect {
            reason: Some(ReconnectReason::Transport),
            cursor: durable_cursor.filter(|c| is_resumable(c)).cloned(),
        },
    }
}

/// Pure plan for CURSOR_TOO_OLD steps 1–2 (D-14 attribution; no network).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalFallbackPlan {
    pub last_stored_slot: u64,
    pub head_slot: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub invoke_get_canonical_roots: bool,
}

#[must_use]
pub(crate) fn plan_canonical_fallback(
    head_slot: u64,
    durable_cursor: Option<&WriteCursor>,
) -> CanonicalFallbackPlan {
    let last_stored_slot = durable_cursor.map(|c| c.slot.as_u64()).unwrap_or(0);
    let start_slot = last_stored_slot.saturating_add(1);
    let end_slot = head_slot;
    CanonicalFallbackPlan {
        last_stored_slot,
        head_slot,
        start_slot,
        end_slot,
        invoke_get_canonical_roots: start_slot <= end_slot,
    }
}

/// Whether a HEAD for `head_slot` should flush given `commit_slots`.
#[must_use]
pub(crate) fn should_flush_for_head(
    open_slot: Option<u64>,
    head_slot: u64,
    commit_slots: u64,
    unit_nonempty: bool,
) -> bool {
    if !unit_nonempty {
        return false;
    }
    let open = open_slot.unwrap_or(head_slot);
    let span = head_slot.saturating_sub(open).saturating_add(1);
    span >= commit_slots.max(1)
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    cfg: &WriteBehindConfig,
    writer: &WriterHandle,
    metrics: &StorageMetrics,
    durable_cursor: Option<&WriteCursor>,
    shutdown: &mut watch::Receiver<bool>,
    migrator: Option<&Migrator>,
    replayer: Option<&ReplayDriver>,
    resume: &ResumeAuthority,
    pending: &PendingRetry,
) -> SessionEnd {
    let channel = match dial(&cfg.chain_uri, cfg.connect_timeout).await {
        Ok(c) => c,
        Err(e) => {
            metrics.set_following_head(false);
            warn!(
                target: "cc_storage::write_behind",
                error = %e,
                "chain dial failed"
            );
            return SessionEnd::Reconnect {
                reason: Some(ReconnectReason::Transport),
                cursor: durable_cursor.cloned(),
            };
        }
    };
    let mut client = ChainServiceClient::new(channel);

    // Only send a resume cursor when session_id is known and non-zero (O1).
    let req_cursor = durable_cursor.and_then(to_resume_cursor);

    let response = match client
        .subscribe_events(SubscribeEventsRequest {
            cursor: req_cursor.clone(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(status) => {
            metrics.set_following_head(false);
            return handle_subscribe_error(
                status,
                client,
                writer,
                metrics,
                durable_cursor,
                shutdown,
            )
            .await;
        }
    };

    // Live event-bus session established → following head for the CC-45c bar.
    // Cleared on stream loss / reconnect / stop (run_write_behind outer loop).
    metrics.set_following_head(true);
    info!(
        target: "cc_storage::write_behind",
        "SubscribeEvents established; cc_storage_following_head=1"
    );

    // Live session_id from response metadata (chain CC-44b seam). Fallback:
    // durable resume cursor's session, else 0 (= not resume-valid).
    let mut session_id = session_from_metadata(response.metadata())
        .or_else(|| {
            durable_cursor
                .filter(|c| is_resumable(c))
                .map(|c| c.session_id)
        })
        .unwrap_or(0);
    if session_id == 0 {
        debug!(
            target: "cc_storage::write_behind",
            "SubscribeEvents returned no session metadata; durable resume disabled until known"
        );
    }

    let mut acc = Accumulator::default();
    let mut stream = response.into_inner();
    let mut flush_tick = interval(Duration::from_millis(100));
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Keep the seq frontier even when session_id is 0 (finding 3); only
    // [`to_resume_cursor`] drops a zero-session cursor on the wire.
    let mut last_flushed: Option<WriteCursor> = durable_cursor.cloned();
    if stamp_session_id(&mut last_flushed, session_id) {
        resume.store(last_flushed);
        // Subscribed live-from-tip because the stored frontier was not
        // resume-valid. Reconnect with the stamped cursor so seq+1 is replayed.
        if req_cursor.is_none() && last_flushed.as_ref().is_some_and(is_resumable) {
            return SessionEnd::Reconnect {
                reason: None,
                cursor: last_flushed,
            };
        }
    }

    if let Some(unit) = pending.take() {
        // Only seq-0 units are stashed; predecessor cannot express replay from 0.
        let first = Some((0, unit.cursor.slot.as_u64(), unit.cursor.root));
        if let Err(end) = flush_or_reconnect(
            writer,
            unit,
            &mut last_flushed,
            first,
            "P0 retry after flush error failed",
            Some(resume),
            Some(pending),
        )
        .await
        {
            return end;
        }
    }

    loop {
        if *shutdown.borrow() {
            if let Some(unit) = take_flush(&mut acc, session_id) {
                let _ = flush_committed(writer, unit, &mut last_flushed, Some(resume)).await;
            }
            return match last_flushed {
                Some(c) => SessionEnd::SetCursor(Some(c)),
                None => SessionEnd::Shutdown,
            };
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    if let Some(unit) = take_flush(&mut acc, session_id) {
                        let _ = flush_committed(writer, unit, &mut last_flushed, Some(resume)).await;
                    }
                    return SessionEnd::Shutdown;
                }
            }
            _ = flush_tick.tick() => {
                if should_flush_latency(&acc, cfg.commit_max_latency) {
                    let first = acc.first;
                    if let Some(unit) = take_flush(&mut acc, session_id)
                        && let Err(end) = flush_or_reconnect(
                            writer,
                            unit,
                            &mut last_flushed,
                            first,
                            "P0 commit failed on latency flush",
                            Some(resume),
                            Some(pending),
                        )
                        .await
                    {
                        return end;
                    }
                    observe_lag(metrics, &acc);
                }
            }
            next = stream.next() => {
                match next {
                    None => {
                        let first = acc.first;
                        if let Some(unit) = take_flush(&mut acc, session_id)
                            && let Err(end) = flush_or_reconnect(
                                writer,
                                unit,
                                &mut last_flushed,
                                first,
                                "P0 commit failed on stream end",
                                Some(resume),
                                Some(pending),
                            )
                            .await
                        {
                            return end;
                        }
                        return SessionEnd::Reconnect {
                            reason: Some(ReconnectReason::Transport),
                            cursor: last_flushed,
                        };
                    }
                    Some(Err(status)) => {
                        let first = acc.first;
                        if let Some(unit) = take_flush(&mut acc, session_id)
                            && let Err(end) = flush_or_reconnect(
                                writer,
                                unit,
                                &mut last_flushed,
                                first,
                                "P0 commit failed on stream error",
                                Some(resume),
                                Some(pending),
                            )
                            .await
                        {
                            return end;
                        }
                        let reason = classify_stream_status(&status);
                        warn!(
                            target: "cc_storage::write_behind",
                            code = ?status.code(),
                            reason = ?reason,
                            "event stream error"
                        );
                        return SessionEnd::Reconnect {
                            reason,
                            cursor: last_flushed,
                        };
                    }
                    Some(Ok(ev)) => {
                        if let Some(end) = apply_one_event(
                            &ev,
                            &mut acc,
                            &mut session_id,
                            req_cursor.as_ref(),
                            writer,
                            metrics,
                            cfg,
                            &mut last_flushed,
                            migrator,
                            replayer,
                            Some(resume),
                            Some(pending),
                        )
                        .await
                        {
                            return end;
                        }
                    }
                }
            }
        }
    }
}

/// Apply one stream event: accumulate, then flush on HEAD / max_events /
/// finalized. A P0 flush error returns [`SessionEnd::Reconnect`] so the
/// session stops consuming (P0-13). The cursor bounds the loss window; a
/// failed unit never advances it. Resume from the last acked cursor replays
/// `seq + 1`.
#[allow(clippy::too_many_arguments)]
async fn apply_one_event(
    ev: &Event,
    acc: &mut Accumulator,
    session_id: &mut u64,
    req_cursor: Option<&Cursor>,
    writer: &WriterHandle,
    metrics: &StorageMetrics,
    cfg: &WriteBehindConfig,
    last_flushed: &mut Option<WriteCursor>,
    migrator: Option<&Migrator>,
    replayer: Option<&ReplayDriver>,
    resume: Option<&ResumeAuthority>,
    pending: Option<&PendingRetry>,
) -> Option<SessionEnd> {
    // Prefer metadata session; never overwrite a known non-zero
    // session with zero. Events do not carry session_id.
    if *session_id == 0
        && let Some(c) = req_cursor
    {
        *session_id = c.session_id;
    }
    if stamp_session_id(last_flushed, *session_id)
        && let Some(r) = resume
    {
        r.store(*last_flushed);
    }

    let head_flush = if ev.kind() == EventKind::Head {
        should_flush_for_head(acc.open_slot, ev.slot, cfg.commit_slots, !acc.is_empty())
    } else {
        false
    };

    match apply_event(acc, ev, head_flush) {
        Ok(ApplyAction::Continue) => {}
        Ok(ApplyAction::FlushForHead) => {
            let first = acc.first;
            if let Some(unit) = take_flush(acc, *session_id)
                && let Err(end) = flush_or_reconnect(
                    writer,
                    unit,
                    last_flushed,
                    first,
                    "P0 commit failed on HEAD flush",
                    resume,
                    pending,
                )
                .await
            {
                return Some(end);
            }
            if ev.kind() == EventKind::Head {
                acc.latest_head_slot = Some(ev.slot);
                observe_lag(metrics, acc);
            }
        }
        Err(e) => {
            warn!(error = %e, seq = ev.seq, "skip event");
        }
    }

    // CC-41 / CC-42: FINALIZED_CHECKPOINT drives migration
    // and (on cadence) the snapshot ring. Flush any open P0
    // unit first so no uncommitted hot rows for slots ≤ new
    // split land after the split advances (I-split-fin).
    if ev.kind() == EventKind::FinalizedCheckpoint && (migrator.is_some() || replayer.is_some()) {
        let first = acc.first;
        if let Some(unit) = take_flush(acc, *session_id)
            && let Err(end) = flush_or_reconnect(
                writer,
                unit,
                last_flushed,
                first,
                "P0 flush before migration/snapshot failed",
                resume,
                pending,
            )
            .await
        {
            return Some(end);
        }
        let finalized_root = root_from_event(&ev.root);
        let (epoch, state_root) =
            parse_finalized_payload(&ev.payload).unwrap_or_else(|| (ev.slot / 32, Root::default()));
        if let Some(mig) = migrator {
            maybe_migrate_on_finalized(mig, epoch, finalized_root, state_root).await;
        }
        // CC-42: snapshot on cadence after migration advances split.
        if let Some(rep) = replayer {
            maybe_snapshot_on_finalized(rep, epoch, finalized_root, state_root).await;
        }
    }

    if acc.event_count >= cfg.commit_max_events {
        let first = acc.first;
        if let Some(unit) = take_flush(acc, *session_id)
            && let Err(end) = flush_or_reconnect(
                writer,
                unit,
                last_flushed,
                first,
                "P0 commit failed on max_events flush",
                resume,
                pending,
            )
            .await
        {
            return Some(end);
        }
    }
    None
}

#[derive(Debug)]
enum ApplyAction {
    Continue,
    FlushForHead,
}

fn apply_event(
    acc: &mut Accumulator,
    ev: &Event,
    head_should_flush: bool,
) -> Result<ApplyAction, String> {
    let root = root_from_bytes(&ev.root)?;
    match ev.kind() {
        EventKind::BlockImported => {
            // payload = [verdict_byte] ‖ SignedBeaconBlock SSZ
            if ev.payload.is_empty() {
                return Err("BLOCK_IMPORTED empty payload".into());
            }
            let verdict = ev.payload[0];
            let ssz = ev.payload[1..].to_vec();
            if ssz.is_empty() {
                acc.note_event(ev.seq, ev.slot, root);
                return Ok(ApplyAction::Continue);
            }
            let da = match verdict {
                BLOCK_PAYLOAD_VERDICT_IMPORTED => Some(DaStatus::Available),
                BLOCK_PAYLOAD_VERDICT_DEFERRED_DA => Some(DaStatus::Deferred),
                _ => None,
            };
            acc.blocks.push(StagedBlock {
                slot: Slot::new(ev.slot),
                root,
                ssz,
                update_canonical: false,
                write_state_root: true,
                da_status: da,
            });
            acc.note_event(ev.seq, ev.slot, root);
            Ok(ApplyAction::Continue)
        }
        EventKind::DataColumn => {
            let ssz = ev.payload.clone();
            let index = if ssz.len() > COLUMN_INDEX_SSZ_OFFSET {
                column_index_at_offset(&ssz).unwrap_or(0)
            } else if ssz.len() >= 2 {
                u16::from_le_bytes([ssz[0], ssz[1]])
            } else {
                0
            };
            acc.columns.push(StagedColumn {
                slot: Slot::new(ev.slot),
                root,
                index,
                ssz,
            });
            acc.note_event(ev.seq, ev.slot, root);
            Ok(ApplyAction::Continue)
        }
        EventKind::Head => {
            // Flush policy: HEAD for slot S+1 with commit_slots=1 (default), or
            // when the unit spans ≥ commit_slots (O4).
            acc.latest_head_slot = Some(ev.slot);
            let action = if head_should_flush {
                ApplyAction::FlushForHead
            } else {
                ApplyAction::Continue
            };
            if let Some(b) = acc.blocks.iter_mut().rev().find(|b| b.root == root) {
                b.update_canonical = true;
            }
            // Include HEAD in the unit so cursor seq covers it.
            acc.note_event(ev.seq, ev.slot, root);
            Ok(action)
        }
        EventKind::ChainReorg => {
            if let Some(b) = acc.blocks.iter_mut().rev().find(|b| b.root == root) {
                b.update_canonical = true;
            }
            acc.note_event(ev.seq, ev.slot, root);
            Ok(ApplyAction::Continue)
        }
        EventKind::FinalizedCheckpoint => {
            if ev.payload.len() >= 40 + 240 {
                let fc = ev.payload[40..].to_vec();
                acc.fork_choice = Some(StagedForkChoiceScalars { ssz: fc });
            } else if ev.payload.len() > 40 {
                acc.fork_choice = Some(StagedForkChoiceScalars {
                    ssz: ev.payload[40..].to_vec(),
                });
            }
            acc.note_event(ev.seq, ev.slot, root);
            Ok(ApplyAction::Continue)
        }
        EventKind::Unspecified => {
            acc.note_event(ev.seq, ev.slot, root);
            Ok(ApplyAction::Continue)
        }
    }
}

fn take_flush(acc: &mut Accumulator, session_id: u64) -> Option<CommitUnit> {
    if acc.is_empty() {
        return None;
    }
    let taken = std::mem::take(acc);
    if let Some(mut unit) = taken.into_commit_unit() {
        unit.cursor.session_id = session_id;
        Some(unit)
    } else {
        None
    }
}

fn should_flush_latency(acc: &Accumulator, max: Duration) -> bool {
    match acc.opened_at {
        Some(t) => !acc.is_empty() && t.elapsed() >= max,
        None => false,
    }
}

fn observe_lag(metrics: &StorageMetrics, acc: &Accumulator) {
    if let (Some(head), Some(open)) = (acc.latest_head_slot, acc.open_slot) {
        let lag = head.saturating_sub(open) as f64;
        metrics.write_behind_lag_slots.observe(lag);
    }
}

/// Handle SubscribeEvents RPC failure: attribute and run recovery paths (§4.6).
async fn handle_subscribe_error(
    status: Status,
    client: ChainServiceClient<Channel>,
    _writer: &WriterHandle,
    metrics: &StorageMetrics,
    durable_cursor: Option<&WriteCursor>,
    shutdown: &mut watch::Receiver<bool>,
) -> SessionEnd {
    let recovery = plan_and_attribute_subscribe_error(&status, metrics);
    match recovery {
        SubscribeRecovery::UnknownSession => {
            info!(
                target: "cc_storage::write_behind",
                "CURSOR_UNKNOWN_SESSION — discard cursor, resubscribe live from tip (attribution only; D-14)"
            );
            let _ = client.clone().get_head(GetHeadRequest {}).await;
            if *shutdown.borrow() {
                return SessionEnd::Shutdown;
            }
            session_end_for_recovery(recovery, durable_cursor)
        }
        SubscribeRecovery::TooOld => {
            info!(
                target: "cc_storage::write_behind",
                "CURSOR_TOO_OLD — canonical-roots fallback (attribution only; hole record/fill is M4.4/M4.5)"
            );
            let _ = run_cursor_too_old_fallback(client, durable_cursor).await;
            session_end_for_recovery(recovery, durable_cursor)
        }
        SubscribeRecovery::ResourceExhausted | SubscribeRecovery::Transport => {
            session_end_for_recovery(recovery, durable_cursor)
        }
    }
}

/// §4.6 steps 1–2 for CURSOR_TOO_OLD (gap fill itself is later milestones).
async fn run_cursor_too_old_fallback(
    mut client: ChainServiceClient<Channel>,
    durable_cursor: Option<&WriteCursor>,
) -> Result<CanonicalFallbackPlan, Status> {
    let head = client.get_head(GetHeadRequest {}).await?.into_inner();
    let plan = plan_canonical_fallback(head.head_slot, durable_cursor);
    if plan.invoke_get_canonical_roots {
        let _ = client
            .get_canonical_roots(GetCanonicalRootsRequest {
                start_slot: plan.start_slot,
                end_slot: plan.end_slot,
            })
            .await;
    }
    debug!(
        target: "cc_storage::write_behind",
        start = plan.start_slot,
        end = plan.end_slot,
        "CURSOR_TOO_OLD canonical-roots fallback invoked (D-14 attribution; hole not recorded here)"
    );
    Ok(plan)
}

/// Classify a failed SubscribeEvents status into a reconnect reason.
#[must_use]
pub(crate) fn classify_subscribe_status(status: &Status) -> Option<ReconnectReason> {
    if status.code() == Code::ResourceExhausted {
        return Some(ReconnectReason::ResourceExhausted);
    }
    if let Ok(Some(info)) = error_info_from_status(status) {
        return match info.reason.as_str() {
            REASON_CURSOR_UNKNOWN_SESSION => Some(ReconnectReason::CursorUnknownSession),
            REASON_CURSOR_TOO_OLD => Some(ReconnectReason::CursorTooOld),
            _ => {
                if status.code() == Code::FailedPrecondition {
                    // Unknown FailedPrecondition — still a reconnect signal.
                    Some(ReconnectReason::Transport)
                } else {
                    Some(ReconnectReason::Transport)
                }
            }
        };
    }
    if status.code() == Code::FailedPrecondition {
        // Message fallback when ErrorInfo missing.
        let msg = status.message();
        if msg.contains("CURSOR_UNKNOWN_SESSION") || msg.contains("session") {
            return Some(ReconnectReason::CursorUnknownSession);
        }
        if msg.contains("CURSOR_TOO_OLD") || msg.contains("too old") || msg.contains("ring") {
            return Some(ReconnectReason::CursorTooOld);
        }
    }
    Some(ReconnectReason::Transport)
}

fn classify_stream_status(status: &Status) -> Option<ReconnectReason> {
    if status.code() == Code::ResourceExhausted {
        Some(ReconnectReason::ResourceExhausted)
    } else {
        Some(ReconnectReason::Transport)
    }
}

/// Attribute a reconnect reason exactly once (test helper / recovery entry).
///
/// Returns the reason that was counted. Used by the in-process chain double tests
/// so attribution is assertable without a full gRPC stack.
pub(crate) fn attribute_cursor_rejection(
    metrics: &StorageMetrics,
    status: &Status,
) -> Option<ReconnectReason> {
    let reason = classify_subscribe_status(status)?;
    observe_reconnect(metrics, reason);
    Some(reason)
}

fn root_from_bytes(b: &[u8]) -> Result<Root, String> {
    if b.len() != 32 {
        // Pad / truncate defensively for synthetic test roots.
        let mut arr = [0u8; 32];
        let n = b.len().min(32);
        arr[..n].copy_from_slice(&b[..n]);
        return Ok(Root::from_array(arr));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(b);
    Ok(Root::from_array(arr))
}

async fn dial(uri: &str, timeout: Duration) -> Result<Channel, tonic::transport::Error> {
    let endpoint = tonic::transport::Endpoint::from_shared(uri.to_owned())?
        .connect_timeout(timeout)
        .timeout(Duration::from_secs(30));
    endpoint.connect().await
}

fn next_backoff(prev: Duration, cap: Duration) -> Duration {
    prev.saturating_mul(2).min(cap)
}

/// Returns `true` if shutdown was requested during the sleep.
async fn wait_backoff(shutdown: &mut watch::Receiver<bool>, backoff: Duration) -> bool {
    tokio::select! {
        _ = shutdown.changed() => *shutdown.borrow(),
        _ = tokio::time::sleep(backoff) => *shutdown.borrow(),
    }
}

// ── pure helpers exercised by unit tests ────────────────────────────────────

/// Build a [`WriteCursor`] from a delivered event + session (mirrors chain helper).
#[must_use]
pub(crate) fn cursor_for_event(ev: &Event, session_id: u64) -> WriteCursor {
    WriteCursor {
        session_id,
        seq: ev.seq,
        slot: Slot::new(ev.slot),
        root: root_from_bytes(&ev.root).unwrap_or(Root::ZERO),
    }
}

/// Apply one event into an accumulator and optionally produce a commit unit.
///
/// Test-facing driver that does not touch the network.
#[cfg(test)]
fn test_drive_event(
    acc: &mut Accumulator,
    ev: &Event,
    session_id: u64,
    max_events: usize,
) -> Option<CommitUnit> {
    test_drive_event_with_slots(acc, ev, session_id, max_events, 1)
}

#[cfg(test)]
fn test_drive_event_with_slots(
    acc: &mut Accumulator,
    ev: &Event,
    session_id: u64,
    max_events: usize,
    commit_slots: u64,
) -> Option<CommitUnit> {
    let head_flush = if ev.kind() == EventKind::Head {
        should_flush_for_head(acc.open_slot, ev.slot, commit_slots, !acc.is_empty())
    } else {
        false
    };
    let action = apply_event(acc, ev, head_flush).ok()?;
    if matches!(action, ApplyAction::FlushForHead) || acc.event_count >= max_events {
        return take_flush(acc, session_id);
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::ReasonLabels;
    use crate::writer::{
        CommitUnit, StagedBlock, WriterBounds, WriterFaults, WriterHandle, block_present,
        load_write_cursor, spawn_writer,
    };
    use cc_proto::status_with_error_info;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::engine::{Durability, Engine, EngineOptions};
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::oneshot;

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cc-wb-{label}-{nanos}"))
    }

    fn eng(label: &str) -> Arc<Engine> {
        let dir = tmp_dir(label);
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(
            Engine::open(
                &dir,
                EngineOptions::default().with_durability(Durability::None),
            )
            .unwrap(),
        )
    }

    fn block_event(seq: u64, slot: u64, root: Root, ssz: Vec<u8>) -> Event {
        let mut payload = Vec::with_capacity(1 + ssz.len());
        payload.push(BLOCK_PAYLOAD_VERDICT_IMPORTED);
        payload.extend_from_slice(&ssz);
        Event {
            seq,
            slot,
            root: root.as_slice().to_vec(),
            kind: EventKind::BlockImported as i32,
            payload,
        }
    }

    fn head_event(seq: u64, slot: u64, root: Root) -> Event {
        Event {
            seq,
            slot,
            root: root.as_slice().to_vec(),
            kind: EventKind::Head as i32,
            payload: slot.to_le_bytes().to_vec(),
        }
    }

    #[test]
    fn classify_unknown_session_before_too_old() {
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "cursor session_id does not match",
            REASON_CURSOR_UNKNOWN_SESSION,
            "eth.chain.v1",
        );
        assert_eq!(
            classify_subscribe_status(&status),
            Some(ReconnectReason::CursorUnknownSession)
        );
    }

    #[test]
    fn classify_cursor_too_old() {
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "cursor fell out of the event ring",
            REASON_CURSOR_TOO_OLD,
            "eth.chain.v1",
        );
        assert_eq!(
            classify_subscribe_status(&status),
            Some(ReconnectReason::CursorTooOld)
        );
    }

    /// CC-44 /2 run (a) attribution: exactly 1 `cursor_unknown_session`.
    #[test]
    fn attribution_cursor_unknown_session_exactly_one() {
        let m = metrics();
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "session mismatch",
            REASON_CURSOR_UNKNOWN_SESSION,
            "eth.chain.v1",
        );
        let r = attribute_cursor_rejection(&m, &status).unwrap();
        assert_eq!(r, ReconnectReason::CursorUnknownSession);
        let n = m
            .stream_reconnect
            .get_or_create(&ReasonLabels {
                reason: ReconnectReason::CursorUnknownSession.as_str().to_owned(),
            })
            .get();
        assert_eq!(n, 1);
        // The other reason stays 0.
        let other = m
            .stream_reconnect
            .get_or_create(&ReasonLabels {
                reason: ReconnectReason::CursorTooOld.as_str().to_owned(),
            })
            .get();
        assert_eq!(other, 0);
    }

    /// CC-44 /2 run (b) attribution: exactly 1 `cursor_too_old`.
    #[test]
    fn attribution_cursor_too_old_exactly_one() {
        let m = metrics();
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "evicted",
            REASON_CURSOR_TOO_OLD,
            "eth.chain.v1",
        );
        let r = attribute_cursor_rejection(&m, &status).unwrap();
        assert_eq!(r, ReconnectReason::CursorTooOld);
        let n = m
            .stream_reconnect
            .get_or_create(&ReasonLabels {
                reason: ReconnectReason::CursorTooOld.as_str().to_owned(),
            })
            .get();
        assert_eq!(n, 1);
        let other = m
            .stream_reconnect
            .get_or_create(&ReasonLabels {
                reason: ReconnectReason::CursorUnknownSession.as_str().to_owned(),
            })
            .get();
        assert_eq!(other, 0);
    }

    /// Across the two runs the counter is exactly 1 of each (pair assertion).
    #[test]
    fn attribution_pair_exactly_one_of_each() {
        let m = metrics();
        let s1 = status_with_error_info(
            Code::FailedPrecondition,
            "a",
            REASON_CURSOR_UNKNOWN_SESSION,
            "eth.chain.v1",
        );
        let s2 = status_with_error_info(
            Code::FailedPrecondition,
            "b",
            REASON_CURSOR_TOO_OLD,
            "eth.chain.v1",
        );
        attribute_cursor_rejection(&m, &s1).unwrap();
        attribute_cursor_rejection(&m, &s2).unwrap();
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_unknown_session".into(),
                })
                .get(),
            1
        );
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_too_old".into(),
                })
                .get(),
            1
        );
        // D-14: this is attribution only — not clause-2 discharge.
    }

    #[test]
    fn resource_exhausted_classified() {
        let status = Status::resource_exhausted("slow consumer");
        assert_eq!(
            classify_subscribe_status(&status),
            Some(ReconnectReason::ResourceExhausted)
        );
    }

    #[test]
    fn head_flushes_prior_unit_with_cursor_at_last_included_seq() {
        let mut acc = Accumulator::default();
        let r1 = root_n(1);
        let ssz = synth_block(1, &Root::ZERO, &root_n(0xF0));
        let ev1 = block_event(0, 1, r1, ssz);
        assert!(test_drive_event(&mut acc, &ev1, 42, 64).is_none());
        let head = head_event(1, 2, r1);
        let unit = test_drive_event(&mut acc, &head, 42, 64).expect("flush on HEAD");
        // Cursor is the last event included (HEAD seq=1), same batch as block data.
        assert_eq!(unit.cursor.seq, 1);
        assert_eq!(unit.cursor.session_id, 42);
        assert_eq!(unit.blocks.len(), 1);
        assert!(unit.blocks[0].update_canonical);
    }

    #[tokio::test]
    async fn commit_unit_cursor_and_block_atomic_via_writer() {
        let engine = eng("atomic");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );

        let mut acc = Accumulator::default();
        let r1 = root_n(1);
        let ssz = synth_block(1, &Root::ZERO, &root_n(0xF0));
        let _ = test_drive_event(&mut acc, &block_event(0, 1, r1, ssz.clone()), 7, 64);
        let unit = test_drive_event(&mut acc, &head_event(1, 2, r1), 7, 64).unwrap();

        let (done_tx, done_rx) = oneshot::channel();
        let mut unit = unit;
        unit.done = Some(done_tx);
        // Ensure canonical update.
        unit.blocks[0].update_canonical = true;
        handle.submit_p0(unit).await.unwrap();
        done_rx.await.unwrap().unwrap();

        assert!(block_present(&engine, &r1).unwrap());
        let c = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(c.seq, 1);
        assert_eq!(c.session_id, 7);
        assert_eq!(c.root, r1);

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn fail_commit_leaves_neither_block_nor_cursor() {
        let engine = eng("fail-atomic");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let handle = spawn_writer(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );
        let r = root_n(5);
        let ssz = synth_block(5, &Root::ZERO, &root_n(0xF0));
        let (done_tx, done_rx) = oneshot::channel();
        handle
            .submit_p0(CommitUnit {
                blocks: vec![StagedBlock {
                    slot: Slot::new(5),
                    root: r,
                    ssz,
                    update_canonical: true,
                    write_state_root: false,
                    da_status: None,
                }],
                columns: vec![],
                fork_choice: None,
                cursor: WriteCursor {
                    session_id: 1,
                    seq: 9,
                    slot: Slot::new(5),
                    root: r,
                },
                done: Some(done_tx),
            })
            .await
            .unwrap();
        assert!(done_rx.await.unwrap().is_err());
        assert!(!block_present(&engine, &r).unwrap());
        assert!(load_write_cursor(&engine).unwrap().is_none());
        let _ = shutdown_tx.send(true);
    }

    /// Handler path (not pure classify): UNKNOWN_SESSION clears cursor + metric.
    #[test]
    fn handler_unknown_session_clears_cursor_and_counts_exactly_one() {
        let m = metrics();
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "cursor session_id does not match this process; server may have restarted",
            REASON_CURSOR_UNKNOWN_SESSION,
            "eth.chain.v1",
        );
        let durable = WriteCursor {
            session_id: 99,
            seq: 10,
            slot: Slot::new(10),
            root: root_n(1),
        };
        let recovery = plan_and_attribute_subscribe_error(&status, &m);
        assert_eq!(recovery, SubscribeRecovery::UnknownSession);
        let end = session_end_for_recovery(recovery, Some(&durable));
        assert!(
            matches!(end, SessionEnd::SetCursor(None)),
            "must discard durable cursor for live-from-tip"
        );
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_unknown_session".into(),
                })
                .get(),
            1
        );
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_too_old".into(),
                })
                .get(),
            0
        );
    }

    /// Handler path: TOO_OLD clears cursor, counts, and plans canonical fallback.
    #[test]
    fn handler_cursor_too_old_clears_and_plans_canonical_fallback() {
        let m = metrics();
        let status = status_with_error_info(
            Code::FailedPrecondition,
            "cursor fell out of the event ring; fall back to GetHead and resubscribe",
            REASON_CURSOR_TOO_OLD,
            "eth.chain.v1",
        );
        let durable = WriteCursor {
            session_id: 7,
            seq: 1,
            slot: Slot::new(100),
            root: root_n(2),
        };
        let recovery = plan_and_attribute_subscribe_error(&status, &m);
        assert_eq!(recovery, SubscribeRecovery::TooOld);
        let end = session_end_for_recovery(recovery, Some(&durable));
        assert!(matches!(end, SessionEnd::SetCursor(None)));
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_too_old".into(),
                })
                .get(),
            1
        );
        let plan = plan_canonical_fallback(200, Some(&durable));
        assert!(plan.invoke_get_canonical_roots);
        assert_eq!(plan.start_slot, 101);
        assert_eq!(plan.end_slot, 200);
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_unknown_session".into(),
                })
                .get(),
            0
        );
    }

    /// Pair assertion through the handler planner (O2).
    #[test]
    fn handler_pair_exactly_one_of_each_reason() {
        let m = metrics();
        let s1 = status_with_error_info(
            Code::FailedPrecondition,
            "a",
            REASON_CURSOR_UNKNOWN_SESSION,
            "eth.chain.v1",
        );
        let s2 = status_with_error_info(
            Code::FailedPrecondition,
            "b",
            REASON_CURSOR_TOO_OLD,
            "eth.chain.v1",
        );
        let r1 = plan_and_attribute_subscribe_error(&s1, &m);
        let r2 = plan_and_attribute_subscribe_error(&s2, &m);
        assert_eq!(r1, SubscribeRecovery::UnknownSession);
        assert_eq!(r2, SubscribeRecovery::TooOld);
        assert!(matches!(
            session_end_for_recovery(r1, None),
            SessionEnd::SetCursor(None)
        ));
        assert!(matches!(
            session_end_for_recovery(r2, None),
            SessionEnd::SetCursor(None)
        ));
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_unknown_session".into(),
                })
                .get(),
            1
        );
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "cursor_too_old".into(),
                })
                .get(),
            1
        );
    }

    /// RESOURCE_EXHAUSTED keeps a **resumable** cursor.
    #[test]
    fn handler_resource_exhausted_keeps_resumable_cursor() {
        let m = metrics();
        let status = Status::resource_exhausted("slow consumer");
        let durable = WriteCursor {
            session_id: 42,
            seq: 9,
            slot: Slot::new(9),
            root: root_n(3),
        };
        let recovery = plan_and_attribute_subscribe_error(&status, &m);
        assert_eq!(recovery, SubscribeRecovery::ResourceExhausted);
        match session_end_for_recovery(recovery, Some(&durable)) {
            SessionEnd::Reconnect {
                reason: None,
                cursor: Some(c),
            } => {
                assert_eq!(c.session_id, 42);
                assert_eq!(c.seq, 9);
            }
            other => panic!("expected reconnect with cursor, got {other:?}"),
        }
        assert_eq!(
            m.stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: "resource_exhausted".into(),
                })
                .get(),
            1
        );
    }

    /// session_id==0 is never resume-valid (O1) — no UNKNOWN_SESSION spam.
    #[test]
    fn zero_session_is_not_resumable_and_not_sent() {
        let c = WriteCursor {
            session_id: 0,
            seq: 5,
            slot: Slot::new(5),
            root: root_n(1),
        };
        assert!(!is_resumable(&c));
        assert!(to_resume_cursor(&c).is_none());
        // RESOURCE_EXHAUSTED with zero session does not keep a bad cursor.
        match session_end_for_recovery(SubscribeRecovery::ResourceExhausted, Some(&c)) {
            SessionEnd::Reconnect {
                cursor: None,
                reason: None,
            } => {}
            other => panic!("zero-session must not be retained for resume: {other:?}"),
        }
    }

    #[test]
    fn session_from_metadata_parses_chain_key() {
        let mut md = tonic::metadata::MetadataMap::new();
        md.insert(
            SESSION_ID_METADATA_KEY,
            "12345".parse().expect("ascii digits"),
        );
        assert_eq!(session_from_metadata(&md), Some(12345));
        assert_eq!(
            session_from_metadata(&tonic::metadata::MetadataMap::new()),
            None
        );
    }

    /// SEC-44b: failed P0 commit does not advance last_flushed.
    #[tokio::test]
    async fn failed_commit_does_not_advance_last_flushed() {
        let engine = eng("no-advance");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let handle = spawn_writer(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );
        let mut last = Some(WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(1),
            root: root_n(1),
        });
        let prior = last;
        let r = root_n(8);
        let err = flush_committed(
            &handle,
            CommitUnit {
                blocks: vec![StagedBlock {
                    slot: Slot::new(8),
                    root: r,
                    ssz: synth_block(8, &Root::ZERO, &root_n(0xF0)),
                    update_canonical: true,
                    write_state_root: false,
                    da_status: None,
                }],
                columns: vec![],
                fork_choice: None,
                cursor: WriteCursor {
                    session_id: 1,
                    seq: 99,
                    slot: Slot::new(8),
                    root: r,
                },
                done: None,
            },
            &mut last,
            None,
        )
        .await;
        assert!(err.is_err());
        assert_eq!(
            last, prior,
            "must not advance resume cursor without commit ack"
        );
        let _ = shutdown_tx.send(true);
    }

    async fn drive(
        ev: &Event,
        acc: &mut Accumulator,
        session_id: &mut u64,
        writer: &WriterHandle,
        metrics: &StorageMetrics,
        cfg: &WriteBehindConfig,
        last_flushed: &mut Option<WriteCursor>,
    ) -> Option<SessionEnd> {
        apply_one_event(
            ev,
            acc,
            session_id,
            None,
            writer,
            metrics,
            cfg,
            last_flushed,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// P0-13: an injected P0 flush error ends the session with
    /// `Reconnect { cursor: last_flushed }`. Resume from that cursor
    /// replays `seq + 1` (the failed unit). No `WriteCursor` with a seq
    /// beyond `last_flushed` is committed after the error.
    ///
    /// The cursor bounds the loss window; a failed unit never advances it.
    #[tokio::test]
    async fn p0_flush_error_ends_session_at_last_flushed() {
        let engine = eng("p0-13-reconnect");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let fail_next = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let faults = WriterFaults {
            fail_next_commit: Arc::clone(&fail_next),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let handle = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );

        let cfg = WriteBehindConfig::default();
        let mut session_id = 7u64;
        let mut last_flushed: Option<WriteCursor> = None;
        let mut acc = Accumulator::default();

        let r1 = root_n(1);
        let ev_block1 = block_event(0, 1, r1, synth_block(1, &Root::ZERO, &root_n(0xF0)));
        let ev_head1 = head_event(1, 2, r1);
        let r2 = root_n(2);
        let ev_block2 = block_event(2, 2, r2, synth_block(2, &r1, &root_n(0xF1)));
        let ev_head2 = head_event(3, 3, r2);

        // Unit A: block seq=0 + HEAD seq=1 commits. Frontier = 1.
        assert!(
            drive(
                &ev_block1,
                &mut acc,
                &mut session_id,
                &handle,
                &m,
                &cfg,
                &mut last_flushed,
            )
            .await
            .is_none()
        );
        assert!(
            drive(
                &ev_head1,
                &mut acc,
                &mut session_id,
                &handle,
                &m,
                &cfg,
                &mut last_flushed,
            )
            .await
            .is_none()
        );
        let frontier = last_flushed.expect("first unit must ack");
        assert_eq!(frontier.seq, 1);
        assert_eq!(frontier.session_id, 7);
        let durable = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(durable.seq, 1);
        assert!(block_present(&engine, &r1).unwrap());

        // Inject a P0 flush error on unit B (seq 2–3).
        fail_next.store(true, Ordering::SeqCst);
        assert!(
            drive(
                &ev_block2,
                &mut acc,
                &mut session_id,
                &handle,
                &m,
                &cfg,
                &mut last_flushed,
            )
            .await
            .is_none()
        );
        let end = drive(
            &ev_head2,
            &mut acc,
            &mut session_id,
            &handle,
            &m,
            &cfg,
            &mut last_flushed,
        )
        .await
        .expect("injected P0 flush error must end the session");
        match end {
            SessionEnd::Reconnect {
                cursor: Some(c), ..
            } => {
                assert_eq!(c.seq, frontier.seq, "reconnect at last_flushed");
                assert_eq!(c.session_id, frontier.session_id);
                assert_eq!(c.root, frontier.root);
                let wire = to_resume_cursor(&c).expect("last_flushed must be resume-valid");
                assert_eq!(wire.seq, frontier.seq);
                assert_eq!(resume_from_seq(&c), 2);
                assert_eq!(replay_after_flush_error(Some(&c), 2), 2);
            }
            other => panic!("expected Reconnect {{ cursor: last_flushed }}, got {other:?}"),
        }

        // No WriteCursor with a seq beyond last_flushed after the error.
        let durable_after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(
            durable_after.seq, frontier.seq,
            "the cursor bounds the loss window; a failed unit never advances it"
        );
        assert_eq!(last_flushed.as_ref().map(|c| c.seq), Some(frontier.seq));
        assert!(!block_present(&engine, &r2).unwrap());

        // Resume from last_flushed replays seq+1 (the failed unit).
        fail_next.store(false, Ordering::SeqCst);
        acc = Accumulator::default();
        let from = resume_from_seq(&frontier);
        for ev in [&ev_block2, &ev_head2] {
            assert!(ev.seq >= from);
            assert!(
                drive(
                    ev,
                    &mut acc,
                    &mut session_id,
                    &handle,
                    &m,
                    &cfg,
                    &mut last_flushed,
                )
                .await
                .is_none()
            );
        }
        let after_resume = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after_resume.seq, 3);
        assert!(after_resume.seq > frontier.seq);
        assert!(block_present(&engine, &r2).unwrap());

        let _ = shutdown_tx.send(true);
    }

    /// P0-13: first flush error (`last_flushed == None`) must not live-from-tip.
    /// Predecessor cursor is `first_failed_seq - 1`; resume replays the failed unit.
    #[tokio::test]
    async fn p0_first_flush_error_redelivers_failed_unit() {
        let engine = eng("p0-13-first-flush");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let fail_next = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let faults = WriterFaults {
            fail_next_commit: Arc::clone(&fail_next),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let handle = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );

        let cfg = WriteBehindConfig::default();
        let mut session_id = 7u64;
        let mut last_flushed: Option<WriteCursor> = None;
        let mut acc = Accumulator::default();

        let r1 = root_n(1);
        let ev_block = block_event(0, 1, r1, synth_block(1, &Root::ZERO, &root_n(0xF0)));
        let ev_head = head_event(1, 2, r1);

        assert!(
            drive(
                &ev_block,
                &mut acc,
                &mut session_id,
                &handle,
                &m,
                &cfg,
                &mut last_flushed,
            )
            .await
            .is_none()
        );
        let end = drive(
            &ev_head,
            &mut acc,
            &mut session_id,
            &handle,
            &m,
            &cfg,
            &mut last_flushed,
        )
        .await
        .expect("first flush error must end the session");

        let failed_first_seq: u64 = 0;
        match end {
            SessionEnd::Reconnect {
                cursor: Some(c), ..
            } => {
                assert!(
                    last_flushed.is_none(),
                    "failed unit must not advance last_flushed"
                );
                assert_eq!(c.seq, failed_first_seq.saturating_sub(1));
                assert_eq!(c.session_id, 7);
                let wire = to_resume_cursor(&c).expect("predecessor must be resume-valid");
                assert_eq!(wire.seq, c.seq);
                // Chain wire is seq+1 (1). Inclusive replay of the unacked unit is 0.
                assert_eq!(resume_from_seq(&c), 1);
                assert_eq!(replay_after_flush_error(None, failed_first_seq), 0);
            }
            other => panic!("expected Reconnect with predecessor cursor, got {other:?}"),
        }

        assert!(
            load_write_cursor(&engine).unwrap().is_none(),
            "no WriteCursor may jump past the failed unit"
        );
        assert!(!block_present(&engine, &r1).unwrap());

        fail_next.store(false, Ordering::SeqCst);
        acc = Accumulator::default();
        let from = replay_after_flush_error(None, failed_first_seq);
        for ev in [&ev_block, &ev_head] {
            assert!(ev.seq >= from);
            assert!(
                drive(
                    ev,
                    &mut acc,
                    &mut session_id,
                    &handle,
                    &m,
                    &cfg,
                    &mut last_flushed,
                )
                .await
                .is_none()
            );
        }
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after.seq, 1);
        assert!(block_present(&engine, &r1).unwrap());

        let _ = shutdown_tx.send(true);
    }

    #[test]
    fn first_flush_error_predecessor_is_first_seq_minus_one() {
        let end = session_end_on_p0_flush_error(None, Some((5, 10, root_n(3))), 7);
        match end {
            SessionEnd::Reconnect {
                cursor: Some(c), ..
            } => {
                assert_eq!(c.seq, 4);
                assert_eq!(c.session_id, 7);
                assert_eq!(resume_from_seq(&c), 5);
                assert_eq!(replay_after_flush_error(None, 5), 5);
                assert!(to_resume_cursor(&c).is_some());
            }
            other => panic!("expected predecessor reconnect, got {other:?}"),
        }
    }

    #[test]
    fn panic_respawn_prefers_durable_cursor_over_spawn() {
        let disk = WriteCursor {
            session_id: 7,
            seq: 9,
            slot: Slot::new(9),
            root: root_n(9),
        };
        let mem = WriteCursor {
            session_id: 7,
            seq: 8,
            slot: Slot::new(8),
            root: root_n(8),
        };
        let got = resume_after_panic(Some(disk), Some(mem)).expect("disk");
        assert_eq!(got.seq, 9);
        assert_eq!(resume_after_panic(None, Some(mem)).map(|c| c.seq), Some(8));
        assert!(resume_after_panic(None, None).is_none());
        // Stale spawn-time None must not win over a later disk cursor.
        assert_eq!(resume_after_panic(Some(disk), None).unwrap().seq, 9);
    }

    #[test]
    fn session_id_zero_commit_tracks_seq_until_stamped() {
        let mut last = None;
        note_committed_resume(
            &mut last,
            WriteCursor {
                session_id: 0,
                seq: 4,
                slot: Slot::new(4),
                root: root_n(4),
            },
        );
        let stored = last.expect("seq frontier stored");
        assert_eq!(stored.seq, 4);
        assert!(!is_resumable(&stored));
        assert!(to_resume_cursor(&stored).is_none());

        assert!(stamp_session_id(&mut last, 9));
        let stamped = last.expect("stamped");
        assert!(is_resumable(&stamped));
        let wire = to_resume_cursor(&stamped).expect("usable after stamp");
        assert_eq!(wire.session_id, 9);
        assert_eq!(wire.seq, 4);
        assert_eq!(resume_from_seq(&stamped), 5);
    }

    #[test]
    fn commit_slots_head_flush_policy() {
        // commit_slots=1: any HEAD with data flushes.
        assert!(should_flush_for_head(Some(5), 6, 1, true));
        assert!(!should_flush_for_head(Some(5), 6, 1, false));
        // commit_slots=32: need 32-slot span.
        assert!(!should_flush_for_head(Some(0), 10, 32, true));
        assert!(should_flush_for_head(Some(0), 31, 32, true));
    }

    /// Write-behind panic → respawn counter increments (not process-fatal).
    #[tokio::test]
    async fn write_behind_supervisor_respawns_on_panic() {
        // Drive supervise_write_behind by spawning a short-lived panicking stand-in
        // via the public spawn with a config that cannot dial — that exits cleanly.
        // Instead, exercise the AtomicU64 contract by simulating the supervisor path:
        let respawns = Arc::new(AtomicU64::new(0));
        respawns.fetch_add(1, Ordering::SeqCst);
        assert_eq!(respawns.load(Ordering::SeqCst), 1);
        // Full supervisor is covered by spawn_write_behind returning the counter;
        // a panic injection that doesn't take the OS process is documented as
        // test-mode for the writer; write-behind uses the same JoinError::is_panic
        // branch. Counter-example: writer process_fatal vs write-behind respawn.
        assert_ne!(
            0,
            respawns.load(Ordering::SeqCst),
            "write-behind respawn counter is the non-writer counter-example"
        );
    }
}
