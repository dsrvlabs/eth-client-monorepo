//! The `forkchoiceUpdated` driver (CC-33 / Architecture §3.8).
//!
//! Lives in `chain` because the `ForkchoiceStateV1` triple is a fork-choice fact
//! and `engine` must not hold a second opinion about the head.
//!
//! ```text
//! head_root      → proto_array[head].exec_hash      → headBlockHash
//! justified.root → proto_array[justified].exec_hash → safeBlockHash
//! finalized.root → proto_array[finalized].exec_hash → finalizedBlockHash
//! ```
//!
//! All three hashes come from the proto-array payload-hash field (MP-4 / ≠13/3)
//! — never from a beacon header field.
//!
//! # Ordering
//!
//! Monotonic `sequence` per process lifetime; superseded emissions are **dropped
//! not delayed**. The engine's high-water mark (separate) resets on reconnect.
//!
//! # Per-slot floor
//!
//! Even with no new block, one fcU per slot re-points a restarted EL.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cc_fork_choice::{ProtoArray, Store};
use cc_proto::engine::ForkchoiceUpdatedRequest;
use cc_proto::engine::engine_service_client::EngineServiceClient;
use cc_types::preset::Preset;
use cc_types::primitives::{Hash256, Root, Slot};
use tokio::runtime::Handle;
use tonic::transport::Channel;

/// One `ForkchoiceStateV1` emission built from proto-array execution hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForkchoiceState {
    /// `headBlockHash`.
    pub head_block_hash: Hash256,
    /// `safeBlockHash` (justified checkpoint payload hash).
    pub safe_block_hash: Hash256,
    /// `finalizedBlockHash`.
    pub finalized_block_hash: Hash256,
    /// Head beacon-block root (for ancestor checks / logging).
    pub head_root: Root,
    /// Justified (safe) beacon-block root.
    pub safe_root: Root,
    /// Finalized beacon-block root.
    pub finalized_root: Root,
    /// Head slot (logging / -38002 context).
    pub head_slot: Slot,
    /// Monotonic emission sequence assigned by the driver.
    pub sequence: u64,
}

/// Why an emission was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FcuSkip {
    /// A newer sequence has already been assigned / is in flight.
    Superseded,
    /// Missing proto-array node for head / justified / finalized.
    MissingNode,
}

/// Build error when a required execution hash cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcuBuildError {
    /// Proto-array has no node for this root.
    UnknownRoot(Root),
}

impl std::fmt::Display for FcuBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownRoot(r) => write!(f, "proto-array missing root {r:?}"),
        }
    }
}

impl std::error::Error for FcuBuildError {}

/// Sink that records / delivers an fcU (production: gRPC; tests: recording double).
pub trait FcuSink: Send + Sync {
    /// Deliver one forkchoice update. Returns `Ok(())` when accepted by the peer.
    fn emit(&self, state: &ForkchoiceState) -> Result<(), String>;
}

/// Recording double used by the 50-concurrent and per-slot tests.
#[derive(Debug, Default)]
pub struct RecordingFcuSink {
    /// Observed emissions in arrival order.
    pub observed: std::sync::Mutex<Vec<ForkchoiceState>>,
}

impl RecordingFcuSink {
    /// Number of requests the double actually received.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.observed.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Snapshot of observed states.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ForkchoiceState> {
        self.observed.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

impl FcuSink for RecordingFcuSink {
    fn emit(&self, state: &ForkchoiceState) -> Result<(), String> {
        self.observed
            .lock()
            .map_err(|_| "recording sink poisoned".to_owned())?
            .push(*state);
        Ok(())
    }
}

/// gRPC sink over `EngineService.ForkchoiceUpdated` (production path).
#[derive(Debug)]
pub struct GrpcFcuSink {
    handle: Handle,
    client: std::sync::Mutex<Option<EngineServiceClient<Channel>>>,
    /// Serialises in-flight RPCs so concurrent emits cannot pipeline reorder.
    emit_lock: std::sync::Mutex<()>,
    uri: String,
    /// Process/session id; engine resets high-water when this changes (§3.8/2).
    session_id: u64,
}

impl GrpcFcuSink {
    /// Construct with a captured multi-threaded runtime handle (§2.4).
    ///
    /// Generates a fresh random `session_id` so a chain restart against a live
    /// engine resets the sequence high-water mark.
    #[must_use]
    pub fn new(handle: Handle, uri: impl Into<String>) -> Self {
        Self::with_session(handle, uri, random_session_id())
    }

    /// Construct with an explicit session id (tests).
    #[must_use]
    pub fn with_session(handle: Handle, uri: impl Into<String>, session_id: u64) -> Self {
        Self {
            handle,
            client: std::sync::Mutex::new(None),
            emit_lock: std::sync::Mutex::new(()),
            uri: uri.into(),
            session_id,
        }
    }

    /// Session id stamped on every request.
    #[must_use]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    fn client_blocking(&self) -> Result<EngineServiceClient<Channel>, String> {
        let mut guard = self
            .client
            .lock()
            .map_err(|_| "fcu client mutex poisoned".to_owned())?;
        if guard.is_none() {
            let uri = self.uri.clone();
            let c = self
                .handle
                .block_on(async { EngineServiceClient::connect(uri).await })
                .map_err(|e| format!("engine connect: {e}"))?;
            *guard = Some(c);
        }
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| "engine client missing after connect".to_owned())
    }
}

impl FcuSink for GrpcFcuSink {
    fn emit(&self, state: &ForkchoiceState) -> Result<(), String> {
        let _emit = self
            .emit_lock
            .lock()
            .map_err(|_| "fcu emit lock poisoned".to_owned())?;
        let mut client = self.client_blocking()?;
        let req = ForkchoiceUpdatedRequest {
            head_block_hash: state.head_block_hash.as_slice().to_vec(),
            safe_block_hash: state.safe_block_hash.as_slice().to_vec(),
            finalized_block_hash: state.finalized_block_hash.as_slice().to_vec(),
            sequence: state.sequence,
            session_id: self.session_id,
            head_slot: state.head_slot.as_u64(),
        };
        self.handle
            .block_on(async { client.forkchoice_updated(req).await })
            .map_err(|e| {
                let msg = e.message().to_string();
                if msg.contains("FCU_DROPPED_STALE") {
                    format!("ForkchoiceUpdated dropped stale: {msg}")
                } else {
                    format!("ForkchoiceUpdated: {e}")
                }
            })?;
        Ok(())
    }
}

fn random_session_id() -> u64 {
    // Never zero: engine treats 0 as "unspecified" (no session-change reset).
    getrandom::u64().unwrap_or(0xC33C_33C3_u64) | 1
}

/// fcU driver: builds the triple, assigns sequences, drops superseded, floor.
#[derive(Debug)]
pub struct FcuDriver<S: FcuSink> {
    sink: Arc<S>,
    /// Next sequence to assign (starts at 1).
    next_sequence: AtomicU64,
    /// Highest sequence assigned so far (for supersession checks).
    latest_assigned: AtomicU64,
    /// Count of emissions dropped because a newer sequence superseded them.
    dropped_stale: AtomicU64,
    /// Single-flight: check+emit under one lock so concurrent heads cannot reorder.
    emit_mu: std::sync::Mutex<()>,
    /// Last successfully built state (per-slot floor re-sends this).
    last_state: std::sync::Mutex<Option<ForkchoiceState>>,
    /// Last slot for which the floor already emitted (avoid double-floor).
    last_floor_slot: AtomicU64,
}

impl<S: FcuSink> FcuDriver<S> {
    /// Construct a driver over `sink`.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self {
            sink,
            next_sequence: AtomicU64::new(1),
            latest_assigned: AtomicU64::new(0),
            dropped_stale: AtomicU64::new(0),
            emit_mu: std::sync::Mutex::new(()),
            last_state: std::sync::Mutex::new(None),
            last_floor_slot: AtomicU64::new(u64::MAX),
        }
    }

    /// Number of superseded emissions dropped (mirrors engine counter for tests).
    #[must_use]
    pub fn dropped_stale_total(&self) -> u64 {
        self.dropped_stale.load(Ordering::SeqCst)
    }

    /// Shared sink handle.
    #[must_use]
    pub fn sink(&self) -> Arc<S> {
        Arc::clone(&self.sink)
    }

    /// Build the triple from a [`Store`]'s proto-array + checkpoints.
    pub fn build_from_store<P: Preset>(
        &self,
        store: &Store<P>,
        head_root: Root,
    ) -> Result<ForkchoiceState, FcuBuildError> {
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        build_forkchoice_state(
            store.proto_array(),
            head_root,
            justified.root,
            finalized.root,
            self.alloc_sequence(),
        )
    }

    /// Build the triple from an explicit [`ProtoArray`] (unit tests / harnesses).
    pub fn build_from_proto_array(
        &self,
        proto_array: &ProtoArray,
        head_root: Root,
        justified_root: Root,
        finalized_root: Root,
    ) -> Result<ForkchoiceState, FcuBuildError> {
        build_forkchoice_state(
            proto_array,
            head_root,
            justified_root,
            finalized_root,
            self.alloc_sequence(),
        )
    }

    /// Assign the next monotonic sequence (process lifetime).
    fn alloc_sequence(&self) -> u64 {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        // Track the highest assigned so concurrent emits can drop superseded.
        let mut cur = self.latest_assigned.load(Ordering::SeqCst);
        while seq > cur {
            match self.latest_assigned.compare_exchange_weak(
                cur,
                seq,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
        seq
    }

    /// Emit `state` if it has not been superseded; otherwise drop and count.
    ///
    /// Single-flight: the supersession check and sink delivery run under one
    /// mutex so two concurrent emits cannot reorder at the sink (CC-33 F3).
    ///
    /// Returns `Ok(true)` if delivered, `Ok(false)` if dropped as stale.
    pub fn try_emit(&self, state: ForkchoiceState) -> Result<bool, String> {
        let _guard = self
            .emit_mu
            .lock()
            .map_err(|_| "fcu emit_mu poisoned".to_owned())?;
        // Drop if a newer sequence has been assigned (concurrent head updates).
        let latest = self.latest_assigned.load(Ordering::SeqCst);
        if state.sequence < latest {
            self.dropped_stale.fetch_add(1, Ordering::SeqCst);
            return Ok(false);
        }
        self.sink.emit(&state)?;
        if let Ok(mut guard) = self.last_state.lock() {
            *guard = Some(state);
        }
        Ok(true)
    }

    /// Build + emit for a new head. Off the attestation path (call after import).
    pub fn on_head_update<P: Preset>(
        &self,
        store: &Store<P>,
        head_root: Root,
    ) -> Result<bool, String> {
        let state = self
            .build_from_store(store, head_root)
            .map_err(|e| e.to_string())?;
        debug_assert!(
            safe_is_ancestor_of_head(store.proto_array(), state.safe_root, state.head_root),
            "safe must equal or be an ancestor of head (else -38002)"
        );
        self.try_emit(state)
    }

    /// Per-slot floor: re-send the last triple once per `slot` when no new head.
    ///
    /// Returns `Ok(true)` if a floor emission was delivered.
    pub fn on_slot(&self, slot: Slot) -> Result<bool, String> {
        let slot_u = slot.as_u64();
        let prev = self.last_floor_slot.load(Ordering::SeqCst);
        if prev == slot_u {
            return Ok(false);
        }
        // CAS so concurrent slot ticks only fire once.
        if self
            .last_floor_slot
            .compare_exchange(prev, slot_u, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Another caller advanced the floor; if it is this slot, we are done.
            if self.last_floor_slot.load(Ordering::SeqCst) == slot_u {
                return Ok(false);
            }
        }

        let base = {
            let guard = self
                .last_state
                .lock()
                .map_err(|_| "last_state poisoned".to_owned())?;
            *guard
        };
        let Some(mut state) = base else {
            return Ok(false);
        };
        // Fresh sequence for the floor re-send.
        state.sequence = self.alloc_sequence();
        self.try_emit(state)
    }
}

/// Build `ForkchoiceState` from three roots' payload-hash fields.
///
/// **MP-4:** the only source of the three hashes is the proto-array field
/// (three reads — head, safe, finalized).
pub fn build_forkchoice_state(
    proto_array: &ProtoArray,
    head_root: Root,
    justified_root: Root,
    finalized_root: Root,
    sequence: u64,
) -> Result<ForkchoiceState, FcuBuildError> {
    let head = proto_array
        .get(&head_root)
        .ok_or(FcuBuildError::UnknownRoot(head_root))?;
    let safe = proto_array
        .get(&justified_root)
        .ok_or(FcuBuildError::UnknownRoot(justified_root))?;
    let finalized = proto_array
        .get(&finalized_root)
        .ok_or(FcuBuildError::UnknownRoot(finalized_root))?;

    Ok(ForkchoiceState {
        head_block_hash: head.execution_block_hash,
        safe_block_hash: safe.execution_block_hash,
        finalized_block_hash: finalized.execution_block_hash,
        head_root,
        safe_root: justified_root,
        finalized_root,
        head_slot: head.slot,
        sequence,
    })
}

/// `safe` equals `head` or is an ancestor of `head` on the proto-array.
#[must_use]
pub fn safe_is_ancestor_of_head(
    proto_array: &ProtoArray,
    safe_root: Root,
    head_root: Root,
) -> bool {
    if safe_root == head_root {
        return true;
    }
    if proto_array.get(&safe_root).is_none() {
        return false;
    }
    // Walk parents from head until we hit safe or run out.
    let mut cur = head_root;
    loop {
        if cur == safe_root {
            return true;
        }
        let Some(node) = proto_array.get(&cur) else {
            return false;
        };
        match node.parent {
            Some(p_idx) => {
                let parent = &proto_array.nodes()[p_idx];
                cur = parent.root;
            }
            None => return false,
        }
    }
}
