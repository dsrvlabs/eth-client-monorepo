//! Direct `cc-engine-api` seam (S1-A-06). Replaces `engine_client.rs`.
//!
//! E3 is a function call from the consensus-core OS thread with an explicit
//! [`Duration`]. Timeouts map to [`cc_state_transition::EngineError::Transport`]
//! so `on_block` parks the import in [`crate::pending_engine`] (ADR-P3-05).

use std::sync::Arc;
use std::time::Duration;

use cc_engine_api::EngineApi;
use cc_engine_api::config::TransportTimeouts;
use cc_engine_api::errors::EngineError as ApiError;
use cc_engine_api::fastpath::sidecars::SidecarTemplate;
use cc_engine_api::methods::new_payload::DecodedPayloadStatus;
use cc_engine_api::metrics::PayloadStatus as WireStatus;
use cc_state_transition::error::EngineError;
use cc_state_transition::{
    ExecutionEngine, NewPayloadRequest, PayloadStatus, get_execution_requests_list,
};
use cc_types::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH;
use cc_types::containers::SignedBeaconBlockHeader;
use cc_types::preset::Preset;
use cc_types::primitives::{Hash256, KzgCommitment, Root};
use ssz::{Decode, Encode};

use crate::da::BlockBranchTrigger;
use crate::fcu_driver::{FcuSink, ForkchoiceState};

/// Default `NewPayload` deadline. Matches engine `TransportTimeouts::new_payload` (8 s).
pub const DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT: Duration = Duration::from_millis(8_000);

/// Default `GetEngineState` / poll deadline.
pub const DEFAULT_ENGINE_GET_STATE_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Default `FetchBlobs` deadline (accelerator; matches engine getBlobs 1 s).
pub const DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Default `ForkchoiceUpdated` deadline. Matches engine `TransportTimeouts`.
pub const DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT: Duration = Duration::from_millis(8_000);

/// In-process engine used by the core thread, restore, and checkpoint seed.
#[derive(Clone)]
pub struct DirectEngine {
    api: EngineApi,
    timeouts: TransportTimeouts,
    session_id: u64,
}

impl std::fmt::Debug for DirectEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectEngine")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl DirectEngine {
    /// Wrap a constructed [`EngineApi`] with explicit per-call deadlines.
    #[must_use]
    pub fn new(api: EngineApi, timeouts: TransportTimeouts) -> Self {
        Self {
            api,
            timeouts,
            session_id: random_session_id(),
        }
    }

    /// Wrap with default EL transport deadlines (8 s / 1 s).
    #[must_use]
    pub fn with_default_timeouts(api: EngineApi) -> Self {
        Self::new(api, TransportTimeouts::from_knobs(&Default::default()))
    }

    /// Override deadlines (injected-timeout tests).
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: TransportTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Deadlines applied to every core-thread call.
    #[must_use]
    pub fn timeouts(&self) -> &TransportTimeouts {
        &self.timeouts
    }

    /// Session id stamped on every fcU.
    #[must_use]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Shared API host.
    #[must_use]
    pub fn api(&self) -> &EngineApi {
        &self.api
    }

    /// Local health snapshot. Transport/timeout → offline (fail-closed).
    #[must_use]
    pub fn is_online(&self) -> bool {
        self.api.is_online(self.timeouts.eth_syncing)
    }

    /// Fire-and-forget block-branch `getBlobs` (template-sized only).
    pub fn fetch_blobs(&self, trigger: &BlockBranchTrigger) {
        let Some(template) = template_from_trigger(trigger) else {
            tracing::debug!("FetchBlobs: malformed template; skipped");
            return;
        };
        self.api.fetch_blobs(
            template,
            trigger.beacon_block_root,
            trigger.slot,
            self.timeouts.get_blobs,
        );
    }
}

impl<P: Preset> ExecutionEngine<P> for DirectEngine {
    fn verify_and_notify_new_payload(
        &self,
        request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        let ssz = request.execution_payload.as_ssz_bytes();
        let versioned_hashes: Vec<Vec<u8>> = request
            .versioned_hashes
            .iter()
            .map(|h| h.as_slice().to_vec())
            .collect();
        let parent_beacon_block_root = request.parent_beacon_block_root.as_slice().to_vec();
        let execution_requests = get_execution_requests_list(request.execution_requests);

        match self.api.new_payload(
            &ssz,
            &versioned_hashes,
            &parent_beacon_block_root,
            &execution_requests,
            self.timeouts.new_payload,
        ) {
            Ok(status) => map_decoded_status(&status),
            Err(e) => Err(map_api_error(e)),
        }
    }
}

impl FcuSink for DirectEngine {
    fn emit(&self, state: &ForkchoiceState) -> Result<(), String> {
        let head_slot = if state.head_slot.as_u64() == 0 {
            None
        } else {
            Some(state.head_slot.as_u64())
        };
        self.api
            .forkchoice_updated(
                state.sequence,
                self.session_id,
                state.head_block_hash.as_slice(),
                state.safe_block_hash.as_slice(),
                state.finalized_block_hash.as_slice(),
                head_slot,
                self.timeouts.forkchoice_updated,
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

fn template_from_trigger(trigger: &BlockBranchTrigger) -> Option<SidecarTemplate> {
    let header = SignedBeaconBlockHeader::from_ssz_bytes(&trigger.signed_block_header_ssz).ok()?;
    let kzg_commitments: Vec<KzgCommitment> = trigger
        .kzg_commitments
        .iter()
        .map(|c| KzgCommitment::from_array(*c))
        .collect();
    if kzg_commitments.is_empty() {
        return None;
    }
    let mut proof = [Root::default(); KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize];
    for (i, p) in trigger.kzg_commitments_inclusion_proof.iter().enumerate() {
        proof[i] = Root::from_array(*p);
    }
    Some(SidecarTemplate::new(header, kzg_commitments, proof))
}

fn map_decoded_status(status: &DecodedPayloadStatus) -> Result<PayloadStatus, EngineError> {
    let latest = status.latest_valid_hash.map(Hash256::from);
    match status.status {
        WireStatus::Valid => Ok(PayloadStatus::Valid),
        WireStatus::Invalid => Ok(PayloadStatus::Invalid {
            latest_valid_hash: latest,
        }),
        WireStatus::Syncing => Ok(PayloadStatus::Syncing),
        WireStatus::Accepted => Ok(PayloadStatus::Accepted),
        WireStatus::InvalidBlockHash => Ok(PayloadStatus::InvalidBlockHash),
    }
}

fn map_api_error(err: ApiError) -> EngineError {
    EngineError::Transport(err.to_string())
}

fn random_session_id() -> u64 {
    getrandom::u64().unwrap_or(0xC33C_33C3_u64) | 1
}

/// Shared handle stored on [`crate::core::CoreConfig`].
pub type SharedEngine = Arc<DirectEngine>;
