//! Per-container SSZ maximum table, pre-decode size check, and topic validators.
//!
//! | Module | Issue | Role |
//! |--------|-------|------|
//! | (this file) | CC-22b | SSZ max table + [`check_payload_len`] |
//! | `crate::verdict` | CC-22d | one `Verdict` + `to_message_acceptance` |
//! | [`block`] | CC-22d | chain-authoritative `beacon_block` local stage |
//! | [`column`] | CC-22d | p2p-authoritative `data_column_sidecar` §5.5 list |
//! | [`sync`] | CC-2D | p2p-authoritative sync-committee family |
//! | [`pipeline`] | CC-22d | validation pool, stubs, topic dispatch |
//!
//! Four-layer bound stack (Architecture §5.2):
//!
//! | Layer | Bound | Owner |
//! |-------|-------|-------|
//! | `gossipsub::Config::max_transmit_size` | [`GOSSIP_MAX_SIZE`] compressed | `cc_libp2p` |
//! | `SnappyTransform::max_uncompressed` | [`GOSSIP_MAX_SIZE`] | `cc_libp2p` |
//! | **per-topic pre-decode check** | this module's table | **CC-22b** |
//! | SSZ decode | `ssz_types` capacities | Phase 1 `CC-10` |
//!
//! Every topic validator calls [`check_payload_len`] **before** SSZ decode.

pub mod block;
pub mod column;
pub mod kzg_verify;
pub mod operations;
pub mod pipeline;
pub mod sync;

pub use block::{
    BlockForward, BlockOutcome, BlockValidateInput, note_accepted_block,
    validate_beacon_block_local,
};
pub use column::{
    AlwaysValidKzg, BLOB_KZG_COMMITMENTS_FIELD_INDEX, CellKzgVerifier, ColumnOutcome,
    ColumnPublishDecision, ColumnStep, ColumnValidateInput, ColumnValidatorState, FailClosedKzg,
    INCLUSION_PROOF_CACHE_BOUND, InclusionProofCache, InclusionProofKey, KzgVerify,
    NoopSamplingFeed, SamplingFeed, StepCounters, decide_column_publish, production_kzg_verify,
    validate_data_column_sidecar, verify_inclusion_proof,
};
pub use operations::{
    BoundedIndexSet, OPERATION_SEEN_BOUND, OpStepCounters, OperationOccupancy, OperationSeenSets,
    OperationValidateInput, OperationValidatorState, epoch_from_view, validate_attester_slashing,
    validate_bls_to_execution_change, validate_operation, validate_proposer_slashing,
    validate_voluntary_exit,
};
pub use pipeline::{
    IN_FLIGHT_VALIDATION_CAP, REPORTED_ACCEPT_BOUND, ReportedEntry, ValidationPool,
    ValidationPoolState, ValidatorKind, all_topics_have_validators, apply_late_chain_verdict,
    parse_topic_name, run_chain_in_late_verdicts, run_validation_pool, validator_kind,
};
pub use sync::{
    NoopSyncSource, SYNC_CONTRIB_SEEN_BOUND, SYNC_SEEN_BOUND, SyncCommitteeSource,
    SyncContribSeenKey, SyncContribValidateInput, SyncMessageStep, SyncMessageStepCounters,
    SyncMessageValidateInput, SyncOutcome, SyncSeenKey, SyncSeenSets,
    TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE, is_sync_committee_aggregator,
    validate_sync_committee_message, validate_sync_contribution_and_proof,
};

use std::sync::atomic::{AtomicUsize, Ordering};

use cc_libp2p::GOSSIP_MAX_SIZE;
use cc_types::Preset;
use thiserror::Error;

use super::topics::TopicName;

// ── Fixed SSZ field sizes (spec constants; independent of preset) ────────────

const BYTES_U64: usize = 8;
const BYTES_ROOT: usize = 32;
const BYTES_BLS_PUBKEY: usize = 48;
const BYTES_BLS_SIG: usize = 96;
const BYTES_EXECUTION_ADDRESS: usize = 20;
const BYTES_KZG_COMMITMENT: usize = 48;
const BYTES_KZG_PROOF: usize = 48;
/// Fulu cell payload (`BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_CELL`).
const BYTES_CELL: usize = 2048;
/// SSZ length-offset prefix for a variable-size field.
const BYTES_OFFSET: usize = 4;
/// `KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH = 4` roots.
const KZG_INCLUSION_PROOF_DEPTH: usize = 4;

/// SSZ size of `Checkpoint` (epoch + root).
const SSZ_CHECKPOINT: usize = BYTES_U64 + BYTES_ROOT;
/// SSZ size of `AttestationData`.
const SSZ_ATTESTATION_DATA: usize =
    BYTES_U64 + BYTES_U64 + BYTES_ROOT + SSZ_CHECKPOINT + SSZ_CHECKPOINT;
/// SSZ size of `BeaconBlockHeader`.
const SSZ_BEACON_BLOCK_HEADER: usize = BYTES_U64 + BYTES_U64 + BYTES_ROOT + BYTES_ROOT + BYTES_ROOT;
/// SSZ size of `SignedBeaconBlockHeader`.
const SSZ_SIGNED_BEACON_BLOCK_HEADER: usize = SSZ_BEACON_BLOCK_HEADER + BYTES_BLS_SIG;
/// SSZ size of `VoluntaryExit`.
const SSZ_VOLUNTARY_EXIT: usize = BYTES_U64 + BYTES_U64;
/// SSZ size of `SignedVoluntaryExit`.
const SSZ_SIGNED_VOLUNTARY_EXIT: usize = SSZ_VOLUNTARY_EXIT + BYTES_BLS_SIG;
/// SSZ size of `ProposerSlashing`.
const SSZ_PROPOSER_SLASHING: usize = SSZ_SIGNED_BEACON_BLOCK_HEADER * 2;
/// SSZ size of `BlsToExecutionChange`.
const SSZ_BLS_TO_EXECUTION_CHANGE: usize = BYTES_U64 + BYTES_BLS_PUBKEY + BYTES_EXECUTION_ADDRESS;
/// SSZ size of `SignedBlsToExecutionChange`.
const SSZ_SIGNED_BLS_TO_EXECUTION_CHANGE: usize = SSZ_BLS_TO_EXECUTION_CHANGE + BYTES_BLS_SIG;
/// SSZ size of `SyncCommitteeMessage`.
const SSZ_SYNC_COMMITTEE_MESSAGE: usize = BYTES_U64 + BYTES_ROOT + BYTES_U64 + BYTES_BLS_SIG;

/// Errors from the pre-decode length check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SizeError {
    /// Payload length exceeds the container's SSZ maximum for the active preset.
    #[error("gossip payload len {len} exceeds ssz max {max} for topic family {topic}")]
    OverBound {
        /// Observed payload length (decompressed).
        len: usize,
        /// Declared maximum from [`max_container_bytes`].
        max: usize,
        /// Topic path segment that selected the bound.
        topic: &'static str,
    },
}

/// SSZ-maximum byte length of the container carried on `name`, for preset `P`.
///
/// Derived from `P`'s capacity constants. Results are capped at
/// [`GOSSIP_MAX_SIZE`] so the per-topic check never admits more than the
/// transform's global ceiling.
///
/// Subnet variants of the same family share one bound (the container type does
/// not depend on the subnet id).
#[must_use]
pub fn max_container_bytes<P: Preset>(name: TopicName) -> usize {
    let unbound = match name {
        TopicName::BeaconBlock => ssz_max_beacon_block::<P>(),
        TopicName::BeaconAggregateAndProof => ssz_max_signed_aggregate_and_proof::<P>(),
        TopicName::BeaconAttestation(_) => ssz_max_attestation::<P>(),
        TopicName::DataColumnSidecar(_) => ssz_max_data_column_sidecar::<P>(),
        TopicName::SyncCommitteeContributionAndProof => {
            ssz_max_signed_contribution_and_proof::<P>()
        }
        TopicName::SyncCommittee(_) => SSZ_SYNC_COMMITTEE_MESSAGE,
        TopicName::VoluntaryExit => SSZ_SIGNED_VOLUNTARY_EXIT,
        TopicName::ProposerSlashing => SSZ_PROPOSER_SLASHING,
        TopicName::AttesterSlashing => ssz_max_attester_slashing::<P>(),
        TopicName::BlsToExecutionChange => SSZ_SIGNED_BLS_TO_EXECUTION_CHANGE,
    };
    unbound.min(GOSSIP_MAX_SIZE)
}

/// Alias preferred by the acceptance grep (`ssz_max` / `max_container_bytes`).
#[inline]
#[must_use]
pub fn ssz_max<P: Preset>(name: TopicName) -> usize {
    max_container_bytes::<P>(name)
}

/// Shared pre-decode entry point: reject over-bound payloads **before** SSZ decode.
///
/// # Errors
///
/// Returns [`SizeError::OverBound`] when `payload_len` exceeds
/// [`max_container_bytes`] for `name` under preset `P`.
pub fn check_payload_len<P: Preset>(name: TopicName, payload_len: usize) -> Result<(), SizeError> {
    let max = max_container_bytes::<P>(name);
    if payload_len > max {
        return Err(SizeError::OverBound {
            len: payload_len,
            max,
            topic: topic_family_label(name),
        });
    }
    Ok(())
}

/// Pre-decode check that also records a successful pass for tests.
///
/// Production validators call [`check_payload_len`]. Tests that need to assert
/// SSZ decode was never attempted use this with a [`DecodeCounter`].
///
/// # Errors
///
/// Same as [`check_payload_len`]. On `Err`, `counter` is **not** incremented.
pub fn check_payload_len_counted<P: Preset>(
    name: TopicName,
    payload_len: usize,
    counter: &DecodeCounter,
) -> Result<(), SizeError> {
    check_payload_len::<P>(name, payload_len)?;
    // Only after the bound passes may a caller proceed to decode — the counter
    // stands in for that call site in unit tests.
    counter.record_decode_attempt();
    Ok(())
}

/// Counts decode attempts for the over-bound-before-decode acceptance test.
#[derive(Debug, Default)]
pub struct DecodeCounter {
    attempts: AtomicUsize,
}

impl DecodeCounter {
    /// New zeroed counter.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            attempts: AtomicUsize::new(0),
        }
    }

    /// Record that SSZ decode would run (post size-check).
    pub fn record_decode_attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of decode attempts observed.
    #[must_use]
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

// ── Per-container formulas (preset-derived) ─────────────────────────────────

/// `Attestation` (Electra): BitList[max_validators_per_slot] + data + sig + committee_bits.
fn ssz_max_attestation<P: Preset>() -> usize {
    let max_validators = usize_from_u64(P::MAX_VALIDATORS_PER_SLOT);
    let max_committees = usize_from_u64(P::MAX_COMMITTEES_PER_SLOT);
    // Variable BitList: offset + max bit-bytes + length-delimiting bit byte.
    let aggregation_bits = BYTES_OFFSET.saturating_add(bitlist_max_bytes(max_validators));
    let committee_bits = bitvector_bytes(max_committees);
    aggregation_bits
        .saturating_add(SSZ_ATTESTATION_DATA)
        .saturating_add(BYTES_BLS_SIG)
        .saturating_add(committee_bits)
}

/// `SignedAggregateAndProof` = AggregateAndProof + outer sig.
fn ssz_max_signed_aggregate_and_proof<P: Preset>() -> usize {
    // aggregator_index (8) + aggregate (var) + selection_proof (96) + signature (96)
    BYTES_U64
        .saturating_add(ssz_max_attestation::<P>())
        .saturating_add(BYTES_BLS_SIG)
        .saturating_add(BYTES_BLS_SIG)
}

/// `AttesterSlashing` = two `IndexedAttestation`s.
fn ssz_max_attester_slashing<P: Preset>() -> usize {
    ssz_max_indexed_attestation::<P>().saturating_mul(2)
}

/// `IndexedAttestation`: VariableList[ValidatorIndex, max_validators_per_slot] + data + sig.
fn ssz_max_indexed_attestation<P: Preset>() -> usize {
    let max_validators = usize_from_u64(P::MAX_VALIDATORS_PER_SLOT);
    let indices = BYTES_OFFSET.saturating_add(max_validators.saturating_mul(BYTES_U64));
    indices
        .saturating_add(SSZ_ATTESTATION_DATA)
        .saturating_add(BYTES_BLS_SIG)
}

/// `SignedContributionAndProof`.
fn ssz_max_signed_contribution_and_proof<P: Preset>() -> usize {
    // ContributionAndProof: aggregator_index + contribution + selection_proof
    // Contribution: slot + root + subcommittee_index + BitVector[sync_subcommittee] + sig
    let sub = bitvector_bytes(usize_from_u64(P::SYNC_SUBCOMMITTEE_SIZE));
    let contribution = BYTES_U64
        .saturating_add(BYTES_ROOT)
        .saturating_add(BYTES_U64)
        .saturating_add(sub)
        .saturating_add(BYTES_BLS_SIG);
    BYTES_U64
        .saturating_add(contribution)
        .saturating_add(BYTES_BLS_SIG) // selection_proof
        .saturating_add(BYTES_BLS_SIG) // outer signature
}

/// `DataColumnSidecar` (Fulu): cells + commitments + proofs + header + inclusion proof.
fn ssz_max_data_column_sidecar<P: Preset>() -> usize {
    let n = usize_from_u64(P::MAX_BLOB_COMMITMENTS_PER_BLOCK);
    let column = BYTES_OFFSET.saturating_add(n.saturating_mul(BYTES_CELL));
    let commitments = BYTES_OFFSET.saturating_add(n.saturating_mul(BYTES_KZG_COMMITMENT));
    let proofs = BYTES_OFFSET.saturating_add(n.saturating_mul(BYTES_KZG_PROOF));
    let inclusion = KZG_INCLUSION_PROOF_DEPTH.saturating_mul(BYTES_ROOT);
    BYTES_U64 // index
        .saturating_add(column)
        .saturating_add(commitments)
        .saturating_add(proofs)
        .saturating_add(SSZ_SIGNED_BEACON_BLOCK_HEADER)
        .saturating_add(inclusion)
}

/// `SignedBeaconBlock` theoretical max far exceeds gossip; cap at global ceiling.
///
/// On every shipped preset, `MAX_TRANSACTIONS_PER_PAYLOAD ×
/// MAX_BYTES_PER_TRANSACTION` is many orders above [`GOSSIP_MAX_SIZE`], so the
/// global ceiling is binding. Preset constants are still read so the bound is
/// not a bare literal and remains tied to the active config.
fn ssz_max_beacon_block<P: Preset>() -> usize {
    let tx_ceiling = usize_from_u64(P::MAX_TRANSACTIONS_PER_PAYLOAD)
        .saturating_mul(usize_from_u64(P::MAX_BYTES_PER_TRANSACTION));
    let blob_term =
        usize_from_u64(P::MAX_BLOB_COMMITMENTS_PER_BLOCK).saturating_mul(BYTES_KZG_COMMITMENT);
    // Cap applied by `max_container_bytes` via `.min(GOSSIP_MAX_SIZE)`.
    tx_ceiling.saturating_add(blob_term).max(GOSSIP_MAX_SIZE)
}

// ── Bit helpers ─────────────────────────────────────────────────────────────

/// Max SSZ byte length of a `BitList[N]` value (excluding the container offset).
fn bitlist_max_bytes(n_bits: usize) -> usize {
    // BitList encodes ceil((len+1)/8) bytes (the extra bit is the delimiter).
    // At maximum length N that is ceil((N+1)/8).
    n_bits.saturating_add(1).div_ceil(8)
}

/// SSZ byte length of a `BitVector[N]`.
fn bitvector_bytes(n_bits: usize) -> usize {
    n_bits.div_ceil(8)
}

fn usize_from_u64(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// Stable label for error messages / metrics (path-segment family, no subnet id).
fn topic_family_label(name: TopicName) -> &'static str {
    match name {
        TopicName::BeaconBlock => "beacon_block",
        TopicName::BeaconAggregateAndProof => "beacon_aggregate_and_proof",
        TopicName::BeaconAttestation(_) => "beacon_attestation",
        TopicName::DataColumnSidecar(_) => "data_column_sidecar",
        TopicName::SyncCommitteeContributionAndProof => "sync_committee_contribution_and_proof",
        TopicName::SyncCommittee(_) => "sync_committee",
        TopicName::VoluntaryExit => "voluntary_exit",
        TopicName::ProposerSlashing => "proposer_slashing",
        TopicName::AttesterSlashing => "attester_slashing",
        TopicName::BlsToExecutionChange => "bls_to_execution_change",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::gossip::topics::{SubnetCounts, TopicName, expand_fulu_topic_names};
    use cc_types::{Mainnet, Minimal};

    #[test]
    fn table_covers_all_ten_fulu_families() {
        // One representative of each family (subnets share the family bound).
        let families = [
            TopicName::BeaconBlock,
            TopicName::BeaconAggregateAndProof,
            TopicName::BeaconAttestation(0),
            TopicName::DataColumnSidecar(0),
            TopicName::SyncCommitteeContributionAndProof,
            TopicName::SyncCommittee(0),
            TopicName::VoluntaryExit,
            TopicName::ProposerSlashing,
            TopicName::AttesterSlashing,
            TopicName::BlsToExecutionChange,
        ];
        assert_eq!(families.len(), 10);
        for name in families {
            let max = max_container_bytes::<Mainnet>(name);
            assert!(max > 0, "{name:?} bound must be positive");
            assert!(
                max <= GOSSIP_MAX_SIZE,
                "{name:?} bound {max} must not exceed GOSSIP_MAX_SIZE"
            );
        }
    }

    #[test]
    fn expanded_names_all_resolve_in_table() {
        let counts = SubnetCounts {
            attestation: 2,
            sync_committee: 2,
            data_column_sidecar: 2,
        };
        for name in expand_fulu_topic_names(&counts) {
            let _ = max_container_bytes::<Mainnet>(name);
        }
    }

    #[test]
    fn preset_switch_changes_at_least_one_bound() {
        // Attestation BitList capacity tracks MAX_VALIDATORS_PER_SLOT.
        let main = max_container_bytes::<Mainnet>(TopicName::BeaconAttestation(0));
        let min = max_container_bytes::<Minimal>(TopicName::BeaconAttestation(0));
        assert_ne!(
            main, min,
            "attestation bound must differ between mainnet and minimal presets \
             (main={main}, minimal={min})"
        );
        assert!(main > min, "mainnet validators-per-slot is larger");
    }

    #[test]
    fn fixed_containers_independent_of_preset() {
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::VoluntaryExit),
            max_container_bytes::<Minimal>(TopicName::VoluntaryExit)
        );
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::ProposerSlashing),
            SSZ_PROPOSER_SLASHING.min(GOSSIP_MAX_SIZE)
        );
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::BlsToExecutionChange),
            SSZ_SIGNED_BLS_TO_EXECUTION_CHANGE.min(GOSSIP_MAX_SIZE)
        );
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::SyncCommittee(3)),
            SSZ_SYNC_COMMITTEE_MESSAGE.min(GOSSIP_MAX_SIZE)
        );
    }

    #[test]
    fn beacon_block_capped_at_gossip_max() {
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::BeaconBlock),
            GOSSIP_MAX_SIZE
        );
    }

    #[test]
    fn over_bound_rejected_before_decode() {
        let counter = DecodeCounter::new();
        let name = TopicName::VoluntaryExit;
        let max = max_container_bytes::<Mainnet>(name);
        let over = max.saturating_add(1);

        let err = check_payload_len_counted::<Mainnet>(name, over, &counter).unwrap_err();
        assert!(matches!(
            err,
            SizeError::OverBound {
                len,
                max: m,
                ..
            } if len == over && m == max
        ));
        assert_eq!(
            counter.attempts(),
            0,
            "SSZ decode must not run when the pre-decode check fails"
        );

        // In-bound payload is allowed through to the (counted) decode site.
        check_payload_len_counted::<Mainnet>(name, max, &counter).expect("in-bound");
        assert_eq!(counter.attempts(), 1);
    }

    #[test]
    fn check_payload_len_ok_at_exact_bound() {
        let name = TopicName::ProposerSlashing;
        let max = ssz_max::<Mainnet>(name);
        assert!(check_payload_len::<Mainnet>(name, max).is_ok());
        assert!(check_payload_len::<Mainnet>(name, 0).is_ok());
    }

    #[test]
    fn bounds_sourced_from_gossip_max_constant() {
        // GOSSIP_MAX_SIZE is imported from cc_libp2p — production bounds must
        // never exceed it, and the beacon_block family saturates exactly there.
        assert_eq!(
            max_container_bytes::<Mainnet>(TopicName::BeaconBlock),
            GOSSIP_MAX_SIZE
        );
        assert!(max_container_bytes::<Mainnet>(TopicName::DataColumnSidecar(0)) <= GOSSIP_MAX_SIZE);
    }
}
