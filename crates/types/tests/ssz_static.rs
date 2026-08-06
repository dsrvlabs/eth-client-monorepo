//! Fulu `ssz_static` runner (CC-10e / Architecture §10.2–§10.3).
//!
//! - One `#[test]` per `(preset, container)` via [`spec_suite!`].
//! - Each case asserts SSZ re-encode byte-equality **and** `hash_tree_root`
//!   against `roots.yaml`.
//! - [`container_coverage`] asserts set equality both directions for both
//!   presets against the on-disk listing resolved through `cc-spec-tests`
//!   (paths come from [`Vectors::handlers`] / [`Vectors::cases`], not literals).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;

use cc_spec_tests::{Vectors, assert_handler_coverage};
use cc_types::ForkName;
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Root, parse_hex_bytes};
use cc_types::registry::{SSZ_STATIC_TYPE_NAMES, ssz_static_handler, ssz_static_types};
use serde::Deserialize;

/// Active fork directory segment under each preset's vector tree.
const FORK: &str = "fulu";

/// Runner name discovered on disk via [`Vectors::handlers`] / [`Vectors::cases`].
const RUNNER: &str = "ssz_static";

#[derive(Debug, Deserialize)]
struct RootsYaml {
    root: String,
}

fn parse_root(hex: &str) -> Root {
    let bytes = parse_hex_bytes::<32>(hex).unwrap_or_else(|e| panic!("bad root hex {hex}: {e}"));
    Root::from_array(bytes)
}

fn run_container_cases<P: Preset>(container: &str) {
    let vectors = Vectors::open()
        .expect("spec vector cache must be present; run scripts/fetch-spec-vectors.sh");
    let handler = ssz_static_handler::<P>(container)
        .unwrap_or_else(|| panic!("no handler registered for {container}"));
    let cases = vectors
        .cases(P::NAME, FORK, RUNNER, container)
        .unwrap_or_else(|e| panic!("enumerate cases {container}/{}: {e}", P::NAME));
    assert!(
        !cases.is_empty(),
        "expected at least one case for {}/{FORK}/{RUNNER}/{container}",
        P::NAME
    );

    for case in &cases {
        let bytes = case
            .ssz_bytes("serialized.ssz_snappy")
            .unwrap_or_else(|e| panic!("decompress {}: {e}", case.rel_path()));
        let out = handler(&bytes).unwrap_or_else(|e| {
            panic!(
                "decode/encode/root failed for {} ({container}): {e:?}",
                case.rel_path()
            )
        });
        assert_eq!(
            out.serialized,
            bytes,
            "serialized round-trip mismatch for {}",
            case.rel_path()
        );
        let roots: RootsYaml = case
            .yaml("roots.yaml")
            .unwrap_or_else(|e| panic!("roots.yaml {}: {e}", case.rel_path()));
        let expected = parse_root(&roots.root);
        assert_eq!(
            out.root,
            expected,
            "hash_tree_root mismatch for {}",
            case.rel_path()
        );
    }
}

/// Generate one `#[test]` per registry container for a single preset.
macro_rules! spec_suite {
    ($mod_name:ident, $preset:ty) => {
        mod $mod_name {
            use super::*;

            macro_rules! one {
                ($test_name:ident, $container:literal) => {
                    #[test]
                    fn $test_name() {
                        run_container_cases::<$preset>($container);
                    }
                };
            }

            one!(aggregate_and_proof, "AggregateAndProof");
            one!(attestation, "Attestation");
            one!(attestation_data, "AttestationData");
            one!(attester_slashing, "AttesterSlashing");
            one!(bls_to_execution_change, "BLSToExecutionChange");
            one!(beacon_block, "BeaconBlock");
            one!(beacon_block_body, "BeaconBlockBody");
            one!(beacon_block_header, "BeaconBlockHeader");
            one!(beacon_state, "BeaconState");
            one!(checkpoint, "Checkpoint");
            one!(consolidation_request, "ConsolidationRequest");
            one!(contribution_and_proof, "ContributionAndProof");
            one!(data_column_sidecar, "DataColumnSidecar");
            one!(
                data_columns_by_root_identifier,
                "DataColumnsByRootIdentifier"
            );
            one!(deposit, "Deposit");
            one!(deposit_data, "DepositData");
            one!(deposit_message, "DepositMessage");
            one!(deposit_request, "DepositRequest");
            one!(eth1_block, "Eth1Block");
            one!(eth1_data, "Eth1Data");
            one!(execution_payload, "ExecutionPayload");
            one!(execution_payload_header, "ExecutionPayloadHeader");
            one!(execution_requests, "ExecutionRequests");
            one!(fork, "Fork");
            one!(fork_data, "ForkData");
            one!(historical_summary, "HistoricalSummary");
            one!(indexed_attestation, "IndexedAttestation");
            one!(light_client_bootstrap, "LightClientBootstrap");
            one!(light_client_finality_update, "LightClientFinalityUpdate");
            one!(light_client_header, "LightClientHeader");
            one!(
                light_client_optimistic_update,
                "LightClientOptimisticUpdate"
            );
            one!(light_client_update, "LightClientUpdate");
            one!(matrix_entry, "MatrixEntry");
            one!(partial_data_column_group_id, "PartialDataColumnGroupID");
            one!(partial_data_column_header, "PartialDataColumnHeader");
            one!(
                partial_data_column_parts_metadata,
                "PartialDataColumnPartsMetadata"
            );
            one!(partial_data_column_sidecar, "PartialDataColumnSidecar");
            one!(pending_consolidation, "PendingConsolidation");
            one!(pending_deposit, "PendingDeposit");
            one!(pending_partial_withdrawal, "PendingPartialWithdrawal");
            one!(pow_block, "PowBlock");
            one!(proposer_slashing, "ProposerSlashing");
            one!(signed_aggregate_and_proof, "SignedAggregateAndProof");
            one!(signed_bls_to_execution_change, "SignedBLSToExecutionChange");
            one!(signed_beacon_block, "SignedBeaconBlock");
            one!(signed_beacon_block_header, "SignedBeaconBlockHeader");
            one!(signed_contribution_and_proof, "SignedContributionAndProof");
            one!(signed_voluntary_exit, "SignedVoluntaryExit");
            one!(signing_data, "SigningData");
            one!(single_attestation, "SingleAttestation");
            one!(sync_aggregate, "SyncAggregate");
            one!(
                sync_aggregator_selection_data,
                "SyncAggregatorSelectionData"
            );
            one!(sync_committee, "SyncCommittee");
            one!(sync_committee_contribution, "SyncCommitteeContribution");
            one!(sync_committee_message, "SyncCommitteeMessage");
            one!(validator, "Validator");
            one!(voluntary_exit, "VoluntaryExit");
            one!(withdrawal, "Withdrawal");
            one!(withdrawal_request, "WithdrawalRequest");
        }
    };
}

spec_suite!(mainnet, Mainnet);
spec_suite!(minimal, Minimal);

/// Set equality both directions for both presets (CC-10/1).
#[test]
fn container_coverage() {
    let vectors = Vectors::open()
        .expect("spec vector cache must be present; run scripts/fetch-spec-vectors.sh");

    for preset in [Mainnet::NAME, Minimal::NAME] {
        let on_disk = vectors
            .handlers(preset, FORK, RUNNER)
            .unwrap_or_else(|e| panic!("handlers {preset}: {e}"));
        assert_handler_coverage(SSZ_STATIC_TYPE_NAMES, &on_disk).unwrap_or_else(|e| {
            panic!("container coverage mismatch for {preset}/{FORK}/{RUNNER}: {e}")
        });

        // Table name set must also match (handlers are generic; names shared).
        let table: BTreeSet<&str> = ssz_static_types::<Mainnet>()
            .iter()
            .map(|(n, _)| *n)
            .collect();
        let names: BTreeSet<&str> = SSZ_STATIC_TYPE_NAMES.iter().copied().collect();
        assert_eq!(table, names);
    }
}

/// Hoodi anchor `BeaconState` decodes under Fulu context (CC-10e AC).
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset so CI without the fixture cache
/// stays green (same policy as the fixtures binary).
#[test]
fn hoodi_beacon_state_decodes_under_fulu() {
    if std::env::var("HOODI_FIXTURES_CACHE")
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        // Also accept default cache path if present.
        let default = dirs_home_cache();
        if !default.join("3649472").join("beacon_state.ssz").is_file() {
            eprintln!("skipping: hoodi fixture cache not present");
            return;
        }
        decode_hoodi_state(&default);
        return;
    }
    let root = std::env::var("HOODI_FIXTURES_CACHE").expect("checked");
    decode_hoodi_state(std::path::Path::new(&root));
}

fn dirs_home_cache() -> std::path::PathBuf {
    let home = std::env::var("HOME").expect("HOME");
    std::path::PathBuf::from(home)
        .join(".cache")
        .join("cc-hoodi-fixtures")
}

fn decode_hoodi_state(cache_root: &std::path::Path) {
    use cc_types::BeaconState;

    let path = cache_root.join("3649472").join("beacon_state.ssz");
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        bytes.len() as u64 >= 150 * 1024 * 1024,
        "hoodi state must be ≥ 150 MB, got {}",
        bytes.len()
    );
    let state = BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("BeaconState decode failed: {e:?}"));
    assert!(state.validators_len() > 0, "decoded state has validators");
    // Proposer lookahead capacity is compile-time; still exercise the accessor.
    assert_eq!(
        state.proposer_lookahead().len(),
        Mainnet::PROPOSER_LOOKAHEAD_LEN as usize
    );
}
