//! S0-A-03 / P0-19/2 — SSZ-decode round-trip, hand-fill forbidden.
//!
//! Same decode-only discipline as the S0a-A-01 R-11 harness
//! (`tests/support/anchor.rs`): the state passed to [`process_block`] is
//! obtained only by SSZ decode. This file is the always-on synthetic
//! regression; Hoodi `process_block` is S0-A-30 (E0.4).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cc_state_transition::{
    compute_time_at_slot, get_beacon_proposer_index, get_current_epoch, get_randao_mix,
    process_block, process_slots, BlockError, EngineError, ExecutionEngine, NewPayloadRequest,
    PayloadStatus, TransitionContext,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, Validator};
use cc_types::primitives::{
    BlsSignature, Epoch, ExecutionAddress, ForkVersion, Gwei, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, ForkName, Minimal};
use ssz::Encode;
use tree_hash::TreeHash;

/// Always-Valid engine (same shape as the in-crate `process_block` unit test).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl ExecutionEngine<Minimal> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, Minimal>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

fn minimal_test_config() -> ChainConfig {
    ChainConfig {
        preset_base: PresetName::Minimal,
        config_name: "minimal".into(),
        genesis_fork_version: ForkVersion::from_array([0; 4]),
        altair_fork_version: ForkVersion::from_array([1; 4]),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array([2; 4]),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array([3; 4]),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array([4; 4]),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array([5; 4]),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array([6; 4]),
        fulu_fork_epoch: Epoch::new(0),
        seconds_per_slot: 6,
        blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap(),
        deposit_chain_id: 0,
        deposit_contract_address: ExecutionAddress::ZERO,
        churn_limit_quotient: 32,
        min_per_epoch_churn_limit_electra: 64_000_000_000,
        max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
        shard_committee_period: Epoch::new(64),
        max_blobs_per_block_electra: 9,
    }
}

/// In-process pre-image. Does not touch `StateCaches`; SSZ drops them anyway.
fn seed_slot_zero() -> BeaconState<Minimal> {
    let mut state = BeaconState::<Minimal>::default();
    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new(0))
            .unwrap();
    }
    state
        .validators_push(Validator {
            pubkey: Default::default(),
            withdrawal_credentials: Root::ZERO,
            effective_balance: Gwei::new(32_000_000_000),
            slashed: false,
            activation_eligibility_epoch: Default::default(),
            activation_epoch: Default::default(),
            exit_epoch: Epoch::new(u64::MAX),
            withdrawable_epoch: Epoch::new(u64::MAX),
        })
        .unwrap();
    state.balances_push(Gwei::new(32_000_000_000)).unwrap();
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body_root: Root::ZERO,
    });
    state.set_slot(Slot::new(0));
    state.set_deposit_requests_start_index(u64::MAX);
    state
}

fn matching_empty_block(
    state: &BeaconState<Minimal>,
    config: &ChainConfig,
) -> BeaconBlock<Minimal> {
    let parent = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));
    let epoch = get_current_epoch(state);
    let mix = get_randao_mix(state, epoch).unwrap();
    let mut body = BeaconBlockBody::<Minimal>::default();
    body.execution_payload.prev_randao = mix;
    body.execution_payload.timestamp =
        compute_time_at_slot(state.genesis_time(), state.slot(), config.seconds_per_slot);
    body.execution_payload.parent_hash = state.latest_execution_payload_header().block_hash;
    // Empty participant set requires the infinity signature (eth_fast_aggregate_verify).
    body.sync_aggregate.sync_committee_signature =
        BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE);
    BeaconBlock {
        slot: state.slot(),
        proposer_index: get_beacon_proposer_index(state).unwrap(),
        parent_root: parent,
        state_root: Root::ZERO,
        body,
    }
}

/// SSZ bytes of a post-`process_slots` state plus the matching empty block.
///
/// The in-memory builder is not the test subject — both `process_block` arms
/// decode these bytes.
fn ssz_fixture() -> (Vec<u8>, BeaconBlock<Minimal>, Root) {
    let mut state = seed_slot_zero();
    let pre_root = process_slots(&mut state, Slot::new(1)).unwrap();
    let config = minimal_test_config();
    let block = matching_empty_block(&state, &config);
    (state.as_ssz_bytes(), block, pre_root)
}

/// Reviewer grep target: this file must not name a cache-population call.
#[test]
fn test_body_contains_no_cache_population_call() {
    let src = include_str!("pubkey_roundtrip.rs");
    for needle in [
        concat!("rebuild_pubkey", "_cache"),
        concat!("top_up_pubkey", "_cache"),
        concat!("pubkeys", ".insert"),
        concat!("caches", "_mut"),
    ] {
        assert!(
            !src.contains(needle),
            "S0-A-03 test body must not contain `{needle}`"
        );
    }
    assert!(
        src.contains("from_ssz_bytes_hydrated"),
        "positive arm must construct state by hydrated SSZ decode"
    );
    assert!(
        src.contains("from_ssz_bytes_with"),
        "negative arm must use the raw decoder"
    );
}

/// Hydrated decode → `process_block` ok; raw decode of the same bytes → `CachePoisoned`.
#[test]
fn ssz_decoded_state_process_block_roundtrip() {
    let (bytes, block, pre_root) = ssz_fixture();
    let config = minimal_test_config();
    let engine = AcceptEngine;

    let mut hydrated = BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert!(
        !hydrated.caches().pubkeys.is_empty(),
        "hydrated decode must fill pubkeys from the registry"
    );
    let ctx = TransitionContext::<Minimal>::new(&config, &engine);
    process_block(&mut hydrated, &block, &ctx, pre_root)
        .expect("process_block must succeed after from_ssz_bytes_hydrated");

    let mut raw = BeaconState::<Minimal>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert!(
        raw.caches().pubkeys.is_empty(),
        "raw SSZ decode must leave pubkeys empty"
    );
    let ctx = TransitionContext::<Minimal>::new(&config, &engine);
    let err = process_block(&mut raw, &block, &ctx, pre_root)
        .expect_err("raw decode must fail process_block");
    assert_eq!(err, BlockError::CachePoisoned);
}
