//! S0-A-30 / E0.4 — R-11 executed against the committed Hoodi anchor.
//!
//! Uses the S0a-A-01 harness (`tests/support/anchor.rs`). The pin is a
//! **post-state** pair (CC-18d): `state.slot == block.slot`, so re-applying
//! the committed block fails at `process_block_header` before
//! `process_sync_aggregate`. The ± top-up assertions therefore run
//! `process_block` on a valid successor of that decoded Hoodi state — the
//! restore/replay shape — without hand-filling the cache.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor.rs"]
mod support;

use std::fs;
use std::time::Instant;

use cc_state_transition::{
    BlockError, EngineError, ExecutionEngine, NewPayloadRequest, PayloadStatus, TransitionContext,
    compute_time_at_slot, get_beacon_proposer_index, get_current_epoch, get_expected_withdrawals,
    get_randao_mix, process_block, process_slots,
};
use cc_types::config::ChainConfig;
use cc_types::containers::SyncAggregate;
use cc_types::execution::ExecutionPayload;
use cc_types::primitives::{BlsSignature, Root, Slot};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, ForkName, Mainnet};
use ssz_types::VariableList;
use support::{CACHE_ENV, FETCH_HINT, cache_env_is_set, load_anchor, load_hoodi_config};
use tree_hash::TreeHash;

/// Always-Valid engine (same shape as S0-A-03 / offline replay).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl ExecutionEngine<Mainnet> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, Mainnet>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

/// Reviewer grep target: this file must not name a cache-population call.
#[test]
fn test_body_contains_no_cache_population_call() {
    let src = include_str!("r11_process_block.rs");
    for needle in [
        concat!("rebuild_pubkey", "_cache"),
        concat!("top_up_pubkey", "_cache"),
        concat!("pubkeys", ".insert"),
        concat!("caches", "_mut"),
    ] {
        assert!(
            !src.contains(needle),
            "S0-A-30 test body must not contain `{needle}`"
        );
    }
    assert!(
        src.contains("from_ssz_bytes_hydrated"),
        "positive arm must construct state by hydrated SSZ decode"
    );
    assert!(
        src.contains("load_anchor"),
        "negative arm must use the S0a-A-01 raw harness"
    );
    assert!(
        src.contains("from_ssz_bytes_with"),
        "negative arm must name the raw decoder (harness + comment)"
    );
}

/// Valid next-slot block against an already-advanced Hoodi state.
///
/// Empty operations + empty sync bits (infinity signature). The CachePoisoned
/// path still fires: `process_sync_aggregate` resolves all 512 committee
/// pubkeys from `PubkeyIndexMap` before reading bits ([Q3] §5).
fn matching_next_block(state: &BeaconState<Mainnet>, config: &ChainConfig) -> BeaconBlock<Mainnet> {
    let proposer = get_beacon_proposer_index(state).expect("proposer");
    let (withdrawals, _) = get_expected_withdrawals(state).expect("withdrawals");
    let epoch = get_current_epoch(state);
    let prev_randao = get_randao_mix(state, epoch).expect("randao");
    let timestamp =
        compute_time_at_slot(state.genesis_time(), state.slot(), config.seconds_per_slot);
    let parent_hash = state.latest_execution_payload_header().block_hash;
    let parent_root = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));

    let payload = ExecutionPayload::<Mainnet> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: state.latest_execution_payload_header().block_number + 1,
        gas_limit: state.latest_execution_payload_header().gas_limit,
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };

    let body = BeaconBlockBody::<Mainnet> {
        execution_payload: payload,
        eth1_data: state.eth1_data(),
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE),
        },
        ..Default::default()
    };

    BeaconBlock {
        slot: state.slot(),
        proposer_index: proposer,
        parent_root,
        state_root: Root::ZERO,
        body,
    }
}

fn advance_one_slot(
    mut state: BeaconState<Mainnet>,
    config: &ChainConfig,
) -> (BeaconState<Mainnet>, BeaconBlock<Mainnet>, Root) {
    let next = Slot::new(state.slot().as_u64() + 1);
    let pre_root = process_slots(&mut state, next, config).expect("process_slots");
    let block = matching_next_block(&state, config);
    (state, block, pre_root)
}

/// Decode the Hoodi pin, `process_block` ± hydration.
///
/// Skips when `HOODI_FIXTURES_CACHE` is unset (CC-10b). When the env is set
/// the cache must be present — same contract as `r11_harness`.
#[test]
fn hoodi_decoded_state_process_block_roundtrip() {
    if !cache_env_is_set() {
        eprintln!(
            "skip: {CACHE_ENV} unset — Hoodi BeaconState SSZ is not in git; \
             {FETCH_HINT} (see crates/types/tests/fixtures/README.md)"
        );
        return;
    }

    let config = load_hoodi_config().unwrap_or_else(|e| panic!("{e}"));
    let engine = AcceptEngine;
    let t0 = Instant::now();

    // load_anchor → decode_anchor_ssz → BeaconState::from_ssz_bytes_with
    // (caches stay empty; this is the negative constructor).
    let mut loaded = load_anchor().unwrap_or_else(|e| panic!("{e}"));
    eprintln!(
        "S0-A-30: raw decode in {:.1}s; validators={} pubkey_cache={}",
        t0.elapsed().as_secs_f64(),
        loaded.validators_len(),
        loaded.pubkey_cache_len()
    );
    assert_eq!(
        loaded.pubkey_cache_len(),
        0,
        "S0a-A-01 harness must leave caches.pubkeys empty"
    );
    assert!(
        loaded.validators_len() > 0,
        "Hoodi registry must be non-empty"
    );

    // Committed pair is a post-state: the real block cannot be re-applied.
    let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
    let pair_err = process_block(&mut loaded.state, &loaded.block.message, &ctx, Root::ZERO)
        .expect_err("committed post-state pair must not re-import the pin block");
    assert!(
        matches!(pair_err, BlockError::BlockSlotNotNewer { .. }),
        "committed pair must fail at the header (post-state), got {pair_err:?}"
    );

    // S2-A-10: the map is on TransitionContext. process_block tops up from
    // the registry, so a raw-decoded successor must succeed (the empty
    // StateCaches field is no longer the CachePoisoned trigger).
    let t1 = Instant::now();
    let (mut raw_state, raw_block, raw_pre) = advance_one_slot(loaded.state, &config);
    let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
    process_block(&mut raw_state, &raw_block, &ctx, raw_pre)
        .expect("raw decode must succeed: ctx tops up from the registry");
    assert_eq!(
        ctx.pubkeys().len(),
        raw_state.validators_len(),
        "process_block must cover the whole registry on the context map"
    );
    eprintln!(
        "S0-A-30: raw successor process_block in {:.1}s; pubkey_cache={}",
        t1.elapsed().as_secs_f64(),
        ctx.pubkeys().len()
    );
    drop(raw_state);

    // Same SSZ through the production decode chokepoint.
    let paths = support::resolve_anchor_paths().unwrap_or_else(|e| panic!("{e}"));
    let bytes = fs::read(&paths.state_ssz)
        .unwrap_or_else(|e| panic!("read {}: {e}", paths.state_ssz.display()));
    let t2 = Instant::now();
    let hydrated = BeaconState::<Mainnet>::from_ssz_bytes_hydrated(ForkName::Fulu, &bytes)
        .unwrap_or_else(|e| panic!("{e:?}"));
    eprintln!(
        "S0-A-30: hydrated decode in {:.1}s; validators={}",
        t2.elapsed().as_secs_f64(),
        hydrated.validators_len()
    );

    let t3 = Instant::now();
    let (mut hyd_state, hyd_block, hyd_pre) = advance_one_slot(hydrated, &config);
    let ctx = TransitionContext::<Mainnet>::new(&config, &engine);
    process_block(&mut hyd_state, &hyd_block, &ctx, hyd_pre)
        .expect("process_block must succeed after from_ssz_bytes_hydrated");
    assert_eq!(ctx.pubkeys().len(), hyd_state.validators_len());
    eprintln!(
        "S0-A-30: positive process_block in {:.1}s",
        t3.elapsed().as_secs_f64()
    );
}
