//! S0-A-11 / E0.3 — the five P0-02 constants are read from a fixture that
//! differs from both compile-time presets, not from `P::NAME`.
//!
//! Official operations / epoch_processing runners keep `spec_config_for_preset`
//! (do not point them at this YAML). These tests bind the production callers.
//!
//! A leftover `deposit_domain()` / deleted `network::*` read at those callers
//! fails the equality against the fixture values.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cc_crypto::{BLS_SIGNATURE_DST, DOMAIN_DEPOSIT, compute_domain, compute_signing_root};
use cc_state_transition::block::operations::apply_deposit;
use cc_state_transition::helpers::accessors::deposit_domain;
use cc_state_transition::helpers::constants::{
    EFFECTIVE_BALANCE_INCREMENT, FAR_FUTURE_EPOCH, GENESIS_SLOT, MAX_EFFECTIVE_BALANCE,
};
use cc_state_transition::helpers::mutators::compute_exit_epoch_and_update_churn;
use cc_state_transition::{
    apply_pending_deposit, process_pending_deposits, process_voluntary_exit,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::containers::{DepositMessage, Validator};
use cc_types::operations::{PendingDeposit, SignedVoluntaryExit, VoluntaryExit};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{
    BlsPublicKey, BlsSignature, Epoch, ForkVersion, Gwei, Root, Slot, ValidatorIndex,
};

const DIFFERING_YAML: &str = include_str!("../../types/tests/fixtures/differing-config.yaml");
const MAINNET_YAML: &str = include_str!("../../types/tests/fixtures/mainnet-config.yaml");

fn differing_config() -> ChainConfig {
    ChainConfig::from_yaml_str(DIFFERING_YAML).expect("differing-config.yaml")
}

fn mainnet_config() -> ChainConfig {
    ChainConfig::from_yaml_str(MAINNET_YAML).expect("mainnet-config.yaml")
}

/// Values the deleted `constants::network` module returned for `P`.
fn preset_p002<P: Preset>() -> (ForkVersion, u64, u64, u64, Epoch) {
    match P::NAME {
        "minimal" => (
            ForkVersion::from_array([0, 0, 0, 1]),
            32,
            64_000_000_000,
            128_000_000_000,
            Epoch::new(64),
        ),
        _ => (
            ForkVersion::from_array([0, 0, 0, 0]),
            65_536,
            128_000_000_000,
            256_000_000_000,
            Epoch::new(256),
        ),
    }
}

struct SignedPop {
    pubkey: BlsPublicKey,
    creds: Root,
    amount: Gwei,
    signature: BlsSignature,
}

fn sign_pop(domain: cc_types::primitives::Domain, seed: u8, amount: Gwei) -> SignedPop {
    let mut ikm = [seed; 32];
    ikm[31] = 0x5a;
    let sk = blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
    let pubkey = BlsPublicKey::from_array(sk.sk_to_pk().compress());
    let creds = Root::from_array({
        let mut c = [0u8; 32];
        c[0] = 0x01;
        c
    });
    let msg = DepositMessage {
        pubkey,
        withdrawal_credentials: creds,
        amount,
    };
    let root = compute_signing_root(&msg, domain);
    let signature =
        BlsSignature::from_array(sk.sign(root.as_slice(), BLS_SIGNATURE_DST, &[]).compress());
    SignedPop {
        pubkey,
        creds,
        amount,
        signature,
    }
}

fn fixture_domain(cfg: &ChainConfig) -> cc_types::primitives::Domain {
    compute_domain(DOMAIN_DEPOSIT, Some(cfg.genesis_fork_version), None)
}

fn active_validator(i: u64) -> Validator {
    let mut pk = [0u8; 48];
    pk[0..8].copy_from_slice(&i.to_le_bytes());
    pk[47] = 0x01;
    Validator {
        pubkey: BlsPublicKey::from_array(pk),
        withdrawal_credentials: Root::ZERO,
        effective_balance: MAX_EFFECTIVE_BALANCE,
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    }
}

fn seed_state<P: Preset>(n: usize, epoch: u64) -> BeaconState<P> {
    let mut state = BeaconState::<P>::default();
    state.set_slot(Slot::new(epoch.saturating_mul(P::SLOTS_PER_EPOCH)));
    for i in 0..n {
        let v = active_validator(i as u64);
        state.validators_push(v).unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
    state
}

fn activation_exit_churn(n_active: u64, cfg: &ChainConfig) -> u64 {
    let total = n_active.saturating_mul(MAX_EFFECTIVE_BALANCE.as_u64());
    let by_quotient = total / cfg.churn_limit_quotient;
    let raw = by_quotient.max(cfg.min_per_epoch_churn_limit_electra);
    let aligned = raw - (raw % EFFECTIVE_BALANCE_INCREMENT.as_u64());
    aligned.min(cfg.max_per_epoch_activation_exit_churn_limit)
}

fn expected_exit_epoch<P: Preset>(
    current_epoch: u64,
    n_active: u64,
    exit_balance: u64,
    cfg: &ChainConfig,
) -> u64 {
    let per_epoch = activation_exit_churn(n_active, cfg);
    let mut earliest = current_epoch
        .saturating_add(1)
        .saturating_add(P::MAX_SEED_LOOKAHEAD);
    if exit_balance > per_epoch {
        let additional = (exit_balance - per_epoch - 1) / per_epoch + 1;
        earliest = earliest.saturating_add(additional);
    }
    earliest
}

fn assert_fixture_differs_from_presets(cfg: &ChainConfig) {
    let mainnet = mainnet_config();
    assert_ne!(cfg.genesis_fork_version, mainnet.genesis_fork_version);
    assert_ne!(cfg.churn_limit_quotient, mainnet.churn_limit_quotient);
    assert_ne!(
        cfg.min_per_epoch_churn_limit_electra,
        mainnet.min_per_epoch_churn_limit_electra
    );
    assert_ne!(
        cfg.max_per_epoch_activation_exit_churn_limit,
        mainnet.max_per_epoch_activation_exit_churn_limit
    );
    assert_ne!(cfg.shard_committee_period, mainnet.shard_committee_period);

    for name in ["mainnet", "minimal"] {
        let (gfv, clq, min_churn, max_churn, scp) = match name {
            "minimal" => preset_p002::<Minimal>(),
            _ => preset_p002::<Mainnet>(),
        };
        assert_ne!(cfg.genesis_fork_version, gfv, "{name} genesis_fork_version");
        assert_ne!(cfg.churn_limit_quotient, clq, "{name} churn_limit_quotient");
        assert_ne!(
            cfg.min_per_epoch_churn_limit_electra, min_churn,
            "{name} min_per_epoch_churn_limit_electra"
        );
        assert_ne!(
            cfg.max_per_epoch_activation_exit_churn_limit, max_churn,
            "{name} max_per_epoch_activation_exit_churn_limit"
        );
        assert_ne!(
            cfg.shard_committee_period, scp,
            "{name} shard_committee_period"
        );
    }
}

/// `apply_deposit` + `apply_pending_deposit` + `process_pending_deposits`
/// honour `config.genesis_fork_version`. A leftover `deposit_domain()`
/// (fork version `None` → `0x00000000`) is a silent no-op here.
fn assert_deposit_callers_use_fixture_gfv<P: Preset>(cfg: &ChainConfig) {
    let amount = Gwei::new(1_000_000_000);
    let fixture = sign_pop(fixture_domain(cfg), 0x11, amount);
    let leftover = sign_pop(deposit_domain(), 0x22, amount);

    let mut applied = BeaconState::<P>::default();
    apply_deposit(
        &mut applied,
        fixture.pubkey,
        fixture.creds,
        fixture.amount,
        fixture.signature,
        cfg,
    )
    .unwrap();
    assert_eq!(
        applied.validators_len(),
        1,
        "{}: apply_deposit must append under fixture GENESIS_FORK_VERSION",
        P::NAME
    );
    assert_eq!(applied.pending_deposits_len(), 1);

    let mut dropped = BeaconState::<P>::default();
    apply_deposit(
        &mut dropped,
        leftover.pubkey,
        leftover.creds,
        leftover.amount,
        leftover.signature,
        cfg,
    )
    .unwrap();
    assert_eq!(
        dropped.validators_len(),
        0,
        "{}: apply_deposit must silent-drop a deposit_domain() PoP",
        P::NAME
    );
    assert_eq!(dropped.pending_deposits_len(), 0);

    let pending_ok = sign_pop(fixture_domain(cfg), 0x33, amount);
    let mut pending_state = BeaconState::<P>::default();
    apply_pending_deposit(
        &mut pending_state,
        &PendingDeposit {
            pubkey: pending_ok.pubkey,
            withdrawal_credentials: pending_ok.creds,
            amount: pending_ok.amount,
            signature: pending_ok.signature,
            slot: Slot::new(GENESIS_SLOT),
        },
        cfg,
    )
    .unwrap();
    assert_eq!(
        pending_state.validators_len(),
        1,
        "{}: apply_pending_deposit must append under fixture GENESIS_FORK_VERSION",
        P::NAME
    );
    assert_eq!(pending_state.balances_get(0).unwrap(), amount);

    let pending_bad = sign_pop(deposit_domain(), 0x44, amount);
    let mut pending_noop = BeaconState::<P>::default();
    apply_pending_deposit(
        &mut pending_noop,
        &PendingDeposit {
            pubkey: pending_bad.pubkey,
            withdrawal_credentials: pending_bad.creds,
            amount: pending_bad.amount,
            signature: pending_bad.signature,
            slot: Slot::new(GENESIS_SLOT),
        },
        cfg,
    )
    .unwrap();
    assert_eq!(
        pending_noop.validators_len(),
        0,
        "{}: apply_pending_deposit must no-op a deposit_domain() PoP",
        P::NAME
    );

    let queued = sign_pop(fixture_domain(cfg), 0x55, amount);
    let mut epoch_ok = BeaconState::<P>::default();
    epoch_ok
        .pending_deposits_push(PendingDeposit {
            pubkey: queued.pubkey,
            withdrawal_credentials: queued.creds,
            amount: queued.amount,
            signature: queued.signature,
            slot: Slot::new(GENESIS_SLOT),
        })
        .unwrap();
    process_pending_deposits(&mut epoch_ok, cfg).unwrap();
    assert_eq!(
        epoch_ok.validators_len(),
        1,
        "{}: process_pending_deposits must apply a fixture-domain PoP",
        P::NAME
    );

    let queued_bad = sign_pop(deposit_domain(), 0x66, amount);
    let mut epoch_drop = BeaconState::<P>::default();
    epoch_drop
        .pending_deposits_push(PendingDeposit {
            pubkey: queued_bad.pubkey,
            withdrawal_credentials: queued_bad.creds,
            amount: queued_bad.amount,
            signature: queued_bad.signature,
            slot: Slot::new(GENESIS_SLOT),
        })
        .unwrap();
    process_pending_deposits(&mut epoch_drop, cfg).unwrap();
    assert_eq!(
        epoch_drop.validators_len(),
        0,
        "{}: process_pending_deposits must not append a deposit_domain() PoP",
        P::NAME
    );
}

/// `compute_exit_epoch_and_update_churn` and `process_voluntary_exit` honour
/// the three churn keys and `shard_committee_period`. Asserts the **epoch
/// number**, not merely `exit_epoch != FAR_FUTURE_EPOCH`.
fn assert_exit_callers_use_fixture<P: Preset>(cfg: &ChainConfig) {
    const LARGE_N: u64 = 5;
    const BIG_EXIT: u64 = 64_000_000_000;
    let want_churn_epoch = expected_exit_epoch::<P>(0, LARGE_N, BIG_EXIT, cfg);
    let preset_churn_epoch = {
        let (gfv, clq, min_churn, max_churn, _scp) = preset_p002::<P>();
        let _ = gfv;
        let total = LARGE_N * MAX_EFFECTIVE_BALANCE.as_u64();
        let raw = (total / clq).max(min_churn);
        let aligned = raw - (raw % EFFECTIVE_BALANCE_INCREMENT.as_u64());
        let per_epoch = aligned.min(max_churn);
        let mut earliest = 1 + P::MAX_SEED_LOOKAHEAD;
        if BIG_EXIT > per_epoch {
            earliest += (BIG_EXIT - per_epoch - 1) / per_epoch + 1;
        }
        earliest
    };
    assert_ne!(
        want_churn_epoch,
        preset_churn_epoch,
        "{}: fixture exit epoch must differ from the deleted network table",
        P::NAME
    );

    let mut churn_state = seed_state::<P>(LARGE_N as usize, 0);
    let got =
        compute_exit_epoch_and_update_churn(&mut churn_state, Gwei::new(BIG_EXIT), cfg).unwrap();
    assert_eq!(
        got.as_u64(),
        want_churn_epoch,
        "{}: compute_exit_epoch_and_update_churn must use fixture churn, not the preset table",
        P::NAME
    );

    let period = cfg.shard_committee_period.as_u64();
    let (_gfv, _clq, _min, _max, preset_period) = preset_p002::<P>();
    assert!(
        period < preset_period.as_u64(),
        "fixture period must be the earlier threshold so a preset read rejects the exit"
    );

    let signed = SignedVoluntaryExit {
        message: VoluntaryExit {
            epoch: Epoch::new(0),
            validator_index: ValidatorIndex::new(0),
        },
        signature: BlsSignature::ZERO,
    };

    let mut too_early = seed_state::<P>(1, period.saturating_sub(1));
    let err = process_voluntary_exit(&mut too_early, &signed, cfg, false).unwrap_err();
    assert!(
        err.to_string().contains("not been active long enough"),
        "{}: epoch {} must still be inside the fixture period: {err}",
        P::NAME,
        period - 1
    );

    let want_exit = expected_exit_epoch::<P>(period, 1, MAX_EFFECTIVE_BALANCE.as_u64(), cfg);
    let mut eligible = seed_state::<P>(1, period);
    process_voluntary_exit(&mut eligible, &signed, cfg, false).unwrap();
    assert_eq!(
        eligible.validators_get(0).unwrap().exit_epoch.as_u64(),
        want_exit,
        "{}: process_voluntary_exit must write the fixture-derived exit epoch",
        P::NAME
    );
}

fn assert_production_reads_fixture<P: Preset>() {
    let cfg = differing_config();
    assert_fixture_differs_from_presets(&cfg);
    assert_deposit_callers_use_fixture_gfv::<P>(&cfg);
    assert_exit_callers_use_fixture::<P>(&cfg);
}

#[test]
fn mainnet_preset_reads_differing_fixture_not_preset_table() {
    assert_production_reads_fixture::<Mainnet>();
}

#[test]
fn minimal_preset_reads_differing_fixture_not_preset_table() {
    assert_production_reads_fixture::<Minimal>();
}
