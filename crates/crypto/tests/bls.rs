//! CC-11a BLS + domain integration tests.
//!
//! Hoodi-derived fixtures load from the CC-10b cache
//! (`${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}/<slot>/`) when
//! present and digest-matched. When the cache is missing the Hoodi tests
//! **skip** with an explicit message (never download).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::path::PathBuf;

use cc_crypto::{
    aggregate_signatures, aggregate_verify, compute_domain, compute_signing_root,
    eth_fast_aggregate_verify, fast_aggregate_verify, get_domain, verify, AggregateSignature,
    PublicKey, Signature, SignatureSet, DOMAIN_BEACON_PROPOSER, DOMAIN_RANDAO,
    DOMAIN_SYNC_COMMITTEE, INFINITY_SIGNATURE,
};
use cc_types::{DomainType, Epoch, Fork, ForkVersion, Root};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Cache / manifest helpers (mirrors CC-10b contract)
// ---------------------------------------------------------------------------

const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";
const CACHE_ENV: &str = "HOODI_FIXTURES_CACHE";
const DEFAULT_CACHE_DIR: &str = "cc-hoodi-fixtures";

fn manifests_dir() -> PathBuf {
    // hoodi-anchor.toml lives in cc-types fixtures (committed expected digests).
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../types/tests/fixtures")
}

fn load_anchor_field(key: &str) -> String {
    let text = fs::read_to_string(manifests_dir().join("hoodi-anchor.toml"))
        .expect("hoodi-anchor.toml must be committed");
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            return v.trim().trim_matches('"').to_string();
        }
    }
    panic!("missing key {key} in hoodi-anchor.toml");
}

fn resolve_cache_root() -> PathBuf {
    if let Ok(p) = std::env::var(CACHE_ENV) {
        let p = p.trim();
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let home = std::env::var("HOME").expect("HOME");
    PathBuf::from(home).join(".cache").join(DEFAULT_CACHE_DIR)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let d = Sha256::digest(bytes);
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    let s = s.trim().trim_start_matches("0x");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn unhex32(s: &str) -> [u8; 32] {
    unhex(s).try_into().expect("32 bytes")
}

/// Skip-friendly open of the Hoodi SSZ pair.
fn try_open_hoodi() -> Result<HoodiBlsFixtures, String> {
    let slot: u64 = load_anchor_field("slot").parse().expect("slot");
    let expect_block = load_anchor_field("block_sha256");
    let expect_state = load_anchor_field("state_sha256");
    let root = resolve_cache_root();
    let slot_dir = root.join(slot.to_string());
    let block_path = slot_dir.join("signed_beacon_block.ssz");
    let state_path = slot_dir.join("beacon_state.ssz");

    if !block_path.is_file() || !state_path.is_file() {
        return Err(format!(
            "Hoodi fixture cache missing at {} ({FETCH_HINT})",
            slot_dir.display()
        ));
    }

    let block = fs::read(&block_path).map_err(|e| e.to_string())?;
    let state = fs::read(&state_path).map_err(|e| e.to_string())?;

    let block_sha = hex_sha256(&block);
    if block_sha != expect_block {
        return Err(format!(
            "signed_beacon_block.ssz SHA256 mismatch: expected {expect_block}, actual {block_sha}; {FETCH_HINT}"
        ));
    }
    let state_sha = hex_sha256(&state);
    if state_sha != expect_state {
        return Err(format!(
            "beacon_state.ssz SHA256 mismatch: expected {expect_state}, actual {state_sha}; {FETCH_HINT}"
        ));
    }
    if state.len() < 150 * 1024 * 1024 {
        return Err(format!(
            "beacon_state.ssz too small ({} bytes); {FETCH_HINT}",
            state.len()
        ));
    }

    HoodiBlsFixtures::extract(block, state)
}

/// Materials extracted from the anchor signed block + post-state SSZ.
struct HoodiBlsFixtures {
    epoch: u64,
    proposer_pubkey: PublicKey,
    block_root: Root,
    parent_root: Root,
    gvr: Root,
    fork: Fork,
    block_signature: Signature,
    randao_reveal: Signature,
    sync_bits: Vec<u8>,
    sync_signature: Signature,
    sync_committee_pubkeys: Vec<PublicKey>,
}

impl HoodiBlsFixtures {
    fn extract(block: Vec<u8>, state: Vec<u8>) -> Result<Self, String> {
        // SignedBeaconBlock: offset(message) || signature[96] || message
        if block.len() < 100 {
            return Err("block SSZ too short".into());
        }
        let msg_off = u32::from_le_bytes(block[0..4].try_into().unwrap()) as usize;
        if msg_off != 100 {
            return Err(format!("unexpected message offset {msg_off}"));
        }
        let sig_bytes: [u8; 96] = block[4..100].try_into().unwrap();
        let block_signature = Signature::deserialize(&sig_bytes)
            .map_err(|e| format!("block signature: {e}"))?;

        let msg = &block[msg_off..];
        if msg.len() < 84 {
            return Err("beacon block message too short".into());
        }
        let slot = u64::from_le_bytes(msg[0..8].try_into().unwrap());
        let proposer_index = u64::from_le_bytes(msg[8..16].try_into().unwrap()) as usize;
        let parent_root = Root::from_array(msg[16..48].try_into().unwrap());
        let body_off = u32::from_le_bytes(msg[80..84].try_into().unwrap()) as usize;
        let body = &msg[body_off..];
        if body.len() < 96 {
            return Err("body too short for randao".into());
        }
        let randao_bytes: [u8; 96] = body[0..96].try_into().unwrap();
        let randao_reveal =
            Signature::deserialize(&randao_bytes).map_err(|e| format!("randao: {e}"))?;

        // Sync aggregate is near the end of Electra/Fulu BeaconBlockBody.
        // Layout after randao(96) is complex; locate by scanning for the known
        // signature from the committed block is fragile. Instead parse body
        // offsets for Fulu BeaconBlockBody variable fields and fixed tails.
        let (sync_bits, sync_sig_bytes) = extract_sync_aggregate(body)?;
        let sync_signature = Signature::deserialize(&sync_sig_bytes)
            .map_err(|e| format!("sync sig: {e}"))?;

        let state_view = AnchorStateView::parse(&state)?;
        if state_view.slot != slot {
            return Err(format!(
                "state slot {} != block slot {slot}",
                state_view.slot
            ));
        }
        let proposer_pubkey = state_view
            .validator_pubkey(proposer_index)
            .map_err(|e| e.to_string())?;
        let block_root = Root::from_array(unhex32(&load_anchor_field("block_root")));

        Ok(Self {
            epoch: slot / 32,
            proposer_pubkey,
            block_root,
            parent_root,
            gvr: state_view.gvr,
            fork: state_view.fork,
            block_signature,
            randao_reveal,
            sync_bits,
            sync_signature,
            sync_committee_pubkeys: state_view.current_sync_committee,
        })
    }

    fn participant_pubkeys(&self) -> Vec<PublicKey> {
        let mut out = Vec::new();
        for i in 0..512 {
            let byte = self.sync_bits[i / 8];
            if byte & (1 << (i % 8)) != 0 {
                out.push(self.sync_committee_pubkeys[i]);
            }
        }
        out
    }

    fn proposer_domain(&self) -> cc_types::Domain {
        get_domain(
            &self.fork,
            DOMAIN_BEACON_PROPOSER,
            Some(Epoch::new(self.epoch)),
            self.gvr,
        )
    }

    fn randao_domain(&self) -> cc_types::Domain {
        get_domain(
            &self.fork,
            DOMAIN_RANDAO,
            Some(Epoch::new(self.epoch)),
            self.gvr,
        )
    }

    fn sync_domain(&self) -> cc_types::Domain {
        get_domain(
            &self.fork,
            DOMAIN_SYNC_COMMITTEE,
            Some(Epoch::new(self.epoch)),
            self.gvr,
        )
    }

    fn block_signing_root(&self) -> [u8; 32] {
        *compute_signing_root(&self.block_root, self.proposer_domain()).as_array()
    }

    fn randao_signing_root(&self) -> [u8; 32] {
        let epoch = Epoch::new(self.epoch);
        *compute_signing_root(&epoch, self.randao_domain()).as_array()
    }

    fn sync_signing_root(&self) -> [u8; 32] {
        // process_sync_aggregate signs the previous slot's block root.
        *compute_signing_root(&self.parent_root, self.sync_domain()).as_array()
    }
}

/// Minimal Electra/Fulu post-state SSZ walker for the fields BLS tests need.
struct AnchorStateView {
    slot: u64,
    gvr: Root,
    fork: Fork,
    validators_bytes: Vec<u8>,
    current_sync_committee: Vec<PublicKey>,
}

impl AnchorStateView {
    fn parse(data: &[u8]) -> Result<Self, String> {
        // Field sizes for Electra/Fulu post-state (mainnet preset).
        const SLOTS_PER_HISTORICAL_ROOT: usize = 8192;
        const EPOCHS_PER_HISTORICAL_VECTOR: usize = 65536;
        const EPOCHS_PER_SLASHINGS_VECTOR: usize = 8192;
        const SYNC_COMMITTEE_SIZE: usize = 512;
        const PROPOSER_LOOKAHEAD: usize = 64; // (MIN_SEED_LOOKAHEAD+1)*SLOTS_PER_EPOCH

        #[derive(Clone, Copy)]
        enum Field {
            Fixed(usize),
            Var,
        }
        let fields: &[(&str, Field)] = &[
            ("genesis_time", Field::Fixed(8)),
            ("genesis_validators_root", Field::Fixed(32)),
            ("slot", Field::Fixed(8)),
            ("fork", Field::Fixed(16)),
            ("latest_block_header", Field::Fixed(112)),
            (
                "block_roots",
                Field::Fixed(SLOTS_PER_HISTORICAL_ROOT * 32),
            ),
            (
                "state_roots",
                Field::Fixed(SLOTS_PER_HISTORICAL_ROOT * 32),
            ),
            ("historical_roots", Field::Var),
            ("eth1_data", Field::Fixed(72)),
            ("eth1_data_votes", Field::Var),
            ("eth1_deposit_index", Field::Fixed(8)),
            ("validators", Field::Var),
            ("balances", Field::Var),
            (
                "randao_mixes",
                Field::Fixed(EPOCHS_PER_HISTORICAL_VECTOR * 32),
            ),
            (
                "slashings",
                Field::Fixed(EPOCHS_PER_SLASHINGS_VECTOR * 8),
            ),
            ("previous_epoch_participation", Field::Var),
            ("current_epoch_participation", Field::Var),
            ("justification_bits", Field::Fixed(1)),
            ("previous_justified_checkpoint", Field::Fixed(40)),
            ("current_justified_checkpoint", Field::Fixed(40)),
            ("finalized_checkpoint", Field::Fixed(40)),
            ("inactivity_scores", Field::Var),
            (
                "current_sync_committee",
                Field::Fixed(SYNC_COMMITTEE_SIZE * 48 + 48),
            ),
            (
                "next_sync_committee",
                Field::Fixed(SYNC_COMMITTEE_SIZE * 48 + 48),
            ),
            ("latest_execution_payload_header", Field::Var),
            ("next_withdrawal_index", Field::Fixed(8)),
            ("next_withdrawal_validator_index", Field::Fixed(8)),
            ("historical_summaries", Field::Var),
            ("deposit_requests_start_index", Field::Fixed(8)),
            ("deposit_balance_to_consume", Field::Fixed(8)),
            ("exit_balance_to_consume", Field::Fixed(8)),
            ("earliest_exit_epoch", Field::Fixed(8)),
            ("consolidation_balance_to_consume", Field::Fixed(8)),
            ("earliest_consolidation_epoch", Field::Fixed(8)),
            ("pending_deposits", Field::Var),
            ("pending_partial_withdrawals", Field::Var),
            ("pending_consolidations", Field::Var),
            ("proposer_lookahead", Field::Fixed(PROPOSER_LOOKAHEAD * 8)),
        ];

        let mut pos = 0usize;
        let mut fixed: std::collections::BTreeMap<&str, &[u8]> = std::collections::BTreeMap::new();
        let mut var_off: std::collections::BTreeMap<&str, usize> =
            std::collections::BTreeMap::new();
        for (name, field) in fields {
            match field {
                Field::Fixed(n) => {
                    if pos + n > data.len() {
                        return Err(format!("state truncated at {name}"));
                    }
                    fixed.insert(*name, &data[pos..pos + n]);
                    pos += n;
                }
                Field::Var => {
                    if pos + 4 > data.len() {
                        return Err(format!("state truncated at offset {name}"));
                    }
                    let off = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    var_off.insert(*name, off);
                    pos += 4;
                }
            }
        }
        let fixed_size = pos;
        let first_var = *var_off.get("historical_roots").ok_or("no historical_roots")?;
        if first_var != fixed_size {
            return Err(format!(
                "SSZ fixed size mismatch: expected first var at {fixed_size}, got {first_var}"
            ));
        }

        let gvr = Root::from_array(fixed["genesis_validators_root"].try_into().unwrap());
        let slot = u64::from_le_bytes(fixed["slot"].try_into().unwrap());
        let fork_bytes = fixed["fork"];
        let fork = Fork {
            previous_version: ForkVersion::from_array(fork_bytes[0..4].try_into().unwrap()),
            current_version: ForkVersion::from_array(fork_bytes[4..8].try_into().unwrap()),
            epoch: Epoch::new(u64::from_le_bytes(fork_bytes[8..16].try_into().unwrap())),
        };

        let val_start = var_off["validators"];
        let bal_start = var_off["balances"];
        if bal_start < val_start || bal_start > data.len() {
            return Err("validators range invalid".into());
        }
        let validators_bytes = data[val_start..bal_start].to_vec();
        const VALIDATOR_SIZE: usize = 121;
        if !validators_bytes.len().is_multiple_of(VALIDATOR_SIZE) {
            return Err(format!(
                "validators length {} not multiple of {VALIDATOR_SIZE}",
                validators_bytes.len()
            ));
        }

        let csc = fixed["current_sync_committee"];
        let mut current_sync_committee = Vec::with_capacity(SYNC_COMMITTEE_SIZE);
        for i in 0..SYNC_COMMITTEE_SIZE {
            let pk_bytes: [u8; 48] = csc[i * 48..(i + 1) * 48].try_into().unwrap();
            let pk = PublicKey::deserialize(&pk_bytes)
                .map_err(|e| format!("sync committee pk {i}: {e}"))?;
            current_sync_committee.push(pk);
        }

        Ok(Self {
            slot,
            gvr,
            fork,
            validators_bytes,
            current_sync_committee,
        })
    }

    fn validator_pubkey(&self, index: usize) -> Result<PublicKey, String> {
        const VALIDATOR_SIZE: usize = 121;
        let n = self.validators_bytes.len() / VALIDATOR_SIZE;
        if index >= n {
            return Err(format!("validator index {index} out of range {n}"));
        }
        let start = index * VALIDATOR_SIZE;
        let pk_bytes: [u8; 48] = self.validators_bytes[start..start + 48]
            .try_into()
            .unwrap();
        PublicKey::deserialize(&pk_bytes).map_err(|e| e.to_string())
    }
}

/// Extract `SyncAggregate` (bits + signature) from a Fulu BeaconBlockBody.
///
/// Fulu body fixed prefix ends with `blob_kzg_commitments` (var) and
/// `execution_requests` (var). `sync_aggregate` is a fixed field just before
/// the execution payload.
fn extract_sync_aggregate(body: &[u8]) -> Result<(Vec<u8>, [u8; 96]), String> {
    // BeaconBlockBody (Electra/Fulu) field order:
    // randao_reveal: 96
    // eth1_data: 72
    // graffiti: 32
    // proposer_slashings: var
    // attester_slashings: var
    // attestations: var
    // deposits: var
    // voluntary_exits: var
    // sync_aggregate: 64 (bits) + 96 (sig) = 160 fixed   <<--
    // execution_payload: var
    // bls_to_execution_changes: var
    // blob_kzg_commitments: var
    // execution_requests: var
    //
    // Fixed portion:
    // 96 + 72 + 32 = 200 fixed head
    // then 4 offsets for: proposer_slashings, attester_slashings, attestations,
    // deposits, voluntary_exits (5 vars before sync_aggregate)
    // then sync_aggregate 160
    // then 4 offsets for: execution_payload, bls_to_execution_changes,
    // blob_kzg_commitments, execution_requests

    const HEAD: usize = 96 + 72 + 32;
    const VARS_BEFORE_SYNC: usize = 5;
    const SYNC_OFF: usize = HEAD + VARS_BEFORE_SYNC * 4;
    const SYNC_SIZE: usize = 64 + 96; // Bitvector[512] + BLSSignature
    if body.len() < SYNC_OFF + SYNC_SIZE {
        return Err(format!(
            "body too short for sync_aggregate: {} < {}",
            body.len(),
            SYNC_OFF + SYNC_SIZE
        ));
    }
    let bits = body[SYNC_OFF..SYNC_OFF + 64].to_vec();
    let sig: [u8; 96] = body[SYNC_OFF + 64..SYNC_OFF + 160]
        .try_into()
        .unwrap();
    Ok((bits, sig))
}

macro_rules! require_hoodi {
    () => {
        match try_open_hoodi() {
            Ok(f) => f,
            Err(msg) => {
                eprintln!("skipping Hoodi BLS test: {msg}");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Hoodi-derived ACs
// ---------------------------------------------------------------------------

#[test]
fn hoodi_block_signature_verifies_with_fetched_gvr() {
    let f = require_hoodi!();
    let msg = f.block_signing_root();
    assert!(
        verify(&f.proposer_pubkey, &msg, &f.block_signature),
        "real Hoodi block signature must verify with fetched genesis_validators_root"
    );

    // Altered byte in the signature must fail.
    let mut bad = f.block_signature.serialize();
    bad[5] ^= 0x01;
    // Invalid point on deserialize also satisfies the AC.
    if let Ok(sig) = Signature::deserialize(&bad) {
        assert!(!verify(&f.proposer_pubkey, &msg, &sig));
    }
}

#[test]
fn hoodi_gvr_zero_makes_block_signature_fail() {
    let f = require_hoodi!();
    // Cross-Requirement Dependency 1: zero GVR → domain wrong → verify fails.
    let zero_gvr = Root::from_array([0u8; 32]);
    let bad_domain = compute_domain(
        DOMAIN_BEACON_PROPOSER,
        Some(f.fork.current_version),
        Some(zero_gvr),
    );
    let bad_msg = *compute_signing_root(&f.block_root, bad_domain).as_array();
    assert!(
        !verify(&f.proposer_pubkey, &bad_msg, &f.block_signature),
        "block signature must NOT verify under Root::ZERO genesis_validators_root"
    );

    // Positive control: real GVR still works.
    let good = f.block_signing_root();
    assert!(verify(&f.proposer_pubkey, &good, &f.block_signature));
}

#[test]
fn hoodi_randao_and_aggregate_verify() {
    let f = require_hoodi!();
    let randao_msg = f.randao_signing_root();
    assert!(
        verify(&f.proposer_pubkey, &randao_msg, &f.randao_reveal),
        "RANDAO reveal must verify"
    );

    // aggregate_verify over two real Hoodi signatures (block + RANDAO) from the
    // same proposer: aggregate the sigs, verify against the two signing roots.
    let block_msg = f.block_signing_root();
    let agg = aggregate_signatures(&[&f.block_signature, &f.randao_reveal])
        .expect("aggregate block+randao");
    assert!(
        aggregate_verify(
            &[f.proposer_pubkey, f.proposer_pubkey],
            &[block_msg, randao_msg],
            &agg
        ),
        "aggregate_verify over Hoodi block + RANDAO must pass"
    );

    // Altered aggregate byte fails.
    let mut bad = agg.serialize();
    bad[15] ^= 0x01;
    if let Ok(bad_sig) = Signature::deserialize(&bad) {
        assert!(!aggregate_verify(
            &[f.proposer_pubkey, f.proposer_pubkey],
            &[block_msg, randao_msg],
            &bad_sig
        ));
    }
}

#[test]
fn hoodi_sync_aggregate_fast_aggregate_verify() {
    let f = require_hoodi!();
    let pks = f.participant_pubkeys();
    assert!(!pks.is_empty(), "sync aggregate should have participants");
    let msg = f.sync_signing_root();
    assert!(
        fast_aggregate_verify(&pks, &msg, &f.sync_signature),
        "sync aggregate must fast_aggregate_verify"
    );
    assert!(
        eth_fast_aggregate_verify(&pks, &msg, &f.sync_signature),
        "eth_fast_aggregate_verify must agree for non-empty set"
    );

    // Altered bit in the aggregate signature → fail.
    let mut bad = f.sync_signature.serialize();
    bad[20] ^= 0xff;
    if let Ok(sig) = Signature::deserialize(&bad) {
        assert!(!fast_aggregate_verify(&pks, &msg, &sig));
    }
}

#[test]
fn hoodi_batch_verification_over_anchor_block() {
    let f = require_hoodi!();
    let mut set = SignatureSet::new();
    set.push(
        f.proposer_pubkey,
        f.block_signing_root(),
        f.block_signature,
    );
    set.push(f.proposer_pubkey, f.randao_signing_root(), f.randao_reveal);
    set.push_aggregate(
        f.participant_pubkeys(),
        f.sync_signing_root(),
        f.sync_signature,
    );
    assert!(set.verify(), "batch over anchor block must verify");

    // Altered block signature byte → whole batch fails.
    let mut bad_bytes = f.block_signature.serialize();
    bad_bytes[3] ^= 0x80;
    if let Ok(bad_sig) = Signature::deserialize(&bad_bytes) {
        let mut bad_set = SignatureSet::new();
        bad_set.push(f.proposer_pubkey, f.block_signing_root(), bad_sig);
        bad_set.push(f.proposer_pubkey, f.randao_signing_root(), f.randao_reveal);
        assert!(!bad_set.verify(), "altered signature must fail the batch");
    }
}

// ---------------------------------------------------------------------------
// Spec / unit ACs (always run)
// ---------------------------------------------------------------------------

#[test]
fn eth_fast_aggregate_empty_vs_plain() {
    let msg = [0x42u8; 32];
    let inf = Signature::infinity();
    assert!(
        eth_fast_aggregate_verify(&[], &msg, &inf),
        "eth_fast_aggregate_verify([], infinity) must be true"
    );
    assert!(
        !fast_aggregate_verify(&[], &msg, &inf),
        "fast_aggregate_verify([], infinity) must be false"
    );
}

#[test]
fn malformed_pubkey_and_signature_rejected_at_deserialize() {
    // All-zero
    assert!(PublicKey::deserialize(&[0u8; 48]).is_err());
    assert!(Signature::deserialize(&[0u8; 96]).is_err());
    // Infinity signature rejected at deserialize (use Signature::infinity for the special case).
    assert!(Signature::deserialize(&INFINITY_SIGNATURE).is_err());
    // Wrong length is not expressible via [u8; N], but a near-infinity flip is.
    let mut bogus = INFINITY_SIGNATURE;
    bogus[1] = 0x01;
    assert!(Signature::deserialize(&bogus).is_err());
}

#[test]
fn synthetic_aggregate_verify_and_batch() {
    // Build keys via blst secret keys through a tiny local helper (not public API).
    use blst::min_pk::SecretKey as BlstSk;

    fn key(ikm: u8) -> (PublicKey, impl Fn([u8; 32]) -> Signature) {
        let sk = BlstSk::key_gen(&[ikm; 32], &[]).unwrap();
        let pk = PublicKey::deserialize(&sk.sk_to_pk().compress()).unwrap();
        let sign = move |msg: [u8; 32]| {
            let sig = sk.sign(
                &msg,
                cc_crypto::BLS_SIGNATURE_DST,
                &[],
            );
            Signature::deserialize(&sig.compress()).unwrap()
        };
        (pk, sign)
    }

    let (pk1, sign1) = key(1);
    let (pk2, sign2) = key(2);
    let m1 = [1u8; 32];
    let m2 = [2u8; 32];
    let s1 = sign1(m1);
    let s2 = sign2(m2);
    let agg = AggregateSignature::aggregate(&[&s1, &s2])
        .unwrap()
        .to_signature();
    assert!(aggregate_verify(&[pk1, pk2], &[m1, m2], &agg));

    let mut set = SignatureSet::new();
    set.push(pk1, m1, s1);
    set.push(pk2, m2, s2);
    assert!(set.verify());

    // Altered message in the set fails.
    let mut bad = SignatureSet::new();
    bad.push(pk1, m1, s1);
    bad.push(pk2, [9u8; 32], s2);
    assert!(!bad.verify());
}

#[test]
fn compute_domain_uses_fork_version_bytes() {
    let gvr = Root::from_array(unhex32(
        "212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f",
    ));
    let fulu = ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]);
    let d = compute_domain(DOMAIN_BEACON_PROPOSER, Some(fulu), Some(gvr));
    assert_eq!(&d.as_slice()[0..4], DOMAIN_BEACON_PROPOSER.as_slice());
    // Domain is domain_type || fork_data_root[0..28]
    assert_ne!(&d.as_slice()[4..], &[0u8; 28]);
}

#[test]
fn infinity_constant_matches_signature_infinity() {
    assert_eq!(Signature::infinity().serialize(), INFINITY_SIGNATURE);
    assert!(Signature::infinity().is_infinity());
}

// Ensure DomainType is used so unused-import free if any re-export churns.
#[test]
fn domain_type_roundtrip_bytes() {
    let t: DomainType = DOMAIN_BEACON_PROPOSER;
    assert_eq!(t.as_array(), &[0, 0, 0, 0]);
}
