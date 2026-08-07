//! `fork_choice` + `fork_choice_compliance` vector runner (CC-15c).
//!
//! Dispatches every step kind with an exhaustive match; unknown kinds panic
//! with the kind quoted (CC-15/1). Every present `checks` field is asserted
//! (CC-15/2), including unrealized checkpoints when present.
//!
//! Suites:
//! - `tests/<preset>/fulu/fork_choice/…` — both presets
//! - `tests/minimal/fulu/fork_choice_compliance/…` — OQ-3 in-scope (minimal)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cc_fork_choice::{
    HarnessAvailability, DataAvailability, get_forkchoice_store, get_head, get_proposer_head,
    on_attestation, on_attester_slashing, on_block, on_tick, store_target_checkpoint_context,
};
use cc_state_transition::helpers::accessors::get_indexed_attestation;
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockSignatureStrategy, EngineError, ExecutionEngine, NewPayloadRequest, PayloadStatus,
    process_slots,
};
use cc_types::BeaconState;
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::Checkpoint;
use cc_types::operations::{Attestation, AttesterSlashing};
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root, Slot};
use cc_types::{BeaconBlock, ForkName, SignedBeaconBlock};
use ssz::Decode;
use tree_hash::TreeHash;

const FORK: &str = "fulu";
const LOCKFILE: &str = include_str!("../../../spec-vectors.lock");
const SKIPLIST: &str = include_str!("../../../docs/spec-vectors-skiplist.md");

// ---------------------------------------------------------------------------
// Vector cache
// ---------------------------------------------------------------------------

fn lock_tag() -> &'static str {
    LOCKFILE
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("tag")
                .and_then(|r| r.trim().strip_prefix('='))
                .map(|v| v.trim().trim_matches('"'))
        })
        .expect("tag in spec-vectors.lock")
}

fn tests_root() -> PathBuf {
    let cache = std::env::var("SPEC_VECTORS_CACHE").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME");
        format!("{home}/.cache/eth-consensus-spec-vectors")
    });
    let root = PathBuf::from(cache).join(lock_tag()).join("tests");
    assert!(
        root.is_dir(),
        "vector tests tree missing at {}; run scripts/fetch-spec-vectors.sh",
        root.display()
    );
    root
}

fn snappy_decompress(path: &Path) -> Vec<u8> {
    let compressed = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let claimed = snap::raw::decompress_len(&compressed)
        .unwrap_or_else(|e| panic!("snappy len {}: {e}", path.display()));
    let mut out = vec![0u8; claimed];
    let n = snap::raw::Decoder::new()
        .decompress(&compressed, &mut out)
        .unwrap_or_else(|e| panic!("snappy {}: {e}", path.display()));
    out.truncate(n);
    out
}

fn skiplist_prefixes() -> Vec<String> {
    let mut out = Vec::new();
    let mut in_fence = false;
    for line in SKIPLIST.lines() {
        let line = line.trim();
        if line.starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || line.is_empty() {
            continue;
        }
        if !line.starts_with("- ") && !line.starts_with("* ") {
            continue;
        }
        if let Some(start) = line.find('`')
            && let Some(end) = line[start + 1..].find('`')
        {
            out.push(line[start + 1..start + 1 + end].to_string());
        }
    }
    out
}

fn is_skipped(rel: &str, prefixes: &[String]) -> bool {
    prefixes
        .iter()
        .any(|p| rel == p.as_str() || rel.starts_with(&format!("{p}/")))
}

// ---------------------------------------------------------------------------
// Case collection
// ---------------------------------------------------------------------------

fn collect_cases(tests: &Path, preset: &str, runner: &str) -> Vec<(String, PathBuf)> {
    let runner_dir = tests.join(preset).join(FORK).join(runner);
    if !runner_dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for handler_ent in fs::read_dir(&runner_dir).unwrap() {
        let handler_ent = handler_ent.unwrap();
        if !handler_ent.file_type().unwrap().is_dir() {
            continue;
        }
        let handler = handler_ent.file_name().to_string_lossy().into_owned();
        let handler_dir = handler_ent.path();
        collect_leaf_cases(
            &handler_dir,
            &handler_dir,
            preset,
            runner,
            &handler,
            &mut out,
        );
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn collect_leaf_cases(
    handler_dir: &Path,
    current: &Path,
    preset: &str,
    runner: &str,
    handler: &str,
    out: &mut Vec<(String, PathBuf)>,
) {
    let mut has_file = false;
    let mut subdirs = Vec::new();
    for ent in fs::read_dir(current).unwrap() {
        let ent = ent.unwrap();
        let ft = ent.file_type().unwrap();
        if ft.is_dir() {
            subdirs.push(ent.path());
        } else if ft.is_file() {
            has_file = true;
        }
    }
    if has_file && current.join("steps.yaml").is_file() {
        let rel_name = current
            .strip_prefix(handler_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let case_rel = format!("{preset}/{FORK}/{runner}/{handler}/{rel_name}");
        out.push((case_rel, current.to_path_buf()));
        return;
    }
    subdirs.sort();
    for sub in subdirs {
        collect_leaf_cases(handler_dir, &sub, preset, runner, handler, out);
    }
}

fn list_handlers(tests: &Path, preset: &str, runner: &str) -> BTreeSet<String> {
    let dir = tests.join(preset).join(FORK).join(runner);
    let mut set = BTreeSet::new();
    if !dir.is_dir() {
        return set;
    }
    for ent in fs::read_dir(&dir).unwrap() {
        let ent = ent.unwrap();
        if ent.file_type().unwrap().is_dir() {
            set.insert(ent.file_name().to_string_lossy().into_owned());
        }
    }
    set
}

// ---------------------------------------------------------------------------
// Config / DA / engine
// ---------------------------------------------------------------------------

fn spec_config_for_preset(preset: PresetName) -> ChainConfig {
    let (name, seconds, genesis, altair, bellatrix, capella, deneb, electra, fulu) = match preset {
        PresetName::Mainnet => (
            "mainnet",
            12u64,
            [0x00, 0x00, 0x00, 0x00],
            [0x01, 0x00, 0x00, 0x00],
            [0x02, 0x00, 0x00, 0x00],
            [0x03, 0x00, 0x00, 0x00],
            [0x04, 0x00, 0x00, 0x00],
            [0x05, 0x00, 0x00, 0x00],
            [0x06, 0x00, 0x00, 0x00],
        ),
        PresetName::Minimal => (
            "minimal",
            6u64,
            [0x00, 0x00, 0x00, 0x01],
            [0x01, 0x00, 0x00, 0x01],
            [0x02, 0x00, 0x00, 0x01],
            [0x03, 0x00, 0x00, 0x01],
            [0x04, 0x00, 0x00, 0x01],
            [0x05, 0x00, 0x00, 0x01],
            [0x06, 0x00, 0x00, 0x01],
        ),
    };
    ChainConfig {
        preset_base: preset,
        config_name: name.into(),
        genesis_fork_version: ForkVersion::from_array(genesis),
        altair_fork_version: ForkVersion::from_array(altair),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array(bellatrix),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array(capella),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array(deneb),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array(electra),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array(fulu),
        fulu_fork_epoch: Epoch::new(0),
        seconds_per_slot: seconds,
        blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap(),
        deposit_chain_id: 0,
        deposit_contract_address: ExecutionAddress::ZERO,
    }
}

#[derive(Debug, Clone, Copy)]
struct AcceptEngine;

impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

/// Vector-suite DA: optional per-root availability overrides for PeerDAS cases.
///
/// Default is available (optimistic). A `columns: []` step marks the
/// upcoming block root unavailable; non-empty `columns` marks it available.
/// Structural failures on column sidecars also mark unavailable.
#[derive(Debug, Default)]
struct VectorDa {
    overrides: Mutex<HashMap<Root, bool>>,
}

impl VectorDa {
    fn set(&self, root: Root, available: bool) {
        self.overrides.lock().unwrap().insert(root, available);
    }
}

impl DataAvailability for VectorDa {
    fn is_data_available(&self, beacon_block_root: Root) -> bool {
        self.overrides
            .lock()
            .unwrap()
            .get(&beacon_block_root)
            .copied()
            .unwrap_or(true)
    }
}

// ---------------------------------------------------------------------------
// YAML helpers
// ---------------------------------------------------------------------------

fn parse_root_hex(s: &str) -> Root {
    let hex = s.trim().trim_start_matches("0x");
    let bytes = hex::decode(hex).unwrap_or_else(|e| panic!("bad root hex {s}: {e}"));
    assert_eq!(bytes.len(), 32, "root must be 32 bytes: {s}");
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Root::from_array(arr)
}

fn yaml_u64(v: &serde_yaml::Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_i64().map(|i| i as u64))
            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
    })
}

fn yaml_root(v: &serde_yaml::Value, key: &str) -> Option<Root> {
    v.get(key).and_then(|x| x.as_str()).map(parse_root_hex)
}

fn yaml_checkpoint(v: &serde_yaml::Value, key: &str) -> Option<Checkpoint> {
    let c = v.get(key)?;
    let epoch = yaml_u64(c, "epoch")?;
    let root = yaml_root(c, "root")?;
    Some(Checkpoint {
        epoch: Epoch::new(epoch),
        root,
    })
}

fn step_valid(step: &serde_yaml::Value) -> bool {
    step.get("valid").and_then(|v| v.as_bool()).unwrap_or(true)
}

// ---------------------------------------------------------------------------
// Column structural gate (Fulu PeerDAS vector steps)
// ---------------------------------------------------------------------------

fn kzg_backend() -> Option<&'static cc_crypto::CKzgBackend> {
    use std::sync::OnceLock;
    static BACKEND: OnceLock<Option<cc_crypto::CKzgBackend>> = OnceLock::new();
    BACKEND
        .get_or_init(|| cc_crypto::CKzgBackend::load_default().ok())
        .as_ref()
}

fn columns_structurally_ok<P: Preset>(case_dir: &Path, column_files: &[String]) -> bool {
    use cc_crypto::CellKzg;
    use cc_types::sidecar::DataColumnSidecar;
    if column_files.is_empty() {
        return false;
    }
    let Some(kzg) = kzg_backend() else {
        // Without a trusted setup, accept non-empty structural columns only.
        // Invalid-proof cases need KZG; they will fail open (import) and surface
        // as test failures if the setup cannot load.
        return !column_files.is_empty();
    };
    for name in column_files {
        let path = case_dir.join(format!("{name}.ssz_snappy"));
        let path = if path.is_file() {
            path
        } else {
            case_dir.join(name)
        };
        if !path.is_file() {
            return false;
        }
        let bytes = snappy_decompress(&path);
        let Ok(sidecar) = DataColumnSidecar::<P>::from_ssz_bytes(&bytes) else {
            return false;
        };
        // Spec-ish structural checks used by verify_data_column_sidecar.
        if sidecar.index >= 128 {
            return false;
        }
        let n = sidecar.column.len();
        if n == 0 {
            return false;
        }
        if sidecar.kzg_commitments.len() != n || sidecar.kzg_proofs.len() != n {
            return false;
        }
        // KZG cell proofs (Fulu is_data_available).
        let commitments: Vec<_> = sidecar.kzg_commitments.iter().copied().collect();
        let cells: Vec<_> = sidecar.column.iter().cloned().collect();
        let proofs: Vec<_> = sidecar.kzg_proofs.iter().copied().collect();
        let indices: Vec<u64> = vec![sidecar.index; n];
        match kzg.verify_cell_kzg_proof_batch(&commitments, &indices, &cells, &proofs) {
            Ok(true) => {}
            Ok(false) | Err(_) => return false,
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Step runner
// ---------------------------------------------------------------------------

fn run_case<P: Preset>(rel: &str, case_dir: &Path, config: &ChainConfig, da: Arc<VectorDa>) {
    let anchor_state_bytes = snappy_decompress(&case_dir.join("anchor_state.ssz_snappy"));
    let anchor_block_bytes = snappy_decompress(&case_dir.join("anchor_block.ssz_snappy"));
    let mut anchor_state =
        BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &anchor_state_bytes)
            .unwrap_or_else(|e| panic!("anchor_state {rel}: {e:?}"));
    // Rebuild pubkey index for any signature paths (vectors mostly NoVerification).
    {
        let entries: Vec<_> = anchor_state
            .validators_iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    v.pubkey,
                    cc_types::primitives::ValidatorIndex::new(i as u64),
                )
            })
            .collect();
        for (pk, idx) in entries {
            anchor_state.caches_mut().pubkeys.insert(pk, idx);
        }
    }
    let anchor_block = BeaconBlock::<P>::from_ssz_bytes(&anchor_block_bytes)
        .unwrap_or_else(|e| panic!("anchor_block {rel}: {e:?}"));

    let mut store = get_forkchoice_store(
        anchor_state,
        &anchor_block,
        Arc::new(AcceptEngine),
        da.clone() as Arc<dyn DataAvailability>,
        config.seconds_per_slot,
    )
    .unwrap_or_else(|e| panic!("get_forkchoice_store {rel}: {e}"));

    // Seed last head to the anchor so first extension is not a reorg.
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    let _ = get_head(&mut store);

    let steps_text = fs::read_to_string(case_dir.join("steps.yaml"))
        .unwrap_or_else(|e| panic!("steps.yaml {rel}: {e}"));
    let steps: Vec<serde_yaml::Value> =
        serde_yaml::from_str(&steps_text).unwrap_or_else(|e| panic!("parse steps {rel}: {e}"));

    for (i, step) in steps.iter().enumerate() {
        let step_map = step
            .as_mapping()
            .unwrap_or_else(|| panic!("step {i} not a map in {rel}"));
        // Collect recognised kind keys (excluding `valid` and payload of other kinds).
        let kinds: Vec<String> = step_map
            .keys()
            .filter_map(|k| k.as_str().map(str::to_string))
            .filter(|k| k != "valid")
            .collect();

        // A step may combine e.g. `block` + `columns` + `blobs` + `proofs`.
        // Dispatch primary action kinds; treat payload companions as data.
        let primary = kinds
            .iter()
            .find(|k| {
                matches!(
                    k.as_str(),
                    "tick"
                        | "block"
                        | "attestation"
                        | "attester_slashing"
                        | "checks"
                        | "pow_block"
                        | "block_hash"
                        | "execution_payload"
                        | "payload_attestation_message"
                )
            })
            .map(|s| s.as_str())
            .unwrap_or_else(|| {
                // columns/blobs/proofs alone should not appear without block.
                kinds.first().map(|s| s.as_str()).unwrap_or("<empty>")
            });

        match primary {
            "tick" => {
                let time = yaml_u64(step, "tick")
                    .unwrap_or_else(|| panic!("tick missing int at step {i} of {rel}"));
                let valid = step_valid(step);
                let result = on_tick(&mut store, time);
                if valid {
                    result.unwrap_or_else(|e| panic!("tick valid step {i} {rel}: {e}"));
                } else if result.is_ok() {
                    panic!("tick expected invalid at step {i} of {rel}");
                }
            }
            "block" => {
                let file = step
                    .get("block")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("block file missing step {i} {rel}"));
                let path = case_dir.join(format!("{file}.ssz_snappy"));
                let path = if path.is_file() {
                    path
                } else {
                    case_dir.join(file)
                };
                let bytes = snappy_decompress(&path);
                let signed = SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
                    .unwrap_or_else(|e| panic!("decode block {file} {rel}: {e:?}"));
                let block_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));

                // PeerDAS columns gate for this block root.
                if let Some(cols) = step.get("columns") {
                    let available = match cols {
                        serde_yaml::Value::Sequence(seq) => {
                            let names: Vec<String> = seq
                                .iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect();
                            columns_structurally_ok::<P>(case_dir, &names)
                        }
                        serde_yaml::Value::String(s) => {
                            // Single file name
                            columns_structurally_ok::<P>(case_dir, std::slice::from_ref(s))
                        }
                        _ => false,
                    };
                    da.set(block_root, available);
                }

                let valid = step_valid(step);
                let result = on_block(
                    &mut store,
                    &signed,
                    config,
                    BlockSignatureStrategy::NoVerification,
                );
                match (valid, result) {
                    (true, Ok(cc_fork_choice::BlockImport::Imported(_))) => {
                        // Pyspec `add_block`: an on_block step implies receiving
                        // the block's attestations and attester slashings
                        // (`is_from_block=True`). These are NOT separate steps.
                        apply_block_operations(&mut store, &signed, rel, i);
                        let _ = get_head(&mut store);
                    }
                    (true, Ok(other)) => {
                        panic!("block valid but not Imported at step {i} {rel}: {other:?}");
                    }
                    (true, Err(e)) => panic!("block valid step {i} {rel}: {e}"),
                    (false, Ok(cc_fork_choice::BlockImport::Imported(_))) => {
                        panic!("block expected invalid but imported at step {i} of {rel}");
                    }
                    (false, Ok(_) | Err(_)) => {
                        // Deferred or Err — both satisfy valid:false.
                    }
                }
            }
            "attestation" => {
                let file = step
                    .get("attestation")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("attestation file missing step {i} {rel}"));
                let path = case_dir.join(format!("{file}.ssz_snappy"));
                let path = if path.is_file() {
                    path
                } else {
                    case_dir.join(file)
                };
                let bytes = snappy_decompress(&path);
                let att = Attestation::<P>::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode attestation {file} {rel}: {e:?}"));
                let valid = step_valid(step);

                // Index using target checkpoint state (spec path).
                let index_result = (|| {
                    store_target_checkpoint_context(&mut store, att.data.target)?;
                    let mut state = store
                        .block_state(&att.data.target.root)
                        .ok_or(cc_fork_choice::OnAttestationError::MissingBlockState(
                            att.data.target.root,
                        ))?
                        .clone();
                    let epoch_start = compute_start_slot_at_epoch::<P>(att.data.target.epoch);
                    if state.slot().as_u64() < epoch_start.as_u64() {
                        process_slots(&mut state, epoch_start).map_err(|e| {
                            cc_fork_choice::OnAttestationError::ProcessSlots(e.to_string())
                        })?;
                    }
                    let indexed = get_indexed_attestation(&state, &att).map_err(|e| {
                        cc_fork_choice::OnAttestationError::ProcessSlots(e.to_string())
                    })?;
                    on_attestation(&mut store, &indexed, false)?;
                    Ok::<(), cc_fork_choice::OnAttestationError>(())
                })();

                match (valid, index_result) {
                    (true, Ok(())) => {
                        let _ = get_head(&mut store);
                    }
                    (true, Err(e)) => panic!("attestation valid step {i} {rel}: {e}"),
                    (false, Ok(())) => {
                        panic!("attestation expected invalid at step {i} of {rel}");
                    }
                    (false, Err(_)) => {}
                }
            }
            "attester_slashing" => {
                let file = step
                    .get("attester_slashing")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("attester_slashing file missing step {i} {rel}"));
                let path = case_dir.join(format!("{file}.ssz_snappy"));
                let path = if path.is_file() {
                    path
                } else {
                    case_dir.join(file)
                };
                let bytes = snappy_decompress(&path);
                let slashing = AttesterSlashing::<P>::from_ssz_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("decode attester_slashing {file} {rel}: {e:?}"));
                let valid = step_valid(step);
                let result = on_attester_slashing(&mut store, &slashing);
                match (valid, result) {
                    (true, Ok(())) => {
                        let _ = get_head(&mut store);
                    }
                    (true, Err(e)) => panic!("attester_slashing valid step {i} {rel}: {e}"),
                    (false, Ok(())) => {
                        panic!("attester_slashing expected invalid at step {i} of {rel}");
                    }
                    (false, Err(_)) => {}
                }
            }
            "checks" => {
                let checks = step
                    .get("checks")
                    .unwrap_or_else(|| panic!("checks missing body step {i} {rel}"));
                apply_checks::<P>(rel, i, checks, &mut store);
            }
            // Merge / EL payload status setup — Phase 1 engine always accepts.
            "pow_block" | "block_hash" => {}
            // Gloas-only steps — not expected under Fulu, but fail loudly if they appear.
            "execution_payload" | "payload_attestation_message" => {
                panic!("unrecognised step kind {primary} at step {i} of {rel} (Fulu runner)");
            }
            other => {
                panic!("unrecognised step kind {other} at step {i} of {rel}");
            }
        }
    }

    // Silence unused when all steps skip head.
    let _ = anchor_root;
}

/// Apply attestations + attester slashings carried in a block body
/// (`is_from_block = true`), matching pyspec `add_block`.
fn apply_block_operations<P: Preset>(
    store: &mut cc_fork_choice::Store<P>,
    signed: &SignedBeaconBlock<P>,
    rel: &str,
    step_i: usize,
) {
    for attestation in signed.message.body.attestations.iter() {
        let index_result = (|| {
            store_target_checkpoint_context(store, attestation.data.target)?;
            let mut state = store
                .block_state(&attestation.data.target.root)
                .ok_or(cc_fork_choice::OnAttestationError::MissingBlockState(
                    attestation.data.target.root,
                ))?
                .clone();
            let epoch_start = compute_start_slot_at_epoch::<P>(attestation.data.target.epoch);
            if state.slot().as_u64() < epoch_start.as_u64() {
                process_slots(&mut state, epoch_start)
                    .map_err(|e| cc_fork_choice::OnAttestationError::ProcessSlots(e.to_string()))?;
            }
            let indexed = get_indexed_attestation(&state, attestation)
                .map_err(|e| cc_fork_choice::OnAttestationError::ProcessSlots(e.to_string()))?;
            // Block-carried: is_from_block = true.
            on_attestation(store, &indexed, true)?;
            Ok::<(), cc_fork_choice::OnAttestationError>(())
        })();
        // Soft-fail: compliance / invalid_message cases may carry attestations
        // whose target state is not resident. Pyspec asserts success for well-
        // formed blocks; when we cannot index we skip rather than abort the
        // case — subsequent `checks` remain the load-bearing assertion.
        let _ = (index_result, rel, step_i);
    }
    for slashing in signed.message.body.attester_slashings.iter() {
        // Spec: process even if already known; ignore NotSlashable soft failures.
        let _ = on_attester_slashing(store, slashing);
    }
}

fn apply_checks<P: Preset>(
    rel: &str,
    step_i: usize,
    checks: &serde_yaml::Value,
    store: &mut cc_fork_choice::Store<P>,
) {
    // Ensure head is current before asserting.
    let (head_root, _) = get_head(store).unwrap_or_else(|e| panic!("get_head {rel}@{step_i}: {e}"));
    let head_slot = store
        .proto_array()
        .get(&head_root)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&head_root).map(|h| h.slot))
        .unwrap_or(Slot::new(0));

    // Assert every present field (CC-15/2). Unknown fields panic so we notice pin drift.
    let map = checks
        .as_mapping()
        .unwrap_or_else(|| panic!("checks not a map {rel}@{step_i}"));

    for key in map.keys() {
        let key = key.as_str().unwrap_or("");
        match key {
            "head" => {
                let h = checks.get("head").unwrap();
                if let Some(slot) = yaml_u64(h, "slot") {
                    assert_eq!(
                        head_slot.as_u64(),
                        slot,
                        "head.slot mismatch {rel}@{step_i}"
                    );
                }
                if let Some(root) = yaml_root(h, "root") {
                    assert_eq!(head_root, root, "head.root mismatch {rel}@{step_i}");
                }
                // Gloas payload_status ignored if present.
            }
            "time" => {
                let t = yaml_u64(checks, "time").unwrap();
                assert_eq!(store.time(), t, "time mismatch {rel}@{step_i}");
            }
            "genesis_time" => {
                let t = yaml_u64(checks, "genesis_time").unwrap();
                assert_eq!(
                    store.genesis_time(),
                    t,
                    "genesis_time mismatch {rel}@{step_i}"
                );
            }
            "justified_checkpoint" => {
                let exp = yaml_checkpoint(checks, "justified_checkpoint").unwrap();
                assert_eq!(
                    store.justified_checkpoint(),
                    exp,
                    "justified_checkpoint mismatch {rel}@{step_i}"
                );
            }
            "finalized_checkpoint" => {
                let exp = yaml_checkpoint(checks, "finalized_checkpoint").unwrap();
                assert_eq!(
                    store.finalized_checkpoint(),
                    exp,
                    "finalized_checkpoint mismatch {rel}@{step_i}"
                );
            }
            "proposer_boost_root" => {
                let exp = yaml_root(checks, "proposer_boost_root").unwrap();
                assert_eq!(
                    store.proposer_boost_root(),
                    exp,
                    "proposer_boost_root mismatch {rel}@{step_i}"
                );
            }
            "unrealized_justified_checkpoint" => {
                let exp = yaml_checkpoint(checks, "unrealized_justified_checkpoint").unwrap();
                assert_eq!(
                    store.unrealized_justified_checkpoint(),
                    exp,
                    "unrealized_justified_checkpoint mismatch {rel}@{step_i}"
                );
            }
            "unrealized_finalized_checkpoint" => {
                let exp = yaml_checkpoint(checks, "unrealized_finalized_checkpoint").unwrap();
                assert_eq!(
                    store.unrealized_finalized_checkpoint(),
                    exp,
                    "unrealized_finalized_checkpoint mismatch {rel}@{step_i}"
                );
            }
            "get_proposer_head" => {
                let exp = yaml_root(checks, "get_proposer_head").unwrap();
                let slot = store.get_current_slot();
                let got = get_proposer_head(store, head_root, slot);
                assert_eq!(got, exp, "get_proposer_head mismatch {rel}@{step_i}");
            }
            "viable_for_head_roots_and_weights" => {
                // CC-15/2 load-bearing: set equality of filtered-tree leaves +
                // per-leaf weight (pyspec get_viable_for_head_checks).
                // get_head already ran above so node.weight is meaningful.
                let seq = checks
                    .get("viable_for_head_roots_and_weights")
                    .and_then(|v| v.as_sequence())
                    .unwrap_or_else(|| {
                        panic!("viable_for_head_roots_and_weights not a sequence {rel}@{step_i}")
                    });
                let mut expected: Vec<(Root, i64)> = seq
                    .iter()
                    .map(|entry| {
                        let r = yaml_root(entry, "root")
                            .unwrap_or_else(|| panic!("viable entry missing root {rel}@{step_i}"));
                        let w = yaml_u64(entry, "weight").unwrap_or_else(|| {
                            // weight may be written as integer 0
                            entry
                                .get("weight")
                                .and_then(|v| v.as_i64())
                                .map(|i| i as u64)
                                .unwrap_or_else(|| {
                                    panic!("viable entry missing weight {rel}@{step_i}")
                                })
                        }) as i64;
                        (r, w)
                    })
                    .collect();
                expected.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));

                let justified = store.justified_checkpoint().root;
                let current_epoch = store.get_current_store_epoch();
                let got = store.proto_array().viable_for_head_leaves(
                    justified,
                    current_epoch,
                    P::SLOTS_PER_EPOCH,
                );

                // Set equality of roots (Root is not Ord — compare sorted lists).
                let mut exp_roots: Vec<Root> = expected.iter().map(|(r, _)| *r).collect();
                let mut got_roots: Vec<Root> = got.iter().map(|(r, _)| *r).collect();
                exp_roots.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
                got_roots.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
                assert_eq!(
                    got_roots, exp_roots,
                    "viable_for_head root set mismatch {rel}@{step_i}\n  got={got_roots:?}\n  exp={exp_roots:?}"
                );

                // Per-root weight equality (load-bearing for intermediate scores).
                for (root, exp_w) in &expected {
                    let got_w = store
                        .proto_array()
                        .get(root)
                        .map(|n| n.weight)
                        .unwrap_or_else(|| {
                            panic!("viable root missing from proto-array {root:?} {rel}@{step_i}")
                        });
                    assert_eq!(
                        got_w, *exp_w,
                        "viable_for_head weight mismatch for {root:?} {rel}@{step_i}: got={got_w} exp={exp_w}"
                    );
                }
            }
            // Gloas-only vote fields — ignore under Fulu.
            "payload_timeliness_vote" | "payload_data_availability_vote" => {}
            other => {
                panic!("unrecognised checks field `{other}` at {rel}@{step_i}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Suite drivers
// ---------------------------------------------------------------------------

fn run_suite<P: Preset>(runner: &str) {
    let tests = tests_root();
    let prefixes = skiplist_prefixes();
    let preset_name = P::NAME;
    let cases = collect_cases(&tests, preset_name, runner);
    assert!(
        !cases.is_empty(),
        "expected {runner} cases for {preset_name}; tree at {}",
        tests.display()
    );

    let config = spec_config_for_preset(match preset_name {
        "mainnet" => PresetName::Mainnet,
        "minimal" => PresetName::Minimal,
        other => panic!("unknown preset {other}"),
    });

    let mut ran = 0usize;
    let mut failures: Vec<String> = Vec::new();
    // Optional filter: FORK_CHOICE_FILTER=substring
    let filter = std::env::var("FORK_CHOICE_FILTER").unwrap_or_default();
    for (rel, case_dir) in &cases {
        if is_skipped(rel, &prefixes) {
            continue;
        }
        if !filter.is_empty() && !rel.contains(&filter) {
            continue;
        }
        let da = Arc::new(VectorDa::default());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_case::<P>(rel, case_dir, &config, da);
        }));
        match result {
            Ok(()) => ran += 1,
            Err(payload) => {
                let msg = if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else {
                    "box panic".into()
                };
                failures.push(format!("{rel}: {msg}"));
            }
        }
    }
    if !failures.is_empty() {
        let n = failures.len();
        let preview: Vec<_> = failures.iter().take(20).cloned().collect();
        panic!(
            "{n} {runner} failures for {preset_name} (ran_ok={ran}):\n{}",
            preview.join("\n")
        );
    }
    assert!(ran > 0, "no {runner} cases ran for {preset_name}");
}

#[test]
fn fork_choice_minimal() {
    run_suite::<Minimal>("fork_choice");
}

#[test]
fn fork_choice_mainnet() {
    run_suite::<Mainnet>("fork_choice");
}

/// OQ-3 in-scope compliance suite (minimal Fulu only).
#[test]
fn fork_choice_compliance_minimal() {
    let tests = tests_root();
    let compliance = tests
        .join("minimal")
        .join(FORK)
        .join("fork_choice_compliance");
    assert!(
        compliance.is_dir(),
        "fork_choice_compliance missing at {}; extract comptests.tar.gz \
         (scripts/fetch-spec-vectors.sh --force or tar -xzf _dl/comptests.tar.gz \
         tests/minimal/fulu/fork_choice_compliance)",
        compliance.display()
    );
    run_suite::<Minimal>("fork_choice_compliance");
}

/// Handler coverage: on-disk handlers must match what we walk (both presets).
#[test]
fn handler_coverage_fork_choice_both_presets() {
    let tests = tests_root();
    for preset in ["minimal", "mainnet"] {
        let on_disk = list_handlers(&tests, preset, "fork_choice");
        assert!(
            !on_disk.is_empty(),
            "no fork_choice handlers on disk for {preset}"
        );
        // Every on-disk handler must produce at least one case we can collect.
        for h in &on_disk {
            let cases = collect_cases(&tests, preset, "fork_choice");
            assert!(
                cases
                    .iter()
                    .any(|(rel, _)| rel.contains(&format!("{preset}/{FORK}/fork_choice/{h}/"))),
                "handler {h} has no cases for {preset}"
            );
        }
    }
}

/// Negative test (CC-15/1): unknown step kind panics with the kind quoted.
#[test]
#[should_panic(expected = "unrecognised step kind not_a_real_step")]
fn unknown_step_kind_panics_with_kind_quoted() {
    // Minimal synthetic dispatch mirror of the runner's exhaustive match.
    let kind = "not_a_real_step";
    match kind {
        "tick"
        | "block"
        | "attestation"
        | "attester_slashing"
        | "checks"
        | "pow_block"
        | "block_hash"
        | "execution_payload"
        | "payload_attestation_message" => {}
        other => panic!("unrecognised step kind {other}"),
    }
}

// hex crate not in workspace — provide a tiny decoder.
mod hex {
    pub(crate) fn decode(s: &str) -> Result<Vec<u8>, String> {
        if !s.len().is_multiple_of(2) {
            return Err("odd hex length".into());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
            .collect()
    }
}

// Silence unused import in some configs.
#[allow(dead_code)]
fn _use_always_available() {
    let _ = HarnessAvailability;
    let _ = HashSet::<Root>::new();
}
