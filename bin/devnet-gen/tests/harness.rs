//! CC-2Ja acceptance tests: config, sidecar verify, proposer/sig, dual-run, golden.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use cc_devnet_gen::chain::verify_block_proposer_sig;
use cc_devnet_gen::config_emit::{assert_devnet_config, render_config_yaml, write_config};
use cc_devnet_gen::generate;
use cc_devnet_gen::genesis::build_genesis;
use cc_devnet_gen::inclusion::{
    BLOB_KZG_COMMITMENTS_FIELD_INDEX, kzg_commitments_inclusion_proof, kzg_commitments_leaf,
    verify_inclusion_proof,
};
use cc_devnet_gen::keys::derive_keys;
use cc_devnet_gen::kzg_columns::{load_kzg, verify_column_cells};
use cc_devnet_gen::manifest::ExpectedManifest;
use cc_devnet_gen::params::DevnetParams;
use cc_state_transition::{
    get_beacon_proposer_index, helpers::misc::is_valid_merkle_branch, process_slots,
};
use cc_types::config::ChainConfig;
use cc_types::preset::Minimal;
use cc_types::primitives::{Epoch, Root, Slot};
use cc_types::{DataColumnSidecar, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, NUMBER_OF_COLUMNS};
use tree_hash::TreeHash;

fn test_out(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "cc-devnet-gen-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

/// Repo-root `devnet/` (worktree-aware): CARGO_MANIFEST_DIR is `bin/devnet-gen`.
fn repo_devnet_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("devnet")
}

/// Structural checks mirroring DA paths (CC-24b-style, no named API yet).
fn assert_sidecar_structure<P: cc_types::preset::Preset>(
    sc: &DataColumnSidecar<P>,
    expected_blobs: usize,
) {
    assert!(
        sc.index < NUMBER_OF_COLUMNS,
        "column index {} out of range",
        sc.index
    );
    assert_eq!(sc.column.len(), expected_blobs, "column cell count");
    assert_eq!(sc.kzg_commitments.len(), expected_blobs, "commitments");
    assert_eq!(sc.kzg_proofs.len(), expected_blobs, "proofs");
    assert_eq!(
        sc.kzg_commitments_inclusion_proof.len(),
        KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
    );
    if expected_blobs > 0 {
        assert!(!sc.column.is_empty());
    }
}

fn verify_sidecar_file(
    path: &Path,
    block: &cc_types::SignedBeaconBlock<Minimal>,
    kzg: &impl cc_crypto::CellKzg,
) {
    let sc_bytes = std::fs::read(path).unwrap();
    let sc: DataColumnSidecar<Minimal> =
        ssz::Decode::from_ssz_bytes(&sc_bytes).expect("decode sidecar");
    let n_blobs = block.message.body.blob_kzg_commitments.len();
    assert_sidecar_structure(&sc, n_blobs);
    assert_eq!(
        sc.kzg_commitments.as_ref(),
        block.message.body.blob_kzg_commitments.as_ref()
    );
    assert!(verify_inclusion_proof(
        &block.message.body,
        &sc.kzg_commitments_inclusion_proof
    ));
    let leaf = kzg_commitments_leaf(&block.message.body);
    let body_root = Root::from_hash256(block.message.body.tree_hash_root());
    let branch: Vec<Root> = sc.kzg_commitments_inclusion_proof.iter().copied().collect();
    assert!(is_valid_merkle_branch(
        leaf,
        &branch,
        KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize,
        BLOB_KZG_COMMITMENTS_FIELD_INDEX as u64,
        body_root,
    ));
    let bundle = cc_devnet_gen::kzg_columns::ColumnBundle {
        index: sc.index,
        cells: sc.column.to_vec(),
        commitments: sc.kzg_commitments.to_vec(),
        proofs: sc.kzg_proofs.to_vec(),
    };
    assert!(
        verify_column_cells(kzg, &bundle).unwrap(),
        "cell KZG failed for column {} at {}",
        sc.index,
        path.display()
    );
}

#[test]
fn config_yaml_fulu_zero_and_bpo_loads_via_chain_config() {
    let params = DevnetParams::for_test(4, [0x11; 32]);
    let yaml = render_config_yaml(&params);
    assert!(yaml.contains("FULU_FORK_EPOCH: 0"));
    assert!(yaml.contains("EPOCH: 5"));
    assert!(yaml.contains("EPOCH: 10"));
    assert!(yaml.contains("SECONDS_PER_SLOT: 3"));
    // 500 * 3 / 12 = 125
    assert!(yaml.contains("MAXIMUM_GOSSIP_CLOCK_DISPARITY: 125ms"));

    let cfg = ChainConfig::from_yaml_str(&yaml).expect("ChainConfig parse");
    assert_devnet_config(&cfg, &params).unwrap();
    assert_eq!(cfg.fulu_fork_epoch, Epoch::new(0));
    assert_eq!(cfg.blob_schedule.entries()[0].epoch, Epoch::new(5));
    assert_eq!(cfg.blob_schedule.entries()[1].epoch, Epoch::new(10));
    assert_eq!(cfg.seconds_per_slot, 3);
}

#[test]
fn disparity_scales_with_slot_time() {
    let mut p = DevnetParams::for_test(1, [0x22; 32]);
    p.seconds_per_slot = 3;
    assert_eq!(p.gossip_disparity_ms(), 125);
    p.seconds_per_slot = 12;
    assert_eq!(p.gossip_disparity_ms(), 500);
    p.seconds_per_slot = 6;
    assert_eq!(p.gossip_disparity_ms(), 250);
    p.maximum_gossip_clock_disparity_ms = Some(999);
    assert_eq!(p.gossip_disparity_ms(), 999);
}

#[test]
fn short_chain_generation_sidecar_and_proposer() {
    let out = test_out("short");
    let mut params = DevnetParams::for_test(4, [0xab; 32]);
    params.output_dir = out.clone();
    // Keep blob counts small for speed.
    params.blobs_per_block_cycle = vec![1, 2, 1, 2];

    let result = generate(&params, Some(&out)).expect("generate");
    assert_eq!(result.blobs_per_block.len(), 4);
    assert!(result.blobs_per_block.iter().all(|&n| n > 0));
    assert!(
        result
            .blobs_per_block
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1,
        "blob counts must vary"
    );

    // Config on disk loads via ChainConfig (network YAML path).
    let cfg = ChainConfig::from_yaml_file(out.join("config.yaml")).unwrap();
    assert_devnet_config(&cfg, &params).unwrap();

    assert!(out.join("manifest.json").is_file());
    assert!(out.join("genesis.ssz").is_file());

    // Key files are mode 0o600 on Unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(out.join("keys/validator_00000.json")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }

    // Reload genesis + advance to first block slot to check proposer match.
    let seed = params.seed_bytes().unwrap();
    let keys = derive_keys(&seed, params.validator_count).unwrap();
    let mut state = build_genesis::<Minimal>(&params, &keys, &cfg).unwrap();
    process_slots(&mut state, Slot::new(1), &cfg).unwrap();
    let expected_proposer = get_beacon_proposer_index(&state).unwrap();

    let first_root = result.block_roots[0];
    assert_ne!(first_root, Root::ZERO);

    let block_bytes = std::fs::read(out.join("chain/slot_000001/block.ssz")).unwrap();
    let block: cc_types::SignedBeaconBlock<Minimal> =
        ssz::Decode::from_ssz_bytes(&block_bytes).expect("decode block");
    assert_eq!(block.message.proposer_index, expected_proposer);
    assert_eq!(
        Root::from_hash256(tree_hash::TreeHash::tree_hash_root(&block.message)),
        first_root
    );

    let pk = keys[expected_proposer.as_u64() as usize]
        .secret
        .public_key();
    assert!(
        verify_block_proposer_sig(&state.fork(), state.genesis_validators_root(), &block, &pk,),
        "proposer signature must verify"
    );

    // Sidecars: columns 0 and 127 (breadth) + structural + inclusion + cell KZG.
    let kzg = load_kzg().unwrap();
    verify_sidecar_file(&out.join("chain/slot_000001/column_000.ssz"), &block, &kzg);
    verify_sidecar_file(&out.join("chain/slot_000001/column_127.ssz"), &block, &kzg);
    // Also sample column mid-range on a multi-blob slot (slot 2 has 2 blobs).
    let block2_bytes = std::fs::read(out.join("chain/slot_000002/block.ssz")).unwrap();
    let block2: cc_types::SignedBeaconBlock<Minimal> =
        ssz::Decode::from_ssz_bytes(&block2_bytes).unwrap();
    assert_eq!(block2.message.body.blob_kzg_commitments.len(), 2);
    verify_sidecar_file(&out.join("chain/slot_000002/column_064.ssz"), &block2, &kzg);

    // Dual-run identity.
    let out2 = test_out("short2");
    let result2 = generate(&params, Some(&out2)).expect("second generate");
    assert!(
        result.manifest.roots_match(&result2.manifest),
        "two runs must match roots"
    );

    let _ = std::fs::remove_dir_all(&out);
    let _ = std::fs::remove_dir_all(&out2);
}

#[test]
fn inclusion_proof_matches_body_root() {
    let body = cc_types::BeaconBlockBody::<Minimal>::default();
    let proof = kzg_commitments_inclusion_proof(&body);
    assert!(verify_inclusion_proof(&body, &proof));
}

/// Bind committed `devnet/expected-manifest.json` to `devnet.toml` + genesis GVR.
///
/// Cheap: no 512-slot chain. Full head-root regen is release/manual (~2.5 min);
/// when `devnet/out/manifest.json` exists (local golden run), head is checked too.
#[test]
fn expected_manifest_matches_devnet_toml_and_genesis_gvr() {
    let devnet = repo_devnet_dir();
    let toml_path = devnet.join("devnet.toml");
    let expected_path = devnet.join("expected-manifest.json");
    assert!(toml_path.is_file(), "missing {}", toml_path.display());
    assert!(
        expected_path.is_file(),
        "missing {}",
        expected_path.display()
    );

    let params = DevnetParams::from_toml_file(&toml_path).unwrap();
    let expected = ExpectedManifest::load(&expected_path).unwrap();
    expected.matches_params(&params).unwrap();

    // Production fixture must stay ≥512 slots.
    assert!(
        expected.slot_count >= 512,
        "committed expected slot_count must be ≥512, got {}",
        expected.slot_count
    );
    assert!(params.slot_count >= 512);

    let gvr = expected
        .genesis_validators_root
        .as_ref()
        .expect("expected-manifest must pin genesis_validators_root");
    let head = expected
        .head_block_root
        .as_ref()
        .expect("expected-manifest must pin head_block_root");
    assert!(gvr.starts_with("0x") && gvr.len() == 66);
    assert!(head.starts_with("0x") && head.len() == 66);

    // Recompute GVR from genesis only (seed + validators); fails on key/genesis drift.
    let seed = params.seed_bytes().unwrap();
    let keys = derive_keys(&seed, params.validator_count).unwrap();
    let cfg = ChainConfig::from_yaml_str(&render_config_yaml(&params)).unwrap();
    let state = build_genesis::<Minimal>(&params, &keys, &cfg).unwrap();
    let recomputed = format!(
        "0x{}",
        hex::encode(state.genesis_validators_root().as_array())
    );
    assert_eq!(
        &recomputed, gvr,
        "genesis_validators_root drifted from expected-manifest.json"
    );

    // If a full fixture is present, bind head + full roots to the golden.
    let out_manifest = devnet.join("out/manifest.json");
    if out_manifest.is_file() {
        let m = cc_devnet_gen::manifest::Manifest::load(&out_manifest).unwrap();
        expected.assert_roots_match(&m).unwrap();
        assert_eq!(m.slot_count, expected.slot_count);
        assert_eq!(m.block_roots.len() as u64, expected.slot_count);
    }
}

#[test]
fn dag_forbids_proto_and_libp2p_declared() {
    // DAG ceiling is checked by scripts/check-crate-dag.sh (allow-list).
    // This test asserts forbidden edges are absent from package metadata.
    let manifest =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .unwrap();
    assert!(
        !manifest.contains("cc-proto"),
        "cc-devnet-gen must not depend on cc-proto"
    );
    assert!(
        !manifest.contains("cc-libp2p"),
        "cc-devnet-gen must not depend on cc-libp2p"
    );
    assert!(manifest.contains("cc-types"));
    assert!(manifest.contains("cc-crypto"));
    assert!(manifest.contains("cc-state-transition"));
    // cc-config is an allowed edge but intentionally unused (network YAML uses
    // cc_types::ChainConfig). Do not require it present.
}

#[test]
fn write_config_roundtrip() {
    let out = test_out("cfg");
    let params = DevnetParams::for_test(1, [0x33; 32]);
    let path = out.join("config.yaml");
    let cfg = write_config(&path, &params).unwrap();
    assert_eq!(cfg.config_name, params.config_name);
    let _ = std::fs::remove_dir_all(&out);
}
