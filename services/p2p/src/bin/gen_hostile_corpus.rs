//! One-shot generator for CC-22e hostile-input seed corpus.
//!
//! ```text
//! cargo run -p cc-p2p --bin gen_hostile_corpus --locked
//! ```

use std::fs;
use std::path::PathBuf;

use cc_types::operations::{
    Attestation, AttesterSlashing, ProposerSlashing, SignedAggregateAndProof,
    SignedBlsToExecutionChange, SignedContributionAndProof, SignedVoluntaryExit,
    SyncCommitteeMessage,
};
use cc_types::preset::Mainnet;
use cc_types::sidecar::DataColumnSidecar;
use cc_types::SignedBeaconBlock;
use ssz::Encode;

fn main() {
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/corpus");
    let seeds = out.join("seeds");
    let sample = out.join("sample_capture");
    let slot_dir = sample.join("slot_000001");
    let ops = sample.join("ops");
    fs::create_dir_all(&seeds).expect("seeds dir");
    fs::create_dir_all(&slot_dir).expect("slot dir");
    fs::create_dir_all(&ops).expect("ops dir");

    let pairs: Vec<(&str, Vec<u8>)> = vec![
        (
            "beacon_block",
            SignedBeaconBlock::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "beacon_aggregate_and_proof",
            SignedAggregateAndProof::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "beacon_attestation",
            Attestation::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "data_column_sidecar",
            DataColumnSidecar::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "sync_committee_contribution_and_proof",
            SignedContributionAndProof::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "sync_committee",
            SyncCommitteeMessage::default().as_ssz_bytes(),
        ),
        (
            "voluntary_exit",
            SignedVoluntaryExit::default().as_ssz_bytes(),
        ),
        (
            "proposer_slashing",
            ProposerSlashing::default().as_ssz_bytes(),
        ),
        (
            "attester_slashing",
            AttesterSlashing::<Mainnet>::default().as_ssz_bytes(),
        ),
        (
            "bls_to_execution_change",
            SignedBlsToExecutionChange::default().as_ssz_bytes(),
        ),
    ];

    for (name, bytes) in &pairs {
        assert!(!bytes.is_empty(), "{name} seed empty");
        let path = seeds.join(format!("{name}.ssz"));
        fs::write(&path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        println!("wrote {} ({} bytes)", path.display(), bytes.len());
    }

    // sample_capture for corpus-from-capture.sh
    fs::write(
        slot_dir.join("block.ssz"),
        SignedBeaconBlock::<Mainnet>::default().as_ssz_bytes(),
    )
    .unwrap();
    fs::write(
        slot_dir.join("column_000.ssz"),
        DataColumnSidecar::<Mainnet>::default().as_ssz_bytes(),
    )
    .unwrap();
    for (name, bytes) in &pairs {
        if *name == "beacon_block" || *name == "data_column_sidecar" {
            continue;
        }
        fs::write(ops.join(format!("{name}.ssz")), bytes).unwrap();
    }
    println!("wrote sample_capture under {}", sample.display());
}
