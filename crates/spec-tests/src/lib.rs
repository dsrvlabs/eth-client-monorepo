//! Shared consensus-spec vector harness (CC-10a / Architecture §10.1).
//!
//! Implements Phase 0 §7.4's consumption contract:
//! - cache root `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}`
//! - tree root `<cache>/<tag>/tests` with `<tag>` from compile-time `spec-vectors.lock`
//! - readiness: four `.complete-*` markers match lockfile digests
//! - on failure: error Display contains `run scripts/fetch-spec-vectors.sh`
//! - readiness only: this crate never fetches remote artifacts
//!
//! This crate has **zero workspace path dependencies**.

#![allow(missing_docs)]

mod cache;
mod case;
mod coverage;
mod error;
mod lockfile;
mod meta;
mod skiplist;
mod snappy;
mod steps;

pub use cache::resolve_cache_root;
pub use case::{Case, Vectors};
pub use coverage::assert_handler_coverage;
pub use error::{Error, FETCH_HINT};
pub use lockfile::{ARTIFACTS, LOCKFILE_SRC, Lockfile};
pub use meta::{BlsSetting, Meta};
pub use skiplist::{SkipEntry, SkipList};
pub use snappy::{MAX_DECOMPRESSED_BYTES, decompress_block};
pub use steps::{load_steps, load_steps_values};

// Frame helper is crate-private (test / format proof only).
#[cfg(test)]
use snappy::try_decompress_frame;

#[cfg(test)]
mod integration {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    fn real_cache_root() -> PathBuf {
        // Prefer the default cache; tests that need isolation use open_in + temp.
        resolve_cache_root().expect("resolve cache root")
    }

    fn open_real() -> Vectors {
        Vectors::open_in(real_cache_root())
            .expect("real vector cache must be present; run scripts/fetch-spec-vectors.sh")
    }

    #[test]
    fn open_empty_cache_contains_fetch_hint() {
        let dir =
            std::env::temp_dir().join(format!("cc-spec-tests-open-empty-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        let err = Vectors::open_in(&dir).expect_err("empty cache");
        let msg = err.to_string();
        assert!(
            msg.contains(FETCH_HINT),
            "Display must contain `{FETCH_HINT}`, got: {msg}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn runners_mainnet_fulu_contains_ssz_static() {
        let v = open_real();
        let runners = v.runners("mainnet", "fulu").expect("runners");
        assert!(
            runners.contains("ssz_static"),
            "expected ssz_static in {runners:?}"
        );
    }

    #[test]
    fn general_tree_reachable_for_ssz_generic() {
        let v = open_real();
        // general.tar.gz layout: tests/general/phase0/ssz_generic
        let runners = v.runners("general", "phase0").expect("general runners");
        assert!(
            runners.contains("ssz_generic"),
            "general tree must expose ssz_generic (from general.tar.gz); got {runners:?}"
        );
        let handlers = v
            .handlers("general", "phase0", "ssz_generic")
            .expect("handlers");
        assert!(
            !handlers.is_empty(),
            "ssz_generic should have handlers on disk"
        );
    }

    #[test]
    fn corrupt_marker_reports_artifact_and_digests() {
        let lock = Lockfile::parse(LOCKFILE_SRC).expect("lock");
        let dir =
            std::env::temp_dir().join(format!("cc-spec-tests-corrupt-int-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let tag_dir = dir.join(&lock.tag);
        fs::create_dir_all(tag_dir.join("tests")).expect("mkdir");
        for (i, art) in ARTIFACTS.iter().enumerate() {
            let digest = if *art == "general.tar.gz" {
                "deadbeef".to_string()
            } else {
                lock.digests[i].clone()
            };
            fs::write(
                tag_dir.join(format!(".complete-{art}")),
                format!("{digest}\n"),
            )
            .expect("marker");
        }
        let err = Vectors::open_in(&dir).expect_err("corrupt marker");
        let msg = err.to_string();
        assert!(msg.contains("general.tar.gz"), "{msg}");
        assert!(msg.contains(&lock.digests[0]), "expected in {msg}");
        assert!(msg.contains("deadbeef"), "found in {msg}");
        assert!(msg.contains(FETCH_HINT), "{msg}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ssz_snappy_block_decompress_real_case() {
        let v = open_real();
        let cases = v
            .cases("mainnet", "fulu", "ssz_static", "Fork")
            .expect("cases");
        assert!(!cases.is_empty(), "Fork ssz_static cases");
        let case = &cases[0];
        let bytes = case.ssz_bytes("serialized.ssz_snappy").expect("decompress");
        // Fork SSZ is 16 bytes (four uint64 / version fields depending on layout).
        assert!(
            !bytes.is_empty(),
            "decompressed payload must be non-empty (got len {})",
            bytes.len()
        );
        // Known size for Fork container on this pin.
        assert_eq!(bytes.len(), 16, "Fork serialized length");
    }

    #[test]
    fn frame_decoder_rejects_real_vector_block_bytes() {
        let v = open_real();
        let cases = v
            .cases("mainnet", "fulu", "ssz_static", "Fork")
            .expect("cases");
        let path = cases[0].path.join("serialized.ssz_snappy");
        let compressed = fs::read(&path).expect("read");
        // Real vectors are block format; FrameDecoder must Err (not panic).
        let err = try_decompress_frame(&compressed).expect_err("frame rejects block");
        assert!(!err.is_empty());
    }

    #[test]
    fn meta_parses_bls_setting_from_real_case() {
        let v = open_real();
        // Known case with meta.yaml: {bls_setting: 1}
        let cases = v
            .cases("mainnet", "fulu", "operations", "proposer_slashing")
            .expect("cases");
        let with_meta = cases
            .iter()
            .find(|c| c.path.join("meta.yaml").is_file())
            .expect("at least one proposer_slashing case has meta.yaml");
        let meta = with_meta.meta().expect("meta");
        assert_eq!(meta.bls_setting, BlsSetting::Required);
    }

    #[test]
    fn meta_defaults_when_meta_yaml_absent() {
        let v = open_real();
        let cases = v
            .cases("mainnet", "fulu", "ssz_static", "Fork")
            .expect("cases");
        let case = cases
            .iter()
            .find(|c| !c.path.join("meta.yaml").is_file())
            .expect("Fork cases have no meta.yaml");
        let meta = case.meta().expect("default meta");
        assert_eq!(
            meta.bls_setting,
            BlsSetting::Optional,
            "documented default is Optional (0) when meta.yaml is absent"
        );
    }

    #[test]
    fn steps_yaml_loads_for_fork_choice_case() {
        let v = open_real();
        let cases = v
            .cases("mainnet", "fulu", "fork_choice", "on_block")
            .expect("cases");
        let case = cases
            .iter()
            .find(|c| c.path.join("steps.yaml").is_file())
            .expect("on_block has steps.yaml cases");
        let steps = case.steps_values().expect("steps");
        assert!(
            !steps.is_empty(),
            "steps.yaml should be a non-empty sequence"
        );
    }

    #[test]
    fn handler_coverage_both_directions_unit() {
        // Covered in coverage::tests; re-export smoke here for AC checklist.
        let on_disk: BTreeSet<String> = ["a", "extra"].into_iter().map(str::to_string).collect();
        let err = assert_handler_coverage(&["a", "missing"], &on_disk).expect_err("mismatch");
        let msg = err.to_string();
        assert!(msg.contains("missing"), "{msg}");
        assert!(msg.contains("extra"), "{msg}");
    }
}
