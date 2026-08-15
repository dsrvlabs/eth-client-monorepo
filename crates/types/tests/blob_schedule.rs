//! CC-1G remainder — dual-source `BLOB_SCHEDULE` identity and boundary coverage.
//!
//! **Sources (test layering)**
//! - **File** (CC-10c): committed `hoodi-config.yaml` / `mainnet-config.yaml` through
//!   [`ChainConfig::from_yaml_file`] (shipping parse path).
//! - **API, post-normalisation:** the same entry table the beacon-API edge yields
//!   after JSON → [`BlobParameters`], then [`BlobSchedule::try_from_entries`] —
//!   the **only** validating constructor. This suite does **not** call
//!   `cc_chain::blob_schedule_from_spec`; the real JSON edge is covered in
//!   `cc-chain` unit tests. Here we assert constructor identity after
//!   normalisation and that `get_blob_parameters` agrees at ±1 epoch of every
//!   BPO boundary for both networks and both post-normalisation schedules.
//!
//! **CC-1G/3 fail-before-bind:** typed reject at load/construction is the
//! property under test (Architecture §2.3). Listener observation is not
//! asserted here (wontfix for this issue — process-level bind ordering is
//! CC-19b lifecycle; no `main` rewrites).
//!
//! **CC-1G/4 (optional CC-0K BPO regression):** not run. The CC-0K devnet
//! (`bpo_1_epoch: 5`) is an optional backstop (plan D2); no Phase 1 proof clause
//! depends on it. Recorded here so the criterion is unambiguous.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cc_types::config::{BlobParameters, BlobSchedule, BlobScheduleError, ChainConfig, ConfigError};
use cc_types::preset::Mainnet;
use cc_types::primitives::Epoch;

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn entry(epoch: u64, max: u64) -> BlobParameters {
    BlobParameters {
        epoch: Epoch::new(epoch),
        max_blobs_per_block: max,
    }
}

/// Post-edge-normalisation Hoodi schedule (API and file both yield this table).
fn hoodi_entries() -> Vec<BlobParameters> {
    vec![entry(52_480, 15), entry(54_016, 21)]
}

/// Post-edge-normalisation mainnet schedule.
fn mainnet_entries() -> Vec<BlobParameters> {
    vec![entry(412_672, 15), entry(419_072, 21)]
}

/// Post-edge-normalisation path: same constructor the API source uses after
/// JSON → [`BlobParameters`]. Not the JSON edge itself (see `cc-chain` tests).
fn schedule_from_normalised_entries(entries: Vec<BlobParameters>) -> BlobSchedule {
    BlobSchedule::try_from_entries(entries).expect("fixture schedule must validate")
}

// ── CC-1G/1: both sources → identical BlobSchedule ──────────────────────────

#[test]
fn both_sources_identical_hoodi_and_mainnet() {
    let hoodi_file = ChainConfig::from_yaml_file(fixture("hoodi-config.yaml"))
        .expect("hoodi-config.yaml")
        .blob_schedule;
    let hoodi_api = schedule_from_normalised_entries(hoodi_entries());
    assert_eq!(
        hoodi_file, hoodi_api,
        "Hoodi file source and post-normalisation API table must match"
    );
    assert_eq!(hoodi_file.entries(), hoodi_entries().as_slice());

    let mainnet_file = ChainConfig::from_yaml_file(fixture("mainnet-config.yaml"))
        .expect("mainnet-config.yaml")
        .blob_schedule;
    let mainnet_api = schedule_from_normalised_entries(mainnet_entries());
    assert_eq!(
        mainnet_file, mainnet_api,
        "mainnet file source and post-normalisation API table must match"
    );
    assert_eq!(mainnet_file.entries(), mainnet_entries().as_slice());
}

// ── CC-1G/2 + /3: ±1 epoch boundaries, both networks, both sources ──────────

/// Expected `(epoch_queried, max_blobs, entry_epoch)` rows for Hoodi.
///
/// Boundaries: `52480 → 15`, `54016 → 21`. Pre-first fallback uses Electra base
/// `(ELECTRA_FORK_EPOCH=2048, Electra max blobs=9)`.
fn hoodi_boundary_rows() -> Vec<(u64, u64, u64)> {
    vec![
        // pre-first / Fulu window
        (50_688, 9, 2_048),
        (52_479, 9, 2_048),
        // first BPO ±1
        (52_479, 9, 2_048),
        (52_480, 15, 52_480),
        (52_481, 15, 52_480),
        // second BPO ±1
        (54_015, 15, 52_480),
        (54_016, 21, 54_016),
        (54_017, 21, 54_016),
    ]
}

/// Expected rows for mainnet: `412672 → 15`, `419072 → 21`; Electra base epoch
/// `364032`, base max `9`.
fn mainnet_boundary_rows() -> Vec<(u64, u64, u64)> {
    vec![
        (411_392, 9, 364_032),
        (412_671, 9, 364_032),
        (412_672, 15, 412_672),
        (412_673, 15, 412_672),
        (419_071, 15, 412_672),
        (419_072, 21, 419_072),
        (419_073, 21, 419_072),
    ]
}

fn assert_boundary_table(cfg: &ChainConfig, rows: &[(u64, u64, u64)], label: &str) {
    for &(query_epoch, want_max, want_entry_epoch) in rows {
        let got = cfg.get_blob_parameters::<Mainnet>(Epoch::new(query_epoch));
        assert_eq!(
            got,
            BlobParameters {
                epoch: Epoch::new(want_entry_epoch),
                max_blobs_per_block: want_max,
            },
            "{label}: epoch {query_epoch}"
        );
    }
}

#[test]
fn boundary_plus_minus_one_both_networks_both_sources() {
    // File source
    let hoodi_file = ChainConfig::from_yaml_file(fixture("hoodi-config.yaml")).expect("hoodi file");
    let mainnet_file =
        ChainConfig::from_yaml_file(fixture("mainnet-config.yaml")).expect("mainnet file");
    assert_boundary_table(&hoodi_file, &hoodi_boundary_rows(), "hoodi/file");
    assert_boundary_table(&mainnet_file, &mainnet_boundary_rows(), "mainnet/file");

    // API source after edge normalisation: same schedule table + electra epochs.
    let hoodi_api = ChainConfig {
        blob_schedule: schedule_from_normalised_entries(hoodi_entries()),
        ..hoodi_file.clone()
    };
    let mainnet_api = ChainConfig {
        blob_schedule: schedule_from_normalised_entries(mainnet_entries()),
        ..mainnet_file.clone()
    };
    assert_eq!(hoodi_api.blob_schedule, hoodi_file.blob_schedule);
    assert_eq!(mainnet_api.blob_schedule, mainnet_file.blob_schedule);
    assert_boundary_table(&hoodi_api, &hoodi_boundary_rows(), "hoodi/api-normalised");
    assert_boundary_table(
        &mainnet_api,
        &mainnet_boundary_rows(),
        "mainnet/api-normalised",
    );
}

// ── CC-1G/3: pre-first fallback + reject malformed / non-monotonic ──────────

#[test]
fn pre_first_entry_falls_back_to_electra_base() {
    let hoodi = ChainConfig::from_yaml_file(fixture("hoodi-config.yaml")).unwrap();
    let before = hoodi.get_blob_parameters::<Mainnet>(Epoch::new(0));
    assert_eq!(
        before,
        BlobParameters {
            epoch: Epoch::new(2_048), // ELECTRA_FORK_EPOCH
            max_blobs_per_block: 9,   // Electra-era base (preset fallback)
        }
    );

    let mainnet = ChainConfig::from_yaml_file(fixture("mainnet-config.yaml")).unwrap();
    let before = mainnet.get_blob_parameters::<Mainnet>(Epoch::new(0));
    assert_eq!(
        before,
        BlobParameters {
            epoch: Epoch::new(364_032),
            max_blobs_per_block: 9,
        }
    );
}

#[test]
fn malformed_and_non_monotonic_rejected_at_load_both_sources() {
    // --- shared constructor (both sources end here) ---
    assert!(matches!(
        BlobSchedule::try_from_entries(vec![]),
        Err(BlobScheduleError::Empty)
    ));
    assert!(matches!(
        BlobSchedule::try_from_entries(vec![entry(100, 15), entry(50, 21)]),
        Err(BlobScheduleError::Unsorted { .. })
    ));
    assert!(matches!(
        BlobSchedule::try_from_entries(vec![entry(100, 15), entry(100, 21)]),
        Err(BlobScheduleError::DuplicateEpoch(_))
    ));

    // --- file source: non-monotonic BLOB_SCHEDULE in YAML fails at parse ---
    let bad_yaml = r#"
PRESET_BASE: mainnet
CONFIG_NAME: bad
GENESIS_FORK_VERSION: 0x00000000
ALTAIR_FORK_VERSION: 0x01000000
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_VERSION: 0x02000000
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_VERSION: 0x03000000
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_VERSION: 0x04000000
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_VERSION: 0x05000000
ELECTRA_FORK_EPOCH: 0
FULU_FORK_VERSION: 0x06000000
FULU_FORK_EPOCH: 0
SECONDS_PER_SLOT: 12
DEPOSIT_CHAIN_ID: 1
DEPOSIT_CONTRACT_ADDRESS: 0x00000000219ab540356cBB839Cbe05303d7705Fa
BLOB_SCHEDULE:
  - EPOCH: 100
    MAX_BLOBS_PER_BLOCK: 15
  - EPOCH: 50
    MAX_BLOBS_PER_BLOCK: 21
"#;
    let err = ChainConfig::from_yaml_str(bad_yaml).unwrap_err();
    assert!(
        matches!(
            err,
            ConfigError::BlobSchedule(BlobScheduleError::Unsorted { .. })
        ),
        "file source must reject non-monotonic at config load: {err:?}"
    );

    // Empty BLOB_SCHEDULE (default when key omitted is empty vec → Empty).
    let empty_yaml = r#"
PRESET_BASE: mainnet
CONFIG_NAME: empty-blob
GENESIS_FORK_VERSION: 0x00000000
ALTAIR_FORK_VERSION: 0x01000000
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_VERSION: 0x02000000
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_VERSION: 0x03000000
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_VERSION: 0x04000000
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_VERSION: 0x05000000
ELECTRA_FORK_EPOCH: 0
FULU_FORK_VERSION: 0x06000000
FULU_FORK_EPOCH: 0
SECONDS_PER_SLOT: 12
DEPOSIT_CHAIN_ID: 1
DEPOSIT_CONTRACT_ADDRESS: 0x00000000219ab540356cBB839Cbe05303d7705Fa
BLOB_SCHEDULE: []
"#;
    let err = ChainConfig::from_yaml_str(empty_yaml).unwrap_err();
    assert!(
        matches!(err, ConfigError::BlobSchedule(BlobScheduleError::Empty)),
        "file source must reject empty schedule at config load: {err:?}"
    );

    // --- API source (post-normalisation): same constructor, same rejection ---
    // (JSON edge cases live in cc-chain; validation identity is what matters here.)
    assert!(
        BlobSchedule::try_from_entries(vec![entry(200, 9), entry(100, 15)]).is_err(),
        "API-normalised non-monotonic entries must fail the validating constructor"
    );
}

/// CC-1G/3 (type-level): a bad schedule cannot produce a `ChainConfig`.
///
/// Listener observation is **not** asserted (wontfix F1 for this issue).
/// Architecture §2.3 makes fail-before-bind a property of the validating
/// constructor / `TryFrom`; process-level “port still free” belongs with
/// CC-19b lifecycle tests, not a `main` rewrite here.
#[test]
fn bad_schedule_yields_no_chain_config() {
    let bad = r#"
PRESET_BASE: mainnet
CONFIG_NAME: no-bind
GENESIS_FORK_VERSION: 0x00000000
ALTAIR_FORK_VERSION: 0x01000000
ALTAIR_FORK_EPOCH: 0
BELLATRIX_FORK_VERSION: 0x02000000
BELLATRIX_FORK_EPOCH: 0
CAPELLA_FORK_VERSION: 0x03000000
CAPELLA_FORK_EPOCH: 0
DENEB_FORK_VERSION: 0x04000000
DENEB_FORK_EPOCH: 0
ELECTRA_FORK_VERSION: 0x05000000
ELECTRA_FORK_EPOCH: 0
FULU_FORK_VERSION: 0x06000000
FULU_FORK_EPOCH: 0
SECONDS_PER_SLOT: 12
DEPOSIT_CHAIN_ID: 1
DEPOSIT_CONTRACT_ADDRESS: 0x00000000219ab540356cBB839Cbe05303d7705Fa
BLOB_SCHEDULE:
  - EPOCH: 10
    MAX_BLOBS_PER_BLOCK: 15
  - EPOCH: 10
    MAX_BLOBS_PER_BLOCK: 21
"#;
    let result = ChainConfig::from_yaml_str(bad);
    assert!(
        result.is_err(),
        "duplicate-epoch schedule must fail config load before any bind"
    );
    // Typed: no Ok(ChainConfig) exists to hand to bootstrap/serve.
    assert!(matches!(
        result.unwrap_err(),
        ConfigError::BlobSchedule(BlobScheduleError::DuplicateEpoch(_))
    ));
}
