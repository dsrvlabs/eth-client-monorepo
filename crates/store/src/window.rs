//! Block serve-window floor + derived `ServeWindow` (Architecture §5.1 / §5.2).
//!
//! # CC-4A — computed floor constant
//!
//! `MIN_EPOCHS_FOR_BLOCK_REQUESTS` was removed from the consensus-specs configs
//! at `v1.7.0-alpha.13`. The authority is the computed function
//! [`compute_min_epochs_for_block_requests`]; any vestigial config field is a
//! **cross-check** only (startup refusal on mismatch).
//!
//! # CC-48 — derived, never assigned
//!
//! [`derive_serve_window`] recomputes `earliest_available_slot` from
//! `AnchorInfo.oldest_block_slot` (`B`), `ColumnInfo.oldest_custodied_column_slot`
//! (`C`), and `ServeWindow.holes`. There is **no** independent setter for
//! `earliest_available_slot` or `cgc` — the only public mutator takes a whole
//! [`ServeWindow`] ([`put_serve_window`]).
//!
//! At M4.4 this module emits **branch 2 only** (`max(B, C)` raised past holes),
//! reproducing Phase 2's answer. The two-branch flip is **`CC-49`**.
//!
//! Incremental maintenance of `B` / `C` / `holes` lives here as pure helpers so
//! backfill and the `CURSOR_TOO_OLD` path can update floors in O(1) without
//! scanning a million slots. Zero-blob slots do **not** break column contiguity.
//!
//! # Security notes
//!
//! - **SEC-4A-1** — arithmetic is **fail-closed** (`checked_div` / `checked_add`);
//!   overflow never wraps to a silent wrong floor.
//! - **SEC-4A-2** (dual authority with Phase 2 `services/p2p`) — **wontfix here**.
//!   Wiring a single authority is `CC-49`.

use std::fs;
use std::path::Path;

use serde::Deserialize;
use ssz::{Decode, Encode};
use ssz_types::VariableList;
use tracing::{error, warn};

use cc_types::{Root, Slot};

use crate::engine::{Batch, Engine, StoreError};
use crate::meta::{
    AnchorInfo, ColumnInfo, KEY_SERVE_WINDOW, ServeWindow, SlotRange, TABLE_META,
};

/// Config scalars that enter the block serve-window floor.
///
/// Architecture §5.1 writes these as fields of `ChainConfig`. They are not yet
/// on [`cc_types::ChainConfig`] (see [`crate::schema::ConfigDigestInput`]), so
/// this thin view carries exactly what the function needs — including the
/// optional vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` field for the cross-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockServeWindowCfg {
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`.
    pub min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`.
    pub churn_limit_quotient: u64,
    /// Vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` if present (Hoodi); `None` on
    /// mainnet-spec configs at `v1.7.0-alpha.13`. Never used as the authority.
    pub min_epochs_for_block_requests: Option<u64>,
}

impl BlockServeWindowCfg {
    /// Construct from the two required scalars with no vestigial field.
    #[must_use]
    pub const fn new(
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
    ) -> Self {
        Self {
            min_validator_withdrawability_delay,
            churn_limit_quotient,
            min_epochs_for_block_requests: None,
        }
    }

    /// Construct with an optional vestigial config field for cross-check.
    #[must_use]
    pub const fn with_vestigial(
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
        min_epochs_for_block_requests: Option<u64>,
    ) -> Self {
        Self {
            min_validator_withdrawability_delay,
            churn_limit_quotient,
            min_epochs_for_block_requests,
        }
    }

    /// Load the three relevant keys from a consensus-specs / eth-clients YAML
    /// config file. Unknown keys are ignored; the two required scalars must be
    /// present.
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, WindowConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| WindowConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_yaml_str(&text)
    }

    /// Parse the three relevant keys from YAML text.
    pub fn from_yaml_str(text: &str) -> Result<Self, WindowConfigError> {
        let raw: RawServeWindowYaml =
            serde_yaml::from_str(text).map_err(|e| WindowConfigError::Yaml(e.to_string()))?;
        Ok(Self {
            min_validator_withdrawability_delay: raw.min_validator_withdrawability_delay,
            churn_limit_quotient: raw.churn_limit_quotient,
            min_epochs_for_block_requests: raw.min_epochs_for_block_requests,
        })
    }
}

/// CC-4A: computed, never read from config. The config field, if present, is a
/// cross-check ([`check_min_epochs_for_block_requests`]).
///
/// Spec (`phase0/p2p-interface.md` @ `v1.7.0-alpha.13`):
/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT // 2`.
///
/// Hoodi / mainnet: `256 + 65536 / 2 = 33024` (see `docs/serve-windows.md`).
///
/// **SEC-4A-1:** uses [`u64::checked_div`] / [`u64::checked_add`] and returns
/// [`WindowConfigError::ArithmeticOverflow`] rather than wrapping. A wrapped
/// floor would under-advertise and under-retain history.
pub fn compute_min_epochs_for_block_requests(
    cfg: &BlockServeWindowCfg,
) -> Result<u64, WindowConfigError> {
    let half = cfg
        .churn_limit_quotient
        .checked_div(2)
        .ok_or(WindowConfigError::ArithmeticOverflow {
            op: "CHURN_LIMIT_QUOTIENT / 2",
            min_validator_withdrawability_delay: cfg.min_validator_withdrawability_delay,
            churn_limit_quotient: cfg.churn_limit_quotient,
        })?;
    cfg.min_validator_withdrawability_delay
        .checked_add(half)
        .ok_or(WindowConfigError::ArithmeticOverflow {
            op: "MIN_VALIDATOR_WITHDRAWABILITY_DELAY + (CHURN_LIMIT_QUOTIENT / 2)",
            min_validator_withdrawability_delay: cfg.min_validator_withdrawability_delay,
            churn_limit_quotient: cfg.churn_limit_quotient,
        })
}

/// Startup cross-check: if the loaded config supplies
/// `MIN_EPOCHS_FOR_BLOCK_REQUESTS`, it must equal the computed value or the
/// node **refuses to start**. Absent field → ok (mainnet-spec configs).
///
/// On success returns the computed floor (the authority). Overflow in the
/// compute path is also a refuse-to-start (SEC-4A-1 fail-closed).
pub fn check_min_epochs_for_block_requests(
    cfg: &BlockServeWindowCfg,
) -> Result<u64, WindowConfigError> {
    let computed = compute_min_epochs_for_block_requests(cfg)?;
    if let Some(stated) = cfg.min_epochs_for_block_requests
        && stated != computed
    {
        return Err(WindowConfigError::MinEpochsMismatch { stated, computed });
    }
    Ok(computed)
}

/// Errors from serve-window config load and the vestigial-field cross-check.
#[derive(Debug, thiserror::Error)]
pub enum WindowConfigError {
    /// Filesystem error reading a config path.
    #[error("failed to read config {path}: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// YAML deserialization failed.
    #[error("yaml parse error: {0}")]
    Yaml(String),
    /// Vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` disagrees with the computed floor.
    ///
    /// `Display` names **both** numbers so an operator can see the mismatch
    /// without re-running the arithmetic by hand (CC-4A /2).
    #[error(
        "MIN_EPOCHS_FOR_BLOCK_REQUESTS mismatch: config has {stated}, computed {computed} \
         (MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2)"
    )]
    MinEpochsMismatch {
        /// Value from the config file.
        stated: u64,
        /// Value from [`compute_min_epochs_for_block_requests`].
        computed: u64,
    },
    /// Checked arithmetic overflow (SEC-4A-1). Refuse rather than wrap.
    #[error(
        "serve-window arithmetic overflow in {op}: \
         MIN_VALIDATOR_WITHDRAWABILITY_DELAY={min_validator_withdrawability_delay}, \
         CHURN_LIMIT_QUOTIENT={churn_limit_quotient}"
    )]
    ArithmeticOverflow {
        /// Which step overflowed (`/ 2` or `+`).
        op: &'static str,
        /// Left operand of the formula.
        min_validator_withdrawability_delay: u64,
        /// Right operand of the formula (before `/ 2`).
        churn_limit_quotient: u64,
    },
}

/// Serde shape for the three keys this module reads from network YAML.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct RawServeWindowYaml {
    min_validator_withdrawability_delay: u64,
    churn_limit_quotient: u64,
    #[serde(default)]
    min_epochs_for_block_requests: Option<u64>,
}

// ---------------------------------------------------------------------------
// CC-48 — derived ServeWindow (branch 2), incremental B / C / holes
// ---------------------------------------------------------------------------

/// Hard bound on `ServeWindow.holes` (Architecture §5.2). A 65th entry is an
/// alarm: the store is too broken to describe precisely.
pub const MAX_SERVE_WINDOW_HOLES: usize = 64;

/// Branch tag written into [`ServeWindow::branch`].
///
/// CC-48 always emits [`WindowBranch::Two`]. CC-49 owns the flip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WindowBranch {
    /// Complete on sidecars over the whole retention period — advertise `B`.
    One = 1,
    /// Incomplete on sidecars — advertise `max(B, C)` (raised past holes).
    Two = 2,
}

impl WindowBranch {
    /// Wire / SSZ tag.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Outcome of appending a hole to the bounded list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoleAppendResult {
    /// Hole recorded (list grew, or an equivalent range already present).
    Appended,
    /// Identical range already present; list unchanged.
    Duplicate,
    /// 65th distinct hole refused — alarm; list unchanged.
    RefusedAtCap,
}

/// Whether a slot is contiguous for column floor `C`.
///
/// A **zero-blob** slot has no columns and does **not** break contiguity.
/// A blob-carrying slot is contiguous only when all custodied indices are present.
#[must_use]
pub const fn column_slot_contiguous(is_zero_blob: bool, all_custodied_present: bool) -> bool {
    is_zero_blob || all_custodied_present
}

/// Raise `base` past every hole that overlaps `[base, +∞)`.
///
/// Contiguous-from-head semantics: any recorded hole inside the advertised
/// range forces the floor up to `hole.end` (honest narrow advertisement).
#[must_use]
pub fn raise_for_holes(base: Slot, holes: &[SlotRange]) -> Slot {
    let mut eas = base.as_u64();
    // Fixpoint: raising past one hole may expose another.
    loop {
        let mut raised = false;
        for h in holes {
            let lo = h.start.as_u64();
            let hi = h.end.as_u64();
            if hi <= lo {
                continue;
            }
            // Overlap of [lo, hi) with [eas, +∞): nonempty iff hi > eas.
            if hi > eas {
                // Hole entirely below eas already filtered (hi <= eas).
                // Straddle or interior → contiguous suffix starts at hi.
                if lo < hi {
                    eas = hi;
                    raised = true;
                }
            }
        }
        if !raised {
            break;
        }
    }
    Slot::new(eas)
}

/// Errors from [`derive_serve_window`] / persist helpers (CC-48).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ServeWindowError {
    /// Caller passed more than [`MAX_SERVE_WINDOW_HOLES`] — refuse, never truncate
    /// (a truncated hole list would free-ride on unrecorded gaps).
    #[error(
        "ServeWindow.holes cap is {cap}; got {got} (refuse 65th — never truncate free-ride)"
    )]
    HoleCapExceeded {
        /// Number of holes supplied.
        got: usize,
        /// Hard cap ([`MAX_SERVE_WINDOW_HOLES`]).
        cap: usize,
    },
}

/// Derive a complete [`ServeWindow`] under **branch 2** (CC-48 / M4.4).
///
/// ```text
/// eas = raise_for_holes(max(B, C), holes)
/// ```
///
/// There is no setter for `earliest_available_slot` or `cgc` independently —
/// callers pass the inputs and receive the whole container.
///
/// **Refuse, never truncate:** `holes.len() > 64` returns
/// [`ServeWindowError::HoleCapExceeded`] so a 65th gap cannot free-ride by
/// being dropped from the record while eas is still computed from a short list.
pub fn derive_serve_window(
    block_floor: Slot,
    column_floor: Slot,
    cgc: u64,
    holes: &[SlotRange],
) -> Result<ServeWindow, ServeWindowError> {
    if holes.len() > MAX_SERVE_WINDOW_HOLES {
        return Err(ServeWindowError::HoleCapExceeded {
            got: holes.len(),
            cap: MAX_SERVE_WINDOW_HOLES,
        });
    }
    let base = Slot::new(block_floor.as_u64().max(column_floor.as_u64()));
    let eas = raise_for_holes(base, holes);
    let holes_list = VariableList::new(holes.to_vec()).map_err(|_| {
        // Length already checked; VariableList capacity is U64 == 64.
        ServeWindowError::HoleCapExceeded {
            got: holes.len(),
            cap: MAX_SERVE_WINDOW_HOLES,
        }
    })?;
    Ok(ServeWindow {
        earliest_available_slot: eas,
        cgc,
        branch: WindowBranch::Two.as_u8(),
        block_floor,
        column_floor,
        holes: holes_list,
    })
}

/// Re-derive from live [`AnchorInfo`] / [`ColumnInfo`] / hole list (branch 2).
pub fn derive_serve_window_from_meta(
    anchor: &AnchorInfo,
    columns: &ColumnInfo,
    holes: &[SlotRange],
) -> Result<ServeWindow, ServeWindowError> {
    derive_serve_window(
        anchor.oldest_block_slot,
        columns.oldest_custodied_column_slot,
        columns.cgc,
        holes,
    )
}

/// Extend block floor `B` when a backfill batch lands the parent of the oldest
/// retained block. O(1).
///
/// Returns `true` when `anchor` was updated.
pub fn extend_block_floor(
    anchor: &mut AnchorInfo,
    landed_slot: Slot,
    landed_root: Root,
    landed_parent: Root,
) -> bool {
    if landed_root != anchor.oldest_block_parent {
        return false;
    }
    // Parent of the previous oldest becomes the new oldest; linkage advances.
    if landed_slot.as_u64() >= anchor.oldest_block_slot.as_u64() {
        // Only extend *downward*.
        return false;
    }
    anchor.oldest_block_slot = landed_slot;
    anchor.oldest_block_parent = landed_parent;
    true
}

/// Try to lower column floor `C` by one slot (the next-older candidate).
///
/// - Zero-blob slots never break contiguity — `C` moves to `candidate`.
/// - Blob-carrying slots require all custodied indices present.
///
/// Returns `true` when `columns.oldest_custodied_column_slot` was lowered.
pub fn try_extend_column_floor(
    columns: &mut ColumnInfo,
    candidate: Slot,
    is_zero_blob: bool,
    all_custodied_present: bool,
) -> bool {
    let current = columns.oldest_custodied_column_slot.as_u64();
    let cand = candidate.as_u64();
    // Only the immediate next-older slot (current − 1), or any lower when
    // walking a contiguous zero-blob run during tests / batch apply.
    if cand >= current {
        return false;
    }
    if !column_slot_contiguous(is_zero_blob, all_custodied_present) {
        return false;
    }
    columns.oldest_custodied_column_slot = candidate;
    true
}

/// Append a hole range. Bounded at [`MAX_SERVE_WINDOW_HOLES`].
///
/// The 65th distinct hole is **refused**, logged as an alarm, and does not
/// mutate `holes`. Callers must refresh metrics from the post-call length.
pub fn append_hole(holes: &mut Vec<SlotRange>, hole: SlotRange) -> HoleAppendResult {
    if hole.end.as_u64() <= hole.start.as_u64() {
        // Degenerate — treat as no-op duplicate rather than pollute the list.
        return HoleAppendResult::Duplicate;
    }
    if holes.iter().any(|h| h.start == hole.start && h.end == hole.end) {
        return HoleAppendResult::Duplicate;
    }
    if holes.len() >= MAX_SERVE_WINDOW_HOLES {
        error!(
            target: "cc_store::window",
            start = hole.start.as_u64(),
            end = hole.end.as_u64(),
            cap = MAX_SERVE_WINDOW_HOLES,
            "ServeWindow.holes cap reached — refusing 65th hole (store too broken to describe)"
        );
        return HoleAppendResult::RefusedAtCap;
    }
    holes.push(hole);
    HoleAppendResult::Appended
}

/// Remove holes fully covered by a successful parent-linkage fill of `[filled_start, filled_end)`.
///
/// Returns the number of hole entries removed.
pub fn shrink_holes_filled(
    holes: &mut Vec<SlotRange>,
    filled_start: Slot,
    filled_end: Slot,
) -> usize {
    let fs = filled_start.as_u64();
    let fe = filled_end.as_u64();
    let before = holes.len();
    holes.retain(|h| {
        let lo = h.start.as_u64();
        let hi = h.end.as_u64();
        // Drop when fully covered by the filled range.
        !(lo >= fs && hi <= fe)
    });
    before.saturating_sub(holes.len())
}

/// Sum of hole lengths (slots) for `cc_storage_window_hole_slots`.
#[must_use]
pub fn hole_slots_total(holes: &[SlotRange]) -> u64 {
    holes.iter().fold(0_u64, |acc, h| {
        acc.saturating_add(h.end.as_u64().saturating_sub(h.start.as_u64()))
    })
}

/// Load the persisted [`ServeWindow`] singleton, if present.
pub fn load_serve_window(engine: &Engine) -> Result<Option<ServeWindow>, StoreError> {
    let rt = engine.read()?;
    let Some(bytes) = rt.get(TABLE_META, KEY_SERVE_WINDOW.as_bytes())? else {
        return Ok(None);
    };
    match ServeWindow::from_ssz_bytes(&bytes) {
        Ok(w) => Ok(Some(w)),
        Err(e) => Err(StoreError::Codec(format!(
            "ServeWindow SSZ decode failed: {e:?}"
        ))),
    }
}

/// Stage a put of the whole [`ServeWindow`] container into `batch`.
///
/// **Only** public mutator for the serve-window record — takes the whole
/// container (no independent `earliest_available_slot` / `cgc` setters).
pub fn put_serve_window(batch: &mut Batch, window: &ServeWindow) {
    batch.put(
        TABLE_META,
        KEY_SERVE_WINDOW.as_bytes(),
        &window.as_ssz_bytes(),
    );
}

/// Derive, put, and commit a [`ServeWindow`] in one batch.
///
/// When `fail_commit` is true the batch is built and then **not** applied
/// (`storage.debug.crash_point = "after_put_before_commit"` / CC-48 /3).
///
/// Hole-cap exceed is mapped to [`StoreError::Codec`] (refuse, never truncate).
pub fn write_derived_serve_window(
    engine: &Engine,
    block_floor: Slot,
    column_floor: Slot,
    cgc: u64,
    holes: &[SlotRange],
    fail_commit: bool,
) -> Result<ServeWindow, StoreError> {
    let window = derive_serve_window(block_floor, column_floor, cgc, holes).map_err(|e| {
        StoreError::Codec(e.to_string())
    })?;
    let mut batch = engine.batch();
    put_serve_window(&mut batch, &window);
    if fail_commit {
        warn!(
            target: "cc_store::window",
            "injected commit failure after put ServeWindow (after_put_before_commit)"
        );
        return Err(StoreError::Engine(
            "injected commit failure after_put_before_commit (CC-48/3)".into(),
        ));
    }
    engine.commit(batch)?;
    Ok(window)
}

/// Convenience: re-derive from meta inputs, put, commit.
pub fn write_serve_window_from_meta(
    engine: &Engine,
    anchor: &AnchorInfo,
    columns: &ColumnInfo,
    holes: &[SlotRange],
    fail_commit: bool,
) -> Result<ServeWindow, StoreError> {
    write_derived_serve_window(
        engine,
        anchor.oldest_block_slot,
        columns.oldest_custodied_column_slot,
        columns.cgc,
        holes,
        fail_commit,
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine::{Durability, EngineOptions};
    use proptest::prelude::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Hoodi / mainnet scalars (V-1, retrieved 2026-08-08 from eth-clients/hoodi).
    const HOODI_WITHDRAWABILITY: u64 = 256;
    const HOODI_CHURN: u64 = 65_536;
    /// Expected floor: 256 + 65536/2. Written only in tests (CC-4A /4).
    const HOODI_FLOOR: u64 = 33_024;

    fn types_fixture(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures")
            .join(name)
    }

    /// CC-4A /1 — first half: Hoodi loaded config → 33 024.
    #[test]
    fn compute_hoodi_fixture_is_33024() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi-config.yaml serve-window fields");
        assert_eq!(cfg.min_validator_withdrawability_delay, HOODI_WITHDRAWABILITY);
        assert_eq!(cfg.churn_limit_quotient, HOODI_CHURN);
        assert_eq!(
            compute_min_epochs_for_block_requests(&cfg).expect("hoodi must compute"),
            HOODI_FLOOR
        );
    }

    /// CC-4A /1 — second half: different `CHURN_LIMIT_QUOTIENT` → different value.
    ///
    /// A test that only asserts `== 33024` would pass with a hard-coded constant;
    /// this half discharges the "computed from cfg fields" claim.
    #[test]
    fn compute_different_churn_limit_quotient_differs() {
        // 256 + 32768/2 = 256 + 16384 = 16640 (issue example: 32768 → 16 640).
        let cfg = BlockServeWindowCfg::new(HOODI_WITHDRAWABILITY, 32_768);
        let value = compute_min_epochs_for_block_requests(&cfg).expect("must compute");
        assert_eq!(value, 16_640);
        assert_ne!(value, HOODI_FLOOR);
    }

    /// SEC-4A-1 — overflow fails closed (no wrap to a silent wrong floor).
    #[test]
    fn compute_overflow_fails_closed() {
        // u64::MAX + (2/2) overflows checked_add.
        let cfg = BlockServeWindowCfg::new(u64::MAX, 2);
        let err = compute_min_epochs_for_block_requests(&cfg).expect_err("must overflow");
        assert!(
            matches!(err, WindowConfigError::ArithmeticOverflow { .. }),
            "got {err:?}"
        );
        // check path is also refuse-to-start.
        let err = check_min_epochs_for_block_requests(&cfg).expect_err("check must refuse");
        assert!(matches!(err, WindowConfigError::ArithmeticOverflow { .. }));
    }

    /// CC-4A /2 positive — Hoodi `config.yaml` vestigial field present and equal → starts.
    #[test]
    fn vestigial_equal_starts() {
        // V-1: Hoodi still carries MIN_EPOCHS_FOR_BLOCK_REQUESTS (fixture mirrors eth-clients/hoodi).
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi-config.yaml");
        assert_eq!(
            cfg.min_epochs_for_block_requests,
            Some(HOODI_FLOOR),
            "hoodi fixture must carry the vestigial field for the equality path"
        );
        let floor = check_min_epochs_for_block_requests(&cfg).expect("equal vestigial must start");
        assert_eq!(floor, HOODI_FLOOR);
    }

    /// CC-4A /2 negative — same config with field mutated to 33023 → refuse with both numbers.
    #[test]
    fn vestigial_unequal_refuses_naming_both() {
        let mut text = std::fs::read_to_string(types_fixture("hoodi-config.yaml")).unwrap();
        // Mutate the vestigial field only (keep the two live scalars).
        text = text.replace(
            "MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33024",
            "MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33023",
        );
        assert!(
            text.contains("MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33023"),
            "mutation must land"
        );
        let cfg = BlockServeWindowCfg::from_yaml_str(&text).expect("mutated hoodi yaml");
        assert_eq!(cfg.min_epochs_for_block_requests, Some(33_023));
        let err = check_min_epochs_for_block_requests(&cfg).expect_err("must refuse");
        let msg = err.to_string();
        match err {
            WindowConfigError::MinEpochsMismatch {
                stated: s,
                computed: c,
            } => {
                assert_eq!(s, 33_023);
                assert_eq!(c, HOODI_FLOOR);
            }
            other => panic!("expected MinEpochsMismatch, got {other:?}"),
        }
        assert!(
            msg.contains("33023"),
            "error must name stated value 33023: {msg}"
        );
        assert!(
            msg.contains("33024"),
            "error must name computed value 33024: {msg}"
        );
    }

    /// CC-4A /3 — mainnet-spec config (field absent) starts normally.
    #[test]
    fn mainnet_absent_field_starts() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("mainnet-config.yaml"))
            .expect("parse mainnet-config.yaml serve-window fields");
        assert_eq!(cfg.min_epochs_for_block_requests, None);
        assert_eq!(cfg.min_validator_withdrawability_delay, HOODI_WITHDRAWABILITY);
        assert_eq!(cfg.churn_limit_quotient, HOODI_CHURN);
        let floor =
            check_min_epochs_for_block_requests(&cfg).expect("absent field must start");
        assert_eq!(floor, HOODI_FLOOR);
    }

    /// Hoodi fixture file itself must also start (vestigial may be absent from
    /// the excerpt; when present and equal it is covered by vestigial_equal).
    #[test]
    fn hoodi_fixture_check_starts() {
        let cfg = BlockServeWindowCfg::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("parse hoodi");
        check_min_epochs_for_block_requests(&cfg).expect("hoodi fixture must start");
    }

    // ── CC-48 derivation ────────────────────────────────────────────────────

    fn hole(start: u64, end: u64) -> SlotRange {
        SlotRange {
            start: Slot::new(start),
            end: Slot::new(end),
        }
    }

    fn temp_engine(label: &str) -> (PathBuf, Engine) {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-window-{label}-{n}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .expect("open engine");
        (dir, eng)
    }

    /// Branch 2: eas = max(B, C) with no holes.
    #[test]
    fn derive_branch_two_is_max_b_c() {
        let w = derive_serve_window(Slot::new(10), Slot::new(20), 4, &[]).unwrap();
        assert_eq!(w.earliest_available_slot, Slot::new(20));
        assert_eq!(w.block_floor, Slot::new(10));
        assert_eq!(w.column_floor, Slot::new(20));
        assert_eq!(w.cgc, 4);
        assert_eq!(w.branch, WindowBranch::Two.as_u8());

        let w2 = derive_serve_window(Slot::new(50), Slot::new(20), 4, &[]).unwrap();
        assert_eq!(w2.earliest_available_slot, Slot::new(50));
    }

    /// Hole at base raises eas to hole.end.
    #[test]
    fn derive_hole_at_eas_raises() {
        let holes = [hole(20, 25)];
        let w = derive_serve_window(Slot::new(10), Slot::new(20), 4, &holes).unwrap();
        assert_eq!(w.earliest_available_slot, Slot::new(25));
    }

    /// Hole near head raises eas past it (contiguous suffix).
    #[test]
    fn derive_hole_near_head_raises() {
        let holes = [hole(99, 100)];
        let w = derive_serve_window(Slot::new(10), Slot::new(10), 4, &holes).unwrap();
        assert_eq!(w.earliest_available_slot, Slot::new(100));
    }

    /// Hole below base is ignored.
    #[test]
    fn derive_hole_below_base_ignored() {
        let holes = [hole(1, 5)];
        let w = derive_serve_window(Slot::new(10), Slot::new(10), 4, &holes).unwrap();
        assert_eq!(w.earliest_available_slot, Slot::new(10));
    }

    /// Zero-blob slot does not break column contiguity; missing custodied does.
    #[test]
    fn zero_blob_slot_does_not_break_c() {
        assert!(column_slot_contiguous(true, false));
        assert!(column_slot_contiguous(true, true));
        assert!(column_slot_contiguous(false, true));
        assert!(!column_slot_contiguous(false, false));

        let mut cols = ColumnInfo {
            cgc: 4,
            oldest_custodied_column_slot: Slot::new(100),
        };
        // Zero-blob at 99 → C moves to 99 even without columns present.
        assert!(try_extend_column_floor(
            &mut cols,
            Slot::new(99),
            /*is_zero_blob=*/ true,
            /*all_custodied_present=*/ false,
        ));
        assert_eq!(cols.oldest_custodied_column_slot, Slot::new(99));

        // Blob-carrying at 98 with missing custodied → C stops.
        assert!(!try_extend_column_floor(
            &mut cols,
            Slot::new(98),
            /*is_zero_blob=*/ false,
            /*all_custodied_present=*/ false,
        ));
        assert_eq!(cols.oldest_custodied_column_slot, Slot::new(99));

        // Blob-carrying with all custodied → C continues.
        assert!(try_extend_column_floor(
            &mut cols,
            Slot::new(98),
            false,
            true,
        ));
        assert_eq!(cols.oldest_custodied_column_slot, Slot::new(98));
    }

    /// holes bound: 65th refused; length stays 64 (truth, not truncated view of a 65th).
    #[test]
    fn holes_bounded_at_64_refuses_65th() {
        let mut holes = Vec::new();
        for i in 0..MAX_SERVE_WINDOW_HOLES {
            let r = append_hole(&mut holes, hole(i as u64 * 2, i as u64 * 2 + 1));
            assert_eq!(r, HoleAppendResult::Appended);
        }
        assert_eq!(holes.len(), 64);
        assert_eq!(hole_slots_total(&holes), 64);

        let r = append_hole(&mut holes, hole(10_000, 10_001));
        assert_eq!(r, HoleAppendResult::RefusedAtCap);
        assert_eq!(holes.len(), 64, "65th must not land");
        assert_eq!(hole_slots_total(&holes), 64);

        // Derive with 64 ok; with 65 must refuse (never truncate free-ride).
        assert!(derive_serve_window(Slot::new(0), Slot::new(0), 4, &holes).is_ok());
        holes.push(hole(10_000, 10_001));
        let err = derive_serve_window(Slot::new(0), Slot::new(0), 4, &holes).unwrap_err();
        assert!(matches!(
            err,
            ServeWindowError::HoleCapExceeded { got: 65, cap: 64 }
        ));
        // Persist path also refuses — nothing lands.
        let (_dir, eng) = temp_engine("hole-cap");
        let write_err = write_derived_serve_window(
            &eng,
            Slot::new(0),
            Slot::new(0),
            4,
            &holes,
            false,
        )
        .unwrap_err();
        assert!(
            write_err.to_string().contains("cap") || write_err.to_string().contains("65"),
            "err={write_err}"
        );
        assert!(load_serve_window(&eng).unwrap().is_none());
    }

    /// I1: derived value decreases as B/C lower; never below what is contiguously present.
    #[test]
    fn i1_decreases_under_backfill_never_below_contiguous() {
        let mut b = Slot::new(100);
        let mut c = Slot::new(100);
        let mut holes: Vec<SlotRange> = Vec::new();
        let mut prev = derive_serve_window(b, c, 4, &holes)
            .unwrap()
            .earliest_available_slot
            .as_u64();

        // Simulate batches lowering B and C, plus a hole that later fills.
        for step in 0..20 {
            if step == 5 {
                assert_eq!(
                    append_hole(&mut holes, hole(90, 92)),
                    HoleAppendResult::Appended
                );
            }
            if step == 12 {
                shrink_holes_filled(&mut holes, Slot::new(90), Slot::new(92));
            }
            b = Slot::new(b.as_u64().saturating_sub(2));
            c = Slot::new(c.as_u64().saturating_sub(1));
            let w = derive_serve_window(b, c, 4, &holes).unwrap();
            let eas = w.earliest_available_slot.as_u64();
            // Never below max(B, C) before hole raise — and never below either floor.
            assert!(eas >= b.as_u64().max(c.as_u64()) || !holes.is_empty());
            let base = b.as_u64().max(c.as_u64());
            assert!(eas >= base || holes.iter().any(|h| h.end.as_u64() > base));
            // Monotone non-increasing once holes are stable or filled.
            if holes.is_empty() {
                assert!(eas <= prev, "eas {eas} rose above {prev} with no holes");
            }
            prev = eas;
        }
    }

    // Property: randomised holes — eas ≥ max(B,C) and ≥ every overlapping hole.end.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn prop_derived_respects_floors_and_holes(
            b in 0u64..1_000,
            c in 0u64..1_000,
            hole_starts in proptest::collection::vec(0u64..1_000, 0..8),
        ) {
            let holes: Vec<SlotRange> = hole_starts
                .into_iter()
                .map(|s| hole(s, s.saturating_add(3)))
                .collect();
            let w = derive_serve_window(Slot::new(b), Slot::new(c), 4, &holes)
                .expect("≤8 holes fits cap");
            let eas = w.earliest_available_slot.as_u64();
            let base = b.max(c);
            prop_assert!(eas >= base, "eas {eas} < base {base}");
            for h in &holes {
                if h.end.as_u64() > base && h.start.as_u64() < h.end.as_u64() {
                    // Every hole that could overlap the window must be behind eas.
                    if h.start.as_u64() < eas || h.end.as_u64() > base {
                        prop_assert!(
                            eas >= h.end.as_u64() || h.end.as_u64() <= base,
                            "hole [{}, {}) not cleared by eas {eas}",
                            h.start.as_u64(),
                            h.end.as_u64(),
                        );
                    }
                }
            }
            // Contiguous present: eas is exactly raise_for_holes(base).
            prop_assert_eq!(eas, raise_for_holes(Slot::new(base), &holes).as_u64());
        }
    }

    /// Sampled-but-not-custodied missing index must not move the window
    /// (carried as criterion: only custodied completeness feeds C).
    #[test]
    fn missing_sampled_not_custodied_does_not_move_window() {
        // Model: C only moves when *custodied* completeness holds.
        // A missing sampled-not-custodied index is invisible to try_extend_column_floor
        // (caller passes all_custodied_present=true even when sampled-only missing).
        let mut cols = ColumnInfo {
            cgc: 4,
            oldest_custodied_column_slot: Slot::new(50),
        };
        assert!(try_extend_column_floor(&mut cols, Slot::new(49), false, true));
        let w =
            derive_serve_window(Slot::new(40), cols.oldest_custodied_column_slot, 4, &[]).unwrap();
        assert_eq!(w.earliest_available_slot, Slot::new(49));
    }

    /// CC-48 /3 (a): no independent setter for eas or cgc in this module.
    #[test]
    fn no_independent_eas_or_cgc_setter() {
        let src = include_str!("window.rs");
        // Split the needle so this assertion text does not false-positive.
        let eas_setter = format!("fn set_{}", "earliest_available_slot");
        let cgc_setter = format!("fn set_{}", "cgc");
        assert!(
            !src.contains(&eas_setter),
            "must not expose independent earliest_available_slot setter"
        );
        assert!(
            !src.contains(&cgc_setter),
            "must not expose independent cgc setter"
        );
        // Sole public mutators take the whole ServeWindow (or derive+put).
        assert!(src.contains("pub fn put_serve_window"));
        assert!(src.contains("pub fn write_derived_serve_window"));
        assert!(src.contains("pub fn derive_serve_window"));
    }

    /// CC-48 /3 (b): abort between put and commit → neither half landed.
    #[test]
    fn fault_injection_after_put_before_commit_lands_nothing() {
        let (_dir, eng) = temp_engine("fault");
        let err = write_derived_serve_window(
            &eng,
            Slot::new(10),
            Slot::new(20),
            4,
            &[],
            /*fail_commit=*/ true,
        )
        .expect_err("must fail");
        assert!(
            err.to_string().contains("after_put_before_commit")
                || err.to_string().contains("injected commit"),
            "err={err}"
        );
        assert!(
            load_serve_window(&eng).expect("load").is_none(),
            "neither eas nor cgc half may land"
        );
    }

    /// Happy path put + load roundtrip (whole container).
    #[test]
    fn put_and_load_serve_window_roundtrip() {
        let (_dir, eng) = temp_engine("roundtrip");
        let holes = [hole(30, 32)];
        let written = write_derived_serve_window(
            &eng,
            Slot::new(10),
            Slot::new(20),
            8,
            &holes,
            false,
        )
        .expect("write");
        assert_eq!(written.earliest_available_slot, Slot::new(32));
        assert_eq!(written.cgc, 8);

        let loaded = load_serve_window(&eng).expect("load").expect("present");
        assert_eq!(loaded, written);
    }

    /// CC-48 /6 — restart invariance: re-derived value ≤ pre-crash value when
    /// floors only move downward (or stay) and holes only grow or stay.
    #[test]
    fn restart_invariance_derived_leq_pre_crash() {
        let (_dir, eng) = temp_engine("restart");
        let pre = write_derived_serve_window(
            &eng,
            Slot::new(100),
            Slot::new(120),
            4,
            &[],
            false,
        )
        .expect("write");
        let pre_eas = pre.earliest_available_slot.as_u64();

        // Simulate restart: load and re-derive from same floors (no progress).
        let loaded = load_serve_window(&eng).unwrap().unwrap();
        let again = derive_serve_window(
            loaded.block_floor,
            loaded.column_floor,
            loaded.cgc,
            loaded.holes.as_ref(),
        )
        .unwrap();
        assert!(
            again.earliest_available_slot.as_u64() <= pre_eas,
            "post-restart {} > pre-crash {pre_eas}",
            again.earliest_available_slot.as_u64()
        );

        // Hole discovered on restart may *raise* (involuntary) — allowed and honest.
        let with_hole = derive_serve_window(
            loaded.block_floor,
            loaded.column_floor,
            loaded.cgc,
            &[hole(120, 125)],
        )
        .unwrap();
        assert_eq!(with_hole.earliest_available_slot, Slot::new(125));
        // That rise is recorded as involuntary; floors themselves did not climb.
        assert_eq!(with_hole.block_floor, loaded.block_floor);
        assert_eq!(with_hole.column_floor, loaded.column_floor);
    }

    /// extend_block_floor only when root matches oldest_block_parent and slot descends.
    #[test]
    fn extend_block_floor_parent_linkage() {
        let mut anchor = AnchorInfo {
            oldest_block_slot: Slot::new(100),
            oldest_block_parent: Root::from_array([1u8; 32]),
            ..AnchorInfo::default()
        };
        let parent_of_landed = Root::from_array([2u8; 32]);
        assert!(extend_block_floor(
            &mut anchor,
            Slot::new(99),
            Root::from_array([1u8; 32]),
            parent_of_landed,
        ));
        assert_eq!(anchor.oldest_block_slot, Slot::new(99));
        assert_eq!(anchor.oldest_block_parent, parent_of_landed);

        // Wrong root — no change.
        assert!(!extend_block_floor(
            &mut anchor,
            Slot::new(98),
            Root::from_array([9u8; 32]),
            Root::default(),
        ));
        assert_eq!(anchor.oldest_block_slot, Slot::new(99));
    }

    /// Three-directory grep hygiene: computation site is this module; no setter.
    #[test]
    fn grep_hygiene_computation_site_no_setter() {
        // Mirrors CC-48 /4 for crates/store: one computation site, no setter.
        let window_src = include_str!("window.rs");
        assert!(window_src.contains("pub fn derive_serve_window"));
        let eas_setter = format!("fn set_{}", "earliest_available_slot");
        assert!(!window_src.contains(&eas_setter));
        // meta defines the container field; derivation assigns only inside derive_*.
        let meta_src = include_str!("meta.rs");
        assert!(meta_src.contains("earliest_available_slot"));
    }
}
