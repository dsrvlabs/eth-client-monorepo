//! Hoodi fixture helper (CC-10b).
//!
//! Resolves `${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}/<slot>/`,
//! verifies each SSZ artifact against the committed SHA-256 digests in
//! `hoodi-anchor.toml` / `hoodi-sequence.toml`, and never downloads.
//!
//! Failure [`Display`] always contains the literal
//! `run scripts/fetch-hoodi-fixtures.sh`.

#![allow(dead_code, unreachable_pub)]

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Literal substring every cache-readiness failure must contain (CC-10b).
pub(crate) const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";

/// Default cache directory name under `$HOME/.cache/`.
pub(crate) const DEFAULT_CACHE_DIR_NAME: &str = "cc-hoodi-fixtures";

/// Env var that points at the fixture cache root.
pub(crate) const CACHE_ENV: &str = "HOODI_FIXTURES_CACHE";

// ── errors ──────────────────────────────────────────────────────────────────

/// Errors produced by the Hoodi fixture helper.
#[derive(Debug)]
pub(crate) enum Error {
    /// Cache missing, incomplete, or otherwise not ready.
    CacheNotReady { detail: String },
    /// An artifact's on-disk SHA-256 does not match the committed digest.
    Sha256Mismatch {
        artifact: String,
        expected: String,
        actual: String,
    },
    /// Environment / path resolution failure.
    Env(String),
    /// Filesystem I/O.
    Io {
        path: PathBuf,
        detail: String,
    },
    /// Manifest parse failure.
    Manifest { detail: String },
}

impl Error {
    fn not_ready(detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self::CacheNotReady {
            detail: if detail.contains(FETCH_HINT) {
                detail
            } else {
                format!("{detail}; {FETCH_HINT}")
            },
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CacheNotReady { detail } => write!(f, "{detail}"),
            Self::Sha256Mismatch {
                artifact,
                expected,
                actual,
            } => write!(
                f,
                "SHA256 mismatch for {artifact}: expected {expected}, actual {actual}; {FETCH_HINT}"
            ),
            Self::Env(msg) => write!(f, "environment: {msg}; {FETCH_HINT}"),
            Self::Io { path, detail } => {
                write!(f, "I/O error at {}: {detail}; {FETCH_HINT}", path.display())
            }
            Self::Manifest { detail } => {
                write!(f, "manifest: {detail}; {FETCH_HINT}")
            }
        }
    }
}

impl std::error::Error for Error {}

// ── manifests ───────────────────────────────────────────────────────────────

/// Committed anchor metadata (`hoodi-anchor.toml`).
#[derive(Debug, Clone)]
pub(crate) struct HoodiAnchor {
    pub(crate) slot: u64,
    pub(crate) epoch: u64,
    pub(crate) block_root: String,
    pub(crate) state_root: String,
    pub(crate) genesis_validators_root: String,
    pub(crate) genesis_time: u64,
    pub(crate) provider: String,
    pub(crate) retrieval_date: String,
    pub(crate) block_size: u64,
    pub(crate) state_size: u64,
    pub(crate) block_sha256: String,
    pub(crate) state_sha256: String,
    pub(crate) max_blob_commitment_count: u64,
    pub(crate) sequence_len: u64,
}

/// One slot in the committed 40-slot sequence (`hoodi-sequence.toml`).
#[derive(Debug, Clone)]
pub(crate) struct SequenceSlot {
    pub(crate) slot: u64,
    pub(crate) root: String,
    pub(crate) parent_root: String,
    pub(crate) blob_commitment_count: u64,
    pub(crate) empty: bool,
    pub(crate) ssz_sha256: Option<String>,
    pub(crate) ssz_size: Option<u64>,
}

/// Committed sequence manifest.
#[derive(Debug, Clone)]
pub(crate) struct HoodiSequence {
    pub(crate) anchor_slot: u64,
    pub(crate) start_slot: u64,
    pub(crate) max_blob_commitment_count: u64,
    pub(crate) slots: Vec<SequenceSlot>,
}

/// Directory containing the committed TOML manifests (crate-relative).
pub(crate) fn manifests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Load `hoodi-anchor.toml` from the committed fixtures directory.
pub(crate) fn load_anchor() -> Result<HoodiAnchor, Error> {
    load_anchor_from(&manifests_dir().join("hoodi-anchor.toml"))
}

/// Load `hoodi-sequence.toml` from the committed fixtures directory.
pub(crate) fn load_sequence() -> Result<HoodiSequence, Error> {
    load_sequence_from(&manifests_dir().join("hoodi-sequence.toml"))
}

pub(crate) fn load_anchor_from(path: &Path) -> Result<HoodiAnchor, Error> {
    let text = fs::read_to_string(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })?;
    parse_anchor(&text)
}

pub(crate) fn load_sequence_from(path: &Path) -> Result<HoodiSequence, Error> {
    let text = fs::read_to_string(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })?;
    parse_sequence(&text)
}

fn parse_anchor(text: &str) -> Result<HoodiAnchor, Error> {
    let map = parse_flat_toml(text)?;
    Ok(HoodiAnchor {
        slot: req_u64(&map, "slot")?,
        epoch: req_u64(&map, "epoch")?,
        block_root: req_str(&map, "block_root")?,
        state_root: req_str(&map, "state_root")?,
        genesis_validators_root: req_str(&map, "genesis_validators_root")?,
        genesis_time: req_u64(&map, "genesis_time")?,
        provider: req_str(&map, "provider")?,
        retrieval_date: req_str(&map, "retrieval_date")?,
        block_size: req_u64(&map, "block_size")?,
        state_size: req_u64(&map, "state_size")?,
        block_sha256: req_str(&map, "block_sha256")?,
        state_sha256: req_str(&map, "state_sha256")?,
        max_blob_commitment_count: req_u64(&map, "max_blob_commitment_count")?,
        sequence_len: req_u64(&map, "sequence_len")?,
    })
}

fn parse_sequence(text: &str) -> Result<HoodiSequence, Error> {
    let mut anchor_slot = None;
    let mut start_slot = None;
    let mut max_blobs = None;
    let mut slots = Vec::new();
    let mut cur: Option<std::collections::BTreeMap<String, String>> = None;

    let flush = |cur: &mut Option<std::collections::BTreeMap<String, String>>,
                 slots: &mut Vec<SequenceSlot>|
     -> Result<(), Error> {
        if let Some(m) = cur.take() {
            let empty = m
                .get("empty")
                .map(|v| v == "true")
                .unwrap_or(false);
            slots.push(SequenceSlot {
                slot: req_u64(&m, "slot")?,
                root: m.get("root").cloned().unwrap_or_default(),
                parent_root: m.get("parent_root").cloned().unwrap_or_default(),
                blob_commitment_count: m
                    .get("blob_commitment_count")
                    .map(|v| parse_u64(v, "blob_commitment_count"))
                    .transpose()?
                    .unwrap_or(0),
                empty,
                ssz_sha256: m.get("ssz_sha256").filter(|s| !s.is_empty()).cloned(),
                ssz_size: m
                    .get("ssz_size")
                    .map(|v| parse_u64(v, "ssz_size"))
                    .transpose()?,
            });
        }
        Ok(())
    };

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with("[[") {
            flush(&mut cur, &mut slots)?;
            cur = Some(std::collections::BTreeMap::new());
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let k = k.trim().to_string();
        let v = strip_quotes(v.trim()).to_string();
        if let Some(ref mut m) = cur {
            m.insert(k, v);
        } else {
            match k.as_str() {
                "anchor_slot" => anchor_slot = Some(parse_u64(&v, "anchor_slot")?),
                "start_slot" => start_slot = Some(parse_u64(&v, "start_slot")?),
                "max_blob_commitment_count" => {
                    max_blobs = Some(parse_u64(&v, "max_blob_commitment_count")?)
                }
                _ => {}
            }
        }
    }
    flush(&mut cur, &mut slots)?;

    Ok(HoodiSequence {
        anchor_slot: anchor_slot.ok_or_else(|| Error::Manifest {
            detail: "missing anchor_slot".into(),
        })?,
        start_slot: start_slot.ok_or_else(|| Error::Manifest {
            detail: "missing start_slot".into(),
        })?,
        max_blob_commitment_count: max_blobs.ok_or_else(|| Error::Manifest {
            detail: "missing max_blob_commitment_count".into(),
        })?,
        slots,
    })
}

// ── cache resolution ────────────────────────────────────────────────────────

/// Whether `HOODI_FIXTURES_CACHE` is set in the process environment.
///
/// Cache-dependent tests **skip** when this returns `false` so CI without the
/// Hoodi fixture cache stays green (CC-10b AC).
pub(crate) fn cache_env_is_set() -> bool {
    match std::env::var(CACHE_ENV) {
        Ok(v) => !v.trim().is_empty(),
        Err(_) => false,
    }
}

/// Resolve `${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}`.
pub(crate) fn resolve_cache_root() -> Result<PathBuf, Error> {
    if let Ok(p) = std::env::var(CACHE_ENV) {
        let p = p.trim();
        if p.is_empty() {
            return Err(Error::Env(format!(
                "{CACHE_ENV} is set but empty"
            )));
        }
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("HOME").map_err(|_| {
        Error::Env(format!(
            "HOME is unset and {CACHE_ENV} was not provided"
        ))
    })?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join(DEFAULT_CACHE_DIR_NAME))
}

/// Resolved fixture set: paths + verified digests.
#[derive(Debug)]
pub(crate) struct HoodiFixtures {
    pub(crate) anchor: HoodiAnchor,
    pub(crate) sequence: HoodiSequence,
    pub(crate) slot_dir: PathBuf,
    pub(crate) block_ssz: PathBuf,
    pub(crate) state_ssz: PathBuf,
}

impl HoodiFixtures {
    /// Open the fixture set from the default cache root + committed manifests.
    pub(crate) fn open() -> Result<Self, Error> {
        let root = resolve_cache_root()?;
        Self::open_in(&root)
    }

    /// Open from an explicit cache root (used by tests).
    pub(crate) fn open_in(cache_root: &Path) -> Result<Self, Error> {
        let anchor = load_anchor()?;
        let sequence = load_sequence()?;
        Self::open_with_manifests(cache_root, anchor, sequence)
    }

    /// Open with caller-supplied manifests (tests inject corrupt digests here).
    pub(crate) fn open_with_manifests(
        cache_root: &Path,
        anchor: HoodiAnchor,
        sequence: HoodiSequence,
    ) -> Result<Self, Error> {
        let slot_dir = cache_root.join(anchor.slot.to_string());
        if !slot_dir.is_dir() {
            return Err(Error::not_ready(format!(
                "slot directory missing: {}",
                slot_dir.display()
            )));
        }

        let block_ssz = slot_dir.join("signed_beacon_block.ssz");
        let state_ssz = slot_dir.join("beacon_state.ssz");

        verify_file_sha256(&block_ssz, &anchor.block_sha256, "signed_beacon_block.ssz")?;
        verify_file_sha256(&state_ssz, &anchor.state_sha256, "beacon_state.ssz")?;

        // Size floor: state ≥ 150 MB (same assertion as the fetch script).
        let state_meta = fs::metadata(&state_ssz).map_err(|e| Error::Io {
            path: state_ssz.clone(),
            detail: e.to_string(),
        })?;
        const MIN_STATE: u64 = 150 * 1024 * 1024;
        if state_meta.len() < MIN_STATE {
            return Err(Error::not_ready(format!(
                "beacon_state.ssz size {} < {MIN_STATE} (150 MB floor)",
                state_meta.len()
            )));
        }

        // Sequence SSZ digests (non-empty slots with a committed sha).
        for entry in &sequence.slots {
            if entry.empty {
                continue;
            }
            let Some(ref expected) = entry.ssz_sha256 else {
                continue;
            };
            let path = slot_dir
                .join("sequence")
                .join(format!("{}.ssz", entry.slot));
            verify_file_sha256(&path, expected, &format!("sequence/{}.ssz", entry.slot))?;
        }

        Ok(Self {
            anchor,
            sequence,
            slot_dir,
            block_ssz,
            state_ssz,
        })
    }

    /// Path to a sequence block SSZ, if the slot is non-empty.
    pub(crate) fn sequence_block_ssz(&self, slot: u64) -> Option<PathBuf> {
        let entry = self.sequence.slots.iter().find(|s| s.slot == slot)?;
        if entry.empty {
            return None;
        }
        Some(self.slot_dir.join("sequence").join(format!("{slot}.ssz")))
    }
}

fn verify_file_sha256(path: &Path, expected: &str, artifact: &str) -> Result<(), Error> {
    if !path.is_file() {
        return Err(Error::not_ready(format!(
            "artifact missing: {artifact} at {}",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })?;
    let actual = hex_sha256(&bytes);
    if actual != expected {
        return Err(Error::Sha256Mismatch {
            artifact: artifact.to_string(),
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ── tiny TOML helpers (manifests are deliberately flat) ─────────────────────

fn parse_flat_toml(text: &str) -> Result<std::collections::BTreeMap<String, String>, Error> {
    let mut map = std::collections::BTreeMap::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('[') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        map.insert(k.trim().to_string(), strip_quotes(v.trim()).to_string());
    }
    Ok(map)
}

fn strip_quotes(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

fn parse_u64(v: &str, key: &str) -> Result<u64, Error> {
    v.parse().map_err(|_| Error::Manifest {
        detail: format!("invalid u64 for {key}: {v}"),
    })
}

fn req_u64(map: &std::collections::BTreeMap<String, String>, key: &str) -> Result<u64, Error> {
    let v = map.get(key).ok_or_else(|| Error::Manifest {
        detail: format!("missing key `{key}`"),
    })?;
    parse_u64(v, key)
}

fn req_str(map: &std::collections::BTreeMap<String, String>, key: &str) -> Result<String, Error> {
    map.get(key)
        .cloned()
        .ok_or_else(|| Error::Manifest {
            detail: format!("missing key `{key}`"),
        })
}

/// CI cache key form: `hoodi-fixtures-<slot>-<block_sha256>`.
pub(crate) fn ci_cache_key(anchor: &HoodiAnchor) -> String {
    format!("hoodi-fixtures-{}-{}", anchor.slot, anchor.block_sha256)
}
