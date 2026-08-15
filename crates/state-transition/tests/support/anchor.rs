//! Hoodi anchor loader for the R-11 falsifiers (S0a-A-01).
//!
//! Bytes live in the fixture cache, not git. Decode uses the raw fork-context
//! constructor so `StateCaches` stay at `Default`.

#![allow(dead_code, unreachable_pub)]

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use cc_types::{BeaconState, ChainConfig, ForkName, Mainnet, SignedBeaconBlock};
use sha2::{Digest, Sha256};

/// Literal substring every cache-readiness failure must contain (CC-10b).
pub const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";

/// Env var that points at the fixture cache root.
pub const CACHE_ENV: &str = "HOODI_FIXTURES_CACHE";

const DEFAULT_CACHE_DIR_NAME: &str = "cc-hoodi-fixtures";
const MIN_STATE_BYTES: u64 = 150 * 1024 * 1024;
const ANCHOR_TOML: &str = "hoodi-anchor.toml";
const CONFIG_YAML: &str = "hoodi-config.yaml";

// ── errors ──────────────────────────────────────────────────────────────────

/// Failures from pin / cache resolution or SSZ decode.
#[derive(Debug)]
pub enum Error {
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
    Io { path: PathBuf, detail: String },
    /// Committed pin or config parse failure.
    Manifest { detail: String },
    /// SSZ decode of the block or state failed.
    Decode {
        artifact: &'static str,
        detail: String,
    },
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
            Self::Manifest { detail } => write!(f, "manifest: {detail}"),
            Self::Decode { artifact, detail } => {
                write!(f, "SSZ decode failed for {artifact}: {detail}")
            }
        }
    }
}

impl std::error::Error for Error {}

// ── pin + paths ─────────────────────────────────────────────────────────────

/// Committed Hoodi pin fields needed to locate and verify the SSZ pair.
#[derive(Debug, Clone)]
pub struct HoodiAnchorPin {
    pub slot: u64,
    pub block_root: String,
    pub state_root: String,
    pub block_sha256: String,
    pub state_sha256: String,
    pub state_size: u64,
}

/// Resolved on-disk paths for the committed Hoodi pair + config.
#[derive(Debug, Clone)]
pub struct HoodiAnchorPaths {
    pub pin: HoodiAnchorPin,
    pub block_ssz: PathBuf,
    pub state_ssz: PathBuf,
    pub config_yaml: PathBuf,
}

/// Hoodi `(state, block)` decoded from SSZ. Caches are whatever decode left.
#[derive(Debug)]
pub struct HoodiAnchor {
    pub state: BeaconState<Mainnet>,
    pub block: SignedBeaconBlock<Mainnet>,
}

impl HoodiAnchor {
    /// Length of `caches.pubkeys` after decode. The R-11 falsifiers need this
    /// to observe an empty map.
    pub fn pubkey_cache_len(&self) -> usize {
        self.state.caches().pubkeys.len()
    }

    /// Validator registry length (M13 / later falsifiers compare against the cache).
    pub fn validators_len(&self) -> usize {
        self.state.validators_len()
    }
}

/// Directory holding `hoodi-anchor.toml` and `hoodi-config.yaml`.
pub fn types_fixtures_dir() -> PathBuf {
    let start = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut dir = start.clone();
    loop {
        let under_crates = dir.join("crates/types/tests/fixtures");
        if under_crates.join(ANCHOR_TOML).is_file() {
            return under_crates;
        }
        let local = dir.join("tests/fixtures");
        if dir.file_name().is_some_and(|n| n == "types") && local.join(ANCHOR_TOML).is_file() {
            return local;
        }
        if !dir.pop() {
            break;
        }
    }
    start.join("../types/tests/fixtures")
}

/// Path to the committed Hoodi consensus config.
pub fn hoodi_config_path() -> PathBuf {
    types_fixtures_dir().join(CONFIG_YAML)
}

/// Parse the committed Hoodi YAML config.
pub fn load_hoodi_config() -> Result<ChainConfig, Error> {
    let path = hoodi_config_path();
    ChainConfig::from_yaml_file(&path).map_err(|e| Error::Manifest {
        detail: format!("{}: {e}", path.display()),
    })
}

/// Load the committed pin (roots + digests only; no SSZ).
pub fn load_pin() -> Result<HoodiAnchorPin, Error> {
    let path = types_fixtures_dir().join(ANCHOR_TOML);
    let text = fs::read_to_string(&path).map_err(|e| Error::Io {
        path,
        detail: e.to_string(),
    })?;
    parse_pin(&text)
}

/// Whether `HOODI_FIXTURES_CACHE` is set (cache-dependent tests skip when not).
pub fn cache_env_is_set() -> bool {
    match std::env::var(CACHE_ENV) {
        Ok(v) => !v.trim().is_empty(),
        Err(_) => false,
    }
}

/// Resolve `${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}`.
pub fn resolve_cache_root() -> Result<PathBuf, Error> {
    if let Ok(p) = std::env::var(CACHE_ENV) {
        let p = p.trim();
        if p.is_empty() {
            return Err(Error::Env(format!("{CACHE_ENV} is set but empty")));
        }
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("HOME")
        .map_err(|_| Error::Env(format!("HOME is unset and {CACHE_ENV} was not provided")))?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join(DEFAULT_CACHE_DIR_NAME))
}

/// Resolve SSZ paths from the default cache root + committed pin.
pub fn resolve_anchor_paths() -> Result<HoodiAnchorPaths, Error> {
    let root = resolve_cache_root()?;
    resolve_anchor_paths_in(&root)
}

/// Resolve SSZ paths under `cache_root` (tests inject a missing tree here).
pub fn resolve_anchor_paths_in(cache_root: &Path) -> Result<HoodiAnchorPaths, Error> {
    let pin = load_pin()?;
    let slot_dir = cache_root.join(pin.slot.to_string());
    if !slot_dir.is_dir() {
        return Err(Error::not_ready(format!(
            "slot directory missing: {}",
            slot_dir.display()
        )));
    }

    let block_ssz = slot_dir.join("signed_beacon_block.ssz");
    let state_ssz = slot_dir.join("beacon_state.ssz");
    if !block_ssz.is_file() {
        return Err(Error::not_ready(format!(
            "artifact missing: signed_beacon_block.ssz at {}",
            block_ssz.display()
        )));
    }
    if !state_ssz.is_file() {
        return Err(Error::not_ready(format!(
            "artifact missing: beacon_state.ssz at {}",
            state_ssz.display()
        )));
    }

    let state_meta = fs::metadata(&state_ssz).map_err(|e| Error::Io {
        path: state_ssz.clone(),
        detail: e.to_string(),
    })?;
    if state_meta.len() < MIN_STATE_BYTES {
        return Err(Error::not_ready(format!(
            "beacon_state.ssz size {} < {MIN_STATE_BYTES} (150 MB floor)",
            state_meta.len()
        )));
    }

    Ok(HoodiAnchorPaths {
        pin,
        block_ssz,
        state_ssz,
        config_yaml: hoodi_config_path(),
    })
}

/// Decode the Hoodi pair from the default cache. Never populates `StateCaches`.
pub fn load_anchor() -> Result<HoodiAnchor, Error> {
    let paths = resolve_anchor_paths()?;
    load_anchor_from_paths(&paths)
}

/// Read + verify + decode the pair at `paths`.
pub fn load_anchor_from_paths(paths: &HoodiAnchorPaths) -> Result<HoodiAnchor, Error> {
    let block_bytes = fs::read(&paths.block_ssz).map_err(|e| Error::Io {
        path: paths.block_ssz.clone(),
        detail: e.to_string(),
    })?;
    verify_sha256(
        &block_bytes,
        &paths.pin.block_sha256,
        "signed_beacon_block.ssz",
    )?;

    let state_bytes = fs::read(&paths.state_ssz).map_err(|e| Error::Io {
        path: paths.state_ssz.clone(),
        detail: e.to_string(),
    })?;
    verify_sha256(&state_bytes, &paths.pin.state_sha256, "beacon_state.ssz")?;

    decode_anchor_ssz(&state_bytes, &block_bytes)
}

/// Decode already-loaded SSZ bytes. The only constructor this rig uses.
pub fn decode_anchor_ssz(state_bytes: &[u8], block_bytes: &[u8]) -> Result<HoodiAnchor, Error> {
    let state =
        BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, state_bytes).map_err(|e| {
            Error::Decode {
                artifact: "beacon_state.ssz",
                detail: format!("{e:?}"),
            }
        })?;
    let block = SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, block_bytes)
        .map_err(|e| Error::Decode {
            artifact: "signed_beacon_block.ssz",
            detail: format!("{e:?}"),
        })?;
    Ok(HoodiAnchor { state, block })
}

fn verify_sha256(bytes: &[u8], expected: &str, artifact: &str) -> Result<(), Error> {
    let actual = hex_sha256(bytes);
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

fn parse_pin(text: &str) -> Result<HoodiAnchorPin, Error> {
    let mut slot = None;
    let mut block_root = None;
    let mut state_root = None;
    let mut block_sha256 = None;
    let mut state_sha256 = None;
    let mut state_size = None;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('[') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let k = k.trim();
        let v = strip_quotes(v.trim());
        match k {
            "slot" => slot = Some(parse_u64(v, "slot")?),
            "block_root" => block_root = Some(v.to_string()),
            "state_root" => state_root = Some(v.to_string()),
            "block_sha256" => block_sha256 = Some(v.to_string()),
            "state_sha256" => state_sha256 = Some(v.to_string()),
            "state_size" => state_size = Some(parse_u64(v, "state_size")?),
            _ => {}
        }
    }

    Ok(HoodiAnchorPin {
        slot: slot.ok_or_else(|| Error::Manifest {
            detail: "missing slot".into(),
        })?,
        block_root: block_root.ok_or_else(|| Error::Manifest {
            detail: "missing block_root".into(),
        })?,
        state_root: state_root.ok_or_else(|| Error::Manifest {
            detail: "missing state_root".into(),
        })?,
        block_sha256: block_sha256.ok_or_else(|| Error::Manifest {
            detail: "missing block_sha256".into(),
        })?,
        state_sha256: state_sha256.ok_or_else(|| Error::Manifest {
            detail: "missing state_sha256".into(),
        })?,
        state_size: state_size.ok_or_else(|| Error::Manifest {
            detail: "missing state_size".into(),
        })?,
    })
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
