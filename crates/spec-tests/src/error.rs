//! Harness error type.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Literal substring every cache-readiness failure must contain (Phase 0 §7.4 / CC-06/4).
pub const FETCH_HINT: &str = "run scripts/fetch-spec-vectors.sh";

/// Errors produced by the vector harness.
///
/// Large payloads are boxed so `Result<T, Error>` stays small
/// (`clippy::result_large_err`).
#[derive(Debug)]
pub enum Error {
    /// Cache missing, incomplete, or digests do not match the lockfile.
    CacheNotReady { detail: String },
    /// A `.complete-<artifact>` marker is wrong.
    MarkerMismatch {
        artifact: String,
        expected: String,
        found: String,
    },
    /// Marker file is absent.
    MarkerMissing { artifact: String, path: PathBuf },
    /// `tests/` tree missing under the tag directory.
    TestsTreeMissing { path: PathBuf },
    /// Failed to read the environment (`HOME` / `SPEC_VECTORS_CACHE`).
    Env(String),
    /// Filesystem I/O.
    Io { path: PathBuf, source: io::Error },
    /// Snappy block decompression failed.
    Snappy { path: PathBuf, detail: String },
    /// YAML parse failure.
    Yaml { path: PathBuf, detail: String },
    /// SSZ decode failure (after snappy decompress).
    Ssz { path: PathBuf, detail: String },
    /// Path component rejected (traversal / absolute).
    InvalidPath { detail: String },
    /// Handler coverage mismatch (missing and/or extra names).
    Coverage {
        missing: Vec<String>,
        extra: Vec<String>,
    },
    /// Skip-list parse or validation failure.
    Skiplist { detail: String },
    /// Lockfile embedded at compile time could not be parsed.
    Lockfile(String),
}

impl Error {
    /// Build a cache-not-ready error that always embeds [`FETCH_HINT`].
    pub fn cache_not_ready(detail: impl Into<String>) -> Self {
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
            Self::MarkerMismatch {
                artifact,
                expected,
                found,
            } => write!(
                f,
                "marker mismatch for {artifact}: expected digest {expected}, found {found}; {FETCH_HINT}"
            ),
            Self::MarkerMissing { artifact, path } => write!(
                f,
                "missing readiness marker for {artifact} at {}; {FETCH_HINT}",
                path.display()
            ),
            Self::TestsTreeMissing { path } => {
                write!(f, "tests tree missing at {}; {FETCH_HINT}", path.display())
            }
            Self::Env(msg) => write!(f, "environment: {msg}; {FETCH_HINT}"),
            Self::Io { path, source } => write!(f, "I/O error at {}: {source}", path.display()),
            Self::Snappy { path, detail } => {
                write!(f, "snappy decode failed for {}: {detail}", path.display())
            }
            Self::Yaml { path, detail } => {
                write!(f, "YAML parse failed for {}: {detail}", path.display())
            }
            Self::Ssz { path, detail } => {
                write!(f, "SSZ decode failed for {}: {detail}", path.display())
            }
            Self::InvalidPath { detail } => write!(f, "invalid path: {detail}"),
            Self::Coverage { missing, extra } => {
                write!(f, "handler coverage mismatch")?;
                if !missing.is_empty() {
                    write!(f, "; missing on disk: [{}]", missing.join(", "))?;
                }
                if !extra.is_empty() {
                    write!(f, "; extra on disk: [{}]", extra.join(", "))?;
                }
                Ok(())
            }
            Self::Skiplist { detail } => write!(f, "skiplist: {detail}"),
            Self::Lockfile(msg) => write!(f, "lockfile: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
