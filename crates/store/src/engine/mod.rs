//! Concrete storage engine seam (Architecture §1.1).
//!
//! One inherent API, no trait / generic / `dyn`. The body lives in [`redb`];
//! a fjall substitution replaces that one file (ADR P4-01).

mod redb;

pub use redb::{Batch, Engine, RangeIter, ReadTxn};

use std::fmt;
use std::path::PathBuf;

/// Hard cap on interned table names (shard tables + meta/hot).
/// Architecture §2.4 ≈ 390 shards + hot/meta; headroom to 512.
pub const MAX_INTERNED_TABLE_NAMES: usize = 512;

/// Hard cap on ops accumulated in one [`Batch`] (cheap mitigation of unbounded growth).
pub const MAX_BATCH_OPS: usize = 65_536;

/// Hard cap on entries materialised by one [`ReadTxn::range`] call (§7.2 / SEC-40b-4).
pub const MAX_RANGE_ENTRIES: usize = 1_048_576;

/// Hard cap on total value+key bytes materialised by one range call.
pub const MAX_RANGE_BYTES: u64 = 512 * 1024 * 1024;

/// Commit durability knob (Architecture §8.4 / ADR P4-05).
///
/// redb's public enum is only `{None, Immediate}`. `Paranoid` maps to
/// `Immediate` **and** enables two-phase commit, so the two **config** settings
/// `immediate | paranoid` are exactly 1PC+C and 2PC.
///
/// [`Durability::None`] exists for tests and bulk-load benches only; it is
/// **rejected** by [`Durability::parse`] / [`Durability::resolve`] so it cannot
/// come from `storage.toml` / `CC_STORAGE_DURABILITY` (SEC-40b-6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Durability {
    /// No fsync (tests / bulk load only — not a production config value).
    None,
    /// 1PC+C — default production setting.
    #[default]
    Immediate,
    /// Immediate + two-phase commit.
    Paranoid,
}

impl Durability {
    /// Parse a **production config** token: `immediate` | `paranoid` only.
    ///
    /// `none` is rejected here (SEC-40b-6). Tests construct [`Durability::None`]
    /// via the enum variant / [`EngineOptions::with_durability`].
    pub fn parse(s: &str) -> Result<Self, StoreError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "immediate" => Ok(Self::Immediate),
            "paranoid" => Ok(Self::Paranoid),
            "none" => Err(StoreError::Config(
                "durability \"none\" is not a production config value (immediate|paranoid only); \
                 use EngineOptions::with_durability(Durability::None) for tests/bulk-load"
                    .into(),
            )),
            other => Err(StoreError::Config(format!(
                "unknown durability {other:?}; expected immediate|paranoid"
            ))),
        }
    }

    /// Resolve durability from a config value with an optional override token
    /// (e.g. figment's `CC_STORAGE_DURABILITY` layer, applied by the service).
    ///
    /// `cc-store` does not read the process environment; callers pass the
    /// already-resolved override so `scripts/check-no-env-reads.sh` stays green.
    /// Both layers use [`Self::parse`] (no `none`).
    pub fn resolve(config: &str, override_token: Option<&str>) -> Result<Self, StoreError> {
        if let Some(v) = override_token
            && !v.is_empty()
        {
            return Self::parse(v);
        }
        Self::parse(config)
    }

    /// Whether commits use redb two-phase commit.
    pub const fn two_phase_commit(self) -> bool {
        matches!(self, Self::Paranoid)
    }
}

/// Store-level error (engine + codecs; schema errors land in CC-40a).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("store engine: {0}")]
    Engine(String),
    #[error("store config: {0}")]
    Config(String),
    #[error("store limit exceeded: {0}")]
    Limit(String),
    #[error("table {0:?} does not exist")]
    TableMissing(String),
}

impl StoreError {
    pub(crate) fn engine(err: impl fmt::Display) -> Self {
        Self::Engine(err.to_string())
    }

    pub(crate) fn limit(msg: impl Into<String>) -> Self {
        Self::Limit(msg.into())
    }
}

/// Options for [`Engine::open`].
#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub durability: Durability,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Immediate,
        }
    }
}

impl EngineOptions {
    pub fn with_durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }
}

/// Path of the primary redb database file under a data directory.
///
/// `path` is a **directory**; the engine creates `store.redb` inside it.
pub fn db_file_path(dir: impl AsRef<std::path::Path>) -> PathBuf {
    dir.as_ref().join("store.redb")
}
