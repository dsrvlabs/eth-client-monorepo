//! ENR manager skeleton — Architecture §6.2, CC-21a / CC-21/1.
//!
//! All ENR field mutations go through [`EnrManager::apply`], which takes a
//! **batch** and produces **exactly one** sequence bump. Field encoders
//! (`eth2`, `attnets`, `syncnets`, `cgc`, `nfd`) land in CC-21c; this module
//! only fixes the shape and the A-P2-4 strategy.

use std::fmt;
use std::net::Ipv4Addr;

use discv5::enr::{CombinedKey, Error as EnrError};
use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig};

/// How [`EnrManager::apply`] advances `seq` and keeps the local ENR signature valid.
///
/// Selected by the A-P2-4 probe (CC-21/1). Outcome recorded in
/// `docs/phase-2-soak.md` §`CC-21/1 ENR sequence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnrSeqStrategy {
    /// Use [`Discv5::enr_insert`] (single field) / insert-then-collapse (batch).
    ///
    /// Probe result for discv5 **0.11.0**: `enr_insert` bumps `seq` and re-signs.
    #[default]
    EnrInsert,
    /// Build the updated field set on a cloned ENR, force `seq = old + 1`, re-sign,
    /// and install via [`Discv5::external_enr`] (local-ENR replacement path).
    RebuildAndReplace,
}

/// One field mutation in an [`EnrManager::apply`] batch.
///
/// `value` is the **payload** (not pre-RLP-encoded). It is RLP-encoded as a
/// byte string by `enr_insert` / `Enr::insert`, matching the probe's
/// `enr_insert("cgc", &v)` call shape. Typed encoders for `cgc` / `eth2` / …
/// arrive in CC-21c.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrFieldChange {
    /// ENR key (e.g. `"cgc"`, `"eth2"`, `"nfd"`).
    pub key: String,
    /// Raw field payload; RLP-encoded as bytes by the insert path.
    pub value: Vec<u8>,
}

impl EnrFieldChange {
    /// Construct a change from a key and a byte payload.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Errors from [`EnrManager::apply`] or handle construction.
#[derive(Debug, thiserror::Error)]
pub enum EnrApplyError {
    /// Underlying `enr` crate error (size, signing, seq overflow, …).
    #[error("enr error: {0}")]
    Enr(#[source] EnrError),
    /// `Discv5::new` rejected the key/ENR pair.
    #[error("discv5 construction failed: {0}")]
    Discv5(&'static str),
    /// Sequence number would overflow `u64`.
    #[error("ENR sequence number overflow")]
    SeqOverflow,
}

/// Single writer for local ENR field mutations (§6.2).
///
/// Holds a real [`Discv5`] handle (never started for the CC-21a probe) and a
/// retained node key for the rebuild-and-replace path.
pub struct EnrManager {
    discv5: Discv5,
    /// Copy of the node key — `Discv5` takes ownership of its own key and does
    /// not expose it; rebuild/collapse paths need a signer.
    enr_key: CombinedKey,
    strategy: EnrSeqStrategy,
}

impl fmt::Debug for EnrManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrManager")
            .field("local_enr", &self.discv5.local_enr())
            .field("strategy", &self.strategy)
            .field("enr_key", &"<redacted>")
            .finish()
    }
}

impl EnrManager {
    /// Build a manager around an existing (not necessarily started) handle.
    ///
    /// `enr_key` must be the **same** secret that signed `discv5`'s local ENR.
    #[must_use]
    pub fn new(discv5: Discv5, enr_key: CombinedKey, strategy: EnrSeqStrategy) -> Self {
        Self {
            discv5,
            enr_key,
            strategy,
        }
    }

    /// Construct a loopback, **no-network** manager for tests and the A-P2-4 probe.
    ///
    /// - Binds nothing: `Discv5::start` is never called.
    /// - Uses a throwaway secp256k1 key (does **not** write `./data/node_key`).
    /// - Listen config is `127.0.0.1:0` (unused without `start`).
    pub fn new_ephemeral(strategy: EnrSeqStrategy) -> Result<Self, EnrApplyError> {
        let enr_key = CombinedKey::generate_secp256k1();
        let enr_key_copy = duplicate_secp256k1(&enr_key)?;

        let enr = Enr::empty(&enr_key).map_err(EnrApplyError::Enr)?;
        let listen_config = ListenConfig::Ipv4 {
            ip: Ipv4Addr::LOCALHOST,
            port: 0,
        };
        let config = ConfigBuilder::new(listen_config).build();
        let discv5 = Discv5::new(enr, enr_key, config).map_err(EnrApplyError::Discv5)?;

        Ok(Self::new(discv5, enr_key_copy, strategy))
    }

    /// Active sequence strategy (probe outcome).
    #[must_use]
    pub const fn strategy(&self) -> EnrSeqStrategy {
        self.strategy
    }

    /// Borrow the underlying discv5 handle.
    #[must_use]
    pub const fn discv5(&self) -> &Discv5 {
        &self.discv5
    }

    /// Snapshot of the local ENR.
    #[must_use]
    pub fn local_enr(&self) -> Enr {
        self.discv5.local_enr()
    }

    /// Apply a **batch** of field changes with **exactly one** sequence bump.
    ///
    /// Empty batches are a no-op (no bump). Non-empty batches leave the local
    /// ENR signature verifying against its public key.
    pub fn apply(
        &self,
        changes: impl IntoIterator<Item = EnrFieldChange>,
    ) -> Result<(), EnrApplyError> {
        let changes: Vec<EnrFieldChange> = changes.into_iter().collect();
        if changes.is_empty() {
            return Ok(());
        }

        match self.strategy {
            EnrSeqStrategy::EnrInsert => self.apply_enr_insert(&changes),
            EnrSeqStrategy::RebuildAndReplace => self.apply_rebuild_and_replace(&changes),
        }
    }

    /// Primary path when A-P2-4 holds: `Discv5::enr_insert` for a single field;
    /// multi-field batches collapse to one bump via insert + `set_seq`.
    fn apply_enr_insert(&self, changes: &[EnrFieldChange]) -> Result<(), EnrApplyError> {
        if let [single] = changes {
            self.discv5
                .enr_insert(single.key.as_str(), &single.value.as_slice())
                .map_err(EnrApplyError::Enr)?;
            return Ok(());
        }
        // discv5 bumps once per enr_insert; coalesce multi-field batches.
        self.apply_rebuild_and_replace(changes)
    }

    /// Rebuild-and-replace fallback (and multi-field coalesce under `EnrInsert`).
    ///
    /// Clones the local ENR, applies every change via `Enr::insert`, forces
    /// `seq = old + 1` (re-signs), and installs the result through
    /// [`Discv5::external_enr`].
    fn apply_rebuild_and_replace(&self, changes: &[EnrFieldChange]) -> Result<(), EnrApplyError> {
        let old_seq = self.discv5.local_enr().seq();
        let new_seq = old_seq.checked_add(1).ok_or(EnrApplyError::SeqOverflow)?;

        let mut enr = self.discv5.local_enr();
        for change in changes {
            enr.insert(change.key.as_str(), &change.value.as_slice(), &self.enr_key)
                .map_err(EnrApplyError::Enr)?;
        }
        enr.set_seq(new_seq, &self.enr_key)
            .map_err(EnrApplyError::Enr)?;

        *self.discv5.external_enr().write() = enr;
        Ok(())
    }
}

/// Duplicate a secp256k1 [`CombinedKey`] via encode → re-import.
///
/// `CombinedKey` is not `Clone`; `Discv5::new` takes ownership of one copy
/// while [`EnrManager`] retains another for rebuild/collapse signing.
fn duplicate_secp256k1(key: &CombinedKey) -> Result<CombinedKey, EnrApplyError> {
    let mut bytes = key.encode();
    CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|_| {
        EnrApplyError::Discv5("failed to duplicate secp256k1 CombinedKey for EnrManager")
    })
}
