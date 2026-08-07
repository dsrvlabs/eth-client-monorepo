//! Epoch-aware fork digest — Architecture §4.1–§4.2, CC-21b / CC-21/2.
//!
//! Fulu redefines `compute_fork_digest(gvr, epoch)` so that for
//! `epoch >= FULU_FORK_EPOCH` the base fork-data root is XOR'd with a SHA-256 of
//! the active blob parameters before truncating to four bytes. A BPO therefore
//! renames every gossip topic and re-keys discovery even though neither the
//! fork version nor the state transition changes.
//!
//! **This module has no libp2p, discv5, or I/O.** It depends only on `cc-types`
//! (runtime config + blob schedule) and `cc-crypto` (`compute_fork_data_root`,
//! `hash_fixed`). Blob-parameter lookup always goes through
//! [`ChainConfig::get_blob_parameters`] — never a hard-coded Electra/Fulu
//! constant (see the CC-21b grep guard).

use std::collections::BTreeMap;

use cc_crypto::{compute_fork_data_root, hash_fixed};
use cc_types::{
    BlobParameters, ChainConfig, Epoch, ForkDigest, ForkVersion, Mainnet, Minimal, PresetName, Root,
};

/// Spec `FAR_FUTURE_EPOCH = 2**64 - 1`. Used when no next fork is scheduled.
pub const FAR_FUTURE_EPOCH: Epoch = Epoch::new(u64::MAX);

/// SSZ `ENRForkID` payload for the ENR `eth2` key (phase0 shape; Fulu changes
/// field *semantics* only — Architecture §4.2 / Fulu p2p-interface).
///
/// - `fork_digest` — digest at the current wall-clock epoch
/// - `next_fork_version` — next **regular** fork's version (unchanged by a BPO);
///   equals the current fork version when no future regular fork is planned
/// - `next_fork_epoch` — next epoch at which the digest changes (regular **or**
///   BPO); `FAR_FUTURE_EPOCH` when nothing is scheduled
///
/// Across a BPO the two "next" fields deliberately disagree: epoch advances,
/// version stays. That disagreement is asserted by tests and by CC-21c / CC-2A.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnrForkId {
    /// Digest at the current epoch.
    pub fork_digest: ForkDigest,
    /// Next regular fork version (or current, if none planned).
    pub next_fork_version: ForkVersion,
    /// Next digest-change epoch (regular or BPO), or [`FAR_FUTURE_EPOCH`].
    pub next_fork_epoch: Epoch,
}

impl EnrForkId {
    /// SSZ encode as a fixed 16-byte container (`ForkDigest` + `Version` + `Epoch`).
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..4].copy_from_slice(self.fork_digest.as_slice());
        out[4..8].copy_from_slice(self.next_fork_version.as_slice());
        out[8..16].copy_from_slice(&self.next_fork_epoch.as_u64().to_le_bytes());
        out
    }
}

/// Spec `compute_fork_version(epoch)` against the loaded runtime config.
///
/// Walks the regular-fork schedule newest-first. Does **not** consult the blob
/// schedule — BPO forks keep the same version.
#[must_use]
pub fn compute_fork_version(cfg: &ChainConfig, epoch: Epoch) -> ForkVersion {
    let e = epoch.as_u64();
    if e >= cfg.fulu_fork_epoch.as_u64() {
        cfg.fulu_fork_version
    } else if e >= cfg.electra_fork_epoch.as_u64() {
        cfg.electra_fork_version
    } else if e >= cfg.deneb_fork_epoch.as_u64() {
        cfg.deneb_fork_version
    } else if e >= cfg.capella_fork_epoch.as_u64() {
        cfg.capella_fork_version
    } else if e >= cfg.bellatrix_fork_epoch.as_u64() {
        cfg.bellatrix_fork_version
    } else if e >= cfg.altair_fork_epoch.as_u64() {
        cfg.altair_fork_version
    } else {
        cfg.genesis_fork_version
    }
}

/// Spec `compute_fork_digest(genesis_validators_root, epoch)` (Fulu form).
///
/// ```text
/// fork_version = compute_fork_version(epoch)
/// base_digest  = compute_fork_data_root(fork_version, gvr)
/// if epoch < FULU_FORK_EPOCH:
///     return base_digest[:4]
/// bp = get_blob_parameters(epoch)   # runtime config — Electra fallback on Hoodi
/// return xor(base_digest, hash(uint64_le(bp.epoch) || uint64_le(bp.max_blobs)))[:4]
/// ```
#[must_use]
pub fn compute_fork_digest(cfg: &ChainConfig, gvr: Root, epoch: Epoch) -> ForkDigest {
    let fork_version = compute_fork_version(cfg, epoch);
    let base = compute_fork_data_root(fork_version, gvr);
    let base_bytes = base.as_slice();

    if epoch.as_u64() < cfg.fulu_fork_epoch.as_u64() {
        let mut out = [0u8; 4];
        out.copy_from_slice(&base_bytes[0..4]);
        return ForkDigest::from_array(out);
    }

    let bp = blob_parameters(cfg, epoch);
    let mut material = [0u8; 16];
    material[0..8].copy_from_slice(&bp.epoch.as_u64().to_le_bytes());
    material[8..16].copy_from_slice(&bp.max_blobs_per_block.to_le_bytes());
    let mask = hash_fixed(&material);

    let mut out = [0u8; 4];
    for i in 0..4 {
        out[i] = base_bytes[i] ^ mask[i];
    }
    ForkDigest::from_array(out)
}

/// Next epoch at which the fork digest changes — regular fork **or** BPO.
///
/// Returns `(boundary_epoch, fork_version_at_boundary, digest_at_boundary)`.
/// The fork version is the regular version at that epoch (unchanged by a pure
/// BPO). Returns `None` when nothing is scheduled after `epoch`.
#[must_use]
pub fn next_fork(
    cfg: &ChainConfig,
    gvr: Root,
    epoch: Epoch,
) -> Option<(Epoch, ForkVersion, ForkDigest)> {
    let boundary = next_digest_boundary_epoch(cfg, epoch)?;
    let version = compute_fork_version(cfg, boundary);
    let digest = compute_fork_digest(cfg, gvr, boundary);
    Some((boundary, version, digest))
}

/// Build the ENR `eth2` / `nfd` view for `epoch`.
///
/// - `next_fork_version` tracks only the next **regular** fork
/// - `next_fork_epoch` tracks the next digest change (regular or BPO)
/// - when no next digest change exists: `next_fork_epoch = FAR_FUTURE_EPOCH`,
///   and callers zero-fill `nfd`
#[must_use]
pub fn enr_fork_id(cfg: &ChainConfig, gvr: Root, epoch: Epoch) -> EnrForkId {
    let fork_digest = compute_fork_digest(cfg, gvr, epoch);
    let next_fork_version = next_regular_fork_version(cfg, epoch);
    let next_fork_epoch = next_digest_boundary_epoch(cfg, epoch).unwrap_or(FAR_FUTURE_EPOCH);
    EnrForkId {
        fork_digest,
        next_fork_version,
        next_fork_epoch,
    }
}

/// Digest of the next scheduled fork/BPO, or [`ForkDigest::ZERO`] when none
/// (`nfd` zero-fill — Fulu p2p-interface / Architecture §4.3).
#[must_use]
pub fn next_fork_digest(cfg: &ChainConfig, gvr: Root, epoch: Epoch) -> ForkDigest {
    match next_fork(cfg, gvr, epoch) {
        Some((_, _, d)) => d,
        None => ForkDigest::ZERO,
    }
}

/// Single source of truth for the current/next digest and the per-epoch cache.
///
/// Constructed once at startup with an initial epoch; refreshed via
/// [`ForkContext::on_epoch`] (driven by the slot clock in CC-20b — until then
/// tests advance the epoch explicitly).
#[derive(Debug, Clone)]
pub struct ForkContext {
    cfg: ChainConfig,
    gvr: Root,
    /// Wall-clock epoch last applied via [`Self::on_epoch`] / constructor.
    current_epoch: Epoch,
    /// Digest at [`Self::current_epoch`].
    current_digest: ForkDigest,
    /// Next digest-change boundary: `(epoch, version_at_boundary, digest)`.
    next: Option<(Epoch, ForkVersion, ForkDigest)>,
    /// Derived ENR `eth2` payload.
    enr_fork_id: EnrForkId,
    /// ENR `nfd` payload (next digest, or zero).
    nfd: ForkDigest,
    /// Per-epoch digest cache for by-range chunk tagging (CC-23/7).
    cache: BTreeMap<Epoch, ForkDigest>,
}

impl ForkContext {
    /// Construct at `epoch`, seeding the cache with the current digest.
    #[must_use]
    pub fn new(cfg: ChainConfig, gvr: Root, epoch: Epoch) -> Self {
        let mut ctx = Self {
            current_digest: ForkDigest::ZERO,
            next: None,
            enr_fork_id: EnrForkId {
                fork_digest: ForkDigest::ZERO,
                next_fork_version: ForkVersion::ZERO,
                next_fork_epoch: FAR_FUTURE_EPOCH,
            },
            nfd: ForkDigest::ZERO,
            cache: BTreeMap::new(),
            cfg,
            gvr,
            current_epoch: epoch,
        };
        ctx.refresh(epoch);
        ctx
    }

    /// Advance to `epoch` (epoch tick). Recomputes current/next/ENRForkID/`nfd`.
    pub fn on_epoch(&mut self, epoch: Epoch) {
        self.refresh(epoch);
    }

    /// Current wall-clock epoch.
    #[must_use]
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Digest at the current epoch.
    #[must_use]
    pub fn current_digest(&self) -> ForkDigest {
        self.current_digest
    }

    /// Next digest-change boundary, if any.
    #[must_use]
    pub fn next(&self) -> Option<(Epoch, ForkVersion, ForkDigest)> {
        self.next
    }

    /// Derived ENR `eth2` payload.
    #[must_use]
    pub fn enr_fork_id(&self) -> EnrForkId {
        self.enr_fork_id
    }

    /// ENR `nfd` value (next digest, or zero-filled).
    #[must_use]
    pub fn nfd(&self) -> ForkDigest {
        self.nfd
    }

    /// Digest at an arbitrary epoch, served from the per-epoch cache.
    ///
    /// Used by by-range response context (CC-23/7): each chunk is tagged with
    /// the digest for *that chunk's* slot, not necessarily `current`.
    pub fn digest_at(&mut self, epoch: Epoch) -> ForkDigest {
        if let Some(d) = self.cache.get(&epoch) {
            return *d;
        }
        let d = compute_fork_digest(&self.cfg, self.gvr, epoch);
        self.cache.insert(epoch, d);
        d
    }

    /// Borrow the runtime config (tests / diagnostics).
    #[must_use]
    pub fn config(&self) -> &ChainConfig {
        &self.cfg
    }

    /// Genesis validators root.
    #[must_use]
    pub fn genesis_validators_root(&self) -> Root {
        self.gvr
    }

    fn refresh(&mut self, epoch: Epoch) {
        self.current_epoch = epoch;
        self.current_digest = compute_fork_digest(&self.cfg, self.gvr, epoch);
        self.cache.insert(epoch, self.current_digest);
        self.next = next_fork(&self.cfg, self.gvr, epoch);
        self.enr_fork_id = enr_fork_id(&self.cfg, self.gvr, epoch);
        self.nfd = next_fork_digest(&self.cfg, self.gvr, epoch);
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn blob_parameters(cfg: &ChainConfig, epoch: Epoch) -> BlobParameters {
    match cfg.preset_base {
        PresetName::Mainnet => cfg.get_blob_parameters::<Mainnet>(epoch),
        PresetName::Minimal => cfg.get_blob_parameters::<Minimal>(epoch),
    }
}

/// Next epoch after `epoch` at which the digest can change (regular fork or BPO).
fn next_digest_boundary_epoch(cfg: &ChainConfig, epoch: Epoch) -> Option<Epoch> {
    let cur = epoch.as_u64();
    let mut best: Option<u64> = None;

    let push = |best: &mut Option<u64>, candidate: u64| {
        if candidate > cur {
            *best = Some(best.map_or(candidate, |b| b.min(candidate)));
        }
    };

    for e in regular_fork_epochs(cfg) {
        push(&mut best, e);
    }
    for entry in cfg.blob_schedule.entries() {
        push(&mut best, entry.epoch.as_u64());
    }

    best.map(Epoch::new)
}

/// Next **regular** fork version after `epoch`, or the current version if none.
fn next_regular_fork_version(cfg: &ChainConfig, epoch: Epoch) -> ForkVersion {
    let cur = epoch.as_u64();
    // Ascending regular-fork table: (epoch, version).
    let schedule: [(u64, ForkVersion); 6] = [
        (cfg.altair_fork_epoch.as_u64(), cfg.altair_fork_version),
        (cfg.bellatrix_fork_epoch.as_u64(), cfg.bellatrix_fork_version),
        (cfg.capella_fork_epoch.as_u64(), cfg.capella_fork_version),
        (cfg.deneb_fork_epoch.as_u64(), cfg.deneb_fork_version),
        (cfg.electra_fork_epoch.as_u64(), cfg.electra_fork_version),
        (cfg.fulu_fork_epoch.as_u64(), cfg.fulu_fork_version),
    ];
    for &(e, v) in &schedule {
        if e > cur {
            return v;
        }
    }
    compute_fork_version(cfg, epoch)
}

fn regular_fork_epochs(cfg: &ChainConfig) -> [u64; 6] {
    [
        cfg.altair_fork_epoch.as_u64(),
        cfg.bellatrix_fork_epoch.as_u64(),
        cfg.capella_fork_epoch.as_u64(),
        cfg.deneb_fork_epoch.as_u64(),
        cfg.electra_fork_epoch.as_u64(),
        cfg.fulu_fork_epoch.as_u64(),
    ]
}
