//! Self-devnet publisher and fault modes (CC-2Jd / CC-2Jb / CC-2Jc).
//!
//! - **`--publish-fixture`**: plain publisher — loads CC-2Ja's chain fixture,
//!   forces conceptual `cgc = 128`, subscribes to all 128 column subnets, and
//!   publishes each slot's block + sidecars at slot wall-clock cadence.
//! - **`withhold-column`** (CC-2Jb): skip listed columns on gossip publish and
//!   refuse them on `DataColumnSidecarsByRoot` until a **flag file** appears.
//!   Seams live in `gossip/validate/column.rs` (publish) and
//!   `reqresp/columns.rs` (by-root serve).
//! - **`misbehave:<kind>`** (CC-2Jc): kinds map to attributable penalty reasons —
//!   `invalid-column` / `malformed` → `gossip_invalid`, `spam` → `rate_limit`,
//!   `custody-refuse` → `custody_unserved`, `stall-reqresp` → `reqresp_fault`.
//! - Process-global active fault ([`install_active_fault`]) is what the seam
//!   call sites consult so production paths stay greppable one-liners.

#![allow(clippy::unwrap_used, clippy::expect_used)] // tests only below

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use cc_libp2p::reexport::futures::StreamExt;
use cc_libp2p::reexport::gossipsub::{IdentTopic, MessageAcceptance, TopicHash};
use cc_libp2p::reexport::identity::{self, Keypair};
use cc_libp2p::reexport::request_response::{
    Event as RequestResponseEvent, Message as RequestResponseMessage,
};
use cc_libp2p::reexport::{Multiaddr, PeerId, SwarmEvent};
use cc_libp2p::{
    CcBehaviour, CcBehaviourEvent, ReqRespRequest, ReqRespResponse, SwarmConfig, build_swarm,
};
use cc_types::{ChainConfig, Epoch, Root, compute_columns_for_custody_group};
use discv5::Enr;
use discv5::enr::{CombinedKey, NodeId};
use sha2::{Digest, Sha256};
use ssz::{Decode, Encode};
use tracing::{info, warn};

use crate::das::CustodyManager;
use crate::fork_digest::compute_fork_digest;
use crate::gossip::validate::column::{ColumnPublishDecision, decide_column_publish};
use crate::gossip::{SubnetCounts, TopicName, format_topic_string};
use crate::metrics::{
    Direction, DirectionLabels, GossipMessageLabels, P2pMetrics, PeerPenaltyReason,
};
use crate::reqresp::columns::ByRootFaultPolicy;
use crate::reqresp::limits::INBOUND_COLUMNS_CAPACITY;

/// Committed seed string used by `up.sh` / [`derive_node_secret`] (CC-2Jd).
pub const DEVNET_KEY_SEED: &str = "cc-devnet-v1";

/// Number of data-column sidecar subnets (Fulu / mainnet & minimal).
pub const COLUMN_SUBNET_COUNT: u64 = 128;

/// Publisher forces full custody coverage.
pub const PUBLISHER_CGC: u64 = 128;

/// Default role whose sampled set must contain every withheld index (CC-2Jb).
pub const DEFAULT_WITHHOLD_TARGET_ROLE: &str = "node-a";

/// Default flag-file path when `--fault-flag-path` / `CC_P2P_FAULT_FLAG` unset.
///
/// Compose mounts `devnet/out/fault` → `/fault` so the scenario can flip the
/// file from the host.
pub const DEFAULT_RELEASE_FLAG_PATH: &str = "/fault/cc-release-columns.flag";

// ── process-global active fault (seam consult) ──────────────────────────────

/// Installed fault state consulted by the Track D seams.
#[derive(Debug, Clone)]
struct ActiveFault {
    mode: FaultMode,
    /// Existence of this path releases withheld columns for by-root serve.
    flag_path: Option<PathBuf>,
}

static ACTIVE_FAULT: RwLock<Option<ActiveFault>> = RwLock::new(None);

/// Install the process-global fault state the two Track D seams consult.
///
/// Call once at publisher/peer start (before gossip publish or req/resp serve).
pub fn install_active_fault(mode: FaultMode, flag_path: Option<PathBuf>) {
    let mut guard = ACTIVE_FAULT
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(ActiveFault { mode, flag_path });
}

/// Clear process-global fault state (tests; production leaves it installed).
pub fn clear_active_fault() {
    let mut guard = ACTIVE_FAULT
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = None;
}

/// Whether the flag file is present (withheld columns may be served by-root).
#[must_use]
pub fn is_withheld_released() -> bool {
    let guard = ACTIVE_FAULT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.as_ref() {
        Some(a) => a.flag_path.as_ref().is_some_and(|p| p.exists()),
        None => false,
    }
}

/// Seam helper: may this column index be published on gossip?
///
/// Defaults to **allow** when no fault is installed (inert production path).
#[must_use]
pub fn active_allows_column_publish(column_index: u64) -> bool {
    let guard = ACTIVE_FAULT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.as_ref() {
        Some(a) => a.mode.allows_publish_column(column_index),
        None => true,
    }
}

/// Seam helper: may a **held** column be served on `DataColumnSidecarsByRoot`?
///
/// Withheld indices return `false` until the release flag file exists.
/// Defaults to **allow** when no fault is installed.
#[must_use]
pub fn active_allows_by_root_serve(column_index: u64) -> bool {
    let guard = ACTIVE_FAULT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.as_ref() {
        Some(a) => {
            let released = a.flag_path.as_ref().is_some_and(|p| p.exists());
            a.mode.allows_by_root_serve(column_index, released)
        }
        None => true,
    }
}

// ── fault kinds ─────────────────────────────────────────────────────────────

/// Adversarial / publisher fault kind.
///
/// Extra gossip publishes per column under `misbehave:spam` (beyond the honest one).
pub const SPAM_GOSSIP_EXTRA_PUBLISHES: u32 = 8;

/// How many column chunks a spam client must request to trip the per-peer inbound
/// column bucket ([`INBOUND_COLUMNS_CAPACITY`] + 1).
#[must_use]
pub const fn spam_columns_to_trip_rate_limit() -> u64 {
    INBOUND_COLUMNS_CAPACITY.saturating_add(1)
}

// ── fault kinds ─────────────────────────────────────────────────────────────

/// CC-2Jc misbehaviour kind — each maps to one attributable penalty reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MisbehaveKind {
    /// Mutate a KZG proof → gossip REJECT → `gossip_invalid` (−10) + P4.
    InvalidColumn,
    /// Truncate / corrupt bytes so decode fails → `gossip_invalid` (−10).
    Malformed,
    /// Over-rate publish + over-limit req/resp → `rate_limit` (−5).
    Spam,
    /// Advertise full custody, never serve by root → `custody_unserved` (−15).
    CustodyRefuse,
    /// Serve by root past TTFB → `reqresp_fault` (−5).
    StallReqresp,
}

impl MisbehaveKind {
    /// Parse a kind token (CLI suffix after `misbehave:` / `misbehave=`).
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("unspecified") {
            bail!(
                "misbehave kind required; expected invalid-column|malformed|spam|custody-refuse|stall-reqresp"
            );
        }
        Ok(match s {
            "invalid-column" | "invalid_column" | "invalid" => Self::InvalidColumn,
            "malformed" => Self::Malformed,
            "spam" => Self::Spam,
            "custody-refuse" | "custody_refuse" => Self::CustodyRefuse,
            "stall-reqresp" | "stall_reqresp" | "stall" => Self::StallReqresp,
            other => bail!(
                "unknown misbehave kind {other:?}; expected invalid-column|malformed|spam|custody-refuse|stall-reqresp"
            ),
        })
    }

    /// Stable CLI / log name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidColumn => "invalid-column",
            Self::Malformed => "malformed",
            Self::Spam => "spam",
            Self::CustodyRefuse => "custody-refuse",
            Self::StallReqresp => "stall-reqresp",
        }
    }

    /// Attributable `cc_p2p_peer_penalty_total{reason}` label for this kind.
    #[must_use]
    pub const fn penalty_reason(self) -> PeerPenaltyReason {
        match self {
            Self::InvalidColumn | Self::Malformed => PeerPenaltyReason::GossipInvalid,
            Self::Spam => PeerPenaltyReason::RateLimit,
            Self::CustodyRefuse => PeerPenaltyReason::CustodyUnserved,
            Self::StallReqresp => PeerPenaltyReason::ReqrespFault,
        }
    }

    /// All four (five tokens) kinds for control-run iteration.
    pub const ALL: [Self; 5] = [
        Self::InvalidColumn,
        Self::Malformed,
        Self::Spam,
        Self::CustodyRefuse,
        Self::StallReqresp,
    ];
}

/// Adversarial / publisher fault kind.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FaultMode {
    /// Plain publisher / no-op relay (default).
    #[default]
    None,
    /// CC-2Jb — withhold one or more columns from gossip + by-root until flag.
    WithholdColumn {
        /// Column indices to withhold.
        columns: Vec<u64>,
    },
    /// CC-2Jc — misbehave (invalid-column / malformed / spam / …).
    Misbehave {
        /// Typed misbehaviour kind.
        kind: MisbehaveKind,
    },
}

impl FaultMode {
    /// Parse `--fault-mode` value.
    ///
    /// Accepted forms:
    /// - empty / `none` / `plain` / `relay` → [`FaultMode::None`]
    /// - `withhold-column` / `withhold-column:1,2` / `withhold-column=1,2`
    ///   → [`FaultMode::WithholdColumn`]
    /// - `misbehave` / `misbehave:spam` → [`FaultMode::Misbehave`]
    /// - `withhold-column` / `withhold-column:1,2` → [`FaultMode::WithholdColumn`]
    /// - `misbehave:spam` / `misbehave=invalid-column` → [`FaultMode::Misbehave`]
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty()
            || s.eq_ignore_ascii_case("none")
            || s.eq_ignore_ascii_case("plain")
            || s.eq_ignore_ascii_case("relay")
        {
            return Ok(Self::None);
        }
        if let Some(rest) = s.strip_prefix("withhold-column") {
            let columns = parse_column_list(rest.trim_start_matches([':', '=']))?;
            return Ok(Self::WithholdColumn { columns });
        }
        if let Some(rest) = s.strip_prefix("misbehave") {
            let kind_s = rest.trim_start_matches([':', '=']).trim();
            let kind = MisbehaveKind::parse(kind_s)?;
            return Ok(Self::Misbehave { kind });
        }
        bail!("unknown fault mode {s:?}; expected none|withhold-column|misbehave:<kind>")
    }

    /// Returns `Ok(())` for all shipped fault modes (plain, withhold-column, misbehave).
    ///
    /// Both fault modes have landed (CC-2Jb + CC-2Jc); this is the post-merge form of
    /// the temporary stubs that previously failed closed for the other stream.
    pub fn ensure_implemented(&self) -> Result<()> {
        match self {
            Self::None | Self::WithholdColumn { .. } | Self::Misbehave { .. } => Ok(()),
        }
    }

    /// Whether gossip may publish this column index.
    #[must_use]
    pub fn allows_publish_column(&self, column_index: u64) -> bool {
        match self {
            Self::None | Self::Misbehave { .. } => true,
            Self::WithholdColumn { columns } => !columns.contains(&column_index),
        }
    }

    /// Whether a held column may be served by-root.
    ///
    /// Withheld indices require `released == true` (flag file present).
    #[must_use]
    pub fn allows_by_root_serve(&self, column_index: u64, released: bool) -> bool {
        match self {
            Self::None | Self::Misbehave { .. } => true,
            Self::WithholdColumn { columns } => {
                if columns.contains(&column_index) {
                    released
                } else {
                    true
                }
            }
        }
    }

    /// Withheld column indices, if any.
    #[must_use]
    pub fn withheld_columns(&self) -> &[u64] {
        match self {
            Self::WithholdColumn { columns } => columns.as_slice(),
            _ => &[],
        }
    }

    /// Refuse to start when any withheld index falls outside the target's
    /// sampled set (same public `get_custody_groups` path node-a uses).
    ///
    /// Returns the target's sampled column indices on success.
    pub fn ensure_withheld_in_sampled(
        &self,
        target_role: &str,
        target_cgc: u64,
    ) -> Result<BTreeSet<u64>> {
        let sampled = sampled_columns_for_role(target_role, target_cgc)?;
        let Self::WithholdColumn { columns } = self else {
            return Ok(sampled);
        };
        if columns.is_empty() {
            bail!("withhold-column requires at least one column index (e.g. withhold-column=3)");
        }
        for &idx in columns {
            if idx >= COLUMN_SUBNET_COUNT {
                bail!("column index {idx} out of range [0, {COLUMN_SUBNET_COUNT})");
            }
            if !sampled.contains(&idx) {
                bail!(
                    "withheld column {idx} is not in {target_role}'s sampled set {:?}; refuse to start",
                    sampled.iter().copied().collect::<Vec<_>>()
                );
            }
        }
        Ok(sampled)
    }

    /// Map onto the by-root Track D policy (CC-2Jc seam in `reqresp/columns.rs`).
    #[must_use]
    pub fn by_root_fault_policy(&self) -> ByRootFaultPolicy {
        match self {
            Self::Misbehave {
                kind: MisbehaveKind::CustodyRefuse,
            } => ByRootFaultPolicy::CustodyRefuse,
            Self::Misbehave {
                kind: MisbehaveKind::StallReqresp,
            } => ByRootFaultPolicy::StallReqresp,
            _ => ByRootFaultPolicy::Honest,
        }
    }

    /// Relay transform for a single payload (block / single-shot column path).
    ///
    /// - [`Self::None`] / [`Self::WithholdColumn`]: identity — column skip is
    ///   the publish seam ([`decide_column_publish`]), not this transform.
    /// - [`Self::Misbehave`]: mutates for invalid/malformed; identity for
    ///   custody-refuse / stall. Prefer [`Self::column_publish_payloads`] for
    ///   multi-publish spam.
    ///
    /// Blocks always pass through for [`Self::None`] and [`Self::WithholdColumn`]
    /// (column skip uses [`Self::allows_publish_column`] /
    /// [`Self::column_publish_payloads`]). Misbehave applies the kind's gossip
    /// mutation (CC-2Jc).
    #[must_use]
    pub fn relay(&self, payload: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::None | Self::WithholdColumn { .. } => Some(payload.to_vec()),
            Self::Misbehave {
                kind: MisbehaveKind::CustodyRefuse | MisbehaveKind::StallReqresp,
            } => Some(payload.to_vec()),
            Self::Misbehave { kind } => Some(transform_column_payload(payload, *kind, 0)),
        }
    }

    /// One or more gossip payloads for a single fixture column under this mode.
    ///
    /// Spam emits the honest payload plus [`SPAM_GOSSIP_EXTRA_PUBLISHES`]
    /// distinct variants so message-ids do not collapse. Withhold emits none.
    #[must_use]
    pub fn column_publish_payloads(&self, payload: &[u8]) -> Vec<Vec<u8>> {
        match self {
            Self::None => vec![payload.to_vec()],
            Self::WithholdColumn { .. } => Vec::new(),
            Self::Misbehave {
                kind: MisbehaveKind::Spam,
            } => {
                let mut out = Vec::with_capacity(SPAM_GOSSIP_EXTRA_PUBLISHES as usize + 1);
                out.push(payload.to_vec());
                for i in 1..=SPAM_GOSSIP_EXTRA_PUBLISHES {
                    out.push(transform_column_payload(payload, MisbehaveKind::Spam, i));
                }
                out
            }
            Self::Misbehave { kind } => vec![transform_column_payload(payload, *kind, 0)],
        }
    }

    /// Whether this mode floods outbound column req/resp (spam).
    #[must_use]
    pub fn spam_reqresp(&self) -> bool {
        matches!(
            self,
            Self::Misbehave {
                kind: MisbehaveKind::Spam
            }
        )
    }
}

/// Mutate a fixture column sidecar payload for a misbehave kind.
///
/// `variant` differentiates spam message-ids (XOR salt). Non-spam kinds ignore it.
#[must_use]
pub fn transform_column_payload(payload: &[u8], kind: MisbehaveKind, variant: u32) -> Vec<u8> {
    match kind {
        MisbehaveKind::InvalidColumn => mutate_kzg_proof(payload),
        MisbehaveKind::Malformed => make_malformed(payload),
        MisbehaveKind::Spam => spam_variant(payload, variant),
        // Custody / stall do not mutate gossip — honest publish, fault on by-root.
        MisbehaveKind::CustodyRefuse | MisbehaveKind::StallReqresp => payload.to_vec(),
    }
}

/// Decode as `DataColumnSidecar`, flip one byte in the first KZG proof, re-encode.
/// Falls back to a trailing XOR if decode fails (still yields a non-honest payload).
fn mutate_kzg_proof(payload: &[u8]) -> Vec<u8> {
    use cc_types::Mainnet;
    use cc_types::sidecar::DataColumnSidecar;

    if let Ok(mut sc) = DataColumnSidecar::<Mainnet>::from_ssz_bytes(payload) {
        if let Some(proof) = sc.kzg_proofs.first_mut() {
            proof.0[0] ^= 0xFF;
        } else {
            // Empty proofs list — raw mutate fallback.
            return trailing_xor(payload, 0xA5);
        }
        return sc.as_ssz_bytes();
    }
    trailing_xor(payload, 0xA5)
}

/// Truncate so SSZ decode fails (tampered length / truncated container).
fn make_malformed(payload: &[u8]) -> Vec<u8> {
    if payload.is_empty() {
        return vec![0xDE, 0xAD];
    }
    // Keep a non-empty prefix so gossip still carries bytes, but drop the tail.
    let keep = (payload.len() / 2)
        .max(1)
        .min(payload.len().saturating_sub(1));
    let mut out = payload[..keep].to_vec();
    // Force an obviously broken length-ish prefix when long enough.
    if out.len() >= 4 {
        out[0] = 0xFF;
        out[1] = 0xFF;
        out[2] = 0xFF;
        out[3] = 0xFF;
    }
    out
}

/// Spam variant: honest body with a trailing salt byte (distinct message-id).
fn spam_variant(payload: &[u8], variant: u32) -> Vec<u8> {
    if variant == 0 {
        return payload.to_vec();
    }
    let mut out = payload.to_vec();
    out.push((variant & 0xFF) as u8);
    out.push(((variant >> 8) & 0xFF) as u8);
    out
}

fn trailing_xor(payload: &[u8], salt: u8) -> Vec<u8> {
    if payload.is_empty() {
        return vec![salt];
    }
    let mut out = payload.to_vec();
    if let Some(last) = out.last_mut() {
        *last ^= salt;
    }
    out
}

fn parse_column_list(s: &str) -> Result<Vec<u64>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for p in s.split(',') {
        let idx = p
            .trim()
            .parse::<u64>()
            .with_context(|| format!("bad column index {p:?}"))?;
        if !out.contains(&idx) {
            out.push(idx);
        }
    }
    Ok(out)
}

/// discv5 [`NodeId`] for a deterministic devnet role (publisher / node-a / …).
pub fn node_id_for_role(role: &str) -> Result<NodeId> {
    let secret = derive_node_secret(role);
    let mut bytes = secret;
    let key = CombinedKey::secp256k1_from_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("CombinedKey for role {role}: {e}"))?;
    let enr = Enr::empty(&key).map_err(|e| anyhow::anyhow!("Enr::empty for role {role}: {e}"))?;
    Ok(enr.node_id())
}

/// Sampled **column** indices for `role` at `cgc` (default node-a: 4 → 8 samples).
pub fn sampled_columns_for_role(role: &str, cgc: u64) -> Result<BTreeSet<u64>> {
    let node_id = node_id_for_role(role)?;
    let mgr = CustodyManager::new(node_id, cgc);
    let mut cols = BTreeSet::new();
    for group in mgr.sampled().iter() {
        for col in compute_columns_for_custody_group(*group) {
            cols.insert(col);
        }
    }
    Ok(cols)
}

/// R-4: fixture under test must have columns (non-zero commitment count).
///
/// Prefers `manifest.json`'s `blobs_per_block` / `blobs_per_block_cycle`, then
/// asserts the loaded store actually holds at least one column sidecar.
pub fn assert_nonzero_commitments(manifest_path: &Path, store: &FixtureStore) -> Result<()> {
    if manifest_path.is_file() {
        let text = fs::read_to_string(manifest_path)
            .with_context(|| format!("read {}", manifest_path.display()))?;
        let v: serde_json::Value = serde_json::from_str(&text).context("manifest json")?;
        if let Some(arr) = v.get("blobs_per_block").and_then(|x| x.as_array()) {
            let any = arr.iter().any(|x| x.as_u64().unwrap_or(0) > 0);
            if !any {
                bail!("manifest blobs_per_block has no non-zero entry (R-4)");
            }
        } else if let Some(cycle) = v.get("blobs_per_block_cycle").and_then(|x| x.as_array()) {
            let any = cycle.iter().any(|x| x.as_u64().unwrap_or(0) > 0);
            if !any {
                bail!("manifest blobs_per_block_cycle has no non-zero entry (R-4)");
            }
        }
    }
    let has_cols = store.slots.iter().any(|s| !s.columns.is_empty());
    if !has_cols {
        bail!("fixture has no column sidecars to withhold (R-4)");
    }
    Ok(())
}

// ── deterministic identity ──────────────────────────────────────────────────

/// Derive a 32-byte secp256k1 secret from the committed seed and a role name.
///
/// `secret = SHA-256(DEVNET_KEY_SEED || ":" || role)`. Deterministic across
/// `up.sh` runs; two runs produce the same peer ids.
#[must_use]
pub fn derive_node_secret(role: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(DEVNET_KEY_SEED.as_bytes());
    h.update(b":");
    h.update(role.as_bytes());
    let digest = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Write raw 32-byte node key with mode `0600` (Unix). Creates parent dirs.
pub fn write_node_key(path: &Path, secret: &[u8; 32]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("create {}", path.display()))?;
        f.write_all(secret)
            .with_context(|| format!("write {}", path.display()))?;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, secret).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

/// Load a 32-byte secp256k1 secret from `path`.
pub fn load_node_key(path: &Path) -> Result<[u8; 32]> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() != 32 {
        bail!(
            "node key at {} must be 32 bytes, got {}",
            path.display(),
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Build a libp2p [`Keypair`] from a raw 32-byte secp256k1 secret.
pub fn keypair_from_secret(secret: &[u8; 32]) -> Result<Keypair> {
    let mut bytes = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("secp256k1 secret: {e}"))?;
    Ok(Keypair::from(identity::secp256k1::Keypair::from(sk)))
}

/// Build a discv5 [`CombinedKey`] from the same secret.
pub fn combined_key_from_secret(secret: &[u8; 32]) -> Result<CombinedKey> {
    let mut bytes = *secret;
    CombinedKey::secp256k1_from_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("discv5 CombinedKey: {e}"))
}

/// Build a signed ENR for a container (static bootnode wiring).
pub fn build_enr(secret: &[u8; 32], ip: Ipv4Addr, tcp_port: u16, udp_port: u16) -> Result<Enr> {
    let key = combined_key_from_secret(secret)?;
    let enr = Enr::builder()
        .ip4(ip)
        .tcp4(tcp_port)
        .udp4(udp_port)
        .build(&key)
        .map_err(|e| anyhow::anyhow!("enr build: {e}"))?;
    Ok(enr)
}

/// Multiaddr for static dialling (`/ip4/…/tcp/…`).
#[must_use]
pub fn multiaddr_for(ip: Ipv4Addr, tcp_port: u16) -> Multiaddr {
    format!("/ip4/{ip}/tcp/{tcp_port}")
        .parse()
        .expect("static multiaddr is well-formed")
}

// ── fixture store ───────────────────────────────────────────────────────────

/// One slot's block + column sidecars as raw SSZ bytes (CC-2Ja layout).
#[derive(Debug, Clone)]
pub struct SlotFixture {
    /// Slot number.
    pub slot: u64,
    /// Block root from `meta.json` when present (0x-hex).
    pub block_root: Option<[u8; 32]>,
    /// `block.ssz` bytes.
    pub block_ssz: Vec<u8>,
    /// Column index → `column_XXX.ssz` bytes.
    pub columns: HashMap<u64, Vec<u8>>,
}

/// In-memory fixture loaded from `devnet/out/chain/`.
///
/// Serves the data plane for gossip publish and (once CC-23a lands) for
/// `DataColumnSidecarsByRoot` / `ByRange`.
#[derive(Debug, Clone, Default)]
pub struct FixtureStore {
    /// Slots ordered ascending.
    pub slots: Vec<SlotFixture>,
    /// `block_root → slot` for by-root lookup.
    by_root: HashMap<[u8; 32], u64>,
}

impl FixtureStore {
    /// Load `chain_dir/slot_NNNNNN/{block.ssz,column_XXX.ssz,meta.json}`.
    pub fn load(chain_dir: &Path) -> Result<Self> {
        if !chain_dir.is_dir() {
            bail!("fixture chain dir missing: {}", chain_dir.display());
        }
        let mut entries: Vec<PathBuf> = fs::read_dir(chain_dir)
            .with_context(|| format!("read_dir {}", chain_dir.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("slot_"))
            })
            .collect();
        entries.sort();

        let mut slots = Vec::with_capacity(entries.len());
        let mut by_root = HashMap::new();

        for dir in entries {
            let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let slot: u64 = name
                .strip_prefix("slot_")
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("slot dir name {name}"))?;
            let block_ssz = fs::read(dir.join("block.ssz"))
                .with_context(|| format!("read {}/block.ssz", dir.display()))?;
            let mut columns = HashMap::new();
            for e in fs::read_dir(&dir)? {
                let e = e?;
                let fname = e.file_name();
                let fname = fname.to_string_lossy();
                if let Some(rest) = fname.strip_prefix("column_")
                    && let Some(idx_s) = rest.strip_suffix(".ssz")
                    && let Ok(idx) = idx_s.parse::<u64>()
                {
                    columns.insert(idx, fs::read(e.path())?);
                }
            }
            let block_root = read_meta_block_root(&dir.join("meta.json"));
            if let Some(root) = block_root {
                by_root.insert(root, slot);
            }
            slots.push(SlotFixture {
                slot,
                block_root,
                block_ssz,
                columns,
            });
        }
        if slots.is_empty() {
            bail!("no slot_* entries under {}", chain_dir.display());
        }
        Ok(Self { slots, by_root })
    }

    /// Slot range covered (min, max), inclusive.
    #[must_use]
    pub fn slot_range(&self) -> (u64, u64) {
        let min = self.slots.first().map(|s| s.slot).unwrap_or(0);
        let max = self.slots.last().map(|s| s.slot).unwrap_or(0);
        (min, max)
    }

    /// Lookup a slot fixture.
    #[must_use]
    pub fn by_slot(&self, slot: u64) -> Option<&SlotFixture> {
        self.slots.iter().find(|s| s.slot == slot)
    }

    /// `DataColumnSidecarsByRoot` answer from the fixture (store layer).
    #[must_use]
    pub fn sidecar_by_root(&self, block_root: &[u8; 32], column: u64) -> Option<&[u8]> {
        let slot = *self.by_root.get(block_root)?;
        self.by_slot(slot)
            .and_then(|s| s.columns.get(&column).map(|v| v.as_slice()))
    }

    /// `DataColumnSidecarsByRange` answer: all sidecars for `column` in
    /// `[start_slot, start_slot + count)`.
    #[must_use]
    pub fn sidecars_by_range(&self, start_slot: u64, count: u64, column: u64) -> Vec<&[u8]> {
        let end = start_slot.saturating_add(count);
        self.slots
            .iter()
            .filter(|s| s.slot >= start_slot && s.slot < end)
            .filter_map(|s| s.columns.get(&column).map(|v| v.as_slice()))
            .collect()
    }

    /// Max column index observed (expect 127 when full 128 columns present).
    #[must_use]
    pub fn max_column_index(&self) -> Option<u64> {
        self.slots
            .iter()
            .flat_map(|s| s.columns.keys().copied())
            .max()
    }
}

fn read_meta_block_root(path: &Path) -> Option<[u8; 32]> {
    let text = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let hex = v
        .get("block_root")
        .or_else(|| v.get("root"))
        .and_then(|x| x.as_str())?;
    parse_root_hex(hex).ok()
}

fn parse_root_hex(s: &str) -> Result<[u8; 32]> {
    let s = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(s).context("hex decode root")?;
    if bytes.len() != 32 {
        bail!("root must be 32 bytes, got {}", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

// ── publisher / mesh runtime ────────────────────────────────────────────────

/// How the process behaves on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevnetRole {
    /// Publish fixture at slot cadence (`--publish-fixture`).
    Publisher,
    /// Subscribe + dial static peers; count received gossip (node-a / node-b).
    Peer,
}

/// Runtime configuration for the self-devnet p2p path.
#[derive(Debug, Clone)]
pub struct DevnetRuntimeConfig {
    /// Publisher or passive peer.
    pub role: DevnetRole,
    /// Fixture chain directory (`devnet/out/chain`).
    pub fixture_chain: PathBuf,
    /// Network `config.yaml` for digests / slot time.
    pub config_yaml: PathBuf,
    /// `manifest.json` for genesis validators root + slot count.
    pub manifest_json: PathBuf,
    /// Persisted node key path (32 raw bytes).
    pub node_key_path: PathBuf,
    /// TCP listen multiaddr (e.g. `/ip4/0.0.0.0/tcp/9000`).
    pub listen: Multiaddr,
    /// Static peers to dial (multiaddrs); typically publisher + siblings.
    pub static_peers: Vec<Multiaddr>,
    /// When true, do not dial (single-peer receive-only mode for CC-2Jb).
    pub disable_dial: bool,
    /// Fault mode ([`FaultMode::None`] plain; [`FaultMode::WithholdColumn`] CC-2Jb).
    pub fault_mode: FaultMode,
    /// Flag file whose presence releases withheld columns for by-root serve.
    pub fault_flag_path: Option<PathBuf>,
    /// Role whose sampled set must contain withheld indices (default `node-a`).
    pub withhold_target_role: String,
    /// Target custody group count used to compute the sampled set (default 4).
    pub withhold_target_cgc: u64,
    /// Metrics bind address.
    pub metrics_addr: SocketAddr,
    /// gRPC bind (health / GetInfo still served).
    pub grpc_addr: SocketAddr,
    /// Optional wall-clock genesis override (unix seconds). Default: now.
    pub genesis_time_override: Option<u64>,
    /// Seconds per slot override; else from config.yaml.
    pub seconds_per_slot_override: Option<u64>,
    /// Slot to begin publishing from (default: first fixture slot).
    pub start_slot: Option<u64>,
    /// Stop after this many slots published (default: all).
    pub max_slots: Option<u64>,
}

/// Load genesis validators root from `manifest.json`.
pub fn load_gvr_from_manifest(path: &Path) -> Result<Root> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&text).context("manifest json")?;
    let hex = v
        .get("genesis_validators_root")
        .and_then(|x| x.as_str())
        .context("manifest.genesis_validators_root")?;
    let arr = parse_root_hex(hex)?;
    Ok(Root::from_array(arr))
}

/// Topic path-segment labels used for metrics (short names).
#[must_use]
pub fn topic_label(name: TopicName) -> String {
    name.path_segment()
}

/// Build the set of topics the publisher subscribes to / publishes on:
/// `beacon_block` + all 128 `data_column_sidecar_{i}`.
#[must_use]
pub fn publisher_topic_names() -> Vec<TopicName> {
    let mut names = vec![TopicName::BeaconBlock];
    for i in 0..COLUMN_SUBNET_COUNT {
        names.push(TopicName::DataColumnSidecar(i));
    }
    names
}

/// Default peer subscription set at M2.1: block + all columns (sampled set
/// arrives with CC-24a; full subscribe keeps smoke deterministic).
#[must_use]
pub fn peer_topic_names() -> Vec<TopicName> {
    publisher_topic_names()
}

/// Run the self-devnet swarm loop (publisher or peer).
///
/// Also keeps Phase 0 gRPC health + `/metrics` alive on the configured addrs.
pub async fn run_devnet(
    cfg: DevnetRuntimeConfig,
    metrics: P2pMetrics,
    registry: Arc<prometheus_client::registry::Registry>,
) -> Result<()> {
    cfg.fault_mode.ensure_implemented()?;

    // R-4 + sampled-set precondition before any bind (withhold only).
    if matches!(cfg.fault_mode, FaultMode::WithholdColumn { .. }) {
        let store_preview = FixtureStore::load(&cfg.fixture_chain)?;
        assert_nonzero_commitments(&cfg.manifest_json, &store_preview)?;
        let sampled = cfg
            .fault_mode
            .ensure_withheld_in_sampled(&cfg.withhold_target_role, cfg.withhold_target_cgc)?;
        info!(
            withheld = ?cfg.fault_mode.withheld_columns(),
            target = %cfg.withhold_target_role,
            sampled = ?sampled.iter().copied().collect::<Vec<_>>(),
            flag = ?cfg.fault_flag_path,
            "withhold-column preconditions ok"
        );
    }

    // Install process-global state the Track D seams consult.
    install_active_fault(cfg.fault_mode.clone(), cfg.fault_flag_path.clone());
    if let Some(ref p) = cfg.fault_flag_path {
        // Ensure a stale flag from a prior run does not pre-release.
        if p.exists() {
            let _ = fs::remove_file(p);
            info!(path = %p.display(), "cleared stale release flag");
        }
    }

    let secret = if cfg.node_key_path.exists() {
        load_node_key(&cfg.node_key_path)?
    } else {
        bail!(
            "node key missing at {} — run devnet/up.sh first",
            cfg.node_key_path.display()
        );
    };
    let keypair = keypair_from_secret(&secret)?;
    let local_peer_id = PeerId::from_public_key(&keypair.public());
    info!(%local_peer_id, role = ?cfg.role, "devnet identity");

    let chain_cfg = ChainConfig::from_yaml_file(&cfg.config_yaml)
        .map_err(|e| anyhow::anyhow!("config.yaml: {e}"))?;
    let gvr = load_gvr_from_manifest(&cfg.manifest_json)?;
    let seconds_per_slot = cfg
        .seconds_per_slot_override
        .unwrap_or(chain_cfg.seconds_per_slot.max(1));

    let store = if cfg.role == DevnetRole::Publisher {
        Some(FixtureStore::load(&cfg.fixture_chain)?)
    } else {
        // Peer may still load for by-root self-test when path exists.
        if cfg.fixture_chain.is_dir() {
            FixtureStore::load(&cfg.fixture_chain).ok()
        } else {
            None
        }
    };

    let behaviour = CcBehaviour::new(&keypair, crate::reqresp::ethereum_behaviour_config())
        .map_err(|e| anyhow::anyhow!("CcBehaviour: {e}"))?;
    let mut swarm = build_swarm(keypair, behaviour, &SwarmConfig::default())
        .map_err(|e| anyhow::anyhow!("build_swarm: {e}"))?;

    swarm
        .listen_on(cfg.listen.clone())
        .with_context(|| format!("listen on {}", cfg.listen))?;

    // Epoch 0 digest for Fulu-at-genesis devnet.
    let digest = compute_fork_digest(&chain_cfg, gvr, Epoch::new(0));
    let topic_names = match cfg.role {
        DevnetRole::Publisher => publisher_topic_names(),
        DevnetRole::Peer => peer_topic_names(),
    };
    let mut topics: HashMap<String, IdentTopic> = HashMap::new();
    for name in &topic_names {
        let s = format_topic_string(&digest, *name);
        let topic = IdentTopic::new(s.clone());
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|e| anyhow::anyhow!("subscribe {s}: {e:?}"))?;
        topics.insert(topic_label(*name), topic);
    }
    info!(count = topics.len(), "subscribed topics");

    // Initial dial of static peers (H2: continuous re-dial keeps recovery alive).
    if cfg.disable_dial {
        info!("dialling disabled (scenario single-peer / no static peers)");
    } else {
        dial_static_peers(&mut swarm, &cfg.static_peers);
    }

    // Metrics exposition (Phase 0 path).
    let metrics_addr = cfg.metrics_addr;
    let reg = Arc::clone(&registry);
    tokio::spawn(async move {
        if let Err(e) = cc_bootstrap::serve_metrics(metrics_addr, reg).await {
            warn!(error = %e, "metrics server exited");
        }
    });

    let genesis_time = cfg.genesis_time_override.unwrap_or_else(now_unix);
    let mut next_publish_slot = cfg
        .start_slot
        .or_else(|| store.as_ref().map(|s| s.slot_range().0));
    let mut published_count: u64 = 0;
    let mut inbound_peers: u64 = 0;
    let mut outbound_peers: u64 = 0;
    // H1: do not catch-up publish until at least one mesh peer is connected
    // (or a timeout elapses so solo publisher can still self-progress).
    let mut mesh_ready = false;
    let mesh_wait_deadline = now_unix().saturating_add(60);

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // H2: periodic re-dial so offline-gap recovery does not need process restart.
    let mut redial_tick = tokio::time::interval(Duration::from_secs(5));
    redial_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!(%peer_id, ?endpoint, "connection established");
                        if endpoint.is_dialer() {
                            outbound_peers = outbound_peers.saturating_add(1);
                        } else {
                            inbound_peers = inbound_peers.saturating_add(1);
                        }
                        set_peer_gauges(&metrics, inbound_peers, outbound_peers);
                        if inbound_peers.saturating_add(outbound_peers) > 0 {
                            mesh_ready = true;
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, endpoint, .. } => {
                        info!(%peer_id, "connection closed");
                        if endpoint.is_dialer() {
                            outbound_peers = outbound_peers.saturating_sub(1);
                        } else {
                            inbound_peers = inbound_peers.saturating_sub(1);
                        }
                        set_peer_gauges(&metrics, inbound_peers, outbound_peers);
                        // Immediate re-dial attempt after disconnect (H2).
                        if !cfg.disable_dial {
                            dial_static_peers(&mut swarm, &cfg.static_peers);
                        }
                    }
                    SwarmEvent::Behaviour(CcBehaviourEvent::Gossipsub(ev)) => {
                        handle_gossip_event(ev, &mut swarm, &metrics, &topics);
                    }
                    SwarmEvent::Behaviour(CcBehaviourEvent::Reqresp(ev)) => {
                        handle_publisher_reqresp(
                            ev,
                            &mut swarm,
                            &cfg.fault_mode,
                            store.as_ref(),
                            &metrics,
                        );
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        warn!(?peer_id, error = %error, "outgoing connection error");
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        warn!(error = %error, "incoming connection error");
                    }
                    _ => {}
                }
            }
            _ = redial_tick.tick() => {
                if !cfg.disable_dial {
                    dial_static_peers(&mut swarm, &cfg.static_peers);
                }
            }
            _ = tick.tick() => {
                if cfg.role != DevnetRole::Publisher {
                    continue;
                }
                let Some(store) = store.as_ref() else { continue };
                let Some(slot) = next_publish_slot else { continue };
                if let Some(max) = cfg.max_slots
                    && published_count >= max
                {
                    continue;
                }
                // H1: wait for mesh (or timeout) before starting catch-up publish.
                if !mesh_ready {
                    if now_unix() >= mesh_wait_deadline {
                        warn!("mesh wait timed out; publishing without peers");
                        mesh_ready = true;
                    } else {
                        continue;
                    }
                }
                let slot_start = genesis_time.saturating_add(slot.saturating_mul(seconds_per_slot));
                let now = now_unix();
                if now < slot_start {
                    continue;
                }
                let Some(fx) = store.by_slot(slot) else {
                    // Missing slot dir: skip cursor without counting as success.
                    next_publish_slot = Some(slot.saturating_add(1));
                    continue;
                };

                // Block first — H3: only advance slot on successful block publish.
                let mut block_ok = false;
                if let Some(topic) = topics.get("beacon_block")
                    && let Some(payload) = cfg.fault_mode.relay(&fx.block_ssz)
                {
                    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
                        Ok(_) => {
                            metrics
                                .gossip_messages
                                .get_or_create(&GossipMessageLabels {
                                    topic: "beacon_block".to_owned(),
                                    verdict: "published".to_owned(),
                                })
                                .inc();
                            block_ok = true;
                        }
                        Err(e) => warn!(slot, error = %e, "publish beacon_block"),
                    }
                }
                if !block_ok {
                    // InsufficientPeers or missing topic: retry same slot next tick.
                    continue;
                }

                // All columns present in fixture (cgc=128 force).
                // Track D publish seam: skip withheld indices (CC-2Jb).
                // CC-2Jc: invalid-column / malformed / spam mutate on the way out;
                // custody-refuse / stall-reqresp publish honestly (fault is by-root).
                for (idx, bytes) in &fx.columns {
                    if decide_column_publish(*idx) != ColumnPublishDecision::Publish {
                        continue;
                    }
                    let label = format!("data_column_sidecar_{idx}");
                    let Some(topic) = topics.get(&label) else { continue };
                    for payload in cfg.fault_mode.column_publish_payloads(bytes) {
                        match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
                            Ok(_) => {
                                metrics
                                    .gossip_messages
                                    .get_or_create(&GossipMessageLabels {
                                        topic: label.clone(),
                                        verdict: "published".to_owned(),
                                    })
                                    .inc();
                            }
                            Err(e) => warn!(slot, column = idx, error = %e, "publish column"),
                        }
                    }
                }

                metrics.backfill_progress_slots.set(slot as i64);
                published_count = published_count.saturating_add(1);
                info!(slot, published_count, "published fixture slot");
                next_publish_slot = Some(slot.saturating_add(1));
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal");
                break;
            }
        }
    }
    Ok(())
}

fn set_peer_gauges(metrics: &P2pMetrics, inbound: u64, outbound: u64) {
    metrics
        .peers
        .get_or_create(&DirectionLabels {
            direction: Direction::Inbound.as_str().to_owned(),
        })
        .set(inbound as i64);
    metrics
        .peers
        .get_or_create(&DirectionLabels {
            direction: Direction::Outbound.as_str().to_owned(),
        })
        .set(outbound as i64);
}

/// Dial every configured static multiaddr (best-effort; ignores "already dialing").
fn dial_static_peers(swarm: &mut cc_libp2p::Swarm<CcBehaviour>, peers: &[Multiaddr]) {
    for addr in peers {
        match swarm.dial(addr.clone()) {
            Ok(()) => info!(%addr, "dialing static peer"),
            Err(e) => {
                // DialError::DialPeerConditionFalse / NoAddresses etc. are noisy at warn.
                tracing::debug!(%addr, error = %e, "static dial skipped/failed");
            }
        }
    }
}

/// Publisher-side by-root/by-range answers from the fixture store (CC-2Jc).
///
/// Applies [`FaultMode::by_root_fault_policy`] so custody-refuse returns
/// ResourceUnavailable and stall-reqresp delays past TTFB.
fn handle_publisher_reqresp(
    ev: RequestResponseEvent<ReqRespRequest, ReqRespResponse>,
    swarm: &mut cc_libp2p::Swarm<CcBehaviour>,
    fault: &FaultMode,
    store: Option<&FixtureStore>,
    metrics: &P2pMetrics,
) {
    use crate::reqresp::Protocol;
    use crate::reqresp::codec::{CONTEXT_BYTES_LEN, ResponseChunk, ResponseCode, SszSnappyFraming};
    use crate::reqresp::columns::{
        ByRootServeDecision, ColumnsByRootRequest, decide_by_root_column_serve,
    };

    let RequestResponseEvent::Message { peer, message, .. } = ev else {
        return;
    };
    let RequestResponseMessage::Request {
        request, channel, ..
    } = message
    else {
        return;
    };

    let protocol_id = request.protocol.to_string();
    let Some(protocol) = Protocol::from_protocol_id(&protocol_id) else {
        let framed = encode_simple_error(ResponseCode::InvalidRequest, b"unknown protocol");
        let _ = swarm
            .behaviour_mut()
            .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
        return;
    };

    metrics.inc_reqresp_inbound(protocol.as_str(), "ok");

    // Only publisher answers column by-root with the fault seam; other protocols
    // get a resource-unavailable so the peer can distinguish "asked" from hang.
    if protocol != Protocol::DataColumnSidecarsByRootV1 {
        let framed = encode_simple_error(
            ResponseCode::ResourceUnavailable,
            b"not served by publisher",
        );
        let _ = swarm
            .behaviour_mut()
            .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
        return;
    }

    let Some(store) = store else {
        let framed = encode_simple_error(ResponseCode::ResourceUnavailable, b"no fixture store");
        let _ = swarm
            .behaviour_mut()
            .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
        return;
    };

    let policy = fault.by_root_fault_policy();
    let req = match ColumnsByRootRequest::from_ssz_bytes(&request.ssz) {
        Ok(r) => r,
        Err(_) => {
            let framed =
                encode_simple_error(ResponseCode::InvalidRequest, b"malformed column by_root");
            let _ = swarm
                .behaviour_mut()
                .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
            return;
        }
    };

    let mut chunks: Vec<ResponseChunk> = Vec::new();
    let mut stall = false;
    for id in &req.identifiers {
        let root = id.block_root.into_array();
        for col_idx in id.columns.iter() {
            let held = store.sidecar_by_root(&root, *col_idx).is_some();
            // Track D seam (same named decision as reqresp/columns.rs).
            let decision = decide_by_root_column_serve(held, *col_idx, policy);
            match decision {
                ByRootServeDecision::Serve | ByRootServeDecision::Stall => {
                    if decision == ByRootServeDecision::Stall {
                        stall = true;
                    }
                    let Some(ssz) = store.sidecar_by_root(&root, *col_idx) else {
                        continue;
                    };
                    // Publisher uses a zero context; peers re-check via digest later.
                    chunks.push(ResponseChunk::Success {
                        context: Some([0u8; CONTEXT_BYTES_LEN]),
                        ssz: ssz.to_vec(),
                    });
                }
                ByRootServeDecision::ResourceUnavailable => {
                    let framed = encode_simple_error(
                        ResponseCode::ResourceUnavailable,
                        b"custody-refuse or missing",
                    );
                    let _ = swarm
                        .behaviour_mut()
                        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
                    info!(%peer, "by-root refused under fault policy");
                    return;
                }
            }
        }
    }

    if chunks.is_empty() {
        let framed =
            encode_simple_error(ResponseCode::ResourceUnavailable, b"no columns available");
        let _ = swarm
            .behaviour_mut()
            .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
        return;
    }

    if stall {
        let delay = crate::reqresp::columns::stall_first_byte_delay();
        info!(%peer, ?delay, "stall-reqresp: delaying first byte past TTFB");
        std::thread::sleep(delay);
    }

    let framed = SszSnappyFraming::encode_response(&chunks, protocol)
        .unwrap_or_else(|_| encode_simple_error(ResponseCode::ServerError, b"encode failed"));
    let _ = swarm
        .behaviour_mut()
        .send_reqresp_response(channel, ReqRespResponse::from_framed(framed));
}

fn encode_simple_error(code: crate::reqresp::codec::ResponseCode, msg: &[u8]) -> Vec<u8> {
    use crate::reqresp::Protocol;
    use crate::reqresp::codec::{ResponseChunk, SszSnappyFraming};
    let chunk = ResponseChunk::Error {
        code: code.as_u8(),
        message: msg.to_vec(),
    };
    SszSnappyFraming::encode_response(&[chunk], Protocol::DataColumnSidecarsByRootV1)
        .unwrap_or_default()
}

fn handle_gossip_event(
    ev: cc_libp2p::reexport::gossipsub::Event,
    swarm: &mut cc_libp2p::Swarm<CcBehaviour>,
    metrics: &P2pMetrics,
    topics: &HashMap<String, IdentTopic>,
) {
    use cc_libp2p::reexport::gossipsub::Event;
    match ev {
        Event::Message {
            propagation_source,
            message_id,
            message,
        } => {
            let label = topic_hash_to_label(&message.topic, topics);
            metrics
                .gossip_messages
                .get_or_create(&GossipMessageLabels {
                    topic: label,
                    verdict: "accept".to_owned(),
                })
                .inc();
            // M6: validate_messages() requires explicit Accept so peers re-gossip.
            // Single report helper (CC-22d) — no direct gossipsub report call here.
            crate::host::report_gossipsub_validation(
                swarm,
                &message_id,
                &propagation_source,
                MessageAcceptance::Accept,
            );
        }
        Event::Subscribed { peer_id, topic } => {
            info!(%peer_id, %topic, "peer subscribed");
        }
        Event::Unsubscribed { peer_id, topic } => {
            info!(%peer_id, %topic, "peer unsubscribed");
        }
        _ => {}
    }
}

fn topic_hash_to_label(hash: &TopicHash, topics: &HashMap<String, IdentTopic>) -> String {
    for (label, t) in topics {
        if t.hash() == *hash {
            return label.clone();
        }
    }
    hash.to_string()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── bootnode emission (used by up.sh via `cc-p2p --emit-bootnodes`) ─────────

/// One container's static identity for bootnode wiring.
#[derive(Debug, Clone)]
pub struct BootnodeSpec {
    /// Role name (`publisher`, `node-a`, `node-b`).
    pub role: String,
    /// Docker-DNS hostname (compose service name).
    pub host: String,
    /// Advertised TCP port.
    pub tcp_port: u16,
    /// Advertised UDP port (discv5).
    pub udp_port: u16,
    /// Optional fixed IPv4 for ENR (compose network IP). When `None`, ENR
    /// omits IP and multiaddr uses the hostname via a separate file.
    pub ip: Option<Ipv4Addr>,
}

/// Write keys + `bootnodes.txt` + `multiaddrs.txt` under `out_dir`.
///
/// Two calls with the same seed/specs produce identical peer ids and ENR
/// signatures (same secrets).
pub fn emit_bootnodes(out_dir: &Path, specs: &[BootnodeSpec]) -> Result<()> {
    fs::create_dir_all(out_dir)?;
    let keys_dir = out_dir.join("node_keys");
    fs::create_dir_all(&keys_dir)?;

    let mut enr_lines = Vec::new();
    let mut multi_lines = Vec::new();
    let mut peer_lines = Vec::new();

    for spec in specs {
        let secret = derive_node_secret(&spec.role);
        let key_path = keys_dir.join(format!("{}.key", spec.role));
        write_node_key(&key_path, &secret)?;
        let kp = keypair_from_secret(&secret)?;
        let peer_id = PeerId::from_public_key(&kp.public());

        let ip = spec.ip.unwrap_or(Ipv4Addr::new(127, 0, 0, 1));
        let enr = build_enr(&secret, ip, spec.tcp_port, spec.udp_port)?;
        enr_lines.push(format!(
            "# role={} peer_id={} host={}\n{}",
            spec.role,
            peer_id,
            spec.host,
            enr.to_base64()
        ));
        // Multiaddr uses hostnames for docker DNS when no fixed IP is forced.
        let ma = if spec.ip.is_some() {
            format!("/ip4/{ip}/tcp/{}", spec.tcp_port)
        } else {
            // libp2p dns multiaddr
            format!("/dns/{}/tcp/{}", spec.host, spec.tcp_port)
        };
        multi_lines.push(format!("{} # {} peer_id={}", ma, spec.role, peer_id));
        peer_lines.push(format!("{} {}", spec.role, peer_id));
    }

    fs::write(out_dir.join("bootnodes.txt"), enr_lines.join("\n") + "\n")?;
    fs::write(
        out_dir.join("multiaddrs.txt"),
        multi_lines.join("\n") + "\n",
    )?;
    fs::write(out_dir.join("peer_ids.txt"), peer_lines.join("\n") + "\n")?;
    info!(dir = %out_dir.display(), "wrote bootnodes + keys");
    Ok(())
}

/// Default three-node topology specs (publisher, node-a, node-b).
#[must_use]
pub fn default_bootnode_specs() -> Vec<BootnodeSpec> {
    vec![
        BootnodeSpec {
            role: "publisher".into(),
            host: "publisher".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
        BootnodeSpec {
            role: "node-a".into(),
            host: "node-a".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
        BootnodeSpec {
            role: "node-b".into(),
            host: "node-b".into(),
            tcp_port: 9000,
            udp_port: 9000,
            ip: None,
        },
    ]
}

// ── CLI helpers ─────────────────────────────────────────────────────────────

/// Parse a listen multiaddr or `host:port` into [`Multiaddr`].
pub fn parse_listen(s: &str) -> Result<Multiaddr> {
    if s.starts_with('/') {
        return Multiaddr::from_str(s).map_err(|e| anyhow::anyhow!("multiaddr: {e}"));
    }
    // host:port → /ip4/0.0.0.0/tcp/port when host is 0.0.0.0 or *
    let (host, port) = s
        .rsplit_once(':')
        .context("listen must be multiaddr or host:port")?;
    let port: u16 = port.parse().context("listen port")?;
    let ip = if host == "0.0.0.0" || host == "*" || host.is_empty() {
        "0.0.0.0"
    } else {
        host
    };
    Multiaddr::from_str(&format!("/ip4/{ip}/tcp/{port}"))
        .map_err(|e| anyhow::anyhow!("multiaddr: {e}"))
}

/// Parse `host:port` into [`SocketAddr`].
pub fn parse_socket_addr(s: &str) -> Result<SocketAddr> {
    if let Ok(a) = SocketAddr::from_str(s) {
        return Ok(a);
    }
    // allow 0.0.0.0:9102 style already covered; try IpAddr
    let (h, p) = s.rsplit_once(':').context("socket addr")?;
    let port: u16 = p.parse()?;
    let ip: IpAddr = h.parse()?;
    Ok(SocketAddr::new(ip, port))
}

/// Read multiaddrs from a file (one per line; `#` comments allowed).
pub fn read_multiaddrs_file(path: &Path) -> Result<Vec<Multiaddr>> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let ma =
            Multiaddr::from_str(line).map_err(|e| anyhow::anyhow!("multiaddr {line:?}: {e}"))?;
        out.push(ma);
    }
    Ok(out)
}

// Silence unused SubnetCounts import warning path for future sampling sets.
#[allow(dead_code)]
fn _subnet_counts_mainnet() -> SubnetCounts {
    SubnetCounts::mainnet()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use cc_types::CUSTODY_REQUIREMENT;

    #[test]
    fn fault_mode_none_parses() {
        assert_eq!(FaultMode::parse("").unwrap(), FaultMode::None);
        assert_eq!(FaultMode::parse("none").unwrap(), FaultMode::None);
        assert_eq!(FaultMode::parse("plain").unwrap(), FaultMode::None);
        FaultMode::None.ensure_implemented().unwrap();
    }

    #[test]
    fn fault_mode_withhold_implemented() {
        let m = FaultMode::parse("withhold-column:3,7").unwrap();
        assert!(matches!(
            m,
            FaultMode::WithholdColumn { ref columns } if columns == &[3, 7]
        ));
        m.ensure_implemented().unwrap();
        assert!(m.allows_publish_column(0));
        assert!(!m.allows_publish_column(3));
        assert!(!m.allows_publish_column(7));
        assert!(!m.allows_by_root_serve(3, false));
        assert!(m.allows_by_root_serve(3, true));
        assert!(m.allows_by_root_serve(0, false));
    }

    #[test]
    fn fault_mode_withhold_equals_form() {
        let m = FaultMode::parse("withhold-column=5").unwrap();
        assert_eq!(m.withheld_columns(), &[5]);
    }

    #[test]
    fn fault_mode_misbehave_kinds_implemented() {
        for kind in MisbehaveKind::ALL {
            let m = FaultMode::parse(&format!("misbehave:{}", kind.as_str())).unwrap();
            assert!(matches!(m, FaultMode::Misbehave { kind: k } if k == kind));
            m.ensure_implemented().unwrap();
        }
        // Bare `misbehave` without kind is rejected.
        assert!(FaultMode::parse("misbehave").is_err());
        assert!(FaultMode::parse("misbehave:nope").is_err());
    }

    #[test]
    fn misbehave_penalty_reasons_map_correctly() {
        assert_eq!(
            MisbehaveKind::InvalidColumn.penalty_reason(),
            PeerPenaltyReason::GossipInvalid
        );
        assert_eq!(
            MisbehaveKind::Malformed.penalty_reason(),
            PeerPenaltyReason::GossipInvalid
        );
        assert_eq!(
            MisbehaveKind::Spam.penalty_reason(),
            PeerPenaltyReason::RateLimit
        );
        assert_eq!(
            MisbehaveKind::CustodyRefuse.penalty_reason(),
            PeerPenaltyReason::CustodyUnserved
        );
        assert_eq!(
            MisbehaveKind::StallReqresp.penalty_reason(),
            PeerPenaltyReason::ReqrespFault
        );
        // Behavioural (P7) is not induced by any kind — still a complete label set.
        assert_eq!(PeerPenaltyReason::Behavioural.as_str(), "behavioural");
    }

    #[test]
    fn by_root_fault_policy_from_misbehave() {
        assert_eq!(
            FaultMode::parse("misbehave:custody-refuse")
                .unwrap()
                .by_root_fault_policy(),
            ByRootFaultPolicy::CustodyRefuse
        );
        assert_eq!(
            FaultMode::parse("misbehave:stall-reqresp")
                .unwrap()
                .by_root_fault_policy(),
            ByRootFaultPolicy::StallReqresp
        );
        assert_eq!(
            FaultMode::parse("misbehave:spam")
                .unwrap()
                .by_root_fault_policy(),
            ByRootFaultPolicy::Honest
        );
        assert_eq!(
            FaultMode::None.by_root_fault_policy(),
            ByRootFaultPolicy::Honest
        );
    }

    #[test]
    fn plain_relay_is_identity() {
        let m = FaultMode::None;
        assert_eq!(m.relay(b"abc").as_deref(), Some(b"abc".as_slice()));
        // Withhold still relays block bytes; column skip is the publish seam.
        let w = FaultMode::WithholdColumn { columns: vec![1] };
        assert_eq!(w.relay(b"block").as_deref(), Some(b"block".as_slice()));
    }

    #[test]
    fn active_fault_seams_honour_flag_file() {
        clear_active_fault();
        let dir = tempfile_dir("flag");
        let flag = dir.join("release.flag");
        let mode = FaultMode::WithholdColumn {
            columns: vec![9, 11],
        };
        install_active_fault(mode, Some(flag.clone()));
        assert!(active_allows_column_publish(0));
        assert!(!active_allows_column_publish(9));
        assert!(!active_allows_by_root_serve(9));
        assert!(active_allows_by_root_serve(0));
        fs::write(&flag, b"release").unwrap();
        assert!(active_allows_by_root_serve(9));
        // Publish still withholds after release (only by-root opens).
        assert!(!active_allows_column_publish(9));
        clear_active_fault();
        assert!(active_allows_column_publish(9));
        assert!(active_allows_by_root_serve(9));
    }

    #[test]
    fn withhold_outside_sampled_refuses() {
        // Pick an index almost certainly not in the 8-sampled set by scanning.
        let sampled = sampled_columns_for_role("node-a", CUSTODY_REQUIREMENT).unwrap();
        assert_eq!(sampled.len(), 8);
        let outside = (0..COLUMN_SUBNET_COUNT)
            .find(|i| !sampled.contains(i))
            .expect("must have non-sampled columns");
        let m = FaultMode::WithholdColumn {
            columns: vec![outside],
        };
        let err = m
            .ensure_withheld_in_sampled("node-a", CUSTODY_REQUIREMENT)
            .unwrap_err()
            .to_string();
        assert!(err.contains("refuse to start"), "{err}");
        assert!(err.contains(&outside.to_string()), "{err}");
    }

    #[test]
    fn withhold_inside_sampled_accepts() {
        let sampled = sampled_columns_for_role("node-a", CUSTODY_REQUIREMENT).unwrap();
        let idx = *sampled.iter().next().unwrap();
        let m = FaultMode::WithholdColumn { columns: vec![idx] };
        let got = m
            .ensure_withheld_in_sampled("node-a", CUSTODY_REQUIREMENT)
            .unwrap();
        assert_eq!(got, sampled);
    }

    /// Scenario helper: write node-a sampled columns (one per line) when
    /// `CC_2JB_PRINT_SAMPLED` is set to an output path.
    #[test]
    fn print_node_a_sampled_for_scenario() {
        let Ok(path) = std::env::var("CC_2JB_PRINT_SAMPLED") else {
            return; // no-op in ordinary `cargo test` runs
        };
        let sampled = sampled_columns_for_role("node-a", CUSTODY_REQUIREMENT).unwrap();
        let mut cols: Vec<u64> = sampled.into_iter().collect();
        cols.sort_unstable();
        let body = cols
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        if let Some(parent) = Path::new(&path).parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(&path, body).unwrap();
        eprintln!("wrote node-a sampled columns to {path}: {cols:?}");
    }

    #[test]
    fn nonzero_commitments_from_store() {
        let root = tempfile_dir("r4");
        let slot_dir = root.join("slot_000001");
        fs::create_dir_all(&slot_dir).unwrap();
        fs::write(slot_dir.join("block.ssz"), b"B").unwrap();
        fs::write(slot_dir.join("column_000.ssz"), b"C0").unwrap();
        let mut meta = fs::File::create(slot_dir.join("meta.json")).unwrap();
        write!(
            meta,
            r#"{{"slot":1,"block_root":"0x{}"}}"#,
            hex::encode([1u8; 32])
        )
        .unwrap();
        let store = FixtureStore::load(&root).unwrap();
        let manifest = root.join("manifest.json");
        fs::write(
            &manifest,
            r#"{"blobs_per_block_cycle":[1,2,3],"blobs_per_block":[1,2]}"#,
        )
        .unwrap();
        assert_nonzero_commitments(&manifest, &store).unwrap();
    }

    #[test]
    fn invalid_column_mutates_payload() {
        let m = FaultMode::parse("misbehave:invalid-column").unwrap();
        let honest = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let bad = m.relay(&honest).unwrap();
        assert_ne!(bad, honest, "invalid-column must change bytes");
        assert!(!bad.is_empty());
    }

    #[test]
    fn malformed_is_shorter_or_corrupt() {
        let m = FaultMode::parse("misbehave:malformed").unwrap();
        let honest = vec![0u8; 64];
        let bad = m.relay(&honest).unwrap();
        assert!(bad.len() < honest.len() || bad[..4] == [0xFF; 4]);
    }

    #[test]
    fn spam_emits_multiple_distinct_payloads() {
        let m = FaultMode::parse("misbehave:spam").unwrap();
        let honest = b"sidecar-ssz".to_vec();
        let payloads = m.column_publish_payloads(&honest);
        assert_eq!(payloads.len(), SPAM_GOSSIP_EXTRA_PUBLISHES as usize + 1);
        assert_eq!(payloads[0], honest);
        // Variants must differ so gossip message-ids do not collapse.
        let set: std::collections::HashSet<_> = payloads.iter().cloned().collect();
        assert_eq!(set.len(), payloads.len());
        assert_eq!(
            spam_columns_to_trip_rate_limit(),
            INBOUND_COLUMNS_CAPACITY + 1
        );
    }

    #[test]
    fn custody_and_stall_publish_honest_columns() {
        let honest = b"column-bytes".to_vec();
        for kind in ["custody-refuse", "stall-reqresp"] {
            let m = FaultMode::parse(&format!("misbehave:{kind}")).unwrap();
            assert_eq!(m.relay(&honest).as_deref(), Some(honest.as_slice()));
        }
    }

    #[test]
    fn key_derivation_is_deterministic() {
        let a = derive_node_secret("publisher");
        let b = derive_node_secret("publisher");
        let c = derive_node_secret("node-a");
        assert_eq!(a, b);
        assert_ne!(a, c);
        let kp1 = keypair_from_secret(&a).unwrap();
        let kp2 = keypair_from_secret(&b).unwrap();
        assert_eq!(
            PeerId::from_public_key(&kp1.public()),
            PeerId::from_public_key(&kp2.public())
        );
    }

    #[test]
    fn emit_bootnodes_stable_peer_ids() {
        let dir1 = tempfile_dir("boot1");
        let dir2 = tempfile_dir("boot2");
        let specs = default_bootnode_specs();
        emit_bootnodes(&dir1, &specs).unwrap();
        emit_bootnodes(&dir2, &specs).unwrap();
        let p1 = fs::read_to_string(dir1.join("peer_ids.txt")).unwrap();
        let p2 = fs::read_to_string(dir2.join("peer_ids.txt")).unwrap();
        assert_eq!(p1, p2);
        assert!(dir1.join("bootnodes.txt").exists());
        assert!(dir1.join("node_keys/publisher.key").exists());
    }

    #[test]
    fn fixture_store_by_root_and_range() {
        let root = tempfile_dir("fixture");
        let slot_dir = root.join("slot_000001");
        fs::create_dir_all(&slot_dir).unwrap();
        fs::write(slot_dir.join("block.ssz"), b"BLOCK1").unwrap();
        fs::write(slot_dir.join("column_000.ssz"), b"COL0").unwrap();
        fs::write(slot_dir.join("column_001.ssz"), b"COL1").unwrap();
        let block_root = [0x11u8; 32];
        let mut meta = fs::File::create(slot_dir.join("meta.json")).unwrap();
        write!(
            meta,
            r#"{{"slot":1,"block_root":"0x{}"}}"#,
            hex::encode(block_root)
        )
        .unwrap();

        let slot2 = root.join("slot_000002");
        fs::create_dir_all(&slot2).unwrap();
        fs::write(slot2.join("block.ssz"), b"BLOCK2").unwrap();
        fs::write(slot2.join("column_000.ssz"), b"COL0s2").unwrap();
        let mut meta2 = fs::File::create(slot2.join("meta.json")).unwrap();
        write!(
            meta2,
            r#"{{"slot":2,"block_root":"0x{}"}}"#,
            hex::encode([0x22u8; 32])
        )
        .unwrap();

        let store = FixtureStore::load(&root).unwrap();
        assert_eq!(store.slot_range(), (1, 2));
        assert_eq!(
            store.sidecar_by_root(&block_root, 0),
            Some(b"COL0".as_slice())
        );
        assert_eq!(
            store.sidecar_by_root(&block_root, 1),
            Some(b"COL1".as_slice())
        );
        assert!(store.sidecar_by_root(&block_root, 2).is_none());
        let range = store.sidecars_by_range(1, 2, 0);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], b"COL0");
        assert_eq!(range[1], b"COL0s2");
    }

    #[test]
    fn publisher_topics_cover_block_and_128_columns() {
        let names = publisher_topic_names();
        assert_eq!(names.len(), 1 + 128);
        assert!(matches!(names[0], TopicName::BeaconBlock));
        assert!(matches!(names[128], TopicName::DataColumnSidecar(127)));
    }

    fn tempfile_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "cc-p2p-fault-{}-{}-{}",
            tag,
            std::process::id(),
            now_unix()
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }
}
