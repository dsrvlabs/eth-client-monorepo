//! ENR manager — Architecture §6.2, CC-21a / CC-21c.
//!
//! All ENR field mutations go through [`EnrManager::apply`], which takes a
//! **batch** and produces **exactly one** sequence bump. Field encoders for
//! `eth2`, `attnets`, `syncnets`, `cgc`, and `nfd` live here beside the matching
//! decoders (same wire shape both directions).

use std::fmt;
use std::net::Ipv4Addr;
use std::str::FromStr;

use alloy_primitives::U256;
use cc_types::{CUSTODY_REQUIREMENT, ForkDigest, get_custody_groups};
use discv5::enr::{CombinedKey, EnrPublicKey, Error as EnrError, NodeId};
use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig};

use crate::fork_digest::{EnrForkId, ForkContext};

// ── ENR key names ───────────────────────────────────────────────────────────

/// ENR key for SSZ `ENRForkID`.
pub const ENR_KEY_ETH2: &str = "eth2";
/// ENR key for next-fork digest (`Bytes4`).
pub const ENR_KEY_NFD: &str = "nfd";
/// ENR key for attestation subnet bitfield (`BitVector[64]`).
pub const ENR_KEY_ATTNETS: &str = "attnets";
/// ENR key for sync-committee subnet bitfield (`BitVector[4]`).
pub const ENR_KEY_SYNCNETS: &str = "syncnets";
/// ENR key for custody group count (`Uint64`, BE, minimal-length).
pub const ENR_KEY_CGC: &str = "cgc";

/// Attestation subnet count (SSZ `BitVector[64]` → 8 bytes).
pub const ATTNETS_BIT_LEN: usize = 64;
/// Sync subnet count (SSZ `BitVector[4]` → 1 byte).
pub const SYNCNETS_BIT_LEN: usize = 4;

// ── field encoders / decoders ───────────────────────────────────────────────

/// Encode `cgc` as `Uint64` **big-endian with no leading zero bytes**.
///
/// Spec delta 10: `0` is the **empty** byte string. A fixed-width 8-byte
/// encoding is a *different* ENR value and must not be used.
#[must_use]
pub fn encode_cgc(cgc: u64) -> Vec<u8> {
    if cgc == 0 {
        return Vec::new();
    }
    let be = cgc.to_be_bytes();
    let start = be.iter().position(|&b| b != 0).unwrap_or(7);
    be[start..].to_vec()
}

/// Decode a `cgc` payload produced by [`encode_cgc`] (or a peer's equivalent).
///
/// Rejects payloads longer than 8 bytes and leading-zero (non-minimal) encodings
/// other than the empty string for zero.
#[must_use]
pub fn decode_cgc(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return Some(0);
    }
    if payload.len() > 8 {
        return None;
    }
    // Minimal-length: leading zero is illegal for non-empty payloads.
    if payload[0] == 0 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf[8 - payload.len()..].copy_from_slice(payload);
    Some(u64::from_be_bytes(buf))
}

/// Encode SSZ `ENRForkID` (16 fixed bytes) for the `eth2` key.
#[must_use]
pub fn encode_eth2(fork_id: EnrForkId) -> Vec<u8> {
    fork_id.to_ssz_bytes().to_vec()
}

/// Decode SSZ `ENRForkID` from an `eth2` payload.
#[must_use]
pub fn decode_eth2(payload: &[u8]) -> Option<EnrForkId> {
    if payload.len() != 16 {
        return None;
    }
    let mut dig = [0u8; 4];
    dig.copy_from_slice(&payload[0..4]);
    let mut ver = [0u8; 4];
    ver.copy_from_slice(&payload[4..8]);
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&payload[8..16]);
    Some(EnrForkId {
        fork_digest: ForkDigest::from_array(dig),
        next_fork_version: cc_types::ForkVersion::from_array(ver),
        next_fork_epoch: cc_types::Epoch::new(u64::from_le_bytes(epoch)),
    })
}

/// Encode `nfd` as `Bytes4` (next-fork digest, or zero-filled).
#[must_use]
pub fn encode_nfd(digest: ForkDigest) -> Vec<u8> {
    digest.as_slice().to_vec()
}

/// Decode `nfd` (`Bytes4`).
#[must_use]
pub fn decode_nfd(payload: &[u8]) -> Option<ForkDigest> {
    if payload.len() != 4 {
        return None;
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(payload);
    Some(ForkDigest::from_array(arr))
}

/// Encode attestation subnet bitfield as SSZ `BitVector[64]` (8 bytes, LE bits).
#[must_use]
pub fn encode_attnets(bits: u64) -> Vec<u8> {
    bits.to_le_bytes().to_vec()
}

/// Decode `attnets` SSZ `BitVector[64]`.
#[must_use]
pub fn decode_attnets(payload: &[u8]) -> Option<u64> {
    if payload.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(payload);
    Some(u64::from_le_bytes(arr))
}

/// Encode sync-committee subnet bitfield as SSZ `BitVector[4]` (1 byte).
#[must_use]
pub fn encode_syncnets(bits: u8) -> Vec<u8> {
    // Only the low 4 bits are meaningful; store the full byte (SSZ packs 4 bits
    // into one byte with high bits zero).
    vec![bits & 0x0f]
}

/// Decode `syncnets` SSZ `BitVector[4]`.
#[must_use]
pub fn decode_syncnets(payload: &[u8]) -> Option<u8> {
    if payload.len() != 1 {
        return None;
    }
    Some(payload[0] & 0x0f)
}

/// Strip a short RLP string header and return the payload bytes.
///
/// Used for fields inserted via `enr_insert(key, &payload.as_slice())`.
fn rlp_string_payload(raw: &[u8]) -> Option<&[u8]> {
    if raw.is_empty() {
        return None;
    }
    let first = raw[0];
    // Single-byte value 0x00..=0x7f is its own RLP encoding.
    if first < 0x80 {
        return Some(&raw[0..1]);
    }
    // Short string: 0x80 + len, len < 56.
    if first <= 0xb7 {
        let len = (first - 0x80) as usize;
        if raw.len() == 1 + len {
            return Some(&raw[1..]);
        }
        // Tolerate trailing-only exact payload after header.
        if raw.len() > len {
            return Some(&raw[1..1 + len]);
        }
        return None;
    }
    None
}

/// Read a field's raw **payload** (RLP string contents) from an ENR.
#[must_use]
pub fn enr_field_payload(enr: &Enr, key: &str) -> Option<Vec<u8>> {
    let raw = enr.get_raw_rlp(key)?;
    rlp_string_payload(raw).map(<[u8]>::to_vec)
}

/// Read a peer's (or local) `cgc`, decoding via [`decode_cgc`].
///
/// Missing key → `None` (callers substitute [`CUSTODY_REQUIREMENT`]).
#[must_use]
pub fn read_cgc(enr: &Enr) -> Option<u64> {
    let payload = enr_field_payload(enr, ENR_KEY_CGC)?;
    decode_cgc(&payload)
}

/// Read `eth2` as [`EnrForkId`].
#[must_use]
pub fn read_eth2(enr: &Enr) -> Option<EnrForkId> {
    let payload = enr_field_payload(enr, ENR_KEY_ETH2)?;
    decode_eth2(&payload)
}

/// Read `nfd`.
#[must_use]
pub fn read_nfd(enr: &Enr) -> Option<ForkDigest> {
    let payload = enr_field_payload(enr, ENR_KEY_NFD)?;
    decode_nfd(&payload)
}

/// Read `attnets` bitmask.
#[must_use]
pub fn read_attnets(enr: &Enr) -> Option<u64> {
    let payload = enr_field_payload(enr, ENR_KEY_ATTNETS)?;
    decode_attnets(&payload)
}

/// Read `syncnets` bitmask (low 4 bits).
#[must_use]
pub fn read_syncnets(enr: &Enr) -> Option<u8> {
    let payload = enr_field_payload(enr, ENR_KEY_SYNCNETS)?;
    decode_syncnets(&payload)
}

/// Whether attestation subnet `s` is set in the ENR (`s < 64`).
#[must_use]
pub fn attnets_has(enr: &Enr, s: u8) -> bool {
    if (s as usize) >= ATTNETS_BIT_LEN {
        return false;
    }
    read_attnets(enr).is_some_and(|bits| bits & (1u64 << s) != 0)
}

/// Whether sync subnet `s` is set (`s < 4`).
#[must_use]
pub fn syncnets_has(enr: &Enr, s: u8) -> bool {
    if (s as usize) >= SYNCNETS_BIT_LEN {
        return false;
    }
    read_syncnets(enr).is_some_and(|bits| bits & (1u8 << s) != 0)
}

/// `node_id` as big-endian `U256` for [`get_custody_groups`].
#[must_use]
pub fn node_id_as_u256(node_id: NodeId) -> U256 {
    U256::from_be_bytes(node_id.raw())
}

/// Custody groups advertised by `enr` (missing `cgc` → [`CUSTODY_REQUIREMENT`]).
#[must_use]
pub fn enr_custody_groups(enr: &Enr) -> std::collections::BTreeSet<u64> {
    let cgc = read_cgc(enr).unwrap_or(CUSTODY_REQUIREMENT);
    let cgc = cgc.min(cc_types::NUMBER_OF_CUSTODY_GROUPS);
    get_custody_groups(node_id_as_u256(enr.node_id()), cgc)
}

// ── typed field-change constructors ────────────────────────────────────────

/// Build the `eth2` + `nfd` batch for an epoch tick from [`ForkContext`].
///
/// Always returns **both** fields so a single [`EnrManager::apply`] call
/// coalesces to one `seq` bump.
#[must_use]
pub fn fork_field_changes(ctx: &ForkContext) -> [EnrFieldChange; 2] {
    [
        EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(ctx.enr_fork_id())),
        EnrFieldChange::new(ENR_KEY_NFD, encode_nfd(ctx.nfd())),
    ]
}

/// Phase-2 default ENR fields: empty attnets/syncnets + fixed `cgc`.
#[must_use]
pub fn phase2_default_field_changes(cgc: u64) -> [EnrFieldChange; 3] {
    [
        EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(0)),
        EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(0)),
        EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(cgc)),
    ]
}

// ── seq strategy / manager ──────────────────────────────────────────────────

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
/// byte string by `enr_insert` / `Enr::insert`.
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
    /// Bootnode ENR string failed to parse.
    #[error("bootnode ENR parse failed: {0}")]
    BootnodeParse(String),
}

/// Single writer for local ENR field mutations (§6.2).
///
/// Holds a real [`Discv5`] handle and a retained node key for the
/// rebuild-and-replace path.
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

    /// Construct a manager for production discovery from a node key + listen UDP.
    ///
    /// Does **not** call `Discv5::start` — the discovery task owns that.
    pub fn from_key_and_listen(
        enr_key: CombinedKey,
        listen_ip: Ipv4Addr,
        listen_udp: u16,
        tcp_port: u16,
        strategy: EnrSeqStrategy,
    ) -> Result<Self, EnrApplyError> {
        let enr_key_copy = duplicate_secp256k1(&enr_key)?;
        let mut builder = Enr::builder();
        builder.ip4(listen_ip).udp4(listen_udp).tcp4(tcp_port);
        let enr = builder.build(&enr_key).map_err(EnrApplyError::Enr)?;
        let listen_config = ListenConfig::Ipv4 {
            ip: listen_ip,
            port: listen_udp,
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

    /// Mutable borrow of the discv5 handle (start / event stream).
    pub fn discv5_mut(&mut self) -> &mut Discv5 {
        &mut self.discv5
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

    /// Epoch-tick helper: write `eth2` + `nfd` from [`ForkContext`] in **one** bump.
    ///
    /// **Idempotent:** if both payloads already match the local ENR, this is a
    /// no-op (no `seq` bump). Crossing a BPO that changes both fields therefore
    /// advances `seq` by exactly **one** (CC-2A/3 coalesce), not once per epoch.
    pub fn apply_fork_context(&self, ctx: &ForkContext) -> Result<(), EnrApplyError> {
        let changes = fork_field_changes(ctx);
        let enr = self.local_enr();
        let eth2_same = read_eth2(&enr).is_some_and(|id| id == ctx.enr_fork_id());
        let nfd_same = read_nfd(&enr).is_some_and(|d| d == ctx.nfd());
        if eth2_same && nfd_same {
            return Ok(());
        }
        self.apply(changes)
    }

    /// Install Phase-2 default empty attnets/syncnets + fixed `cgc`.
    pub fn apply_phase2_defaults(&self) -> Result<(), EnrApplyError> {
        self.apply(phase2_default_field_changes(CUSTODY_REQUIREMENT))
    }

    /// Seed bootnode ENRs into the routing table (pre-start or post-start).
    pub fn add_bootnodes(&self, bootnodes: &[Enr]) -> usize {
        let mut added = 0;
        for enr in bootnodes {
            if self.discv5.add_enr(enr.clone()).is_ok() {
                added += 1;
            }
        }
        added
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

/// Parse a bootnode string: bare base64 ENR or `enr:` URI.
pub fn parse_bootnode(s: &str) -> Result<Enr, EnrApplyError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(EnrApplyError::BootnodeParse("empty bootnode string".into()));
    }
    let body = trimmed
        .strip_prefix("enr:")
        .or_else(|| trimmed.strip_prefix("ENR:"))
        .unwrap_or(trimmed);
    Enr::from_str(body).map_err(EnrApplyError::BootnodeParse)
}

/// Parse a list of bootnode strings; skip blank / comment lines when present.
pub fn parse_bootnodes(lines: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Vec<Enr>, EnrApplyError> {
    let mut out = Vec::new();
    for line in lines {
        let s = line.as_ref().trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        // YAML-list form: "- enr:…"
        let s = s.strip_prefix("- ").unwrap_or(s).trim();
        out.push(parse_bootnode(s)?);
    }
    Ok(out)
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

/// Compressed secp256k1 public key bytes from an ENR (for PeerId bridging).
#[must_use]
pub fn enr_secp256k1_pubkey_bytes(enr: &Enr) -> Option<Vec<u8>> {
    let pk = enr.public_key();
    match pk {
        discv5::enr::CombinedPublicKey::Secp256k1(_) => Some(pk.encode()),
        discv5::enr::CombinedPublicKey::Ed25519(_) => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn cgc_encode_minimal_be() {
        assert_eq!(encode_cgc(0), Vec::<u8>::new());
        assert_eq!(encode_cgc(4), vec![4]);
        assert_eq!(encode_cgc(128), vec![128]);
        assert_eq!(encode_cgc(256), vec![1, 0]);
        // Fixed-width 8-byte must differ from minimal for 4.
        assert_ne!(encode_cgc(4), 4u64.to_be_bytes().to_vec());
    }

    #[test]
    fn cgc_roundtrip_values() {
        for v in [0u64, 4, 128, 255, 256, u64::MAX] {
            let enc = encode_cgc(v);
            assert_eq!(decode_cgc(&enc), Some(v), "value {v}");
        }
        // Leading zero rejected.
        assert_eq!(decode_cgc(&[0, 4]), None);
        // Too long rejected.
        assert_eq!(decode_cgc(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), None);
    }

    #[test]
    fn cgc_real_enr_roundtrip_4_and_128() {
        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        for v in [4u64, 128, 0] {
            manager
                .apply([EnrFieldChange::new(ENR_KEY_CGC, encode_cgc(v))])
                .unwrap();
            let enr = manager.local_enr();
            assert!(enr.verify());
            let decoded = read_cgc(&enr).expect("cgc present");
            assert_eq!(decoded, v, "cgc round-trip for {v}");
            // Fixed-width encoding must not equal the stored payload for non-full values.
            if v != 0 && v < (1u64 << 56) {
                let fixed = v.to_be_bytes().to_vec();
                let payload = enr_field_payload(&enr, ENR_KEY_CGC).unwrap();
                assert_ne!(
                    payload, fixed,
                    "minimal encoding must differ from fixed 8-byte for {v}"
                );
            }
        }
    }

    #[test]
    fn eth2_nfd_coalesce_one_seq_bump() {
        use crate::fork_digest::EnrForkId;
        use cc_types::{Epoch, ForkVersion};

        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let seq_before = manager.local_enr().seq();
        let eth2 = EnrForkId {
            fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
            next_fork_version: ForkVersion::from_array([5, 6, 7, 8]),
            next_fork_epoch: Epoch::new(99),
        };
        let nfd = ForkDigest::from_array([9, 10, 11, 12]);
        manager
            .apply([
                EnrFieldChange::new(ENR_KEY_ETH2, encode_eth2(eth2)),
                EnrFieldChange::new(ENR_KEY_NFD, encode_nfd(nfd)),
            ])
            .unwrap();
        let enr = manager.local_enr();
        assert_eq!(enr.seq(), seq_before + 1, "eth2+nfd must coalesce to one bump");
        assert!(enr.verify());
        assert_eq!(read_eth2(&enr).unwrap(), eth2);
        assert_eq!(read_nfd(&enr).unwrap(), nfd);
    }

    #[test]
    fn attnets_syncnets_bits() {
        let manager = EnrManager::new_ephemeral(EnrSeqStrategy::EnrInsert).unwrap();
        let att = 1u64 << 3 | 1u64 << 10;
        manager
            .apply([
                EnrFieldChange::new(ENR_KEY_ATTNETS, encode_attnets(att)),
                EnrFieldChange::new(ENR_KEY_SYNCNETS, encode_syncnets(0b0101)),
            ])
            .unwrap();
        let enr = manager.local_enr();
        assert!(attnets_has(&enr, 3));
        assert!(attnets_has(&enr, 10));
        assert!(!attnets_has(&enr, 0));
        assert!(syncnets_has(&enr, 0));
        assert!(!syncnets_has(&enr, 1));
        assert!(syncnets_has(&enr, 2));
    }
}
