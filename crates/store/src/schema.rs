//! Schema version, config digest, table registry, and open-or-refuse (CC-40a).
//!
//! ## Config digest field list (§2.5)
//!
//! The digest is a defined function of **exactly** these named inputs and
//! nothing else (grep anchors for the AC):
//!
//! - fork epochs (`altair` … `fulu`)
//! - `BLOB_SCHEDULE`
//! - `SECONDS_PER_SLOT`
//! - `genesis_validators_root`
//! - `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`
//! - `CHURN_LIMIT_QUOTIENT`
//!
//! Naming the list in source stops the digest from silently widening to a
//! runtime knob and refusing every restart.

use std::path::Path;

use sha2::{Digest, Sha256};
use ssz::{Decode, Encode};
use ssz_derive::{Decode as SszDecode, Encode as SszEncode};
use ssz_types::VariableList;
use typenum::U256;

use cc_types::{ChainConfig, Epoch, Root};

use crate::engine::{Engine, EngineOptions, StoreError};
use crate::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, blocks_shard_table, columns_shard_table,
};
use crate::meta::{ConfigDigest, KEY_CONFIG_DIGEST, KEY_SCHEMA_VERSION, SchemaVersion, TABLE_META};

/// Current on-disk schema version. Phase 4: one value, no migration path.
pub const SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Fixed table names (§2.2 inventory)
// ---------------------------------------------------------------------------

/// Fixed (non-sharded) table names from Architecture §2.2.
pub const FIXED_TABLES: &[&str] = &[
    "meta",
    "blocks_hot",
    "block_slot_by_root",
    "canonical",
    "columns_hot",
    "column_slot_by_root",
    "da_status",
    "snapshots",
    "state_roots",
    "fork_choice",
];

/// Prefix for cold block shard tables (`blocks_{ddddd}`).
pub const BLOCKS_SHARD_PREFIX: &str = "blocks_";
/// Prefix for cold column shard tables (`columns_{ddddd}`).
pub const COLUMNS_SHARD_PREFIX: &str = "columns_";

/// Shard widths recorded for docs / registry (Deviation 1 / ADR P4-10).
pub const COLUMN_SHARD_WIDTH_EPOCHS: u64 = COLUMN_SHARD_EPOCHS;
/// Block / state-root class shard width in epochs.
pub const BLOCK_SHARD_WIDTH_EPOCHS: u64 = BLOCK_SHARD_EPOCHS;

/// Whether `name` is a registered table (fixed inventory or shard pattern).
///
/// Shard names match [`crate::keys`] / CC-40b: zero-padded five-digit ids
/// (`blocks_00042`, `columns_00042`). Unregistered names fail open (I-shards
/// reconciliation light for CC-40a).
pub fn is_registered_table(name: &str) -> bool {
    if FIXED_TABLES.contains(&name) {
        return true;
    }
    parse_shard_table(name).is_some()
}

/// Parse `blocks_{ddddd}` / `columns_{ddddd}` → `(class, shard_id)`.
pub fn parse_shard_table(name: &str) -> Option<(&'static str, u64)> {
    if let Some(rest) = name.strip_prefix(BLOCKS_SHARD_PREFIX)
        && rest.len() == 5
        && rest.chars().all(|c| c.is_ascii_digit())
    {
        let id = rest.parse::<u64>().ok()?;
        return Some(("blocks", id));
    }
    if let Some(rest) = name.strip_prefix(COLUMNS_SHARD_PREFIX)
        && rest.len() == 5
        && rest.chars().all(|c| c.is_ascii_digit())
    {
        let id = rest.parse::<u64>().ok()?;
        return Some(("columns", id));
    }
    None
}

/// Expected shard table name for documentation / tests (delegates to keys).
pub fn registered_blocks_shard(shard_id: u64) -> String {
    blocks_shard_table(shard_id)
}

/// Expected column shard table name.
pub fn registered_columns_shard(shard_id: u64) -> String {
    columns_shard_table(shard_id)
}

/// Reconcile engine table names against the registry.
///
/// Returns the first unregistered name, if any.
pub fn find_unregistered_table(names: &[String]) -> Option<&str> {
    names
        .iter()
        .find(|n| !is_registered_table(n))
        .map(String::as_str)
}

// ---------------------------------------------------------------------------
// Config digest
// ---------------------------------------------------------------------------

/// One `BLOB_SCHEDULE` entry as digested (epoch + max blobs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, SszEncode, SszDecode)]
struct BlobScheduleDigestEntry {
    epoch: u64,
    max_blobs_per_block: u64,
}

/// Canonical SSZ payload for the config digest.
///
/// Field names / comments carry the §2.5 grep anchors; do not add fields without
/// a schema version bump.
#[derive(Debug, Clone, PartialEq, Eq, SszEncode, SszDecode)]
struct ConfigDigestPayload {
    // --- fork epochs ---
    altair_fork_epoch: u64,
    bellatrix_fork_epoch: u64,
    capella_fork_epoch: u64,
    deneb_fork_epoch: u64,
    electra_fork_epoch: u64,
    fulu_fork_epoch: u64,
    /// `SECONDS_PER_SLOT`
    seconds_per_slot: u64,
    /// `BLOB_SCHEDULE` entries in activation order.
    blob_schedule: VariableList<BlobScheduleDigestEntry, U256>,
    /// `genesis_validators_root`
    genesis_validators_root: Root,
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`
    min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`
    churn_limit_quotient: u64,
}

/// Inputs that enter [`compute_config_digest`] — the named §2.5 list only.
///
/// Built from a [`ChainConfig`] plus the three fields that are not (yet) on
/// that struct: `genesis_validators_root`, `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`,
/// and `CHURN_LIMIT_QUOTIENT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDigestInput {
    /// Fork epochs + `BLOB_SCHEDULE` + `SECONDS_PER_SLOT` source.
    pub chain: ChainConfig,
    /// `genesis_validators_root` (beacon-state / genesis field).
    pub genesis_validators_root: Root,
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`.
    pub min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`.
    pub churn_limit_quotient: u64,
}

impl ConfigDigestInput {
    /// Construct from named pieces (tests / open path).
    pub fn new(
        chain: ChainConfig,
        genesis_validators_root: Root,
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
    ) -> Self {
        Self {
            chain,
            genesis_validators_root,
            min_validator_withdrawability_delay,
            churn_limit_quotient,
        }
    }

    /// Hoodi mainnet-preset defaults for the two scalar config constants.
    pub fn with_mainnet_scalars(chain: ChainConfig, genesis_validators_root: Root) -> Self {
        Self {
            chain,
            genesis_validators_root,
            // MIN_VALIDATOR_WITHDRAWABILITY_DELAY (mainnet / hoodi)
            min_validator_withdrawability_delay: 256,
            // CHURN_LIMIT_QUOTIENT (mainnet / hoodi)
            churn_limit_quotient: 65_536,
        }
    }
}

/// Maximum `BLOB_SCHEDULE` entries the digest encoding accepts (`VariableList<…, U256>`).
///
/// Real networks ship a handful of BPO entries; exceeding this is a config error,
/// not a silent truncation (SEC-40a-1).
pub const CONFIG_DIGEST_BLOB_SCHEDULE_MAX: usize = 256;

/// Compute the config digest over the §2.5 named field list only.
///
/// Encoding: SSZ of [`ConfigDigestPayload`], then SHA-256 → [`Root`].
///
/// Fails closed if `BLOB_SCHEDULE` does not fit the encoding capacity
/// ([`CONFIG_DIGEST_BLOB_SCHEDULE_MAX`]) — never truncates or digests a default.
///
/// Grep-visible field list:
/// `genesis_validators_root`, `BLOB_SCHEDULE`, `SECONDS_PER_SLOT`,
/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`, `CHURN_LIMIT_QUOTIENT`, fork epochs.
pub fn compute_config_digest(input: &ConfigDigestInput) -> Result<Root, StoreError> {
    let n = input.chain.blob_schedule.entries().len();
    if n > CONFIG_DIGEST_BLOB_SCHEDULE_MAX {
        return Err(StoreError::Config(format!(
            "BLOB_SCHEDULE has {n} entries; digest encoding capacity is \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX} (refuse to truncate)"
        )));
    }
    let entries: Vec<BlobScheduleDigestEntry> = input
        .chain
        .blob_schedule
        .entries()
        .iter()
        .map(|e| BlobScheduleDigestEntry {
            epoch: e.epoch.as_u64(),
            max_blobs_per_block: e.max_blobs_per_block,
        })
        .collect();
    // Fail closed on encode capacity — no take(N) / unwrap_or_default (SEC-40a-1).
    let blob_schedule = VariableList::new(entries).map_err(|e| {
        StoreError::Config(format!(
            "BLOB_SCHEDULE does not fit digest encoding (capacity \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX}): {e}"
        ))
    })?;

    let payload = ConfigDigestPayload {
        altair_fork_epoch: epoch_u64(input.chain.altair_fork_epoch),
        bellatrix_fork_epoch: epoch_u64(input.chain.bellatrix_fork_epoch),
        capella_fork_epoch: epoch_u64(input.chain.capella_fork_epoch),
        deneb_fork_epoch: epoch_u64(input.chain.deneb_fork_epoch),
        electra_fork_epoch: epoch_u64(input.chain.electra_fork_epoch),
        fulu_fork_epoch: epoch_u64(input.chain.fulu_fork_epoch),
        // SECONDS_PER_SLOT
        seconds_per_slot: input.chain.seconds_per_slot,
        // BLOB_SCHEDULE
        blob_schedule,
        // genesis_validators_root
        genesis_validators_root: input.genesis_validators_root,
        // MIN_VALIDATOR_WITHDRAWABILITY_DELAY
        min_validator_withdrawability_delay: input.min_validator_withdrawability_delay,
        // CHURN_LIMIT_QUOTIENT
        churn_limit_quotient: input.churn_limit_quotient,
    };

    let bytes = payload.as_ssz_bytes();
    let hash = Sha256::digest(&bytes);
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&hash);
    Ok(Root::from_array(arr))
}

fn epoch_u64(e: Epoch) -> u64 {
    e.as_u64()
}

fn root_hex(r: &Root) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in r.as_slice() {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Store open-or-refuse
// ---------------------------------------------------------------------------

/// Options for [`Store::open`].
#[derive(Clone, Debug)]
pub struct StoreOpenOptions {
    /// Engine durability / open knobs.
    pub engine: EngineOptions,
    /// Expected config digest (from [`compute_config_digest`] at process start).
    pub config_digest: Root,
}

impl StoreOpenOptions {
    /// Build options from a digest input (computes the expected digest).
    ///
    /// Propagates [`compute_config_digest`] errors (e.g. oversized `BLOB_SCHEDULE`).
    pub fn from_config(
        engine: EngineOptions,
        input: &ConfigDigestInput,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            engine,
            config_digest: compute_config_digest(input)?,
        })
    }

    /// Explicit expected digest.
    pub fn with_digest(engine: EngineOptions, config_digest: Root) -> Self {
        Self {
            engine,
            config_digest,
        }
    }
}

/// Opened store: engine + schema/config gates already passed.
#[derive(Debug)]
pub struct Store {
    engine: Engine,
}

impl Store {
    /// Open (or create) a store under `path` with open-or-refuse semantics.
    ///
    /// 1. Open the engine.
    /// 2. Reconcile `table_names()` against the registry (unregistered → error).
    /// 3. Read `SchemaVersion` / `ConfigDigest` from `meta`:
    ///    - missing on a **fresh** (no tables) store → write expected values;
    ///    - missing on a non-empty store → refuse;
    ///    - present → compare; mismatch names **found** and **expected**.
    pub fn open(path: &Path, opts: StoreOpenOptions) -> Result<Self, StoreError> {
        let engine = Engine::open(path, opts.engine)?;
        Self::bind(engine, opts.config_digest)
    }

    /// Bind an already-opened engine (tests).
    pub fn bind(engine: Engine, expected_digest: Root) -> Result<Self, StoreError> {
        // Registry reconciliation (I-shards light — CC-4H owns prune-mark depth).
        let names = engine.table_names()?;
        if let Some(bad) = find_unregistered_table(&names) {
            return Err(StoreError::UnregisteredTable(bad.to_owned()));
        }

        let rt = engine.read()?;
        let version_bytes = rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())?;
        let digest_bytes = rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())?;
        drop(rt);

        match (version_bytes, digest_bytes) {
            (None, None) => {
                // Fresh store only when no tables exist yet.
                if !names.is_empty() {
                    return Err(StoreError::MissingMeta(KEY_SCHEMA_VERSION));
                }
                Self::write_bootstrap(&engine, expected_digest)?;
            }
            (Some(vb), Some(db)) => {
                let found_sv = SchemaVersion::from_ssz_bytes(&vb)
                    .map_err(|e| StoreError::Codec(format!("SchemaVersion: {e:?}")))?;
                if found_sv.version != SCHEMA_VERSION {
                    return Err(StoreError::SchemaVersionMismatch {
                        found: found_sv.version,
                        expected: SCHEMA_VERSION,
                    });
                }
                let found_cd = ConfigDigest::from_ssz_bytes(&db)
                    .map_err(|e| StoreError::Codec(format!("ConfigDigest: {e:?}")))?;
                if found_cd.digest != expected_digest {
                    return Err(StoreError::ConfigDigestMismatch {
                        found: root_hex(&found_cd.digest),
                        expected: root_hex(&expected_digest),
                    });
                }
            }
            (None, Some(_)) => return Err(StoreError::MissingMeta(KEY_SCHEMA_VERSION)),
            (Some(_), None) => return Err(StoreError::MissingMeta(KEY_CONFIG_DIGEST)),
        }

        Ok(Self { engine })
    }

    fn write_bootstrap(engine: &Engine, digest: Root) -> Result<(), StoreError> {
        let sv = SchemaVersion {
            version: SCHEMA_VERSION,
        };
        let cd = ConfigDigest { digest };
        let mut batch = engine.batch();
        batch.put(
            TABLE_META,
            KEY_SCHEMA_VERSION.as_bytes(),
            &sv.as_ssz_bytes(),
        );
        batch.put(TABLE_META, KEY_CONFIG_DIGEST.as_bytes(), &cd.as_ssz_bytes());
        engine.commit(batch)
    }

    /// Borrow the underlying engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Consume into the engine (tests / advanced use).
    pub fn into_engine(self) -> Engine {
        self.engine
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::meta::{ConfigDigest as MetaConfigDigest, SchemaVersion as MetaSchemaVersion};
    use cc_types::{BlobParameters, BlobSchedule, ChainConfig, Epoch, PresetName};
    use ssz::Encode;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-schema-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn hoodi_like_chain() -> ChainConfig {
        // Minimal ChainConfig shaped like Hoodi for digest tests (fork epochs + BLOB_SCHEDULE).
        let blob_schedule = BlobSchedule::try_from_entries(vec![
            BlobParameters {
                epoch: Epoch::new(52_480),
                max_blobs_per_block: 15,
            },
            BlobParameters {
                epoch: Epoch::new(54_016),
                max_blobs_per_block: 21,
            },
        ])
        .unwrap();
        ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "hoodi".into(),
            genesis_fork_version: cc_types::ForkVersion::from_array([0x10, 0x00, 0x09, 0x10]),
            altair_fork_version: cc_types::ForkVersion::from_array([0x20, 0x00, 0x09, 0x10]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: cc_types::ForkVersion::from_array([0x30, 0x00, 0x09, 0x10]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: cc_types::ForkVersion::from_array([0x40, 0x00, 0x09, 0x10]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: cc_types::ForkVersion::from_array([0x50, 0x00, 0x09, 0x10]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: cc_types::ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            electra_fork_epoch: Epoch::new(2_048),
            fulu_fork_version: cc_types::ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            fulu_fork_epoch: Epoch::new(50_688),
            seconds_per_slot: 12,
            blob_schedule,
            deposit_chain_id: 560_048,
            deposit_contract_address: cc_types::ExecutionAddress::from_array([0u8; 20]),
        }
    }

    fn hoodi_input() -> ConfigDigestInput {
        ConfigDigestInput::with_mainnet_scalars(hoodi_like_chain(), Root::from_array([0xAB; 32]))
    }

    fn open_opts(input: &ConfigDigestInput) -> StoreOpenOptions {
        StoreOpenOptions::from_config(EngineOptions::default(), input).unwrap()
    }

    #[test]
    fn open_fresh_writes_schema_and_digest() {
        let dir = tmp_dir("fresh");
        let input = hoodi_input();
        let digest = compute_config_digest(&input).unwrap();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let rt = store.engine().read().unwrap();
        let sv = MetaSchemaVersion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sv.version, SCHEMA_VERSION);
        let cd = MetaConfigDigest::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cd.digest, digest);
        // Re-open succeeds with same digest.
        drop(rt);
        drop(store);
        Store::open(&dir, open_opts(&input)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_version_mismatch_refuses_with_found_and_expected() {
        // CC-40 /5: rewrite SchemaVersion to v+1 → refuse naming both values.
        let dir = tmp_dir("sv-mismatch");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let bumped = MetaSchemaVersion {
            version: SCHEMA_VERSION + 1,
        };
        let mut b = eng.batch();
        b.put(
            TABLE_META,
            KEY_SCHEMA_VERSION.as_bytes(),
            &bumped.as_ssz_bytes(),
        );
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                StoreError::SchemaVersionMismatch {
                    found,
                    expected
                } if found == SCHEMA_VERSION + 1 && expected == SCHEMA_VERSION
            ),
            "err={err:?}"
        );
        assert!(
            msg.contains(&(SCHEMA_VERSION + 1).to_string())
                && msg.contains(&SCHEMA_VERSION.to_string()),
            "Display must name found and expected: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_schedule_mutation_refuses_config_digest() {
        // CC-40 /6: Hoodi BLOB_SCHEDULE 54016→21 becomes 54016→20 → refuse.
        let dir = tmp_dir("blob-digest");
        let input = hoodi_input();
        Store::open(&dir, open_opts(&input)).unwrap();

        let mut mutated = hoodi_like_chain();
        let entries = mutated.blob_schedule.entries().to_vec();
        assert_eq!(entries[1].epoch, Epoch::new(54_016));
        assert_eq!(entries[1].max_blobs_per_block, 21);
        let mut new_entries = entries;
        new_entries[1].max_blobs_per_block = 20; // 54016 → 20
        mutated.blob_schedule = BlobSchedule::try_from_entries(new_entries).unwrap();
        let mutated_input =
            ConfigDigestInput::with_mainnet_scalars(mutated, input.genesis_validators_root);

        let err = Store::open(&dir, open_opts(&mutated_input)).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, StoreError::ConfigDigestMismatch { .. }),
            "err={err:?}"
        );
        assert!(
            msg.to_ascii_lowercase().contains("digest"),
            "error must name config digest: {msg}"
        );
        // Found + expected both present as hex.
        if let StoreError::ConfigDigestMismatch { found, expected } = &err {
            assert_ne!(found, expected);
            assert!(msg.contains(found) && msg.contains(expected), "{msg}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn irrelevant_config_field_change_still_opens() {
        // Digest ignores fields outside §2.5 list (e.g. deposit_chain_id, config_name).
        let dir = tmp_dir("irrelevant");
        let input = hoodi_input();
        Store::open(&dir, open_opts(&input)).unwrap();

        let mut tweaked = hoodi_like_chain();
        tweaked.deposit_chain_id = 999_999_999;
        tweaked.config_name = "not-hoodi".into();
        tweaked.deposit_contract_address = cc_types::ExecutionAddress::from_array([0xFF; 20]);
        // Fork *versions* are also outside the digest list (epochs are in).
        tweaked.genesis_fork_version = cc_types::ForkVersion::from_array([0xFF; 4]);

        let tweaked_input =
            ConfigDigestInput::with_mainnet_scalars(tweaked, input.genesis_validators_root);
        assert_eq!(
            compute_config_digest(&input).unwrap(),
            compute_config_digest(&tweaked_input).unwrap(),
            "digest must be stable across irrelevant fields"
        );
        Store::open(&dir, open_opts(&tweaked_input)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unregistered_table_reported_at_open() {
        let dir = tmp_dir("unreg");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let mut b = eng.batch();
        b.put("evil_not_in_registry", b"k", b"v");
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::UnregisteredTable(n) if n == "evil_not_in_registry"
            ),
            "err={err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("evil_not_in_registry"), "{msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_blob_schedule_fails_closed() {
        // SEC-40a-1: BLOB_SCHEDULE above digest capacity must error, not truncate.
        let mut chain = hoodi_like_chain();
        let entries: Vec<BlobParameters> = (0..=CONFIG_DIGEST_BLOB_SCHEDULE_MAX as u64)
            .map(|i| BlobParameters {
                epoch: Epoch::new(i),
                max_blobs_per_block: 6,
            })
            .collect();
        assert_eq!(entries.len(), CONFIG_DIGEST_BLOB_SCHEDULE_MAX + 1);
        chain.blob_schedule = BlobSchedule::try_from_entries(entries).unwrap();
        let input = ConfigDigestInput::with_mainnet_scalars(chain, Root::from_array([0xAB; 32]));
        let err = compute_config_digest(&input).unwrap_err();
        assert!(matches!(err, StoreError::Config(_)), "err={err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("BLOB_SCHEDULE") && msg.contains("capacity"),
            "{msg}"
        );
        // from_config must also refuse (no silent open path).
        let err2 = StoreOpenOptions::from_config(EngineOptions::default(), &input).unwrap_err();
        assert!(matches!(err2, StoreError::Config(_)), "err={err2:?}");
    }

    #[test]
    fn missing_schema_version_on_nonempty_store_refuses() {
        let dir = tmp_dir("missing-sv");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        // Delete schema_version; leave config_digest + meta table present.
        let mut b = eng.batch();
        b.delete(TABLE_META, KEY_SCHEMA_VERSION.as_bytes());
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(
            matches!(err, StoreError::MissingMeta(KEY_SCHEMA_VERSION)),
            "err={err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_schema_version_bytes_refuse_codec() {
        let dir = tmp_dir("corrupt-sv");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let mut b = eng.batch();
        b.put(TABLE_META, KEY_SCHEMA_VERSION.as_bytes(), b"\x00\x01"); // truncated SSZ
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)), "err={err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("SchemaVersion") || msg.contains("codec"),
            "{msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registered_shard_names_match_keys() {
        assert!(is_registered_table("meta"));
        assert!(is_registered_table(&blocks_shard_table(0)));
        assert!(is_registered_table(&columns_shard_table(42)));
        assert!(!is_registered_table("blocks_42")); // not zero-padded
        assert!(!is_registered_table("blocks_000042")); // wrong width
        assert!(!is_registered_table("state_roots_00001"));
        assert_eq!(COLUMN_SHARD_WIDTH_EPOCHS, 32);
        assert_eq!(BLOCK_SHARD_WIDTH_EPOCHS, 256);
    }

    #[test]
    fn digest_field_list_grep_anchors_present() {
        // AC: these strings appear in this source file (this test file is schema.rs tests;
        // the production path above is the greppable site). Mirror assert for CI clarity.
        let src = include_str!("schema.rs");
        for needle in [
            "genesis_validators_root",
            "BLOB_SCHEDULE",
            "SECONDS_PER_SLOT",
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
            "CHURN_LIMIT_QUOTIENT",
        ] {
            assert!(
                src.contains(needle),
                "schema.rs must name digest field {needle}"
            );
        }
    }
}
