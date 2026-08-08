# Storage schema (CC-40a)

On-disk layout for `cc-store`: table inventory, key codecs, shard widths, and
the meta singletons that gate `Store::open`. Design rationale lives in
Architecture §2.2–2.5; this file is the inventory, not a re-derivation.

Cross-reference: engine body, runtime table names / interning, and the flat vs
sharded falsifier decision are **CC-40b** (`docs/storage-engine.md`, R-14).

## Schema version and config digest

| Record | Meta key | Role |
|---|---|---|
| `SchemaVersion { version: u32 }` | `schema_version` | Integer written at create; mismatch → **refuse open** (CC-40 /5). Phase 4 has version **`1`** and no migration path. |
| `ConfigDigest { digest: Root }` | `config_digest` | SHA-256 of an SSZ payload over the named config field list; mismatch → **refuse open** (CC-40 /6). |

**Digest field list** (named in `crates/store/src/schema.rs` — do not widen without a schema bump):

- fork epochs: Altair, Bellatrix, Capella, Deneb, Electra, Fulu
- `BLOB_SCHEDULE` (epoch + `max_blobs_per_block` per entry)
- `SECONDS_PER_SLOT`
- `genesis_validators_root`
- `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`
- `CHURN_LIMIT_QUOTIENT`

Fields outside that list (`deposit_chain_id`, `config_name`, fork *versions*, …)
do not enter the digest.

`compute_config_digest` **fails closed** if `BLOB_SCHEDULE` exceeds the SSZ list
capacity (256 entries): it returns `StoreError::Config` and never truncates or
digests a default empty schedule (SEC-40a-1).

Open-or-refuse errors name **found** and **expected** in `Display`
(`SchemaVersionMismatch`, `ConfigDigestMismatch`).

## Shard widths (Deviation 1 / ADR P4-10)

| Class | Shard width | Prune cadence | Notes |
|---|---|---|---|
| columns | **32 epochs** | 32 epochs | One prune tick retires one whole shard → `drop_table` |
| blocks | **256 epochs** | 256 epochs | Not 32: a 32-epoch block shard cannot drop until the 256-epoch prune tick |
| state roots | (single table) | with blocks | `state_roots` is one table in §2.2; pruned with blocks |

Shard table names match CC-40b key helpers: zero-padded five-digit ids
(`blocks_00000`, `columns_00042`). Runtime names are interned to `'static` for
redb `open_table` (R-14); the registry in `schema.rs` is what `Engine::table_names()`
is reconciled against at open.

## Table inventory (§2.2)

| Table | Key layout | Value | Notes |
|---|---|---|---|
| `meta` | ASCII name, ≤ 16 B | SSZ singleton (§2.5) | See meta keys below |
| `blocks_hot` | `slot:u64be ‖ root:32` (40 B) | `SignedBeaconBlock` SSZ (opaque) | Every block above the split, canonical or not |
| `blocks_{shard}` | `slot:u64be` (8 B) | block SSZ (opaque) | Cold; root drops out of the key (§2.3) |
| `block_slot_by_root` | `root:32` | `slot:u64be ‖ region:u8` | ByRoot index |
| `canonical` | `slot:u64be` | `root:32` | Canonical chain as storage knows it |
| `columns_hot` | `slot:u64be ‖ root:32 ‖ idx:u16be` (42 B) | sidecar SSZ (opaque) | Hot region keeps root |
| `columns_{shard}` | `slot:u64be ‖ idx:u16be` (10 B) | sidecar SSZ (opaque) | Cold key **is** the spec `(slot, column_index)` order |
| `column_slot_by_root` | `root:32 ‖ idx:u16be` | `slot:u64be` | ByRoot column index |
| `da_status` | `root:32` | `u8 status ‖ slot:u64be` | Available \| Deferred; read on replay |
| `snapshots` | `slot:u64be` | `BeaconState` SSZ (opaque) | Snapshot ring |
| `state_roots` | `slot:u64be` | `root:32` | Historical state_root per canonical slot |
| `fork_choice` | `"current"` | SSZ `ForkChoiceScalars` | ~300 B scalars (ADR P4-06) |

Codecs: `crates/store/src/keys.rs`. Values stay opaque in `cc-store` (no
consensus-type decode in this crate).

### Meta keys (≤ 16 ASCII bytes)

| Key | Record |
|---|---|
| `schema_version` | `SchemaVersion` |
| `config_digest` | `ConfigDigest` |
| `split` | `Split` |
| `anchor_info` | `AnchorInfo` (includes `node_id: Bytes32`) |
| `column_info` | `ColumnInfo` |
| `serve_window` | `ServeWindow` (`earliest_available_slot` + `cgc` in one container) |
| `write_cursor` | `WriteCursor` |
| `fc_scalars` | `ForkChoiceScalars` |
| `prune_marks` | `PruneMarks` |
| `backfill_prog` | `BackfillProgress` |

Record types: `crates/store/src/meta.rs`. Population is owned by later issues
(CC-41, CC-44b, CC-45b, CC-46a, CC-47a, CC-48, …).

## Registry reconciliation

At `Store::open`, every name from `Engine::table_names()` must be either a
fixed inventory name or a `blocks_{ddddd}` / `columns_{ddddd}` shard. Any other
name yields `StoreError::UnregisteredTable` (fatal at open). Full
`I-shards` / prune-mark depth is **CC-4H**.

## Flat layout note (CC-40b)

If shards are collapsed to key prefixes inside one table per class
(`shard:u16be ‖ …`), the registry shrinks to the fixed list plus one table
per class; key codecs already provide `encode_flat_column_key`. Phase 4
default remains **sharded tables** after the CC-40b falsifier.
