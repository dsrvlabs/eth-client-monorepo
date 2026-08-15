# Spike notes

Findings from the S0a / [ARCH] B.2 spikes. One heading per question.

## Q-9

**Fails** — an uncovered handler does **not** log-and-continue. The harness check is `assert_handler_coverage` at [`crates/spec-tests/src/coverage.rs:18-38`](../../crates/spec-tests/src/coverage.rs): it diffs declared handler names against `Vectors::handlers` and, on any mismatch, returns `Err(Error::Coverage { missing, extra })` (`coverage.rs:37`; `Display` lists both sides at [`crates/spec-tests/src/error.rs:122-130`](../../crates/spec-tests/src/error.rs)). There is no `eprintln!` / `warn!` path in the helper. Callers that use it panic the test ([`crates/types/tests/ssz_static.rs:183-185`](../../crates/types/tests/ssz_static.rs), [`ssz_generic.rs:934-935`](../../crates/types/tests/ssz_generic.rs), [`custody.rs:205-206`](../../crates/types/tests/custody.rs)); STF runners do the same with `assert_eq!` on their on-disk handler sets (e.g. [`crates/state-transition/tests/operations.rs:1128-1138`](../../crates/state-transition/tests/operations.rs)). Scope is **handler-set equality inside a claimed runner**, not a walk of every on-disk case: unowned Fulu runners (`light_client`, `transition`, `fork`, `sync`, `merkle_proof` — [`spec-vectors-layout.md:58`](../../spec-vectors-layout.md)) and `OUT_OF_SCOPE_HANDLERS` (e.g. [`crates/types/tests/custody.rs:41-53`](../../crates/types/tests/custody.rs)) never enter this check and stay silent. Skiplist-empty is already a hard fail ([`crates/spec-tests/src/skiplist.rs:306-314`](../../crates/spec-tests/src/skiplist.rs)).

**W1 measurable as the harness stands: yes.** Extra/missing handlers in a claimed runner fail the suite, and a non-empty `docs/spec-vectors-skiplist.md` fails CI, so "green + skiplist empty" means what M2a/W1's clause says. It does **not** mean [ARCH] §5.6 total-coverage (every on-disk vector claimed or skiplisted); that remains `S4c-04`. No S3a report→fail follow-on — the answer is **fail**, not report.

## Q-10

**Answer: the serve window is never published. There is no production publisher.**
`[ARCH]` §3 / `[PRD]` J-3's "never published" claim holds; the empty-window seed
and the missing publisher are now both verified.

The *intended* publish path exists and is explicitly unwired:

- `StorageServer::publish_window` at `services/storage/src/serve.rs:273-294` is
  marked `#[allow(dead_code)]` with the comment *"exercised by unit tests;
  production callers land with CC-47/48 wiring"*. Every call site is inside
  `#[cfg(test)]` (`serve.rs:1510`, `:2171`, `:2191`, `:2251`, `:2398`, `:2440`,
  `:2485`).
- `StorageServer::derive_and_publish_window` at
  `services/storage/src/serve.rs:296-335` is likewise `#[allow(dead_code)]`
  (*"wired by backfill / hole paths as they land"*) and has **zero** call sites
  in the tree — definition only. That is the only non-test caller of
  `cc_store::write_derived_serve_window`.

What *does* exist, and is not a publisher of a real window:

- The gRPC RPC `WatchServeWindow` (`proto/eth/storage/v1/storage.proto:32`) is
  implemented at `services/storage/src/serve.rs:882-893`. It streams the current
  `watch` channel value; it does not derive or update the window.
- Boot seeds that channel from `empty_window()` (`serve.rs:1084-1095`,
  `earliest_available_slot: u64::MAX` at `:1086-1087`) via `StorageServer::stub`
  (`:225`) or from `load_window_or_default` (`:1097-1123`) via
  `StorageServer::new` (`:249`). The loader returns the empty seed unless
  `KEY_SERVE_WINDOW` is already on disk.
- Persist helpers `put_serve_window` / `write_derived_serve_window` /
  `write_serve_window_from_meta` (`crates/store/src/window.rs:565`, `:579`,
  `:608`) have no production callers. Writes of `KEY_SERVE_WINDOW` in
  `services/storage/src/durable_set.rs` and `bin/cc-store/src/lib.rs` are
  test-fixture / operator-tool only.
- Ingest, prune, write-behind, replay, and migrate never write
  `KEY_SERVE_WINDOW` or call `publish_window`. `PutBackfillBatch`
  (`serve.rs:767-879`) commits blocks, columns, and progress only.
- p2p is a consumer only: `spawn_watch_serve_window_with_metrics`
  (`services/p2p/src/storage_client.rs:389`) applies stream messages at
  `:508-518` onto the ADR-P2-14 `AtomicU64`. Spawned from
  `services/p2p/src/service.rs:394`.

**Implication for `S3a-B-08`:** P0-17c remains a wiring task — attach production
callers to `publish_window` / `derive_and_publish_window` from backfill, prune,
and CGC-raise. There is no hidden live publisher that would invert that framing.

Closes `[PRD]` J-3.

## Q-3

**YES** — `superstruct` composes with milhouse's `List<T, N, U>` third type
parameter. The planned S4 order **4a → 4b → 4c** stands. **`S4-ALT` is not
selected.**

**Settling declaration** (the combination `[Q5]` §6 said nobody had read):

`sigp/lighthouse` `stable` @ `b263df596671`,
`consensus/types/src/state/beacon_state.rs:528-530`

```rust
#[superstruct(only(Altair, Bellatrix, Capella, Deneb, Electra, Fulu, Gloas))]
#[cfg_attr(feature = "arbitrary", arbitrary(default))]
pub inactivity_scores: List<u64, E::ValidatorRegistryLimit>,
```

`List` here is `milhouse::List` (`:11`), on a `#[superstruct]` `BeaconState`
(`:278`, `:433`). `U` is the defaulted third parameter
(`List<T, N, U: UpdateMap<T> = MaxMap<VecMap<T>>>`); writing `List<T, N>` *is*
`List<T, N, U>`.

**The third parameter is also spelled explicitly** on a field of the same
superstruct (`:73-74`, `:479`):

```rust
pub type Validators<E> =
    List<Validator, <E as EthSpec>::ValidatorRegistryLimit, BTreeMap<usize, Validator>>;
// …
pub validators: Validators<E>,
```

That is `U = BTreeMap<usize, Validator>` — a non-default `UpdateMap` — hosted
as an ordinary superstruct field. Superstruct-generated partial getters then
return the milhouse list; e.g. `self.inactivity_scores_mut()?.push(0)` at
`:2040-2041`.

Further `#[superstruct(only(…))]` + milhouse `List` fields on the same struct:
`:495-497` (`previous_epoch_attestations`, Base-only), `:504-507`
(`previous_epoch_participation`), `:613-614` (`pending_deposits`), `:634-635`
(`builders`, Gloas-only). The Gloas −1/+9 field list is already expressed this
way.

**Path note.** `[PLAN]` / `S0a-B-09` name
`consensus/types/src/beacon_state.rs`. On current `stable` the file lives at
`consensus/types/src/state/beacon_state.rs` (moved into the `state/` module).
Same type, new path.

**S4 implication.** ⟡ D-8 ch.2 holds: milhouse (4a) still reduces the
four-place schema to two *before* the Gloas superstruct edit (4b). Do not
invert. No change to `s4-fork-seam.md` dependency tables.

## Q-2

**Issue** `S0a-B-10` · **redb** 4.1.0 (`Cargo.lock` pin; `docs/storage-engine.md` V-4) ·
**host** macOS (Darwin, rustc 1.97.1) · **date** 2026-08-15

**Question.** A second opener of the slashing-protection DB must **error**, not block — that is what
catches "operator started two validator clients on one key". If redb blocks, wrap the open in
`flock(LOCK_EX | LOCK_NB)`. This is the backend tiebreak for `S0-B-14` / ADR-R-05 (`[PRD]` P1-F/1
says SQLite; `[Q4]` says redb).

**Method.** Two-process experiment, recorded as
`crates/store/tests/q2_redb_exclusive_open.rs` (re-run: `cargo test -p cc-store --test q2_redb_exclusive_open -- --nocapture`):

1. Process 1 calls `Engine::open` on a temp data dir (redb `Database::create` of `store.redb`) and
   commits one row under `Durability::Immediate` so the file is a real store.
2. Process 2 is a re-exec of the same test binary against that dir; the parent kills it if it has
   not exited within 3 s.
3. Production `Engine::open` locking policy was **not** changed.

**Result.** Process 2 returned `StoreError::DatabaseLocked` in **12.5 ms** (spawn + open) and
exited 2. It did not block. `Engine` maps `redb::DatabaseError::DatabaseAlreadyOpen` →
`StoreError::DatabaseLocked` (`crates/store/src/engine/redb.rs:19-24`).

**Mechanism (redb 4.1.0).** `FileBackend::new_internal` takes a non-blocking exclusive lock:

- `file.try_lock()` (writers) / `file.try_lock_shared()` (read-only)
- `TryLockError::WouldBlock` → `DatabaseError::DatabaseAlreadyOpen`

Source: `redb-4.1.0/src/tree_store/page_store/file_backend/optimized.rs:27-39`. On macOS / Linux /
Windows this is the platform file-lock equivalent of `flock(LOCK_EX | LOCK_NB)`. An extra
`flock` wrapper is not required. Platforms without file locks log and proceed unlocked; they are
not this workspace's targets.

Same-process writer vs read-only was already covered by `open_read_only_refuses_live_writer_lock`
in `engine/redb.rs`; this spike is the **cross-process** writer-vs-writer case Q-2 asked for.

**Durability (already present; not this spike).** `[Q4]`'s record→fsync→sign surface is
`Durability::Immediate` and `Paranoid` (`Immediate` + `set_two_phase_commit(true)`) at
`crates/store/src/engine/redb.rs:476-493`. Q-2 was only the exclusive-open question.

**Verdict for `S0-B-14`'s backend clause: `redb` (fail-fast confirmed).**

## Q-7

**Answer: no.** The only `preset.rs` constant that is config-scoped in the spec and preset-resolved in code is the already-known `MAX_BLOBS_PER_BLOCK_BASE` (`MAX_BLOBS_PER_BLOCK_ELECTRA`). Nothing else in `Preset` is the P0-02 / P2-A/8 mistake. **`S0-A-07` gains no new fields; its 2–3 pd / 5 pts estimate is unchanged.**

**Issue** `S0-A-05` · **first task of P0-02** (R-9) · **blocks** `S0-A-07` · **date** 2026-08-15

**Question.** `crates/types/src/preset.rs` was never audited. P0-02's class is the *mismatch* between a value's spec scope (`config`) and its resolution key (compile-time preset). Are there **other** values that landed in the preset by the same mistake?

**Method.** Every `const` on the `Preset` trait ([`crates/types/src/preset.rs:31-196`](../../crates/types/src/preset.rs)) was checked against upstream `ethereum/consensus-specs` `master` (fetched 2026-08-15):

- [`configs/mainnet.yaml`](https://github.com/ethereum/consensus-specs/blob/master/configs/mainnet.yaml) and [`configs/minimal.yaml`](https://github.com/ethereum/consensus-specs/blob/master/configs/minimal.yaml)
- [`presets/mainnet/{phase0,altair,bellatrix,capella,deneb,electra}.yaml`](https://github.com/ethereum/consensus-specs/tree/master/presets/mainnet) (minimal altair checked for the same keys)
- [`specs/altair/validator.md` Constants](https://github.com/ethereum/consensus-specs/blob/master/specs/altair/validator.md#constants) for the one const that is in neither YAML tree
- [`ChainConfig` / `RawChainConfig`](../../crates/types/src/config.rs) (`config.rs:138-177`, `:288-309`) for the parse column

Typenum associated types (`type SlotsPerEpoch`, …) are compile-time mirrors of the scalar consts already in the table; they are not independent spec keys and are omitted as rows.

The five `pub mod network` functions ([Q5] §1 / P0-02) live in `constants.rs`, **not** in `preset.rs`. They stay on `S0-A-07`'s existing field list and are not re-litigated here.

### Every `Preset` const

| `preset.rs` const | Spec home | Spec key | `ChainConfig` parses? |
|---|---|---|:--:|
| `NAME` | n/a — compile-time identifier; config `PRESET_BASE` *selects* the preset | — | ✅ as `preset_base` (`PRESET_BASE`), not this const |
| `SLOTS_PER_EPOCH` | `presets/*/phase0.yaml` | `SLOTS_PER_EPOCH` | ❌ |
| `MAX_COMMITTEES_PER_SLOT` | `presets/*/phase0.yaml` | `MAX_COMMITTEES_PER_SLOT` | ❌ |
| `TARGET_COMMITTEE_SIZE` | `presets/*/phase0.yaml` | `TARGET_COMMITTEE_SIZE` | ❌ |
| `MAX_VALIDATORS_PER_COMMITTEE` | `presets/*/phase0.yaml` | `MAX_VALIDATORS_PER_COMMITTEE` | ❌ |
| `SHUFFLE_ROUND_COUNT` | `presets/*/phase0.yaml` | `SHUFFLE_ROUND_COUNT` | ❌ |
| `MIN_SEED_LOOKAHEAD` | `presets/*/phase0.yaml` | `MIN_SEED_LOOKAHEAD` | ❌ |
| `MAX_SEED_LOOKAHEAD` | `presets/*/phase0.yaml` | `MAX_SEED_LOOKAHEAD` | ❌ |
| `EPOCHS_PER_ETH1_VOTING_PERIOD` | `presets/*/phase0.yaml` | `EPOCHS_PER_ETH1_VOTING_PERIOD` | ❌ |
| `SLOTS_PER_HISTORICAL_ROOT` | `presets/*/phase0.yaml` | `SLOTS_PER_HISTORICAL_ROOT` | ❌ |
| `EPOCHS_PER_HISTORICAL_VECTOR` | `presets/*/phase0.yaml` | `EPOCHS_PER_HISTORICAL_VECTOR` | ❌ |
| `EPOCHS_PER_SLASHINGS_VECTOR` | `presets/*/phase0.yaml` | `EPOCHS_PER_SLASHINGS_VECTOR` | ❌ |
| `HISTORICAL_ROOTS_LIMIT` | `presets/*/phase0.yaml` | `HISTORICAL_ROOTS_LIMIT` | ❌ |
| `VALIDATOR_REGISTRY_LIMIT` | `presets/*/phase0.yaml` | `VALIDATOR_REGISTRY_LIMIT` | ❌ |
| `SYNC_COMMITTEE_SIZE` | `presets/*/altair.yaml` | `SYNC_COMMITTEE_SIZE` | ❌ |
| `EPOCHS_PER_SYNC_COMMITTEE_PERIOD` | `presets/*/altair.yaml` | `EPOCHS_PER_SYNC_COMMITTEE_PERIOD` | ❌ |
| `MAX_BLOB_COMMITMENTS_PER_BLOCK` | `presets/*/deneb.yaml` | `MAX_BLOB_COMMITMENTS_PER_BLOCK` | ❌ |
| `MAX_PROPOSER_SLASHINGS` | `presets/*/phase0.yaml` | `MAX_PROPOSER_SLASHINGS` | ❌ |
| `MAX_ATTESTER_SLASHINGS` | `presets/*/phase0.yaml` | `MAX_ATTESTER_SLASHINGS` | ❌ |
| `MAX_ATTESTER_SLASHINGS_ELECTRA` | `presets/*/electra.yaml` | `MAX_ATTESTER_SLASHINGS_ELECTRA` | ❌ |
| `MAX_ATTESTATIONS` | `presets/*/phase0.yaml` | `MAX_ATTESTATIONS` | ❌ |
| `MAX_ATTESTATIONS_ELECTRA` | `presets/*/electra.yaml` | `MAX_ATTESTATIONS_ELECTRA` | ❌ |
| `MAX_DEPOSITS` | `presets/*/phase0.yaml` | `MAX_DEPOSITS` | ❌ |
| `MAX_VOLUNTARY_EXITS` | `presets/*/phase0.yaml` | `MAX_VOLUNTARY_EXITS` | ❌ |
| `MAX_BLS_TO_EXECUTION_CHANGES` | `presets/*/capella.yaml` | `MAX_BLS_TO_EXECUTION_CHANGES` | ❌ |
| `MAX_WITHDRAWALS_PER_PAYLOAD` | `presets/*/capella.yaml` | `MAX_WITHDRAWALS_PER_PAYLOAD` | ❌ |
| `MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP` | `presets/*/capella.yaml` | `MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP` | ❌ |
| `MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP` | `presets/*/electra.yaml` | `MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP` | ❌ |
| `MAX_BYTES_PER_TRANSACTION` | `presets/*/bellatrix.yaml` | `MAX_BYTES_PER_TRANSACTION` | ❌ |
| `MAX_TRANSACTIONS_PER_PAYLOAD` | `presets/*/bellatrix.yaml` | `MAX_TRANSACTIONS_PER_PAYLOAD` | ❌ |
| `BYTES_PER_LOGS_BLOOM` | `presets/*/bellatrix.yaml` | `BYTES_PER_LOGS_BLOOM` | ❌ |
| `MAX_EXTRA_DATA_BYTES` | `presets/*/bellatrix.yaml` | `MAX_EXTRA_DATA_BYTES` | ❌ |
| `PENDING_DEPOSITS_LIMIT` | `presets/*/electra.yaml` | `PENDING_DEPOSITS_LIMIT` | ❌ |
| `PENDING_PARTIAL_WITHDRAWALS_LIMIT` | `presets/*/electra.yaml` | `PENDING_PARTIAL_WITHDRAWALS_LIMIT` | ❌ |
| `PENDING_CONSOLIDATIONS_LIMIT` | `presets/*/electra.yaml` | `PENDING_CONSOLIDATIONS_LIMIT` | ❌ |
| `MAX_DEPOSIT_REQUESTS_PER_PAYLOAD` | `presets/*/electra.yaml` | `MAX_DEPOSIT_REQUESTS_PER_PAYLOAD` | ❌ |
| `MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD` | `presets/*/electra.yaml` | `MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD` | ❌ |
| `MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD` | `presets/*/electra.yaml` | `MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD` | ❌ |
| **`MAX_BLOBS_PER_BLOCK_BASE`** | **`configs/*.yaml`** | **`MAX_BLOBS_PER_BLOCK_ELECTRA`** | **❌** (YAML key discarded; fallback reads `P::MAX_BLOBS_PER_BLOCK_BASE` at `config.rs:101`) |
| `SYNC_COMMITTEE_SUBNET_COUNT` | neither YAML tree — Altair validator **Constants** (`Uint64(2**2)` = 4), not Configuration | `SYNC_COMMITTEE_SUBNET_COUNT` | ❌ |
| `MAX_VALIDATORS_PER_SLOT` | derived: `MAX_VALIDATORS_PER_COMMITTEE × MAX_COMMITTEES_PER_SLOT` | — | ❌ |
| `PROPOSER_LOOKAHEAD_LEN` | derived: `(MIN_SEED_LOOKAHEAD + 1) × SLOTS_PER_EPOCH` | — | ❌ |
| `ETH1_DATA_VOTES_LENGTH` | derived: `EPOCHS_PER_ETH1_VOTING_PERIOD × SLOTS_PER_EPOCH` | — | ❌ |
| `SYNC_SUBCOMMITTEE_SIZE` | derived: `SYNC_COMMITTEE_SIZE / SYNC_COMMITTEE_SUBNET_COUNT` | — | ❌ |

### Config-scoped and preset-resolved

One row.

| Spec key | `preset.rs` name | Already on `S0-A-07`? | Add to `S0-A-07`? |
|---|---|:--:|---|
| `MAX_BLOBS_PER_BLOCK_ELECTRA` | `MAX_BLOBS_PER_BLOCK_BASE` | yes — `max_blobs_per_block_electra` (P2-A/8 / [PRD] J-11) | **no — already listed** |

`preset.rs:246` already admits the mis-home: *"Electra base (`MAX_BLOBS_PER_BLOCK_ELECTRA` in network config.yaml)"*. Mainnet and minimal configs both ship `9`; Hoodi's fixture does too (`crates/types/tests/fixtures/hoodi-config.yaml:47`). Same latency shape as the four unparsed `pub mod network` churn/exit keys: Hoodi agrees with mainnet, a customising devnet diverges silently because `RawChainConfig` has no field and serde drops the key.

`MAX_BLOBS_PER_BLOCK` (Deneb, `6`) is also config-scoped and also unparsed. It is **not** a `Preset` const — only the Electra base is. Out of this audit's scope; the Fulu `BLOB_SCHEDULE` path already covers post-Electra bounds.

`SYNC_COMMITTEE_SUBNET_COUNT` is a protocol constant (always 4, identical on mainnet and minimal). Parking it on `Preset` is organisation, not the config-vs-preset class. Do not add it to `ChainConfig`.

### Estimate (in writing)

- **`S0-A-05`:** 1–2 pd / 3 pts. No extra rows to chase. Unchanged.
- **`S0-A-07` field list:** still the four missing `pub mod network` fields + `max_blobs_per_block_electra`. **Q-7 adds zero.**
- **`S0-A-07` estimate:** remains **2–3 pd / 5 pts**. Unchanged.

Do not implement `S0-A-07` from this spike. No `ChainConfig` fields were added.
