# Q5 — the fork-evolution seam (Stage 4 / "Phase 4.5")

## Recommendation

1. **`superstruct` for the 6 containers Gloas reshapes.** Lighthouse expresses
   `BeaconBlock` across 8 forks in a **29-line** attribute block; Grandine's
   hand-written equivalent is **~1,650 lines, 60–70 % match-arm boilerplate**.
2. **Dispatch the STF by monotone capability predicates, not per-fork modules.**
   Lighthouse's `per_block_processing` uses `fork_name.gloas_enabled()` /
   `.capella_enabled()` inside one generic function — no trait objects, no exhaustive
   enum match, no copied handlers. This is a change to how the study framed Stage 4.
3. **Move fork authority into `cc-types` in Stage 0, not Stage 4.** It is **S**
   (2–4 days), it fixes a live consensus bug, and the precedent is exact:
   Lighthouse's `ChainSpec::fork_name_at_epoch` lives in `consensus/types`, and its
   `ForkContext` is *built from* it — one runtime authority feeding both the STF and the
   p2p digest walk.
4. **Sequence milhouse (Q2) before the Gloas schema work.** Gloas is −1/+9 fields on
   `BeaconState`, whose schema is maintained in four hand-synchronised places; milhouse
   deletes two of them. Free reordering.

The decode chokepoints the study assumes already exist: two functions, five call sites.

---

## 1. The identity bug class — verified against the spec YAML

`crates/state-transition/src/helpers/constants.rs:128-172` defines `pub mod network`,
whose every function resolves a value by `match P::NAME` — the **compile-time preset**.

Verified against `ethereum/consensus-specs` `configs/mainnet.yaml`
(<https://github.com/ethereum/consensus-specs/blob/dev/configs/mainnet.yaml>, fetched
2026-08-15) — the **runtime config** file, loaded per network:

| Key | `configs/mainnet.yaml` | In `pub mod network`? | In `ChainConfig`? |
|---|---|:--:|:--:|
| `GENESIS_FORK_VERSION` | `0x00000000` | ✅ | ✅ (`config.rs:144`) |
| `SHARD_COMMITTEE_PERIOD` | `256` | ✅ | ❌ |
| `CHURN_LIMIT_QUOTIENT` | `65536` | ✅ | ❌ |
| `MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA` | `128000000000` | ✅ | ❌ |
| `MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT` | `256000000000` | ✅ | ❌ |
| `MAX_BLOBS_PER_BLOCK_ELECTRA` | `9` | via `P::MAX_BLOBS_PER_BLOCK_BASE` (`config.rs:101`) | ❌ |

**All five are config-scoped. None belongs in a preset.** The module is correctly
*named*; the key is wrong. The class in one sentence:

> **A module named `network` that resolves config-scoped values from a preset-scoped
> key.**

`deposit.rs:42-45` is the instance that bites today —
`compute_domain(DOMAIN_DEPOSIT, Some(network::genesis_fork_version::<P>()), None)` — and
`crates/types/tests/fixtures/hoodi-config.yaml` proves it: `PRESET_BASE: mainnet`,
`GENESIS_FORK_VERSION: 0x10000910`. Preset `mainnet` → `0x00000000`. Every Hoodi deposit
PoP verified under the wrong domain. The other four are latent only because Hoodi
happens to reuse mainnet's values (verified against the same fixture) — a devnet that
customises `CHURN_LIMIT_QUOTIENT` diverges silently.

**The second half of the class:** `RawChainConfig` (`config.rs:286-309`) declares 19
fields with no `#[serde(deny_unknown_fields)]`, so those same keys are read from disk
and **discarded** — along with `GLOAS_FORK_VERSION`/`GLOAS_FORK_EPOCH` and `HEZE_*`,
which are already in the upstream config. `config.rs:101` is one member of a set of six,
not a one-off. Both halves must close together: the reader takes from the wrong source,
*and* the loader does not take from the right one.

**Forward hazard (not live):** today's upstream `configs/mainnet.yaml` has no
`SECONDS_PER_SLOT` — it has `SLOT_DURATION_MS: 12000` plus BPS-based timing params —
while `RawChainConfig.seconds_per_slot` is required with no default. The repo loads its
own fixtures, which carry both keys, so nothing fails today; it fires the first time an
operator supplies an upstream config file.

### 1.1 The precedent: `ChainSpec` is the type to copy

Lighthouse puts fork authority in its **types** crate, exactly where the study wants it:

- `consensus/types/src/core/chain_spec.rs` —
  `pub fn fork_name_at_epoch(&self, epoch: Epoch) -> ForkName`, implemented as a
  **descending data table**, not an if/else chain:
  `let forks = [(self.gloas_fork_epoch, ForkName::Gloas), (self.fulu_fork_epoch, ForkName::Fulu), …]`.
  Adding a fork is one row.
- `consensus/types/src/fork/fork_context.rs` — `ForkContext::new(…, spec: &ChainSpec)`
  is **built from** `spec.fork_name_at_epoch(epoch)` + `spec.compute_fork_digest(gvr, epoch)`.
  The p2p digest context is a *derivative* of the authority, not a parallel copy.
- Consumers thread `spec: &ChainSpec` and call it. The closest parallel to this repo's
  bug: `consensus/types/src/exit/voluntary_exit.rs::get_domain` resolves a **signature
  domain** via `spec.fork_name_at_epoch(self.epoch)` (the EIP-7044 case). Same operation
  as `deposit.rs:42`; right source instead of wrong one.

This repo has the data and not the authority. `ChainConfig` (`config.rs:138-177`)
already carries every fork version and epoch — so the move is mostly *deleting the
second source*. But `cc-types` exposes no `fork_version_at_epoch`, so the walk is
reimplemented **five** ways outside it:

- `services/p2p/src/gossip/validate/column.rs:707`
- `services/p2p/src/gossip/validate/sync.rs:661`
- `services/p2p/src/gossip/validate/operations.rs:882` — these three byte-identical
- `services/p2p/src/backfill/below.rs:153`
- `ForkContext` (`services/p2p/src/fork_digest.rs:211-226`), keeping its own
  `next: Option<(Epoch, ForkVersion, ForkDigest)>` and per-epoch cache

One already mishandles `FAR_FUTURE_EPOCH` entries (`fork_digest.rs:350`).
`services/engine/src/version.rs:94` is a legitimate sixth — the EL schedule is
**timestamp**-keyed and must stay separate.

### 1.2 How the move makes the class unrepresentable

1. Add the missing config fields to `ChainConfig`/`RawChainConfig`, each
   `#[serde(default = …)]` with the mainnet value so existing fixtures keep loading.
2. **Delete `pub mod network` entirely.** Every caller must then have `&ChainConfig` in
   scope to compile.
3. Capture-and-WARN unknown config keys (`ignored: BTreeMap<String, Value>` logged at
   load) rather than `deny_unknown_fields`, which would break on every upstream config
   that adds a Heze key.
4. Add the accessors, as a data table per `ChainSpec`:
   ```rust
   impl ChainConfig {
       pub fn fork_name_at_epoch(&self, epoch: Epoch) -> ForkName;
       pub fn fork_version_at_epoch(&self, epoch: Epoch) -> ForkVersion;
       pub fn fork_epoch(&self, fork: ForkName) -> Epoch;
       pub fn next_fork_after(&self, epoch: Epoch) -> Option<(ForkName, Epoch)>;
   }
   ```
   Delete the four duplicate walks; rebuild `ForkContext` on top of these.

---

## 2. What EIP-7732 / Gloas reshapes

From `ethereum/consensus-specs` `specs/gloas/beacon-chain.md` §Containers and
`specs/gloas/fork.md` (commit `caeca85`, fetched 2026-08-15).

**Modified — exactly six** (the study's "~6" is exact):
`Attestation`, `IndexedAttestation`, `BeaconBlockBody`, `BeaconState`,
`ExecutionPayload`, `ExecutionRequests`.

**New — thirteen:** `Builder`, `BuilderPendingPayment`, `BuilderPendingWithdrawal`,
`BuilderDepositRequest`, `BuilderExitRequest`, `PayloadAttestationData`,
`PayloadAttestation`, `PayloadAttestationMessage`, `IndexedPayloadAttestation`,
`ExecutionPayloadBid`, `SignedExecutionPayloadBid`, `ExecutionPayloadEnvelope`,
`SignedExecutionPayloadEnvelope`.

**`BeaconState` specifically** (`fork.md`): **removed** `latest_execution_payload_header`
(*"Removed in Gloas:EIP7732"*); **added** `builders`, `next_withdrawal_builder_index`,
`execution_payload_availability` (bitfield init to 1s), `builder_pending_payments`,
`builder_pending_withdrawals`, `latest_execution_payload_bid`,
`payload_expected_withdrawals`, `ptc_window`, `latest_block_hash`. `upgrade_to_gloas`
also runs `onboard_builders_from_pending_deposits()`.

**One the study's list misses:** Lighthouse declares
`#[superstruct(variants(Fulu, Gloas), …)]` on `DataColumnSidecar`
(`consensus/types/src/data/data_column_sidecar.rs`), consistent with
`specs/gloas/partial-columns/` existing. The PeerDAS sidecar — this repo's central Fulu
object — is fork-shaped at Gloas. Add it to the Stage-4 inventory.

---

## 3. How the two Rust clients express it

### 3.1 Containers

**Lighthouse — `superstruct` proc-macro.** One annotated struct generates the enum,
per-variant structs, and getters. `consensus/types/src/block/beacon_block.rs`, `stable`,
verified 2026-08-15 — a **29-line** attribute block:

```rust
#[superstruct(
    variants(Base, Altair, Bellatrix, Capella, Deneb, Electra, Fulu, Gloas),
    variant_attributes(derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, TreeHash, Educe), …),
    ref_attributes(derive(Debug, PartialEq, TreeHash), tree_hash(enum_behaviour = "transparent")),
    map_ref_into(BeaconBlockBodyRef, BeaconBlock),
    map_ref_mut_into(BeaconBlockBodyRefMut)
)]
```

then per-field: `#[superstruct(getter(copy))] pub slot: Slot` (shared, free getter) and
`#[superstruct(only(Base), partial_getter(rename = "body_base"))]` (variant-only, getter
returns `Result`).

Three things that matter here:

- `tree_hash(enum_behaviour = "transparent")` makes the enum hash as the **variant**,
  not as a union — the property this repo's opaque-SSZ contract depends on.
- Gloas is already in `variants(…)` on `stable`. Absorbing EPBS was a variant-list edit
  plus per-variant bodies. That is the "Gloas becomes a new module, not a 44-file
  retrofit" property, demonstrated.
- Superstruct removes struct boilerplate but not dispatch: Lighthouse still hand-writes
  `map_fork_name!` (`consensus/types/src/fork/fork_macros.rs`), whose own doc says it
  exists because *"polymorphism in the return type … is not usually possible in Rust
  without trait objects."* That macro is the honest cost line.

**Grandine — hand-written.** Independent struct per fork in
`types/src/{phase0,…,gloas}/`, then `types/src/combined.rs`
(<https://github.com/grandinetech/grandine/blob/master/types/src/combined.rs>) —
**~1,650 lines**, `BeaconState` with 8 variants `Phase0(Hc<Phase0BeaconState<P>>)`…`Gloas`,
`#[derive(From, VariantCount)]`, `#[serde(untagged)]`, `#[duplicate_item]` stamping
repeated impls, and `const_assert_eq!` guards that fail compilation if a phase is added
without updating matchers. Fork-agnostic access is exhaustive `match` plus
`post_altair() -> Option<&dyn Trait>` escape hatches. **~60–70 % of the file is
match-arm boilerplate.**

### 3.2 STF dispatch — neither trait objects nor exhaustive match

This is the part the study framed loosely, and the answer is better than "per-fork
dispatch."

**Lighthouse: monotone capability predicates on `ForkName`, inside one generic
function.** `consensus/state_processing/src/per_block_processing.rs`:

```rust
pub fn per_block_processing<E: EthSpec, Payload: AbstractExecPayload<E>>(
    state: &mut BeaconState<E>,
    signed_block: &SignedBeaconBlock<E, Payload>,
    block_signature_strategy: BlockSignatureStrategy,
    verify_block_root: VerifyBlockRoot,
    ctxt: &mut ConsensusContext<E>,
    spec: &ChainSpec,
) -> Result<(), BlockProcessingError>
```

Dispatch inside is `if fork_name.gloas_enabled() { … }`,
`if state.fork_name_unchecked().capella_enabled() { … }`, and superstruct's optional
accessors: `if let Ok(sync_aggregate) = block.body().sync_aggregate() { … }`.
`spec: &ChainSpec` is threaded to every fork-specific handler.

**Why monotone predicates beat per-fork modules:** `X_enabled()` means "fork X or
later", so a handler is written **once** and gated, rather than copied into 8 modules.
Per-fork modules give you N copies of `process_attestation` to keep in sync — which is
the same synchronisation hazard as the four-place `BeaconState` schema.

**Grandine: per-fork modules + a shared `unphased`.**
`transition_functions/src/{phase0,altair,bellatrix,…}/{block_processing.rs,epoch_processing.rs}`,
enum match at the entry, generic over `P: Preset` beneath, with `unphased` holding what
is common. This is the study's "per-fork STF dispatch" literally — and it is the more
expensive of the two.

**Neither client uses trait objects for the STF.** The dispatch is static in both.

**Cost at ~7–8 forks: I have no measurement, and will not estimate one.** What is
certain is *where* the cost lands: superstruct is proc-macro expansion (compile time);
per-fork modules are source volume; and **both multiply by the preset generic** — the
forks × presets monomorphisation is the binary-size driver, not the fork enum. If a
number is needed, it is a half-day `cargo build --timings` spike on a scratch branch;
record it as unmeasured rather than taking an estimate.

### 3.3 The comparison

| | Lighthouse / superstruct | Grandine / hand-written |
|---|---|---|
| Add a fork variant | edit `variants(…)` + variant-only fields | new module + N match arms per enum per impl |
| STF handler per fork | one handler, predicate-gated | one copy per fork module |
| Compile-time cost | high proc-macro expansion | low macro cost, large source |
| Ergonomics | partial getters return `Result` everywhere | exhaustive match, explicit and greppable |
| Safety net | the macro | `const_assert_eq!` + non-exhaustive-match errors |
| Seam line cost | ~29 lines of attribute per container | ~1,650 lines in one file |

Both are production-proven; the difference is where the boilerplate goes.

---

## 4. Applying it here

### 4.1 The chokepoints already exist

Two fork-aware decode functions:

- `crates/types/src/block.rs:135` — `SignedBeaconBlock::from_ssz_bytes_with(fork_name: ForkName, bytes: &[u8]) -> Result<Self, DecodeError>`
- `crates/types/src/state/mod.rs:223` — `BeaconState::from_ssz_bytes_with(…)`

Five production call sites, all passing hardcoded `ForkName::Fulu`:
`checkpoint_sync.rs:1001`, `:1023`, `import.rs:1018`, `replay.rs:595`, `:644`. Both
already reject non-Fulu with tests (`block.rs:191`, `mod.rs:304`). Replacing the literal
with `config.fork_name_at_epoch(epoch)` at those five sites **is** the seam.

**One bypass to close first:** `services/chain/src/restore.rs:437` uses the raw
`BeaconState::from_ssz_bytes(...)`. Until it is routed through `from_ssz_bytes_with`,
the chokepoint is not one. (Same site Q3 needs for the pubkey top-up — one edit, two
fixes.)

`ForkName` (`crates/types/src/fork.rs:17-59`) is already an ordered enum with
`all() -> [Self; 7]` and `as_str()`; it becomes `[Self; 8]` with Gloas and is the
natural superstruct variant list. Add the monotone predicates
(`fulu_enabled()`, `gloas_enabled()`, …) to it at the same time — they are what §3.2's
dispatch needs and they cost ~10 lines.

### 4.2 Sequencing

| Order | Work | Why here |
|---|---|---|
| **Stage 0** | Deposit domain → `&ChainConfig` | already scheduled; live consensus bug on Hoodi |
| **Stage 0** | Add missing config fields; **delete `pub mod network`** | kills the class at its root; **S** |
| **Stage 0** | `seconds_per_slot` optional + `SLOT_DURATION_MS` fallback; WARN on unknown keys | forward hazard on any upstream config file |
| **Stage 0** | `ChainConfig` fork accessors as a data table; delete the 4 duplicate walks; rebuild `ForkContext` on them | one authority; also fixes the `FAR_FUTURE_EPOCH` bug |
| **Stage 2** | Route `restore.rs:437` through `from_ssz_bytes_with` | closes the bypass; shares an edit with Q3 |
| **Stage 4a** | milhouse swap on `BeaconState` (Q2 Stage A) | four schema places → two, **before** Gloas edits them |
| **Stage 4b** | `ForkName::Gloas` + predicates; superstruct the 6 modified containers; `upgrade_to_gloas` | now a variant-list edit on a two-place schema |
| **Stage 4c** | 13 new containers + `DataColumnSidecar(Fulu, Gloas)` | additive, no retrofit |

**Two reorderings against the study's plan, both free:**

- **milhouse before Gloas schema work.** Gloas is −1/+9 on `BeaconState`, whose schema
  is maintained in four hand-synchronised places (Q2 §6) — one of which, the
  `StateField` discriminant order vs. the struct field order, produces a **wrong state
  root** rather than a compile error when it drifts. milhouse deletes two of the four.
- **Config authority from Stage 4 to Stage 0.** It costs days, depends on nothing, and
  fixes a live consensus bug that is already half-scheduled in Stage 0 for the deposit
  fix alone.

---

## 5. Effort

| Piece | Size | Rationale |
|---|---|---|
| Delete `pub mod network`; 5 constants → `ChainConfig` | **S** | 5 functions, ~20 call sites (`grep -rn "constants::network::"`); `#[serde(default = …)]` so no fixture breaks. **2–4 days**, highest value-per-hour item here. |
| `ChainConfig` fork accessors; delete 4 walks; rebuild `ForkContext` | **S–M** | ~1 week incl. the `FAR_FUTURE_EPOCH` fix. |
| `SLOT_DURATION_MS` / unknown-key WARN | **S** | ~1 day; needs a decision on accepting both keys vs migrating. |
| `ForkName::Gloas` + predicates + superstruct on 6 containers | **M–L** | Annotation is small; the ripple is every `match fork_name` and the `crates/spec-tests` harness. **4–8 weeks** — and it is the first time this codebase has had two decodable forks, which is itself the risk. |
| Per-fork STF gating + `upgrade_to_gloas` | **M** | Lower than the study implies *if* predicates are used instead of per-fork modules. `upgrade_to_gloas` alone is 9 new state fields + `onboard_builders_from_pending_deposits`. |
| 13 new Gloas containers + PTC/builder machinery | **XL** | This is Gloas *implementation*, not the seam. Out of Stage-4 scope; listed so the two are not conflated. |

**Stage 4 as scoped by the study (seam only): L.** Its config half is **S** and belongs
in Stage 0.

---

## 6. What I could not determine

- **superstruct's compile-time cost on this workspace.** No measurement exists that I
  could find, and neither the README nor the guide publishes one. Spike it before
  committing — build time is a daily tax and it is the main argument *for* Grandine's
  approach. Same for binary size at 7–8 forks × 2 presets.
- **Whether superstruct composes with milhouse's `List<T, N, U>` third type parameter**
  on `BeaconState` fields. Lighthouse does both, so it evidently works, but I did not
  read a Lighthouse `BeaconState` field declaration combining `#[superstruct(only(…))]`
  with a milhouse `List`. **Verify before committing to the 4a → 4b order** — if they
  conflict, §4.2's sequencing argument changes.
- **The full EIP-7732 p2p and fork-choice deltas.** I enumerated *containers* from
  `beacon-chain.md` and `fork.md`. `specs/gloas/p2p-interface.md` (59 KB) and
  `fork-choice.md` (41 KB) certainly reshape req/resp protocols and the fork-choice
  store (PTC, payload availability); I did not read them. The container count is
  verified; a Gloas **effort** estimate is not.
- **Whether `specs/gloas/partial-columns/` changes the DAS sidecar shape this repo
  depends on.** Lighthouse's `DataColumnSidecar(Fulu, Gloas)` is strong circumstantial
  evidence; I did not read the spec.
- **Whether `SECONDS_PER_SLOT` is formally deprecated upstream or merely absent from
  `configs/mainnet.yaml`.** The repo's Hoodi fixture carries both keys, suggesting a
  transition period. The symptom is verified from the two files; the upstream policy is
  not.
- **Whether other config-scoped values landed in the preset by the same mistake.** I
  checked the five functions in `pub mod network`; I did **not** audit
  `crates/types/src/preset.rs`. `MIN_PER_EPOCH_CHURN_LIMIT` (`4`) and
  `MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT` (`8`, deprecated) are the obvious next
  suspects. **That audit is the natural first task of the fix** — the class is defined
  by the mismatch, so enumerate every instance before fixing any.
- **Heze.** `configs/mainnet.yaml` already declares `HEZE_FORK_VERSION`/`HEZE_FORK_EPOCH`
  plus `INCLUSION_LIST_DUE_BPS`, `MAX_REQUEST_INCLUSION_LIST`,
  `MAX_TRANSACTIONS_BYTES_PER_INCLUSION_LIST`. I did not research it. A seam designed
  for exactly one more fork will be wrong within a year; design for N.
