# Q3 — the pubkey cache

**Verdict:** The study's framing understates this. It is **not** a ~1.5–3 s/block
performance tax — `process_sync_aggregate` has **no linear-scan fallback**; it returns
`BlockError::CachePoisoned`, classified `GossipClass::Internal`, so a checkpoint-synced
or restored node **fails every block import, silently, forever**. And it is **not**
latent behind the dead gossip wiring: both `RestoreFromStore` (`restore.rs:437` →
`:525`) and storage replay (`replay.rs:644` → `:572`) run the state transition on an
SSZ-decoded state today, so this fires on the second boot of any node that has taken a
snapshot. Root cause: the cache is a `#[ssz(skip_deserializing)]` field *on
`BeaconState`*, while every production client keeps it *outside* the state. Fix is
**S**, and it is a Stage 0 correctness fix, not a Stage 5 optimisation.

**Question.** How do production clients build, persist and invalidate the pubkey→index
cache? What invalidation events exist? How does it interact with checkpoint sync?
Deliver a design sized for `crates/types` + `crates/state-transition`.

---

## 1. What this repo actually does

### 1.1 The cache exists — inside the state, excluded from SSZ

`crates/types/src/state/caches.rs:509-516`:

```rust
/// Append-only pubkey → validator index map (filled by deposit processing).
#[derive(Clone, Default)]
pub struct PubkeyIndexMap {
    map: HashMap<BlsPublicKey, ValidatorIndex>,
    linear_scan_count: u64,
}
```

It lives at `StateCaches.pubkeys` (`caches.rs:867`), and `StateCaches` is a field of
`BeaconState` marked (`crates/types/src/state/mod.rs:104-106`):

```rust
#[ssz(skip_serializing, skip_deserializing)]
#[tree_hash(skip_hashing)]
caches: StateCaches<P>,
```

Correct for consensus (caches must not affect the state root). **Fatal for
reachability:** every `BeaconState` produced by SSZ decode starts with an empty
`PubkeyIndexMap`.

### 1.2 Production fills it in exactly two places, both deposit-driven

- `crates/state-transition/src/block/operations/deposit.rs:101` and `:142`
- `crates/state-transition/src/epoch/pending_deposits.rs:53`
- plus lazy backfill in `get_validator_index_by_pubkey`
  (`crates/state-transition/src/helpers/accessors.rs:509-527`), which *does* scan and
  counts the scan

So the map is populated only for validators this process saw deposited **since the
state was decoded**.

### 1.3 Every production entry point decodes a state

| Path | Site | Fills pubkeys? |
|---|---|---|
| checkpoint sync | `services/chain/src/checkpoint_sync.rs:1023` — `BeaconState::<P>::from_ssz_bytes_with(ForkName::Fulu, &state_bytes)` | **no** |
| `RestoreFromStore` | `services/chain/src/restore.rs:437` — `BeaconState::<P>::from_ssz_bytes(input.state_ssz)` | **no** |
| storage replay | `services/storage/src/replay.rs:644` | **no** |

`services/chain/src/main.rs` has no genesis-state-file load path at all (grep for
`genesis` returns only `genesis_validators_root` config plumbing), so **checkpoint sync
and restore are the only two ways a production chain service acquires a state**, and
neither fills the cache. `get_forkchoice_store` (`crates/fork-choice/src/on_block.rs:111-179`)
does not fill it either — it seeds votes, balances and a `CheckpointContext`, nothing
else.

### 1.4 `process_sync_aggregate` requires the map and has no fallback

`crates/state-transition/src/block/sync_aggregate.rs:117-131`:

```rust
// Resolve committee indices **only** through PubkeyIndexMap (no linear scan).
let proposer_index = get_beacon_proposer_index(state)?;
let mut committee_indices = Vec::with_capacity(sync_size);
for i in 0..sync_size {
    let pk = committee.pubkeys.get(i).ok_or(BlockError::ArithmeticOverflow)?;
    let idx = state
        .caches()
        .pubkeys
        .get(pk)
        .ok_or(BlockError::CachePoisoned)?;   // ← no scan fallback
    committee_indices.push(idx);
}
```

`process_sync_aggregate` is unconditional in `process_block`
(`crates/state-transition/src/block/mod.rs:158`) — sync-committee rewards are
consensus-relevant, so it runs under every `BlockSignatureStrategy`, including
`NoVerification`.

### 1.5 The failure is silent

`crates/state-transition/src/error.rs:287-298` classifies `CachePoisoned` as
`GossipClass::Internal` — deliberately, *"State-resident BLS failures and nested NYI
must not look like peer fault (SEC-12a-1, SEC-12a-2)"*. Correct as a peer-scoring
decision. The consequence is that a node in this state:

- imports no blocks,
- descores no peers,
- rotates no peers,
- emits no `Reject` and no invalid-block metric,
- and the health DAG (which roots at `chain` and checks liveness, not progress) stays
  **green**.

This is the same class as study finding 01: a spec-complete subsystem severed by
something no compiler and no test could see.

### 1.6 Why no test catches it — this is the finding

Every test that exercises `process_block` hand-fills the map first:

- `crates/state-transition/tests/random.rs:172`
- `crates/state-transition/tests/epoch_processing.rs:183`
- `crates/state-transition/tests/sanity.rs:171`
- `crates/state-transition/tests/finality.rs:169`
- `crates/state-transition/tests/operations.rs:387`
- `crates/state-transition/src/block/mod.rs:253-259` (the unit test's own comment:
  *"Map default (zero) sync-committee pubkeys to the seeded validator so reward
  accounting can resolve indices without a registry scan."*)
- `crates/fork-choice/tests/fork_choice.rs:472`
- `services/chain/tests/offline_replay.rs:264`

And the devnet generator does it in-process at genesis construction
(`bin/devnet-gen/src/genesis.rs:101-107`), with the comment
*"Pubkey cache (required by process_sync_aggregate)"* — then serialises the state to
SSZ, which drops it.

**The suite fills by hand exactly the thing production never fills.** Eight test
harnesses independently discovered the requirement and each patched around it locally.
That is a strong signal the invariant is real and the wiring is missing.

### 1.7 Reachability — this is **live today on the boot path**, not latent

The obvious assumption is that this is latent behind study finding 01 (gossip is never
subscribed, so no block reaches `on_block`). **That assumption is wrong.** Two
production paths run the state transition on an SSZ-decoded state with no gossip
involved:

**Path 1 — `RestoreFromStore`.** `services/chain/src/restore.rs:437` decodes the
snapshot state, `:471` builds the store from it, and `:525` calls `on_block(...)` in a
loop over the restore blocks — which runs `state_transition` → `process_block` →
`process_sync_aggregate`. The first restore block carrying a sync aggregate (i.e. any
post-Altair block) returns `CachePoisoned` and the loop reports
`"restore block[{i}] root {root}: on_block error: {e}"` (`restore.rs:555`).
**`RestoreFromStore` cannot advance past its anchor.**

**Path 2 — storage replay.** `services/storage/src/replay.rs:643-646`
(`decode_mainnet_state`) decodes a snapshot, and `:572` calls
`state_transition(state, &signed, &ctx, BlockSignatureStrategy::NoVerification)` per
replayed block. Same failure.

So the bug fires on **the second boot of any node that has taken a snapshot** — the
restart-recovery path the whole storage design exists to serve. That is a plausible
explanation for why "the broken cross-process restart choreography" (study §1) has never
demonstrably worked end to end.

**Why no test catches Path 2 either:** `state_transition(` appears exactly **once** in
`services/storage/src/replay.rs` — the production call at `:572`. Every replay test
exercises the empty-slot branch or the error branch; the one named for it is
`own_replay_process_slots_advances_without_blocks` (`replay.rs:1238`), and `process_slots`
never calls `process_sync_aggregate`. The block-replay branch has **zero** STF coverage.

Checkpoint sync (`checkpoint_sync.rs:1023`) is the third path and *is* latent behind
gossip — it fails on the first live block after Stage 3 wires subscribe, presenting as
*"we wired gossip, peers are healthy, and it imports nothing"* with no error naming the
cause.

### 1.8 Two more costs on the same code

**(a) The cache is on the wrong side of the clone boundary.** `PubkeyIndexMap` derives
`Clone` and `StateCaches` is cloned with the state. At 1 M validators a
`HashMap<BlsPublicKey, ValidatorIndex>` with 48-byte keys
(`crates/types/src/primitives.rs:292-294`) is ~80–100 MB of table, deep-copied on every
one of the 3–5 `BeaconState` clones per import (Q2). Moving the cache off the state
fixes this too — and it is a prerequisite for milhouse's O(1) clone to actually be
O(1).

**(b) There is no decompressed-pubkey cache.** `process_sync_aggregate` calls
`decode_state_pubkey(pk)` (`sync_aggregate.rs:67`) for every participating committee
member, and `decode_state_pubkey` is a bare
`PublicKey::deserialize(pk.as_array())` (`crates/state-transition/src/signatures.rs:127-130`)
— a blst decompress plus subgroup check, per key, per block, uncached.
`SYNC_COMMITTEE_SIZE` is 512 on mainnet, and the attestation/slashing/exit paths
decompress from the registry the same way (`signatures.rs:297`, `:265-272`). Lighthouse
caches the decompressed `PublicKey` for exactly this reason (§2).

---

## 2. What production clients do

**Lighthouse** — `beacon_node/beacon_chain/src/validator_pubkey_cache.rs`
(<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_chain/src/validator_pubkey_cache.rs>),
source tree, fetched 2026-08-15:

```rust
pubkeys: Vec<PublicKey>,                    // decompressed, index-ordered
indices: HashMap<PublicKeyBytes, usize>,    // reverse lookup
pubkey_bytes: Vec<PublicKeyBytes>,          // compressed, index-ordered
```

Four properties matter, and this repo has none of them:

1. **It is not on `BeaconState`.** It hangs off `BeaconChain`
   (`self.validator_pubkey_cache`,
   <https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_chain/src/beacon_chain.rs>)
   behind a `parking_lot::RwLock` accessed via `upgradable_read()` — read on the fast
   path, upgraded to write only when new validators appear.
2. **It is persisted.** `import_new_pubkeys(&state) -> Result<Vec<StoreOp<'static, E>>>`
   *returns* store operations, with the doc comment *"NOTE: The caller **must** commit
   the returned I/O batch as part of the block import process."* The cache write is
   atomic with the block that introduced the validators — never a separate,
   independently-losable write.
3. **It is rebuilt/topped-up at startup.** `load_from_store()` reads the persisted
   entries; then `beacon_node/beacon_chain/src/builder.rs`
   (<https://github.com/sigp/lighthouse/blob/stable/beacon_node/beacon_chain/src/builder.rs>)
   calls `import_new_pubkeys(&head_snapshot.beacon_state)`
   with the comment *"If any validators weren't persisted to disk on previous runs,
   this will use the head state to 'top-up' the in-memory validator cache and its
   on-disk representation with any missing validators."* This is the checkpoint-sync
   answer: the anchor state's `validators` list is the source of truth, and a
   fresh/empty cache is topped up from it, not treated as an error.
4. **It is append-only, explicitly.** *"does not delete any keys from `self` if they
   don't appear in `state`."* `import_new_pubkeys` only scans
   `cache.len()..state.validators().len()`, decompressing in parallel on the initial
   build and sequentially for incremental updates.

The source contains **no** handling for a cache that is *ahead* of the state — it is
structurally impossible in their design because the cache only ever grows toward the
state's length.

### Invalidation events — there are none

The validator registry is **append-only in the spec**: `process_deposit` /
`apply_deposit` only push; exits, slashings and consolidations mutate a `Validator`'s
epoch fields but never remove the entry or change its pubkey. Therefore:

- **No invalidation event exists.** The map is monotone.
- The only "staleness" question is *length*: is `cache.len() >= state.validators_len()`?
- **Reorgs do not invalidate it.** A validator present on an orphaned branch but not on
  the canonical one leaves a stale extra entry. That is harmless: the entry maps a
  pubkey to an index that, on the canonical chain, is not yet occupied. Any consumer
  that resolves an index must still bounds-check against the *state's* registry length
  — which `process_sync_aggregate` does implicitly (the pubkey it looks up came from
  the state's own `current_sync_committee`).
- This is why Lighthouse can persist it once and top it up forever.

I did not verify the Prysm/Teku/Nimbus implementations from their source trees; see §5.

---

## 3. Concrete design for this repo

Three changes, in dependency order. Change 1 alone removes the import stall.

### Change 1 — top up from the registry after every state decode (**Stage 0, do now**)

Add to `crates/types` (next to `PubkeyIndexMap`) or `crates/state-transition/src/helpers`:

```rust
/// Fill `caches.pubkeys` for every validator index not yet present.
///
/// Append-only: never removes. Idempotent. O(V) once, O(new) thereafter.
/// MUST be called on any state that did not come from an in-process transition
/// (SSZ decode, checkpoint sync, restore, replay).
pub fn top_up_pubkey_cache<P: Preset>(state: &mut BeaconState<P>) {
    let start = state.caches().pubkeys.len();
    let len = state.validators_len();
    for i in start..len {
        if let Some(v) = state.validators_get(i) {
            let pk = v.pubkey;
            state.caches_mut().pubkeys.insert(pk, ValidatorIndex::new(i as u64));
        }
    }
}
```

(The `start` shortcut is only valid while the map's length tracks a contiguous prefix.
Safer first cut: iterate all indices and `insert` unconditionally — it is idempotent and
runs once per decode, not per block.)

Call sites — all three, no exceptions:

- `services/chain/src/checkpoint_sync.rs:1023`, **after** the
  `computed_state_root != signed_block.message.state_root` check at `:1044` (the cache
  is tree-hash-skipped so ordering is not a correctness issue, but keeping it after
  verification keeps the "don't touch the state before it's proven" discipline).
- `services/chain/src/restore.rs:437`, before `get_forkchoice_store`.
- `services/storage/src/replay.rs:644`.

Better still: make it unskippable. Add a `BeaconState::from_ssz_bytes_hydrated(...)`
constructor in `crates/types` that decodes **and** tops up, and make the raw
`from_ssz_bytes` `pub(crate)` outside tests. Then the bug class ("someone adds a fourth
decode site in 2027") is unrepresentable — the same argument the whole consolidation
rests on.

**Cost at 1 M validators:** one pass over the registry building a 1 M-entry `HashMap`
with 48-byte keys — order 1–3 s and ~100 MB, **once per boot**. Acceptable. Lighthouse
pays the same on a cold cache and parallelises the decompression.

### Change 2 — a regression test that would have caught it (**do with Change 1**)

The bug's whole character is that eight test harnesses hand-fill the cache. So the test
must **not** be allowed to:

```rust
#[test]
fn ssz_round_tripped_state_imports_a_block_with_a_sync_aggregate() {
    let state = build_state_with_sync_committee();          // fills caches in-process
    let bytes = state.as_ssz_bytes();
    let mut decoded = BeaconState::<Minimal>::from_ssz_bytes(&bytes).unwrap();
    // NO hand-fill here — that is the point.
    top_up_pubkey_cache(&mut decoded);
    process_block(&mut decoded, &block, &ctx, pre_root).unwrap();
}
```

Plus a negative assertion that omitting the top-up yields `BlockError::CachePoisoned` —
so the invariant is documented by a test rather than by eight copies of a workaround.

### Change 3 — move the cache off `BeaconState` (**Stage 2, with the storage fold**)

Target shape, mirroring Lighthouse:

```rust
// crates/types (or a new cc-cache module owned by chain)
pub struct ValidatorPubkeyCache {
    pubkeys: Vec<PublicKey>,                     // DECOMPRESSED, index-ordered
    pubkey_bytes: Vec<BlsPublicKey>,             // compressed, index-ordered
    indices: HashMap<BlsPublicKey, ValidatorIndex>,
}

impl ValidatorPubkeyCache {
    pub fn get_index(&self, pk: &BlsPublicKey) -> Option<ValidatorIndex>;
    pub fn get_pubkey(&self, i: ValidatorIndex) -> Option<&PublicKey>;   // no decompress
    pub fn len(&self) -> usize;
    /// Append-only top-up; returns the new entries for durable write.
    pub fn import_new<P: Preset>(&mut self, state: &BeaconState<P>) -> Vec<(ValidatorIndex, BlsPublicKey)>;
}
```

Owned by the chain core alongside the `Store` (single-owner, same thread), reached by
the state transition through a `&ValidatorPubkeyCache` on `TransitionContext` — which
already exists (`crates/state-transition/src/block/mod.rs:149`,
`ctx: &TransitionContext<'_, P>`) and already carries the chain config. That is the
natural place and it costs one field.

Payoffs beyond the stall fix:

- the ~100 MB `HashMap` leaves the state clone path (§1.8a) — a prerequisite for Q2's
  O(1) clones to be real
- `get_pubkey(i) -> &PublicKey` removes the per-block blst decompression for sync
  committees, attestations, slashings and exits (§1.8b)
- persistence becomes possible (Change 4)

**Do not do Change 3 before Change 1.** Change 1 is a Stage-0 correctness fix that must
land in days; Change 3 touches `TransitionContext`, every signature helper and the
spec-test harness, and belongs with the storage fold.

### Change 4 — persist it (**Stage 2, optional**)

Once the cache is a chain-level structure, a `pubkeys` table in `cc-store` keyed by
`index: u64be` → 48-byte pubkey makes boot O(new) instead of O(V), and Lighthouse's
rule applies verbatim: **the cache write must be in the same batch as the block that
introduced the validators.** Note the tension with `services/storage`'s write-behind
design — see Q4. Because the cache is *reconstructible from the state* (unlike slashing
protection), losing it is merely a slow boot, so it is safe on the write-behind path.
That distinction is worth writing into the module docs so the two cases are not
conflated later.

### Checkpoint-sync interaction — the summary

Checkpoint sync is precisely the case that breaks today and precisely the case
Lighthouse designed for. The anchor state's `validators` list **is** the ground truth
for indices 0..N; there is no need for a "backfill" of pubkeys from before the anchor,
because a pubkey that does not appear in the anchor registry cannot be referenced by
any post-anchor block. So: build from the anchor registry, then grow by deposits.
No special case, no historical fetch.

This is the same shape as fork-choice's `resize_votes` gap (review finding
`crates/fork-choice/src/on_block.rs:447`): both are "the anchor registry is a
lower bound, and nothing grows it." Fixing them together in one Stage-0 PR is
sensible — they have the same trigger (checkpoint sync) and the same fix (read the
post-state).

---

## 4. Effort estimate

| Change | Size | Rationale |
|---|---|---|
| 1 — `top_up_pubkey_cache` + 3 call sites | **S** | ~30 lines plus 3 one-line calls. Half a day. |
| 1b — `from_ssz_bytes_hydrated` chokepoint | **S** | ~1 day; makes the class unrepresentable rather than fixed-thrice. |
| 2 — round-trip regression test | **S** | ~1 day including the negative case. |
| 3 — move off `BeaconState` onto `TransitionContext` | **M** | `TransitionContext` threading through `signatures.rs` (12 push-* helpers), `sync_aggregate.rs`, `deposit.rs`, `pending_deposits.rs`, `accessors.rs:509`, plus the 8 test harnesses that currently hand-fill and would instead construct a cache. 1–2 weeks. |
| 4 — persist in `cc-store` | **S–M** | New table + boot path; rides Stage 2's storage fold. |

**Total for the correctness fix: S. Do it in Stage 0.** The rest is genuine
optimisation and can wait.

---

## 5. What I could not determine

- **Whether the failure is *exactly* "every block".** It is every block whose
  `sync_committee_bits` resolution needs a pubkey the map lacks — which, on a
  freshly-decoded state, is all 512 committee members regardless of participation,
  because the loop at `sync_aggregate.rs:120-131` resolves **all** `sync_size` indices
  before checking bits. I read the code and traced the two live boot paths (§1.7), but
  I did not **execute** it. **Run this before quoting the severity**: decode the
  committed Hoodi anchor state from SSZ and call `process_block` with a real block —
  a ~30-line test that settles the claim definitively. The strongest form of the
  finding ("restore has never worked past its anchor") deserves an executed
  reproduction, not a code read.
- **Whether restore has ever been observed working past its anchor on a real store.**
  The study records all acceptance runs as `NOT_RUN`, and `replay.rs` has no
  block-replay STF test, so I found no evidence either way — but absence of evidence is
  not the same as the confirmed failure I am asserting from the code path.
- **How Prysm, Teku, Nimbus and Grandine implement this.** I verified only Lighthouse
  from source. The "no invalidation events" claim rests on the spec's append-only
  registry (which I am confident about) plus Lighthouse's explicit comment, not on a
  four-client survey.
- **The real cost of building a 1 M-entry map at boot** on this repo's hashing setup.
  Lighthouse parallelises decompression; the estimate of 1–3 s is a guess from the
  1 M × (48-byte SipHash + insert) shape, not a measurement.
- **Whether `linear_scan_count` / `note_linear_scan` instrumentation
  (`caches.rs:548-563`, asserted by `operations.rs:1665-1702`) would fire usefully as a
  production alarm.** It counts scans in `get_validator_index_by_pubkey` — but
  `process_sync_aggregate` never scans, so the counter is silent in exactly the failure
  mode that matters. A `pubkey_cache_len` gauge compared against `validators_len` would
  be the honest metric, and I did not check whether the chain metrics facade has a
  natural home for it.
- **Whether any Phase 5–7 stub already assumes a chain-level pubkey cache** (the
  `GetValidatorPubkeys` query at `services/chain/src/core.rs:63` and
  `MAX_VALIDATOR_PUBKEYS_PER_REQUEST = 256` suggest a design intent), which would change
  where Change 3 should land. Worth a look before designing `TransitionContext`'s field.
