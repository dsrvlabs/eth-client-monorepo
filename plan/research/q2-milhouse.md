# Q2 — milhouse / tree-states integration cost

**Verdict:** Cheaper than the study implies, and the repo's own "not indicated" gate
answered the wrong question. The seam is **not** "only a type alias" — it is a type
alias *plus* private fields *plus* 118 hand-written accessors whose signatures already
match milhouse's API *plus* an existing `commit()` hook. milhouse 0.9.0's dependency
pins match this workspace exactly. Estimate: **M** for the in-memory swap (a net
deletion), **L** for on-disk diffs, which should be deferred.

**Question.** Study finding 10: ~3–5 full 150–200 MB `BeaconState` clones per import,
O(V) `get_head` copies, no pubkey cache; "the milhouse seam is still only a type
alias." Where does the alias live? What does milhouse actually demand of consuming
code? How did Lighthouse stage its migration and what did it cost? What does the
diff-based on-disk storage add? And how does study finding 15 ("BeaconState schema
maintained in four places") change the estimate?

---

## 1. Where the seam is, and how good it already is

`crates/types/src/state/mod.rs:44-52`:

```rust
// CC-1H seam (Architecture §3.4): only this module should name these backends.
/// Variable-length list. Phase 1: `ssz_types::VariableList`. CC-1H: milhouse.
pub type List<T, N> = ssz_types::VariableList<T, N>;
/// Fixed-length vector. Phase 1: `ssz_types::FixedVector`. CC-1H: milhouse.
pub type Vector<T, N> = ssz_types::FixedVector<T, N>;
```

The study calls this "only a type alias." **Measured, that is wrong.** Three
enforcement mechanisms sit on top of it:

1. **Every spec field of `BeaconState` is private** (`mod.rs:65-107`), with a
   `compile_fail,E0616` doctest at `mod.rs:7-14` proving external crates cannot index
   `state.validators[0]`.
2. **Reach through 118 `pub fn` accessors** in `crates/types/src/state/accessors.rs`.
   Measured: `List<` / `Vector<` appears **5 times** in the entire tree outside
   `crates/types` (`crates/state-transition/src`, `crates/fork-choice/src`,
   `services/`), and there are **61** call sites of the list accessors in
   `crates/state-transition/src`.
3. **A `commit()` hook already exists** and already documents its milhouse role
   (`accessors.rs:977-985`):
   ```rust
   /// No-op under `ssz_types`. Under milhouse (CC-1H) this will call `apply_updates()`
   /// on every list. Always invoked at the top of [`Self::canonical_root`].
   pub fn commit(&mut self) { /* ssz_types: nothing to flush. */ }
   ```
   It is called from `slots.rs:51`, `block/mod.rs:161`, `accessors.rs:993` and
   `mod.rs:318` — i.e. at the top of every state-root computation and at the end of
   `process_block`. That is exactly where milhouse needs `apply_updates()`.

**The accessor signatures are already milhouse-shaped.** Compare:

| This repo (`accessors.rs`) | milhouse `List<T, N, U>` (`src/list.rs`) |
|---|---|
| `validators_get(&self, i) -> Option<&Validator>` (`:194`) | `get(&self, index: usize) -> Option<&T>` |
| `validators_get_mut(&mut self, i) -> Option<&mut Validator>` (`:622`) | `get_mut(&mut self, index: usize) -> Option<&mut T>` |
| `validators_push(&mut self, v) -> Result<(), StateAccessError>` (`:646`) | `push(&mut self, value: T) -> Result<(), Error>` |
| `validators_set(&mut self, i, v) -> Result<(), StateAccessError>` (`:633`) | (via `get_mut`) |
| `validators_iter(&self) -> impl Iterator<Item = &Validator>` (`:199`) | `iter(&self) -> InterfaceIter<'_, T, U>` (yields `&T`) |
| `validators_len(&self) -> usize` (`:184`) | `len(&self) -> usize` |

Fallibility is already in the signatures. `balances_get(i) -> Option<Gwei>` is
`self.balances.get(i).copied()` — works unchanged. This is a coincidence of good
design, not luck: whoever wrote CC-1H's seam wrote the accessors to milhouse's shape.

**Verified by fetching the source tree** (`sigp/milhouse`, branch `main`, 2026-08-15):
- <https://github.com/sigp/milhouse/blob/main/src/list.rs>
- <https://github.com/sigp/milhouse/blob/main/Cargo.toml>

## 2. milhouse: what it is and what it demands

A **persistent binary merkle tree**. Descendant states reference a base tree plus their
mutations, so:

- clone is **O(1)** instead of O(n) — this is the half that matters here
- random access degrades **O(1) → O(log n)**; iteration stays O(n) with a worse constant
- **the tree is the hash cache** — cached subtree hashes are shared by every descendant
- `rebase(&self, base: &Self)` deduplicates a freshly-loaded tree against an in-memory
  one by recursively comparing leaves and reusing matching subtrees

(Source for the complexity claims: Sigma Prime's own write-up,
<https://sigmaprime.io/blog/tree-states-part1/> — **a blog post, not the source tree**.
The API claims below are from the source tree.)

### What consuming code must change

**Type parameters.** `List<T: Value, N: Unsigned, U: UpdateMap<T> = MaxMap<VecMap<T>>>`
— a *third* parameter with a default, so `List<T, N>` still parses. The alias swap is
literally two lines.

**`Value` bound.** `T` must implement milhouse's `Value` trait. `Validator`, `Gwei`
(u64 newtype), `Root`, `u8` participation flags, `PendingDeposit` etc. will each need
it. For primitives and `Hash256` milhouse provides impls; for repo-local structs it is
a blanket-ish impl requiring `TreeHash + Clone + Debug + PartialEq`. This is the one
real unknown-sized piece.

**`apply_updates()` discipline.** Mutations land in an update map; `tree_hash_root()`
asserts no pending updates. The repo's `commit()` already sits at every root-computing
site, so this is a body fill, not a call-site hunt. `pop_front()` and `intra_rebase()`
call `apply_updates()` internally.

**`FixedVector` → `Vector`.** `randao_mixes_set`, `block_roots_set`, `state_roots_set`,
`slashings_set`, `proposer_lookahead` — all already `Result`-returning setters.

### What becomes dead code

`crates/types/src/state/caches.rs` is **1,015 lines**, and the bulk of it —
`ListHashCache` (`:88-…`, a hand-rolled flat merkle arena with dirty-leaf tracking,
power-of-two capacity growth, `ZERO_HASHES` padding and `mix_in_length`),
`FieldRootCache`, `list_id` (`:22-35`), and the `recompute_caches` machinery in
`accessors.rs:1005+` — exists **only** because `ssz_types::VariableList` has no hash
cache. Under milhouse, all of it is deleted.

That is why Lighthouse's equivalent PR was net-negative (below).

### Dependency compatibility — the pleasant surprise

milhouse 0.9.0 (`Cargo.toml`, source tree):

| milhouse dep | milhouse pin | this workspace (`Cargo.toml`) | match |
|---|---|---|---|
| `ethereum_ssz` | `0.10` | `0.10.4` (`features = ["context_deserialize"]`) | ✅ |
| `tree_hash` | `0.12` | `0.12.1` | ✅ |
| `ethereum_hashing` | `0.8` | `0.8.0` | ✅ |
| `typenum` | `1.14.0` | `1.20.1` | ✅ |
| `arbitrary` | `1.2.3` (optional) | `1.4.2` | ✅ |

milhouse also ships a **`context_deserialize` feature** — which is exactly the
ethereum_ssz feature this repo already enables and uses for
`BeaconState::from_ssz_bytes_with(ForkName::Fulu, …)` (`checkpoint_sync.rs:1023`,
`replay.rs:644`). **No dependency upgrade is required.** Adding milhouse is one
append-only line in `[workspace.dependencies]`.

## 3. How Lighthouse staged it, and what it cost

From the GitHub API (`sigp/lighthouse`), verified 2026-08-15:

| PR | What | Dates | Size |
|---|---|---|---|
| [#3206](https://github.com/sigp/lighthouse/pull/3206) | umbrella: "Upgrade in-memory **and** on-disk state representation with tree states" | opened 2022-05-23, closed 2024-12-19, **never merged as one unit** | — |
| [#5533](https://github.com/sigp/lighthouse/pull/5533) | **"In-memory tree states"** — *"the memory-only portion, i.e. without any database schema changes"* | 2024-04-08 → merged 2024-04-24 | **+2,032 / −2,756 over 108 files, 46 commits** |
| [#5067](https://github.com/sigp/lighthouse/pull/5067) | DB schema upgrade to **v24** for tree-states | 2024-01-15 → 2024-06-14 | separate |
| [#5978](https://github.com/sigp/lighthouse/pull/5978) + [#6040](https://github.com/sigp/lighthouse/pull/6040), [#6386](https://github.com/sigp/lighthouse/pull/6386) | tree-states **archive** — hierarchical diffs in the freezer | 2024-07 → 2024-09 | separate |
| [#7041](https://github.com/sigp/lighthouse/pull/7041)/[#7087](https://github.com/sigp/lighthouse/pull/7087)/[#7176](https://github.com/sigp/lighthouse/pull/7176) | **hot** tree-states (hdiff on the hot DB), incl. *"Fix test OOM issues on tree-states-hot"* | 2025-03 → 2025-04 | separate |

**The three lessons:**

1. **The umbrella PR was abandoned.** A single PR doing memory *and* disk sat open for
   **2 years 7 months** and was closed unmerged. The work only shipped once it was cut
   into memory-only / DB-schema / freezer-diffs / hot-diffs.
2. **The memory-only change net-deleted 724 lines across 108 files.** It also *removed*
   concepts: `StateProcessingStrategy` deleted, `block_production_state` deleted,
   snapshot cache replaced by a state cache holding **32× more states for about the
   same memory** (PR #5533 description). Shipped as default in **v5.2.0**.
3. **The disk half caused the operational pain** — OOMs in tests, a bespoke migration,
   two more years of PRs.

## 4. The on-disk diff scheme (hierarchical state diffs / hdiff)

Since v6.0.0 the freezer stores hierarchical diffs. `--hierarchy-exponents` defaults to
`"5,9,11,13,16,18,21"`: each value is a power-of-two slot interval, `2^5 = 32` slots
(one epoch) for the closest diff layer, `2^21` slots (~291 days) for full snapshots.
Reconstruction fetches the last snapshot, applies successively finer diff layers, then
replays blocks. Published trade-off table (Lighthouse Book,
<https://lighthouse-book.sigmaprime.io/advanced_database.html> — **documentation, not
source**):

| Config | Disk | Historic-state query | Backfill/sync |
|---|---|---|---|
| default `5,9,11,13,16,18,21` | 418 GiB | ≤ 10 s | ~1 week |
| per-slot `0,5,7,11` | 2500 GiB | ≤ 4 s | ~7 weeks |

**Against this repo's current design:** `crates/store/src/snapshots.rs:1-30` stores
*full uncompressed state SSZ*, cadence `DEFAULT_SNAPSHOT_EPOCHS = 32`, ring depth
default **4**, hard cap `MAX_SNAPSHOT_BYTES = 512 MiB`, with the in-file note
*"Measured Hoodi states are ~196 MiB"*. So the current cost is ~784 MiB of ring for a
4-deep history, and the module explicitly says it "never compresses (ADR P4-14)". That
is Lighthouse's *pre*-hdiff design.

The relevant point for staging: **hdiff needs a diff over the tree, which needs the
tree.** The on-disk work is downstream of the in-memory work and shares its seam
(`TABLE_SNAPSHOTS` values are opaque bytes to `cc-store`, so the format change is
confined to `services/storage/src/replay.rs`). Nothing about hdiff is on the critical
path for Stages 0–4.

## 5. The repo's own gate answered the wrong question

`docs/phase-1-soak.md` records two measured gates, and both closed "CC-1H is not
promoted":

- **Early gate** (`docs/phase-1-soak.md:30-62`), 5 epoch boundaries from the Hoodi
  anchor at slot 3649472, M4 Pro: max epoch wall **689.91 ms**, mean hash share
  **23.3 %**. Verdict: *"< 25 % hashing … milhouse would not address the cost."*
- **Mid gate** (`:140-198`), same but with the fork-choice clone in the path: max wall
  **774.81 ms**, mean hash share **21.0 %**, **mean fc clone 66.56 ms, max 109.55 ms**.
  Verdict: *"Epoch cost remains transition-side / **clone-dominant** rather than
  `canonical_root()`-bound. Milhouse is still not indicated."*

The gate criterion is stated in `crates/state-transition/tests/cc1h_early_gate.rs:157-159`:
`">50% hashing → milhouse is the right fix"` / `"<25% hashing → milhouse will not
help"`.

**That criterion treats milhouse as a hashing-cache fix. It is two fixes.** The second
is O(1) structural-sharing clones — and the mid gate's own text says the cost is
*clone-dominant*, then measures a clone at **66.56 ms mean / 109.55 ms max** and
concludes the clone-fix library is not indicated. The gate measured the right number
and applied the wrong rule.

Scale that: study finding 10 says ~3–5 clones per import. At the measured 66.56 ms
mean that is **200–333 ms per block of pure `memcpy`**, on a fast dev machine, on an
idle queue, before any of the *other* clone sites:

- `crates/fork-choice/src/on_block.rs:396` — `let mut pull_state = state.clone();`
- `crates/fork-choice/src/on_block.rs:447` — `store.insert_block(root, header, state.clone())`
- `crates/fork-choice/src/on_block.rs:162` — anchor clone at store init
- `services/chain/src/restore.rs:974,1005` — two per replayed block
- `crates/fork-choice/src/head_cache.rs:181-184` — the O(V) `get_head` copies:
  `justified_balances_snapshot(...)`, `store.justified_balances().to_vec()`,
  `proto_array().indices().clone()`, `equivocating_indices().clone()`

The last group is *not* fixed by milhouse (they are `Vec`/`HashMap`, not state lists) —
worth saying plainly so the estimate is honest.

**Recommendation on the gate:** do not re-run it as written. Replace the criterion with
`clone_ms / import_ms` and re-decide. On the numbers already in the file, that ratio is
already decisive.

## 6. The "four places" (study finding 15) — enumerated

Adding or reshaping one `BeaconState` field today requires editing, in lockstep:

1. **`crates/types/src/state/mod.rs:65-107`** — the struct's 38 spec fields; field
   *order* is load-bearing (SSZ + `TreeHash` derive).
2. **`crates/types/src/state/mod.rs:108-…`** — the hand-written `impl Default`, which
   repeats all 38 fields.
3. **`crates/types/src/state/caches.rs:19` + `:40-79`** — `BEACON_STATE_FIELD_COUNT = 38`
   and the `StateField` enum with 38 explicit discriminants that must equal the struct's
   field order. A silent mismatch here produces a **wrong state root**, not a compile
   error.
4. **`crates/types/src/state/accessors.rs`** — 118 accessors, each of which must mark
   the correct `StateField` dirty and, for the five cached lists, the correct
   `list_id::*` (`caches.rs:22-35`, a fifth place with `COUNT = 5`).

**This is an argument *for* milhouse, not against it.** Places 3 and 4's cache-marking
half exist only to serve the hand-rolled hash cache; under milhouse, `StateField` can
collapse to a derive and `list_id` disappears entirely. The migration *reduces* the
number of places the schema is maintained from four (five) to two — and place 2 can go
too by deriving `Default`.

Conversely, EPBS/Gloas (Q5) will reshape ~6 containers and change `BeaconState`'s field
list. **Doing that retrofit against four hand-synchronised places is the expensive
version.** Sequencing milhouse *before* the Gloas schema work is the cheaper order.

---

## 7. Recommendation for this codebase

**Stage A — in-memory swap only (do this).** Mirror Lighthouse PR #5533 exactly: no
storage-format change, no on-disk migration.

1. Add `milhouse = "0.9"` (append-only, per the workspace convention).
2. Implement `milhouse::Value` for the ~8 element types used in state lists.
3. Flip the two aliases at `crates/types/src/state/mod.rs:48,51`.
4. Fill in `commit()` (`accessors.rs:983`) with `apply_updates()` per list; it is
   already called everywhere it needs to be.
5. Delete `ListHashCache`, `list_id`, and the list half of `recompute_caches`.
   Keep `EpochCache`, `ShufflingCache`, `PubkeyIndexMap` — they are orthogonal.
6. Keep `canonical_root()`'s public signature; its body becomes `commit()` +
   `tree_hash_root()`.
7. Re-run `crates/spec-tests` and `crates/types/tests/ssz_static.rs` unchanged — the
   SSZ bytes and roots must be bit-identical. **This is the acceptance criterion**, and
   the repo already has the vectors to prove it.

Everything outside `crates/types` should compile untouched. If it doesn't, the seam
was leakier than measured and you find out on day one for the cost of a `cargo check`.

**Stage B — `rebase()` on load (do this next, cheap).** After Stage A, states decoded
from `TABLE_SNAPSHOTS` or restore are fresh trees sharing nothing with memory. One
`rebase_on(&head_state)` call at `services/chain/src/restore.rs:437` and
`services/chain/src/checkpoint_sync.rs:1023` recovers the sharing. Small, high value,
directly attacks the restore path's per-block `parent_state.clone()`
(`restore.rs:974`).

**Stage C — hdiff on disk (defer).** Not on the Stage 0–4 critical path. Lighthouse
spent 2024-07 → 2025-04 on it and hit test OOMs. Revisit only when the 4-deep, ~784 MiB
snapshot ring is measured to be a real constraint on a supernode.

**Do not** attempt A+C as one change. That is precisely the PR that sat open for 2.5
years and was closed unmerged.

## 8. Effort estimate

| Stage | Size | Rationale |
|---|---|---|
| A — in-memory swap | **M** | Confined to `crates/types/src/state/*` (2,537 lines across 3 files); 5 `List<`/`Vector<` uses outside `crates/types`; accessor signatures already match; deps already pinned compatibly; the change is **net-negative lines**. Lighthouse's equivalent was 108 files / −724 net but against a far larger codebase with no equivalent accessor wall. Budget **2–4 weeks**, dominated by (a) `Value` impls and (b) proving root-identity on the spec vectors. |
| B — `rebase` on load | **S** | 2–3 call sites; ~1 week including a memory-residency test. |
| C — hdiff on disk | **L** | New diff format, a store migration, `replay.rs` snapshot/restore rewrite, and a whole falsifier re-run (`docs/storage-engine.md` bounds). Lighthouse took ~9 months. |
| — total if attempted as one | **XL** | Do not. |

**Risk that would raise A to L:** if `milhouse::Value` turns out to require blanket
impls the repo's `Validator`/`PendingDeposit`/`SyncCommittee` types cannot satisfy
without derive changes that ripple into `crates/types/src/containers.rs`. Spike this
first — it is a half-day `cargo check` against a scratch branch and it is the single
number that decides M vs L.

## 9. What I could not determine

- **Whether `milhouse::Value` is satisfiable for every element type here without
  container changes.** I read `src/list.rs`'s signatures but not `src/value.rs` or its
  blanket impls. This is the load-bearing unknown in the estimate and it should be
  spiked before anyone commits to a number.
- **Whether milhouse's `Vector<T, N>` supports `Root`/`Hash256` element packing the way
  `FixedVector` does**, i.e. whether `block_roots`/`state_roots`/`randao_mixes` keep the
  same SSZ layout. The spec vectors would catch a regression, but I did not verify it
  in advance.
- **Whether `Encode`/`Decode` for milhouse `List` supports the `context_deserialize`
  path** this repo relies on (`from_ssz_bytes_with(ForkName::Fulu, …)`). milhouse
  *declares* a `context_deserialize` feature, which is strong evidence, but I did not
  read its implementation.
- **The line count of Lighthouse's `consensus/types` before and after #5533**, so I
  cannot give a like-for-like scaling factor from their 108 files to this repo's 3.
- **Whether the mid-gate's 66.56 ms clone figure is a `BeaconState` clone alone or
  clone + `process_justification_and_finalization`.** `docs/phase-1-soak.md:148-149`
  says *"`fc_clone_ms` is the clone + J&F portion alone"* — so 66.56 ms is an **upper
  bound** on the clone, not the clone. The conclusion survives (J&F is not 66 ms of
  arithmetic on an epoch that totals ~550 ms), but the exact per-clone cost needs a
  dedicated measurement before anyone quotes "200–333 ms/block".
- **The diff/compression algorithm behind hdiff.** The Lighthouse Book does not name
  it; a PR comment mentions `xdelta3` compilation problems on macOS ARM
  ([#6040](https://github.com/sigp/lighthouse/pull/6040)), which suggests xdelta3, but I
  did not confirm it from the source tree.
