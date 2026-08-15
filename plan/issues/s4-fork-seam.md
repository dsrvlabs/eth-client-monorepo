# S4 — the fork seam · wk 48–59

**Entry.** Phase 1–4 clauses discharged (**M2 = 0**) · **P0-19/3 landed at S2** (else milhouse's O(1)
clone is a lie) · **Q-3 answered** (`S0a-B-09`, scheduled wk 1).
**Ships.** Gloas becomes a **new module**, not a 44-file retrofit.

**Internally ordered 4a → 4b → 4c**, and the ordering is **conditional on Q-3** — see the inversion
branch below.

**Out of scope** — milhouse **on-disk diffs** (**L**, deferred by `[Q2]`); any Phase 5 code (P1-E/S4's
whole point is that the seam lands *before* it).

Estimate provenance and the points scale are defined in [`s0a-gate-restoration.md`](s0a-gate-restoration.md).

---

## ⚠ The ordering is conditional, and the condition was answered in wk 1

`S0a-B-09` (**Q-3**) asks: *does `superstruct` compose with milhouse's `List<T, N, U>` third type
parameter?* It is **one hour** of reading Lighthouse's `consensus/types/src/beacon_state.rs`, and
`[PLAN]` §8 schedules it in **wk 1**, not at S4 entry, precisely because **it can invert 4a → 4b**.

| Q-3 answer | Order | Rationale |
|---|---|---|
| **YES — composes** *(the planned branch)* | **4a → 4b → 4c** | ⟡ D-8 ch.2: milhouse reduces the state schema from **four** hand-synchronised places to **two** *before* a −1/+9-field fork edits it, **at no extra cost**. One of the four — the `StateField` discriminant order — yields a **wrong state root, not a compile error**, when it drifts (D5) |
| **NO — does not compose** | **`S4-ALT`: 4b → 4a → 4c** | The Gloas containers must be superstructed against the *current* `BeaconState` representation; milhouse then swaps underneath. This **loses D-8 ch.2's benefit** — the Gloas schema edit becomes a four-place synchronised change — so `S4-ALT-01` (below) becomes mandatory |

### `S4-ALT-01` · The inversion branch, named so it is not a footnote

**Only if Q-3 came back NO.** The dependency edges change as follows:

- `S4b-01` … `S4b-06` move **before** `S4a-01` … `S4a-06`.
- `S4a-04` (the `StateField` discriminant deletion) no longer precedes the Gloas schema edit, so the
  Gloas edit touches **four** hand-synchronised places (`[Q2]` §6). **Add a mandatory
  four-place-synchronisation guard** — a test that computes the state root from an independently
  ordered field list and asserts equality — because the drift failure mode here is a **wrong state
  root**, not a compile error.
- Estimate delta: **+2–3 pd** for the guard, plus the two-place-vs-four-place edit cost in `S4b-03`
  (+1–2 pd).
- `[PLAN]` D6 records this inversion as a real dependency, not a hypothetical.

**If Q-3 was never run, S4 cannot start.** It is one hour. Run it.

---

## Issue index

### Pre-S4 spikes (wk 46, before S4 scoping closes)

| Id | Title | Stream | pd | pts | Blocks |
|---|---|---|---:|---:|---|
| `S4-Q-04` | **Spike Q-4** — `superstruct`'s compile-time cost on this workspace | A | 0.5–1 | 2 | superstruct-vs-hand-written |
| `S4-Q-05` | **Spike Q-5** — does `specs/gloas/partial-columns/` change the DAS sidecar shape? | B | 0.5–1 | 2 | the scope of ⟡ D-7 (`S4c-03`) |

### S4a — milhouse · **serial**

| Id | Title | Stream | pd | pts | Deps |
|---|---|---|---:|---:|---|
| `S4a-01` | Add milhouse 0.9; verify the workspace dep set is unchanged | A | 1–1.5 | 3 | `S0a-B-09` |
| `S4a-02` | Swap the `List`-shaped `BeaconState` fields to milhouse | A | 3–4 | 8 | `S4a-01` |
| `S4a-03` | Reconcile the accessors against milhouse's API | A | 2.5–3.5 | 5 | `S4a-02` |
| `S4a-04` | Delete the `StateField` discriminant table (schema place 3 of 4) | A | 2–3 | 5 | `S4a-03` |
| `S4a-05` | Delete the second hand-synchronised schema place; P2-B/6 rides | A | 1.5–2.5 | 3 | `S4a-04` |
| `S4a-06` | **State-root equivalence** — pre-swap vs post-swap over the vector corpus | A | 2–3 | 5 | `S4a-05` |
| `S4a-07` | **Clone-cost measurement** — the O(1) claim, measured against S2's baseline | A | 1.5–2 | 3 | `S4a-06` |
| `S4a-08` | `ADR-P1-04` superseded (the cached state-root path) | A | 0.75–1 | 2 | `S4a-03` |
| | **4a total** | | **14.25–20.5** | **34** | |

### S4b — the Gloas schema

| Id | Title | Stream | pd | pts | Deps |
|---|---|---|---:|---:|---|
| `S4b-01` | `ForkName::Gloas` + monotone capability predicates (~10 lines) | A | 1–1.5 | 3 | `S4a-05` |
| `S4b-02` | Turn the five hardcoded `ForkName::Fulu` call sites into `fork_name_at_epoch` | A | 1.5–2 | 3 | `S4b-01` |
| `S4b-03` | superstruct on `BeaconState` and `BeaconBlockBody` | A | 3–4 | 8 | `S4b-02` |
| `S4b-04` | superstruct on `ExecutionPayload` and `ExecutionRequests` | B | 2.5–3.5 | 5 | `S4b-03` |
| `S4b-05` | superstruct on `Attestation` and `IndexedAttestation` | B | 2.5–3.5 | 5 | `S4b-03` |
| `S4b-06` | `upgrade_to_gloas` | A | 2–3 | 5 | `S4b-03` |
| `S4b-07` | Per-fork STF dispatch by predicate — block processing | A | 3–4 | 8 | `S4b-01`, `S4b-03` |
| `S4b-08` | Per-fork STF dispatch by predicate — epoch processing | A | 2.5–3.5 | 5 | `S4b-07` |
| `S4b-09` | P1-B/8 sibling check + P2-B/6 (`SigningData` duplication) | B | 1–1.5 | 3 | `S4b-01` |
| | **4b total** | | **19–26.5** | **45** | |

### S4c — the new containers and enforcement

| Id | Title | Stream | pd | pts | Deps |
|---|---|---|---:|---:|---|
| `S4c-01` | The 13 new Gloas containers — types + SSZ + tree-hash (1 of 2) | B | 3–4 | 8 | `S4b-03` |
| `S4c-02` | The 13 new Gloas containers (2 of 2) | B | 3–4 | 8 | `S4c-01` |
| `S4c-03` | **⟡ D-7** — `DataColumnSidecar(Fulu, Gloas)`, omitted by both sources | B | 2.5–3.5 | 5 | `S4c-01`, `S4-Q-05` |
| `S4c-04` | **Blocking total-coverage enforcement** (§5.6) | B | 3–4 | 8 | `S4c-02` |
| `S4c-05` | Gloas spec-vector suites green, both presets | B | 2.5–3.5 | 5 | `S4c-04` |
| `S4c-06` | M3 ledger maintenance for S4 rows | B | 0.5 | 1 | — |
| | **4c total** | | **14.5–19.5** | **35** | |

**Phase totals.** **48.75–68.5 pd · 118 pts**, including the two pre-S4 spikes.

**Parallel duration.** **4a is serial** — it deletes two schema places 4b depends on, and its issues
form a single chain. 4b/4c parallelise. Stream A binds:
4a 14.25–20.5 pd (serial, ~4 pd/wk) = **3.6–5.1 wk**, then 4b's stream-A chain 13.5–18 pd = 3.4–4.5 wk
overlapped with 4c's stream-B work → **≈ 9.5–13 wk**, against `[PLAN]`'s **10–14 wk**. Consistent.

---

## S4a — milhouse

**Sizing** ⌂ `[Q2]`: **M** for the in-memory swap (3–5 wk in `[PLAN]` §3/S4), **L** for on-disk diffs
— **deferred**. `[Q2]`'s headline: *cheaper than assumed — the seam is not "only a type alias"; the
accessors already match milhouse's API and milhouse 0.9's deps match this workspace exactly.* The
in-memory swap is **a net deletion, mirroring Lighthouse PR #5533**.

**Note on the repo's own prior measurement.** `[Q2]` records that the CC-1H gate measured
"clone-dominant" and then applied a hashing-share rule — **it answered the wrong question**. Do not
cite CC-1H as evidence for or against this work; `S4a-07` is the measurement that matters.

### `S4a-01` · Add milhouse 0.9 · 1–1.5 pd / **3 pts**
**Acceptance** — `cargo tree` shows **no new transitive dependency** outside the existing workspace
set. `[Q2]` verifies milhouse 0.9's deps match this workspace exactly; if that has drifted since the
brief was written, stop and re-scope rather than absorbing new supply-chain surface (the workspace
sets `unsafe_code = "deny"` ✓ `Cargo.toml:38`).

### `S4a-02` · Swap the `List`-shaped `BeaconState` fields · 3–4 pd / **8 pts**
**Deps** `S2-A-10..12` — **P0-19/3 must be landed.** `StateCaches` derives `Clone`; if the ~80–100 MB
pubkey map is still on `BeaconState`, milhouse's O(1) clone is a lie and this whole sub-stage measures
no improvement (D4). `S2-A-12`'s recorded measurement is the precondition — check it before starting.

### `S4a-03` · Reconcile the accessors · 2.5–3.5 pd / **5 pts**
⌂ `[Q2]`: the accessors **already match** milhouse's API — this is reconciliation, not redesign. A
large diff here is a signal that something is being redesigned; stop and review.

### `S4a-04` · Delete the `StateField` discriminant table · 2–3 pd / **5 pts**
**This is the dangerous one, and it is why the ordering exists.** `[Q2]` §6 / P1-D/15: the
`BeaconState` schema is maintained in **four hand-synchronised places**, and this one —
the `StateField` **discriminant order** — yields a **wrong state root rather than a compile error**
when it drifts from the struct's field order.
**Acceptance** — the discriminant table no longer exists; `S4a-06`'s equivalence test is green across
the full vector corpus. Do not merge this issue on a subset.

### `S4a-05` · Delete the second schema place; **P2-B/6 rides** · 1.5–2.5 pd / **3 pts**
Discharges the remaining half of P1-D/15's schema-place reduction (4 → 2). **P2-B/6** —
`crates/crypto/src/domain.rs:52`, cc-crypto defines its own `SigningData` duplicating
`cc_types::containers::SigningData` — is dispositioned `patch @ S4` *with P1-D/15* and rides here.

### `S4a-06` · **State-root equivalence** · 2–3 pd / **5 pts**
**Acceptance (falsifiable)** — for every state in the committed vector corpus, the pre-swap and
post-swap `hash_tree_root` are **byte-identical**. Not "the suites still pass" — an explicit
root-vs-root comparison, because the failure mode this sub-stage introduces is a **wrong root with a
passing compile**.

### `S4a-07` · **Clone-cost measurement** · 1.5–2 pd / **3 pts**
**Acceptance** — the per-import state-clone cost measured against `S2-A-12`'s recorded post-P0-19/3
baseline, with absolute numbers in the exit note. `[PRD]` P1-D/10's premise is **~3–5 full 150–200 MB
`BeaconState` clones per import**; if the measured improvement is not material, that is a finding to
record, not a number to omit. `[PRD]` §7.5: an invented number is an anti-metric.

### `S4a-08` · `ADR-P1-04` superseded · 0.75–1 pd / **2 pts**
⌂ `[ARCH]` §10.4: `ADR-P1-04` (the cached state-root path on `BeaconState`,
`crates/types/src/state/accessors.rs:987`) is a **(b)** row marked *"milhouse changes this — supersede
at S4a."* `S1-B-16` wrote it `Status: proposed` + *"revisit at S4a"* to make the S2 entry gate
satisfiable (R-17). **This is the revisit.** If it is skipped, the S2 gate's `proposed` status was a
fudge rather than a mechanism.

---

## S4b — the Gloas schema

**Sizing** ⌂ `[Q5]` / `[PRD]` J-17: **M, re-sized from L.**

### The dispatch mechanism is settled, and the reason is not cost

`[Q5]` verified from Lighthouse source that `per_block_processing` uses **neither trait objects nor an
exhaustive per-fork enum match**, but **monotone capability predicates on `ForkName`** —
`if fork_name.gloas_enabled()` — inside **one** function generic over `EthSpec`, with variant-specific
fields arriving through superstruct **partial getters**. Because `X_enabled()` means *"fork X or
later"*, **each handler is written once and gated, never copied per fork**.

Grandine's per-fork `block_processing.rs` / `epoch_processing.rs` modules are the **literal** reading
of `[AS]` §8's *"per-fork STF dispatch"*. `[PRD]` J-17 records the departure and the deciding
argument, which **is not cost**:

> Per-fork modules copied N times **reintroduce the same N-copies synchronisation hazard as the
> four-place `BeaconState` schema** in P1-D/15 — a hazard this program exists to delete, and one where
> drift yields a **wrong state root rather than a compile error**. Adopting the literal reading of
> `[AS]` §8 would have made S4 both more expensive **and less safe**.

**Anyone who proposes per-fork modules during S4b is re-opening a decision recorded in `[PRD]` J-17
and must supersede it with an ADR, not with a PR.**

### `S4b-01` · `ForkName::Gloas` + the predicates · 1–1.5 pd / **3 pts**
⌂ `[Q5]`: **~10 lines of predicates on the existing ordered `ForkName`**
(`crates/types/src/fork.rs:17-59`, already an ordered enum). `GLOAS_FORK_EPOCH = FAR_FUTURE_EPOCH`
makes the addition **inert** — `[ARCH]` §9.1 notes S4's rollback is additive for exactly this reason.
**Depends on `S0-A-12`'s ordered `ChainConfig` fork accessors**, landed at S0 by ⟡ D-8 ch.1.

### `S4b-02` · The five hardcoded `ForkName::Fulu` call sites · 1.5–2 pd / **3 pts**
⌂ `[ARCH]` §5.3: **the seam already exists.** There are exactly **two decode chokepoints**, both
verified, both already rejecting non-Fulu:

| Chokepoint | Rejection test |
|---|---|
| `crates/types/src/block.rs:135` — `SignedBeaconBlock::from_ssz_bytes_with(fork_name, bytes)` | `block.rs:191` |
| `crates/types/src/state/mod.rs:223` — `BeaconState::from_ssz_bytes_with(fork_name, bytes)` | `mod.rs:304` |

**Five production call sites, every one passing a hardcoded `ForkName::Fulu`** ✓:
`services/chain/src/import.rs:1018` · `services/chain/src/checkpoint_sync.rs:1001,1023` ·
`services/storage/src/replay.rs:595,644`.

> *"Turning `ForkName::Fulu` into `config.fork_name_at_epoch(epoch)` at those five sites is the whole
> seam. That is a genuinely small change, and it is the strongest argument that S4 is tractable."*
> — `[ARCH]` §5.3

**Note** — the sixth site, the raw-`from_ssz_bytes` **bypass** at `services/chain/src/restore.rs:437`,
was closed at **S0** by `S0-A-02` (one rewrite closing the chokepoint bypass, the cache hydration, and
the whole "someone adds a fourth decode site" class) and the file was deleted at S2 by `S2-J-02`. If
either slipped, this issue is blocked — the chokepoint is not one while that site exists.

**Post-S2 line numbers will have moved.** These five are recorded against `4146791`; re-derive them
by symbol, not by line, at S4.

### `S4b-03` · `S4b-04` · `S4b-05` · superstruct on the **exactly 6** reshaped containers

**Combined** 8–11 pd / **18 pts** · ⌂ `[Q5]` §2 — *"`[AS]`'s '~6' is exact"*

| Container | Issue |
|---|---|
| `BeaconState` | `S4b-03` |
| `BeaconBlockBody` | `S4b-03` |
| `ExecutionPayload` | `S4b-04` |
| `ExecutionRequests` | `S4b-04` |
| `Attestation` | `S4b-05` |
| `IndexedAttestation` | `S4b-05` |

⌂ `[Q5]`: a **29-line superstruct attribute** against Grandine's ~1,650-line hand-written
`combined.rs`. `S4-Q-04` (compile-time cost) is the check on that trade; run it at wk 46 before
committing.

**`S4b-03` carries the −1/+9-field `BeaconState` edit** and is the issue the whole 4a→4b ordering
exists to protect. Under the planned branch it touches **two** synchronised places; under `S4-ALT`,
**four** — see the inversion branch.

### `S4b-06` · `upgrade_to_gloas` · 2–3 pd / **5 pts**
**Acceptance** — an `upgrade_to_gloas` vector suite green. Note `[PRD]` P1-D/16 records that
`upgrade_to_fulu`, transition/core and light_client suites are **run by no crate** today — that is an
*unowned spec-vector coverage hole* and `S4c-04`'s total-coverage enforcement is what closes it. Do
not add `upgrade_to_gloas` to the same unowned set.

### `S4b-07` · `S4b-08` · Per-fork STF dispatch by predicate · 5.5–7.5 pd / **13 pts**
Each handler written **once and gated**, never copied per fork. Variant-specific fields arrive through
superstruct **partial getters**.
**Acceptance** — a grep shows no per-fork module directory under `crates/state-transition`; the count
of `gloas_enabled()` call sites is small and each one is in a handler that also serves Fulu.

### `S4b-09` · P1-B/8 sibling check + P2-B/6 · 1–1.5 pd / **3 pts**
P1-B/8 (the hand-duplicated fork-version schedule walk across three validator files) was **discharged
at S0** by `S0-A-12` per `[PLAN]` C-11. This issue **verifies** that — a grep for duplicate schedule
walks returns the single `cc-types` implementation — and confirms its sibling **P1-B/6**
(`request_limits` in the codec) was discharged at **S3a** with `cc-wire` (`S3a-A-03`), not here.
Two rows that read as one; recorded so neither is lost.

---

## S4c — the new containers and enforcement

### `S4c-01` · `S4c-02` · The 13 new Gloas containers · 6–8 pd / **16 pts** (≈)
Types, SSZ, tree-hash, and vector coverage.
**Under-specified — flag before starting.** Neither source enumerates the 13 by name; `[Q5]` gives the
count. The first deliverable of `S4c-01` is the **enumerated list**, derived from the Gloas spec, with
each container mapped to its module. If the count is not 13, correct the source rather than padding
the list.

### `S4c-03` · **⟡ D-7** — `DataColumnSidecar(Fulu, Gloas)` · 2.5–3.5 pd / **5 pts**
**Omitted by both `[AS]` and `[PRD]`.** `[ARCH]` §5.4 ⟡ D-7: **the PeerDAS sidecar is *also*
fork-shaped at Gloas.** A container that both source documents' inventories miss is one nobody is
watching for.
**Deps `S4-Q-05`** — *does `specs/gloas/partial-columns/` change the DAS sidecar shape?* That spike
sets this issue's scope; run it at wk 46.
**Acceptance** — `DataColumnSidecar` is superstructed over `(Fulu, Gloas)`; a Gloas-shaped sidecar
round-trips; the Fulu shape is byte-unchanged.

### `S4c-04` · **Blocking total-coverage enforcement** · 3–4 pd / **8 pts**
⌂ `[ARCH]` §5.6 · **Discharges** the *unowned spec-vector coverage holes* half of P1-D/16.
**Acceptance (falsifiable)** — the suite **fails** unless **every on-disk vector is claimed or
skiplisted**. `upgrade_to_fulu`, transition/core and light_client are currently run by no crate; after
this issue, an unclaimed vector is a red build.
**Interacts with `S0a-B-07` (Q-9)** — if the existing coverage check only *reports*, this issue is
where it starts *failing*, and `S3b-W-01`'s Phase-1 clause 1 depended on that distinction.

### `S4c-05` · Gloas spec-vector suites green, both presets · 2.5–3.5 pd / **5 pts**
Both presets, **skiplist empty**, against `S4c-04`'s blocking enforcement.

### `S4c-06` · M3 ledger maintenance · 0.5 pd / **1 pt**
P1-D/10, P1-D/15, P1-D/16, P1-E/S4, P1-B/8 (verified), P2-B/6 recorded with commit SHAs.

**Convention.** This is the S4 instance of the ~0.5 pd stage-exit line item `S0-B-18` writes down
(`[PRD]` §5.0 / E0.9). Also append `S4` / `S4a` / `S4b` / `S4c` to `CLAIM_STAGES` in
`scripts/check-m3-discharged-by.sh`. P1-B/8 was already discharged at S0 (`[PLAN]` C-11) — verify
the SHA, do not blank it.

---

## Ledger rows discharged by S4

| Row | Disposition | Issue |
|---|---|---|
| P1-D/10 | `patch @ S4a`, **after P0-19/3** | `S4a-01` … `S4a-07` |
| P1-D/15 | `patch @ S4a` | `S4a-04`, `S4a-05`, `S4b-09` |
| P1-D/16 | `patch @ S4b`, sized **M** (was **L**) | `S4b-07`, `S4b-08`, `S4c-04` |
| P1-E/S4 | ordered 4a → 4b → 4c | the whole file |
| P2-B/6 | `patch @ S4` with P1-D/15 | `S4a-05` |
| P1-B/8 | `patch @ S4` — **discharged early at S0** (`[PLAN]` C-11) | verified by `S4b-09` |

---

## Drift and under-specification — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **Total.** `[PLAN]` sizes 4a at **3–5 wk** ⌂ `[Q2]`, 4b at **3–5 wk** ⌂ `[Q5]`/J-17, 4c at **3–4 wk** ≈ — i.e. 9–14 wk. Decomposed: 4a 14.25–20.5 pd (≈ 3.6–5.1 wk serial), 4b 19–26.5 pd, 4c 14.5–19.5 pd. **Consistent.** S4 is the phase where the plan's sizings survive decomposition best, because both `[Q2]` and `[Q5]` sized the work against reference-client source rather than by judgement. |
| 2 | **The 13 new containers are a count, not a list.** `S4c-01`'s first deliverable is the enumeration. Sizing 6–8 pd for 13 unenumerated containers is ≈ this decomposition's judgement and should be re-estimated once the list exists. |
| 3 | **`S4-ALT` costs +3–5 pd and is not in `[PLAN]`'s range.** If Q-3 comes back NO, S4 runs at the top of its window or past it. The one-hour spike is what keeps that from being discovered at wk 48. |
| 4 | **Post-S2 line numbers.** Every `file:line` in this file is recorded against `develop @ 4146791`. S1, S2 and S3a move most of these files between crates. Re-derive by symbol at S4; the line numbers are provenance, not addresses. |
| 5 | **`[PLAN]` schedules Q-4 and Q-5 at "wk 46", i.e. inside S3b.** They are engineering spikes run during a phase whose stream-A capacity is reserved for window defect fixes (`S3b-FIX`). Budget them explicitly or they compete with the fix queue. |
