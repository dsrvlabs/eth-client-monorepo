# S0 — correctness floor · wk 2–8

**Entry.** S0a exit (M7 = 0, `make ci` green).
**Contains.** 15 `patch @ S0` P0 rows (P0-08 landed in S0a) · 19 `patch @ S0` P1 rows ·
`cc-scheduler` + Loop B · the config-authority half of S4 pulled forward (⟡ D-8 ch.1) · the D-5
restore `block_on` patch · the P1-F/1 ADR (negative half) · the S0 exit falsifiers.

**Out of scope — stated so creep is visible** (`[PLAN]` §3/S0):

- **Any `deleted @ Sn` row.** P1-A/4, P1-A/22, P1-A/23, P1-B/9, P1-D/13 are `deleted @ S2` and are
  **not patched here**. P1-A/22 and P1-A/23 carry a *diagnostic* obligation instead — `S0-A-30` and
  `S0-A-31`. A diagnosis is not a patch and does not violate the `Disposition` rule.
- **Any structural move.** `[ARCH]` §9.1: *"Moves: nothing."*
- **Loop A** (gossip validation scheduling) — **L**, topology-dependent, `[ARCH]` §3.8 sequences it
  after S3. Loop B is the S0 half; treating them as one item pulls an L-sized topology-dependent
  piece into S0 (`[PLAN]` §9, "What only looks parallel").
- `drop_during_sync` shedding policy — `[ARCH]` B.1 does not adopt it.

**Four rows are patched here *and* deleted later. Both facts are true; neither is a reason to skip
the patch.** P0-07 and P1-A/27 (`deleted @ S2`), P0-13 (`deleted @ S2`), P0-15 / P1-B/11
(`deleted @ S1`). Each issue below states the deletion stage in its header.

Estimate provenance and the points scale are defined in [`s0a-gate-restoration.md`](s0a-gate-restoration.md).

---

## Issue index

### Stream A — consensus core

| Id | Title | pd | pts | Deps | Ledger |
|---|---|---:|---:|---|---|
| `S0-A-01` | P0-19/1 — `top_up_pubkey_cache` + 3 decode call sites | 1.5–2.5 | 3 | `S0a-A-01` | P0-19 |
| `S0-A-02` | P0-19/1b — `from_ssz_bytes_hydrated` chokepoint (same edit as the `from_ssz_bytes_with` route) | 1–1.5 | 3 | `S0-A-01` | P0-19 |
| `S0-A-03` | P0-19/2 — round-trip regression test, hand-fill **forbidden** | 1–1.5 | 3 | `S0-A-02` | P0-19 |
| `S0-A-04` | M13 gauge — `pubkey_cache_len` vs `validators_len` | 0.75–1.25 | 2 | `S0-A-01` | M13 |
| `S0-A-05` | **Spike Q-7** — audit `preset.rs` for other config-scoped values | 1–2 | 3 | — | P0-02 |
| `S0-A-06` | Q-6 — `SECONDS_PER_SLOT` vs `SLOT_DURATION_MS` loader defaulting | 0.5–1 | 2 | `S0-A-05` | J-12 |
| `S0-A-07` | P0-02(a) — add the 4 missing `ChainConfig` fields + `max_blobs_per_block_electra` | 2–3 | 5 | `S0-A-05` | P0-02, P2-A/8 |
| `S0-A-08` | P0-02(b) — the loader half: captured-and-WARN-logged unknown-key map | 1.5–2 | 3 | `S0-A-07` | P0-02 |
| `S0-A-09` | P0-02(c) — thread `&ChainConfig`, **delete `pub mod network`** | 3–4 | 8 | `S0-A-07` | P0-02, M6b |
| `S0-A-10` | P2-B/3 — `apply_deposit` reimplements `get_validator_index_by_pubkey` | 0.5–1 | 2 | `S0-A-09` | P2-B/3 |
| `S0-A-11` | E0.3 — a config fixture that **differs from mainnet on all five** constants | 1.5–2 | 3 | `S0-A-09` | R-9 |
| `S0-A-12` | D-8 ch.1 — ordered `ChainConfig` fork accessors + delete the 5 duplicated schedule walks | 2–3 | 5 | `S0-A-09` | P1-B/8, P1-D/15(part) |
| `S0-A-13` | Loop B/1 — `cc-scheduler` substrate | 1.5–2.5 | 3 | — | P1-D/09(B) |
| `S0-A-14` | Loop B/2 — `tick` lane, never-shed + **P0-12's second half** | 1.5–2.5 | 3 | `S0-A-13` | P0-12 |
| `S0-A-15` | Loop B/3 — `import` + `query_p0` lanes | 1–2 | 3 | `S0-A-14` | P1-D/09(B) |
| `S0-A-16` | Loop B/4 — `query_p1` + `attestation` (LIFO) lanes | 1–2 | 3 | `S0-A-15` | P1-D/09(B) |
| `S0-A-17` | Loop B/5 — **cutover**: delete the single channel, rewrite the 3 `capacity()` gauges and the backpressure tests (one PR) | 1.5–2.5 | 3 | `S0-A-16` | P0-12 |
| `S0-A-18` | Loop B/6 — ⟡ D-4 flush-trigger test (`HEAD` for `S+1` within one slot) | 0.75–1.25 | 2 | `S0-A-17` | — |
| `S0-A-19` | P1-D/14 — fire the shutdown watch | 0.5–1 | 2 | `S0-A-14` | P1-D/14 |
| `S0-A-20` | P0-09 — grow the fork-choice vote and balance tables in `integrate_block` | 2–3 | 5 | — | P0-09 |
| `S0-A-21` | P0-10 — wire `ProtoArray::prune` to the FINALIZED event | 1–2 | 3 | — | P0-10 |
| `S0-A-22` | P1-A/19 — `update_latest_messages` partial mutation on OOB index | 0.5–1 | 2 | — | P1-A/19 |
| `S0-A-23` | P1-A/20 — justified `CheckpointContext` overwritten from the wrong state | 1–1.5 | 3 | — | P1-A/20 |
| `S0-A-24` | P1-A/21 — `get_proposer_head`'s invented equivocation branch | 1–1.5 | 3 | — | P1-A/21 |
| `S0-A-25` | P0-03 — `CoreConfig::default()` must not ship `NoVerification` | 1–1.5 | 3 | — | P0-03, M5 |
| `S0-A-26` | P0-11 — proposer-lookahead fallback returns `None`, not a wrong proposer | 1–2 | 3 | — | P0-11 |
| `S0-A-27` | P0-15 — hard deadlines on **every** chain→engine RPC, routed to the deferral | 2–3 | 5 | — | P0-15, P1-B/11 |
| `S0-A-28` | ⟡ D-5 — the restore-path `block_on` panic (`spawn_blocking`) | 0.5–1 | 2 | — | P0-15(tail) |
| `S0-A-29` | **ADR-R-06** — does the restore/replay path call the engine at all? | 0.5 | 1 | `S0-A-28` | ADR-R-06 |
| `S0-A-30` | **E0.4** — R-11 executed: decode the Hoodi anchor, `process_block`, ± the top-up | 1.5–2 | 3 | `S0a-A-01`, `S0-A-03` | R-11, P0-19 |
| `S0-A-31` | **R-13 observation (b)** — an instrumented real restore run: *where* does it fail? | 1.5–2 | 3 | `S0-A-30` | R-13, P1-A/22, /23 |
| `S0-A-32` | **E0.5** — P0-09 falsifier: post-anchor validator against a grown registry | 1–1.5 | 3 | `S0-A-20`, `S0a-A-01` | R-16 |
| `S0-A-33` | P1-A/18 — sync-aggregate signature verified under `NoVerification` | 0.75–1.25 | 2 | `S0-A-25` | P1-A/18 |
| `S0-A-34` | P1-B/10 — `block_to_epoch` catch-all relabels every error | 0.5–1 | 2 | — | P1-B/10 |
| `S0-A-35` | P1-B/12 — `SignatureSet` batch bypasses the `BLS_VERIFY_COUNT` contract | 1–1.5 | 3 | — | P1-B/12 |
| | **Stream A total** | **40.75–64.75** | **107** | | |

### Stream B — edge & platform

| Id | Title | pd | pts | Deps | Ledger |
|---|---|---:|---:|---|---|
| `S0-B-01` | P0-01 — drop the `0.0.0.0` publishes `9001–9006`; bind `9101–9106` to loopback | 0.5–1 | 2 | — | P0-01, M4 |
| `S0-B-02` | P0-07 + P1-A/27 — the three missing URI overrides + the identity mount | 0.5–1 | 2 | — | P0-07, P1-A/27 |
| `S0-B-03` | P1-A/29 — `devnet/faults.sh` pinned to the wrong compose file | 0.25–0.5 | 1 | — | P1-A/29 |
| `S0-B-04` | P0-04 — `BeaconBlocksByRange` v2 encodes 24 bytes | 1–1.5 | 3 | — | P0-04 |
| `S0-B-05` | P0-05 — `BlocksByRoot` v2 encodes bare `32·n` roots | 1–1.5 | 3 | — | P0-05 |
| `S0-B-06` | The three duplicated request-limit tables reconciled | 0.75–1.25 | 2 | `S0-B-04`, `S0-B-05` | P0-04, P0-05 |
| `S0-B-07` | P0-06 — restore the wire prober's **independence** (separate PR, from spec) | 0.5–1 | 2 | — | P0-06 |
| `S0-B-08` | P0-13 — end the write-behind session with `Reconnect { cursor: last_flushed }` | 2–3 | 5 | — | P0-13 |
| `S0-B-09` | P0-14 — `rewrite_from_head` deletes canonical rows for vacated slots | 2–3 | 5 | — | P0-14 |
| `S0-B-10` | P1-A/1 — `PutBackfillBatch` anchor/frontier binding, progress not optional | 2–3 | 5 | — | P1-A/1 |
| `S0-B-11` | P1-A/5 — CC-4A block floor read from a build-machine fixture path | 0.75–1.25 | 2 | — | P1-A/5 |
| `S0-B-12` | P1-B/1 — `observe_lag` runs on the already-reset accumulator | 0.25–0.75 | 1 | — | P1-B/1 |
| `S0-B-13` | P1-A/8, /9, /10 — gossip validation and registry rows | 3.5–5 | 8 | — | P1-A/8, /9, /10 |
| `S0-B-14` | P1-F/1 — **ADR-R-05**, the backend-independent half | 0.5–1 | 2 | `S0a-B-10` | P1-F/1 |
| `S0-B-15` | P1-A/12 — empty in-window results answered with error code 3 | 1–1.5 | 3 | — | P1-A/12 |
| `S0-B-16` | P1-A/13 — column by-range chunks served in request order | 1.5–2 | 3 | — | P1-A/13 |
| `S0-B-17` | **Early P2-E triage** — rows 8 and 31 (and 1, 6, 21) | 1–1.5 | 3 | — | M12 (part) |
| `S0-B-18` | **E0.9** — create the M3 `Discharged by` artifact and its maintenance convention | 0.75–1.25 | 2 | — | M3 |
| `S0-B-19` | **E0.7** — the §9.0 A/B **baseline** run, all six binaries | 1.5–2 | 3 | all patches | E0.7 |
| `S0-B-20` | **E0.8** — off-host port scan from a second host, recorded | 0.25–0.75 | 1 | `S0-B-01` | M4 |
| | **Stream B total** | **21.5–33.75** | **58** | | |

**Phase totals.** **62.25–98.5 pd · 165 pts.** Parallel duration at A-1/A-2 (4 effective
pd/engineer-week): stream A binds at **40.75–64.75 pd → 10.2–16.2 wk**. See the drift note.

---

## Stream A issues

### `S0-A-01` · P0-19/1 — `top_up_pubkey_cache` + 3 decode call sites

**Stream** A · **Est** 1.5–2.5 pd / **3 pts** · ⌂ `[Q3]` Change 1 = **S** · **Deps** `S0a-A-01` ·
**Discharges** P0-19 (part)

**This is the first item in stream A, and it is not reorderable.** R-12: P0-19 is the only P0 that is
both *firing on the committed tree today* and *invisible to every existing signal, log line and
healthcheck*. It fires on the second boot of any node that has taken a snapshot — no gossip required.

**Touch points**
- `crates/types/src/state/mod.rs:106` — `caches: StateCaches<P>` carries
  `#[ssz(skip_serializing, skip_deserializing)]`, so every decoded state starts with an empty
  `PubkeyIndexMap`
- `crates/state-transition/src/block/sync_aggregate.rs:125-129` — the `.ok_or(BlockError::CachePoisoned)?`
  with **no linear-scan fallback**
- call site 1: `services/chain/src/restore.rs:437` (decode) → `:525` (`on_block`)
- call site 2: `services/storage/src/replay.rs:644` (decode) → `:572` (`state_transition(`, the sole
  occurrence in that file — the production call, not a test)
- call site 3: `services/chain/src/checkpoint_sync.rs:1023`

**Design** — `top_up_pubkey_cache` fills from the validator registry: append-only, idempotent, O(V)
once per decode.

**Acceptance (falsifiable)**
1. All three decode sites call the top-up before any `state_transition` / `on_block` reachable from
   them.
2. Calling the top-up twice on the same state is a no-op (idempotence asserted by test).
3. `linear_scan_count` is **not** the instrument here — `process_sync_aggregate` never scans, it
   errors (`[Q3]` §5). Do not add an assertion that depends on it.

---

### `S0-A-02` · P0-19/1b — `from_ssz_bytes_hydrated` chokepoint

**Stream** A · **Est** 1–1.5 pd / **3 pts** · ⌂ `[Q3]` Change 1b = **S** (~1 day) ·
**Deps** `S0-A-01` · **Discharges** P0-19 (part)

Changes the class from *fixed three times* to *unrepresentable*. `[ARCH]` §9.1 records that ⟡ D-15's
chokepoint and `[Q5]` §5.3's required fix (route `restore.rs:437` through `from_ssz_bytes_with`) are
**the same edit at the same line** — schedule them as one task, which is this issue.

**Touch points**
- `crates/types/src/state/mod.rs` — add `BeaconState::from_ssz_bytes_hydrated`
- `services/chain/src/restore.rs:437` — replace the raw `BeaconState::<P>::from_ssz_bytes(...)`; this
  one line **also** closes the fork-chokepoint bypass (`[Q5]` §4.1: the chokepoint is not one while
  this site exists)

**Acceptance**
1. Every production decode of a `BeaconState` goes through the hydrated constructor. A grep for
   `BeaconState::<.*>::from_ssz_bytes(` outside `#[cfg(test)]` returns 0.
2. The raw constructor is either private or `#[doc(hidden)]` with a comment naming this issue.

---

### `S0-A-03` · P0-19/2 — round-trip regression test, hand-fill forbidden

**Stream** A · **Est** 1–1.5 pd / **3 pts** · ⌂ `[Q3]` Change 2 = **S** · **Deps** `S0-A-02`

`[PRD]` §5.1.2/3: **eight** existing harnesses hand-fill the cache that production never fills —
eight independent authors each patched around the requirement locally. That is why no test catches
it, and it is why this test must be *forbidden* to hand-fill.

**Acceptance (falsifiable)**
1. The test constructs its state **only** by SSZ decode, and the positive assertion is that
   `process_block` succeeds.
2. The **negative** assertion: with the top-up removed (feature flag, or a direct call to the raw
   decoder inside the test), the same block yields `BlockError::CachePoisoned`. A test with only the
   positive half does not discharge this issue.
3. A reviewer can confirm by grep that the test body contains no cache-population call.

---

### `S0-A-04` · M13 gauge — `pubkey_cache_len` vs `validators_len`

**Stream** A · **Est** 0.75–1.25 pd / **2 pts** (≈) · **Deps** `S0-A-01` · **Discharges** M13

Same species as M8: a failure the node currently cannot report. `[PRD]` R-12 names M13 as the
detector if P0-19 somehow slips.

**Acceptance** — two gauges emitted on every state the core imports against; an alert rule fires when
`pubkey_cache_len < validators_len`. The alert rule ships with the gauge, not later.

- [x] Two gauges (`cc_chain_pubkey_cache_len`, `cc_chain_validators_len`) emitted on every state the core imports against (live parent, restore snapshot, checkpoint spawn).
- [x] Alert rule ships with the gauges and fires when `pubkey_cache_len < validators_len`.

---

### `S0-A-05` · **Spike Q-7** — audit `preset.rs` for other config-scoped values

**Stream** A · **Est** 1–2 pd / **3 pts** · ⌂ `[ARCH]` B.2 = **S** · **This is the first task of
P0-02**, per R-9's own text · **Blocks** `S0-A-07`

**Question.** `crates/types/src/preset.rs` was never audited. P0-02's class is defined by the
*mismatch* between a value's spec scope (`config`) and its resolution key (compile-time preset). Are
there **other** values that landed in the preset by the same mistake? Completeness of the
`pub mod network` deletion depends on the answer.

**Deliverable** — a table of every constant in `preset.rs`, its spec home (`configs/*.yaml` vs
`presets/*/*.yaml`), and whether `ChainConfig` parses it. Any row that is config-scoped and
preset-resolved is added to `S0-A-07`'s field list **and to this issue's estimate**, in writing.

**Answered 2026-08-15** in [`spike-notes.md` § Q-7](spike-notes.md). Only
`MAX_BLOBS_PER_BLOCK_BASE` is config-scoped and preset-resolved; it is already
`S0-A-07`'s `max_blobs_per_block_electra`. **No additional fields. This issue's
estimate stays 1–2 pd / 3 pts.**

---

### `S0-A-06` · Q-6 — `SECONDS_PER_SLOT` vs `SLOT_DURATION_MS`

**Stream** A · **Est** 0.5–1 pd / **2 pts** · ⌂ `[ARCH]` B.2 = **S**, routed into the S0 config work
rather than run as a standalone spike · **Deps** `S0-A-05`

Today's upstream `configs/mainnet.yaml` carries `SLOT_DURATION_MS: 12000` and **no
`SECONDS_PER_SLOT`**, while `RawChainConfig.seconds_per_slot` has no `#[serde(default)]` — so
**loading the current upstream mainnet config fails to parse** (`[Q5]` §1.4). The repo's Hoodi fixture
carries both keys, which is why nothing has caught it.

**Open decision, explicitly the team lead's call (`[PRD]` J-12).** This is a *loader-defaulting* bug,
not a preset-keying one, so it is deliberately **not folded into P0-02** and is **not tiered**. It may
warrant its own P0. This issue answers the question that decides it: *is `SECONDS_PER_SLOT` formally
removed upstream, or merely absent from `configs/mainnet.yaml`?* — which determines whether the fix is
"accept both keys" or "migrate".

**Deliverable** — the upstream answer with a URL, a recommendation (accept-both vs migrate), and a
one-line escalation to the lead with a proposed tier. Do not silently create or omit the P0.

**Answered 2026-08-15** in [`spike-notes.md` § Q-6](spike-notes.md). Formally removed from
consensus-specs YAML ([#4926](https://github.com/ethereum/consensus-specs/pull/4926)); both keys
still exist in the spec family (eth-clients Hoodi/mainnet, Beacon API). **Accept both.** Escalation:
**no new P0** — this loader default discharges the parse failure; residual ms migrate is P2.

- [x] Upstream answer with URL: formally removed from consensus-specs YAML in #4926; both keys still exist in the spec family.
- [x] Recommendation: accept-both (not migrate). Escalation: no new P0.

---

### `S0-A-07` · P0-02(a) — add the four missing `ChainConfig` fields

**Stream** A · **Est** 2–3 pd / **5 pts** · ⌂ `[Q5]` §1 sizes the **whole class** as **S** (2–4 d);
this decomposition splits that across `S0-A-07..12` and judges the class larger — see the drift note ·
**Deps** `S0-A-05` · **Discharges** P0-02 (part), **P2-A/8** (`[PRD]` J-11)

All five functions in `pub mod network` resolve a **config**-scoped value from the **compile-time
preset** key `P::NAME`. `ChainConfig` parses exactly one of the five.

| Function | Parsed today? | Impact on a non-mainnet-preset network |
|---|:--:|---|
| `genesis_fork_version` | ✅ `config.rs:144` | **LIVE.** Hoodi GVF `0x10000910`; preset returns `0x00000000` — every deposit PoP verifies under the wrong domain and is silently dropped |
| `churn_limit_quotient` | ❌ | latent, consensus-critical — diverges on validator-set changes every epoch |
| `min_per_epoch_churn_limit_electra` | ❌ | latent, consensus-critical |
| `max_per_epoch_activation_exit_churn_limit` | ❌ | latent, consensus-critical |
| `shard_committee_period` | ❌ | latent — gates voluntary-exit eligibility |

**Touch points**
- `crates/types/src/config.rs:144` (`genesis_fork_version`, the one already parsed), `:286-309`
  (`RawChainConfig`, 19 fields)
- `crates/types/src/config.rs:101` — `MAX_BLOBS_PER_BLOCK_ELECTRA` compiled in via
  `P::MAX_BLOBS_PER_BLOCK_BASE`. **This is P2-A/8, the same class by a different mechanism**, and
  `[Q5]` §1.5 step 1 lists `max_blobs_per_block_electra` among the fields P0-02 must add anyway.
  Adding it here discharges P2-A/8.
- **Q-7 (`S0-A-05`, 2026-08-15):** every other `Preset` const is preset-scoped, derived, or an
  Altair protocol constant (`SYNC_COMMITTEE_SUBNET_COUNT`). **No additional fields.** Estimate
  stays **2–3 pd / 5 pts**. Table: [`spike-notes.md` § Q-7](spike-notes.md).

**Acceptance**
1. Each new field carries `#[serde(default = …)]` at the **mainnet** value, so no existing fixture
   breaks (R-9's mitigation, stated as a hard requirement).
2. `crates/types/tests/fixtures/hoodi-config.yaml` still loads unchanged.
3. `P2-A/8` is annotated `discharged by P0-02` in the M3 ledger (`S0-B-18`), not deleted — it remains
   a distinct `[RV]` finding with its own file:line, and P2-A stays at 8 (`[PRD]` J-11).

---

### `S0-A-08` · P0-02(b) — the loader half

**Stream** A · **Est** 1.5–2 pd / **3 pts** · ⌂ `[Q5]` §1.3 · **Deps** `S0-A-07`

`RawChainConfig` (`crates/types/src/config.rs:286-309`) declares 19 fields with **no**
`#[serde(deny_unknown_fields)]`, so these keys are **read from disk and discarded**. Both halves are
in P0-02's scope: the reader takes the value from the wrong source, and the loader does not take it
from the right one.

**Design constraint.** Prefer an **explicit captured-and-WARN-logged unknown-key map** over bare
`deny_unknown_fields`, which would break on every upstream config that adds a Heze key. Today's
upstream `configs/mainnet.yaml` already declares `GLOAS_FORK_*` and `HEZE_FORK_*`, which `ChainConfig`
has no fields for.

**Acceptance**
1. Loading a config with an unknown key succeeds and emits one WARN naming the key.
2. A test asserts that a config carrying `CHURN_LIMIT_QUOTIENT` produces a `ChainConfig` whose field
   holds that value — i.e. the key is no longer discarded. This is the direct falsifier for the
   loader half.

---

### `S0-A-09` · P0-02(c) — thread `&ChainConfig`, delete `pub mod network`

**Stream** A · **Est** 3–4 pd / **8 pts** · ≈ this decomposition (⌂ `[Q5]` sizes the class **S**;
this slice alone is ~20 call sites) · **Deps** `S0-A-07` · **Discharges** P0-02, **M6b**

**Cannot be split below the module deletion.** Deleting `pub mod network` is the forcing function —
it turns "omitting the config" into a compile error. A partial edit leaves the module half-deleted
and the compile-error property unearned, so this stays a single ~3–4 pd issue rather than faked
granularity.

**Touch points**
- `crates/state-transition/src/helpers/constants.rs:128-172` — the five functions; **delete the
  module**
- `crates/state-transition/src/block/operations/deposit.rs:42` (the `cc_crypto::compute_domain(` call)
  / `:44` (the offending argument `network::genesis_fork_version::<P>()`) — `[PRD]` J-2 records both
  line numbers as the same three-line expression
- `crates/state-transition/src/epoch/pending_deposits.rs` — the same fix
- the ~20 other `constants::network::` call sites

**The documented-as-intentional trap (R-9).** `deposit.rs:29-30` carries a doc comment stating the
preset's `GENESIS_FORK_VERSION` is used *"so minimal vectors verify correctly"* ✓ — the bug is
documented as a feature, and a naive fix breaks the minimal-preset spec vectors. The fix is **config
plumbing, not a constant swap**.

**Acceptance (falsifiable)**
1. `grep -c 'constants::network::'` over the tree = **0** (E0.2 / M6b).
2. Omitting `&ChainConfig` at any of the touched call sites is a **compile error**, not a runtime
   fallback.
3. **Both** preset vector suites green — mainnet and minimal.
4. `pub mod network` no longer exists.

- [x] `constants::network::` is 0 in `*.rs`; `pub mod network` is deleted.
- [x] Omitting `&ChainConfig` at a touched call site is a compile error.
- [x] Mainnet and minimal vector suites stay green.

---

### `S0-A-10` · P2-B/3 — `apply_deposit` reimplements `get_validator_index_by_pubkey`

**Stream** A · **Est** 0.5–1 pd / **2 pts** (≈) · **Deps** `S0-A-09` · **Discharges** P2-B/3

Rides with P0-02 per `[PRD]` §5.3.1. Bypasses scan accounting.
**Touch point** — `crates/state-transition/src/block/operations/deposit.rs:119`

- [x] `apply_deposit` uses `get_validator_index_by_pubkey` (scan accounting / cache backfill).

---

### `S0-A-11` · E0.3 — a config fixture that differs from mainnet on all five

**Stream** A · **Est** 1.5–2 pd / **3 pts** · **Deps** `S0-A-09` · **Discharges** R-9's mitigation,
S0 exit criterion E0.3

**Why this is a separate issue and not an acceptance line on `S0-A-09`.** `[Q5]` §1.2 verified against
`crates/types/tests/fixtures/hoodi-config.yaml` that Hoodi happens to use mainnet's values for four
of the five constants. So **a naive fix passes on Hoodi and diverges on a customising devnet** — the
venue where it would be caught is the one nobody runs. Testing only against `hoodi-config.yaml` is
the trap, not the test.

**Acceptance (falsifiable)**
1. A committed fixture config differs from mainnet on **all five** P0-02 constants.
2. Both preset vector suites run green against it in CI.
3. A deliberately reverted `S0-A-09` hunk makes at least one assertion against that fixture **fail**.
   Demonstrated once in the PR. Without this, the fixture proves nothing.

- [x] Fixture differs from mainnet on all five P0-02 constants.
- [x] Official vector suites stay green (official per-preset configs).
- [x] Production-path tests against the fixture fail if values are read from the deleted preset table.

---

### `S0-A-12` · ⟡ D-8 ch.1 — ordered `ChainConfig` fork accessors, delete the five schedule walks

**Stream** A · **Est** 2–3 pd / **5 pts** · ⌂ `[PLAN]` C-3 / `[ARCH]` §5.5 · **Deps** `S0-A-09` ·
**Discharges** **P1-B/8** (`patch @ S4`, discharged here per `[PLAN]` C-11), part of P1-D/15

The S4 config-authority work pulled forward. P0-02 removes the preset-keying; this adds the ordered
accessors and deletes the duplicated walks, in the same edit — strictly cheaper than doing it twice.

**Design** — model on Lighthouse's `ChainSpec::fork_name_at_epoch`
(`consensus/types/src/core/chain_spec.rs`), implemented as a **descending data table** so adding a
fork is one row rather than an if/else edit.

**Touch points**
- `crates/types/src/fork.rs:17-59` — already an ordered enum
- `services/p2p/src/gossip/validate/column.rs:707` — the fork-version schedule walk hand-duplicated
  across **three validator files** (P1-B/8); `[Q5]` §1.5 counts **five** duplicate walks in total,
  not three

**Acceptance**
1. Exactly one fork-schedule walk exists outside `cc-types`; the count is asserted by a grep-based
   check added to the `S0a-B-04` gate list.
2. P1-B/8 is recorded in the M3 ledger as discharged **at S0**, with a note that its `@ S4`
   disposition was superseded by C-11 — so a reader tracking `@ S4` does not lose it.
3. P1-B/6 (`request_limits` duplicated in the codec) is **unaffected** and stays at S3a with
   `cc-wire`. Do not fold it in.

- [x] Ordered `ChainConfig` fork accessors; five duplicated walks deleted.
- [x] Grep gate asserts exactly one remaining walk outside `cc-types`.
- [x] P1-B/6 not folded.

- [x] P1-B/8 discharged at S0 per `[PLAN]` C-11 (`@ S4` superseded). M3 ledger not yet created
      (`S0-B-18`); recorded here. P1-B/6 `request_limits` stays S3a.

---

### `S0-A-13` … `S0-A-19` · Loop B — five lanes + `cc-scheduler`

**Combined estimate** 8–14 pd / **19 pts** · ⌂ `[ARCH]` §3.8 sizes Loop B **M** (1–2 wk **including
test rewrites**) · **Stream** A · **Discharges** P0-12, P1-D/09 (Loop B half), P1-D/14

Topology-neutral: survives S1/S2/S3 unchanged, needs no new transport, and fixes a
consensus-correctness bug rather than a throughput number. `max_workers` stays **1** — there is one
`Store` on one thread by design (ADR-P1-09 ✓ `core.rs:1`, `:469`); introducing workers here would be
a rewrite, not a refactor.

| # | Lane | Variants | Queue | Depth | Never shed? |
|---|---|---|---|---|---|
| 1 | `tick` | `SlotTick`, `Shutdown` | FIFO | 4 | **yes** |
| 2 | `import` | `ImportBlock`, `ImportBlockGossip`, `DataAvailable` | FIFO | 64 | no (policy A) |
| 3 | `query_p0` | `Query{Head, IsOptimistic}` + head probes | FIFO | 64 | no |
| 4 | `attestation` | `ApplyAttestations` | **LIFO** | sized from active validators | no — evict oldest |
| 5 | `query_p1` | `Query{CommitteeShuffling, ValidatorPubkeys, ValidatorRecords, CanonicalRoots}` | FIFO | 64 | no |

**Ordering is strict: A-13 → A-14 → A-15 → A-16 → A-17 → A-18.** A-19 may land any time after A-14.

#### `S0-A-13` — `cc-scheduler` substrate · 1.5–2.5 pd / 3 pts
The shared substrate per `[ARCH]` §3.1: manager, lane registry, queue-type and depth configuration,
and the **flat first-match-wins selection chain**. *The order of that chain is the policy* — encode it
as data, not as control flow, so a reviewer can diff it. No consumers wired in this issue.

#### `S0-A-14` — `tick` lane + **P0-12's second half** · 1.5–2.5 pd / 3 pts
`store.time` advances **only** via a `thread::sleep`-driven `SlotTick` that is phase-unaligned,
drifts, and is **silently dropped** when the 64-deep command channel is full ✓
(`services/chain/src/core.rs:527-532` — this is overflow policy **D**, the one whose caller-visible
signal is *none*).

**The lane is necessary but not sufficient.** P0-12 also requires calling
`on_tick(store, wall_clock_now)` at the top of each import, or aligning the ticker to genesis-derived
boundaries, so the clock is never *only* as fresh as the last delivered tick. **Do both.** Allow
`MAXIMUM_GOSSIP_CLOCK_DISPARITY`.

**Acceptance** — `core::slot_tick_is_never_shed` (the `[ARCH]` §2.2 policy-D conformance test) passes;
a test saturating every other lane still advances `store.time` within one slot; a block gossiped early
in its slot is not IGNOREd as `future_slot`.

#### `S0-A-15` — `import` + `query_p0` lanes · 1–2 pd / 3 pts
`DataAvailable` **stays in the `import` lane, not above it** — it re-drives a parked block, so
promoting it gains nothing and risks starving imports (`[q1]` §2.3). `query_p0` mirrors Lighthouse's
`ApiRequestP0`: a `GetHead` behind three block imports currently waits for three state transitions.

- [x] Import lane (FIFO) carries ImportBlock / ImportBlockGossip / DataAvailable.
- [x] query_p0 lane serves GetHead / IsOptimistic / StoreClock without waiting on mixed p1.
- [x] Shutdown rides the never-shed tick lane.

#### `S0-A-16` — `query_p1` + `attestation` lanes · 1–2 pd / 3 pts
`attestation` is **LIFO**, sized from active validators, evict-oldest.

- [x] `query_p1` FIFO-64 serving-read lane; `attestation` LIFO evict-oldest.
- [x] Dropping the attestation receiver fails in-flight waiters.

- [x] query_p1 lane (FIFO 64) serves CommitteeShuffling / ValidatorPubkeys / ValidatorRecords / CanonicalRoots.
- [x] attestation lane is LIFO, sized from active validators, evict-oldest.

#### `S0-A-17` — **cutover** · 1.5–2.5 pd / 3 pts
Delete the single 64-deep command channel and route every producer through the manager.

**This issue cannot be split, and two rewrites must land in the same PR** (`[ARCH]` §3.2's migration
hazard, `[q1]` §4): the queue-depth gauges derived from `cmd_tx.capacity()` ✓
(`core.rs:254-256`, `:290-292`, `:318-320`) — a five-lane manager has no single `capacity()` — and the
backpressure tests at `services/chain/tests/import_path.rs`. A PR that lands the manager and leaves
either behind ships a metric that reads a channel that no longer exists.

#### `S0-A-18` — ⟡ D-4 flush-trigger test · 0.75–1.25 pd / 2 pts
Closes `[q1]` §5's explicitly-open item. Re-ordering *commands* is safe for the `SubscribeEvents`
cursor — `seq` is assigned monotonically by the single-threaded events task at the point of receipt ✓
(`services/chain/src/events/mod.rs:3-13`), so no cursor becomes invalid. **The caveat is the flush
trigger, not the cursor:** promoting `query_p0` above `attestation` can delay an
`ApplyAttestations`-triggered head recompute, changing *when* the `HEAD` for `S+1` is emitted and
therefore the commit-unit boundary ✓ (`write_behind.rs:64-70`).

**Acceptance** — a test asserting that under a **saturated `query_p1` lane** the `HEAD`-for-`S+1`
event still arrives within one slot, so `commit_max_latency` (4 s) is not silently promoted from a
backstop to the primary trigger.

#### `S0-A-19` — P1-D/14, fire the shutdown watch · 0.5–1 pd / 2 pts
`Shutdown` rides the never-shed `tick` lane. Storage's shutdown watch never fires; biased selects
hot-spin on `watch` Err; drain-timeout exits 0. **Only the "fire the watch" half is S0**; the rest of
P1-D/14 is `deleted @ S2`.

- [x] SIGTERM/SIGINT fire the storage shutdown watch (`send(true)`) so writer / write-behind / replay / prune leave their selects.

---

### `S0-A-20` · P0-09 — grow the fork-choice vote and balance tables

**Stream** A · **Est** 2–3 pd / **5 pts** (≈ — needs a grown-registry test fixture) ·
**Discharges** P0-09

The tables are sized to the **anchor** registry and never grown. Any attestation naming a validator
activated after checkpoint sync hits non-deferrable `ValidatorIndexOutOfRange` and the aggregate is
dropped; `justified_balances_snapshot` truncates to stale capacity. On mainnet the registry grows
every epoch, so **LMD weights diverge within hours**.

**Touch points** (recovered by grep — `[PRD]` P0-10/P0-09 cite the crate, `[RV]` cites the line)
- `crates/fork-choice/src/on_block.rs:447` — `integrate_block`'s post-state path; the fix site
- `crates/fork-choice/src/on_block.rs:170` — the **anchor-only** `store.resize_votes(n)`, sized from
  `justified_balances.len().max(anchor_state.validators_len())`. This is the one production call, and
  it runs once at the anchor
- `crates/fork-choice/src/store.rs:601` — `pub fn resize_votes`

**Acceptance**
1. `integrate_block` grows votes and balances from the **trusted post-state**.
2. A test with a registry grown past the anchor size imports an attestation naming a post-anchor
   validator without `ValidatorIndexOutOfRange`.
3. `justified_balances_snapshot` length tracks the registry, asserted after a growth step.

The **falsifier** for this row is `S0-A-32` (E0.5), not this issue's unit test — see R-16.

---

### `S0-A-21` · P0-10 — wire `ProtoArray::prune` to the FINALIZED event

**Stream** A · **Est** 1–2 pd / **3 pts** (≈) · **Discharges** P0-10

`[PRD]` P0-10's evidence column names only `[AS] §4/07; crates/fork-choice` — **no file:line**.
Recovered here by grep:

**Touch points**
- `crates/fork-choice/src/proto_array.rs:325` — `pub fn prune(&mut self, finalized_root: Root)`; the
  only other reference in the tree is the unit test at `:1046`, i.e. **no production caller**
- `services/chain/src/events/mod.rs:206` — `kind: EventKind::FinalizedCheckpoint`; `:183` documents
  the payload shape. This is the event to hang the call off
- related but out of scope: `crates/fork-choice/src/da_seam.rs:162` `prune_except` — a different
  prune, do not conflate

**Acceptance** — a test drives two finalizations and asserts the proto-array node count **decreases**;
without a call site, fork-choice memory grows without bound.

- [x] `ProtoArray::prune` is called from the FINALIZED publish path after the REORG walk.
- [x] Two-finalization tests assert the proto-array node count decreases.

---

### `S0-A-22` · P1-A/19 — `update_latest_messages` partial mutation on OOB index
**Est** 0.5–1 pd / **2 pts** (≈) · **Touch** `crates/fork-choice/src/on_attestation.rs:284`
Partially mutates vote trackers on an out-of-bounds index and skips the counter bump.
**Acceptance** — the mutation is all-or-nothing; a test with one OOB index in a multi-index attestation
leaves *no* tracker mutated and bumps the counter.

- [x] Mutation is all-or-nothing, including when an OOB index is already equivocating.
- [x] OOB multi-index test leaves no tracker mutated and bumps the counter.

### `S0-A-23` · P1-A/20 — justified `CheckpointContext` overwritten from the wrong state
**Est** 1–1.5 pd / **3 pts** (≈) · **Touch** `crates/fork-choice/src/on_block.rs:452`
Overwritten from the **importing block's post-state**, not the checkpoint state.
**Acceptance** — a test where the two states differ asserts the stored context matches the checkpoint
state's effective balances.

- [x] Justified context is built from the checkpoint post-state, not the importing post-state.
- [x] Test where the two states differ asserts stored balances match the checkpoint state.

### `S0-A-24` · P1-A/21 — `get_proposer_head`'s invented equivocation branch
**Est** 1–1.5 pd / **3 pts** (≈) · **Touch** `crates/fork-choice/src/head_cache.rs:151`
A branch with no spec basis that bypasses the spec's safety conditions.
**Acceptance** — the branch is deleted; the spec's conditions are the only path; the relevant
fork-choice spec vectors are green (and, if any were skiplisted for this, the skiplist entry is
removed — which interacts with `S0a-B-07`'s Q-9 finding).

- [x] Invented equivocation branch deleted; spec safety conditions are the only reorg path.
- [x] Fork-choice `get_proposer_head` vectors green.

---

### `S0-A-25` · P0-03 — `CoreConfig::default()` must not ship `NoVerification`

**Stream** A · **Est** 1–1.5 pd / **3 pts** (≈) · **Discharges** P0-03, **M5**

Production `main.rs` builds `CoreConfig { ..default() }` without raising it, so live import **never
verifies** RANDAO / attestation / exit / slashing / sync-aggregate signatures, and skips the proposer
signature on the unary path.

**Touch points** — `services/chain/src/core.rs:188` (the default); `services/chain/src/import.rs:636`
(the proposer-signature skip)

**Acceptance**
1. Default is `VerifyIndividual`.
2. `NoVerification` is reachable **only** from the restore-replay path that already overrides it —
   asserted by a test, and by a grep showing no other construction site raises it.
3. The unary path verifies the proposer signature.

---

### `S0-A-26` · P0-11 — proposer-lookahead fallback returns `None`

**Stream** A · **Est** 1–2 pd / **3 pts** (≈) · **Discharges** P0-11 · **Touch**
`services/chain/src/import.rs:697`

When a block slot is outside the Fulu lookahead window, the code reads the head state's
*current-epoch* row (`slot % SLOTS_PER_EPOCH`) as the "expected proposer", which almost never matches
— producing a **Reject verdict and a peer descore** for any valid block arriving >~2 epochs after the
head state's epoch (first block after a long gap; a lagging fork branch).

**Acceptance** — outside the window the function returns `None` (unknown) and `on_block` validates;
prefer the parent state where available. A test with a block 3 epochs past the head state yields no
Reject and no descore.

- [x] Outside the Fulu window the cheap-path lookup returns `None`; parent state is preferred.
- [x] A block 3 epochs past the head is not Rejected or descored.

---

### `S0-A-27` · P0-15 — hard deadlines on every chain→engine RPC

**Stream** A · **Est** 2–3 pd / **5 pts** (≈) · **Discharges** P0-15, P1-B/11 ·
**`patch @ S0 → deleted @ S1`** — the whole `engine_client.rs` bridge is deleted when S1 folds the EL
bridge. Patch it anyway: S1 is 7–9 weeks out and a black-holed engine parks the whole consensus core
today.

**Touch points** — `services/chain/src/engine_client.rs:177` is the cited line; `[ARCH]` §2.3 verifies
`handle.block_on` with **no timeout** at `:91, :141, :166, :178, :196, :234` — **six further sites**.
"Every chain→engine RPC" means all of them.

**Acceptance (falsifiable)**
1. Every `block_on` site carries an explicit `Duration`.
2. A timeout routes to the **existing optimistic-import deferral** (`services/chain/src/pending_engine.rs`,
   ADR-P3-05's separate map) — the machinery exists but can never trigger today.
3. A test with an injected black-holed engine shows the block **deferred**, not parked. This is the
   same assertion `S1-A-18` re-runs post-move; write it once here.

- [x] Every production `block_on` (engine client + fcU) has an explicit Duration.
- [x] Timeout maps to Transport and the existing pending-engine deferral.
- [x] Black-holed engine test shows the block deferred, not parked.

---

### `S0-A-28` · ⟡ D-5 — the restore-path `block_on` panic

**Stream** A · **Est** 0.5–1 pd / **2 pts** · ⌂ `[ARCH]` §4.2b ⟡ D-5 · **Discharges** P0-15's tail

`[ARCH]` §4.2(b) verifies the full chain ✓: `service.rs:565` (tonic handler, on a runtime worker) →
`restore.rs:703` → `:715` → **`restore.rs:748 apply_restore_set` — a SYNCHRONOUS fn with no
`spawn_blocking`** → `restore.rs:453 EngineApiClient::new` → `on_block` → `on_block.rs:249-251` →
`execution_payload.rs:82` → **`engine_client.rs:234 self.handle.block_on(...)` — PANIC**.

`Handle::block_on` panics when called from a thread driving a tokio runtime. **So the first restore
block carrying an execution payload aborts the chain process.** It has never fired because every test
of this path substitutes a local `AcceptEngine` double ✓ (`restore.rs:812-818`), and because E4 has
never carried a non-empty restore set on a real network.

**The minimum fix is `tokio::task::spawn_blocking` around `apply_restore_set`.** Do it at S0 either
way. `[PRD]` P0-15's main clause (deadlines) does not touch this line, and S2 is 15+ weeks out.
`EngineApiClient::client()` reaches `block_on` even earlier at the lazy connect ✓
(`engine_client.rs:91`, `:104`) — cover it too.

**Acceptance** — a test that runs `apply_restore_set` from a runtime worker with a **real**
`EngineApiClient` (not the `AcceptEngine` double) and a payload-carrying block does not panic. Using
the double reproduces the reason this never fired.

- [x] `apply_restore_set` runs under `spawn_blocking` from the tonic handler.
- [x] Real-`EngineApiClient` payload test does not panic; cancel clears `in_flight`.

---

### `S0-A-29` · **ADR-R-06** — does the restore/replay path call the engine at all?

**Stream** A · **Est** 0.5 pd / **1 pt** · ⌂ `[ARCH]` §10.5 (stub, *"decision required at S0"*) ·
**Deps** `S0-A-28`

The alternative to `spawn_blocking` — give `EngineApiClient` a **restore mode** that refuses to call
the engine at all, on the argument that the restore path replays blocks whose payloads were already
validated at first import, so `AcceptEngine`'s test semantics are arguably the correct *production*
semantics there — **is a consensus decision, not an implementation choice** (`[ARCH]` §9.1).

**Acceptance** — `docs/adr/ADR-R-06.md` committed in MADR format with a decision or an explicit
`Status: proposed` + *"revisit at S2"*, since the question is moot once S2 deletes the path. The
`spawn_blocking` fix (`S0-A-28`) ships regardless and is **not** blocked on this.

- [x] `docs/adr/ADR-R-06.md` in MADR format; Status: proposed, revisit at S2.

---

### `S0-A-30` · **E0.4** — R-11 executed

**Stream** A · **Est** 1.5–2 pd / **3 pts** · ⌂ `[Q3]` §5 names it as a ~30-line test ·
**Deps** `S0a-A-01`, `S0-A-03` · **S0 exit criterion**

**Why an exit criterion and not a test.** P0-19's severity is a **code-path trace, not an executed
reproduction** (R-11, `[PRD]` J-10). `[Q3]` §5 flags it against itself: *"I read the code and believe
it is unconditional, but I did not execute it against a Hoodi state. Run that before quoting me."*
The block-replay branch has **zero** test coverage, which is consistent with the trace but is not
confirmation. This gates the *claim*, not the work — the fix is **S** either way.

**Acceptance (falsifiable)**
1. Decode the **committed Hoodi anchor state** from SSZ and call `process_block` on a **real** block.
   Import succeeds.
2. **The negative:** omit the top-up ⇒ `BlockError::CachePoisoned`.
3. The result is recorded in the S0 exit note either as *confirms a live ship-blocker* or as
   *downgrades the claim*. **Do not quote P0-19's severity outside this repo until this has run**
   (R-11, verbatim).

---

### `S0-A-31` · **R-13 observation (b)** — an instrumented real restore run

**Stream** A · **Est** 1.5–2 pd / **3 pts** (≈ — **this decomposition's addition**; see the drift
note) · **Deps** `S0-A-30` · **Diagnostic obligation standing in for P1-A/22 and P1-A/23**

**`[PLAN]` §3/S0 says "E0.4 is that obligation". Against `[PRD]` R-13 that is one observation short.**
R-13 requires **two**, and is explicit about why:

> (a) R-11's test — does `CachePoisoned` fire on a decoded state? **Confirms the mechanism.**
> (b) A real restore run instrumented for where it actually fails — does it fail at `process_block`
> before reaching `end_stream` at all? **Only (b) confirms the attribution.**

**The sharper risk is a false diagnosis.** P1-A/22 is a *notification* gap
(`RestoreGate::end_stream` never notifies waiters — `services/chain/src/restore.rs:189`) and P1-A/23
is a *re-drive* gap (restore silently drops DA-deferred blocks — `restore.rs:656`), whereas
`CachePoisoned` would make restore fail **earlier and differently** than either symptom describes.
Someone runs `S0-A-30`, sees `CachePoisoned`, and marks /22 and /23 diagnosed when they have not been.

Both rows are `deleted @ S2`. **This issue does not patch them** — patching a `deleted @ Sn` row is
forbidden (`[PRD]` R-8). It records *where restore fails*, 15+ weeks before S2 deletes the evidence.

**Acceptance (falsifiable)**
1. A restore run against a real snapshot, instrumented at `restore.rs:437` (decode), `:525`
   (`on_block`), `:189` (`end_stream`) and `:656` (DA-deferred drop), producing a recorded trace of
   which point it reaches.
2. The exit note states one of two conclusions explicitly:
   - restore fails at `process_block` and never reaches `end_stream` ⇒ P0-19 is the common cause;
     record it on both rows and let S2's deletion stand;
   - restore **reaches** `end_stream` ⇒ **P0-19 is real *and* /22 and /23 are independent bugs**, and
     deleting them at S2 without fixing them is the wrong call — escalate for re-disposition to
     `patch @ S0 → deleted @ S2` **by explicit decision**, per the `[PRD]` §6 sequencing rule.
3. Neither conclusion may be inferred from `S0-A-30`'s result alone.

---

### `S0-A-32` · **E0.5** — the P0-09 falsifier

**Stream** A · **Est** 1–1.5 pd / **3 pts** · ⟡ `[PLAN]`'s addition (R-16) · **Deps** `S0-A-20`,
`S0a-A-01` · **S0 exit criterion**

R-16: **S0's exit gate cannot detect its two most severe rows.** P0-19 and P0-09 both require a real
post-checkpoint-sync state; `make ci` green and both preset suites green are silent on each. E0.4
covers P0-19 only; P0-09 has the same *invisible-to-`make ci`* shape and needs its own falsifier.

**Acceptance** — an attestation naming a validator activated **after** the anchor, evaluated against a
**grown** registry, does not hit `ValidatorIndexOutOfRange` and is not dropped. Run from the
`S0a-A-01` harness, i.e. against a real decoded state, not a synthetic store.

---

### `S0-A-33` · P1-A/18 — sync-aggregate signature verified under `NoVerification`
**Est** 0.75–1.25 pd / **2 pts** (≈) · **Deps** `S0-A-25` · **Touch**
`crates/state-transition/src/block/mod.rs:160`
**Acceptance** — under `NoVerification` the sync-aggregate signature is **not** verified; a test
asserts the BLS verify count is 0 on that strategy. (Interacts with `S0-A-35`'s instrumentation.)

- [x] Under `NoVerification`, sync-aggregate BLS count is 0.

### `S0-A-34` · P1-B/10 — `block_to_epoch` catch-all relabels every error
**Est** 0.5–1 pd / **2 pts** (≈) · **Touch** `crates/state-transition/src/epoch/mod.rs:53`
Every unexpected error is relabelled `ArithmeticOverflow`.
**Acceptance** — the catch-all is replaced by explicit arms; an injected non-arithmetic error
surfaces with its own variant.

- [x] `block_to_epoch` is exhaustive; `CachePoisoned` is not relabelled `ArithmeticOverflow`.

### `S0-A-35` · P1-B/12 — `SignatureSet` batch bypasses `BLS_VERIFY_COUNT`
**Est** 1–1.5 pd / **3 pts** (≈) · **Touch** `crates/crypto/src/bls/batch.rs:127`
**Acceptance** — batch verification increments the instrumentation by the number of signatures
verified; a test asserts batch and individual paths report the same count for the same set.

- [x] Batch and individual BLS paths report the same verify count.

---

## Stream B issues

### `S0-B-01` · P0-01 — drop the `0.0.0.0` publishes; bind metrics to loopback

**Stream** B · **Est** 0.5–1 pd / **2 pts** (≈ — one line each in compose) · **Discharges** P0-01
(**both** HIGH security findings), **M4**

**Touch points**
- `docker-compose.yml:32,53,73,99,128,150` — the six `9001–9006` publishes
- the pattern to copy: `devnet/compose.yml:11-12,90-91` (`SEC-H1`)
- metrics ports `9101–9106` → `127.0.0.1` (`[PRD]` J-9 — neither source lists these among the six bus
  ports, but `[RV]`'s own recommendation is that only beacon-api and metrics scrape targets be
  host-reachable, *and those on loopback*)

**Sequencing note (`[ARCH]` §4.2a).** This closes the *reachability* of the unauthenticated
`RestoreFromStore` takeover surface **today**; S2 closes the surface itself by deleting it. The port
fix does not wait for S2, and S2 does not excuse skipping the port fix.

**Acceptance** — `S0-B-20` (the off-host scan). A compose diff is not evidence.

---

### `S0-B-02` · P0-07 + P1-A/27 — the three missing URI overrides and the identity mount

**Stream** B · **Est** 0.5–1 pd / **2 pts** (≈) · **Discharges** P0-07, P1-A/27 ·
**`patch @ S0 → deleted @ S2`** — the whole compose cross-service surface disappears when boot
collapses to two processes (`[ARCH]` §4.2). Patch anyway: import is dead today.

Their absence makes localhost TOML defaults apply in-container: `chain` dials its own `9004` (engine
unavailable, **import dead**), `engine` has no `CC_ENGINE_P2P_URI`, `p2p` has no
`CC_P2P_PEERS__STORAGE` (serve window fail-closes to `NOT_SERVING`). Every *other* peer is overridden
— an oversight, not a design ✓ (zero occurrences of the three keys).

**Touch points** — `docker-compose.yml:30` (P0-07); `docker-compose.yml:127` (P1-A/27 — no identity
mount, no `CC_STORAGE_NODE_KEY_PATH`, so storage can never enforce `I-node-id`, ADR-P4-13)

**Acceptance** — a **block-import smoke test** in CI that would have caught this: bring the stack up,
assert a block imports. `[PRD]` P0-07 asks for exactly this, and it is the only acceptance criterion
here that is not re-reading the compose file.

- [x] Three URI overrides: `CC_CHAIN_ENGINE_URI`, `CC_ENGINE_P2P_URI`, `CC_P2P_PEERS__STORAGE`.
- [x] Storage identity mount (`CC_STORAGE_NODE_KEY_PATH` + `cc-p2p-identity:/identity:ro`).
- [x] CI asserts the keys and `:ro` identity mount (`scripts/check-compose-uri-overrides.sh`); live Hoodi import smoke deferred (needs a synced EL).

---

### `S0-B-03` · P1-A/29 — `devnet/faults.sh` pinned to the wrong compose file
**Est** 0.25–0.5 pd / **1 pt** (≈) · **Touch** `devnet/faults.sh:27`
Pinned to `devnet/compose.yml`, but the CC-4N kill-9 clause documents it against the main stack.
**Acceptance** — the script takes the compose file as a parameter defaulting to the stack the clause
names; the clause doc and the script agree, asserted by the clause's own runner.

- [x] `faults.sh` takes `-f` and defaults to `docker-compose.yml`; self-test asserts the effective pin.

---

### `S0-B-04` · P0-04 — `BeaconBlocksByRange` v2 encodes 24 bytes

**Stream** B · **Est** 1–1.5 pd / **3 pts** (≈) · **Discharges** P0-04

Currently encodes 16 (`start_slot`, `count`), dropping the spec's mandatory `step: uint64` —
deprecated but **never removed from the SSZ schema**. The primary block-sync protocol is fully
interop-broken **in both directions**.

**Touch points** — `services/p2p/src/reqresp/blocks.rs:46`

**Acceptance** — `step: u64` present and set to 1; `BY_RANGE_SSZ_LEN = 24`; a round-trip test against
a **hand-written 24-byte fixture** (not against our own encoder — that is the P0-06 failure shape).
The live falsifier is M2e at `S3b-W-10`.

---

### `S0-B-05` · P0-05 — `BlocksByRoot` v2 encodes bare `32·n` roots

**Stream** B · **Est** 1–1.5 pd / **3 pts** (≈) · **Discharges** P0-05

Currently prepends a **spurious 4-byte offset**. The request is a top-level `List[Root]` of
fixed-size elements, and offsets only exist for variable-size element lists.

**Touch points** — `services/p2p/src/reqresp/blocks.rs:148`

**Acceptance** — decode succeeds on `len % 32 == 0` and rejects otherwise; a hand-written fixture of
3 roots (96 bytes, no prefix) round-trips.

---

### `S0-B-06` · The three duplicated request-limit tables reconciled

**Stream** B · **Est** 0.75–1.25 pd / **2 pts** (≈) · **Deps** `S0-B-04`, `S0-B-05`

P0-04 and P0-05 each name *"the duplicated limits"*. There are **three** copies and two of them
**already disagree** (P1-B/6).

**Touch points**
- `services/p2p/src/reqresp/mod.rs:203`
- `crates/libp2p/src/ssz_snappy_codec.rs:204,210`
- `Protocol::request_limits`

**Scope boundary.** This issue makes the three copies **agree**. It does **not** deduplicate them —
that is P1-B/6, `patch @ S3` with `cc-wire` (`S3a-A-03`). Deduplicating early would move an S3 crate
extraction into S0.

**Acceptance** — a test asserts all three tables are equal, so the next divergence is a CI failure
rather than an interop bug.

---

### `S0-B-07` · P0-06 — restore the wire prober's independence

**Stream** B · **Est** 0.5–1 pd / **2 pts** (≈) · **Discharges** P0-06 · **Touch**
`bin/serve-probe/src/protocols.rs:258`

The prober reproduces the **same** bogus 4-byte `BlocksByRoot` offset, so probe and node agree with
each other and both diverge from spec — defeating its stated purpose (ADR-P4-12: *"independent so a
shared bug cannot make the probe pass against our own node"*).

**PR-boundary constraint — this must be a separate PR from `S0-B-05`, and derived from the spec, not
from the node's fix.** Copying `S0-B-05`'s implementation across re-creates the exact coupling
ADR-P4-12 forbids and that P0-06 exists because of. `[ARCH]` §9.1/S3 additionally forbids
`bin/serve-probe` ever taking `cc-wire`.

**Acceptance**
1. The probe encodes bare `32·n` **by its own independent implementation**.
2. A reviewer confirms no shared module was introduced between `bin/serve-probe` and
   `services/p2p`/`crates/libp2p`.
3. `S0-B-17` triages **P2-E row 31** (`bin/serve-probe/src/protocols.rs:429`, empty
   `ColumnsByRootRequest` encodes 4 bytes instead of zero) — **same class as P0-06**; if it promotes,
   it lands in this PR.

---

### `S0-B-08` · P0-13 — end the write-behind session with `Reconnect { cursor: last_flushed }`

**Stream** B · **Est** 2–3 pd / **5 pts** (≈) · **Discharges** P0-13 ·
**`patch @ S0 → deleted @ S2`**

Today the unit is logged and dropped while the session keeps consuming events; the next successful
flush commits a `WriteCursor` with a **later** seq, so the failed unit's events are **claimed durable
and skipped forever on resume** — an unrecorded data-loss hole that violates the
cursor-bounds-the-loss-window contract.

**Touch points** — `services/storage/src/write_behind.rs:657` (and `:650-658`, the path that claims a
failed flush durable — `[Q4]` cites this same defect as one of the two reasons the slashing DB must
not ride this path)

**Not thrown away at S2.** The invariant this establishes — *the cursor bounds the loss window; a
failed unit never advances it* — is restated by S2's top-of-batch continuity bind (`[ARCH]` §4.3).
State it in the same words in both places.

**Acceptance** — an injected P0 flush error ends the session with `Reconnect { cursor: last_flushed }`;
a resume from that cursor re-delivers the failed unit's events; a test asserts no `WriteCursor` with
a seq beyond `last_flushed` is ever committed after the error.

- [x] Injected P0 flush error ends the session at last_flushed (including first flush / last_flushed = None).
- [x] Resume re-delivers the failed unit; no WriteCursor jumps past it.

---

### `S0-B-09` · P0-14 — `rewrite_from_head` deletes canonical rows for vacated slots

**Stream** B · **Est** 2–3 pd / **5 pts** (≈ — needs a slot-skipping reorg test) ·
**Discharges** P0-14 · **Touch** `crates/store/src/canonical.rs:91`

It only ever *writes* rows on the new branch; there is **no `TABLE_CANONICAL` delete anywhere in the
tree**. A reorg onto a branch that skips a slot the old branch occupied (or a shorter head) leaves the
stale `canonical[slot]` row forever, violating the module's *"can never disagree with the blocks it
indexes"* contract.

**Acceptance** — `batch.delete` staged during the walk; a **slot-skipping reorg test**: branch A
occupies slots {10,11,12}, branch B occupies {10,12} and is heavier; after the rewrite,
`canonical[11]` is absent, not stale.

- [x] `batch.delete` staged during the walk for vacated slots (including same-batch parents).
- [x] Slot-skipping reorg test: after rewrite onto {10,12}, `canonical[11]` is absent.

---

### `S0-B-10` · P1-A/1 — `PutBackfillBatch` anchor/frontier binding

**Stream** B · **Est** 2–3 pd / **5 pts** (≈) · **Discharges** P1-A/1 (server-side validation for
`[RV]` Vuln 2, `[PRD]` J-8)

`PutBackfillBatch` can overwrite the canonical index at any slot: no anchor/frontier binding, progress
optional. A **single-block batch with empty `progress` bypasses the descending-contiguity and
progress-monotony checks entirely** ✓ (`serve.rs:781-782`, `:821`).

**Touch points** — `services/storage/src/serve.rs:821` and `:781-782`

**State the invariant in the same words S2 will use** (`[ARCH]` §4.3): *a batch may only extend the
durable frontier, never jump it.* S2's top-of-batch continuity bind restates it on the direct ingest
path; if the two are worded differently, the S0 work gets re-litigated at S2.

**Acceptance** — an empty-`progress` single-block batch is **rejected**, not fast-pathed; a batch
whose parent is not durable and is not the first row of the same batch is rejected; both asserted by
test.

- [x] Empty-progress single-block batch is rejected.
- [x] Parent not durable and not first row of the same batch is rejected.
- [x] Progress-only RPC cannot plant an arbitrary `blocks_oldest_parent`.

---

### `S0-B-11` · P1-A/5 — CC-4A block floor from a build-machine fixture path
**Est** 0.75–1.25 pd / **2 pts** (≈) · **Touch** `services/storage/src/main.rs:352`
Read from a build-machine fixture path with a silent hardcoded fallback and `unwrap_or(0)`.
**Acceptance** — the floor comes from config; a missing/unreadable source is a **startup error**, not
a silent 0. A 0 floor is maximally destructive for retention (cf. P2-E row 21, triaged in `S0-B-17`).

- [x] Floor comes from `network_config`; missing/unreadable/zero is a startup error.

### `S0-B-12` · P1-B/1 — `observe_lag` on the already-reset accumulator
**Est** 0.25–0.75 pd / **1 pt** (≈) · **Touch** `services/storage/src/write_behind.rs:599`
`write_behind_lag_slots` **never records**. (This is also P2-D/20's "a lag metric that never emits".)
**Acceptance** — a test drives a known lag and asserts the histogram observes it.

- [x] `take_flush` preserves lag slots; a test drives a known lag and the histogram observes it.

---

### `S0-B-13` · P1-A/8, /9, /10 — gossip validation and registry rows

**Stream** B · **Est** 3.5–5 pd / **8 pts** (≈) · **Discharges** P1-A/8, P1-A/9, P1-A/10

Three independent fixes grouped for review economy; **land as three commits**, each with its own test.
Split into separate issues if the sprint needs 1–2 day units.

| Row | Touch | Defect | Acceptance |
|---|---|---|---|
| P1-A/8 | `services/p2p/src/gossip/validate/pipeline.rs:897` | clock disparity quantized **up to a full slot** — widens all gossip timeliness checks **24×** | disparity is applied at its configured value; a test asserts a message `MAXIMUM_GOSSIP_CLOCK_DISPARITY + 1ms` outside the window is IGNOREd |
| P1-A/9 | `services/p2p/src/gossip/validate/column.rs:445` | column validator omits two spec REJECT conditions (slot > parent slot; finalized-ancestor) | one test per condition, each asserting **REJECT** (not IGNORE) |
| P1-A/10 | `services/p2p/src/gossip/registry.rs:581` | `advance_to` requires exact epoch equality; **a missed epoch tick permanently wedges subscriptions** | a test skipping one epoch tick still converges to the correct subscription set |

Note P1-A/9 interacts with `S3a-B-18` — the same validator consults the 1,900-line `fault_mode` global
(P1-D/18), which is removed from production paths at S3a, not here.

---

### `S0-B-14` · P1-F/1 — **ADR-R-05**, the backend-independent half

**Stream** B · **Est** 0.5–1 pd / **2 pts** · ⌂ `[Q4]` — *the decision costs nothing today* ·
**Deps** `S0a-B-10` (for the backend clause only) · **Discharges** P1-F/1 (`write @ S0`)

**`[PLAN]` C-9 splits this row.** The **negative decision** is writable at S0 with no dependency and
is where `[Q4]` says the value is:

- a dedicated `crates/slashing-protection`, a **leaf crate depending only on `cc-types`**, on the
  **validator-client** side of the S5 boundary — it **moves with the signer**, not with the beacon node
- it **must not ride `services/storage`**: that path acknowledges up to **4 s** before it commits ✓
  (`config/storage.toml:46`, `write_behind.rs:70`) and has a path that claims a failed flush durable ✓
  (`write_behind.rs:653-657` = P0-13). **Either inverts record-before-sign**, the one ordering that
  prevents a slashable signature
- **record-then-sign** as the only path; a **fused** `check_and_insert_*` API so check and insert are
  one atomic exclusive operation
- store the complete form, export the minimal form (three integers per validator)
- the contrast with P0-19/4, written into the module docs: the pubkey cache **is** reconstructible
  from the state, so losing it is merely a slow boot and it **is** safe on the write-behind path. The
  two cases must never be conflated

**The backend clause is gated on `S0a-B-10` (Q-2)** and is the only part that waits. SQLite must be
recorded as a **stated exception**, never taken by default (`[ARCH]` §9.1/S5): `rusqlite` pulls
`libsqlite3-sys` (C FFI) into a workspace that sets `unsafe_code = "deny"` ✓ (`Cargo.toml:38`) and
that chose redb after a documented falsifier exercise (`docs/storage-engine.md`).

**Id discrepancy — resolve it in this PR.** The slashing ADR is called **`ADR-R-05`** in `[ARCH]`
§10.5 and B.2/Q-2, and **`ADR-R-04`** in `[ARCH]` §9.1/S5 (`[PLAN]` X-3). **Use `ADR-R-05`**, matching
§10.5's own enumeration, and fix the §9.1 reference — so the id does not join the phantom corpus it
exists to fix. See also the `X-6` finding in [`README.md`](README.md).

**Acceptance** — `docs/adr/ADR-R-05.md` committed in MADR format; the backend clause either decided
(Q-2 answered) or carrying `Status: proposed` + *"revisit when Q-2 lands"*.

---

### `S0-B-15` · P1-A/12 — empty in-window results answered with error code 3
**Est** 1–1.5 pd / **3 pts** (≈) · **Touch** `services/p2p/src/reqresp/blocks.rs:446`
**Acceptance** — an in-window request with no results returns an **empty success stream**; a foreign
client's decoder sees a well-formed zero-chunk response. Falsified live at `S3b-W-10`.

- [x] In-window empty ByRange is a zero-chunk success, not error code 3.

### `S0-B-16` · P1-A/13 — column by-range chunks served in request order
**Est** 1.5–2 pd / **3 pts** (≈) · **Touch** `services/p2p/src/reqresp/columns.rs:536`
Served in request order, not ascending `(slot, column_index)`; **the comment contradicts the code**.
**Acceptance** — chunks are emitted ascending by `(slot, column_index)` for an out-of-order request;
the comment and the code agree.

---

### `S0-B-17` · Early P2-E triage — rows 8 and 31 (and 1, 6, 21)

**Stream** B · **Est** 1–1.5 pd / **3 pts** · ⌂ `[PLAN]` §4 (*"8 and 31 bear directly on P0-02 and
P0-06 and should be triaged during S0, not S1"*) · **Discharges** part of M12

The bulk 35-row R-P2-triage runs in S1 (`S1-B-19`, `S1-B-20`). These five are pulled forward because
`[PLAN]` §4 names them as the rows most likely to promote, and two of them bear on P0 work landing
**this phase**.

| Row | Location | Why now |
|---|---|---|
| **8** | `crates/state-transition/src/helpers/accessors.rs:545` | `deposit_domain()` disagrees with the real deposit-domain computation — **check against P0-02 before dismissing**; if it promotes, it lands in `S0-A-09` |
| **31** | `bin/serve-probe/src/protocols.rs:429` | empty `ColumnsByRootRequest` encodes 4 bytes instead of zero — **same class as P0-06**; if it promotes, it lands in `S0-B-07` |
| 1 | `services/chain/src/core.rs:776` | `CanonicalRoots` fabricates the head root where `get_ancestor` fails (`get_ancestor(head, s).unwrap_or(head)` ✓ `core.rs:772-777`). Its role in **durable** writes ends at S2 (`[ARCH]` §4.3) but the RPC survives for API consumers, so it must still be triaged |
| 6 | `crates/types/src/config.rs:58` | empty `BLOB_SCHEDULE` (spec-legal) rejected at load — same file as `S0-A-07`/`S0-A-08` |
| 21 | `services/storage/src/prune/mod.rs:155` | `PruneConfig::default` maps a failed floor computation to 0 — **maximally destructive retention**; same defect shape as P1-A/5 (`S0-B-11`) |

**Acceptance** — each of the five is **promoted with a tier and disposition** or **dismissed with a
recorded reason**, written into `[PRD]` §5.3.2. No row is left in an unknown state. `S1-B-19`/`-20`
then cover the remaining 30.

---

### `S0-B-18` · **E0.9** — create the M3 `Discharged by` artifact

**Stream** B · **Est** 0.75–1.25 pd / **2 pts** (≈ — **this decomposition's addition**; neither source
creates the artifact) · **Discharges** M3's tracking mechanism

M3's target is *"0 open, **with the discharging commit or stage recorded per row**"*. **Nothing in
either source creates the artifact that makes that readable.**

**Acceptance (falsifiable)**
1. A `Discharged by` column exists on `[PRD]` §5.1 and §5.2, holding either a **commit SHA** (patch)
   or a **stage id** (deletion).
2. **Zero blank cells for rows this stage claimed** — asserted by a script, not by eye. Add the script
   to the `S0a-B-04` gate list so the check runs in CI.
3. The maintenance convention is written down: each stage exit carries a ~0.5 pd line item to update
   it (`S1-B-21`, `S2-B-16`, `S3a-B-27`). This is also the mechanism behind the program's
   *no-P0-vanishes-silently* check.

---

### `S0-B-19` · **E0.7** — the §9.0 A/B baseline run

**Stream** B · **Est** 1.5–2 pd / **3 pts** · ⌂ `[ARCH]` §9.0 · **Deps** every patch issue above ·
**S0 exit criterion**

Establishes the baseline every later stage diffs against. Per `[ARCH]` §9.0:

1. Build both topologies from the same commit (`make build`).
2. Run both against the **same self-devnet** for **≥ 1 h**, same fixtures, same EL snapshot.
3. Diff **three families**: `cc_chain_import_result{result=*}` (verdict distribution),
   head-lag histogram buckets, and the §2.2 overflow counters (`*_rejected_backpressure`, `*_dropped`,
   subscriber terminations).
4. **A non-zero diff in family 3 with a zero diff in families 1–2 is the dangerous case** — behaviour
   unchanged at test load, *contract* changed. **Stage blocker, not a curiosity.**
5. Record in the stage exit note.

**Acceptance** — all six binaries built and run; the three families recorded with absolute numbers, not
"no significant change".

---

### `S0-B-20` · **E0.8** — off-host port scan

**Stream** B · **Est** 0.25–0.75 pd / **1 pt** (≈ — **this decomposition prices it; `[PLAN]` names the
criterion but not the work**) · **Deps** `S0-B-01` · **Discharges** M4's verification

**Acceptance** — a scan **from a second host** reaches none of `9001`–`9006`, and `9101`–`9106` answer
on `127.0.0.1` only. Recorded in the exit note with the scanning host's address and the raw output.
Without this check S0 ships the security fix with nothing asserting it.

---

## S0 exit criteria — the falsifiers, and which issue earns each

| # | Criterion | Earned by |
|---|---|---|
| E0.1 | M7 = 0 sustained; `make ci` green | `S0a-B-01..04` (carried) |
| E0.2 | **M6b = 0** — `pub mod network` deleted; omitting `&ChainConfig` is a compile error | `S0-A-09` |
| E0.3 | Both preset vector suites green **against a fixture that differs from mainnet on all five constants** | `S0-A-11` |
| E0.4 | **R-11 executed** — decode the Hoodi anchor, `process_block`, and the negative | `S0-A-30` |
| E0.5 | **P0-09 falsifier executed** | `S0-A-32` |
| E0.6 | **M5** — signature verification on the production import path; `NoVerification` reachable only from restore-replay | `S0-A-25`, `S0-A-33` |
| E0.7 | §9.0 A/B baseline recorded | `S0-B-19` |
| E0.8 | **M4 = 0** — off-host scan reaches none of `9001`–`9006`; metrics loopback-only | `S0-B-20` |
| E0.9 | **M3 tracking artifact exists**, zero blank cells for rows this stage claimed | `S0-B-18` |
| — | *(not a `[PLAN]` exit criterion; this decomposition recommends adding it)* **R-13 observation (b)** recorded, with an explicit conclusion on P1-A/22–23 attribution | `S0-A-31` |

---

## Drift against `[PLAN]` §3/S0 — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **Total.** `[PLAN]`'s work-item table sums to **56–87 pd**; this decomposition sums to **62.25–98.5 pd**. The delta is ~+6–11 pd of exit-evidence and diagnostic work the table does not itemise: `S0-A-31` (R-13 observation b), `S0-A-29` (ADR-R-06), `S0-B-17` (early P2-E triage), `S0-B-18` (E0.9), `S0-B-20` (E0.8). None is optional — E0.8 and E0.9 are named exit criteria with no line in the estimate. |
| 2 | **Calendar.** `[PLAN]` states *"→ 6–10 calendar wk at A-1/A-2"*. At A-1 (2 streams) and A-2 (efficiency 0.8), one engineer delivers 4 effective pd/wk, so **`[PLAN]`'s own 56–87 pd is 7–11 wk**, not 6–10. This decomposition's stream A carries 40.75–64.75 pd → **10.2–16.2 wk**, and stream A is the binding constraint. **S0 as decomposed does not fit its wk 2–8 window.** Two levers, both from `[PLAN]` §12: a third stream buys ~2 wk (S0's content parallelises well across disjoint crates), or the fork-choice group (`S0-A-20..24`, 5.5–9 pd) moves to stream B — it touches `crates/fork-choice` only, which stream B does not otherwise open, giving A 35.25–55.75 pd → **8.8–13.9 wk**. Recommend the second; it is free. |
| 3 | **`[PLAN]` says S0 contains "16 `patch @ S0` P0 rows"** but P0-08 is promoted to S0a by C-1. S0's body carries **15**. The arithmetic is otherwise exact: 15 P0 + 19 P1 = 34 discrete rows (`[PLAN]` says 35, counting P0-08). |
| 4 | **Group sizings that do not survive decomposition.** `[PLAN]`'s "Chain import" row is 4–6 pd for P0-03 + P0-11 + P0-15 + D-5; P0-15 alone is *"every chain→engine RPC"* and `[ARCH]` §2.3 verifies **seven** deadline-less `block_on` sites, plus ADR-R-06 is a consensus decision. Decomposed: 5–8.5 pd. Similarly "Compose surface" 1–2 pd → 1.25–2.5 pd, and "S0 exit falsifiers" 3–5 pd → 5.75–8 pd once E0.8, E0.9 and R-13(b) are priced. |
| 5 | **P0-10 and P0-18 ship with no `file:line` in `[PRD]`** — both cite only `[AS] §4/0n` and a crate name. `S0-A-21`'s touch points are recovered here by grep and should be written back into `[PRD]` §5.1 so the next reader does not repeat the search. P0-18's are recovered in [`s2-fold-storage.md`](s2-fold-storage.md). |
