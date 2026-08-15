# S0a — gate restoration · wk 1

**Sources.** `[PLAN]` = `plan/project-plan.md` §3/S0a, §8 · `[PRD]` = `plan/prd.md`
§5.1 · `[ARCH]` = `plan/architecture.md` · `[Qn]` = `plan/research/`.

**Phase contract.** Entry: none — this is the program's first work item. Exit: **M7 = 0**.
Out of scope: any correctness fix, any file move. This phase changes **gates and CI only**, so the
S0 body's first commit has a trustworthy baseline to diff against (`[PLAN]` R-5 / C-1).

**Estimate provenance.** ⌂ = derived from a research brief or `[ARCH]` S/M/L/XL sizing · ≈ = this
decomposition's judgement, stated as a range. Points snap to Fibonacci and encode size class +
uncertainty; **the day range is the authoritative estimate.** Phase point totals are for sprint
capacity only and will not equal 2 × the day total.

| midpoint pd | ≤ 0.5 | 0.6–1.0 | 1.1–2.0 | 2.1–3.0 | 3.1–5.0 | > 5.0 |
|---|---|---|---|---|---|---|
| **pts** | 1 | 2 | 3 | 5 | 8 | 13 |

---

## Issue index

| Id | Title | Stream | pd | pts | Deps |
|---|---|---|---:|---:|---|
| `S0a-B-01` | P0-08(a) — route the production `env::var` reads through `cc-config` | B | 0.75–1.5 | 2 | — |
| `S0a-B-02` | P0-08(b) — fix the `awk` `#[cfg(test)]` exemption and broaden the pattern | B | 0.75–1.5 | 2 | — |
| `S0a-B-03` | P0-08(c) — `cargo fmt --check` green on `HEAD` | B | 0.25–0.75 | 1 | — |
| `S0a-B-04` | P0-08(d) — `make ci` mirrors the CI job list, asserted by a diffing test | B | 1–1.5 | 3 | `S0a-B-01..03` |
| `S0a-B-05` | P1-A/28 — proto breaking baseline hardcodes `branch=develop` | B | 0.25–0.75 | 1 | — |
| `S0a-B-06` | P1-C/1 — pin GitHub Actions to commit SHAs | B | 0.5–1 | 2 | — |
| `S0a-B-07` | **Spike Q-9** — does the spec-vector coverage check report or fail? | B | 0.25–0.75 | 1 | — |
| `S0a-B-08` | **Spike Q-10** — is the serve window truly never published? | B | 0.25–0.75 | 1 | — |
| `S0a-B-09` | **Spike Q-3** — does `superstruct` compose with milhouse's `List<T, N, U>`? | B | 0.125–0.25 | 1 | — |
| `S0a-B-10` | **Spike Q-2** — does redb give a fail-fast cross-process exclusive open? | B | 0.75–1.5 | 2 | — |
| `S0a-A-01` | R-11 falsifier harness skeleton — committed Hoodi anchor state fixture rig | A | 1–2 | 3 | — |
| | **Total** | | **5.9–12.25** | **19** | |

**Parallel duration, 2 streams.** Stream A 1–2 pd, stream B **4.9–10.25 pd**. At `[PLAN]` A-1/A-2
(5 pd/engineer-week × 0.8 efficiency = 4 effective pd/wk) stream B binds at **1.2–2.6 wk**, against
`[PLAN]`'s stated **0.5–1 wk**. See the drift note at the bottom of this file.

---

## `S0a-B-01` · P0-08(a) — route the production `env::var` reads through `cc-config`

**Stream** B · **Est** 0.75–1.5 pd / **2 pts** (≈) · **Deps** none · **Discharges** P0-08(a), part of M7

`scripts/check-no-env-reads.sh` is a **blocking** CI gate (`ci.yml:88`) that exits 1 on the committed
tree ✓. Every later phase's entry gate is unevaluable until it exits 0 (`[PLAN]` R-5, D1).

**Touch points**
- `services/storage/src/replay.rs:829,832`
- `services/p2p/src/main.rs:536,537`
- `bin/cc-store/src/lib.rs:13,52,138`
- `crates/config` (the `cc-config` surface each read is routed into)

**Under-specified — state your call in the PR.** `[PRD]` P0-08 counts **4 production reads** but the
script's evidence list names **7 sites across 3 files**. The likely reading is that the two service
binaries hold the 4 production reads and `bin/cc-store` (an operator tool) is allowlist-eligible. The
row does not say. Decide per site, and record the split in the PR description — "route" vs "annotated
allowlist" is a policy choice, not a mechanical one.

**Acceptance (falsifiable)**
1. [x] `scripts/check-no-env-reads.sh` exits **0** on `HEAD`.
2. [x] Every site not routed through `cc-config` carries an inline allowlist annotation naming *why*, and
   the annotation is the mechanism the script recognises — not a pattern exclusion in the script.
   (Vacuous: all four production reads are routed; `bin/cc-store` has no `env::var`.)
3. [x] `cargo test` compiles all three touched crates.

---

## `S0a-B-02` · P0-08(b) — fix the `awk` `#[cfg(test)]` exemption and broaden the pattern

**Stream** B · **Est** 0.75–1.5 pd / **2 pts** (≈) · **Deps** none · **Discharges** P0-08(b)

The script's `awk` `#[cfg(test)]` exemption **never resets**, so one test attribute silently exempts
the remainder of the file. The pattern also matches only `env::var`, missing `var_os` and `vars`.

**Touch points**
- `scripts/check-no-env-reads.sh:17` (the awk exemption)

**Acceptance (falsifiable)**
1. [x] A **negative fixture** is committed: a file containing a `#[cfg(test)] mod` followed by a
   production `std::env::var` read. The script **fails** on it. This is the direct falsifier for the
   never-resetting exemption — without it, the fix is asserted, not demonstrated.
2. [x] Three further negative fixtures for `env::var_os`, `env::vars`, and a fully-qualified
   `std::env::var` each cause a failure.
3. [x] A positive fixture (a read genuinely inside `#[cfg(test)]`) still passes.
4. [x] The script exits 0 on `HEAD` after `S0a-B-01`.

---

## `S0a-B-03` · P0-08(c) — `cargo fmt --check` green on `HEAD`

**Stream** B · **Est** 0.25–0.75 pd / **1 pt** (≈) · **Deps** none · **Discharges** P0-08(c), part of M7

M7's baseline is **2** red gates; this is the second (3 diffs ✓).

**Acceptance**
1. [x] `cargo fmt --all --check` exits 0 on `HEAD`.
2. [x] The commit is formatting-only — no semantic hunk in the diff. Reviewer check, not CI check.

---

## `S0a-B-04` · P0-08(d) — `make ci` mirrors the CI job list, asserted by a diffing test

**Stream** B · **Est** 1–1.5 pd / **3 pts** (≈) · **Deps** `S0a-B-01`, `S0a-B-02`, `S0a-B-03` ·
**Discharges** P0-08(d), P2-B/8 (promoted), P1-D/17(b), M7 target

`[PLAN]` E0.1 requires this be **asserted by a test that diffs the two lists, not by inspection** —
a `make ci` that drifts from `ci.yml` is exactly the class of gate this phase exists to close.

**Touch points**
- `Makefile:193` (`make deps` / `lint` / `ci` and the guard-script header claiming wiring that does
  not exist)
- `.github/workflows/ci.yml` (the job list)

**Acceptance (falsifiable)**
1. [x] A test enumerates the job ids in `.github/workflows/ci.yml` and the gate list in `Makefile`, and
   **fails** when the two sets differ in either direction.
2. [x] Deleting a job from `ci.yml` without deleting it from the `Makefile` makes that test red —
   demonstrated once in the PR, not merely claimed.
3. [x] `make ci` is green on `HEAD`.
   (`CI_JOBS` mirrors `ci.yml`; `make ci` runs `lint test proto deps`. Full nextest/clippy not re-run in this issue.)

---

## `S0a-B-05` · P1-A/28 — proto breaking baseline hardcodes `branch=develop`

**Stream** B · **Est** 0.25–0.75 pd / **1 pt** (≈) · **Deps** none · **Discharges** P1-A/28

**Touch points** — `.github/workflows/ci.yml:208`

**Acceptance** — [x] the baseline branch is derived from the push target, so a push to `main` is compared
against `main`; asserted by a workflow-level fixture or a dry-run, not by reading the YAML.

---

## `S0a-B-06` · P1-C/1 — pin GitHub Actions to commit SHAs

**Stream** B · **Est** 0.5–1 pd / **2 pts** (≈) · **Deps** none · **Discharges** P1-C/1

The repo's own `docs/supply-chain.md` states an exact-pin policy that `ci.yml` contradicts.

**Touch points** — `.github/workflows/ci.yml:49` and every other `uses:` line in `.github/workflows/`

**Acceptance**
1. [x] No `uses:` line in `.github/workflows/` carries a mutable tag.
2. [x] A grep-based check for `uses: .*@v[0-9]` is added to the gate list from `S0a-B-04`, so this cannot
   silently regress. (Standalone `scripts/check-gha-sha-pins.sh` + clippy step; `make ci` wiring is S0a-B-04.)

---

## `S0a-B-07` · **Spike Q-9** — does the spec-vector coverage check report or fail?

**Stream** B · **Est** 0.25–0.75 pd / **1 pt** · ⌂ `[ARCH]` B.2 sizes this **XS** ·
**Blocks** `S3b-W-01` (Phase 1 clause 1's "skiplist empty" clause)

**Question.** `crates/spec-tests` runs a coverage check. Does an uncovered vector *report* (log and
continue) or *fail* the suite? M2a's clause 1 requires "suites green both presets, **skiplist
empty**" — if the check only reports, "green" today does not mean what the clause needs it to mean.

**Deliverable** — [x] a one-paragraph finding committed to `plan/issues/spike-notes.md` naming
the file:line of the check and its behaviour, plus a yes/no on whether W1's clause is measurable
as the harness stands. If the answer is "reports", the follow-on work item is sized here and filed
against S3a.

---

## `S0a-B-08` · **Spike Q-10** — is the serve window truly never published?

**Stream** B · **Est** 0.25–0.75 pd / **1 pt** · ⌂ `[ARCH]` B.2 **XS** · **Blocks** `S3a-B-08`
(P0-17c's framing)

**Question.** `[ARCH]` §3 asserts the serve window is *never published*; only the empty-window seed
`earliest_available_slot: u64::MAX` at `services/storage/src/serve.rs:1086-1087` has been verified ✓
(`[PRD]` J-3). Those are different claims. Find the publish path, or establish there is none.

**Deliverable** — [x] a finding naming either the publisher's file:line or the absence, committed to the
spike notes. `[PRD]` J-3 asks for exactly this before S3.

---

## `S0a-B-09` · **Spike Q-3** — does `superstruct` compose with milhouse's `List<T, N, U>`?

**Stream** B · **Est** 0.125–0.25 pd (**1 hour**) / **1 pt** · ⌂ `[ARCH]` B.2 · **Blocks** the
**S4a → S4b ordering itself** (`[PLAN]` D6)

**Why an hour of work is on this list.** `[PLAN]` ⟡ D-8 sequences milhouse (4a) before the Gloas
schema (4b) because milhouse reduces a four-place hand-synchronised state-schema edit to two places
before a −1/+9-field fork edits it. **A negative answer inverts that order** and rewrites S4's
dependency edges. It is one hour of reading Lighthouse's `consensus/types/src/beacon_state.rs`, and
`[PLAN]` §8 schedules it in wk 1 rather than at S4 entry precisely so the inversion is cheap.

**Deliverable**
1. [x] A yes/no with the Lighthouse file:line that settles it.
2. [x] **If NO** — file the inversion against `s4-fork-seam.md`'s named alternate branch (`S4-ALT`), which already
   states the reversed edge set. Do not leave it as a footnote; the S4 file's dependency table has
   two versions and this spike picks one.

(Answer was YES; planned 4a → 4b → 4c stands. Item 2 does not apply.)

---

## `S0a-B-10` · **Spike Q-2** — does redb give a fail-fast cross-process exclusive open?

**Stream** B · **Est** 0.75–1.5 pd / **2 pts** · ⌂ `[ARCH]` B.2 sizes this **S** ·
**Blocks** the backend clause of `S0-B-14` (P1-F/1's ADR)

**Question.** A second opener of the slashing-protection DB must **error**, not block — that is what
catches "operator started two validator clients on one key". `[ARCH]` §9.1/S5 calls this the *only*
remaining argument for SQLite. If redb blocks, the fix is to wrap the open in
`flock(LOCK_EX | LOCK_NB)`.

**Touch points (for the experiment, not a patch)**
- `crates/store/src/engine/redb.rs:476-493` — the existing `Durability::Immediate` / `Paranoid`
  two-phase-commit surface, which is the evidence redb already exposes what `[Q4]` needs

**Deliverable**
1. [x] A two-process experiment: process 1 opens, process 2 opens the same path. Record whether process 2
   returns an error or blocks, with the redb version.
2. [x] A one-line verdict feeding `S0-B-14`'s backend clause: **redb** (fail-fast confirmed), **redb +
   flock** (blocks), or **SQLite as a stated exception** (`[ARCH]` §9.1 requires SQLite be recorded as
   a stated exception, never taken by default).

**Note the source disagreement this resolves.** `[PRD]` P1-F/1 writes *"SQLite, `POOL_SIZE=1`,
`locking_mode=EXCLUSIVE`"* citing `[Q4]`; `[Q4]` §line 16 says **"Backend: redb, not SQLite — despite
Lighthouse"**, naming SQLite as the alternative (`[PLAN]` R-18 / X-4). This spike is the tiebreak.

---

## `S0a-A-01` · R-11 falsifier harness skeleton

**Stream** A · **Est** 1–2 pd / **3 pts** (≈) · **Deps** none · **Enables** `S0-A-30` (E0.4),
`S0-A-31` (R-13 observation b), `S0-A-32` (E0.5)

`[PLAN]` §3/S0a assigns stream B the four spikes and *"the S0 exit falsifiers' harness skeleton (R-11
test rig)"*. This is that rig — assigned to stream A here because every consumer of it is a stream-A
falsifier. **No production commit lands from this issue**; it is test scaffolding only, which is what
keeps it inside S0a's "gates and CI only" scope.

**Touch points**
- a new test-support module (suggested `crates/state-transition/tests/support/anchor.rs` or a
  `dev-dependencies` fixture crate) that loads the **committed Hoodi anchor state** from SSZ and a
  real block, and exposes them to a test without hand-filling any cache
- `crates/types/tests/fixtures/hoodi-config.yaml` — the config side of the same fixture

**Acceptance (falsifiable)**
1. [x] A test can obtain `(BeaconState, SignedBeaconBlock)` decoded **from committed SSZ bytes**, with no
   constructor path that populates `StateCaches`. (SSZ blobs live in the existing Hoodi cache; pin + SHA-256 are committed.)
2. [x] The harness exposes a hook to assert `pubkey_cache_len` on the decoded state — the rig must be able
   to observe an *empty* cache, since that is the condition all three falsifiers turn on.
3. [x] A guard test fails if the harness itself ever hand-fills the cache. `[PRD]` §5.1.2/3 records that
   **eight** existing harnesses hand-fill it; this rig must not become the ninth.

---

## Drift against `[PLAN]` §3/S0a — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **Duration.** `[PLAN]` sizes S0a at 0.5–1 wk. Decomposed, stream B carries 4.9–10.25 pd, which at A-1/A-2's 4 effective pd/engineer-week is **1.2–2.6 wk**. The phase is single-owner on the gate work (the four P0-08 halves are one script and one Makefile), so a second engineer only absorbs the spikes and `S0a-A-01`. Either S0a runs ~1 wk longer than planned, or the four spikes move into S0's first week. Recommend the latter — the spikes have no dependency on the gate being green, and only Q-3 has a wk-1 deadline that matters. |
| 2 | **P0-08's "4 production reads" vs 7 evidence sites.** See `S0a-B-01`. The row's own evidence list does not support its own count; the split is a judgement call and the PR must record it. |
| 3 | **`cargo fmt --all` and plain `cargo test` are already red at `HEAD`.** `S0a-B-03` closes the first. Note for anyone running the gate locally: `make test` is the canonical gate in this repo, not bare `cargo test`. |
