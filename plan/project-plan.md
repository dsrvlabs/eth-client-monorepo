# Project plan — `beacon-core` consolidation

**Sources.** `[PRD]` = `plan/prd.md` (revision 2, 2026-08-15) · `[ARCH]` =
`plan/architecture.md` (2026-08-15 @ `4146791`) · `[Qn]` = `plan/research/`.
Where this plan changes a `[PRD]` §6 stage boundary or duration, the change is stated in §10 with
its reason. Estimates derived from a research brief's S/M/L/XL sizing are marked **⌂**; estimates
that are this plan's judgement are marked **≈** and are ranges, not points.

**The one rule that outranks the schedule.** `[PRD]` §5.0's `Disposition` column is normative for
work order; priority tier ranks severity only. No row dispositioned `deleted @ Sn` is patched
without an explicit re-disposition (`[PRD]` §6 sequencing rule, R-8). No stage is reordered by
tier.

---

## 1. Planning assumptions

| # | Assumption | Consequence if wrong |
|---|---|---|
| A-1 | **Two engineering streams** (§7), plus the per-commit `make test` gate on every commit | Durations in §3 scale roughly linearly for S0–S3a; **S3b does not** — it is wall-clock bound (R-19) |
| A-2 | **Parallel efficiency ≈ 0.8** for S0–S1 (streams touch disjoint crate sets), **≈ 0.6** for S2's last third (both streams converge on `bin/beacon-core` boot, which §4.2 makes single-owner) | Calendar weeks in §3 move by ±25 % |
| A-3 | Person-day sizings below are **effort including tests and review**, not keyboard time. The repo's convention is test-first with a `fmt`/`clippy`/`test` gate before every commit | Under-sizing by ~30 % if read as keyboard time |
| A-4 | Hoodi and the Phase-3 exclusive machine are **single resources**. Only `self-devnet` and `self-devnet-compressed` windows run concurrently with a Hoodi window | S3b lengthens; see the §5 window schedule |
| A-5 | The five foreign clients for OQ-1 / M2e require **external coordination with lead time** | OQ-1 slips to the end of S3b and blocks M2d |

---

## 2. Phase list — durations and the revision from `[PRD]` §6

| Phase | `[PRD]` §6 weeks | **Revised** | Basis for the revision | Ships |
|---|---|---|---|---|
| **S0a — gate restoration** | *(not a phase)* | **wk 1** (0.5–1 wk) | R-5: two blocking CI gates are red on the committed tree, so **no later entry gate is evaluable**. Promoted out of S0's body to a phase with its own exit. | A tree where "did this break something?" has an answer |
| **S0 — correctness floor** | wk 1–3 | **wk 2–8** (6–10 wk; S0a + S0 together occupy wk 1–8) | 35 discrete ledger rows + Loop B (**M**, 1–2 wk ⌂ `[ARCH]` §3.8) + the config-authority pull-forward (D-8) + P0-19 (added after §6 was written). §6's 3 weeks = 6 person-weeks for ~52–82 person-days of content. **Largest single revision in this plan.** | Six services, safe to operate |
| **S1 — fold the EL bridge** | wk 3–8 | **wk 9–16** (7–9 wk) | Body matches §6 at 5–7 wk. Extended because the **S2 entry gate (ADR corpus + P2-E triage, ≈ 4–5 person-weeks) must complete inside this window** — it gates S2 entry, so it cannot start at S2. | 3 containers · engine-fastpath DA end to end |
| **S2 — fold storage** | wk 8–16 | **wk 17–25** (8–10 wk) | Body matches §6. Adds P0-19/3 (**M**, 1–2 wk ⌂ [Q3]) which §6 does not itemise. | 2 processes · archive-hole class gone |
| **S3a — wiring + instrumentation** | *(inside wk 16–28)* | **wk 26–35** (8–11 wk) | Split from S3. Ends when the node is wireable-complete **and the X1/X2 instruments are validated**, before any window opens. | A node that can be soaked |
| **S3b — acceptance windows** | *(inside wk 16–28)* | **wk 36–47** (10–14 wk) | Split from S3, and **wall-clock bound, not effort bound**: 34 clause rows with hard minima (2 × 24 h, ≥6 h exclusive, a retention plateau, a Hoodi week, 20 restart trials, five-client interop). §5 is the derivation. | **M1** and **M2 → 0** |
| **S4 — the fork seam** | after S3 | **wk 48–59** (10–14 wk) | 4a milhouse **M** ⌂ [Q2] · 4b **M** (not L) ⌂ [Q5]/J-17 · 4c ≈ | Gloas is a new module |
| **S5 — Phases 5–7** | after S4 | wk 60+ — **not planned here** | Out of this plan's scope; only its S0 dependencies (P1-F/1, Q-2) are scheduled | Interoperable by construction |

**Total to S3b exit (M1 + M2 = 0): ≈ 47 weeks**, against `[PRD]` §6's implied 28. The two drivers
are S0 (+5 wk) and S3 (+7 to +11 wk). §10 states both changes and their reasons. §11 lists the
compression levers and what each does *not* buy.

---

## 3. Phase detail

### S0a — gate restoration · wk 1

| | |
|---|---|
| **Entry** | none — this is the program's first work item |
| **Contains** | **P0-08** (a) route the 4 production `std::env::var` reads through `cc-config`/allowlist · (b) fix `check-no-env-reads.sh`'s never-resetting `awk` `#[cfg(test)]` exemption and broaden to `env::var`/`var_os`/`vars` · (c) `cargo fmt --check` green · (d) `make ci` mirrors the CI job list. Plus **P1-A/28** (proto breaking baseline hardcodes `branch=develop` on push to `main`) and **P1-C/1** (pin Actions to commit SHAs) — both CI-surface rows that belong with the gate work |
| **Exit (falsifiable)** | **M7 = 0**: `scripts/check-no-env-reads.sh` exits 0 and `cargo fmt --check` is clean on `HEAD`; `make ci` and `.github/workflows/ci.yml` enumerate the same job list, asserted by a test that diffs the two lists rather than by inspection |
| **Out of scope** | any correctness fix; any file move. This phase changes gates and CI only — so that the S0 body's first commit has a trustworthy baseline to diff against |
| **Stream B during this week** | the four XS spikes (Q-9, Q-10, Q-2, Q-3 — §8) and the S0 exit falsifiers' harness skeleton (R-11 test rig). No production commits land until the gate is green |

### S0 — correctness floor · wk 2–8 (6–10 wk)

**Entry:** S0a exit. **Content:** 16 `patch @ S0` P0 rows · ~19 `patch @ S0` P1 rows · `cc-scheduler`
+ Loop B · the config-authority half of S4 pulled forward (**⟡ D-8 change 1**, adopted) · the D-5
restore `block_on` patch · the P1-F/1 ADR (negative half — see R-18) · the S0 exit falsifiers.

| Work item | Rows | Size (person-days) | Basis |
|---|---|---|---|
| Compose surface | P0-01, P0-07, P1-A/27, P1-A/29 | 1–2 | ≈ one-line edits + a block-import smoke test |
| Wire encodings | P0-04, P0-05, P0-06, dup limits ×3 | 3–5 | ≈ |
| Fork choice | P0-09, P0-10, P1-A/19, P1-A/20, P1-A/21 | 5–8 | ≈ P0-09 needs a grown-registry test fixture |
| Chain import | P0-03, P0-11, P0-15 + **⟡ D-5** `block_on` | 4–6 | ≈ D-5's second option (restore-mode `AcceptEngine`) is a **consensus decision needing an ADR**, not an implementation choice |
| Loop B — 5 lanes + `cc-scheduler` | P0-12, P1-D/14 (fire the watch) | **7–12** | ⌂ `[ARCH]` §3.8 (**M**, 1–2 wk incl. test rewrites) |
| **P0-19 trio** | 1 `top_up_pubkey_cache` + 3 call sites · 1b `from_ssz_bytes_hydrated` chokepoint · 2 regression test forbidden to hand-fill | **5–7** | ⌂ [Q3] (S + S + S) |
| **P0-02 class + config authority** | P0-02 (5 functions, ~20 call sites, loader half), P2-A/8, **P2-B/3**, Q-7 audit, Q-6, `ChainConfig` fork accessors, delete 5 duplicated schedule walks (**discharges P1-B/8**, see C-11) | **11–17** | ⌂ [Q5] sizes the P0-02 fix **S** (2–4 d); this plan adds Q-7's preset audit (R-9's named first task), the loader half ([Q5] §1.3), the differing-from-mainnet fixture (R-9), and D-8's accessor pull-forward |
| Storage | P0-13, P0-14, P1-A/1, P1-A/5, P1-B/1 | 7–11 | ≈ P0-14 needs a slot-skipping reorg test |
| Gossip / p2p `patch @ S0` | P1-A/8, P1-A/9, P1-A/10, P1-A/12, P1-A/13 | 6–9 | ≈ |
| State transition / crypto | P1-A/18, P1-B/10, P1-B/12 | 3–4 | ≈ |
| P1-F/1 ADR (negative half) | P1-F/1 | 0.5 | ⌂ [Q4] — the decision costs nothing today |
| **S0 exit falsifiers** | R-11 harness + P0-09 falsifier + baseline A/B run | 3–5 | ≈ — see the exit criteria below |
| | **Total** | **56–87 pd ≈ 11–17 pw** | → **6–10 calendar wk** at A-1/A-2 |

**Exit criteria (all falsifiable):**

| # | Criterion | Instrument | Why it is here |
|---|---|---|---|
| E0.1 | M7 = 0 sustained; `make ci` green | CI | carried from S0a |
| E0.2 | **M6b = 0** — `pub mod network` deleted; omitting `&ChainConfig` is a compile error | `grep -c 'constants::network::'` = 0 | `[PRD]` §7.3 |
| E0.3 | Both preset vector suites green **against a fixture that differs from mainnet on all five P0-02 constants** — not only `hoodi-config.yaml` | `crates/spec-tests` | R-9: the naive fix passes on Hoodi and diverges on a customising devnet |
| E0.4 | **R-11 executed**: decode the committed Hoodi anchor state from SSZ, call `process_block` on a real block; assert import succeeds, and assert the negative (omit the top-up ⇒ `CachePoisoned`) | the ~30-line test named in [Q3] §5 | **P0-19's severity is a code-path trace, not an executed reproduction.** This is the S0 gate that converts it |
| E0.5 | **P0-09 falsifier executed**: an attestation naming a validator activated *after* the anchor, against a grown registry, does not hit `ValidatorIndexOutOfRange` | new test | **⟡ this plan's addition** — E0.4 covers P0-19 only; P0-09 has the same "invisible to `make ci`" shape and needs its own falsifier (R-16) |
| E0.6 | M5 — signature verification running on the production import path; `NoVerification` reachable only from restore-replay | `services/chain` test | `[PRD]` §7.3 |
| E0.7 | §9.0 A/B baseline recorded (all six binaries, ≥1 h self-devnet, three metric families diffed) | stage exit note | `[ARCH]` §9.0 — establishes the baseline every later stage diffs against |
| E0.8 | **M4 = 0** — an **off-host port scan** reaches none of `9001`–`9006`, and the metrics ports `9101`–`9106` answer on `127.0.0.1` only | scan from a second host, recorded in the exit note | P0-01 closes both HIGH security findings; J-9 extends it to the metrics ports. Without this check S0 ships the security fix with nothing asserting it |
| E0.9 | **M3 tracking artifact exists** — a `Discharged by` column on `[PRD]` §5.1, updated at every stage exit with the commit SHA (patch) or the stage id (deletion), with **zero blank cells for rows this stage claimed** | `plan/prd.md` §5.1 | M3's target is "0 open, **with the discharging commit or stage recorded per row**". Nothing in either source creates the artifact that makes that readable |

**Out of scope for S0 — stated so creep is visible:**

- Any `deleted @ Sn` row: P1-A/4, P1-A/22, P1-A/23, P1-B/9, P1-D/13 (`deleted @ S2`).
  **P1-A/22 and P1-A/23 carry a diagnostic obligation instead** — E0.4 is that obligation (R-13).
- Any structural move. S0 moves no code between crates (`[ARCH]` §9.1: *"Moves: nothing"*).
- Loop A (gossip validation scheduling) — `[ARCH]` §3.8 sequences it **after S3**, and Q-8 says its
  dominant stall is analytic while gossip is unsubscribed.
- `drop_during_sync` shedding policy — `[ARCH]` B.1 does not adopt it; revisit at S3.
- The four `deleted @ Sn` P0 halves: P0-07/P0-13 are patched here **and** deleted at S2; P0-15 is
  patched here **and** deleted at S1. Both facts are true; neither is a reason to skip the patch.

### S1 — fold the EL bridge · wk 9–16 (7–9 wk)

| | |
|---|---|
| **Entry** | S0 exit criteria E0.1–E0.7 all met, under `make ci` |
| **Moves** | `services/engine/{transport,jwt,state,version,errors,capabilities,config}.rs`, `methods/`, `fastpath/` → `crates/engine-api` **verbatim with tests** |
| **Adds** | `cc-seam` (`ChainIngress`, `P2pEgress`, `SeamError`, the 11-test conformance suite) · **E1/E2 typed but not moved** (R-1 discharge order) · the core-liveness probe (M8) · real KZG replacing `kzg: None` |
| **Deletes** | `services/chain/src/engine_client.rs` (E3 and P0-15's surface), `EngineStream`'s engine half, the `trusted_local` bool, `services/engine/src/{service,main,inject}.rs` |
| **Ledger rows** | P1-E/S1, P0-16 (engine half), P1-A/24, P1-A/25, P1-A/26, P1-B/11 (deleted), P1-D/11 (typing, S1–S2), P2-D/19, P2-B/5 |

| Work item | Size (pd) | Basis |
|---|---|---|
| `crates/engine-api` extraction (verbatim + tests) | 8–12 | ≈ a move, but the JWT/health-machine/three-lane transport is delicate |
| `cc-seam` + conformance suite (11 tests, both impls) | 10–14 | ≈ new crate; `[ARCH]` §2.1–2.2 specifies the shape |
| E1/E2 trait definitions + `check-crate-dag.sh` prohibitions (`cc-p2p` ↛ `cc-chain`, and back) | 3–4 | ≈ |
| **⟡ D-10 security re-point** — `check-crate-dag.sh` JWT rule names `cc-engine-api` **in the same PR that creates the crate** | 1–2 | `[ARCH]` §6.2: a stage that lands the crate without it has silently deleted a mechanically-enforced invariant |
| Core-liveness probe (M8) + injected-black-hole demonstration | 5–8 | ≈ `[ARCH]` §7.2 |
| P1-A/24–26 + P2-D/19 + P2-B/5 (blob fastpath, real KZG, engine policy edges) | 6–9 | ≈ |
| S1 testability test (`import → engine → fork-choice`, `newPayload` timeout ⇒ **deferral** not park) | 2–3 | `[ARCH]` §8.2 — the direct regression test for P0-15 |
| A/B run + exit note | 2–3 | `[ARCH]` §9.0 |
| **Stream-B concurrent: S2 entry gate** (§4) | **19–26** | see §4 |
| | **Total 56–81 pd** | → **7–9 calendar wk** |

**Exit criteria:**

| # | Criterion | Instrument |
|---|---|---|
| E1.1 | §9.0 A/B clean: `cc_chain_import_result{*}` distribution and head-lag buckets match the S0 baseline; **any non-zero diff in the overflow-counter family with zero diff in the other two is a stage blocker, not a curiosity** (`[ARCH]` §9.0/4) | stage exit note |
| E1.2 | **M8 demonstrated red** against an injected engine black-hole | healthcheck output in the exit note |
| E1.3 | `cargo test -p cc-seam` passes against **both** impls (`InProcess` and `Ipc`) | CI |
| E1.4 | `check-crate-dag.sh` names `cc-engine-api` in the JWT rule and `cc-chain` is **not** on the grandfather list | CI |
| E1.5 | 3 containers run; engine-fastpath DA works end to end on self-devnet | devnet run |
| E1.6 | **S2 entry gate discharged** (§4) — this is an S1 exit item, not an S2 start item | §4 table |

**Out of scope for S1:** moving any transport (E1/E2 are typed only — R-1's discharge order is
types-then-transport); merging moved services into `beacon-core` as **modules** (`[ARCH]` §9.2 —
deletes `check-crate-dag.sh`'s five named prohibitions, ⟡ D-1); any direct `cc-chain` ↔ `cc-p2p`
call not routed through `cc-seam` (forecloses D-2); storage work.

### S2 — fold storage · wk 17–25 (8–10 wk)

| | |
|---|---|
| **Entry** | **§4's gate discharged** — ADR corpus (M11) + P2-E triage (**M12 = 0**), both completed inside S1 |
| **Moves** | `services/storage/*` → `crates/storage-core`; `services/chain/*` → `crates/chain-core`; boot into `bin/beacon-core` |
| **Adds** | `ArchiveWrite` on the seam; direct typed column ingest with the top-of-batch continuity bind; typed event structs |
| **Deletes** | E4 `RestoreFromStore` (server **and** client), `services/chain/src/restore.rs` (1,227 lines), `services/storage/src/{write_behind,restore_client}.rs`, E5/E6/E7 transport, the ring's durability role |
| **Ledger rows** | P1-E/S2, **P0-18**, **P0-19/3** (+ /4 optional), P1-A/2, P1-A/3, P1-A/6, P1-B/2, P1-B/3, P2-B/1, P2-B/2; **discharged by deletion**: P0-07, P0-13, P1-A/4, P1-A/22, P1-A/23, P1-A/27, P1-B/9, P1-D/13, P1-D/14 (second half) |

| Work item | Size (pd) | Basis |
|---|---|---|
| `crates/storage-core` + `crates/chain-core` extraction | 12–16 | ≈ |
| `bin/beacon-core` boot: one process opens redb before any subsystem starts | 5–8 | ≈ `[ARCH]` §4.2 — **single-owner work item**, the A-2 efficiency drop |
| `ArchiveWrite` + direct column ingest + continuity bind | 8–11 | ≈ `[ARCH]` §4.3 |
| Event bus demoted to API/observer; typed event structs | 4–6 | ≈ |
| **P0-18** — three storage scale time bombs (interned table-name exhaustion at ~30 d; contig-walk cap below the serve window; multi-GB invariant scans blocking `open`) | 8–12 | ≈ — and they become **restart-critical-path** as of this stage (`[ARCH]` §4.2) |
| **P0-19/3** — move the pubkey cache off `BeaconState` onto `TransitionContext` | **5–10** | ⌂ [Q3] (**M**, 1–2 wk). **Hard prerequisite for S4a** |
| Storage `patch @ S2` rows (7) | 8–12 | ≈ |
| Rollback procedure doc + **restart drill rehearsal** (WriteCursor stream-seq → batch-seq requires a clean shutdown before rollback) | 3–5 | `[ARCH]` §9.1 — the only stage with a data-shape consequence |
| S2 testability test (`import → durable`, one TempDir, one redb, **no gRPC anywhere**) | 3–4 | `[ARCH]` §8.2 |
| A/B run + exit note | 2–3 | |
| | **Total 58–87 pd** | → **8–10 calendar wk** |

**Exit criteria:** E2.1 §9.0 A/B clean (same three families, same blocker rule) · E2.2 two processes
run on self-devnet · E2.3 the `import → durable` in-process test exists and runs in CI (**half of
M9**) · E2.4 the rollback procedure is **rehearsed**, not documented — a clean shutdown, a redeploy
of the previous topology against the same data directory, and a successful open · E2.5 P0-19/3
landed, with a measurement showing the state clone no longer deep-copies the pubkey map (this is
what makes S4a's O(1) claim true rather than asserted) · E2.6 M10 restated on the **8-edge**
denominator (⟡ D-2): E3–E7 deleted, E1/E2/E8 remaining.

**Out of scope for S2:** wiring any dead island (that is S3 — P0-16, P0-17); selecting the E1/E2
transport; changing the redb on-disk schema (rollback depends on it being unchanged); any direct
`cc-chain` ↔ `cc-p2p` call outside `cc-seam`.

### S3a — wiring completion + instrumentation · wk 26–35 (8–11 wk)

**Entry:** S2 shipped; **D-2's criteria X1–X5 fixed in advance** (`[PRD]` §9 requires this before S3
opens; `[ARCH]` §9.1 adds that the *instruments* must be too).

| Work item | Rows | Size (pd) | Basis |
|---|---|---|---|
| **P0-19/1 must already be landed** (S0) before this begins | — | 0 | R-12: gossip does not cause the cache stall but **widens its blast radius to the live-import path, where the failure names no cause** |
| P0-17a gossip subscribe + §5.6 scoring | P0-17a | 8–12 | ≈ |
| P0-16 real DA feed (p2p half) | P0-16 | 8–12 | ≈ the fork's central mechanism has no production path today |
| P0-17b `kzg_tx` reconnect + slot-bounded wait + redrive hoist | P0-17b, P1-B/7 | 4–7 | ⌂ `[ARCH]` §3.8 (**S/M**) |
| P0-17c serve-window publication | P0-17c | 4–6 | ≈ — Q-10 must have closed the "never published" claim first |
| P0-17d + P1-D/12 backfill: client write method, oldest-first cursor deadlock on empty slots, below-anchor leak | P0-17d, P1-A/16, P1-A/17 | 10–15 | ≈ ~3,200 lines with zero call sites |
| `cc-wire` extraction (three codec copies deleted) | P1-A/7, P1-B/6 | 6–9 | ≈ |
| p2p `patch @ S3` rows | P1-A/11, /14, /15; P1-B/4, /5 | 6–9 | ≈ |
| P1-D/18 `fault_mode` out of production paths | P1-D/18, P2-C/1 | 3–5 | ≈ 1,900-line global consulted by the column validator |
| **X1 instrument** — supervisor panic counter, labelled by task | ⟡ D-11 | **3–5** | see the dedicated rows below |
| **X2 instrument** — `Ipc` impl `Backpressure` counter under real peer load | `[ARCH]` §2.5 | 2–4 | |
| **X3 restatement + incident classification procedure** | ⟡ D-3 | **2–4** | see the dedicated rows below |
| M9 full test (`gossip receipt → durable`, entering at `ChainIngress`) | M9 | 4–6 | `[ARCH]` §8.2 ⟡ D-12 — reading (a); the wire half is a separate thinner test |
| | | **Total 58–90 pd** | → **8–11 calendar wk** |

**The X1 instrument — owner, deadline, and what "done" means.**

| | |
|---|---|
| **Why it is on the critical path** | `services/p2p/src/supervisor.rs` implements a per-task panic policy (ADR P2-13): a task that panics is **caught and restarted**, so a libp2p panic may never become a process abort and may never be attributed. X1 would return **0 for the wrong reason**, and `[PRD]` §9 explicitly forbids reading absent evidence as a vote for the default (⟡ D-11) |
| **Owner** | Stream B (p2p) |
| **Deadline** | **≥ 2 weeks before the first S3b window opens** — i.e. landed by wk 33 against a wk 36 window start |
| **Exit — validated, not shipped** | A counter incremented in the supervisor's catch path, labelled by task, **demonstrated incrementing against a deliberately injected panic in a p2p task on the self-devnet**. A counter wired to the wrong catch path still returns 0 for the wrong reason |
| **If it slips** | The soak produces no X1 evidence and D-2 defaults to Single Hull **by accident rather than by data**. The mitigation is not "decide anyway" — it is to hold the window |

**The X3 deliverable — the same problem arriving through the other door.** X1 and X3 are the **only
two criteria that can carry Gatehouse & Keep**, and **both** are unmeasurable today: ⟡ D-11 for X1,
⟡ **D-3** for X3. Fixing only X1 leaves the decision defaulting by accident anyway.

| | |
|---|---|
| **Why X3 as written cannot be measured** | X3 asks whether the jittered reconnect loop *survives an in-process port unchanged*. Under `InProcess` it is **not ported — it is not instantiated** (`[ARCH]` §2.5). There is nothing to port, so the question has no answer rather than a clean one |
| **Deliverable 1 — the restatement, formally adopted** | ⟡ D-3's form: *"did any incident in the soak window require a reconnect-and-resume that a single process could not have handled by restarting?"* `[PRD]` §9 requires the criteria be **fixed before S3 opens**; X3 as written is not one, so adopting the restatement is itself an S3a deliverable, recorded as an ADR alongside the D-2 decision record |
| **Deliverable 2 — the classification procedure** | Unlike X1, restated-X3 is **not read off a counter** — it is read off *classified incidents*. If nobody defines the classification before W2, X3 returns "no incidents" for the same wrong reason X1 would have returned 0. The procedure names, per incident: what disconnected, whether the seam resumed from a cursor, and whether a whole-process restart would have lost committed progress |
| **Owner / deadline** | Stream B (p2p), with the soak operator as reviewer. Same deadline as X1: **landed by wk 33, ≥ 2 wk before W0** |
| **Definition of done** | The restatement is committed as the operative X3 wording, **and** the classification procedure has been exercised against at least one deliberately induced p2p disconnect on the self-devnet, producing a filled-in incident record |

**Exit criteria:** E3a.1 every dead island in `[PRD]` §1.A has a production sender/caller, asserted
by a test per island, not by inspection · E3a.2 M9 exists and runs in CI · E3a.3 **X1 counter
demonstrated against an injected p2p-task panic** · E3a.4 X2 counter emitting non-zero under
saturating self-devnet load · E3a.5 M2e precondition: the five foreign clients are **sourced and
scheduled** (A-5 lead time) · E3a.6 both seam impls build and pass conformance; **neither is
deleted** · **E3a.7 X3 restated and its classification procedure exercised** against an induced p2p
disconnect · E3a.8 all five of X1–X5 have a named instrument and a named reader, recorded before W0
opens (`[PRD]` §9: *"fixed before S3 opens"*; `[ARCH]` §9.1 adds that the instruments must be too).

**Out of scope for S3a:** **selecting the E1/E2 transport** — that is an S3b *exit* decision, not an
S3a one (R-14); deleting the losing seam impl (`[ARCH]` §9.2 — it is the conformance suite's second
subject); Loop A's queue taxonomy and `ValidationPoolState` sharding (**L**, `[ARCH]` §3.8
sequences it after S3, and Q-8 is only measurable here); `bin/serve-probe`'s independent codec
(ADR P4-12's independence claim is why P0-06 exists).

### S3b — acceptance windows · wk 36–47 (10–14 wk)

**Entry:** S3a exit, **including E3a.3** (X1 validated). **This phase is wall-clock bound; adding
people does not compress it** (R-19). Its content is §5's window schedule.

**Exit criteria:** **M1 observed** (a foreign peer's block imports end to end on Hoodi and the node
holds head: `phase-2-soak.md` clause 2 + clause 3 with the `le=1` head-lag bucket ≥ 0.95, then
sustained) · **M2 = 0** (34 clause rows discharged **by running windows**; a row edited to remove
`NOT_RUN` without a run is a §7.5 anti-metric violation, not progress) · **M2e = 5/5** ·
**M6 observed** (accepted-deposit set matches a reference client over the same window, on a
non-zero-GVF venue) · **D-2 resolved** by reading X1–X5 off the collected data, with the decision
recorded as an ADR — and with X1 and X3 each carrying a **positive or negative reading**, never an
absent one (`[PRD]` §9: *"absent evidence is not a vote for the default"*).

**Out of scope for S3b:** any code change that is not a window blocker. A wiring gap found mid-window
is a defect fixed and the window **re-run**, not a window amended.

### S4 — the fork seam · wk 48–59 (10–14 wk)

**Entry:** Phase 1–4 clauses discharged (M2 = 0) · **P0-19/3 landed at S2** (else milhouse's O(1)
clone is a lie) · **Q-3 answered** (see below).

| Sub-stage | Contains | Size | Basis |
|---|---|---|---|
| **4a** | milhouse swap on `BeaconState` (P1-D/10); deletes two of the four hand-synchronised schema places (P1-D/15, and **P2-B/6** rides with it) | **M** ⌂ [Q2] — 3–5 wk | in-memory swap is a net deletion, mirroring Lighthouse PR #5533; on-disk diffs are **L** and **deferred** |
| **4b** | `ForkName::Gloas`; superstruct on the **exactly 6** reshaped containers; per-fork STF dispatch as **monotone capability predicates** (`fork.gloas_enabled()`), ~10 lines on the existing ordered `ForkName`; `upgrade_to_gloas` | **M**, re-sized from L ⌂ [Q5]/J-17 | per-fork modules copied N times reintroduce the same N-copies hazard this program exists to delete |
| **4c** | the 13 new containers; **`DataColumnSidecar(Fulu, Gloas)`** (⟡ D-7 — omitted by both sources); blocking total-coverage enforcement | ≈ 3–4 wk | |

**The ordering is conditional, and the condition is cheap.** ⟡ D-8 sequences 4a before 4b because
milhouse reduces the state-schema edit from four synchronised places to two before a −1/+9-field
fork edits it — at no extra cost. **But Q-3 (does `superstruct` compose with milhouse's
`List<T, N, U>` third type parameter?) can invert that ordering.** It is **one hour of reading
Lighthouse's `beacon_state.rs`**, and §8 schedules it in wk 1, not at S4 entry.

**Out of scope for S4:** milhouse on-disk diffs (**L**, deferred by [Q2]); any Phase 5 code —
`[PRD]` P1-E/S4's whole point is that the seam lands *before* it.

---

## 4. The S2 entry gate, sized — the ADR corpus and the P2 triage

`[PRD]` R-6 and ⟡ D-13/D-14 make this a **work item with its own estimate**, not a documentation
chore. `[ARCH]` §10.1 measures the corpus at **207 citations / 58 distinct ids / 541
`Architecture §` occurrences / 0 ADR files**. The gate is restated (⟡ D-13) as *"every cited id
resolves to a committed document and the reconciliation table has no unclassified rows"* — **not**
"write 58 ADRs".

| Bucket | Count | Size each | Total (pd) | Note |
|---|---:|---|---:|---|
| **(a) re-derivable** — write it by reading the citation site | **43** | ~1 h ⌂ *(`[ARCH]`'s estimate, not a measurement)* | 6–9 | parallelises across writers |
| **(b) needs a decision recorded** | **12** | 0.5–1 d ≈ | 6–12 | **does not parallelise onto one writer** — each needs the person who owns the subject |
| **(c) stale — delete the citation** | **3** | ~0.5 h | 0.5 | |
| **The reconciliation mechanism itself** (J-16 — it does not exist; `[ARCH]` cites its own §10 ten times) | 1 | ≈ | 1–2 | **first task**, not last |
| **CI resolver gate** — extract every `ADR[ -]<id>` / `Architecture §<n>` and fail when it does not resolve under `docs/adr/` | 1 | ≈ | 1–2 | without it the corpus re-diverges the week after the gate passes |
| **R-P2-triage** (**= M12: 35 → 0**) — one adversarial verification pass over all 35 P2-E rows, each promoted with a disposition or dismissed with a reason | 35 | ≈ | 5–7 | rows **1, 6, 8, 21, 31** most likely to promote; **8 and 31 bear directly on P0-02 and P0-06** and should be triaged during S0, not S1 |
| **M3 ledger update** — carry the `Discharged by` column (E0.9) forward for every row S1 patched or deleted | — | ≈ | 0.5 | the artifact is created at S0; each stage exit maintains it |
| | | | **19–32 pd ≈ 4–6.5 pw** | |

**⟡ This plan's addition — the gate as literally worded is not satisfiable at S2 entry.** At least
three (b)-class ids are decided by *later* stages: `ADR-07` is *"decided again at S3"*, `ADR-P1-04`
is *"supersede at S4a"*, and `ADR-R-02` is *"created at S2"*. Resolution: the gate is satisfied by a
committed document per id, where a document may carry `Status: proposed` plus an explicit
**"revisit at Sn"** line — the MADR format already has the field. Without saying this, someone
either stalls S2 or fudges the gate. (R-17.)

**Scheduling:** this runs as stream B's second half **inside S1** (wk 9–16), because it gates S2
*entry*. Starting it at S2 makes it a serial prefix to the longest structural stage.

---

## 5. The acceptance-window schedule (M2 → 0)

M2's unit is a **clause row**, and rows are discharged only by running windows at their named
venues. Baseline **34**: Phase 1 (3 named clauses) + Phase 2 (9) + Phase 3 (10) + Phase 4 (11) +
OQ-1 (1). Venue is load-bearing — `[PRD]` §7.5 makes "run at the wrong venue" an anti-metric.

| W | Window | Venue | Duration (incl. one re-run allowance) | Discharges | Prerequisites | Re-run cost |
|---|---|---|---|---|---|---|
| **W0** | Instrument validation + §9.0 A/B | self-devnet | 1–2 wk | none | S3a exit | cheap |
| **W1** | Phase 1 clause 1 — spec-vector suites green both presets, **skiplist empty** | CI | concurrent | 1 row | **Q-9** (does the coverage check *report* or *fail*?) | cheap |
| **W2** | Phase 1 clauses 2–3 — ≥ 24 h continuous Hoodi soak + timing budgets (epoch p95 ≤ 1000 ms, `process_block` p95 ≤ 400 ms). **Carries M6** — see below | **Hoodi** | 1–1.5 wk | 2 rows **+ M6** | W0 | full 24 h |
| **W3** | Phase 2 — 9 rows: peers ≥ 25 **and** custody ≥ 8 over 24 h (`min_over_time`, **one dip fails**), 10-min gap recovery within 32 slots, withheld-column deferral+recovery, scoring across the −4000 bucket | **Hoodi** | 1.5–2 wk | 9 rows · **M1 is read here** | W2 | **full 24 h per dip** — budget 2 attempts |
| **W4** | Phase 3 — 10 rows: `is_optimistic==0` ≥ 99 % over ≥ 6 h / ≥ 20 finalized epochs with bootstrap excluded; geth head within 1 block ≥ 99 % with zero `-38002`/`-38006`; getBlobsV2 fastpath (non-zero complete **and** zero engine-sourced columns, else FAIL) | **exclusive machine** | 1.5–2 wk | 10 rows | blockers B1–B5 cleared; **serialises against every other window wanting that box** | full window |
| **W5** | Phase 4 clause 1 — 20/20 restart trials ≤ 60 s, `following_head=1`, identical `GetHead` roots | Hoodi | 0.5–1 wk | 1 row | S2 (R-10: before S2, every restart costs an external checkpoint re-sync) | per-trial |
| **W6** | Phase 4 clause 3 **row A** — compressed-retention plateau, **discharging** | `self-devnet-compressed` | 1.5–2 wk | 1 row | separate hardware | full plateau |
| **W7** | Phase 4 clause 3 **row B** — Hoodi week, **`confirmation, non-discharging`** | Hoodi | 1 wk | **0 rows** | runs concurrent with W6 | — |
| **W8** | Phase 4 clauses 4–7 — full-window block and column serve; clause 6's negative side (`ResourceUnavailable` **only** below `eas`); clause 7 `cc_storage_earliest_available_slot == cc_p2p_*` | Hoodi | 1 wk | 4 rows | P0-17c wired | full window |
| **W9** | Phase 4 clause 2 — cursor fallback in three stages: attribution / hole recorded / **hole closed** (discharged only when the third is present) | Hoodi | 1 wk | 1 row | | |
| **W10** | **OQ-1** foreign-peer probe, five clients × 5–10 peers each + **M2e** codec validation 5/5 | Hoodi | 2–3 wk | 1 row + M2e | **external lead time — begin sourcing at S2** (A-5) | high — reschedules five parties |
| | | | **Σ 10–14 wk** with W6/W7 and W10's outreach overlapped | **34 rows** | | |

**M6 needs a window and neither source gives it one.** M6 — *"deposits accepted on a non-zero-GVF
network; matches a reference client over the same window"* — is the **live falsifier for P0-02** in
exactly the way M2e (W10) is for P0-04/05/06. E0.2/E0.3 discharge the code-level metric (**M6b**,
`constants::network::` call sites → 0); they cannot discharge M6, which is an observation. Instrument:
over W2's 24 h Hoodi window, diff our accepted-deposit set against a reference client's on the same
slot range — Hoodi's GVF is `0x10000910`, so a node still resolving the domain from the `mainnet`
preset accepts **zero**, and the comparison is unambiguous. If W2's window contains no deposits, M6
rolls to W3 and, failing that, to a targeted devnet with injected deposit traffic — the **venue must
have a non-zero GVF**, which is the whole point of the metric.

**Three things this table makes visible that a week range does not:**

1. **W6 and W7 must not be merged.** Clause 3's two rows are separately marked; the plateau at
   `self-devnet-compressed` is the discharging one and the Hoodi week is explicitly
   *non-discharging*. Merging them discharges nothing and looks like progress.
2. **W4 is a resource bottleneck, not a duration.** The exclusive machine serialises against
   everything else; it is the reason W6 is scheduled on separate hardware.
3. **W3 carries M1.** M1 is not a separate window — it is read off Phase 2 clause 2 (DA-gated
   import) and clause 3 (head lag). Everything before W3 is preparation for one observation.

---

## 6. Dependency graph

```
  P0-08 (S0a) ──────────────────────────────────────────────────────────► every entry gate  [R-5]
     │
     ├─► Q-7 preset audit ──► P0-02 class ──► ChainConfig fork accessors (D-8 ch.1) ──► S4b
     │                            ▲
     │                       Q-6 (SECONDS_PER_SLOT)
     │
     ├─► P0-19/1+1b+2 ──┬──► R-11 test (E0.4) ──┬──► P0-17a gossip subscribe (S3a)   [R-12]
     │                  │                        └──► diagnose P1-A/22, P1-A/23 BEFORE S2 deletes
     │                  │                             the restore surface              [R-13]
     │                  └──► P0-19/3 (S2) ──► P1-D/10 milhouse (S4a) ──► Gloas schema (S4b)
     │                                             ▲                          [D-8 ch.2]
     │                                         Q-3 (1 h) — can INVERT this order
     │
     ├─► Loop B 5 lanes (S0) ──► P0-12 discharged ──► gossip timeliness testable at S3a
     │
     └─► P1-D/11 type the contracts ──► cc-seam (S1) ──┬──► E3 deleted (S1)
                                                        ├──► E4/E5/E6/E7 deleted (S2)
                                                        ├──► M9 full test (S3a)
                                                        └──► X2 instrument ──┐
                                                                              │
  ADR corpus + P2-E triage (during S1) ──► S2 ENTRY                          │
                                                                              │
  supervisor panic counter (X1)  ───────────────────────────────────────────►├──► S3b windows
  X3 restated + incident classification (D-3) ──────────────────────────────►│      │
  two-process + Ipc topology held through the soak (R-14) ──────────────────►│      │
  five foreign clients sourced (from S2) ────────────────────────────────────►┘      ├─► M1 (W3)
                                                                                     ├─► M2=0
  Q-9 ──► Phase 1 clause 1        Q-10 ──► P0-17c serve window                       └─► D-2 decision
  Q-2 ──► P1-F/1 backend clause                                                            │
                                                                        Phase 1–4 discharged ──► S4
```

**The real serializations — the ones that are not obvious from the stage table:**

| # | Blocker | Blocks | Why it is genuinely serial |
|---|---|---|---|
| D1 | **P0-08** | every stage entry gate | R-5: a red baseline makes "did this change break something?" unanswerable. This is why S0a exists |
| D2 | **P0-19/1** | **P0-17a** | R-12: gossip does not *cause* the cache stall (it fires on boot today) but it **widens the blast radius to the live-import path, where the failure names no cause** — no Reject, no descore, health DAG green |
| D3 | **R-11's test** | S2's deletion of `restore.rs` | R-13: P1-A/22 and P1-A/23 are `deleted @ S2`. If P0-19 is their common cause and it is never tested, the deletion **hides** the bug and it reappears in `beacon-core`, which still decodes a state on restore |
| D4 | **P0-19/3** | **P1-D/10 milhouse** | `StateCaches` derives `Clone`; an ~80–100 MB map deep-copies on each of the 3–5 state clones per import. Landing milhouse first makes its O(1) clone a lie and the migration measures no improvement |
| D5 | **milhouse (4a)** | **Gloas schema (4b)** | ⟡ D-8: four hand-synchronised schema places → two, before a −1/+9-field fork edits them. One of the four (`StateField` discriminant order) yields a **wrong state root**, not a compile error, when it drifts |
| D6 | **Q-3** | the 4a→4b **order itself** | if `superstruct` does not compose with milhouse's third type parameter, D5 inverts. 1 h of reading; scheduled wk 1 |
| D7 | **P1-D/11 typed contracts** | every S1–S2 transport move | R-1: the failure mode is silent, so there is no report to fix it from (ADR-R-01's rejected alternative) |
| D8 | **ADR corpus + P2-E triage** | **S2 entry** | must therefore run *inside S1* — see §4 |
| D9 | **X1 counter, validated** — and **X3 restated with a classification procedure** | the first soak window | ⟡ D-11 and ⟡ D-3. X1 and X3 are the only two criteria that can carry Gatehouse & Keep, and both are unmeasurable as they stand (§3/S3a) |
| D10 | **Two-process + `Ipc` topology** | X1 **and** X2 readability | ⟡ **this plan's finding** — see R-14 |
| D11 | **S2** | Phase 4 clause 1 (W5) | R-10: `chain` cannot restart without external checkpoint re-sync, and doing so permanently holes the archive. 20/20 restart trials are not affordable before S2 removes the cause |
| D12 | **Five foreign clients sourced** | W10 (OQ-1 + M2e) | external parties; lead time starts at S2 |

---

## 7. Critical path

Named explicitly, computed by **duration**, not by severity:

```
S0a  P0-08 gate restoration                              0.5–1 wk
S0   P0-02 class (Q-7 audit → 5 fns → ~20 call sites →   2.5–3.5 wk   ← longest S0 chain
     loader half → differing fixture → accessors)
     ‖ Loop B 5 lanes (M, 1–2 wk) runs concurrent on the other stream
S0   E0.4/E0.5 exit falsifiers                           0.5–1 wk
S1   cc-seam + conformance suite → typed E1/E2           2–3 wk       ← gates every later move
S1   ‖ ADR corpus + P2-E triage (stream B)               4–6.5 pw     ← gates S2 entry
S2   storage+chain extraction → beacon-core boot         4–5 wk       ← single-owner join
S2   P0-19/3 cache off BeaconState (M)                   1–2 wk       ← gates S4a
S3a  gossip subscribe + DA feed + backfill               6–8 wk
S3a  X1 supervisor counter, validated                    0.5–1 wk     ← gates the soak
S3a  X3 restated + classification exercised              0.5–1 wk     ← gates the soak
S3b  W2→W3→W4→W5→W8→W9→W10 (Hoodi + exclusive box)      10–14 wk     ← wall-clock bound
S4a  milhouse                                            3–5 wk
S4b  Gloas schema + STF predicates                       3–5 wk
```

**The panic-attribution instrumentation is on this path by construction, not by size.** It is 3–5
person-days of work that gates 10–14 weeks of windows. If it lands late, the windows either wait or
produce no X1 evidence — and D-2 then defaults to Single Hull *by accident rather than by data*,
which `[PRD]` §9 forbids in as many words. Owner: stream B. Deadline: wk 33 (≥ 2 weeks before W0).
Definition of done: **demonstrated incrementing against an injected panic in a p2p task**, labelled
by task — not merely merged. **X3's restatement and classification procedure carry the same
deadline and the same definition-of-done shape**: X1 and X3 are the only two criteria that can
carry Gatehouse & Keep, so fixing one and not the other leaves the decision defaulting by accident
through the other door.

**What is not on the critical path, despite being P0:** P0-01, P0-03, P0-04/05/06, P0-07, P0-09,
P0-10, P0-11, P0-13, P0-14. All are ship-blockers by severity; none gates a downstream stage. They
fill stream capacity around the path above. This is exactly the tier-vs-disposition split `[PRD]`
J-6 makes explicit, and it is why they are not reordered to the front.

---

## 8. Spike schedule

Cheap open questions are scheduled as spikes, not carried as unknowns. Sizes from `[ARCH]` B.2.

| Q | Question | Size | **Scheduled** | Blocks |
|---|---|---|---|---|
| **Q-9** | Does `crates/spec-tests`' coverage check *report* or *fail*? | XS | **wk 1** | M2a's "skiplist empty" clause (W1) |
| **Q-10** | Is the serve window truly never published? (only the `u64::MAX` seed was verified) | XS | **wk 1** | P0-17c's framing; J-3 asks for this before S3 |
| **Q-3** | Does `superstruct` compose with milhouse's `List<T, N, U>`? | 1 h | **wk 1** | **the S4a→S4b order — and it can invert it** |
| **Q-2** | Does redb give a **fail-fast** cross-process exclusive open, or does it block? | S | **wk 1** | the slashing-DB backend clause of **P1-F/1, which is `write @ S0`** (R-18) |
| **Q-7** | Are there other config-scoped values that landed in the preset by the same mistake? (`preset.rs` was never audited) | S | **first task of P0-02** | completeness of the `pub mod network` deletion; R-9 names it as the first task |
| **Q-6** | Is `SECONDS_PER_SLOT` formally removed upstream, or merely absent from `configs/mainnet.yaml`? | S | with the S0 config work | the shape of the fix (accept both keys, or migrate) — and J-12, which may warrant its own P0 |
| **Q-1** | `check-crate-dag.sh` allowlist minimality (`--check-unused`) | S | S1 open | nothing; hygiene |
| **Q-4** | superstruct's compile-time cost on this workspace | spike | before S4 commits (wk 46) | superstruct-vs-hand-written |
| **Q-5** | Does `specs/gloas/partial-columns/` change the DAS sidecar shape? | read | before S4 scoping (wk 46) | the scope of ⟡ D-7 |
| **Q-8** | Is the 12 s gossip→chain wait ever reached in practice? | — | **explicitly NOT a spike** | measured at S3a with gossip live; moving it earlier is meaningless while gossip is unsubscribed |

---

## 9. Parallel work streams

**Stream A — consensus core.** `crates/{types,state-transition,fork-choice,store}`,
`services/chain`, later `crates/{chain-core,engine-api}`.
**Stream B — edge & platform.** `services/{p2p,storage,engine}`, `crates/{libp2p,proto}`, CI,
compose, docs/ADRs, soak operations.

| Phase | Stream A | Stream B | Genuinely parallel? |
|---|---|---|---|
| S0a | (idle on production commits — spikes only) | P0-08, P1-A/28, P1-C/1 | n/a — one week, one owner |
| S0 | P0-19 trio, P0-02 class + accessors, Loop B + `cc-scheduler`, P0-03/09/10/11/15 + D-5, P1-A/18–21, P1-B/10,12 | P0-01/04/05/06/07/13/14, P1-A/1,5,8,9,10,12,13,27,29, P1-B/1 | **Yes** — disjoint crate sets. **One conflict: P0-19 and P0-02 both edit `crates/state-transition`.** Both are stream A, sequenced P0-19 first (it is the only P0 both firing today and invisible to every signal — R-12) |
| S1 | `crates/engine-api` extraction, `cc-seam` traits + conformance, S1 testability test | ADR corpus + P2-E triage, D-10 crate-dag rule, P1-A/24–26, P2-D/19 | **Yes** — and this is the window where the corpus must land (D8) |
| S2 (first ⅔) | `chain-core` extraction, P0-19/3, event typing | `storage-core` extraction, P0-18, storage `patch @ S2` rows | **Yes** |
| S2 (last ⅓) | — | — | **No — this is a join.** `[ARCH]` §4.2: *one* process opens redb in `bin/beacon-core` before any subsystem starts. Boot is single-owner; the second stream reviews and writes the rollback rehearsal |
| S3a | DA feed chain half, M9, seam impl selection prep | gossip subscribe, `cc-wire`, backfill, serve window, **X1/X2 instruments**, `fault_mode` removal | **Yes**, with one caution: P0-17a (B) depends on P0-19/1 (A, landed at S0) |
| S3b | defect fixes found by windows | window operation, evidence capture, X1–X5 collection | **Partly** — see R-19; the constraint is venues (A-4), not people |
| S4 | 4a milhouse, then 4b STF predicates | 4b container superstruct, 4c the 13 new containers + `DataColumnSidecar`, total-coverage enforcement | **4a is serial** (it deletes two schema places 4b depends on). 4b/4c parallelise |

**What only looks parallel:**

- **S1's engine fold and S2's storage fold.** Different services, so they read as independent. They
  are not: both re-host into `beacon-core`, both touch boot, and `cc-seam` must exist before either
  transport moves. `[ARCH]` §2.3 stages E3 at S1 and E4–E7 at S2 for this reason.
- **The ADR corpus's 12 (b)-class rows.** They look like 12 documents one writer can produce. Each
  records a decision whose owner is the person who made it; they parallelise across *people who know
  the subject*, not across writers (§4).
- **Loop A and Loop B.** Both are "the scheduler work". Loop B is **S0, topology-independent**; Loop
  A is **L, topology-dependent, and sequenced after S3** (`[ARCH]` §3.8). Treating them as one item
  pulls an L-sized topology-dependent piece into S0.

---

## 10. Changes from `[PRD]` §6, stated explicitly

| # | Change | Reason |
|---|---|---|
| **C-1** | **S0a split out of S0** as a phase with its own exit gate | R-5 / `[ARCH]` D-5: two blocking CI gates are red on the committed tree. Until they are green, no later phase's entry gate is *evaluable*. That makes gate restoration the program's first work item, not a cleanup task riding inside S0 |
| **C-2** | **S0 extended from 3 weeks to 6–10 (planning at 7, wk 2–8; with S0a's week, 8 to S1 entry)** | §6's range predates three additions: P0-19 (arrived at revision 1, with an S-sized trio at S0), Loop B's 5-lane rewrite (**M**, 1–2 wk ⌂ `[ARCH]` §3.8), and the config-authority pull-forward (⟡ D-8 ch.1). S0 now carries **35 discrete ledger rows** ≈ 56–87 person-days. §6's 3 weeks at 2 streams is 6 person-weeks |
| **C-3** | **The config-authority half of S4 moved into S0** | ⟡ D-8 ch.1, adopted as instructed. P0-02 already deletes the preset-keying; adding the ordered `ChainConfig` accessors and deleting the five duplicated schedule walks in the same edit is strictly cheaper than doing it twice |
| **C-4** | **milhouse sequenced before the Gloas schema work** (S4a → S4b) | ⟡ D-8 ch.2, adopted. Reduces a four-place synchronised state-schema edit to a two-place one at no extra cost, before a −1/+9-field fork edits it. **Conditional on Q-3**, scheduled wk 1 |
| **C-5** | **S1 extended to 7–9 weeks** | Its body matches §6 at 5–7 wk. The extension is the S2 entry gate: the ADR corpus (§4, 19–32 pd) gates S2 *entry*, so scheduling it at S2 makes it a serial prefix to the longest structural stage |
| **C-6** | **S3 split into S3a (wiring) and S3b (windows), 18–25 wk total against §6's 12** | Two different kinds of work with different scaling. S3a is effort-bound and parallelises; S3b is **wall-clock bound** — 34 clause rows with hard minima (2 × 24 h continuous, ≥ 6 h on an exclusive machine, a retention plateau, a Hoodi week, 20 restart trials, five-client interop with external lead time) plus a re-run allowance for the `min_over_time` clauses where one dip fails. §5 is the derivation; this is the largest schedule risk in the plan and R-3 already predicts it |
| **C-7** | **Transport selection moved from S3 entry/mid to S3b exit** | R-14 — see below. `[PRD]` §6 says S3 "resolves D-2" and `[ARCH]` §9.1 says the stage "selects the impl"; neither states that the soak's own topology is a precondition for reading X1 and X2 |
| **C-8** | **Two S0 exit falsifiers added** (E0.4 for P0-19, E0.5 for P0-09) | R-16 — `make ci` green cannot detect either row; both need a real post-anchor state. R-11 already requires E0.4's test; E0.5 is this plan's addition |
| **C-9** | **P1-F/1 split** into a backend-independent half (`write @ S0`) and a backend clause gated on Q-2 | R-18 — see below |
| **C-10** | **P1-D/09 is split across S0, S3a and post-S3**, where §6 lists it whole under S3 | The row is one program covering both critical loops, and its two halves have different sizes and different topology-dependence. **Loop B** (the chain core's five lanes, subsuming P0-12) is **M and topology-independent** → S0. **P0-17b and P1-B/7** (the `kzg_tx` reconnect and the 12 s gossip→chain wait) → S3a. **Loop A** (queue taxonomy, concurrency, `ValidationPoolState` sharding) is **L and topology-dependent** and `[ARCH]` §3.8 sequences it **after S3** — with Q-8 noting that its head-of-line argument is purely analytic while gossip is unsubscribed. This resolves a real `[PRD]`/`[ARCH]` tension rather than smoothing it; scheduling the whole row at S3 would pull an L-sized topology-dependent piece into the stage that already carries the wiring backlog |
| **C-11** | **P1-B/8 (`patch @ S4`) is discharged at S0** | C-3 pulls the config-authority work forward, and deleting the five duplicated fork-schedule walks is exactly what P1-B/8 asks for (the hand-duplicated walk across three validator files is three of the five). Recorded so a reader tracking its `@ S4` disposition does not lose it. Its sibling P1-B/6 (`request_limits` duplicated in the codec) is unaffected and stays at S3a with `cc-wire` |

---

## 11. Risk register

`[PRD]` §8's R-1…R-13 and `[ARCH]` B.2's Q-1…Q-10 are carried by reference; the table below adds
this plan's scheduling response and six new rows (R-14…R-19).

### Top 5, ranked by (probability × impact × how early it bites)

| Rank | ID | Risk | Response in this plan |
|---|---|---|---|
| **1** | **R-14** *(new)* | **The first soak's own topology is a precondition for two of the five decision criteria.** X1 counts libp2p-attributable panics **in the `p2p` process**; X2 is read off the **`Ipc` impl's** `Backpressure` counter (`[ARCH]` §2.5). If S3 selects `InProcess` before the soak, neither exists to be measured — the same failure shape ⟡ D-11 found for X1 alone, extended to X2. **Neither document constrains the soak's topology.** X1 and X3 are the *only* criteria that can carry Gatehouse & Keep, so this is a decision-integrity risk, not a measurement inconvenience | **C-7**: S3a wires and soaks on the **two-process `Ipc`** configuration; transport selection is an **S3b exit** decision. Both seam impls stay buildable (`[ARCH]` §9.2 already forbids deleting the loser). Paired with the X1 **and X3** deliverables in §3/S3a — fixing only X1 lets the same default-by-accident arrive through X3 |
| **2** | **R-15** *(new)* | **M1 gets discharged on the pre-selection topology.** If W3 reads M1 on two-process/`Ipc` and the program then selects Single Hull, the primary metric — the one that outranks everything — was observed on a topology that is about to change. §9.0's A/B is a **self-devnet** procedure; M1 is a **Hoodi** metric | Budget a **post-selection Hoodi confirmation window** (1 wk, folded into S3b's upper bound) **or** record an explicit decision that self-devnet A/B is sufficient for the swap. This plan budgets the window. Not deciding is the failure mode |
| **3** | **R-5 · ⟡ D-11 · ⟡ D-3** | **The baseline is red, and neither of the two criteria that can decide D-2 is measurable.** No stage entry gate is evaluable today (R-5); X1 has no instrument (D-11); X3 has no answerable form (D-3) | S0a (C-1) for the first; §3/S3a's dedicated owner, wk-33 deadline and *validated-not-shipped* exit for **both** X1 and X3 |
| **4** | **R-3 / R-19** | **The wiring backlog is the long pole, and its verification is wall-clock bound.** S3a+S3b is 18–25 wk — nearly half the program — and **adding people does not compress S3b** | §5's window schedule with venues, prerequisites and re-run costs; §12's compression levers state plainly what a third stream does and does not buy |
| **5** | **R-13 / R-11** | **S2 deletes the evidence for P0-19's causal claim.** P1-A/22 and P1-A/23 are `deleted @ S2`; if P0-19 is their common cause and that is never tested, deletion **hides** the bug and it reappears in `beacon-core`, which still decodes a state on restore | **E0.4 is an S0 exit criterion** — the diagnosis runs 15+ weeks before the deletion. This is the one case in the ledger where a `deleted @ Sn` disposition carries a diagnostic obligation first |

### Full register

| ID | Risk | Scheduling response |
|---|---|---|
| R-1 | Transport collapse silently changes backpressure semantics | P1-D/11 types the contracts at S1 before any transport moves; `cc-seam`'s conformance suite runs against both impls; `[ARCH]` §2.2's four preserved policies are quoted per PR; **a non-zero overflow-counter diff with zero import/head-lag diff is a stage blocker** (E1.1, E2.1) |
| R-2 | One process, one blast radius | Accepted explicitly. The `cc-seam` trait keeps the sandbox option one link-time stage apart |
| R-3 | The wiring backlog is the long pole | Top-5 #4 |
| R-4 | A parked core reports healthy | M8 ships at **S1** (`[ARCH]` §9.1), not "before S3" — earlier than `[PRD]` R-4 requires, and demonstrated red against an injected black-hole as an S1 exit criterion |
| R-5 | The baseline is not green | Top-5 #3 → S0a |
| R-6 | Refactoring against ~745 unresolvable citations | §4 sizes it into three buckets + the mechanism + the CI resolver; scheduled inside S1; gate restated per ⟡ D-13 |
| R-7 | Deleting the process boundary deletes the one real isolation it buys | `[ARCH]` §9.2's S0–S2 prohibition on un-seamed `cc-chain` ↔ `cc-p2p` calls, enforced in `check-crate-dag.sh` |
| R-8 | Repairing surfaces a later stage deletes | Per-phase out-of-scope lists (§3) name the `deleted @ Sn` rows explicitly |
| R-9 | P0-02's documented-as-intentional trap generalises to all five constants | **E0.3** requires a fixture that *differs* from mainnet on all five. Q-7's `preset.rs` audit is the first task, per R-9's own text |
| R-10 | `chain` cannot restart without external checkpoint re-sync | D11: Phase 4 clause 1 (W5) is scheduled **after S2**; checkpoint re-sync time is budgeted into every S0–S2 restart drill |
| R-11 | P0-19's severity and reach are derived, not executed | **E0.4** — the ~30-line test is an S0 **exit criterion**, so it gates the claim before S2 gates the evidence |
| R-12 | P0-19 fires on boot; deferring it leaves a live defect in every restart | Non-deferrable out of S0; first item in stream A; M13 gauge ships with it |
| R-13 | S2 may delete the evidence for P0-19's causal claim | Top-5 #5 |
| **R-14** *(new)* | The soak's topology is a precondition for X1 and X2 | Top-5 #1 → **C-7** |
| **R-15** *(new)* | M1 discharged on the pre-selection topology | Top-5 #2 |
| **R-16** *(new)* | **S0's exit gate cannot detect its two most severe rows.** P0-19 and P0-09 both require a real post-checkpoint-sync state; `make ci` green and both preset suites green are silent on each | **E0.4** and **E0.5** — two falsifiers, not one. `[PRD]` §7.5 already warns that unit tests passing is not evidence |
| **R-17** *(new)* | **The S2 entry gate is not satisfiable as literally worded.** ≥ 3 (b)-class ids are decided by later stages (`ADR-07` at S3, `ADR-P1-04` at S4a, `ADR-R-02` created at S2), so "every cited id resolves" cannot mean "every decision is final" | §4: a resolving document may carry `Status: proposed` + an explicit "revisit at Sn" line. State it, or someone stalls S2 or fudges the gate |
| **R-18** *(new)* | **P1-F/1 records a backend its own source rejected.** `[PRD]` P1-F/1 writes *"a dedicated `crates/slashing-protection` (SQLite, `POOL_SIZE=1`, `locking_mode=EXCLUSIVE`…)"* citing [Q4]; [Q4] §line 16 says **"Backend: redb, not SQLite — despite Lighthouse"**, naming SQLite as the *alternative*. The ADR is `write @ S0`, and **Q-2 is the open question that decides between them** | **C-9**: split the ADR. The negative decision (must **not** ride `services/storage`; record-then-sign; fused `check_and_insert_*`) is writable at S0 with no dependency and is where [Q4] says the value is. The backend clause waits on **Q-2, scheduled wk 1** |
| **R-19** *(new)* | **S3b is wall-clock bound, and a failed window costs a full re-run.** Phase 2's clauses use `min_over_time` where **one dip fails**; Phase 3 needs an exclusive machine; Phase 4 clause 3's discharging row needs a plateau | §5 budgets one re-run per Hoodi window and overlaps only what A-4 permits. Adding a third stream does not shorten this phase |
| Q-1…Q-10 | Ten open questions | §8 schedules eight as spikes (four in wk 1), routes Q-6 into the S0 config work, and marks **Q-8 explicitly not a spike** |

---

## 12. Compression levers — and what each does not buy

| Lever | Buys | Does **not** buy |
|---|---|---|
| A third stream in S0 | ~2 wk (S0's content parallelises well across disjoint crates) | anything in S3b |
| A dedicated technical writer for the ADR corpus | S1 back to 5–7 wk | the **12 (b)-class** rows — they need the decision owners (§9) |
| A second Hoodi node | W6/W7 already overlap; a second node could parallelise W8/W9 with W3 | W4 — the exclusive machine is the constraint, not the network |
| Starting OQ-1 outreach at S1 instead of S2 | removes W10 from the critical path | the interop **fixes** that 0/5 → 5/5 may require |
| Deferring S4c | 3–4 wk | nothing on the path to M1 — S4 is entirely post-M2 |
| **Skipping the X1 / X3 deliverables** | 5–9 person-days | **nothing.** They forfeit the D-2 decision, and `[PRD]` §9 forbids reading the resulting silence as a vote. Listed here only to name them as non-levers |

---

## 13. Counts checked, and discrepancies found

Verified mechanically against the committed documents before planning:

| Check | Result |
|---|---|
| `[PRD]` §0 P0 count vs §5.1 row count | **Agree — 19.** Nineteen distinct ids `P0-01`…`P0-19`; the §5.1 disposition summary (16 `patch @ S0` + 1 `patch @ S2` + 2 `wire @ S3`) reconciles, and §6's "16 P0 rows at S0" matches |
| P1 = 58 | **Reconciles** — A 29 + B 12 + C 1 + D 10 + E 5 + F 1 |
| P2 = 54 | **Reconciles** — A 8 + B 8 + C 1 + D 2 + E 35 |
| Total = 131 | **Reconciles** — 19 + 58 + 54 |
| M2 baseline = 34 | **Reconciles** — P1 3 + P2 9 + P3 10 + P4 11 + OQ-1 1 |

| # | Discrepancy | This plan's handling |
|---|---|---|
| X-1 | **`[ARCH]` §10.4's own table classifies 43 (a) / 12 (b) / 3 (c) = 58**, but ⟡ D-13 and §9.1 both state **42 / 12 / 4** | §4 sizes off the **enumerated table** (43/12/3), since that is the artifact the work is done against. The ±1 does not move the estimate. Flagged, not adjudicated |
| X-2 | **`[PRD]` J-15 already records** ~59 ids / ~204 citations vs `[ARCH]`'s 58 / 207 | Carried as recorded. Both give the same order of magnitude, which is the only load-bearing property |
| X-3 | The slashing ADR is called **`ADR-R-05`** in `[ARCH]` B.2/Q-2 and **`ADR-R-04`** in §9.1/S5 | Pick one when P1-F/1 is written at S0; noted so the id does not join the phantom corpus it is meant to fix |
| X-4 | **`[PRD]` P1-F/1 says SQLite; [Q4] says redb** (§R-18) | **C-9** — split the ADR, gate the backend clause on Q-2 |
| X-5 | `[PRD]` M11's baseline reads ~745; `[ARCH]` §10.1 recommends **748** (541 + 207) and that the unit be stated as *occurrences*, not lines | Adopt 748/occurrences when the reconciliation table lands; it settles X-2 as a side effect |

---

*Written 2026-08-15 against `develop` @ `4146791`. Durations marked **⌂** derive from a research
brief's S/M/L/XL sizing; those marked **≈** are this plan's judgement and are stated as ranges.
Where this plan changes a `[PRD]` §6 boundary or duration, §10 states the change and its reason.*
