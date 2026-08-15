# `beacon-core` consolidation — sprint issues

Decomposed from `plan/project-plan.md` (`[PLAN]`), against
`plan/prd.md` (`[PRD]`), `plan/architecture.md` (`[ARCH]`) and
`plan/research/` (`[Qn]`). Written 2026-08-15 against `develop @ 4146791`.

| Phase | File | Issues | pd | pts | Parallel duration (2 streams) | `[PLAN]` says |
|---|---|---:|---:|---:|---|---|
| S0a — gate restoration | [`s0a-gate-restoration.md`](s0a-gate-restoration.md) | 11 | 5.9–12.25 | 19 | 1.2–2.6 wk | 0.5–1 wk |
| S0 — correctness floor | [`s0-correctness-floor.md`](s0-correctness-floor.md) | 55 | 62.25–98.5 | 165 | 10.2–16.2 wk *(8.8–13.9 rebalanced)* | 6–10 wk |
| S1 — fold the EL bridge | [`s1-fold-el-bridge.md`](s1-fold-el-bridge.md) | 41 | 62.25–88.25 | 144 | 8.25–11.9 wk | 7–9 wk |
| S2 — fold storage | [`s2-fold-storage.md`](s2-fold-storage.md) | 33 | 61.5–85.5 | 147 | 8.25–11.5 wk | 8–10 wk |
| S3a — wiring + instrumentation | [`s3a-wiring-instrumentation.md`](s3a-wiring-instrumentation.md) | 35 | 66.5–92.5 | 163 | 12.8–17.9 wk *(8.7–12.2 rebalanced)* | 8–11 wk |
| S3b — acceptance windows | [`s3b-acceptance-windows.md`](s3b-acceptance-windows.md) | 14 | **not pointed** — 57–83 operator-days | — | **10–14 wk wall-clock** | 10–14 wk |
| S4 — the fork seam | [`s4-fork-seam.md`](s4-fork-seam.md) | 26 | 48.75–68.5 | 118 | 9.5–13 wk | 10–14 wk |
| | | **215** | **307–446 pd** | **756** | **51–74 wk** to S3b exit *(45–66 rebalanced)* | 47 wk |

The headline: **with both recommended rebalances (open decisions 3 and 4), the low end of this
decomposition lands on `[PLAN]`'s 47 weeks and the high end is +19.** Without them, S0 and S3a each
overrun by 4–7 weeks on their binding stream. The overrun is concentrated in exactly two phases, and
both have a named, free-or-cheap lever.

**Points scale.** Fibonacci, encoding size class + uncertainty. **The day range is the authoritative
estimate**; point totals are for sprint capacity only and do not equal 2 × the day total.

| midpoint pd | ≤ 0.5 | 0.6–1.0 | 1.1–2.0 | 2.1–3.0 | 3.1–5.0 | > 5.0 |
|---|---|---|---|---|---|---|
| **pts** | 1 | 2 | 3 | 5 | 8 | 13 |

**Estimate provenance.** ⌂ = derived from a research brief's or `[ARCH]`'s S/M/L/XL sizing, cited at
the issue · ≈ = this decomposition's judgement, stated as a range. Every issue carries one or the
other. Where a `[PLAN]` group sizing did not survive decomposition, the phase file's *drift* table
says so rather than reconciling silently.

**Parallel duration** is computed as `binding stream pd ÷ (5 pd/engineer-week × A-2 efficiency)`, with
efficiency **0.8** for S0–S1 and S2's first two thirds and **0.6** for S2's last third
(`bin/beacon-core` boot is single-owner). It is **not** total ÷ 2.

---

## The rule that outranks the schedule

**`[PRD]` §5.0's `Disposition` column is normative for work order; priority tier ranks severity only.**
No row dispositioned `deleted @ Sn` is patched without an explicit re-disposition. **No issue in this
corpus patches a `deleted @ Sn` row.**

Nine rows are discharged **by deletion** rather than by patch: P1-A/4, P1-A/22, P1-A/23, P1-B/9,
P1-D/13 (`deleted @ S2`), plus P0-07, P0-13, P1-A/27, P1-D/14 (patched at S0 **and** deleted at S2)
and P0-15 / P1-B/11 (patched at S0 **and** deleted at S1). Each S0 issue that patches a
later-deleted surface states the deletion stage in its own header.

**The one exception, and it is a diagnosis rather than a patch.** P1-A/22 and P1-A/23 are
`deleted @ S2` **but diagnose first** (R-13). `S0-A-30` (E0.4) and `S0-A-31` (the instrumented restore
run) discharge that obligation without touching either row's code.

---

## Critical-path issue chain

Computed by **duration**, not severity. `‖` marks work that runs concurrently on the other stream and
is itself a gate.

```
S0a-B-01…04    P0-08 gate restoration ─────────────────────────► every later entry gate  [R-5/D1]
      │
S0-A-05        Q-7 preset audit  (the first task of P0-02, per R-9)
S0-A-07        ChainConfig fields
S0-A-09        thread &ChainConfig, delete `pub mod network`      ← longest S0 chain, 2.5–3.5 wk
S0-A-11/12     differing fixture + ordered fork accessors
      │
S0-A-01        P0-19/1 top_up_pubkey_cache ──┬──────────────────► S3a-B-01 (P0-17a)        [D2/R-12]
S0-A-30        E0.4 R-11 executed            │
S0-A-31        R-13 observation (b) ─────────┴──────────────────► S2-J-02 (delete restore.rs) [D3]
S0-A-32        E0.5 P0-09 falsifier
      │
S1-A-07        cc-seam traits
S1-A-08/09     both impls
S1-A-10…12     conformance suite, both impls ──────────────────► gates every later transport move
   ‖ S1-B-05…20  ADR corpus + P2-E triage ────────────────────► gates S2 ENTRY              [D8]
      │
S2-A-01…03 / S2-B-01…03   chain-core + storage-core extraction
S2-J-01        bin/beacon-core boot  ← SINGLE-OWNER JOIN, does not parallelise
S2-J-02        delete E4 + restore.rs
   ‖ S2-A-10…12  P0-19/3 cache off BeaconState ───────────────► gates S4a                  [D4]
   ‖ S2-B-15     five foreign clients sourced ────────────────► gates S3b-W-10             [D12]
      │
S3a-B-01 / B-04 / B-10    gossip subscribe · DA feed · backfill
S3a-B-19 → S3a-B-20       X1 counter → X1 VALIDATED     wk 33 ─┐
S3a-B-22 → B-23 → B-24    X3 restated → procedure → EXERCISED ─┤ ► gates every S3b window  [D9]
S3a-A-07                  two-process Ipc topology held        ─┘                          [D10/R-14]
      │
S3b-W-00 → W2 → W3(M1) → W4 → W5 → W8 → W9 → W10  ← WALL-CLOCK BOUND, 10–14 wk            [R-19]
S3b-D-01       D-2 resolved by reading X1–X5
      │
S4a-01…07      milhouse                            (order conditional on Q-3 — `S4-ALT`)   [D5/D6]
S4b-01…08      Gloas schema + STF predicates
```

**The panic-attribution instrumentation is on this path by construction, not by size.**
`S3a-B-19`+`S3a-B-20` and `S3a-B-22`…`S3a-B-24` are **7–11 person-days that gate 10–14 weeks of
windows**. If they land late the windows either wait or produce no X1/X3 evidence, and D-2 then
defaults to Single Hull *by accident rather than by data* — which `[PRD]` §9 forbids in as many words.
`[PLAN]` §12 lists skipping them as a **non-lever**: it buys 5–9 person-days and **nothing** else.

**What is not on the critical path despite being P0:** P0-01, P0-03, P0-04/05/06, P0-07, P0-09, P0-10,
P0-11, P0-13, P0-14. All are ship-blockers by severity; none gates a downstream stage. They fill
stream capacity around the chain above. This is the tier-vs-disposition split `[PRD]` J-6 makes
explicit, and it is why they are not reordered to the front.

---

## Coverage check 1 — every P0 row maps to at least one issue

**All 19 P0 rows are mapped. None vanishes silently.** Verified mechanically, not from notes:

```sh
cd plan/issues
for n in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19; do
  printf "P0-%s: " $n; grep -l "P0-$n" *.md | tr '\n' ' '; echo
done
# → every id resolves to at least one phase file
```

| P0 | Disposition | Issue(s) | Falsifier / live check |
|---|---|---|---|
| **P0-01** | `patch @ S0` | `S0-B-01` | **`S0-B-20`** — off-host scan from a second host (E0.8) |
| **P0-02** | `patch @ S0` | `S0-A-05` (Q-7 audit, *first task*), `S0-A-07`, `S0-A-08`, `S0-A-09`, `S0-A-10`, `S0-A-11`, `S0-A-12` | `S0-A-11` (E0.3, differing fixture) · **live: M6 at `S3b-W-02`** |
| **P0-03** | `patch @ S0` | `S0-A-25`, `S0-A-33` | E0.6 / M5 |
| **P0-04** | `patch @ S0` | `S0-B-04`, `S0-B-06` | **live: M2e at `S3b-W-10`** |
| **P0-05** | `patch @ S0` | `S0-B-05`, `S0-B-06` | **live: M2e at `S3b-W-10`** |
| **P0-06** | `patch @ S0` | `S0-B-07` (separate PR, from spec) | **live: M2e at `S3b-W-10`** |
| **P0-07** | `patch @ S0 → deleted @ S2` | `S0-B-02`; **deleted** by `S2-J-01`'s compose collapse | block-import smoke test |
| **P0-08** | `patch @ S0` *(promoted to S0a by C-1)* | `S0a-B-01` … `S0a-B-04` | M7 = 0, `S0a-B-02`'s negative fixture |
| **P0-09** | `patch @ S0` | `S0-A-20` | **`S0-A-32`** (E0.5) — R-16: `make ci` is silent on this row |
| **P0-10** | `patch @ S0` | `S0-A-21` | node-count decrease across two finalizations |
| **P0-11** | `patch @ S0` | `S0-A-26` | |
| **P0-12** | `patch @ S0` | `S0-A-14`, `S0-A-17` | `core::slot_tick_is_never_shed` (policy **D**) |
| **P0-13** | `patch @ S0 → deleted @ S2` | `S0-B-08`; **deleted** by `S2-A-09` | invariant restated at `S2-A-06` |
| **P0-14** | `patch @ S0` | `S0-B-09` | slot-skipping reorg test |
| **P0-15** | `patch @ S0 → deleted @ S1` | `S0-A-27`, `S0-A-28`, `S0-A-29`; **deleted** by `S1-A-06` | `S1-A-18` (timeout ⇒ deferral) |
| **P0-16** | `wire @ S3` (engine half @ S1) | `S1-B-01` (engine); `S3a-B-04`, `S3a-B-05` (p2p); `S3a-A-01` (chain) | `S3a-B-03` island test · **live: `S3b-W-04` clause 4** |
| **P0-17** | `wire @ S3` | a: `S3a-B-01`, `S3a-B-02` · b: `S3a-B-06`, `S3a-B-07` · c: `S3a-B-08`, `S3a-B-09` · d: `S3a-B-10` … `S3a-B-13` | `S3a-B-03` · **live: `S3b-W-08`** for (c) |
| **P0-18** | `patch @ S2` | `S2-B-04`, `S2-B-05`, `S2-B-06` | `S2-B-06` measured **at supernode scale** |
| **P0-19** | `patch @ S0` **+ S2 follow-on** | /1 `S0-A-01` · /1b `S0-A-02` · /2 `S0-A-03` · M13 `S0-A-04` · **/3 `S2-A-10` … `S2-A-12`** · **/4 not scheduled** | **`S0-A-30`** (E0.4) + **`S0-A-31`** (R-13 obs. b) |

**The one unscheduled P0 sub-item.** **P0-19/4** — persist the pubkey cache in `cc-store`, `[Q3]`
**S–M**, dispositioned `patch @ S2` **optional**. It is not scheduled here and is not on any critical
path. Recorded so it is a decision rather than an omission; see the open-decisions table.

**Rows discharged by deletion, accounted for explicitly** (never patched):

| Row | Stage | Deleted by |
|---|---|---|
| P1-A/4 (`write_behind.rs:214`) | S2 | `S2-A-09` |
| P1-A/22 (`restore.rs:189`) | S2 | `S2-J-02` — **gated on `S0-A-31`'s conclusion** |
| P1-A/23 (`restore.rs:656`) | S2 | `S2-J-02` — **gated on `S0-A-31`'s conclusion** |
| P1-A/27 (`docker-compose.yml:127`) | S2 | patched at `S0-B-02`, deleted with the compose surface |
| P1-B/9 (`storage_client.rs:240`) | S2 | `S2-A-09` |
| P1-B/11 (`engine_client.rs:177`) | S1 | patched at `S0-A-27` (= P0-15), deleted at `S1-A-06` |
| P1-D/13 (event ring as data plane) | S2 | `S2-A-05`, `S2-A-09` |
| P1-D/14 (second half) | S2 | fired at `S0-A-19`, remainder deleted at `S2-A-09` |

---

## Coverage check 1b — the whole 131-row ledger, per register

`[PRD]` §13's counts reconcile: 19 + 58 + 54 = **131**. Every register is accounted for below —
mapped to an issue, discharged by a topology change, or **explicitly listed as unscheduled**. Nothing
is silently dropped.

| Register | Count | Mapped to issues | Discharged by deletion | Unscheduled — and why |
|---|---:|---:|---:|---|
| **P0** | 19 | 19 | — (P0-07, P0-13, P0-15 are patched **and** later deleted) | **P0-19/4 only** — a sub-item dispositioned `patch @ S2` **optional** |
| **P1-A** | 29 | 24 | 5 (A/4, A/22, A/23 @ S2; A/27 patched-then-deleted; A/26 → `S1-B-02`) | 0 |
| **P1-B** | 12 | 11 | 1 (B/9 @ S2; B/11 = P0-15, patched then deleted @ S1) | 0 |
| **P1-C** | 1 | 1 (`S0a-B-06`) | — | 0 |
| **P1-D** | 10 | 9 | 1 (D/13 @ S2; D/14 fired at S0, remainder deleted @ S2) | 0 — but **D/09 is split across S0, S3a and post-S3** (`[PLAN]` C-10); **Loop A is deliberately out of scope** for this corpus, sequenced after S3 |
| **P1-E** | 5 | 4 (S1, S2, S3, S4) | — | **S5** — out of this program's scope by `[PLAN]` §2 |
| **P1-F** | 1 | 1 (`S0-B-14`) | — | 0 |
| **P2-A** | 8 | 2 (`/8` → `S0-A-07`; `/6` noted at `S3a-B-04`) | — | **6** — `[PRD]` dispositions them *"fold into the owning stage"* and names no stage. See open decision 7 |
| **P2-B** | 8 | 8 (`/1,/2` → `S2-B-12`; `/3` → `S0-A-10`; `/5` → `S1-B-04`; `/6` → `S4a-05`; `/8` → `S0a-B-04`; `/4,/7` opportunistic, unowned) | — | 2 marked *opportunistic* with no owning stage in `[PRD]` |
| **P2-C** | 1 | 1 (`S3a-B-18`) | — | 0 |
| **P2-D** | 2 | 1 (`/19` → `S1-B-04`, 3 of 19 edges named) | — | **`/20`** — *opportunistic*, no owning stage. See open decision 7 |
| **P2-E** | 35 | 35 **triaged** (`S0-B-17` × 5, `S1-B-19/20` × 30) | — | 0 — but triage **produces** issues; promoted rows are filed against their owning stage and are **not** in this corpus's totals. That is the point of a triage gate |

**Total genuinely unscheduled: P0-19/4 (optional by disposition) · P2-A's six · P2-B/4, /7 · P2-D/20 ·
P1-E/S5 (out of scope) · Loop A (sequenced after S3).** All seven are named in the open-decisions
table, not omitted.

---

## Coverage check 2 — the eight spikes

| Q | Question | Size | Issue | Scheduled | Blocks |
|---|---|---|---|---|---|
| **Q-9** | Does `crates/spec-tests`' coverage check *report* or *fail*? | XS ⌂ | `S0a-B-07` | **wk 1** | `S3b-W-01` (M2a "skiplist empty") |
| **Q-10** | Is the serve window truly never published? | XS ⌂ | `S0a-B-08` | **wk 1** | `S3a-B-08` (P0-17c's framing) |
| **Q-3** | Does `superstruct` compose with milhouse's `List<T, N, U>`? | 1 h ⌂ | **`S0a-B-09`** | **wk 1** | **the S4a→S4b order — and it can invert it** (`S4-ALT`) |
| **Q-2** | Does redb give a **fail-fast** cross-process exclusive open? | S ⌂ | **`S0a-B-10`** | **wk 1** | the backend clause of `S0-B-14` (P1-F/1, `write @ S0`) |
| **Q-7** | Other config-scoped values in `preset.rs`? | S ⌂ | `S0-A-05` | **first task of P0-02** | completeness of the `pub mod network` deletion (R-9) |
| **Q-1** | `check-crate-dag.sh` allowlist minimality | S ⌂ | `S1-B-22` | **closed** (not minimal: `cc-devnet-gen`→`cc-config`) | nothing; hygiene |
| **Q-4** | superstruct's compile-time cost on this workspace | spike | `S4-Q-04` | wk 46 | superstruct-vs-hand-written |
| **Q-5** | Does `specs/gloas/partial-columns/` change the DAS sidecar shape? | read | `S4-Q-05` | wk 46 | the scope of ⟡ D-7 (`S4c-03`) |

**Two rows that are deliberately not spikes.** **Q-6** (`SECONDS_PER_SLOT` vs `SLOT_DURATION_MS`) is
**routed into the S0 config work** as `S0-A-06`, not run standalone. **Q-8** (is the 12 s gossip→chain
wait ever reached?) is **explicitly not a spike** — it is *measured* at S3a with gossip live
(`S3a-A-08`); moving it earlier is meaningless while gossip is unsubscribed.

---

## Coverage check 3 — the S2 entry gate, decomposed

Scheduled **inside S1** (D8), because it gates S2 *entry*. It is an **S1 exit item** (E1.6).
**22.25–30.25 pd** across 17 S1 issues, plus `S0-B-17`'s 1–1.5 pd pulled forward into S0 → **23.25–31.75
pd** for the gate as a whole. Against `[PLAN]` §4's **19–32 pd**.

| Bucket | Count | Issues | pd |
|---|---:|---|---:|
| **The reconciliation mechanism itself** (J-16 — it does not exist) | 1 | `S1-B-05` — **first task, not last** | 1.5–2 |
| **CI resolver gate** | 1 | `S1-B-06` — without it the corpus re-diverges the week after the gate passes | 1.5–2 |
| **(a) re-derivable** — write it by reading the citation site | **43** | `S1-B-07` (9) · `S1-B-08` (13) · `S1-B-09` (8) · `S1-B-10` (13) — parallelises across writers | 7–9 |
| **(b) needs a decision recorded** | **12** | `S1-B-11` (ADR-P3-16, highest priority) · `S1-B-12` (ADR-P3-02) · `S1-B-13` (ADR-P3-15) · `S1-B-14` (ADR-P4-03) · `S1-B-15` (ADR-P2-13, **on the X1 path**) · `S1-B-16` (the other 7, `Status: proposed` + *revisit at Sn*) — **does not parallelise onto one writer** | 6.75–9.5 |
| **(c) stale — delete the citation** | **3** | `S1-B-17` | 0.5 |
| **ADRs this refactor creates, due at S1** | 1 | `S1-B-18` (ADR-R-01) | 0.5–0.75 |
| **R-P2-triage** (**M12: 35 → 0**) | 35 | `S0-B-17` (rows 1, 6, 8, 21, 31 — **pulled into S0**; 8 and 31 bear on P0-02 and P0-06) · `S1-B-19` (16) · `S1-B-20` (14) | 5–7.5 |
| **M3 ledger maintenance** | — | `S1-B-21` | 0.5 |

**The gate, restated (⟡ D-13):** *"every cited id resolves to a committed document and the
reconciliation table has no unclassified rows"* — **not** "write 58 ADRs".

**R-17 — the gate as literally worded is not satisfiable at S2 entry.** At least three (b)-class ids
are decided by *later* stages (`ADR-07` at S3, `ADR-P1-04` at S4a, `ADR-R-02` created at S2). The
resolution, carried into `S1-B-16`: **a resolving document may carry `Status: proposed` plus an
explicit "revisit at Sn" line.** The revisits are scheduled — `S4a-08` is `ADR-P1-04`'s, `S2-A-07` is
`ADR-R-02`'s creation. Without them, `proposed` was a fudge rather than a mechanism.

**Bucket counts** are sized off the **enumerated** `[ARCH]` §10.4 table (**43 / 12 / 3**), which is the
artifact the work is done against. §10.4's own totals line and ⟡ D-13/§9.1 say **42 / 12 / 4**;
`[PLAN]` X-1 flags the ±1 and declines to adjudicate it. Carried the same way here.

---

## Open decisions for the team lead

| # | Decision | Input | Where it sits |
|---|---|---|---|
| 1 | **`SECONDS_PER_SLOT` / `SLOT_DURATION_MS` — does it warrant its own P0?** `[PRD]` J-12 says explicitly *"that is the team lead's call, not this document's."* Loading the current upstream `configs/mainnet.yaml` **fails to parse** today; the repo's Hoodi fixture carries both keys, which is why nothing has caught it. Deliberately **not** folded into P0-02 (that class is preset-vs-config keying; this is a missing serde default) and **not tiered** | **Answered `S0-A-06` (2026-08-15):** formally removed from consensus-specs YAML ([#4926](https://github.com/ethereum/consensus-specs/pull/4926)); both keys remain in the spec family. Accept-both landed. **Proposed tier: no new P0** (parse failure discharged); residual ms migrate is P2 | escalate; no issue created or omitted pre-emptively |
| 2 | **P0-19/4** — persist the pubkey cache in `cc-store`. `patch @ S2`, **optional**, `[Q3]` **S–M**. Not scheduled | `S2-A-12`'s clone-cost measurement will show whether boot time justifies it | S2 planning |
| 3 | **The S3a stream assignment.** As `[PLAN]` §9 assigns them, stream B carries 51.25–71.75 pd against stream A's 15.25–20.75 — a **3:1 imbalance** that puts the phase at **12.8–17.9 wk** against a stated 8–11. The rebalance moves backfill and the p2p `patch @ S3` rows to stream A (→ **8.7–12.2 wk**), **which crosses the §9 skill boundary** (A is consensus core, B is edge & platform) | see [`s3a-wiring-instrumentation.md`](s3a-wiring-instrumentation.md)'s imbalance section | a **staffing** decision, stated rather than silently applied |
| 4 | **S0's fork-choice group** (`S0-A-20` … `S0-A-24`, 5.5–9 pd) touches `crates/fork-choice` only, which stream B does not otherwise open. Moving it to B takes S0's binding stream from 10.2–16.2 wk to **8.8–13.9 wk** at no skill cost | | S0 planning; recommended |
| 5 | **`S3b-W-11`** — R-15's post-selection Hoodi confirmation window. `[PLAN]` budgets it in prose (§11) and omits it from §5's table. Run it, **or record an explicit decision that self-devnet A/B is sufficient for the swap**. Not deciding is the failure mode | `S3b-D-01`'s outcome | S3b planning |
| 6 | **`X-6`, this decomposition's finding.** `[ARCH]` §10.4 routes `ADR-P3-15`'s replacement decision to **`ADR-R-05`**, but §10.5's `ADR-R-05` is the **slashing-protection** record written at S0. Two decisions, one id. Recommended: `ADR-P3-15`'s successor becomes `ADR-R-07` | | resolve in `S1-B-13`; `S0-B-14` also resolves `[PLAN]` X-3 (`ADR-R-04` vs `ADR-R-05` for slashing — use **`ADR-R-05`**, per §10.5's enumeration) |
| 7 | **P2-A's remaining 6 rows** and **P2-D/20** have no owning issue. `[PRD]` dispositions P2-A as *"fold into the owning stage"* and P2-D/20 as *"opportunistic"*. `/8` folds into P0-02 (`S0-A-07`) and `das/sampling.rs:596` is noted at `S3a-B-04`; the other six (`window.rs:270`, `prune/mod.rs:897`, `column.rs:401`, `fork_digest.rs:350`, `discovery/predicate.rs:79`, `new_payload.rs:311`) land nowhere concrete | | assign to owning stages at S1 planning, or accept them as unscheduled in writing |

---

## Where the plan is under-specified for estimation

Stated plainly rather than papered over. Each is repeated in its phase file's drift table.

| # | Gap | Consequence | Handled by |
|---|---|---|---|
| 1 | **P0-10 and P0-18 carry no `file:line`** — both cite only `[AS] §4/0n` plus a crate name | two P0 rows are unestimatable as written | recovered by grep: `S0-A-21` (`proto_array.rs:325`, `events/mod.rs:206`) and `S2-B-04..06` (`keys.rs:51,56,61`, `invariants.rs:45-50`, `:271,332,596-624`). **Write them back into `[PRD]` §5.1** |
| 2 | **The `cc-seam` conformance suite is "11 tests"; `[ARCH]` §2.2 names 4.** Seven are unspecified in either source | 5–7 pd sized against an unenumerated list | `S1-A-12`'s first deliverable is the enumeration, reviewed before any test is written. If the honest count is not 11, correct `[ARCH]` §2.1 |
| 3 | **X4 and X5 have an exit criterion (E3a.8) and no deliverable.** `[PLAN]` §3/S3a creates work items for X1, X2, X3 only | E3a.8 — *all five have a named instrument and a named reader before W0* — is unsatisfiable | `S3a-B-25` proposes instruments and readers. Both decide toward Single Hull, so the stakes are lower than X1/X3 |
| 4 | **`[PLAN]` says E0.4 discharges the P1-A/22–23 diagnostic obligation. `[PRD]` R-13 requires two observations**, and is explicit that R-11's test *"confirms the mechanism"* but **"cannot confirm the attribution"** | someone runs E0.4, sees `CachePoisoned`, and marks /22 and /23 diagnosed when they have not been — then S2 deletes the evidence | **`S0-A-31`** is observation (b): an instrumented real restore run. **`S2-J-02` is gated on its conclusion** |
| 5 | **E0.8 (off-host scan) and E0.9 (the M3 `Discharged by` artifact) are named exit criteria with no line in `[PLAN]`'s estimate table**, and E0.9's artifact *"nothing in either source creates"* | S0 ships two exit criteria with no budgeted work | `S0-B-20`, `S0-B-18`, plus ~0.5 pd per later stage for maintenance |
| 6 | **`[PLAN]`'s S0 calendar contradicts its own arithmetic.** 56–87 pd at A-1 (2 streams) and A-2 (0.8) is **7–11 wk**, not the stated 6–10 | the program's largest phase is under-scheduled before decomposition even starts | flagged; the decomposed figure is 10.2–16.2 wk (8.8–13.9 rebalanced) |
| 7 | **P0-08 counts "4 production `env::var` reads"; its own evidence list names 7 sites across 3 files** | the route-vs-allowlist split is a policy call presented as a count | `S0a-B-01` requires the split be recorded in the PR |
| 8 | **P2-D/19 is "19 engine policy edges" with 3 named and no `file:line` for the other 16** | 16 edges unestimatable | `S1-B-04` scopes to the three named + P2-B/5, and says so |
| 9 | **P0-17a and P0-16 are 8–12 pd blocks with no sub-structure** in `[PLAN]` | the largest S3a items have the least decomposition provenance | sub-issues in [`s3a-wiring-instrumentation.md`](s3a-wiring-instrumentation.md) are marked ≈ and derived from `file:line` evidence, not from a source sizing |
| 10 | **The 13 new Gloas containers are a count, not a list** | `S4c-01/02`'s 6–8 pd is judgement against an unenumerated set | `S4c-01`'s first deliverable is the enumeration; re-estimate after |
| 11 | **ADR corpus counts disagree with themselves** — `[ARCH]` §10.4 enumerates 43/12/3 and totals 42/12/4 (`[PLAN]` X-1); `[PRD]` J-15 records ~59 ids / ~204 citations against §10.1's 58 / 207 (X-2) | ±1 id, ∓3 citations | sized off the enumerated table; M11's baseline adopted as **748 occurrences** (X-5), which settles X-2 as a side effect |
| 12 | **Two ADR id collisions** — X-3 (`ADR-R-04` vs `ADR-R-05` for slashing) and **X-6** (`ADR-R-05` double-booked for `ADR-P3-15`'s successor, this decomposition's finding) | ids joining the phantom corpus they exist to fix | resolved in `S0-B-14` and `S1-B-13`; see open decision 6 |
| 13 | **`[PLAN]` §5 undercounts Phase 4 by 3 rows — and assigns them to the wrong venues.** Verified against `docs/phase-4-soak.md:1063-1073` (11 rows): §5 counts **clause 2 as 1 row @ Hoodi** when it is **3 rows @ in-process-double + self-devnet ×2**, and **clauses 4–7 as 4 rows @ Hoodi** when they are **5 rows**, two of which are @ self-devnet. §5's discharge column sums to **30**, not 34 | **M2 = 0 is unreachable as scheduled**, and — because `[PRD]` §7.5 makes venue an anti-metric — W9 at Hoodi would discharge **none** of its three rows | corrected in [`s3b-acceptance-windows.md`](s3b-acceptance-windows.md)'s `S3b-W-08`/`S3b-W-09`. **It relieves the A-4 bottleneck**: W9 needs no Hoodi time at all. **Phases 2 and 3 were not re-derived row by row** — do that before booking W3 and W4 |

---

## PR-boundary constraints that override "size issues at 1–2 days"

Three places where the correct issue size is set by a rule, not by effort:

| Constraint | Effect | Issues |
|---|---|---|
| **`[ARCH]` §6.2** — the `check-crate-dag.sh` JWT rule must name `cc-engine-api` **in the same PR that creates the crate**; otherwise the stage has silently deleted a mechanically-enforced invariant | forces **coarser**: the crate skeleton and the dag rule are one ticket | `S1-A-01` |
| **`[ARCH]` §3.2** — the `capacity()`-derived gauges and `import_path.rs`'s backpressure tests must be rewritten **in the same PR** as the lane-manager cutover | forces **coarser** | `S0-A-17` |
| **`[ARCH]` §9.2** — *changing an overflow policy in the same PR that moves a transport* is prohibited; it makes the diff unreviewable | forces **finer**: the policy change is its own PR | `S2-A-07` (ADR-R-02), separate from `S2-A-05`/`S2-A-06`/`S2-A-09` |
| **ADR-P4-12 / `[PRD]` P0-06** — the wire prober's codec is deliberately independent; fixing it by copying the node's fix re-creates the coupling the row exists because of | forces a **separate PR derived from the spec** | `S0-B-07` |

Two issues genuinely cannot be decomposed below their size, and the reason is stated at each:
**`S0-A-09`** (deleting `pub mod network` is the compile-error forcing function; a partial edit leaves
the property unearned) and **`S2-J-01`** (`bin/beacon-core` boot is single-owner by `[ARCH]` §4.2 — it
is *why* A-2 drops parallel efficiency to 0.6).

---

## Anti-metrics — what does not count as progress

Carried from `[PRD]` §7.5 because several of these issues are close to the temptation:

- **All six healthchecks green.** The compose stack reports green today with a dead import path.
- **A clause read by eye off a Grafana panel, or run at the wrong venue.** Both fail to discharge.
- **Any invented number.** `NOT_RUN` with named blockers is the correct value until a real window runs;
  a clause row edited to remove `NOT_RUN` without a run is a violation, not progress.
- **Unit or property tests passing.** Every dead island in `[PRD]` §1.A is well unit-tested.
- **An instrument that reports 0 because it was never wired.** Worse than no instrument, because it
  reads as evidence — which is why `S3a-B-20` and `S3a-B-24` exit on **validated**, not **shipped**.
