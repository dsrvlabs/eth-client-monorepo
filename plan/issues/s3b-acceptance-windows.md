# S3b — acceptance windows · wk 36–47

**Entry.** S3a exit, **including E3a.3 (X1 validated) and E3a.7 (X3 exercised)**.
**Ships.** **M1** observed and **M2 → 0**.

---

# ⚠ This phase is wall-clock bound. Adding people does not compress it.

**The issues in this file are not story-pointed, and that is deliberate.** `[PLAN]` R-19 / A-1: S3b's
content is **windows with prerequisites, venues and re-run costs**, not engineering work that finishes
faster with more engineers. A window is a duration at a venue. Two engineers do not halve a 24-hour
soak.

Each issue below therefore carries:

- **Wall-clock** — the window's duration **including one re-run allowance** (`[PLAN]` §5). Do not
  budget a second re-run on top; the allowance is already in the number.
- **Venue** — load-bearing. `[PRD]` §7.5 makes *"run at the wrong venue"* an **anti-metric**: a clause
  run at the wrong venue **does not discharge**, and the run is wasted.
- **Operator-days** — the real human effort to run and read the window. This is a staffing input, not
  a schedule input; it does not shorten the wall-clock.
- **Prerequisites** and **re-run cost**.

**The one genuinely pointable work in this phase** is stream A's mid-window defect fixes
(`S3b-FIX`, below). `[PLAN]` §9: during S3b, stream A does *"defect fixes found by windows"* while
stream B does *"window operation, evidence capture, X1–X5 collection"*.

**A wiring gap found mid-window is a defect fixed and the window RE-RUN, not a window amended**
(`[PLAN]` §3/S3b, out of scope). Editing a clause row to remove `NOT_RUN` without a run is a `[PRD]`
§7.5 anti-metric violation, not progress.

**A-4 — the venue constraint.** Hoodi and the Phase-3 exclusive machine are **single resources**. Only
`self-devnet` and `self-devnet-compressed` windows run concurrently with a Hoodi window. This, not
headcount, is what sets the phase length.

---

## Window index

| Id | Window | Venue | Wall-clock | Op-days | Discharges | Prereq | Re-run cost |
|---|---|---|---|---:|---|---|---|
| `S3b-W-00` | Instrument validation + §9.0 A/B | self-devnet | 1–2 wk | 4–6 | 0 rows | S3a exit | cheap |
| `S3b-W-01` | Phase 1 clause 1 — spec-vector suites, **skiplist empty** | CI | concurrent | 1–2 | **1 row** | **Q-9** (`S0a-B-07`) | cheap |
| `S3b-W-02` | Phase 1 clauses 2–3 — ≥ 24 h Hoodi soak + timing budgets. **Carries M6** | **Hoodi** | 1–1.5 wk | 5–7 | **2 rows + M6** | W0 | **full 24 h** |
| `S3b-W-03` | Phase 2 — 9 rows. **M1 is read here** | **Hoodi** | 1.5–2 wk | 8–11 | **9 rows + M1** | W2 | **full 24 h per dip** — budget 2 attempts |
| `S3b-W-04` | Phase 3 — 10 rows | **exclusive machine** | 1.5–2 wk | 8–11 | **10 rows** | blockers B1–B5 cleared | full window |
| `S3b-W-05` | Phase 4 clause 1 — 20/20 restart trials | Hoodi | 0.5–1 wk | 4–6 | **1 row** | **S2** (R-10 / D11) | per-trial |
| `S3b-W-06` | Phase 4 clause 3 **row A** — compressed-retention plateau, **discharging** | `self-devnet-compressed` | 1.5–2 wk | 5–7 | **1 row** | separate hardware | full plateau |
| `S3b-W-07` | Phase 4 clause 3 **row B** — Hoodi week, **`confirmation, non-discharging`** | Hoodi | 1 wk | 3–4 | **0 rows** | runs concurrent with W6 | — |
| `S3b-W-08` | Phase 4 clauses 4–7 — full-window block and column serve | Hoodi **+ self-devnet** (see below) | 1 wk | 4–6 | **5 rows** | P0-17c wired (`S3a-B-08/09`) | full window |
| `S3b-W-09` | Phase 4 clause 2 — cursor fallback in three stages | **in-process-double + self-devnet** (see below) | 1 wk | 4–6 | **3 rows** | — | full window |
| `S3b-W-10` | **OQ-1** — five clients × 5–10 peers + **M2e** | Hoodi | 2–3 wk | 8–12 | **1 row + M2e** | **external lead time — sourcing began at S2** | **high — reschedules five parties** |
| `S3b-W-11` | **R-15 post-selection Hoodi confirmation** (see below) | Hoodi | 1 wk | 3–4 | 0 rows | D-2 resolved | full window |
| `S3b-D-01` | **D-2 resolved** — read X1–X5 off the collected data; record the ADR | — | 2–4 pd | — | the decision | W2–W10 | n/a |
| `S3b-FIX` | Stream A — defect fixes found by windows (**the pointable part**) | — | continuous | — | — | — | — |
| | **Σ 10–14 wk** with W6/W7 and W10's outreach overlapped | | | **≈ 57–83 op-days** | **34 rows** | | |

**M2 baseline = 34 rows:** Phase 1 (3 named clauses) + Phase 2 (9) + Phase 3 (10) + Phase 4 (11) +
OQ-1 (1). Discharged above: 1 + 2 + 9 + 10 + 1 + 1 + 0 + **5** + **3** + 1 = **33**, plus W7's
explicitly non-discharging Hoodi week = the 34th Phase-4 row. **Reconciled against
`docs/phase-4-soak.md:1063-1073` — see the venue finding immediately below.**

---

## ⚠ `[PLAN]` §5 undercounts Phase 4 by 3 rows, and it undercounts them at the wrong venues

**Read this before booking Hoodi time for W8 or W9.**

`[PLAN]` §5's window table sums its own discharge column to **30**, not the 34 it claims. Verified
against `docs/phase-4-soak.md`'s clause table (**11 rows**, `:1063-1073`), the gap is exact and it is
not a rounding error — `[PLAN]` counts **clauses**, while M2's unit is a **clause row**:

| `[PLAN]` §5 says | The clause table actually has | Δ |
|---|---|---|
| W9 = *"clause 2 — cursor fallback in three stages"*, **1 row**, venue **Hoodi** | **3 rows**, one per stage: `attribution` @ **in-process-double** · `hole recorded` @ **self-devnet** · `hole closed` @ **self-devnet** | **+2** |
| W8 = *"clauses 4–7"*, **4 rows**, venue **Hoodi** | **5 rows**: clause 4 @ hoodi · clause 5 @ hoodi · clause 6 negative side @ **hoodi** · clause 6 negative side @ **self-devnet** (early falsification, M4.3 / CC-4F) · clause 7 advertisement-equals-served @ **self-devnet** | **+1** |

**Two consequences, and the second is the expensive one.**

1. **M2 = 0 is unreachable against `[PLAN]` §5's row attribution.** Three rows exist in the clause
   table with no window scheduled against them. Counted correctly, W8 discharges 5 and W9 discharges
   3, and the 34 reconciles.
2. **`[PRD]` §7.5 makes "run at the wrong venue" an anti-metric — and `[PLAN]` §5 assigns both windows
   to Hoodi when four of their eight rows name a different venue.** Running clause 2 at Hoodi
   discharges **none** of its three rows; running clause 7 at Hoodi discharges nothing. A window that
   looks like it cleared 5 rows would have cleared 3.

**This relieves the A-4 venue bottleneck rather than worsening it.** W9 needs **no Hoodi time at all**
(in-process-double + self-devnet), and 2 of W8's 5 rows are self-devnet. Both can run concurrent with
a Hoodi window, which is what A-4 permits. Re-plan W8/W9 accordingly and correct `[PLAN]` §5's table.

**Do the same reconciliation for Phases 2 and 3 before booking W3 and W4.** This decomposition
verified Phase 4's 11 rows against the clause table; Phase 2's 9 (`phase-2-soak.md:474`) and Phase 3's
10 (`phase-3-acceptance.md:492`) were **not** re-derived row by row, and the same clause-vs-row and
venue-vs-window slippage may be present. `scripts/soak-report.sh --phase N` is the authority.

---

## Three things this schedule makes visible that a week range does not

1. **W6 and W7 must not be merged.** Clause 3's two rows are separately marked: the plateau at
   `self-devnet-compressed` is the **discharging** one; the Hoodi week is explicitly
   ***non-discharging***. **Merging them discharges nothing and looks like progress.**
2. **W4 is a resource bottleneck, not a duration.** The exclusive machine **serialises against every
   other window that wants that box**. It is the reason W6 is scheduled on separate hardware.
3. **W3 carries M1.** M1 is not a separate window — it is read off Phase 2 clause 2 (DA-gated import)
   and clause 3 (head lag). **Everything before W3 is preparation for one observation.**

---

## `S3b-W-00` · Instrument validation + §9.0 A/B

**Venue** self-devnet · **Wall-clock** 1–2 wk · **Op-days** 4–6 · **Discharges** 0 rows ·
**Prereq** S3a exit · **Re-run** cheap

**This window discharges no clause row and is not optional.** It is where the instruments are
confirmed live against the soak collector before a venue-constrained window depends on them.

**Acceptance (falsifiable)**
1. X1's counter is scraped and visible in the soak dashboard, with at least one **injected** panic
   producing a labelled increment on the collector — not just on the node (re-running `S3a-B-20`'s
   demonstration through the full collection path).
2. X2's `Backpressure` counter is non-zero under saturating load, scraped.
3. X3's classification procedure has a filled-in worked example (from `S3a-B-24`) in the operator's
   hands.
4. X4 and X5's readers are named in the window log.
5. §9.0 A/B clean against the S2 baseline — same three families, same blocker rule.

---

## `S3b-W-01` · Phase 1 clause 1 — spec-vector suites, skiplist empty

**Venue** CI · **Wall-clock** concurrent with W0 · **Op-days** 1–2 · **Discharges** 1 row ·
**Prereq** **Q-9** (`S0a-B-07`) · **Re-run** cheap

**Q-9 is the prerequisite for a reason.** The clause requires *"suites green both presets, **skiplist
empty**"*. If `crates/spec-tests`' coverage check only **reports** rather than **fails**, "green"
today does not mean what the clause needs it to mean, and the row would be discharged against a check
that cannot fail. Q-9's answer either confirms the clause is measurable or files the follow-on work.

**Acceptance** — both preset suites green; `docs/spec-vectors-skiplist.md` empty; the coverage check
**fails** on an uncovered vector, demonstrated once.

---

## `S3b-W-02` · Phase 1 clauses 2–3 — the 24 h Hoodi soak. **Carries M6.**

**Venue** **Hoodi** · **Wall-clock** 1–1.5 wk · **Op-days** 5–7 · **Discharges** 2 rows **+ M6** ·
**Prereq** W0 · **Re-run cost: a full 24 h**

**Clauses** — ≥ 24 h continuous Hoodi soak; timing budgets: **epoch p95 ≤ 1000 ms**, **`process_block`
p95 ≤ 400 ms**. Read off **histogram buckets, never quantile interpolation** (ADR-P1-15 ✓
`services/chain/src/metrics.rs:6`, which `[ARCH]` §10.4 marks *survives and is load-bearing*).

### M6 rides this window, and neither source gives it one

**M6** — *"deposits accepted on a non-zero-GVF network; matches a reference client over the same
window"* — is the **live falsifier for P0-02** in exactly the way M2e (W10) is for P0-04/05/06.
E0.2/E0.3 discharge the **code-level** metric (M6b, `constants::network::` call sites → 0); **they
cannot discharge M6, which is an observation.**

**Instrument** — over this 24 h Hoodi window, diff our accepted-deposit set against a reference
client's on the **same slot range**. Hoodi's GVF is `0x10000910`, so a node still resolving the domain
from the `mainnet` preset accepts **zero** — the comparison is unambiguous.

**Fallback chain, and the venue constraint that governs it**
1. This window (W2). If it contains **no deposits**, →
2. `S3b-W-03` (W3), →
3. a **targeted devnet with injected deposit traffic**.

**The venue must have a non-zero GVF** — that is the whole point of the metric. A devnet configured
with mainnet's GVF discharges nothing.

**Acceptance** — both clause rows discharged at the named venue **and** M6 recorded as a set
comparison with a named reference client, or explicitly rolled to W3 with the reason.

---

## `S3b-W-03` · Phase 2 — 9 rows. **M1 is read here.**

**Venue** **Hoodi** · **Wall-clock** 1.5–2 wk · **Op-days** 8–11 · **Discharges** 9 rows **+ M1** ·
**Prereq** W2 · **Re-run cost: a full 24 h per dip — budget 2 attempts**

**Clauses** (`docs/phase-2-soak.md:474`) — healthy peer count **≥ 25** *and* custody **≥ 8** over 24 h;
10-minute gap recovery within 32 slots; withheld-column deferral + recovery; scoring penalty crossing
the **−4000** bucket.

### The `min_over_time` trap

**Phase 2's peer/custody clauses use `min_over_time`, where a single dip fails the whole 24 h.** There
is no partial credit and no averaging. This is why the re-run cost is a full 24 h **per dip** and why
`[PLAN]` §5 budgets two attempts rather than one. Treat any dip as a defect for `S3b-FIX`, not as
noise to re-run through.

### M1 — the metric that outranks everything

**M1: a block produced by a foreign peer imports end to end on Hoodi and the node holds head.** Not a
healthcheck, not a unit test, not a Grafana panel read by eye. Baseline: **never observed** — 0
live-network acceptance runs.

**Instrument** — `docs/phase-2-soak.md` clause 2 (*DA-gated import*) + clause 3 (*head lag ≤ 1
typical*, bucket `le=1` **≥ 0.95**), then **sustained**.

**M1 is the gate on the word "works". No other metric may be reported as success while M1 is unmet.**

**R-15 applies to this window.** M1 will be read on the **pre-selection** topology (two-process /
`Ipc`, held by `S3a-A-07`). If the program then selects Single Hull, the primary metric was observed
on a topology about to change — and §9.0's A/B is a **self-devnet** procedure while M1 is a **Hoodi**
metric. `S3b-W-11` is the budgeted response. **Not deciding is the failure mode.**

---

## `S3b-W-04` · Phase 3 — 10 rows on the exclusive machine

**Venue** **exclusive machine** · **Wall-clock** 1.5–2 wk · **Op-days** 8–11 · **Discharges** 10 rows ·
**Prereq** blockers **B1–B5** cleared · **Re-run cost: full window**

**This window serialises against every other window that wants that box.** It is a resource
bottleneck, not a duration — a second Hoodi node does not help (`[PLAN]` §12).

**Clauses** (`docs/phase-3-acceptance.md:492`)
- clause 1 — `is_optimistic == 0` for **≥ 99 %** of samples over a **≥ 6 h / ≥ 20-finalized-epoch**
  window, **with bootstrap excluded**
- clause 3 — geth head within 1 block **≥ 99 %** with **zero `-38002` / `-38006`**
- clause 4 — getBlobsV2 fastpath: **non-zero complete AND zero engine-sourced columns**. Anything else
  is a **FAIL**, including a non-zero complete count with non-zero engine-sourced columns

**Prerequisite note** — clause 4 depends on `S1-B-01` (real KZG replacing `kzg: None`). Shipping that
issue makes the clause *runnable*; it does not discharge it.

---

## `S3b-W-05` · Phase 4 clause 1 — 20/20 restart trials

**Venue** Hoodi · **Wall-clock** 0.5–1 wk · **Op-days** 4–6 · **Discharges** 1 row ·
**Prereq** **S2** · **Re-run cost: per-trial**

**Clause** — 20/20 restart trials **≤ 60 s**, `following_head=1`, **identical `GetHead` roots**.

**Why this cannot run before S2 (R-10 / D11).** `chain` cannot restart without an external checkpoint
re-sync, and doing so **permanently holes the archive**. Twenty trials before S2 would cost twenty
checkpoint re-syncs and twenty archive holes. S2's in-process boot removes the cause; `S2-B-14`'s
restart drill is the rehearsal for this window.

**Acceptance** — the trial table filled in with all 20 rows, each carrying its restart duration and
`GetHead` root. A row without a recorded root does not count toward 20.

---

## `S3b-W-06` · `S3b-W-07` · Phase 4 clause 3 — the two rows that must not be merged

| | W6 — **row A** | W7 — **row B** |
|---|---|---|
| **Venue** | `self-devnet-compressed` — **separate hardware** | Hoodi |
| **Wall-clock** | 1.5–2 wk | 1 wk, **concurrent with W6** |
| **Op-days** | 5–7 | 3–4 |
| **Status** | **discharging** | **`confirmation, non-discharging`** |
| **Discharges** | **1 row** | **0 rows** |
| **Re-run** | full plateau | — |

**Four bars, each row** — 24 h slope **< 1 %** of plateau; prune/ingest within **5 %**; prune
deadlines **< 1 %**; hot-path p99 within **10 %**.

**Merging W6 and W7 discharges nothing and looks like progress.** The clause table marks the two rows
separately and marks the Hoodi week non-discharging. Only the compressed-retention plateau counts
toward M2. W6 runs on separate hardware precisely so W7 can run concurrently on Hoodi without either
contending for the other's venue.

---

## `S3b-W-08` · Phase 4 clauses 4–7 — full-window block and column serve

**Venue** Hoodi **for 3 of 5 rows; self-devnet for 2** · **Wall-clock** 1 wk · **Op-days** 4–6 ·
**Discharges 5 rows** · **Prereq** P0-17c wired (`S3a-B-08`, `S3a-B-09`) · **Re-run cost: full window**

**The direct falsifier for P0-17c.** Today every serve answers `ResourceUnavailable`.

| Row | Venue | Threshold |
|---|---|---|
| clause 4 · full-window **block** serve | hoodi | serve-probe positives: **zero** `ResourceUnavailable`; `eas ≤ start_slot(current_epoch − 33024)` |
| clause 5 · full-window **column** serve | hoodi | full requested ∩ held set per block; zero `ResourceUnavailable` |
| clause 6 · **negative side** | hoodi | below `eas`: **every** response `ResourceUnavailable` (3); **never** empty success; blocks + columns × by-range + by-root |
| clause 6 · negative side, **early falsification** | **self-devnet** | same negative, at M4.3 / CC-4F |
| clause 7 · advertisement equals served | **self-devnet** | `cc_storage_earliest_available_slot == cc_p2p_*` |

**Clause 6's negative side is the load-bearing half:** a node that serves everything and never returns
`ResourceUnavailable` fails this clause as surely as one that returns it always. Note it has **two
rows at two venues** — `[PLAN]` §5 counts one.

---

## `S3b-W-09` · Phase 4 clause 2 — cursor fallback in three stages

**Venue** **in-process-double + self-devnet — no Hoodi time required** · **Wall-clock** 1 wk ·
**Op-days** 4–6 · **Discharges 3 rows** · **Re-run cost: full window**

| Stage / row | Venue | Threshold |
|---|---|---|
| attribution | **in-process-double** | exactly 1 of each reason on `cc_storage_stream_reconnect_total`; **stage only** (D-14) |
| hole recorded | **self-devnet** | the hole durably in `ServeWindow.holes`; parent-linkage walk; **stage only** (D-14) |
| **hole closed** | **self-devnet** | store has **no gap** after the parent-linkage walk |

**The clause is discharged only when the third stage is present** (D-14). The first two rows are
`stage only` — they are individually countable toward M2, but **clause 2 as a clause is not satisfied
without `hole closed`**. A window that produces attribution and a recorded hole has cleared 2 of 3
rows and 0 of 1 clauses; report both numbers.

**This window needs no Hoodi time.** It can therefore run concurrent with a Hoodi window under A-4 —
`[PLAN]` §5 schedules it at Hoodi, where none of its three rows would discharge.

---

## `S3b-W-10` · **OQ-1** — five foreign clients + M2e

**Venue** Hoodi · **Wall-clock** 2–3 wk · **Op-days** 8–12 · **Discharges** 1 row **+ M2e** ·
**Prereq** external lead time; **sourcing began at S2** (`S2-B-15`) · **Re-run cost: HIGH — reschedules
five parties**

**Clause** — foreign-peer probe against **all five** clients, **5–10 peers each**. **M2e** — codec
validation **5/5**: they decode our requests and we decode theirs. Baseline **0/5**, and OQ-1's
baseline is **0 peers for all five clients**.

**M2e is the direct falsifier for P0-04, P0-05 and P0-06.** Those three were fixed at S0 against
hand-written fixtures; this is where the fix meets five independent implementations. A round-trip
against our own encoder proves nothing — that is exactly the P0-06 failure shape (probe and node agree
with each other and both diverge from spec).

**The re-run cost is the highest in the phase and it is not ours to absorb.** Five external parties
must be rescheduled. `[PLAN]` §12: starting outreach at S1 instead of S2 removes W10 from the critical
path — but buys **nothing** toward the interop **fixes** that 0/5 → 5/5 may require. Budget fix time
inside `S3b-FIX`.

---

## `S3b-W-11` · **R-15** — the post-selection Hoodi confirmation window

**Venue** Hoodi · **Wall-clock** 1 wk · **Op-days** 3–4 · **Discharges** 0 rows ·
**Prereq** `S3b-D-01` (D-2 resolved) · **Re-run cost: full window**

**`[PLAN]` §5's window table has no row for this, and its §11 R-15 entry budgets it "folded into S3b's
upper bound".** It is given an id here so it cannot vanish between the table and the range — and so
the decision to skip it, if taken, is explicit.

**The risk (R-15, top-5 #2).** M1 is read at W3 on the **pre-selection** topology (two-process /
`Ipc`). If the program then selects Single Hull, **the primary metric — the one that outranks
everything — was observed on a topology that is about to change.** §9.0's A/B is a **self-devnet**
procedure; M1 is a **Hoodi** metric, so the A/B does not cover it.

**Two acceptable outcomes, and one unacceptable one**
- ✅ run this window post-selection and re-observe M1 on the selected topology; **or**
- ✅ record an **explicit decision** that self-devnet A/B is sufficient for the swap, with the reasoning
  and the signer;
- ❌ **not deciding.** That is the failure mode `[PLAN]` names.

**If the selection is `Ipc` (Gatehouse & Keep), this window is unnecessary** — the topology M1 was
observed on is the topology that ships. Record that as the outcome rather than skipping silently.

---

## `S3b-D-01` · **D-2 resolved** — read X1–X5 off the data

**Est** 2–4 pd (this is *decision* work, not window work) · **Prereq** W2–W10 collected ·
**Discharges** `[PRD]` §9's open decision · **S3b exit criterion**

**The decision rule, unchanged from `[PRD]` §9:**

> **X1 and X3 are the only criteria that can carry Gatehouse & Keep on their own.** If both come back
> clean, the panel's 2–1 default (Single Hull) stands. **Absent evidence is not a vote for the
> default** — an S3 that produces no measurement on X1–X5 does not discharge this decision.

**Acceptance (falsifiable)**
1. Each of X1–X5 carries a **reading** with the window it was read from and the reader who read it.
2. **X1 and X3 each carry a positive or negative reading, never an absent one.** An X1 of 0 is
   acceptable **only** if `S3b-W-00` confirmed the counter increments through the full collection path
   — otherwise a 0 is an absent reading wearing a number.
3. The decision is recorded as an **ADR**, alongside `S3a-B-22`'s X3 restatement.
4. Whichever transport is selected, **the losing seam impl is not deleted** (`[ARCH]` §9.2) — it is
   demoted to a test fixture and remains the conformance suite's second subject.
5. `S3b-W-11`'s outcome (run / explicitly waived) is recorded in the same ADR.

---

## `S3b-FIX` · Stream A — defect fixes found by windows

**The pointable part of this phase.** `[PLAN]` §9 assigns stream A to defect fixes during S3b while
stream B operates the windows.

**Estimate: unsizable in advance, and saying so is the honest answer.** The number of defects a first
live soak surfaces is exactly the thing the soak exists to discover. Two things bound it:

- `[PLAN]` §5's window durations **already include one re-run allowance each**, which implicitly
  budgets the fix-and-rerun cycle for one defect per window.
- W10's interop fixes are the known unknown: `[PLAN]` §12 states plainly that starting OQ-1 outreach
  early buys nothing toward *"the interop **fixes** that 0/5 → 5/5 may require"* — i.e. the source
  anticipates fixes and declines to size them.

**Working assumption for capacity planning** — reserve **one full-time stream-A engineer for the
duration of S3b**, ≈ 50–70 pd. This is capacity held, not work scoped. Track actual defects as issues
filed against their owning phase's file, not against this line.

**The rule that governs every one of them:** a wiring gap found mid-window is a defect fixed and the
**window re-run**, not a window amended. A clause row edited to remove `NOT_RUN` without a run is a
`[PRD]` §7.5 anti-metric violation.

---

## S3b exit criteria

| # | Criterion | Earned by |
|---|---|---|
| **M1 observed** | A foreign peer's block imports end to end on Hoodi **and the node holds head** — `phase-2-soak.md` clause 2 + clause 3 with the `le=1` head-lag bucket ≥ 0.95, then **sustained** | `S3b-W-03` |
| **M2 = 0** | 34 clause rows discharged **by running windows**. A row edited to remove `NOT_RUN` without a run is an anti-metric violation, not progress | `S3b-W-01` … `S3b-W-10` |
| **M2e = 5/5** | five foreign implementations decode our requests and we decode theirs | `S3b-W-10` |
| **M6 observed** | accepted-deposit set matches a reference client over the same window, **on a non-zero-GVF venue** | `S3b-W-02` (fallback W3, then an injected-deposit devnet) |
| **D-2 resolved** | read off X1–X5, recorded as an ADR, with **X1 and X3 each carrying a positive or negative reading, never an absent one** | `S3b-D-01` |

---

## Notes on this phase's estimates

| # | Note |
|---|---|
| 1 | **No story points appear in this file.** Per `[PLAN]` R-19 and A-1, S3b does not scale with headcount. Assigning points would make the phase look compressible in a sprint tool, which is the specific error the plan warns against. |
| 2 | **Operator-days are ≈ this decomposition's judgement**, derived from window duration and the evidence-capture work each clause table implies. `[PLAN]` sizes the windows in wall-clock and does not price operator effort. They are given so the phase can be staffed; they are **not** a schedule input. |
| 3 | **The wall-clock figures are `[PLAN]` §5's, carried unchanged**, including its one-re-run allowance. Σ 10–14 wk with W6/W7 and W10's outreach overlapped, matching `[PLAN]` §3/S3b's stated range. This is the one phase where the decomposition introduces **no drift** — because there is nothing to decompose. |
| 4 | **`S3b-W-11` is the only added row**, and it is added because `[PLAN]` §11/R-15 budgets the window in prose while §5's table omits it. Giving it an id costs nothing and stops a budgeted week from evaporating. |
| 5 | **Phase 4's per-window attribution was re-derived against `docs/phase-4-soak.md:1063-1073` and `[PLAN]` §5 was found to undercount it by 3 rows, at the wrong venues** — see the venue finding above. **Phases 2 and 3 have not been re-derived row by row** (`phase-2-soak.md:474`, `phase-3-acceptance.md:492`); do that before booking W3 and W4, since the same clause-vs-row and venue-vs-window slippage may be present. `scripts/soak-report.sh --phase N` is the authority, not this file. |
