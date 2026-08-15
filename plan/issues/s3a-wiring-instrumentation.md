# S3a — wiring completion + instrumentation · wk 26–35

**Entry.** S2 shipped; **D-2's criteria X1–X5 fixed in advance** (`[PRD]` §9 requires this before S3
opens; `[ARCH]` §9.1 adds that the *instruments* must be too).
**Ships.** A node that can be soaked. The stage ends when the node is wireable-complete **and the
X1/X2/X3 instruments are validated** — before any window opens.

**Out of scope** (`[PLAN]` §3/S3a, `[ARCH]` §9.2):

- **Selecting the E1/E2 transport.** That is an **S3b exit** decision, not an S3a one (R-14).
- **Deleting the losing seam impl** — it is the conformance suite's second subject.
- **Loop A** (queue taxonomy, `ValidationPoolState` sharding) — **L**, topology-dependent, sequenced
  after S3; Q-8 is only measurable here, which is the point.
- **`bin/serve-probe` taking `cc-wire`** — ADR-P4-12's independence claim is why P0-06 exists.

Estimate provenance and the points scale are defined in [`s0a-gate-restoration.md`](s0a-gate-restoration.md).

---

# ⚠ The two issues that gate everything after this stage

**`S3a-B-19` + `S3a-B-20` (X1) and `S3a-B-23` + `S3a-B-24` (X3) are on the critical path by
construction, not by size.** Between them they are **7–11 person-days that gate 10–14 weeks of
acceptance windows.**

`[PRD]` §9's decision rule: **X1 and X3 are the only two criteria that can carry Gatehouse & Keep.**
If both come back clean, the panel's 2–1 default (Single Hull) stands. **Absent evidence is not a
vote for the default** — an S3 that produces no measurement on X1–X5 does not discharge the decision.

Both are **unmeasurable today**: ⟡ D-11 for X1, ⟡ D-3 for X3. Fixing only one lets the same
default-by-accident arrive through the other door.

| | X1 | X3 |
|---|---|---|
| **Owner** | Stream B (p2p) — **named individual, recorded in the sprint** | Stream B (p2p), with the soak operator as reviewer |
| **Deadline** | **wk 33** — ≥ 2 weeks before W0 opens at wk 36 | **wk 33**, same |
| **Definition of done** | **Validated, not shipped.** A counter demonstrated incrementing against a deliberately injected panic in a p2p task on the self-devnet, labelled by task | **Exercised, not written.** The classification procedure run against a deliberately induced p2p disconnect on the self-devnet, producing a filled-in incident record |
| **If it slips** | The soak produces no X1 evidence and D-2 defaults to Single Hull **by accident rather than by data**. The mitigation is **not** "decide anyway" — it is to **hold the window** | Same shape, through the other door |

**Why "validated" is the exit and "merged" is not.** `services/p2p/src/supervisor.rs` implements a
per-task panic policy (ADR-P2-13 ✓, 7 citations): a task that panics is **caught and restarted**, so a
libp2p panic may never become a process abort and may never be attributed. **A counter wired to the
wrong catch path returns 0 for the wrong reason** — and an instrument that reports 0 because it was
never wired is worse than no instrument, because it reads as evidence. `[PRD]` §9 forbids reading
absent evidence as a vote for the default in as many words.

**`[PLAN]` §12 lists skipping these as a non-lever:** it buys 5–9 person-days and *"**nothing.** They
forfeit the D-2 decision."*

**R-14, the topology precondition.** X1 counts libp2p-attributable panics **in the `p2p` process**;
X2 is read off the **`Ipc` impl's** `Backpressure` counter. **If S3 selects `InProcess` before the
soak, neither exists to be measured.** S3a therefore wires and soaks on the **two-process `Ipc`**
configuration (`S3a-A-07`), and transport selection moves to S3b exit (C-7).

---

## Issue index

### Stream A — consensus core

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S3a-A-01` | P0-16 chain half — `DataAvailable` re-drives parked blocks | 3–4 | 8 | `S3a-B-05` |
| `S3a-A-02` | `crates/wire` skeleton — one codec | 2–3 | 5 | — |
| `S3a-A-03` | Delete the three codec copies (P1-A/7, P1-B/6) | 3–4 | 8 | `S3a-A-02` |
| `S3a-A-04` | P1-A/7 — the 32 MiB response-stream cap truncates legitimate responses | 1.5–2 | 3 | `S3a-A-03` |
| `S3a-A-05` | **M9 full** — `gossip receipt → durable`, entering at `ChainIngress` | 2.5–3 | 5 | `S3a-B-01` |
| `S3a-A-06` | M9 — the thinner **wire-half** test (⟡ D-12: two tests, not one) | 1.5–2 | 3 | `S3a-A-03` |
| `S3a-A-07` | **R-14** — record and enforce the two-process `Ipc` soak topology | 0.75–1.25 | 2 | — |
| `S3a-A-08` | **Q-8 measured** — is the 12 s gossip→chain wait ever reached? | 1–1.5 | 3 | `S3a-B-01` |
| | **Stream A total** | **15.25–20.75** | **37** | |

### Stream B — edge & platform

| Id | Title | pd | pts | Deps |
|---|---|---:|---:|---|
| `S3a-B-01` | **P0-17a** — `SwarmCommand::Subscribe` production sender | 3–4 | 8 | **P0-19/1 landed at S0** |
| `S3a-B-02` | P0-17a — §5.6 gossip scoring weights (today **0** on every topic) | 3–4 | 8 | `S3a-B-01` |
| `S3a-B-03` | **E3a.1** — a test per dead island asserting a production sender exists | 2–3 | 5 | all wiring |
| `S3a-B-04` | **P0-16 p2p half** — replace `NoopSamplingFeed` | 3–4 | 8 | — |
| `S3a-B-05` | P0-16 — `da_tx` wired to `ChainIngress::notify_data_available` | 2–3 | 5 | `S3a-B-04` |
| `S3a-B-06` | **P0-17b** — reconnect `kzg_tx`; the verify pool's dropped sender | 2–3 | 5 | — |
| `S3a-B-07` | P1-B/7 — the 12 s block on the chain import reply; redrive hoist | 2–3 | 5 | `S3a-B-06` |
| `S3a-B-08` | **P0-17c** — publish the serve window | 2–3 | 5 | `S0a-B-08` (Q-10) |
| `S3a-B-09` | P0-17c — `cc_storage_earliest_available_slot == cc_p2p_*` | 1.5–2 | 3 | `S3a-B-08` |
| `S3a-B-10` | **P0-17d** — backfill's client-side storage write method | 3–4 | 8 | `S2-A-04` |
| `S3a-B-11` | P1-A/16 — `drain_imports` **deadlocks** at any slot without a block | 3–4 | 8 | `S3a-B-10` |
| `S3a-B-12` | P1-A/17 — below-anchor results leak into the forward-only hold map | 2–3 | 5 | `S3a-B-11` |
| `S3a-B-13` | Backfill end-to-end test (~3,200 lines, zero call sites today) | 2–3 | 5 | `S3a-B-12` |
| `S3a-B-14` | P1-A/11 — persisted ENR seq never wired | 1.5–2 | 3 | — |
| `S3a-B-15` | P1-A/14 — re-dial churn loop on bad `app_score` | 1.5–2 | 3 | — |
| `S3a-B-16` | P1-A/15 — `ClosePeer` Goodbye almost never reaches the wire | 1.5–2 | 3 | — |
| `S3a-B-17` | P1-B/4 + P1-B/5 — production-dead serve handlers; stubbed score param | 2–3 | 5 | — |
| `S3a-B-18` | P1-D/18 + P2-C/1 — `fault_mode` out of production paths | 3–4 | 8 | — |
| `S3a-B-19` | **X1** — the supervisor panic counter, labelled by task | 1.5–2 | 3 | `S1-B-15` |
| `S3a-B-20` | **X1 VALIDATED** — demonstrated against an injected p2p-task panic | 1.5–2 | 3 | `S3a-B-19` |
| `S3a-B-21` | **X2** — the `Ipc` impl's `Backpressure` counter under real peer load | 2–3 | 5 | `S3a-A-07` |
| `S3a-B-22` | **X3 restated** — adopted as the operative wording, recorded as an ADR | 1–1.5 | 3 | — |
| `S3a-B-23` | **X3 procedure** — the incident classification procedure | 1–1.5 | 3 | `S3a-B-22` |
| `S3a-B-24` | **X3 EXERCISED** — run against an induced p2p disconnect | 1.5–2 | 3 | `S3a-B-23` |
| `S3a-B-25` | **X4 + X5** — instruments and named readers (E3a.8's missing half) | 1.5–2 | 3 | — |
| `S3a-B-26` | **E3a.5** — the five foreign clients sourced **and scheduled** | 0.75–1.25 | 2 | `S2-B-15` |
| `S3a-B-27` | M3 ledger maintenance for S3a rows | 0.5 | 1 | — |
| | **Stream B total** | **51.25–71.75** | **126** | |

**Phase totals.** **66.5–92.5 pd · 163 pts.** Against `[PLAN]`'s **58–90 pd**.

---

## ⚠ Stream imbalance — the largest scheduling finding in this decomposition

`[PLAN]` §9 assigns S3a as: **stream A** = *"DA feed chain half, M9, seam impl selection prep"*;
**stream B** = *"gossip subscribe, `cc-wire`, backfill, serve window, X1/X2 instruments, `fault_mode`
removal."* Decomposed against that assignment:

| | Stream A | Stream B |
|---|---:|---:|
| Effort | 15.25–20.75 pd | 51.25–71.75 pd |
| Duration at 4 effective pd/wk | 3.8–5.2 wk | **12.8–17.9 wk** |

**Stream B binds at 12.8–17.9 wk against `[PLAN]`'s stated 8–11 wk for the phase.** This is a real
finding, not a rounding difference: `[PLAN]` §9 puts nine of the eleven work items on one stream while
stream A idles for two thirds of the stage.

**Recommended rebalance** — move `S3a-B-10` … `S3a-B-13` (backfill, 10–14 pd) and `S3a-B-14` …
`S3a-B-17` (the p2p `patch @ S3` rows, 6.5–9 pd) to stream A, giving A 31.75–43.75 pd (7.9–10.9 wk)
and B 34.75–48.75 pd (**8.7–12.2 wk**), which fits the window at the low end. **This requires stream A to take p2p work**,
which crosses the §9 skill boundary (A is consensus core, B is edge & platform). That is a **staffing
decision, not a scheduling one**, and it is stated here rather than silently applied.

**What the rebalance must not touch.** `S3a-B-19` … `S3a-B-24` (X1, X3) stay with stream B and its
p2p owner. They are the two deliverables with a hard wk-33 deadline and they need the person who owns
`supervisor.rs` and `chain_stream/client.rs`.

---

## The X1 issues

### `S3a-B-19` · **X1** — the supervisor panic counter

**Stream** B · **Est** 1.5–2 pd / **3 pts** · ⌂ `[PLAN]` §3/S3a sizes X1 at 3–5 pd across both halves ·
**Deps** `S1-B-15` (ADR-P2-13's record, which states the catch path must gain a counter) ·
**Owner: named p2p engineer** · **Deadline: wk 33**

**Touch points**
- `services/p2p/src/supervisor.rs` — the per-task **catch** path (ADR-P2-13 ✓, 7 citation sites). This
  is the exact path the counter must hang off; a counter on the process-abort path returns 0 forever,
  because the policy is catch-and-restart

**Design** — a counter incremented in the supervisor's catch path, **labelled by task**. The label is
load-bearing: X1 asks for *libp2p-attributable* panics, and an unlabelled total cannot distinguish a
libp2p panic from an application-logic panic in an unrelated task.

**Acceptance** — the counter exists, is labelled by task, is exported on the metrics endpoint, and is
scraped by the soak's collector. **This issue does not close X1** — `S3a-B-20` does.

---

### `S3a-B-20` · **X1 VALIDATED** — demonstrated against an injected panic

**Stream** B · **Est** 1.5–2 pd / **3 pts** · **Deps** `S3a-B-19` · **S3a exit criterion E3a.3** ·
**Deadline: wk 33** · **Blocks: every S3b window**

**This is the issue that closes X1, and it is not "merge the counter".**

**Acceptance (falsifiable, all four)**
1. A panic is **deliberately injected** into a p2p task on the **self-devnet** — not a unit test, not
   a mock supervisor.
2. The counter increments by exactly 1.
3. The **task label** on the incremented series names the task that panicked.
4. A second injection into a *different* task increments a *different* labelled series — which is the
   only way to demonstrate the label is real rather than constant.

**Recorded artifact** — the scrape output before and after each injection, pasted into the S3a exit
note. `[PLAN]` §7: *"Definition of done: **demonstrated incrementing against an injected panic in a
p2p task**, labelled by task — not merely merged."*

**If this slips past wk 33** — hold W0. Do not open the window and collect an X1 reading of 0. That
reading would be indistinguishable from a genuine zero and would silently discharge D-2 toward the
default, which `[PRD]` §9 forbids.

---

## The X3 issues

### `S3a-B-22` · **X3 restated** — adopt the new wording

**Stream** B · **Est** 1–1.5 pd / **3 pts** · ⌂ ⟡ D-3 · **S3a deliverable, not a documentation task**

**X3 as written cannot be measured.** X3 asks whether the jittered reconnect loop *survives an
in-process port unchanged*. **Under `InProcess` it is not ported — it is not instantiated**
(`[ARCH]` §2.5). There is nothing to port, so the question has **no answer rather than a clean one**.
`[ARCH]`'s honest reading: *"the value it preserves — session resumption across a peer restart — is
exactly the value that has no meaning when the peer cannot restart independently."*

**The restatement, ⟡ D-3's form, adopted verbatim:**

> *Did any incident in the soak window require a reconnect-and-resume that a single process could not
> have handled by restarting?*

**Why adopting it is itself a deliverable.** `[PRD]` §9 requires the criteria be **fixed before S3
opens**. X3 as written is not one of them. Leaving it unrestated means S3b runs against a criterion
that cannot return a reading.

**Acceptance** — the restatement is **committed as the operative X3 wording**, recorded as an ADR
alongside the D-2 decision record, and `[PRD]` §9's X3 row is updated to match. A restatement that
lives only in a plan document has not replaced the criterion.

---

### `S3a-B-23` · **X3 procedure** — the incident classification procedure

**Stream** B · **Est** 1–1.5 pd / **3 pts** · **Deps** `S3a-B-22` · **Deadline: wk 33**

**Unlike X1, restated-X3 is not read off a counter — it is read off *classified incidents*.** If
nobody defines the classification before W2, **X3 returns "no incidents" for the same wrong reason X1
would have returned 0.**

**The procedure names, per incident:**
1. **What disconnected** — which peer, which side, what the trigger was.
2. **Whether the seam resumed from a cursor** — i.e. whether the jittered reconnect loop did work that
   mattered.
3. **Whether a whole-process restart would have lost committed progress** — the actual X3 question.

**Acceptance** — the procedure is committed as an operator document with a filled-in **template**, and
the soak operator has reviewed it. A procedure the operator has not read is not a procedure.

---

### `S3a-B-24` · **X3 EXERCISED** — run against an induced disconnect

**Stream** B · **Est** 1.5–2 pd / **3 pts** · **Deps** `S3a-B-23` · **S3a exit criterion E3a.7** ·
**Deadline: wk 33** · **Blocks: every S3b window**

**Acceptance (falsifiable)**
1. A p2p disconnect is **deliberately induced** on the self-devnet.
2. The procedure is run against it, producing a **filled-in incident record** with all three fields
   answered — not "N/A", not blank.
3. The record is committed alongside the S3a exit note as the worked example the soak operator will
   pattern-match against.

`[PLAN]` §7: *"X3's restatement and classification procedure carry the same deadline and the same
definition-of-done shape"* as X1 — *"fixing one and not the other leaves the decision defaulting by
accident through the other door."*

---

## The other three criteria

### `S3a-B-21` · **X2** — the `Ipc` impl's `Backpressure` counter

**Stream** B · **Est** 2–3 pd / **5 pts** · ⌂ `[ARCH]` §2.5 · **Deps** `S3a-A-07` ·
**S3a exit criterion E3a.4**

X2 asks whether the p2p↔chain seam exhibits backpressure-loss incidents once modelled as an
in-process channel. `[ARCH]` §2.5 names the direct instrument: **instrument the `Ipc` impl for
`Backpressure` frequency during the first soak.**

**R-14 applies here as much as to X1** — if the transport is selected as `InProcess` before the soak,
the `Ipc` impl is not running and X2 has nothing to read.

**Acceptance** — the counter is **emitting non-zero under saturating self-devnet load**. A counter
that has only ever read 0 under test load has not been shown to work; saturate the seam deliberately
and record the reading.

---

### `S3a-B-25` · **X4 + X5** — instruments and named readers

**Stream** B · **Est** 1.5–2 pd / **3 pts** (≈ — **this decomposition's addition**) ·
**Discharges** the missing half of E3a.8

**E3a.8 requires that *all five* of X1–X5 have a named instrument and a named reader, recorded before
W0 opens.** `[PLAN]` §3/S3a creates work items for X1, X2 and X3 only. **X4 and X5 have an exit
criterion and no deliverable** — flagged as under-specified.

| | X4 | X5 |
|---|---|---|
| **Criterion** (`[PRD]` §9) | whether any **dead-wiring regression** recurs at the p2p seam during S3 despite the compile-time coupling of S1–S2 | the **operator-observed cost of the split** at S3: restart-choreography incidents, lockstep-version incidents, split-brain peer-state incidents attributable to the boundary |
| **Decides toward** | recurrence → **Single Hull** | material → **Single Hull** |
| **Proposed instrument** | `S3a-B-03`'s per-island production-sender tests, run continuously in CI, plus a count of newly-discovered zero-sender/zero-caller surfaces opened during S3a | an operator incident log with the three named categories, filled from the same soak-window incident stream `S3a-B-23`'s procedure reads |
| **Reader** | engineering owner | the soak operator |

**Acceptance** — both instruments exist, both have a **named individual** as reader, and both are
recorded in the S3a exit note **before W0 opens**. Note that both X4 and X5 decide toward Single Hull,
so a null reading on either is *not* the decision-integrity hazard X1 and X3 carry — but E3a.8 still
requires them recorded rather than assumed.

---

### `S3a-A-07` · **R-14** — record and enforce the soak topology

**Stream** A · **Est** 0.75–1.25 pd / **2 pts** · ⌂ `[PLAN]` C-7 / R-14 (top-5 risk #1) ·
**S3a exit criterion E3a.6**

**The soak's own topology is a precondition for two of the five decision criteria, and neither source
document constrains it.** This issue is the constraint, written down.

**Acceptance**
1. S3a wires and soaks on the **two-process `Ipc`** configuration; recorded as a decision, not a
   default.
2. **Both seam impls build and pass conformance; neither is deleted** (`[ARCH]` §9.2's S3
   prohibition — the losing impl is the conformance suite's second subject).
3. `S3b`'s exit criteria carry the transport selection; **S3a's do not** (C-7).
4. **R-15 is recorded as an open item**: if W3 reads M1 on two-process/`Ipc` and the program then
   selects Single Hull, the primary metric was observed on a topology about to change. `[PLAN]`
   budgets a post-selection Hoodi confirmation window — see [`s3b-acceptance-windows.md`](s3b-acceptance-windows.md) `S3b-W-11`. **Not
   deciding is the failure mode.**

---

## The wiring issues

### `S3a-B-01` · **P0-17a** — the gossip subscribe production sender

**Stream** B · **Est** 3–4 pd / **8 pts** (≈) · **Discharges** P0-17a ·
**Hard dependency: P0-19/1 must already be landed at S0** (D2 / R-12)

`SwarmCommand::Subscribe` has a handler and **zero senders** ✓ (`services/p2p/src/host.rs:1299`). The
node runs its most spec-sensitive logic only in tests.

**Why the P0-19 dependency is real even though P0-19 does not *cause* the stall.** Gossip fires the
cache stall on the **live-import** path, where the failure names no cause: *"we wired gossip, the node
connects, peers are healthy, and it imports nothing"* — **no `Reject`, no descore, no invalid-block
metric, health DAG green** (R-12, `[PRD]` §5.1.2/2). P0-19 fires on boot today; P0-17a widens the
blast radius. If `S0-A-01` did not land, this issue must not.

**Acceptance** — a production sender exists; a self-devnet node subscribes to the correct topic set
for the current fork digest; `S3a-B-03`'s island test covers it.

---

### `S3a-B-02` · P0-17a — §5.6 gossip scoring weights · 3–4 pd / **8 pts** (≈)
Gossip topic scoring weight is **0 on every topic** today ✓ (`services/p2p/src/gossip/scoring.rs:6`,
ADR-P2-10, with `OQ-P2-3` deferred). **P0-17a wires §5.6 scoring at S3 — the deferral ends; record the
new weights** in the ADR (`S1-B-16` wrote the placeholder).
**Acceptance** — non-zero weights per §5.6; W3's Phase-2 clause (*scoring penalty crossing the −4000
bucket*) is reachable, which it is not while every weight is 0.

### `S3a-B-03` · **E3a.1** — a test per dead island · 2–3 pd / **5 pts** (≈)
`[PLAN]` E3a.1: *every dead island in `[PRD]` §1.A has a production sender/caller, **asserted by a
test per island, not by inspection***.
**Islands** — gossip subscribe (`host.rs:1299`), the KZG verify pool (`service.rs:714`), serve-window
publication (`serve.rs:1086-1087`), backfill's client write method (`storage_client.rs:210`), the DA
feed (`das/sampling.rs:154,200,538`), proto-array prune (`proto_array.rs:325`, closed at S0).
**Acceptance** — six tests, each failing if its production sender/caller is removed. Also the X4
instrument (`S3a-B-25`).

---

### `S3a-B-04` · `S3a-B-05` · `S3a-A-01` · **P0-16** — the real DA feed

**Combined** 8–11 pd / **21 pts** (≈ — *the fork's central mechanism has no production path today*) ·
**Discharges** P0-16 (p2p and chain halves; the engine half landed at `S1-B-01`)

`NoopSamplingFeed` and `da_tx = None` sever the PeerDAS loop from both p2p and engine to chain, so
**every blob-carrying block parks and is dropped after 4 slots**.

- `S3a-B-04` (3–4 pd) — replace `NoopSamplingFeed` with the production feed ✓
  (`services/p2p/src/das/sampling.rs:154,200,538`).
- `S3a-B-05` (2–3 pd) — `da_tx` wired to `ChainIngress::notify_data_available` (E2, through `cc-seam`
  — **not** a direct `cc-p2p` → `cc-chain` call; `S1-A-13`'s dag rule enforces this).
- `S3a-A-01` (3–4 pd) — the chain half: `DataAvailable` re-drives parked blocks in the `import` lane
  (it **stays in that lane, not above it** — `[ARCH]` §3.2/`[q1]` §2.3).

**Acceptance** — a blob-carrying block on self-devnet imports rather than parking and being dropped
after 4 slots. Note P2-A/6 (`das/sampling.rs:596`, `expire_task` lacks the M1 empty-required guard and
emits a **false `DataAvailable`**) is in the same file and should be checked while here — it is a P2-A
row folded into its owning stage.

---

### `S3a-B-06` · `S3a-B-07` · **P0-17b** — the KZG reconnect and the 12 s wait

**Combined** 4–6 pd / **10 pts** · ⌂ `[ARCH]` §3.8 sizes P0-17b **S/M** · **Discharges** P0-17b,
P1-B/7

- `S3a-B-06` — `kzg_tx: _` is dropped at destructure ✓ (`services/p2p/src/service.rs:714`), so the
  verify pool is unreachable. Reconnect it. ADR-P2-08 (KZG cross-sidecar batching with per-sidecar
  re-verification before penalising) and ADR-P2-02 (dedicated OS threads for the pool) both survive
  and are load-bearing — do not restructure the pool while reconnecting it.
- `S3a-B-07` — P1-B/7: the single-worker validation loop blocks up to **12 s** on the chain
  block-import reply, stalling all gossip validation ✓
  (`services/p2p/src/gossip/validate/pipeline.rs:621`). Slot-bounded wait + redrive hoist.

**Scope boundary.** This is the P0-17b half of P1-D/09. **Loop A** — the queue taxonomy, concurrency
and `ValidationPoolState` sharding — is **L**, topology-dependent, and sequenced **after S3**
(`[ARCH]` §3.8, `[PLAN]` C-10). Do not pull it in.

---

### `S3a-B-08` · `S3a-B-09` · **P0-17c** — the serve window

**Combined** 3.5–5 pd / **8 pts** (≈) · **Deps** `S0a-B-08` (Q-10 must have closed the
"never published" claim first) · **Discharges** P0-17c

Today **every block and column serve answers `ResourceUnavailable`**, seeded
`earliest_available_slot: u64::MAX` ✓ (`services/storage/src/serve.rs:1086-1087`), matching p2p's
`EMPTY_WINDOW_SLOT`. Post-S2 the mechanism is **one `AtomicU64` read** — already the shape p2p uses
internally ✓ (`services/p2p/src/backfill/window.rs:1`, ADR-P2-14, which **replaces E6**).

**Acceptance**
- `S3a-B-08` — a served block returns data; `ResourceUnavailable` is returned **only** below `eas`.
  That negative is W8's clause 6 and is the direct falsifier here.
- `S3a-B-09` — `cc_storage_earliest_available_slot == cc_p2p_*` advertisement, asserted by a test;
  this is W8's clause 7.

---

### `S3a-B-10` … `S3a-B-13` · **P0-17d + P1-D/12** — backfill

**Combined** 10–14 pd / **26 pts** (≈ — *~3,200 lines with zero call sites*) · **Discharges** P0-17d,
P1-A/16, P1-A/17, P1-D/12

| Id | Item | Touch |
|---|---|---|
| `S3a-B-10` | the client-side storage write method, **absent** — becomes `storage_core::backfill::admit()` behind `ArchiveWrite` (E5 as an RPC is deleted at S2) | `services/p2p/src/storage_client.rs:210` |
| `S3a-B-11` | P1-A/16 — `drain_imports` **deadlocks** the oldest-first cursor at any slot without a block; the cursor **cannot represent missed proposals** | `services/p2p/src/backfill/planner.rs:1158` |
| `S3a-B-12` | P1-A/17 — below-anchor batch results leak into the forward-only hold map, **permanently blocking completion** | `services/p2p/src/backfill/planner.rs:1114` |
| `S3a-B-13` | the end-to-end test the 3,200 lines have never had | — |

**Acceptance for `S3a-B-11`** — a backfill across a range containing at least one **empty slot**
completes. This is the direct falsifier and it is the case that deadlocks today.

---

### `S3a-B-14` … `S3a-B-17` · p2p `patch @ S3` rows · 6.5–9 pd / **14 pts** (≈)

| Id | Row | Touch | Defect |
|---|---|---|---|
| `S3a-B-14` | P1-A/11 | `services/p2p/src/service.rs:535` | CC-4E persisted ENR seq never wired: seq resets to ~1 on every restart / discovery respawn |
| `S3a-B-15` | P1-A/14 | `services/p2p/src/peer_manager/dial.rs:101` | scheduler re-dials peers just disconnected for bad `app_score` → **1 s connect/Goodbye churn loop** |
| `S3a-B-16` | P1-A/15 | `services/p2p/src/host.rs:1342` | `ClosePeer` sends Goodbye then immediately disconnects — Goodbye almost never reaches the wire |
| `S3a-B-17` | P1-B/4, P1-B/5 | `services/p2p/src/reqresp/server.rs:111`, `limits.rs:329` | `serve_block_protocol` / `serve_column_protocol` are **production-dead** and `host.rs` re-implements the pipeline and **has already drifted**; `record_violation`'s `&mut f64` score param is always stubbed, double-counting the penalty metric |

`S3a-B-15` note: ADR-P2-09 (*score decay ticks; a bad score does not itself disconnect*) survives —
the fix is in the dial scheduler, not the scoring policy.

---

### `S3a-B-18` · P1-D/18 + P2-C/1 — `fault_mode` out of production paths

**Stream** B · **Est** 3–4 pd / **8 pts** (≈) · **Discharges** P1-D/18, P2-C/1

A **1,900-line `fault_mode` global is consulted by the column validator** — test-harness state on a
production path. P2-C/1: `services/p2p/src/fault_mode.rs:16` carries a file-wide
`#![allow(clippy::unwrap_used, expect_used)]` exempting ~1,600 lines of **production** code, far
beyond the test-module allowance in `docs/dev-conventions.md`.

**Acceptance** — the column validator has no reference to `fault_mode`; the file-wide `allow` is gone
or the file is `#[cfg(test)]`-only; `cargo clippy` is green without it. Interacts with `S0-B-13`
(P1-A/9), which added two REJECT conditions to the same validator.

---

## `crates/wire` and M9

### `S3a-A-02` · `S3a-A-03` · `S3a-A-04` · `cc-wire` · 6.5–9 pd / **16 pts** (≈)

Three codec copies deleted. `S3a-A-03` discharges **P1-B/6** — `request_limits` duplicated in
`crates/libp2p/src/ssz_snappy_codec.rs:204` and `Protocol::request_limits`, **and the two already
disagree** (`S0-B-06` made them agree at S0; this deletes the duplication).

**`bin/serve-probe` must NOT take `cc-wire`** (`[ARCH]` §9.1/S3, ADR-P4-12). Its independent codec is
the reason P0-06 exists. Add this as a `check-crate-dag.sh` prohibition in the same PR as
`S3a-A-02`, in the style of `S1-A-01`.

`S3a-A-04` — P1-A/7: the 32 MiB response-stream cap ✓ (`crates/libp2p/src/ssz_snappy_codec.rs:35`)
**silently truncates** legitimate column/block responses.
**Acceptance** — a response above the old cap is either streamed or rejected with a named error;
silent truncation is impossible, asserted by test.

### `S3a-A-05` · `S3a-A-06` · **M9** · 4–5 pd / **8 pts**
⌂ `[ARCH]` §8.2 ⟡ D-12: **two tests, not one.** `S3a-A-05` is reading (a) — `gossip receipt → durable`,
**entering at `ChainIngress`**. `S3a-A-06` is the separate, thinner **wire-half** test.
**Acceptance (E3a.2)** — M9 exists and runs in CI. Combined with S2's `import → durable` half
(`S2-A-14`), M9's target — *"exists and runs in CI"* — is met.

### `S3a-A-08` · **Q-8 measured** · 1–1.5 pd / **3 pts**
`[PLAN]` §8 marks Q-8 **explicitly NOT a spike**: *is the 12 s gossip→chain wait ever reached in
practice?* It is **measured at S3a with gossip live**; moving it earlier is meaningless while gossip
is unsubscribed, because its head-of-line argument is purely analytic until then.
**Acceptance** — a distribution of the wait recorded over a self-devnet run with gossip live, written
into the S3a exit note as the input to Loop A's post-S3 scoping. This does **not** authorise starting
Loop A.

### `S3a-B-26` · **E3a.5** — five foreign clients sourced **and scheduled** · 0.75–1.25 pd / **2 pts**
**Deps** `S2-B-15`. The criterion is *sourced and scheduled*, not *contacted*. Acceptance: five named
implementations, five named contacts, five tentative dates on the calendar, recorded in the exit note.

### `S3a-B-27` · M3 ledger maintenance · 0.5 pd / **1 pt**

---

## S3a exit criteria — and which issue earns each

| # | Criterion | Earned by |
|---|---|---|
| E3a.1 | Every dead island has a production sender/caller, **asserted by a test per island** | `S3a-B-03` |
| E3a.2 | **M9** exists and runs in CI | `S3a-A-05`, `S3a-A-06` (+ `S2-A-14`) |
| E3a.3 | **X1 counter demonstrated against an injected p2p-task panic** | **`S3a-B-20`** |
| E3a.4 | X2 counter emitting non-zero under saturating self-devnet load | `S3a-B-21` |
| E3a.5 | M2e precondition: five foreign clients **sourced and scheduled** | `S3a-B-26` |
| E3a.6 | Both seam impls build and pass conformance; **neither is deleted** | `S3a-A-07` |
| E3a.7 | **X3 restated and its classification procedure exercised** against an induced p2p disconnect | **`S3a-B-24`** |
| E3a.8 | **All five of X1–X5** have a named instrument and a named reader, recorded before W0 opens | `S3a-B-20`, `S3a-B-21`, `S3a-B-24`, **`S3a-B-25`** |

---

## Drift against `[PLAN]` §3/S3a — stated, not smoothed

| # | Observation |
|---|---|
| 1 | **The stream imbalance above is the headline.** As `[PLAN]` §9 assigns the streams, stream B carries 51.25–71.75 pd and binds the phase at **12.8–17.9 wk** against a stated 8–11. The rebalance is a staffing decision. |
| 2 | **Total.** `[PLAN]` 58–90 pd; decomposed **66.5–92.5 pd**. The +8.5 at the floor is `S3a-B-25` (X4/X5, unpriced in `[PLAN]`), `S3a-B-03` (E3a.1's per-island tests, a named exit criterion with no line item), `S3a-A-08` (Q-8's measurement, which `[PLAN]` §8 schedules here but does not price), and `S3a-A-07` (R-14's topology record). |
| 3 | **X4 and X5 have an exit criterion (E3a.8) and no deliverable in either source.** `S3a-B-25` fills the gap with proposed instruments. Both decide toward Single Hull, so the decision-integrity stakes are lower than X1/X3 — but E3a.8 as written is unsatisfiable without them. |
| 4 | **P0-17a and P0-16 are 8–12 pd each in `[PLAN]` with no sub-structure.** The decomposition here (3–4 pd per sub-item) is ≈ this plan's judgement, derived from the `file:line` evidence rather than from a source sizing. Treat the sub-issue boundaries as a proposal, not as an estimate with provenance. |
| 5 | **P2-D/20** (brittle self-referential tests, 1,600-line god-metric facades, racy hand-rolled gauges) is *opportunistic* in `[PRD]` and lands nowhere. Several of its instances live in files this stage rewrites. Flagged in [`README.md`](README.md)'s open-decisions table. |
