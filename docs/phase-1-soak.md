# Phase 1 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section.

| Section | Owner |
|---|---|
| `## CC-1H early gate` | CC-13d |
| `## OQ-3 — comptests` | CC-15a |
| `## CC-1H mid gate` | CC-18d |
| `## Run record` | CC-1Ad (numbers; **Status: NOT_RUN** until 24 h window) |
| `## Timing` | CC-1Ad (numbers; **Status: NOT_RUN** until report emits) |

## CC-1H early gate

**Date:** 2026-08-07  
**Machine:** Apple M4 Pro, 24 GB RAM, macOS aarch64 (dev machine)  
**Method:** `process_slots` across **5** epoch boundaries from the committed
Hoodi anchor state (slot `3649472`, state root
`0x2d4f2b8d81bcb72c3556846a532fca7679e9349b0dc37c9c0812f383daa41c55`), empty
slots only. Cache warmed with one `canonical_root()` before measurement.
Hash share = wall time spent inside `measured_canonical_root` /
epoch wall time.

**Command:**

```text
cargo test -p cc-state-transition --test cc1h_early_gate -- --ignored --nocapture
```

| epoch | from → to | wall (ms) | hash (ms) | hash share | root calls |
|---:|---|---:|---:|---:|---:|
| 0 | 3649472 → 3649504 | 689.91 | 52.15 | 7.6 % | 32 |
| 1 | 3649504 → 3649536 | 522.69 | 143.26 | 27.4 % | 32 |
| 2 | 3649536 → 3649568 | 524.27 | 143.53 | 27.4 % | 32 |
| 3 | 3649568 → 3649600 | 530.35 | 144.92 | 27.3 % | 32 |
| 4 | 3649600 → 3649632 | 533.97 | 142.26 | 26.6 % | 32 |

| Aggregate | Value |
|---|---|
| max wall | **689.91 ms** |
| mean wall | 560.24 ms |
| mean hash share | **23.3 %** |
| max hash share | 27.4 % |

### Threshold verdict

**&lt; 700 ms** (max epoch wall 689.91 ms). Proceed with flat-struct state backing.
CC-1H remains a P2 contingency and is **closed for the early gate**.

### Attribution verdict

**&lt; 25 % hashing** (mean 23.3 %). The epoch figure is transition-side arithmetic
dominant, not `canonical_root()`-bound. **CC-1H must not be triggered** on this
number — milhouse would not address the cost. If later gates regress, respond
with a per-handler profile plus CC-1I batching rather than a state-backing swap.

### Explicit decision

**CC-1H is not promoted.** Early gate **closed**; re-measure only at the mid
gate (CC-18d) if the mid-gate path is exercised, or if R-3 early-warning
signals change. M1.2 may exit without landing CC-1H.

## OQ-3 — comptests

**Date:** 2026-08-07  
**Artifact:** `comptests.tar.gz` (pinned digest
`a4319cd2e0253433022b3261ed53ee20c3bccb33517454ddce1a895280ac4e4e` in
`spec-vectors.lock`, tag `v1.7.0-alpha.13`).  
**Method:** list archive contents; inspect Fulu fork-choice material against
the Clause 1 suite table (`mainnet.tar.gz` / `minimal.tar.gz` →
`tests/<preset>/fulu/fork_choice/…`).

### Directories inspected

| Path in archive | Kind |
|---|---|
| `tests/minimal/fulu/fork_choice_compliance/` | **Pre-generated vector cases** (SSZ-snappy + `steps.yaml`) |
| `tests/minimal/{altair,bellatrix,capella,deneb,electra,gloas}/fork_choice_compliance/` | Same suite for other forks (out of Phase 1 Fulu scope) |
| `tests/formats/fork_choice/` | Shared step-format docs (same as standard `fork_choice`) |
| `tests/generators/compliance_runners/fork_choice/` | Generator sources (MiniZinc models, yaml configs) |
| `tests/core/pyspec/…/test/fulu/fork_choice/` | Pyspec *source* generators only (not runnable vectors) |
| `tests/formats/fast_confirmation/` + phase0 pyspec | **Not** Fulu fork-choice (separate format; no Fulu emission) |

No `tests/mainnet/**/fork_choice_compliance` paths exist in this artifact
(minimal preset only).

### Fulu emission under `fork_choice_compliance`

Handlers and case counts (each case is a `pyspec_tests/<name>/` directory with
`steps.yaml`, `anchor_state.ssz_snappy`, `anchor_block.ssz_snappy`, and step
payloads):

| Handler | Cases |
|---|---:|
| `attester_slashing_test` | 128 |
| `block_cover_test` | 192 |
| `block_tree_test` | 512 |
| `block_weight_test` | 256 |
| `invalid_message_test` | 128 |
| `shuffling_test` | 256 |
| **Total Fulu** | **1472** |

**Case shape (for CC-15c's runner):** identical to standard
`fork_choice` format (`tests/formats/fork_choice/README.md`): ordered
`steps.yaml` with keys `tick`, `block` (+ optional `valid`), `attestation`,
`attester_slashing`, and `checks` (head / justified / finalized /
`proposer_boost_root` / time). Sample step keys observed across handlers:
`tick`, `block`, `attestation`, `attester_slashing`, `checks`. No Fulu-only
step kind beyond what the mainnet/minimal `fork_choice` runner already must
dispatch.

### Verdict

**In scope for CC-15c's runner — Clause 1's suite table gains a row.**

These are **additional** fork-choice cases the standard
`tests/<preset>/fulu/fork_choice/…` walk (from `mainnet.tar.gz` /
`minimal.tar.gz`) will **not** cover: different handler path
(`fork_choice_compliance` vs `fork_choice`), different generation method
(constraint-based compliance generator), and they live only in
`comptests.tar.gz`. They are **not** mere duplicates of the pyspec
`fork_choice` suite.

**Consequence for Clause 1 / CC-15c:**

| Suite | Artifact | Preset(s) | Owner | Notes |
|---|---|---|---|---|
| `fork_choice` | `mainnet.tar.gz`, `minimal.tar.gz` | both | CC-15c | existing Clause 1 row |
| **`fork_choice_compliance`** | **`comptests.tar.gz`** | **minimal only** | **CC-15c** | **new row — same step runner, different suite root** |

`fork_choice` may not be declared green (CC-15c) before this section is
honoured: the runner must either execute `fork_choice_compliance` under Fulu
minimal or explicitly skiplist it with a reviewed owner. Recommended path:
one runner implementation, two suite roots
(`…/fulu/fork_choice` and `…/fulu/fork_choice_compliance`).

## CC-1H mid gate

**Date:** 2026-08-07  
**Machine:** Apple M4 Pro, 24 GB RAM, macOS aarch64 (dev machine)  
**Method:** Re-run of the CC-13d early-gate measurement **with fork choice in
the path**: `process_slots` across **5** epoch boundaries from the committed
Hoodi anchor state (slot `3649472`), then a `BeaconState` clone +
`process_justification_and_finalization` (the `compute_pulled_up_tip` /
`integrate_block` pull-up body). Cache warmed with one `canonical_root()`
before measurement. Hash share = wall time spent inside
`measured_canonical_root` / epoch wall time. `fc_clone_ms` is the clone + J&F
portion alone.

**Command:**

```text
cargo test -p cc-chain --test offline_replay cc1h_mid_gate_with_fork_choice_clone -- --nocapture
```

| epoch | from → to | wall (ms) | hash (ms) | hash share | root calls | fc clone (ms) |
|---:|---|---:|---:|---:|---:|---:|
| 0 | 3649472 → 3649504 | 774.81 | 50.59 | 6.5 % | 32 | 109.55 |
| 1 | 3649504 → 3649536 | 572.11 | 139.89 | 24.5 % | 32 | 68.22 |
| 2 | 3649536 → 3649568 | 550.59 | 139.00 | 25.2 % | 32 | 51.52 |
| 3 | 3649568 → 3649600 | 576.94 | 137.32 | 23.8 % | 32 | 51.44 |
| 4 | 3649600 → 3649632 | 552.01 | 137.67 | 24.9 % | 32 | 52.08 |

| Aggregate | Value |
|---|---|
| max wall | **774.81 ms** |
| mean wall | 605.29 ms |
| mean hash share | **21.0 %** |
| max hash share | 25.2 % |
| mean fc clone | 66.56 ms |
| max fc clone | 109.55 ms |

### Threshold verdict (mid gate bar = 1000 ms)

**&lt; 1000 ms** (max epoch wall 774.81 ms). **CC-1H is not promoted** before
M1.4. The mid gate is **closed**.

### Early-gate flagged band (700–1500 ms)

The early gate closed under **700 ms** (max 689.91 ms) and was never in the
flagged band. Mid-gate max **774.81 ms** sits in the early gate's 700–1500 ms
numeric range only because the FC state clone is now included (~50–110 ms); it
remains **under the mid-gate 1000 ms bar**. The early-gate “flagged → re-measure
at mid” contingency is **resolved without promoting CC-1H**.

### Attribution

Mean hash share **21.0 %** (&lt; 25 %). Epoch cost remains transition-side /
clone-dominant rather than `canonical_root()`-bound. Milhouse is still not
indicated; if a later gate regresses, profile per-handler plus CC-1I batching
before a state-backing swap.

### Explicit decision

**CC-1H is not promoted.** Mid gate **closed**. Re-measure only at the late
gate (CC-1Ad soak) if R-3 early-warning signals change.

### Offline replay note (same issue) — AC adaptation

**Written AC** (“import the CC-10b **recorded** 40-slot sequence”) is **fixture-
topology impossible offline**: the pin is a 40-slot window *ending at* the
anchor; only the anchor `BeaconState` is cached; providers return 500/501 for
pre-sequence historical state. Forward `ImportBlock` of those past SSZs against
an anchor-seeded store is not the M1.4 path.

**Accepted amendment (CC-18d):**

| Path | What is exercised |
|---|---|
| Recorded `sequence/*.ssz` | Parent-link walk, SSZ decode, tree-hash vs pin, **file SHA-256** digests for anchor + non-empty sequence SSZ |
| gRPC `ImportBlock` | **40 post-anchor** full-ST blocks built from the digest-verified Hoodi anchor state |

`cargo test -p cc-chain --test offline_replay offline_replay_40_slots_via_grpc`
asserts `IMPORTED` / mid-replay `DUPLICATE`, parent-linked head advance, and
head-event ordering. Residual risk (first production historical/live block SSZ
with blobs / real ops / FFG movement) is owned by M1.4 / CC-19.

**Finalized-epoch AC:** empty synthetic blocks do not advance FFG; gauge is
asserted non-decreasing from the seeded finalized epoch (explicit waiver of
“advances ≥ once” for this offline path).

## Run record

**Owner:** CC-1Ad (numbers) — skeleton from CC-1Ac  
**Status:** **`NOT_RUN`** — the ≥ 24 h continuous Hoodi soak (Clause 2) and the
steady-state Clause 3 timing window have **not** been executed. Wall-clock for
a valid attempt is ~26 h (bootstrap + catch-up, then 24 h from steady-state
open). This commit records (1) the filled field schema, (2) a short rig
rehearsal, and (3) an operator checklist so Phase 1 exit is **not** falsely
claimed green. **Do not treat any `_NOT_RUN_` cell below as a pass.**

Five fields cannot be reconstructed after the fact — missing any one voids the
*report*, not the run. Fill only from a single continuous `chain` process.

### 24 h soak — field schema (real numbers go here)

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | Process start timestamp (UTC) | `_NOT_RUN_` | Wall-clock when the running `chain` binary started |
| 1b | Git SHA of the running binary | `_NOT_RUN_` | Pin from `cc_build_info` / `CC_GIT_SHA`; a mid-run binary swap voids the run. Placeholder until start: tip of soak branch |
| 2 | Catch-up complete timestamp (UTC) | `_NOT_RUN_` | From `cc_driver_catchup_complete_timestamp` (CC-1C/3). Opens the steady-state window. **Not** hand-entered when the report script runs |
| 2b | Steady-state window end (UTC) | `_NOT_RUN_` | ≥ 24 h after field 2 |
| 3a | Hour-2 RSS (KiB) | `_NOT_RUN_` | Clause 2/5 baseline — earliest health read (sampler series) |
| 3b | Hour-24 RSS (KiB) | `_NOT_RUN_` | Clause 2/5 end-of-window pair; pass ⇔ within 20 % of 3a |
| 3c | RSS ratio (3b / 3a) | `_NOT_RUN_` | Record margin, not just pass/fail |
| 4a | Driver (block-feed) provider | `_NOT_RUN_` | The provider `bin/driver` polls for blocks (full beacon-API, not checkpoint-only) |
| 4b | Independent reference provider | `_NOT_RUN_` | Head-agreement sampler target; **must differ** from 4a (Clause 2/2). Enforced by `scripts/soak-sampler.sh` |
| 5 | Machine spec | `_NOT_RUN_` | CPU / cores / RAM / OS; sleep + auto-updates disabled |
| 6 | Per-slot load/RSS series path | `_NOT_RUN_` | Output of `scripts/soak-sampler.sh` → input to R-1 load-spike guard |
| 7 | Head-agreement CSV path | `docs/soak/head-agreement.csv` | Header committed; data rows only after a real window |
| 8 | `cc_driver_gap_abandoned_total` (end) | `_NOT_RUN_` | Zero-tolerance early signal on the 2 h rehearsal; post-run parent-linkage is Clause 2/3 judgement |
| 9 | Post-run parent-linkage walk | `_NOT_RUN_` | Anchor → final head; no missing block (Clause 2/3) |
| 10 | Finality: `cc_chain_finalized_epoch` | `_NOT_RUN_` | Increases ≥ once per epoch; no stall > 4 epochs (Clause 2/4) |

### Events log (rotations / gaps)

| Timestamp (UTC) | Event | Detail |
|---|---|---|
| — | *(empty until soak)* | e.g. provider rotation, `429`, `cc_driver_gap_abandoned_total` increment |

### Rig rehearsal (CC-1Ad — minutes, not hours)

**Date (UTC):** 2026-08-06 ~20:57Z  
**Machine:** Apple M4 Pro, 14 cores, 24 GB RAM, macOS aarch64 (Darwin 25.6.0)  
**Git tip at rehearsal:** `7a092265ea17150915122221d460e0241c6ea093` (`chore(soak): …` CC-1Ac on `develop`)  
**Purpose:** prove sampler CSV path + report self-test + independent-provider
guard. **Not** a 2 h rehearsal of a live `chain` process and **not** Clause 2/3
evidence.

| Step | Command / action | Outcome |
|---|---|---|
| Report self-test | `bash scripts/soak-report.sh --self-test` | **PASSED** (clean emit PASS; R-1 spike refuse exit 3; missing/zero catch-up refuse; identical-provider sampler refuse) |
| Provider smoke | `GET …/eth/v1/beacon/headers/head` | Checkpoint-only hosts (ethpandaops checkpoint-sync, ethstaker, sigp, chainsafe checkpoint, stakely, attestant) → **404** (not full beacon-API). Working full APIs: `https://beacon.hoodi.ethpandaops.io`, `https://lodestar-hoodi.chainsafe.io` |
| Sampler 3 slots | `bash scripts/soak-sampler.sh --driver-provider https://beacon.hoodi.ethpandaops.io --ref-provider https://lodestar-hoodi.chainsafe.io --out /tmp/…/soak-samples.csv --slots 3 --seconds-per-slot 3` | **3 CSV rows written.** Reference head polled OK (slots 3653038–3653039). Local `GetHead` **failed every tick** (no `chain` on `127.0.0.1:9001`) — expected; `agree=0`, empty `local_root` / `rss_kib`. Load1 observed ~2.3. |
| Provider guard | same driver+ref URL | **Refused** (exit 1); no CSV created |
| Report dry-run | `soak-report.sh` on real sampler-shaped CSV + **synthetic** histogram scrapes | Path **emitted** a Timing fragment (synthetic budget deliberately non-passing). **Numbers discarded** — synthetic only; not written into § Timing below |

**Rehearsal residual:** no continuous `chain`, no catch-up gauge, no 2 h RSS
slope, no post-run parent walk. Next operator step is the **2 h live rehearsal**
(issue AC) then the ≥ 24 h window — see checklist.

### Operator checklist (before claiming Clause 2 / 3 green)

1. **Same-day providers (R-4):** `bash scripts/probe-providers.sh` and append
   to `docs/running.md`. Driver and reference must be **full beacon-API** bases
   that answer `/eth/v1/beacon/headers/head` (checkpoint-only hosts are not
   enough for the sampler). Reference ≠ driver.
2. **Flat machine:** sleep + auto-updates off; no `cargo build`, no container
   rebuild, no heavy process for the window (`docs/running.md` § Soak).
3. **Pin binary:** `export CC_GIT_SHA="$(git rev-parse --short HEAD)"`, build
   once, record field 1 / 1b at process start. Any redeploy voids the run.
4. **2 h rehearsal first:** sampler running; confirm
   `cc_driver_gap_abandoned_total == 0`, `cc_chain_import_queue_depth` flat,
   RSS slope not visibly linear. Dirty rehearsal → fix, do not start 24 h.
5. **Steady-state open:** after `cc_driver_catchup_complete_timestamp` > 0,
   scrape `curl -sS http://127.0.0.1:9101/metrics > metrics-start.txt`.
6. **Window:** ≥ 24 h continuous `chain` lifetime from catch-up complete; keep
   sampler CSV; log every rotation/gap with UTC timestamp in Events log.
7. **Window end:** scrape `metrics-end.txt` + driver metrics; run
   `bash scripts/soak-report.sh …` (must **emit**, not R-1-refuse). Copy
   numbers into § Timing (or `--write`). Fill fields 3a/3b/8/9/10 from series
   + post-run walk.
8. **Commit artifacts:** filled § Run record + § Timing, data rows in
   `docs/soak/head-agreement.csv` (or path named in field 6/7), no fabricated
   margins. If either Clause 3 budget misses → state **CC-1H promoted to P0**
   and re-soak (contingency, not redesign).

### Sampler / report commands (fill-in)

```bash
# Independent reference ≠ driver provider (guard refuses otherwise).
# Use full beacon-API bases (headers/head), not checkpoint-only hosts.
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv

# Capture histogram scrape pair after catch-up completes (steady-state open),
# then again at window end:
curl -sS http://127.0.0.1:9101/metrics > metrics-start.txt
# … ≥ 24 h steady-state …
curl -sS http://127.0.0.1:9101/metrics > metrics-end.txt
curl -sS http://127.0.0.1:9110/metrics > driver-metrics.txt

bash scripts/soak-report.sh \
  --samples         soak-samples.csv \
  --metrics-start   metrics-start.txt \
  --metrics-end     metrics-end.txt \
  --driver-metrics  driver-metrics.txt \
  --out             timing-fragment.md
# optional: --write  → replace this file's ## Timing section
```

## Timing

**Owner:** CC-1Ad (numbers) — skeleton from CC-1Ac  
**Status:** **`NOT_RUN`** — no steady-state histogram scrape pair from a live
`chain` process. Clause 3 (epoch p95 ≤ 1000 ms / `process_block` p95 ≤ 400 ms
as bucket fractions at `le=1.0` / `le=0.4`) is **unproven**. Cells below stay
empty until `scripts/soak-report.sh` emits against a real window.

**Generated by:** `scripts/soak-report.sh` (fill-in at hour 24)  
**Overall verdict:** **`NOT_RUN`** (not PASS, not FAIL)

| Field | Value |
|---|---|
| Block bucket fraction at `le=0.4` | `_NOT_RUN_` (Δbucket / Δcount) |
| Block budget | p95 ≤ 0.4 s ⇔ fraction ≥ 0.95 |
| Block margin (fraction − 0.95) | `_NOT_RUN_` |
| Block pass | `_NOT_RUN_` |
| Epoch bucket fraction at `le=1.0` | `_NOT_RUN_` (Δbucket / Δcount) |
| Epoch budget | p95 ≤ 1.0 s ⇔ fraction ≥ 0.95 |
| Epoch margin (fraction − 0.95) | `_NOT_RUN_` |
| Epoch pass | `_NOT_RUN_` |
| Measurement window (steady-state) | `_NOT_RUN_ → _NOT_RUN_` |
| Excluded catch-up window | `_NOT_RUN_ → catch-up complete_` (from `cc_driver_catchup_complete_timestamp`) |
| Head-agreement (steady) | `_NOT_RUN_` (≥ 99 % of slots; ≤ 1 slot lag) |
| Driver provider | `_NOT_RUN_` |
| Reference provider | `_NOT_RUN_` |
| RSS hour-2 / end (KiB) | `_NOT_RUN_ / _NOT_RUN_` |
| R-1 load guard | `_NOT_RUN_` (refuse if sustained spike in window) |
| Machine spec | `_NOT_RUN_` |
| Overall verdict | **`NOT_RUN`** |

### Method

Bucket fraction is a **counting question** (Architecture §11.2 / CC-1C/4): scrape
`cc_chain_process_block_seconds` / `cc_chain_process_epoch_seconds` at the start
and end of the steady-state window, subtract cumulative counts, and compute

```text
fraction(le=L) = (bucket[le=L]_end − bucket[le=L]_start) / (count_end − count_start)
```

Pass ⇔ fraction ≥ 0.95. No quantile interpolation. Catch-up is excluded via the
scrape pair after `cc_driver_catchup_complete_timestamp` and reported above
(CC-1C/3). The report states the **margin**, not just pass/fail — Fulu's ~4 s
attestation deadline covers consensus + execution + DA; Phase 1 pays only the
first.

### Contingency

Exceeding either budget triggers **CC-1H** (the recorded contingency), not a
redesign — and the filled report must say so explicitly. A late trigger costs a
re-soak (the reason CC-13d / CC-18d early and mid gates existed). While status
is `NOT_RUN`, CC-1H remains the P2 contingency from the closed mid gate; it is
**not** promoted on the basis of an unrun soak.

### Head-agreement artifact

Committed path: [`docs/soak/head-agreement.csv`](soak/head-agreement.csv) —
**header only** until a real soak (or full 2 h live rehearsal with local
`chain`) produces rows. Do not invent agreement percentages.
