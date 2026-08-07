# Phase 3 acceptance

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton created by
**CC-39b** (`## Entry`). **CC-3Ab** adds `## Run record` and `## Clause table`
skeletons (Amendment 5). Later issues append numbers only:

| Section | Owner |
|---|---|
| `## Entry` | **CC-39b** |
| `## Run record` skeleton | **CC-3Ab** (numbers: **CC-3Ac**) |
| `## Clause table` skeleton | **CC-3Ab** (script) / **CC-3Ac** (numbers for 1, 3, 4); clause 2 rows: **CC-36b** |
| `## Clauses 1, 3, 4` | **CC-3Ac** (append-only — Amendment 5) |
| `## Clause 2 — EL restart` | **CC-36b** (append-only — Amendment 5) |
| Gates and outstanding debt | **CC-3Kb** |

**No invented numbers.** Unmeasured fields stay `NOT_RUN` / `_NOT_RUN_` with the
reason and any partial measurements that *were* taken.

**D-12 / R-12 — two reports, two soaks.** Phase 3's ≥ 6 h window does **not**
discharge Phase 1's Clause 2, and Phase 1's Clause 2 would **not** discharge
Phase 3's clause 1. `docs/phase-1-soak.md` and `docs/phase-3-acceptance.md` stay
**two separate reports even if the runs share one process lifetime.** They
measure different things, on different stacks, for different durations. Phase 1's
Clause 2 remains outstanding until its own 24 h window is filled.

## Entry

**Owner:** CC-39b  
**Milestone:** M3.5 entry gate  
**Date of V-5 re-read:** 2026-08-07 (see V-5 table below — re-read immediately
before restore, not at planning time)

### V-5 — live snapshot pointer (re-read before restore)

| Field | Value |
|---|---|
| Read at (UTC) | `2026-08-07T19:21:36Z` |
| `curl -s https://snapshots.ethpandaops.io/hoodi/geth/latest` | **`3370000`** |
| Snapshot URL | `https://snapshots.ethpandaops.io/hoodi/geth/3370000/snapshot.tar.zst` |
| `content-length` (HEAD) | **`93911978209`** bytes (**87.46 GiB**) |
| `last-modified` (HEAD) | `Fri, 07 Aug 2026 09:22:02 GMT` |
| Age / catch-up note | ≈ **10.0 h** old at V-5 read — catch-up is ~10 h of Hoodi blocks on top of extract, not the planning-era 3 360 000 / 87.4 GiB figure |
| Planning figure (stale) | 2026-08-07 plan: block 3 360 000, 93 801 864 026 B; **not** the restore input |

Commands used:

```bash
date -u +"%Y-%m-%dT%H:%M:%SZ"
curl -sS https://snapshots.ethpandaops.io/hoodi/geth/latest
curl -sI https://snapshots.ethpandaops.io/hoodi/geth/3370000/snapshot.tar.zst
```

### V-2 — fork / BPO / Amsterdam (second fire)

Source: live `https://raw.githubusercontent.com/eth-clients/hoodi/main/metadata/config.yaml`
and `metadata/genesis.json` (EL schedule); local pin
`config/engine.toml` `[el_forks]` (retrieval date 2026-08-07).

| Check | Result | Source |
|---|---|---|
| Approx slot / epoch at check | slot **3 659 762** / epoch **114 367** (UTC 2026-08-07T19:22:27Z) | `MIN_GENESIS_TIME+GENESIS_DELAY` + 12 s slots |
| Regular fork in next 48 h | **no** — ELECTRA/FULU epochs long past | hoodi `config.yaml` `*_FORK_EPOCH` |
| BPO boundary in next 48 h | **no** — `BLOB_SCHEDULE` epochs 52 480 / 54 016 long past | hoodi `config.yaml` |
| EL Osaka / BPO times in next 48 h | **no** — `osakaTime` / `bpo1Time` / `bpo2Time` all past | hoodi `genesis.json` + `config/engine.toml` |
| `AmsterdamTime` on Hoodi | **still unset** (no `amsterdamTime` key in genesis; `amsterdam_time` commented unset in `config/engine.toml`) | genesis.json / engine.toml |
| Window moved? | **no** — no boundary inside the next 48 h | — |

### Machine cleanliness (shown)

| Check | Result |
|---|---|
| `docker ps --format '{{.Names}}'` | **Not exclusive** at script-land time: another worktree stack was present (`subagent-019fdcaf-…` six CC services + `el`). Full restore / Phase A–B live run requires an exclusive machine; see **Restore status** |
| `pmset -g` sleep | sleep **not** disabled for the exclusive-run checklist (`sleep 1`, disksleep 10, displaysleep 10) — must be disabled before a real multi-hour restore |
| Phase 2 M2.5 24 h soak outstanding? | **yes / serialised** — Phase 2 soak remains `NOT_RUN` residual in `docs/phase-2-soak.md`; this milestone and M2.5 are mutually exclusive on the machine (R-7). Order is the operator's call; this issue did **not** open a soak window |
| Dev-machine spec | Apple Silicon host; `hw.ncpu=14`, `hw.memsize=24 GiB`; Docker reports CPUs=4 / Mem≈3.8 GiB; root volume free at V-5 ≈ **262 GiB** (`df -h` / `df -k`) |

### Geth image digest (CC-39a)

| Field | Value |
|---|---|
| Image | `ethereum/client-go:v1.17.5` |
| Resolved digest (V-3) | `ethereum/client-go@sha256:523d3ba26623a619e912019068dc2784f02934070ac46bdae4d5b9df0d917814` |
| Recorded | 2026-08-07 in `docs/el-runbook.md` |

### Restore numbers (CC-39 /4, OQ-P3-5)

Stream-extract form (no second copy; never download-to-file then extract):

```bash
# scripts/el-snapshot-restore.sh — resolves latest, then:
wget --tries=0 --retry-connrefused -O - \
  https://snapshots.ethpandaops.io/hoodi/geth/<block>/snapshot.tar.zst \
  | tar -I zstd -xvf - -C <elstore>
du -sh <elstore>
```

| Field | Value |
|---|---|
| Snapshot block number | **3370000** (from V-5) |
| Download size (bytes) | **93911978209** (`content-length`) |
| `du -sh` of restored datadir | **`NOT_RUN`** — full stream-extract not completed in this session (see Restore status) |
| Restore wall clock | **`NOT_RUN`** |
| `eth_syncing == false` (UTC) | **`NOT_RUN`** |
| `eth_syncing` command + output | **`NOT_RUN`** |

#### Restore status (honest partial)

| Measurement | Value |
|---|---|
| Free disk before (`df -k` on restore parent) | **≈ 262 GiB** free on `/System/Volumes/Data` |
| Compressed `content-length` | **87.46 GiB** |
| Uncompressed size | **not guessed** (OQ-P3-5 / A-P3-4) — only `du -sh` after a real extract is authoritative |
| R-3 early-warning (60 s sample, 2026-08-07T19:32:54Z) | **1 156 041 785** bytes in **60** s → **19 267 363 B/s** (~18.4 MiB/s); projected download alone **≈ 1.35 h** (threshold 4 h — **not crossed**) |
| Pipeline smoke | first ~50 MiB of tarball listed via `curl \| tar -I zstd -tvf -` (paths under `./blobpool/…`); truncated-stream EOF expected |
| Why full restore is `NOT_RUN` here | Machine **not exclusive** (foreign `cc-*` stack still up); sleep not disabled; free disk **261 GiB** vs compressed **87.46 GiB** — uncompressed footprint **unknown** (must not be guessed) so a multi-hour stream-extract was not opened in this session. Scripts, V-5, V-2, dry-run, pipeline smoke, and R-3 probe are landed; operator re-runs `scripts/el-snapshot-restore.sh` on a clean exclusive machine and pastes the four numbers + timestamped gate into this table |

#### Integrity residual (publisher checksum)

ethPandaOps does **not** publish a per-snapshot digest next to
`snapshot.tar.zst` (probed: `.sha256` / `SHA256SUMS` / `manifest.json` → 404).
Restore integrity is therefore:

1. **TLS** to an **HTTPS host allowlist** (`snapshots.ethpandaops.io` by default;
   override only via `ALLOWED_SNAPSHOT_HOSTS`)
2. **Path-safe stream extract** (`assert_safe_member` / `assert_safe_tarball` —
   reject absolute paths and `..` components; post-extract realpath confinement)
3. Geth **image** digest is separate (CC-39a / `docs/el-runbook.md` § Image and digest)

No invented content hash is recorded in this table.

Re-run recipe (exclusive machine, sleep disabled, empty elstore):

```bash
# 0) clean machine
docker ps --format '{{.Names}}'    # must show only this stack (or empty before up)
# disable sleep (macOS): sudo pmset -a sleep 0 disksleep 0 displaysleep 0

# 1) V-5 + restore (stream-extract; path-safe members; HTTPS allowlist)
export PATH="/opt/homebrew/opt/gnu-tar/libexec/gnubin:$PATH"   # macOS GNU tar for -I zstd
export ELSTORE="${ELSTORE:-./.data/elstore}"
bash scripts/el-snapshot-restore.sh --elstore "$ELSTORE"
# paste: block, content-length, du -sh, wall clock

# 2) point compose elstore at the restored datadir (bind mount) or docker cp into volume,
#    then: docker compose up -d el
# 3) wait for gate (wait-only — does NOT re-extract; elstore may be non-empty)
bash scripts/el-snapshot-restore.sh --wait-synced --el-http http://127.0.0.1:8545
# paste: UTC timestamp + eth_syncing JSON
# one-shot extract+wait: bash scripts/el-snapshot-restore.sh --restore --wait-synced ...

# 4) two-phase acceptance (different asserts)
bash scripts/phase-3-acceptance.sh --phase a    # during catch-up only
bash scripts/phase-3-acceptance.sh --phase b    # after eth_syncing==false
# negative proof:
bash scripts/phase-3-acceptance.sh --negative b # during catch-up must fail
bash scripts/phase-3-acceptance.sh --negative a # after gate must fail
```

### Two-phase acceptance script (CC-39 /6)

Script: `scripts/phase-3-acceptance.sh`

| Phase | When | Asserts (different on purpose) |
|---|---|---|
| **A** | `eth_syncing != false` (catch-up) | head advances; `cc_chain_is_optimistic == 1`; `cc_engine_payload_status_total{method="newPayloadV4",status="SYNCING"}` **strictly increasing** |
| **B** | `eth_syncing == false` (post-gate) | `cc_chain_optimistic_transitions_total{direction="validated"}` increasing; `cc_chain_optimistic_nodes == 0` |

Phase A → Phase B boundary is emitted as machine-readable `window_start_utc` /
`window_start_unix` / `phase_boundary=A_to_B` (file default
`.data/phase3-window-start`) for CC-3Ab `soak-report.sh`.

| Check | Result |
|---|---|
| `bash scripts/phase-3-acceptance.sh --self-test` | **PASS** (2026-08-07 land; offline fixtures + negatives) |
| Phase A live during catch-up | **`NOT_RUN`** (needs restored + catching-up EL + engine/chain metrics) |
| Phase B live after gate | **`NOT_RUN`** |
| Negative: Phase B during catch-up fails | covered by `--self-test`; live tail **`NOT_RUN`** |
| Negative: Phase A after gate fails | covered by `--self-test`; live tail **`NOT_RUN`** |

### Fallback (A-P3-3)

If `snapshots.ethpandaops.io` is unavailable: cold snap-sync from empty datadir,
**wall clock unmeasured until someone measures it once**. Do not plan against a
guess (OQ-P3-4). Procedure: `docs/el-runbook.md` § Snapshot restore.

## Run record

**Owner:** CC-3Ab (skeleton) / **CC-3Ac** (numbers) — clause 2 live numbers: CC-36b  
**Date (CC-3Ac fill):** 2026-08-07  
**Tip commit at fill:** `293e9c2` (`origin/develop` tip this branch was cut from)  
**Status:** **`NOT_RUN`** — attempt **0** of 2. The 1 h live rehearsal and the
≥ 6 h continuous Hoodi window (clauses 1, 3, 4) were **not** opened. Named
blockers below. Instrument readiness (report self-test, dry-run clause table,
wire-capture recipe, hit-rate path) **is** recorded. **Do not treat any
`_NOT_RUN_` / `NOT_RUN` cell as a pass. No invented green numbers.**

**It measures the run; it is not the run** (D-6). Fill live cells only from a
single continuous process lifetime after CC-36b's drills and a fresh
`eth_syncing == false` timestamp.

Five items cannot be reconstructed after the fact — missing any one voids the
*report*, not the run: process start + git SHA; **Phase A → Phase B
`window_start` / `phase_b_boundary`**; geth image digest + snapshot block;
per-slot series path; machine exclusivity evidence (load + disk queue).

### Blockers (why the live window was not opened)

| # | Blocker | Evidence at CC-3Ac fill (UTC 2026-08-07 ~20:05) |
|---|---|---|
| B1 | **Machine not exclusive** | Foreign stack `subagent-019fdcaf-…` (six CC services + `el`) owns host ports; `docker ps` shows 7 containers not owned by this worktree |
| B2 | **Synced EL unavailable** | No this-worktree EL; foreign `el` answers `eth_syncing` with a non-false object (still syncing / cold); CC-39b restore `du -sh` / wall clock / gate timestamp still `NOT_RUN` in `## Entry` |
| B3 | **CC-36b live drills not discharged** | Clause 2 section is honest `NOT_RUN` for live; drills **must** complete and re-gate `eth_syncing == false` **before** this window (D-11) |
| B4 | **Sleep / auto-updates not disabled** | `pmset -g`: `sleep 1`, `disksleep 10`, `displaysleep 10` — exclusive-run checklist not met |
| B5 | **Phase 2 M2.5 soak residual** | Serialised with this machine (R-7); Phase 2 soak remains `NOT_RUN` in `docs/phase-2-soak.md` |

**Consequence:** clauses 1, 3, and 4 (and the bootstrap row) are **`NOT_RUN`**
naming **B1–B5**. Attempt count stays **0** — not a voided attempt, because no
window was opened. Two attempts remain budgeted once blockers clear.

### V-2 reading carried into the run record (from CC-39b)

Source commit of the reading: **CC-39b** land on develop (Entry section of this
file; V-5 / V-2 tables dated **2026-08-07T19:21–19:22Z**). This issue **carries**
that reading; it re-reads only if the window slips past the 48 h horizon that
reading covered.

| Check | Carried value | Inside a future ≥ 6 h window? |
|---|---|---|
| Regular fork in next 48 h (at read) | **no** | n/a — no window opened |
| BPO boundary in next 48 h (at read) | **no** | n/a |
| EL Osaka / BPO times in next 48 h | **no** | n/a |
| `AmsterdamTime` on Hoodi | **still unset** | n/a |
| Window moved? | **no** (at Entry read) | If a boundary falls inside a real window, **move the window and say so here** |
| Horizon still valid at CC-3Ac fill? | **yes** — fill is same calendar day as the Entry read (< 48 h) | Re-read live if the operator's window opens after `2026-08-09T19:22Z` |

A fork or BPO boundary **inside** the window **invalidates the run**.

### Window structure (1 h rehearsal → ≥ 6 h continuous)

Phase 3 deliberately uses a **shorter** continuous window than Phases 1 and 2
(24 h). Clauses 1 and 3 are about a **state machine reaching and holding a
state**, not about a peer set surviving churn. Six hours is ~1 800 slots and
~28 epochs — enough finalization turns for the `finalizedBlockHash` half of
clause 3 to mean something. Phase 3 **inherits** Phases 1 and 2 soak runs rather
than repeating them, and does **not** discharge them (D-12).

| Stage | Duration | Starts when | Job | Voids / stops |
|---|---|---|---|---|
| **0. Prerequisites** | wall-clock restore + catch-up | exclusive machine; `el-snapshot-restore.sh` | V-5 pin, stream-extract, `eth_syncing == false` | non-exclusive machine; inventing `du -sh` |
| **1. CC-36b drills** | minutes–hours | post-gate EL | both restart shapes; **fresh** `eth_syncing == false` after unclean | overlapping clause 1 window (D-11) |
| **2. 1 h rehearsal** | **≥ 1 continuous hour** | after stage 1 fresh gate | catch panic / missing metric / report cell the script cannot compute (R-11) | any `cc_engine_worker_panics_total` increment (P0); blank report cell |
| **3. ≥ 6 h window** | **≥ 6 continuous hours** and **≥ 20 finalized epochs** | Phase A → Phase B boundary (`window_start` / `phase_b_boundary`) | clauses 1, 3, 4 **share one window** — a `chain` panic at hour 5 costs all three | restart, rebuild, non-exclusivity, fork boundary inside window |
| **Bootstrap burst** | Phase A only | process start → `window_start` | **excluded** from clause 1 %; reported as its **own** row | folding Phase A into clause 1 (would measure the disk) |

**There is no consensus persistence until Phase 4.** A restart re-checkpoint-syncs
and empties the Phase 2 backfill cache. **A restart is a failed run, never a
recovered one.** Two attempts are budgeted; if two die for the **same** cause,
that cause is a bug and the next action is a **fix**, not a third attempt.

### 1 h rehearsal — structure and evidence

**Job (specific):** catch a panic, a missing metric, or a report row the script
cannot compute — the three things that turn hour 6 into hour 0 (R-11).

| Check | Pass bar |
|---|---|
| Duration | ≥ 1 continuous hour on the **exclusive** Hoodi stack post-gate |
| Report | `bash scripts/soak-report.sh --phase 3 …` → **a number or explicit `NOT_RUN` in every cell** |
| Panics | zero increments of `cc_engine_worker_panics_total{worker}` (family may be ABSENT — then record `ABSENT`, do not invent 0) |
| Metrics present | every family the clause table reads present on `/metrics` (chain 9101, engine **9104**, p2p 9102) |
| Tail | commit description / Events log carries the rehearsal end scrape summary |

#### Rig rehearsal (CC-3Ac — minutes, not hours)

**Date (UTC):** 2026-08-07  
**Machine:** Apple Silicon; `hw.ncpu=14`, `hw.memsize=24 GiB`; free disk ≈ **261 GiB**  
**Git tip:** `293e9c2`  
**Purpose:** prove report + two-phase acceptance + dry-run clause emission.  
**Not** a 1 h live Hoodi rehearsal and **not** clauses 1/3/4 evidence.

| Step | Command / action | Outcome |
|---|---|---|
| Report self-test | `bash scripts/soak-report.sh --self-test` | **PASSED** (phase1 + phase2 + phase3: table/venue/bootstrap/clause4 FAIL+NOT_RUN paths) |
| Phase A/B self-test | `bash scripts/phase-3-acceptance.sh --self-test` | **PASS** (offline fixtures + negatives) |
| Dry-run clause table (no samples) | `bash scripts/soak-report.sh --phase 3 --out /tmp/phase3-clause-dry.md` | **every cell a number or `NOT_RUN`** — E/bootstrap/1/2a/2b/3/4/5/CC-3C/CC-3B all emit; clause 3 notes wire capture absent; clause 4 names `CC-38b`, `CC-24c`, `CC-24d` when families absent |
| Live 1 h rehearsal | sampler + scrapes for 3600 s on exclusive post-gate stack | **`NOT_RUN`** (blockers B1–B5) |
| ≥ 6 h window | continuous process ≥ 6 h from `window_start` | **`NOT_RUN`** |
| `soak-report.sh --phase 3` on real window scrapes | paste into clause table | **`NOT_RUN`** |

**Rehearsal residual:** no exclusive this-worktree stack, no restored synced EL,
no Phase A→B `window_start`, no per-slot `is_optimistic` series, no ≥ 100-slot
wire capture, no getBlobs hit-rate over a window. Next operator step:
**exclusive machine → restore + gate → CC-36b both shapes → 1 h rehearsal →
≥ 6 h window**.

### §9.4 named fields

| Field | Value | Source / notes |
|---|---|---|
| Snapshot block number | **3370000** (from `## Entry` / V-5) | CC-39b |
| Download size | **93911978209** bytes (`content-length`) | CC-39b |
| `du -sh` of restored datadir | `_NOT_RUN_` (blocker B2) | CC-39b restore |
| Restore wall clock | `_NOT_RUN_` (blocker B2) | CC-39b |
| `eth_syncing == false` timestamp (UTC) | `_NOT_RUN_` (blockers B2, B3) | Opens Phase B; re-gated after CC-36b before clause 1 |
| Resolved geth image digest | `ethereum/client-go@sha256:523d3ba26623a619e912019068dc2784f02934070ac46bdae4d5b9df0d917814` | CC-39a / Entry |
| Dev-machine spec | Apple Silicon; `hw.ncpu=14`, `hw.memsize=24 GiB` (see Entry) | CC-39b |
| `CC-37` /9 hit rate (`cc_engine_getblobs_total{result}`) | `_NOT_RUN_` (no window; see hit-rate path below) | Same ≥ 6 h window as clauses 1/3/4; **low rate is legitimate PASS** |
| `CC-38` /8 end-to-end record | `_NOT_RUN_` | Block root + `t_complete` + `t_data_available` for ≥ 1 complete→`DataAvailable` before by-root |

### Window pin (fill from one continuous process)

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | Process start timestamp (UTC) | `_NOT_RUN_` (B1–B5) | Wall-clock when the running binary started |
| 1b | Git SHA of the running binary | `_NOT_RUN_` — pin at start; tip at doc fill: `293e9c2` | Mid-run swap voids the run |
| 2 | Phase A → Phase B `window_start` (UTC) | `_NOT_RUN_` (B2, B3) | From `scripts/phase-3-acceptance.sh` boundary file; **opens** the ≥ 6 h window |
| 2b | Window end (UTC) | `_NOT_RUN_` | ≥ 6 h after field 2 **and** ≥ 20 finalized epochs |
| 3 | Finalized epochs spanned | `_NOT_RUN_` | Need ≥ 20; script-computed, not eye-balled |
| 4a | Per-slot samples path | `_NOT_RUN_` | CSV columns include `is_optimistic`, `optimistic_nodes`, `el_head_lag_blocks`, `finalized_epoch`, `load1` (+ disk queue when available) |
| 4b | Boundary file path | `.data/phase3-window-start` | Default; override with `--boundary-file` |
| 5 | Machine exclusivity | **`NOT_RUN` / failed pre-check** — foreign stack present; sleep not off (B1, B4) | Per-slot load + disk queue required on a real window (R-7) |
| 6 | Wire capture (≥ 100 slots) | `_NOT_RUN_` — method ready, capture not taken (see wire-capture section) | CC-33 /3 + CC-31 /8 |
| 7 | `cc_engine_worker_panics_total` (window) | `_NOT_RUN_` (no window); family may be ABSENT on engine exposition | Any increment is P0, not a void |
| 8 | `cc_chain_valid_became_invalid_total` | `_NOT_RUN_` (no window); must be **zero** on a real run | EL consensus failure / stop-everything if non-zero |
| 9 | Attempt count / void reason | **attempt 0 / window not opened** (blockers B1–B5) | At most two attempts; same cause twice → fix, not third attempt |

### What voids a run

Written **in advance** so the decision is not made at hour 5 by a tired operator.

| Event | Effect |
|---|---|
| `chain` (or any of our six services) panic / OOM / exit / restart | **Void.** No persistence until Phase 4 — restart re-checkpoint-syncs; never a recovered run |
| Any code change, rebuild, redeploy, or git-SHA swap mid-window | **Void.** Pin field 1b once |
| Build, container build, test run, second `cc-*` stack, or Phase 2 soak on this machine | **Void.** Phase 3 contention is **disk and CPU** (EL continuous DB work; `cc_engine_request_seconds` tail is clause 3 / Trigger C) |
| Machine sleep, reboot, thermal throttle, OS / automatic update | **Void.** |
| EL container restart **inside** the clause 1 window | **Void for clauses 1/3/4.** (An EL restart **is** clause 2 and must run **before** the window — D-11) |
| Fork or BPO boundary inside the window; `AmsterdamTime` becoming set mid-window | **Void.** Move the window; record that it was moved |
| Missing any of the five non-reconstructible report inputs | **Voids the report** (not the wall-clock run) — re-run with capture |

**Not voids (must still be logged):** engine worker panic counter increment (P0
*with* the run); `valid_became_invalid` (stop-everything event — counter's only
job is to be zero); stream reconnects; a **low** getBlobs hit rate (legitimate).

### Wire-capture readiness (CC-31 /8 + CC-33 /3)

**Status:** method documented; **capture `NOT_RUN`** (no exclusive post-gate
stack). Discharge of the three wire assertions is part of clause 3 over the
**same** ≥ 6 h window (Amendment 2). Minimum length: **≥ 100 slots**
(~20 minutes at 12 s).

#### Assertions (all three required)

| # | Assertion | Source issue |
|---|---|---|
| W1 | **No `forkchoiceUpdated` out of fork-choice order** | CC-33 /3 |
| W2 | **`newPayload` before `forkchoiceUpdated` for every new head** | CC-33 /3 — geth's fcU path calls `BeaconSync(header, finalized)` with a header stashed from an earlier `newPayload`; fcU-only CLs force devp2p hunts |
| W3 | **`payloadAttributes` absent on every emission** | CC-31 /8 + CC-33 /4 — not defined-and-null; field must not appear on the wire at all (block production is Phase 7) |

#### Capture method (named — operator picks one; record which)

Port **8551** (Engine API) is **not** published to the host — only on the compose
`cc` network (`engine` → `http://el:8551`). Capture must see that path.

**Preferred — application-level ordered log (no JWT TLS gymnastics):**

1. Raise engine/chain log level for method emission so each
   `engine_newPayloadV4` / `engine_forkchoiceUpdatedV3` line carries:
   UTC timestamp, method, head/safe/finalized execution hashes (fcU), payload
   block hash (newPayload), and whether `payloadAttributes` is present.
2. Run for ≥ 100 slots after `window_start`.
3. Offline check:
   - sort by timestamp; for each new head hash, the first `newPayload` for that
     hash precedes the first `forkchoiceUpdated` that sets it as `headBlockHash`;
   - fcU `headBlockHash` sequence is monotonic w.r.t. fork-choice order (no
     interleaving of superseded heads — pair with `cc_engine_fcu_dropped_stale_total`);
   - zero log lines contain `payloadAttributes` / `payload_attributes`.
4. Paste into harness JSON for the report:

```json
{
  "clause3_wire": {
    "slots": 100,
    "method": "engine-json-rpc-log|tcpdump-docker-net|mitm-sidecar",
    "fcu_in_order": true,
    "newpayload_before_fcu": true,
    "payload_attributes_absent": true
  }
}
```

`scripts/soak-report.sh --phase 3` reads `harness-json.clause3_wire` and emits
the wire fragment into clause 3's measured cell.

**Alternative — docker-network packet capture:**

```bash
# Identify the compose network and el IP (8551 is internal-only).
docker network ls | grep _cc
EL_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(docker compose ps -q el)")
# From a debug container on the same network, or host tcpdump on the bridge:
# docker run --rm --net <project>_cc nicolaka/netshoot \
#   tcpdump -i any -s 0 -w /tmp/engine-api.pcap host el and port 8551
# Decode JWT-bearing HTTPS/HTTP JSON-RPC offline; assert W1–W3 on method order.
# Do **not** commit raw captures that embed the JWT secret.
```

**JWT warning:** authrpc is authenticated. Packet captures contain Bearer tokens
— treat as secret, scrub before any share, never commit.

Record in field 6: slot count, method name, and the three booleans. Without
the capture, clause 3 cannot fully discharge (script emits wire-absent /
`NO_DATA` even if lag and error counters look green).

### Hit-rate instrumentation path (clause 4 / CC-37 /9 / CC-38 /8)

**Status:** path ready in `scripts/soak-report.sh --phase 3`; live rate
`_NOT_RUN_` (no window).

| Step | What | Where |
|---|---|---|
| 1 | Scrape engine metrics at window open and close | `http://127.0.0.1:9104/metrics` → `engine-metrics-{start,end}.txt` |
| 2 | Read `cc_engine_getblobs_total{result="complete\|miss\|partial\|error"}` | counter **delta** over the window (prefer start/end pair) |
| 3 | Hit rate | `complete / (complete + miss + partial)` when the denominator > 0; **low rate is PASS** |
| 4 | Failure condition (checked, not assumed) | non-zero `complete` **and** zero `cc_p2p_columns_received_total{source="engine"}` → **FAIL** |
| 5 | Engine-sourced columns | scrape p2p `http://127.0.0.1:9102/metrics` — label `source="engine"` must exist for a discharging run |
| 6 | CC-38 /8 end-to-end | for ≥ 1 block: block root, `t_complete` (getBlobs complete), `t_data_available` (`DataAvailable(root)` removed it from `pending_da` **before** by-root ladder); paste into §9.4 |
| 7 | Zero complete responses in window | record hit rate **0** and name which of **CC-38 /1** or **/3** discharged the clause instead |
| 8 | V-6 said no at M3.4 entry / families absent | row **`NOT_RUN` naming `CC-38b`, `CC-24c`, `CC-24d`** (D-13) — dry-run confirmed this emission path |

Dry-run without engine/p2p scrapes (2026-08-07): clause 4 measured cell =

`NOT_RUN naming blockers CC-38b, CC-24c, CC-24d (no engine-sourced columns / getblobs family on stack — D-13; …)`.

### Events log (restarts / voids / rotations)

| Timestamp (UTC) | Event | Detail |
|---|---|---|
| 2026-08-07T20:05Z | CC-3Ac fill — window **not opened** | attempt 0; blockers B1–B5; instrument self-tests PASS; no void (no attempt) |
| — | *(live run rows append below)* | e.g. CC-36b drill complete, rehearsal end, void reason, provider blip |

### Operator checklist (before claiming clauses green)

1. **Entry recorded** — `## Entry` has V-5, V-2, digest, and (when exclusive) restore numbers; Phase A/B script self-test green.
2. **Exclusive machine** — `docker ps` shows only this stack; sleep + auto-updates off; no Phase 2 soak (B1, B4, B5).
3. **CC-36b drills first** (D-11) — both restart shapes at `local compose + EL`; re-gate `eth_syncing == false` with a **fresh** timestamp; that timestamp feeds field 2 / Phase B boundary.
4. **1 h rehearsal** — `bash scripts/soak-report.sh --phase 3` produces a **number or explicit `NOT_RUN` in every cell**; zero worker panics (or ABSENT recorded); every metric family present on `/metrics`.
5. **Pin binary once** — `export CC_GIT_SHA="$(git rev-parse --short HEAD)"`; fields 1 / 1b at process start. Any redeploy voids.
6. **Window** — ≥ 6 h from `window_start` / `phase_b_boundary` **and** ≥ 20 finalized epochs; sampler running (incl. load + disk queue); scrapes at start and end for chain / engine / p2p; wire capture ≥ 100 slots.
7. **Report** — `bash scripts/soak-report.sh --phase 3 …` emits the clause table; paste into `## Clause table` / `## Clauses 1, 3, 4` (or `--write`). Bootstrap burst must appear as its **own** row.
8. **Trigger C** — append production soft-deadline reading to `docs/engine-latency.md` `## Verdict` (**additions only**).
9. **Commit artifacts** — filled run record + clause numbers; **no fabricated margins**. Phase 1's report is **not** edited (D-12).

### Sampler / report commands (fill-in)

```bash
# After CC-36b drills and a fresh eth_syncing == false:
bash scripts/phase-3-acceptance.sh --phase b   # writes window_start / phase_b_boundary

# Per-slot series (Phase 3 columns for clauses 1/3):
# ts_unix,…,load1,is_optimistic,optimistic_nodes,el_head_lag_blocks,finalized_epoch
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv

# Scrape pair at window open and close (engine is 9104, not 9103):
curl -sS http://127.0.0.1:9101/metrics > chain-metrics-start.txt
curl -sS http://127.0.0.1:9104/metrics > engine-metrics-start.txt
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-start.txt
# … ≥ 6 h + wire capture ≥ 100 slots …
curl -sS http://127.0.0.1:9101/metrics > chain-metrics-end.txt
curl -sS http://127.0.0.1:9104/metrics > engine-metrics-end.txt
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-end.txt

bash scripts/soak-report.sh --phase 3 \
  --samples soak-samples.csv \
  --boundary-file .data/phase3-window-start \
  --chain-metrics-start chain-metrics-start.txt \
  --chain-metrics-end   chain-metrics-end.txt \
  --engine-metrics-start engine-metrics-start.txt \
  --engine-metrics-end   engine-metrics-end.txt \
  --p2p-metrics-start p2p-metrics-start.txt \
  --p2p-metrics-end   p2p-metrics-end.txt \
  --harness-json harness-results.json \
  --out clause-table.md
# optional: --write → replace this file's ## Clause table
# clause filters: --clause 1|3|4|bootstrap|E
```

## Clause table

**Owner:** CC-3Ab (skeleton / script) / **CC-3Ac** (numbers for E, bootstrap, 1,
3, 4); clause 2: **CC-36b**; clause 5 / gates: **CC-3Kb**  
**Status:** **`NOT_RUN`** for live discharge (CC-3Ac 2026-08-07). Rows for
clauses **1, 3, 4** and the **bootstrap** row carry script-shaped measured text
with **named blockers** — not blank, not green. A clause read by eye off a
Grafana panel does **not** discharge it. A clause run at the wrong venue does
**not** discharge either. **No invented numbers.**

| Clause | Venue | Measured | Threshold | Pass/Fail |
|---|---|---|---|---|
| E · entry condition (synced EL) | dev machine | `NOT_RUN` (no boundary file `window_start_unix` / `phase_b_boundary` and no harness-json.entry); blockers B1–B3 | `eth_syncing == false`; snapshot restore recorded (CC-39b) | **`NOT_RUN`** |
| bootstrap catch-up burst (excluded from clause 1) | Hoodi | `NOT_RUN` (no `window_start` / `phase_b_boundary` — cannot split bootstrap burst from steady-state); B2 | reported separately; not folded into clause 1 ≥ 99 % bar | **`NOT_RUN`** |
| 1 · head marked VALID by the EL | Hoodi | `NOT_RUN` (no samples/metrics to compute); is_optimistic series and gauge absent; optimistic_nodes series and gauge absent; payload_status VALID absent; payload_status INVALID absent; blockers **B1–B5** | `is_optimistic==0` ≥ 99 % of samples; `optimistic_nodes==0`; payload_status VALID↑ INVALID=0 (bootstrap excluded via `window_start` / `phase_b_boundary`) | **`NOT_RUN`** |
| 2a · EL restart clean (compose restart) | local compose + EL | `NOT_RUN` (harness-json.clause2.clean absent — live discharge is CC-36b) | outage: `el_offline==1` + optimistic; fcU within 1 slot of re-sync; one VALID clears set, no payload re-sub | **`NOT_RUN`** |
| 2b · EL restart unclean (kill -9) | local compose + EL | `NOT_RUN` (harness-json.clause2.unclean absent — live discharge is CC-36b) | same assertions as 2a (CC-36b) | **`NOT_RUN`** |
| 3 · EL stays synced via forkchoiceUpdated | Hoodi | `NOT_RUN` (no samples/metrics/wire); el_head_lag_blocks series absent; errors -38002 absent; errors -38006 absent; wire capture absent (CC-33/3, CC-31/8); blockers **B1–B5** | geth head within 1 block ≥ 99 %; errors `-38002`/`-38006` zero; wire: no fcU out of order; newPayload before fcU; payloadAttributes absent | **`NOT_RUN`** |
| 4 · getBlobsV2 fast path + DA edge | Hoodi | `NOT_RUN` naming blockers **B1–B5** (no ≥ 6 h window); instrument path also emits D-13 names `CC-38b`, `CC-24c`, `CC-24d` when engine-sourced columns / getblobs families are absent from scrapes | hit rate recorded (low is PASS); non-zero complete + zero engine-sourced columns = FAIL; else `NOT_RUN` naming `CC-38b`, `CC-24c`, `CC-24d` (D-13) | **`NOT_RUN`** |
| 5 · Phase 1 spec-vector suites stay green | dev machine | `NOT_RUN` (harness-json.clause5 absent — re-asserted by CC-3Kb / `cargo nextest -p cc-spec-tests`) | `cargo nextest -p cc-spec-tests` green both presets; skiplist empty | **`NOT_RUN`** |
| CC-3C · latency numbers + CC-1H verdict | dev machine | Trigger A/B decided at M3.4 (`docs/engine-latency.md`); **Trigger C `NOT_RUN`** — production soft-deadline needs this window (CC-3Ac) | **P1, no clause** | **`NOT_RUN`** (Trigger C) |
| CC-3B · optimistic / el_offline surface | dev machine | `NOT_RUN` (CC-3B surface — no Phase 3 caller) | **P1, no clause** | **`NOT_RUN`** |

### Method (CC-3Ab instrument / CC-3Ac numbers)

- **Venue column is machine-checked** — exact strings `Hoodi`, `local compose + EL`,
  `dev machine`. `bash scripts/soak-report.sh --phase 3 --venue 'dev machine'`
  refuses to emit clause 1 (whose venue is Hoodi).
- **Clause 1 window** starts at CC-39b's Phase A → Phase B boundary
  (`window_start_unix` / `phase_b_boundary` from `scripts/phase-3-acceptance.sh`).
  Bootstrap catch-up is excluded and reported as its own row.
- **Clause 4** computes both `cc_engine_getblobs_total{result="complete"}` and
  `cc_p2p_columns_received_total{source="engine"}`. Non-zero complete with zero
  engine-sourced columns is **FAIL**.
- **Every row is computed** by the script — no blank, no `<unset>`. Live ≥ 6 h
  numbers remain **CC-3Ac** residual until blockers B1–B5 clear.
- **Gates** (scoped `rustfmt --check`, env-read count == 6) are **CC-3Kb**, not
  rows in this table.
- **Phase 1's Clause 2 remains outstanding** (D-12) — see the note under the
  document title.

## Clauses 1, 3, 4

**Owner:** **CC-3Ac**  
**Venue:** **Hoodi** — compose stack (six CC services + geth `--hoodi`) on an
**exclusive machine**  
**Shared window:** clauses 1, 3, and 4 discharge over the **same** ≥ 6 h
continuous process lifetime. A failure at hour 5 costs all three.  
**Status (2026-08-07):** **`NOT_RUN`** for all three — blockers **B1–B5** in
`## Run record`. Detail below is the discharge contract + honest residual, not a
green claim.

### Clause 1 — head marked VALID by the EL

| Field | Value |
|---|---|
| Venue | Hoodi |
| Window | Phase A → Phase B `window_start` → end; bootstrap **excluded** |
| Bar | `cc_chain_is_optimistic == 0` for **≥ 99 %** of per-slot samples; `cc_chain_optimistic_nodes == 0` at sampled head for those slots; `cc_engine_payload_status_total{method="newPayloadV4",status="VALID"}` **strictly increasing**; `{status="INVALID"}` **zero** |
| Script | `bash scripts/soak-report.sh --phase 3 --clause 1 --venue Hoodi` → `PASS` with venue Hoodi |
| Bootstrap row | own row; not folded into the 99 %; not silently dropped |
| **Measured (this fill)** | **`NOT_RUN`** — no samples/metrics; no `window_start`; blockers B1–B5 |
| **Pass/Fail** | **`NOT_RUN`** |

### Clause 3 — EL stays synced via `forkchoiceUpdated`

| Field | Value |
|---|---|
| Venue | Hoodi (same window as clause 1) |
| Bar | geth head tracks fork-choice head **within 1 block for ≥ 99 %** of per-slot samples (`el_head_lag_blocks`); `cc_engine_errors_total{code="-38002"}` and `{code="-38006"}` **zero** |
| Wire (CC-33 /3, CC-31 /8) | ≥ 100-slot capture: **no fcU out of fork-choice order**; **`newPayload` before `forkchoiceUpdated` for every new head**; **`payloadAttributes` absent on every emission** — method in Run record § Wire-capture readiness |
| Script | `bash scripts/soak-report.sh --phase 3 --clause 3 --venue Hoodi` (+ `harness-json.clause3_wire`) |
| **Measured (this fill)** | **`NOT_RUN`** — no lag series, no error deltas, wire capture absent; blockers B1–B5 |
| **Pass/Fail** | **`NOT_RUN`** |

### Clause 4 — getBlobsV2 fast path + DA edge

| Field | Value |
|---|---|
| Venue | Hoodi (same window as clauses 1 and 3) |
| Hit rate | `cc_engine_getblobs_total{result}` over the window; **low rate is PASS** |
| Failure condition (must be checked) | non-zero `complete` **and** zero `cc_p2p_columns_received_total{source="engine"}` → **FAIL** |
| CC-38 /8 | block root + `t_complete` + `t_data_available` for ≥ 1 complete response that produced `DataAvailable(root)` before by-root |
| Zero completes in window | record **0** and name which of CC-38 /1 or /3 discharged instead |
| V-6 / missing families | **`NOT_RUN` naming `CC-38b`, `CC-24c`, `CC-24d`** (D-13) |
| Script | `bash scripts/soak-report.sh --phase 3 --clause 4 --venue Hoodi` |
| **Measured (this fill)** | **`NOT_RUN`** — no window (B1–B5); dry-run path also names D-13 blockers when families absent |
| **Pass/Fail** | **`NOT_RUN`** |

### Cross-checks recorded either way (when a window runs)

| Metric / item | Expectation |
|---|---|
| `cc_engine_worker_panics_total{worker}` | recorded; any increment is **P0**, not a void |
| `cc_chain_valid_became_invalid_total` | **zero** — non-zero is EL consensus failure / stop-everything |
| Per-slot load average + disk queue depth | exclusivity evidence (R-7) |
| Trigger C | soft-deadline ratio over this window → append-only `docs/engine-latency.md` `## Verdict` |

## Gates and outstanding debt

**Owner:** CC-3Kb  
**Date:** 2026-08-08  
**Milestone:** M3.5 exit — the admission audit (not the edge admission; D-1).

### Local gates recorded (CI has never executed — G3 / D-2)

| Gate | Command | Result |
|---|---|---|
| `cargo deny check` | `cargo deny check advisories bans licenses sources` | **ok** (advisories/bans/licenses/sources); duplicate-crate warnings only |
| `buf lint` | `cd proto && buf lint --path eth/{engine,p2p,chain}/v1` | **exit 0** all three packages (DEFAULT→STANDARD deprecation WARN only) |
| `buf format` | `cd proto && buf format --path … --diff --exit-code` | **exit 0** all three; no label |
| `buf breaking` | `cd proto && buf breaking --against '../.git#branch=origin/develop,subdir=proto' --path …` | **exit 0** all three; no label. **`eth.chain.v1` included** (CC-3B's contract — the one that gets forgotten) |
| `check-no-remodelling` | extended with `ExecutionPayload` / `ExecutionRequests` / `DataColumnSidecar` | **exit 0**; negative: `message ExecutionPayloadV3 { }` in `engine.proto` → non-zero naming the file |
| `check-crate-dag` | `bash scripts/check-crate-dag.sh` | **ok (16 members)**; `cc-engine` row = `cc-bootstrap cc-config cc-proto cc-types cc-crypto`; ADR P3-16 installed; `cc-fork-choice` row appends `cc-proto` (dev-dep from ea3c079, admitted here) |
| Engine service isolation (CC-3K /3) | `cargo metadata … \| jq` on `cc-engine` deps | **no other service crate**; ninth contract is `cc-proto` generated code |
| Member count (CC-3K /8) | `cargo metadata --format-version 1 --no-deps \| jq '.packages \| length'` | **16** (not 17 — CC-28 retired `bin/driver` 17→16 before Phase 3). Phase 3 **adds no workspace member** (ADR P3-01 / ≠13/1). Names: `cc-attestation cc-beacon-api cc-bootstrap cc-chain cc-config cc-crypto cc-devnet-gen cc-engine cc-fork-choice cc-libp2p cc-p2p cc-proto cc-spec-tests cc-state-transition cc-storage cc-types` |
| Clause 5 re-assert | `cargo nextest run -p cc-spec-tests` + `cargo nextest run -p cc-fork-choice --test fork_choice` | **33 + 5 passed**; `docs/spec-vectors-skiplist.md` has **no skip entries** (header-only skeleton; `committed_skiplist_parses_empty` green) |
| Closing green | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo build --workspace --locked` | **0 warnings / exit 0** (one-shot `gen_hostile_corpus` allow for expect/unwrap/panic — fixture tool, not service code) |

### Runnable restatements (criteria that cannot be written as "passes")

**phase-3-base** = `59a2389342a37d57a9e69781569d0bf5679cf8cc` (parent of CC-3Ka / `a144c10`).

#### Restatement 1 — formatting, scoped

```sh
git diff --name-only --diff-filter=ACMR 59a2389342a37d57a9e69781569d0bf5679cf8cc..HEAD -- '*.rs' \
  | xargs rustfmt --edition 2024 --check
```

- **Result at CC-3Kb:** **exit 1** — 64 of 84 Phase-3-touched `.rs` files fail `rustfmt --check`.
- **Not absorbed:** Phase 3 does **not** reformat the tree. `cargo fmt --all --check 2>&1 | grep -c '^Diff in'` = **880** (planning figure was ~340). Owner of the backlog: **outside this phase** (workspace-wide rustfmt debt; absorbing it would make every Phase 3 review unreadable — PRD Non-Goals / D-2).
- Obligation kept: **no reformat-only file** was introduced by this audit commit to green-wash the restatement.

#### Restatement 2 — env reads, as a count

```sh
scripts/check-no-env-reads.sh 2>&1 | grep -c 'std::env::var'
```

| Snapshot | Count (`grep -c 'std::env::var'` on script stderr+stdout) | Notes |
|---|---|---|
| Planning AC | **6** | Planning-time figure; already wrong at M3.1 entry |
| CC-3Ka / phase-3-base (`a144c10` / `59a2389`) | **10** hits in `services/` (+ script error header → **11** with `grep -c`) | Pre-existing chain/p2p test + main.rs debt |
| HEAD (post Phase 3) | **13** hits (**14** with error header) | **+3** in `services/engine/tests/encode_real_hoodi_payload.rs` (CC-3C latency fixture) |

- `git diff 59a2389..HEAD -- scripts/check-no-env-reads.sh` is **empty** — no silent exemption.
- Phase 3's **production JWT / config path** does not add `std::env::var` (config via `cc-config`); the three new hits are **test-only** fixture loaders in CC-3C.
- Fixing the ten baseline hits (or the three test hits) is a **named debt with an owner outside this phase**; this audit records the count rather than weakening the script.

### Outstanding debt (narrowest honest form)

| Debt | Status | Owner |
|---|---|---|
| **CI runner** (G3) | Every completed Actions run on `develop` failed *"job was not acquired by Runner of type hosted"*; branch protection bypassed. **CI has never executed here.** | Outside Phase 3 (PRD Non-Goals). Phase 3 substitutes **local invocations with tails** (D-2). |
| **`cargo fmt --all` backlog** | **880** `Diff in` lines; scoped Phase 3 restatement also red (64/84 files) | Outside this phase — do not absorb into a Phase 3 diff |
| **`check-no-env-reads` hits** | Count **13** (not 6); script unchanged | Outside this phase; no seventh *production* path; CC-3C added three test hits |
| **Phase 1 Clause 2 + Clause 3** | `docs/phase-1-soak.md` remains **`NOT_RUN`** in both `## Run record` and `## Timing` | **Phase 1's owners** — **not** Phase 3 |
| **Phase 3 ≥ 6 h window** | `## Run record` / `## Clause table` still **`NOT_RUN`** (CC-3Ac) | Phase 3 soak operators; separate from Phase 1 |

### D-12 — two reports, two soaks (R-12)

**Phase 1's Clause 2 (the ≥ 24 h Hoodi soak) and Clause 3 (the timing window) remain `NOT_RUN`.**
Phase 3's ≥ 6 h window **does not discharge them**, and they **would not discharge Phase 3's clause 1** —
different things, different stacks, different durations. The two reports
(`docs/phase-1-soak.md` and this file) stay **separate even if the runs shared one process lifetime**.
`git diff` against Phase 1's soak doc from this commit is **empty** — the debt is recorded here, not
by editing Phase 1's report.

## Clause 2 — EL restart

**Owner:** CC-36b  
**Venue:** `local compose + EL` *(on Hoodi data)* — the only clause whose venue
is neither Hoodi nor the dev machine (D-7 / D-11).  
**Status:** **`NOT_RUN`** for live discharge — full EL on Hoodi data with
`eth_syncing == false` was not available in this session. Drill **scripts**,
offline assertion proof, and soak-report wiring are landed; see **Offline /
mock verification** below. **No invented live numbers.**

### Why this venue

Hoodi will not restart an EL on cue. Phase 2's self-devnet has **no EL
containers** by design. The fault is induced at the container level:

| Shape | Exact command |
|---|---|
| **clean** | `docker compose restart el` |
| **unclean** | `docker kill -s KILL <el-container> && docker compose up -d el` |

(`scripts/el-restart-drills.sh` resolves the compose container id via
`docker compose ps -q el` so `docker kill -s KILL` targets the right box under
a project-prefixed name.)

Both shapes get **their own report row** — never averaged. A clean shutdown
leaves geth at the last `forkchoiceUpdated` head; `kill -9` may leave it
behind. Either answers `SYNCING` until re-acquire, but durations and client
behaviour differ.

**D-11 scheduling:** both drills run **before** clause 1's ≥ 6 h window opens
(an EL restart drives `cc_chain_is_optimistic` to 1 for its whole duration and
would void the ≥ 99 % bar). After both drills, `eth_syncing == false` is
re-gated and re-timestamped; that fresh timestamp is what CC-3Ac's window
starts from.

### Three assertions per shape

1. **During the outage:** `cc_engine_el_offline == 1`; blocks continue importing
   with `cc_chain_is_optimistic == 1` and `cc_chain_optimistic_nodes > 0`.
2. **After the EL answers:** a `forkchoiceUpdated` is issued **within one slot**
   (Hoodi **12 s**) of `eth_syncing == false` — both timestamps recorded.
3. **One `VALID` clears the whole optimistic set in one ancestor pass with zero
   payload re-submission:**
   - `cc_chain_optimistic_transitions_total{direction="validated"}` jump
     **exactly equals** the recorded outage block count
   - `cc_chain_optimistic_nodes` returns to **0**
   - `cc_engine_request_seconds_count{method="newPayloadV4"}` delta equals only
     new blocks arrived in that interval (asserted on the request count, not a
     log line)

Also recorded: `cc_engine_state` never enters `auth_failed` (JWT unchanged);
`cc_engine_capability_missing` re-populates to pre-outage; any increment of
`cc_engine_worker_panics_total` is a **P0** against §2.5 (reported, never
hidden). The family is currently **ABSENT** on engine exposition — drills
record `worker_panics_metric=ABSENT` rather than inventing zeros.

### Report rows (live — NOT_RUN)

| Field | 2a · clean (`compose restart`) | 2b · unclean (`kill -s KILL`) |
|---|---|---|
| Command | `docker compose restart el` | `docker kill -s KILL el && docker compose up -d el` |
| Outage duration (s) | `_NOT_RUN_` | `_NOT_RUN_` |
| Outage block count | `_NOT_RUN_` | `_NOT_RUN_` |
| `cc_engine_el_offline == 1` throughout | `_NOT_RUN_` | `_NOT_RUN_` |
| `cc_chain_is_optimistic == 1` throughout | `_NOT_RUN_` | `_NOT_RUN_` |
| `cc_chain_optimistic_nodes > 0` throughout | `_NOT_RUN_` | `_NOT_RUN_` |
| `eth_syncing == false` timestamp (UTC) | `_NOT_RUN_` | `_NOT_RUN_` |
| `forkchoiceUpdated` emission timestamp (UTC) | `_NOT_RUN_` | `_NOT_RUN_` |
| fcU elapsed (s) / within one slot (12 s) | `_NOT_RUN_` | `_NOT_RUN_` |
| `transitions_validated` before → after / Δ | `_NOT_RUN_` | `_NOT_RUN_` |
| Δ == outage_block_count (exact) | `_NOT_RUN_` | `_NOT_RUN_` |
| `cc_chain_optimistic_nodes` after | `_NOT_RUN_` | `_NOT_RUN_` |
| `newPayloadV4` request count before → after | `_NOT_RUN_` | `_NOT_RUN_` |
| ΔnewPayloadV4 == new_blocks_in_interval | `_NOT_RUN_` | `_NOT_RUN_` |
| States seen (`cc_engine_state`) | `_NOT_RUN_` | `_NOT_RUN_` |
| `auth_failed` seen? | `_NOT_RUN_` | `_NOT_RUN_` |
| `capability_missing` re-populated? | `_NOT_RUN_` | `_NOT_RUN_` |
| `cc_engine_worker_panics_total` Δ | `_NOT_RUN_` (metric ABSENT today) | `_NOT_RUN_` |
| Pass/Fail | **`NOT_RUN`** | **`NOT_RUN`** |

#### D-11 after both drills

| Field | Value |
|---|---|
| Both drills completed before clause 1 window? | `_NOT_RUN_` (live) — scheduling rule stated; scripts emit `d11.drills_before_clause1_window` |
| Fresh `eth_syncing == false` after unclean re-acquire (UTC) | `_NOT_RUN_` — this is the timestamp CC-3Ac's window starts from |

### Offline / mock verification (this commit)

| Check | Result |
|---|---|
| `bash -n scripts/el-restart-drills.sh` | **PASS** |
| `bash scripts/el-restart-drills.sh --self-test` | **PASS** — evaluate_shape PASS path; FAIL on one-pass mismatch, late fcU (>12 s), newPayload re-submission, and `auth_failed`; harness → soak-report emits **both** rows with **PASS** on fixtures |
| `bash scripts/soak-report.sh --self-test` | **PASS** (phase 1 + 2 + 3; phase 3 clause 2 venue present) |
| `bash scripts/soak-report.sh --phase 3 --clause 2 --venue 'local compose + EL' --harness-json .data/el-restart-drills.json` | **both rows emitted** with **`NOT_RUN`** (honest; live venue unavailable) |
| `bash scripts/el-restart-drills.sh --check-prereqs` | **REFUSED** — this worktree has no `el` service up; foreign `subagent-019fdcaf-…` stack owns ports; `eth_syncing != false` (cold/empty snap-sync, no Hoodi snapshot restore) |
| Live clean / unclean drill | **`NOT_RUN`** — would disrupt foreign stack and cannot discharge without synced Hoodi data |
| Machinery exercised offline | CC-36a state machine / upcheck / fcU re-send (unit tests on develop); CC-34b ancestor pass + zero re-submission property; CC-33 fcU driver — **not re-built here** (D-7: split by venue) |

### Operator recipe (exclusive machine, synced EL)

```bash
# 0) exclusive machine; sleep off; EL restored + eth_syncing==false (CC-39b)
docker ps --format '{{.Names}}'   # only this stack
bash scripts/el-snapshot-restore.sh --wait-synced --el-http http://127.0.0.1:8545

# 1) both shapes (clean then unclean); writes harness JSON
bash scripts/el-restart-drills.sh --shape both \
  --out .data/el-restart-drills.json

# 2) report both rows (venue machine-checked)
bash scripts/soak-report.sh --phase 3 --clause 2 \
  --venue 'local compose + EL' \
  --harness-json .data/el-restart-drills.json

# 3) re-gate eth_syncing==false — paste fresh timestamp into Run record field 2
#    and into this section's D-11 table; then open CC-3Ac's window
bash scripts/phase-3-acceptance.sh --phase b
```

**What voids a live run:** rebuild/redeploy between drills and the window;
restarting **our** services (no persistence until Phase 4 — a `chain` restart
re-checkpoint-syncs and is a failed run, never a recovered one); overlapping
clause 1's window (D-11). An `el` container restart **is** clause 2 and is
**void for clause 1**.
