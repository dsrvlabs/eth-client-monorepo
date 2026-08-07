# Phase 3 acceptance

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton created by
**CC-39b** (`## Entry`). **CC-3Ab** adds `## Run record` and `## Clause table`
skeletons (Amendment 5). Later issues append numbers only:

| Section | Owner |
|---|---|
| `## Entry` | **CC-39b** |
| `## Run record` skeleton | **CC-3Ab** (numbers: **CC-3Ac**) |
| `## Clause table` skeleton | **CC-3Ab** (script) / **CC-3Ac** (numbers); clause 2 rows: **CC-36b** |
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

**Owner:** CC-3Ab (skeleton) / CC-3Ac (numbers) — clause 2 numbers: CC-36b  
**Status:** **`NOT_RUN`** — the ≥ 6 h continuous Hoodi window (clauses 1, 3, 4),
the restart drills (clause 2), and the 1 h rehearsal have **not** been executed.
This section records (1) the filled field schema from §9.4 plus Phase 1's run-
record template, (2) pointers into `## Entry` for restore/gate fields already
owned by CC-39b, and (3) an operator checklist so M3.5 exit is **not** falsely
claimed green. **Do not treat any `_NOT_RUN_` cell below as a pass. No invented
numbers.**

**It measures the run; it is not the run** (D-6). Fill only from a single
continuous process lifetime after CC-36b's drills and a fresh
`eth_syncing == false` timestamp.

Five items cannot be reconstructed after the fact — missing any one voids the
*report*, not the run: process start + git SHA; **Phase A → Phase B
`window_start` / `phase_b_boundary`**; geth image digest + snapshot block;
per-slot series path; machine exclusivity evidence (load + disk queue).

### §9.4 named fields (skeleton — empty until measured)

| Field | Value | Source / notes |
|---|---|---|
| Snapshot block number | **3370000** (from `## Entry` / V-5) | CC-39b |
| Download size | **93911978209** bytes (`content-length`) | CC-39b |
| `du -sh` of restored datadir | `_NOT_RUN_` | CC-39b restore; paste when exclusive machine finishes extract |
| Restore wall clock | `_NOT_RUN_` | CC-39b |
| `eth_syncing == false` timestamp (UTC) | `_NOT_RUN_` | Opens Phase B; re-gated after CC-36b drills before clause 1 |
| Resolved geth image digest | `ethereum/client-go@sha256:523d3ba26623a619e912019068dc2784f02934070ac46bdae4d5b9df0d917814` | CC-39a / Entry |
| Dev-machine spec | Apple Silicon; `hw.ncpu=14`, `hw.memsize=24 GiB` (see Entry) | CC-39b |
| `CC-37` /9 hit rate (`cc_engine_getblobs_total{result}`) | `_NOT_RUN_` | Same ≥ 6 h window as clauses 1/3/4; low rate is legitimate |
| `CC-38` /8 end-to-end record | `_NOT_RUN_` | Block root + both timestamps (`t_complete`, `t_data_available`) |

### Window pin (fill from one continuous process)

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | Process start timestamp (UTC) | `_NOT_RUN_` | Wall-clock when the running binary started |
| 1b | Git SHA of the running binary | `_NOT_RUN_` | Pin from build info / `CC_GIT_SHA`; mid-run swap voids the run |
| 2 | Phase A → Phase B `window_start` (UTC) | `_NOT_RUN_` | From `scripts/phase-3-acceptance.sh` boundary file (`window_start_unix` / `phase_b_boundary`); **opens** the ≥ 6 h window |
| 2b | Window end (UTC) | `_NOT_RUN_` | ≥ 6 h after field 2 and ≥ 20 finalized epochs |
| 3 | Finalized epochs spanned | `_NOT_RUN_` | Need ≥ 20 |
| 4a | Per-slot samples path | `_NOT_RUN_` | Sampler CSV with `is_optimistic`, `optimistic_nodes`, `el_head_lag_blocks` |
| 4b | Boundary file path | `.data/phase3-window-start` | Default; override with `--boundary-file` |
| 5 | Machine exclusivity | `_NOT_RUN_` | No build / second stack / Phase 2 soak; sleep off; per-slot load + disk queue |
| 6 | Wire capture (≥ 100 slots) | `_NOT_RUN_` | CC-33 /3 + CC-31 /8: fcU order, newPayload before fcU |
| 7 | `cc_engine_worker_panics_total` (window) | `_NOT_RUN_` | Any increment is P0 |
| 8 | `cc_chain_valid_became_invalid_total` | `_NOT_RUN_` | Must be zero |
| 9 | Attempt count / void reason | `_NOT_RUN_` | At most two attempts; same cause twice → fix, not third attempt |

### Events log (restarts / voids / rotations)

| Timestamp (UTC) | Event | Detail |
|---|---|---|
| — | *(empty until run)* | e.g. CC-36b drill complete, void reason, provider blip |

### Operator checklist (before claiming clauses green)

1. **Entry recorded** — `## Entry` has V-5, V-2, digest, and (when exclusive) restore numbers; Phase A/B script self-test green.
2. **CC-36b drills first** (D-11) — both restart shapes at `local compose + EL`; re-gate `eth_syncing == false` with a **fresh** timestamp; that timestamp is field 2.
3. **1 h rehearsal** — `bash scripts/soak-report.sh --phase 3` produces a **number or explicit `NOT_RUN` in every cell**; zero worker panics; every metric family present on `/metrics`.
4. **Exclusive machine** — no builds, no second stack, no Phase 2 soak; sleep + auto-updates off.
5. **Window** — ≥ 6 h from `window_start` / `phase_b_boundary`; sampler running; scrapes at start and end for chain / engine / p2p.
6. **Report** — `bash scripts/soak-report.sh --phase 3 …` emits the clause table; paste into `## Clause table` (or `--write`). Bootstrap burst must appear as its **own** row.
7. **Commit artifacts** — filled run record + clause table; no fabricated margins. Phase 1's report is **not** edited.

### Sampler / report commands (fill-in)

```bash
# After CC-36b drills and a fresh eth_syncing == false:
bash scripts/phase-3-acceptance.sh --phase b   # writes window_start / phase_b_boundary

# Per-slot series (extend sampler columns as available):
# ts_unix,…,is_optimistic,optimistic_nodes,el_head_lag_blocks,finalized_epoch
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv

# Scrape pair at window open and close:
curl -sS http://127.0.0.1:9101/metrics > chain-metrics-start.txt
curl -sS http://127.0.0.1:9103/metrics > engine-metrics-start.txt
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-start.txt
# … ≥ 6 h …
curl -sS http://127.0.0.1:9101/metrics > chain-metrics-end.txt
curl -sS http://127.0.0.1:9103/metrics > engine-metrics-end.txt
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
  --out clause-table.md
# optional: --write → replace this file's ## Clause table
```

## Clause table

**Owner:** CC-3Ab (skeleton / script) / CC-3Ac (numbers); clause 2: CC-36b  
**Status:** **`NOT_RUN`** — fill from `scripts/soak-report.sh --phase 3` against a
real window (or rehearsal). A clause read by eye off a Grafana panel does **not**
discharge it. A clause run at the wrong venue does **not** discharge either.
Empty placeholders only; **no invented numbers.**

| Clause | Venue | Measured | Threshold | Pass/Fail |
|---|---|---|---|---|
| E · entry condition (synced EL) | dev machine | `_NOT_RUN_` | `eth_syncing == false`; snapshot restore recorded (CC-39b) | `_NOT_RUN_` |
| bootstrap catch-up burst (excluded from clause 1) | Hoodi | `_NOT_RUN_` | reported separately; not folded into clause 1 ≥ 99 % bar | `_NOT_RUN_` |
| 1 · head marked VALID by the EL | Hoodi | `_NOT_RUN_` | `is_optimistic==0` ≥ 99 % of samples; `optimistic_nodes==0`; payload_status VALID↑ INVALID=0 (bootstrap excluded via `window_start` / `phase_b_boundary`) | `_NOT_RUN_` |
| 2a · EL restart clean (compose restart) | local compose + EL | `_NOT_RUN_` | outage: `el_offline==1` + optimistic; fcU within 1 slot of re-sync; one VALID clears set, no payload re-sub | `_NOT_RUN_` |
| 2b · EL restart unclean (kill -9) | local compose + EL | `_NOT_RUN_` | same assertions as 2a (CC-36b) | `_NOT_RUN_` |
| 3 · EL stays synced via forkchoiceUpdated | Hoodi | `_NOT_RUN_` | geth head within 1 block ≥ 99 %; errors `-38002`/`-38006` zero; wire order | `_NOT_RUN_` |
| 4 · getBlobsV2 fast path + DA edge | Hoodi | `_NOT_RUN_` | hit rate recorded (low is PASS); non-zero complete + zero engine-sourced columns = FAIL; else `NOT_RUN` naming `CC-38b`, `CC-24c`, `CC-24d` (D-13) | `_NOT_RUN_` |
| 5 · Phase 1 spec-vector suites stay green | dev machine | `_NOT_RUN_` | `cargo nextest -p cc-spec-tests` green both presets; skiplist empty | `_NOT_RUN_` |
| CC-3C · latency numbers + CC-1H verdict | dev machine | `_NOT_RUN_` | **P1, no clause** | `_NOT_RUN_` |
| CC-3B · optimistic / el_offline surface | dev machine | `_NOT_RUN_` | **P1, no clause** | `_NOT_RUN_` |

### Method (CC-3Ab)

- **Venue column is machine-checked** — exact strings `Hoodi`, `local compose + EL`,
  `dev machine`. `bash scripts/soak-report.sh --phase 3 --venue 'dev machine'`
  refuses to emit clause 1 (whose venue is Hoodi).
- **Clause 1 window** starts at CC-39b's Phase A → Phase B boundary
  (`window_start_unix` / `phase_b_boundary` from `scripts/phase-3-acceptance.sh`).
  Bootstrap catch-up is excluded and reported as its own row.
- **Clause 4** computes both `cc_engine_getblobs_total{result="complete"}` and
  `cc_p2p_columns_received_total{source="engine"}`. Non-zero complete with zero
  engine-sourced columns is **FAIL**.
- **Every row is computed** by the script — no blank, no `<unset>`. Numbers for
  the ≥ 6 h window are **CC-3Ac**; this skeleton is the instrument only.
- **Gates** (scoped `rustfmt --check`, env-read count == 6) are **CC-3Kb**, not
  rows in this table.
- **Phase 1's Clause 2 remains outstanding** (D-12) — see the note under the
  document title.
