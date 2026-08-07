# Phase 3 acceptance

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton created by
**CC-39b** (`## Entry`). Later issues append:

| Section | Owner |
|---|---|
| `## Entry` | **CC-39b** (this file) |
| Clause table + run-record skeleton | CC-3Ab |
| Clause 2 | CC-36b |
| Clauses 1, 3, 4 + run-record numbers | CC-3Ac |
| Gates | CC-3Kb |

**No invented numbers.** Unmeasured fields stay `NOT_RUN` with the reason and
any partial measurements that *were* taken.

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
