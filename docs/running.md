# Running

This document is **append-only after creation** in `##`-level named sections.
Later issues add new sections; they must not rewrite existing ones (Plan §5).

## Spec vectors

Phase 1 (and later) consensus-spec tests need the pinned `ethereum/consensus-specs`
release artifacts on disk. Phase 0 ships the fetch harness and the consumption
contract; nothing in the Rust build or `cargo nextest` downloads them.

### Fetch

```bash
bash scripts/fetch-spec-vectors.sh
```

Optional flags: `--force` (re-download every artifact), `--verify` (accepted for
the contract; the default path already re-hashes all four).

The script reads `spec-vectors.lock` (tag + four SHA-256 digests), downloads the
four release tarballs if missing, verifies digests, and unpacks into the cache.
A digest mismatch is a hard failure — re-download, do not “fix” the lockfile
unless you intentionally bump the pin.

### Cache location (`SPEC_VECTORS_CACHE`)

| | |
|---|---|
| Env var | `SPEC_VECTORS_CACHE` (optional) |
| Default root | `$HOME/.cache/eth-consensus-spec-vectors` |
| Effective root | `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}` |
| Tree root | `<cache root>/<tag>/tests` (`<tag>` from `spec-vectors.lock`) |
| Tarball dir | `<cache root>/<tag>/_dl/` (retained for re-verification) |

The cache lives **outside** the workspace (never under `target/`), so
`cargo clean` cannot wipe ~8–10 GB of vectors.

### Disk budget

Expect on the order of **8–10 GB** free for a full ready cache (≈1.74 GB of
tarballs under `_dl/` plus the unpacked tree). CI warms only `_dl/` (see the
`vectors` job in `.github/workflows/ci.yml`); unpacking in-job is cheap.

### Never implicit

No `build.rs`, crate, or workspace script invokes `fetch-spec-vectors.sh`.
Code that needs vectors and does not find a ready cache must fail with the
literal string:

```text
run scripts/fetch-spec-vectors.sh
```

Operators run the harness explicitly (locally or via the non-required `vectors`
CI job). Layout of the on-disk tree is recorded in `spec-vectors-layout.md`
(regenerate with `scripts/record-vector-layout.sh` after a pin bump).

## Compose stack

The six Phase 0 services (`chain`, `p2p`, `attestation`, `engine`, `beacon-api`,
`storage`) run under `docker-compose.yml`. One multi-stage `Dockerfile` builds
every binary; `ARG SERVICE` selects which image layer each compose service
ships. Configuration reaches containers as `CC_<SERVICE>_<FIELD>` environment
variables (see `docker-compose.yml`); `config/*.toml` holds local-dev defaults
only.

### Prerequisites

- Docker with Compose v2 (`docker compose`)
- `jq` (used by `scripts/wait-healthy.sh`)
- Host-side `grpc-health-probe` (or `grpc_health_probe`) on `PATH` for
  `scripts/prove-mutual-health.sh` — same tool the images embed at
  `/usr/local/bin/grpc-health-probe` (pin: Dockerfile `HEALTH_PROBE_VERSION`)
- Optional: free disk for the spec-vector cache (order **8–10 GB**; see
  [Disk budget](#disk-budget) under Spec vectors)

### Bring-up

```bash
export CC_GIT_SHA="$(git rev-parse --short HEAD)"   # so cc_build_info is not unknown
docker compose build
docker compose up -d
bash scripts/wait-healthy.sh                        # all six healthy within 90 s
```

Published host ports follow Architecture §6.3 (gRPC `9001`–`9006`, metrics
gRPC+100 → `9101`–`9106`). Services address each other by compose DNS name
inside the `cc` network; host ports exist only for operator probes and the
proof scripts.

```bash
curl -s localhost:9101/metrics | head
grpc-health-probe -addr=127.0.0.1:9001              # aggregate "" health
docker compose down                                 # no containers left
```

## Mutual-health proof

Success-metric clause 2 is automated by `scripts/prove-mutual-health.sh`
(Architecture §6.5). Against an already-healthy stack:

```bash
bash scripts/prove-mutual-health.sh
```

The script:

1. Delegates the “all six healthy” precondition to `wait-healthy.sh`.
2. Runs `docker compose stop chain`.
3. Polls each of the five dependents from the **host** with
   `grpc-health-probe` (aggregate `""`) and asserts **NOT_SERVING** within
   **15 s**, cross-checking `cc_peer_health{peer="chain"} == 0` on each
   service’s `/metrics`.
4. Runs `docker compose start chain` and asserts all six **SERVING** and every
   `cc_peer_health` gauge back to **1** within **30 s**.
5. On any failure, exits non-zero and names the offending service and its
   observed state.

End-to-end from a clean checkout (the same sequence as the non-required
`compose` CI job):

```bash
export CC_GIT_SHA="$(git rev-parse --short HEAD)"
docker compose build
docker compose up -d
bash scripts/wait-healthy.sh
bash scripts/prove-mutual-health.sh
```

The `compose` job in `.github/workflows/ci.yml` is **not** a required status
check (deliberately outside the warm-CI ten-minute budget). It runs on push to
`main`/`develop` and on PRs that touch `Dockerfile`, `docker-compose.yml`,
`crates/bootstrap/**`, or `scripts/{wait-healthy,prove-mutual-health}.sh`.

## Persistence

Phase 0 services are **ephemeral**: there is no durable volume for chain state,
attestations, or storage. `services/storage` stores nothing yet. A
`docker compose down` (or a host reboot) loses process state; the stack is
**not restartable across restarts** in any meaningful consensus sense until
Phase 4 persistence work. Treat every `up` as a fresh hello-world topology.

### Chain restart and checkpoint sync (Phase 1)

`chain` has **no durable storage** in Phase 1. A process restart discards the
in-memory fork-choice store, residency pins, and event ring. On the next start,
when `checkpoint_providers` is configured, the node **re-checkpoint-syncs**
from a provider (bind first, then bootstrap; aggregate health stays
NOT_SERVING until bootstrap completes). There is no snapshot resume and no
walk-forward from a prior head. Operators and the 24 h soak treat a restart as
voiding the run — that is expected Phase 1 behaviour, not a defect.

## Configuration and environment reads

`crates/config` is the **only** crate permitted to read the process
environment (Architecture §5). Services load `CC_<SERVICE>_*` through
figment’s prefixed env layer on top of `config/<service>.toml`; they must not
call `std::env::var` (or equivalent) directly. Compose injects topology via
env; TOML remains the local-dev default path. Both routes go through
`cc-config` so the CC-09/3 grep stays meaningful.

## Checkpoint providers

Append-only probe log (R-4 / CC-19a). Generated by
`bash scripts/probe-providers.sh`. Each run adds a dated table;
do not rewrite prior rows.

Code must **not** assume by-root works everywhere — fallback to the
`finalized` alias is part of the design (Architecture §8.2).

Checkpoint bootstrap is the **only** entry path (no genesis replay). Configure
`chain.checkpoint_providers` (ordered list) and `chain.network_config` (path to
a consensus-specs YAML for the `/eth/v1/config/spec` cross-check). Optional
`chain.checkpoint_root` is the operator-supplied finalized root; when unset,
assertion 1 is skipped and the resolved root is logged at `warn` with the
provider name.

Per provider: connect timeout 5 s, total timeout 180 s for the state body,
**2 network retries after the first attempt** (3 total tries) with exponential
backoff, then advance. Body hard caps: state **400 MiB**, block **8 MiB**
(fail closed oversize). Provider bases must be **https://** (loopback `http://`
only for offline tests). A provider that fails a *verification* assertion is
not retried. Optional `checkpoint_root` is the weak-subjectivity pin
(SEC-19a-1): when unset, the first self-consistent provider is trusted — set
it for prod/mainnet soak. Metric: `cc_chain_bootstrap_attempts_total{provider,result}`.

```bash
bash scripts/probe-providers.sh
```

### Probe 2026-08-06 19:29:37Z

| Date (UTC) | Provider | Answers | Eth-Consensus-Version | By-root | Notes |
|---|---|---|---|---|---|
| 2026-08-06 | `checkpoint-sync.hoodi.ethpandaops.io` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.beaconstate.ethstaker.cc` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.checkpoint.sigp.io` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `beaconstate-hoodi.chainsafe.io` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.beaconstate.info` | no | — | no | genesis HTTP 000 |
| 2026-08-06 | `hoodi-checkpoint-sync.stakely.io` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi-checkpoint-sync.attestant.io` | yes | — | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |


### Probe 2026-08-06 19:31:16Z

| Date (UTC) | Provider | Answers | Eth-Consensus-Version | By-root | Notes |
|---|---|---|---|---|---|
| 2026-08-06 | `checkpoint-sync.hoodi.ethpandaops.io` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.beaconstate.ethstaker.cc` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.checkpoint.sigp.io` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `beaconstate-hoodi.chainsafe.io` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi.beaconstate.info` | no | — | no | genesis HTTP 000 |
| 2026-08-06 | `hoodi-checkpoint-sync.stakely.io` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |
| 2026-08-06 | `hoodi-checkpoint-sync.attestant.io` | yes | fulu | yes | by-root stream started (capped); state_root=0xbefe6e6db0911482… |

## Soak

Operating rules for the Phase 1 24 h Hoodi soak (Clause 2 + Clause 3). The
measurement **rig** is CC-1Ac (`scripts/soak-sampler.sh`, `scripts/soak-report.sh`,
`docs/phase-1-soak.md` skeletons); the rehearsal and the run itself are CC-1Ad.

### No-CPU rule (R-1)

**No compilation, no container builds, no test runs, and no other CPU-heavy
process on the soak machine for the duration of the window.**

A `cargo build` during the soak **invalidates Clause 3 without invalidating
Clause 2**: the process never restarts, logs stay clean, head agreement stays
green, and the timing histograms are quietly contaminated. Nothing surfaces it
unless the mechanical guard fires.

`scripts/soak-report.sh` reads the per-slot load-average series from the
sampler and **refuses to emit a report** if a sustained load spike appears
inside the steady-state window (threshold and window are config:
`SOAK_LOAD_THRESHOLD` / `--load-threshold`, default `4.0`;
`SOAK_LOAD_WINDOW_SAMPLES` / `--load-window-samples`, default `5`). A refused
report is recoverable; a quietly wrong one is not.

Also before starting: **disable sleep and automatic updates** (and prefer
disabling thermal-throttling surprises where the OS allows).

What may proceed in parallel:

- Zero-CPU operator work (reading specs, drafting issues, reviewing PRs,
  watching dashboards, filling the run-record skeleton).
- M1.5 P1 tail **on a second machine only**. On one machine it lands after the run.
- Nothing that requires the run to be paused. There is no pause.

### Independent reference provider (Clause 2/2)

The head-agreement sampler compares local `GetHead` against an **independent**
beacon-API provider — not the same base URL the driver uses for its block feed.
Both names go in the run record. `scripts/soak-sampler.sh` refuses to start if
the two providers normalize equal.

```bash
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv
```

### Restart policy

| Event | Verdict | Note |
|---|---|---|
| `chain` panics, OOMs, or exits | **Void.** Restart from zero after a fix. | Clause 2/1; Phases 0–2 are not restartable by design. |
| Any code change, rebuild, or redeploy | **Void.** | Run record pins a git SHA; a binary swap means the 24 h was not one binary's 24 h. |
| Machine sleep, reboot, thermal throttle, OS update | **Void.** | Disable sleep and automatic updates before starting. Throttling corrupts Clause 3 silently. |
| Any build or heavy process on the soak machine | **Clause 3 void, Clause 2 intact.** | Treat as a void — a report you cannot defend is worth nothing. R-1's load guard refuses the report. |
| **Driver** restart or crash | **Permitted; usually fatal in practice.** | Clause 2/1 binds `chain`, not the driver scaffold. Long outages typically fail Clause 2/2 or 2/3 — record either way. |
| Provider outage, `429`, or rotation | **Not a void.** | Designed response (CC-1A/4, CC-19/5). Record rotation with timestamp. |
| `cc_driver_gap_abandoned_total` increments | **Not a void; likely a failed proof.** | Driver keeps polling forward. Clause 2/3's post-run parent-linkage walk is the judgement. |

### Report

```bash
# After catch-up: scrape start; at window end: scrape end + driver metrics.
bash scripts/soak-report.sh \
  --samples        soak-samples.csv \
  --metrics-start  metrics-start.txt \
  --metrics-end    metrics-end.txt \
  --driver-metrics driver-metrics.txt \
  --out            timing-fragment.md
```

Catch-up is excluded via `cc_driver_catchup_complete_timestamp` (metric present
and non-zero required; absent/zero → refuse). Budget verdict is the **bucket
fraction at the exact 0.4 / 1.0 boundaries** (Architecture §11.2), not a
quantile interpolation. Numbers land in `docs/phase-1-soak.md` § Timing
(CC-1Ad fill-in).

Rig self-check (no live stack):

```bash
bash scripts/soak-report.sh --self-test
```

## Phase 2 stack (post driver retirement)

Phase 2 retires the Phase 1 HTTP block-feed scaffold (`bin/driver`, CC-28). The
compose stack is again **six services only** — `chain`, `p2p`, `attestation`,
`engine`, `beacon-api`, `storage` — with **no driver service in the file**.

What the stack is now:

| Input | Path | Lifetime |
|---|---|---|
| Checkpoint bootstrap | HTTP (`chain.checkpoint_providers`, CC-19) | **Once** at process start, before the import path exists |
| Blocks, columns, attestations, peer discovery | **P2P only** (`services/p2p` ↔ `chain` stream) | Continuous after bootstrap |
| HTTP block poller (`bin/driver`) | **Removed** | Gone; do not re-add to compose |

### Bring-up (Phase 2)

Same six-service sequence as [Compose stack](#compose-stack); nothing extra to
start for a block source. Configure `chain.checkpoint_providers` (and
`chain.network_config`) when the node must checkpoint-sync; empty providers keep
Phase 0 compose healthy with `NOT_BOOTSTRAPPED` fork-choice RPCs.

```bash
export CC_GIT_SHA="$(git rev-parse --short HEAD)"
docker compose build
docker compose up -d
bash scripts/wait-healthy.sh                        # all six healthy within 90 s
```

Head then follows over gossip alone (with PeerDAS DA gating once CC-24d is live).
Assert structural retirement locally:

```bash
test ! -d bin/driver
grep -c driver docker-compose.yml                   # must print 0
bash scripts/check-no-http-import-path.sh           # no HTTP on ImportBlock path; p2p tree clean
```

### Standing limitation (read before a soak)

**The node is not restartable until Phase 4; a restart re-checkpoint-syncs and empties the backfill cache.**

A restart voids a 24 h run **by design, not by accident**. There is no durable
chain state and no backfill-cache resume in Phase 2; process death returns the
node to a cold checkpoint bootstrap. The restart-policy table in the Phase 2
plan is the long form of the same sentence. Treat every `docker compose up` as a
fresh process lifetime for soak purposes.

### Phase 2 Hoodi soak (CC-29c — clauses 1, 2, 3)

Proof clauses 1–3 are **results**, not code. The instrument is CC-29b
(`scripts/soak-sampler.sh`, `scripts/soak-report.sh --phase2`,
`docs/phase-2-soak.md` template). The rehearsal, 24 h window, and filled
numbers are **CC-29c**. Until a real window is measured, every cell stays
`_NOT_RUN_` — do not invent margins.

**Before the window opens** (entry checks; helper prints V-2 / V-4 tables and
the full command recipe):

```bash
bash scripts/phase2-soak-entry-checks.sh           # live re-fetch
bash scripts/phase2-soak-entry-checks.sh --offline # fixture-only smoke
bash scripts/soak-report.sh --self-test            # rig self-check
```

| Step | What | Gate |
|---|---|---|
| V-2 | Re-read Hoodi `config.yaml`: any fork **or** BPO in next 48 h? `GLOAS_FORK_EPOCH` still absent? | Boundary in window → land CC-2A or move window |
| V-4 | Re-fetch `bootstrap_nodes.yaml`; date the list | Stale list ≡ wrong digest at diagnosis time |
| 60 min hold | `min_over_time` peers ≥ 25 and custody ≥ 8 on **today's** peer set | Fail → do not start soak (D-8, R-2) |
| Machine | No self-devnet, no second `cc-p2p`, no builds; sleep/updates off | D-6, R-8 |
| 2 h rehearsal | Peers hold ≥ 25; deferred non-pathological; `worker_panics` flat 0 | Dirty rehearsal → fix, do not open 24 h |
| R-9 | Corpus from rehearsal Hoodi capture; hostile-input green | Before the 24 h attempt |
| Steady-state | Window opens at **peer-set-stable** (sampler meta), not process start | Timestamp is unrecoverable later |

**Report (after ≥ 24 h from peer-set-stable):**

```bash
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-start.txt   # after stable
# … ≥ 24 h …
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-end.txt
bash scripts/soak-report.sh --phase2 \
  --samples            soak-samples.csv \
  --run-meta           soak-samples.meta \
  --p2p-metrics-start  p2p-metrics-start.txt \
  --p2p-metrics-end    p2p-metrics-end.txt \
  --out                clause-table.md
```

Numbers land in `docs/phase-2-soak.md` § Run record and § Clause table. Full
checklist, void/non-void table, and residual `NOT_RUN` fields: that file.
Phase 1 no-CPU / R-1 load-guard rules still apply on the soak machine.

## Phase 3 stack (EL joins compose)

Phase 3 adds the execution client as compose service `el`
(`ethereum/client-go:v1.17.5` by exact tag — see `docs/el-runbook.md`). The six
consensus services remain; `engine` gains `depends_on: el` with
`condition: service_started` (**not** `service_healthy` — ADR P3-14). EL health
is `eth_syncing == false`, which is minutes to hours; Phase A of acceptance
exercises the client **while** the EL catches up.

### JWT secret

```bash
mkdir -p secrets
openssl rand -hex 32 > secrets/jwt.hex
chmod 0600 secrets/jwt.hex
```

Path is `secrets/jwt.hex` (not `jwt/`). Mounted read-only into `el` at
`/jwt/jwt.hex`. Never commit the file — `.gitignore` rules `secrets/*` and
`**/jwt.hex` (with `secrets/.gitkeep` tracked as the placeholder).

### Bring-up (Phase 3)

```bash
export CC_GIT_SHA="$(git rev-parse --short HEAD)"
# JWT required before el will authenticate usefully (geth will silently mint its
# own secret if the mount is empty — see docs/el-runbook.md R-6).
test -f secrets/jwt.hex
docker compose build
docker compose up -d
bash scripts/wait-healthy.sh                        # six CC services healthy
# el may stay unhealthy for a long time while snap-syncing; that is expected.
curl -s -X POST localhost:8545 \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}'
```

Port **8551** (authenticated Engine API) is **not** published to the host — only
on the `cc` network. Host-visible: **8545** (`eth_syncing`), **6060** (geth
metrics / Prometheus text), **30303** (devp2p).

### Persistence asymmetry (read before restarting)

**From Phase 3 onward the EL's datadir survives a restart while ours does not.**

| Side | Volume | After `docker compose down` (default) | After `down -v` / volume rm |
|---|---|---|---|
| `el` | named volume `elstore` → `/data` | **Persists** | Lost — full snap/snapshot restore again |
| Our six services | none (ephemeral) | Process state gone; next `up` **re-checkpoint-syncs** | same |

There is **no durable consensus storage until Phase 4**. Operators who treat
`docker compose down` as "reset everything" will be surprised that geth still has
chain data (and that deleting `elstore` costs a large re-download). Snapshot
restore procedure is CC-39b; until then, protect `elstore` deliberately.
Full auth-trap and `-38002` lookup: `docs/el-runbook.md`.

### Phase 3 run record (CC-3Ac)

The M3.5 proof is **not** the compose bring-up above — it is a pinned continuous
window recorded in `docs/phase-3-acceptance.md` (`## Run record` + `## Clauses 1,
3, 4`). Shape:

| Stage | Bar |
|---|---|
| Entry (CC-39b) | V-5 snapshot pin, stream-extract, `eth_syncing == false` |
| Clause 2 drills (CC-36b) | both EL restart shapes **before** the window (D-11); fresh gate timestamp |
| **1 h rehearsal** | report emits a number or explicit `NOT_RUN` in every cell; zero worker panics; metrics families present |
| **≥ 6 h window** | ≥ 6 continuous hours **and** ≥ 20 finalized epochs from Phase A→B `window_start`; clauses **1, 3, 4 share one window** |
| Bootstrap burst | Phase A only — **excluded** from clause 1 %; own report row |
| Wire capture | ≥ 100 slots for CC-33 /3 + CC-31 /8 (`newPayload` before fcU; fcU in order; no `payloadAttributes`) |
| Hit rate | `cc_engine_getblobs_total{result}` over the **same** window; low rate is PASS; non-zero complete + zero `source="engine"` columns = FAIL |

**Machine must be exclusive** (no build, no second stack, no Phase 2 soak; sleep
off). **A restart is a failed run**, never a recovered one — no consensus
persistence until Phase 4. Two attempts budgeted; same cause twice → fix, not a
third attempt.

**Status residual (CC-3Ac fill):** live rehearsal and ≥ 6 h window are
**`NOT_RUN`** with named blockers (non-exclusive machine, no this-worktree
synced EL, CC-36b live drills outstanding). Instrument self-tests pass. Do
**not** claim M3.5 exit green from this residual — see
`docs/phase-3-acceptance.md`.

```bash
# After exclusive restore + CC-36b + fresh eth_syncing==false:
bash scripts/phase-3-acceptance.sh --phase b          # writes window_start
# 1 h rehearsal, then ≥ 6 h with sampler + scrapes (engine metrics on :9104)
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
```

## Phase 4 durable surface (CC-4N)

Phase 4 mounts consensus state on two **named volumes**. Phase 3's `elstore`
for the EL is independent; the two consensus volumes below are what make the
`kill -9` restart clauses of Phase 4 possible.

### Named volumes

| Volume | Service | Mount | Contents |
|---|---|---|---|
| `cc-store-data` | `storage` | `/app/data` | redb store (~59 GiB steady; 128 GiB provisioned — §9 / V-6) |
| `cc-p2p-identity` | `p2p` | `/app/data` | `node_key` (CC-20b) + `<node_key>.seq` (CC-4E); mode 0600 |

Compose env (via `cc-config`'s `CC_<SERVICE>_<FIELD>` layer — no direct
`std::env` in services):

- `CC_STORAGE_DATA_DIR=/app/data`
- `CC_P2P_NODE_KEY_PATH=/app/data/node_key`

```bash
docker compose config --volumes
# expect: cc-store-data, cc-p2p-identity (and elstore when Phase 3's el is present)
```

### The two volumes are one backup unit

**The two volumes are one backup unit.** Back up and restore `cc-store-data`
and `cc-p2p-identity` together. A store restored beside a *different* node key
is a store of wrong-index columns: custody groups are
`get_custody_groups(node_id, cgc)`, and a new secp256k1 key is a new discv5
`NodeId`. The store records the `NodeId` its columns were custodied for
(`AnchorInfo.node_id`, §2.5). An open with a mismatched key **refuses to start**
with a named error (`I-node-id` / ADR P4-13) rather than silently re-backfilling.

Negative exercise (after the store is open with an anchor):

```bash
# Replace the persisted key, then restart without destroying volumes.
docker compose exec p2p sh -c 'rm -f /app/data/node_key'   # or overwrite with a fresh key
docker compose kill -s SIGKILL storage p2p
docker compose up -d
# storage must refuse: error names both stored AnchorInfo.node_id and the derived one
```

### kill -9 clause — never `down -v`

**The `kill -9` clause runs `docker compose kill -s SIGKILL` then `up`, never `down`.**

```bash
docker compose kill -s SIGKILL storage    # or the full stack service list
docker compose up -d
# volumes survive; process state is recreated from the durable surface
```

`docker compose down` removes containers but **keeps** named volumes.
**`docker compose down -v` destroys the proof and is unrecoverable** — it
deletes `cc-store-data` and `cc-p2p-identity` (and `elstore` if present). Do
not use `-v` in restart trials, soak recoveries, or the kill-9 acceptance
clauses.

The same path is encoded in `devnet/faults.sh restart <service>` so the clause
cannot be run wrongly by hand:

```bash
bash devnet/faults.sh restart storage
# → docker compose kill -s SIGKILL storage && docker compose up -d storage
```

`CC-4D` later appends `clock-jump` and `restart --hold` on top of this
SIGKILL+up base. Removing the Phase 2 *"not restartable until Phase 4"* soak
caveat is **CC-45c**, once the durable set is proven end to end.

