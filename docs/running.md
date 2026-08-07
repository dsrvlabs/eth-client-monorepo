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

