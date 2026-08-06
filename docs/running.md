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

