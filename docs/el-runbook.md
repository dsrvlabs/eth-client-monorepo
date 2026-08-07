# EL runbook

Operator notes for the compose `el` service (`ethereum/client-go`, Hoodi).
This file is **append-only at `##` section level** after creation — later issues
add named sections; they must not rewrite existing ones.

## Container-auth trap

All three symptoms present as "the EL won't talk to us." Only one is a JWT
problem. A CL that reads **403 as auth failed** will chase the token for an hour.

| Symptom | Actual cause | Fix |
|---|---|---|
| **connection refused** | `--authrpc.addr` is still geth's default `localhost`, so the listener is not reachable from another container. | `--authrpc.addr=0.0.0.0` |
| **HTTP 403**, body `invalid host specified` | Our `Host:` header is the EL's compose DNS name (`el`) when the client calls `http://el:8551`. geth's `virtualHostHandler` serves any **IP** Host unconditionally but **validates hostnames** against `--authrpc.vhosts`, whose default is `["localhost"]`. | `--authrpc.vhosts=el` (or `*`) — not `engine`; that is the *caller* service name, not the Host header |
| **HTTP 401**, body `missing token` / `missing issued-at` / `stale token` / `future token` / a JWT parse error | A genuine JWT problem. geth validates `Authorization: Bearer <token>` (HS256), then claims with a ±60 s `iat` window. | Check the crc32 pair (`docker compose logs \| grep -i crc32`) before anything else — both sides should print the same crc32 of the same 32-byte secret |

**geth silent JWT generate (R-6).** If the secret path is **missing but writable**,
geth generates its own secret, writes it `0600`, and continues (`Generated JWT
secret path=…`). Logs look healthy; every authenticated call 401s forever against
our secret. Our `engine` service **must abort** before bind when the secret is
missing (CC-30a); the EL will not warn you. With our compose mount
(`./secrets:/jwt:ro`), an empty `secrets/` instead fatals on write
(`open /jwt/jwt.hex: read-only file system`) — still broken, just louder. Confirm
a loaded shared secret with:

```bash
docker compose logs el | grep -i crc32
# healthy: Loaded JWT secret file path=/jwt/jwt.hex crc32=0x…
# and, once engine loads the secret, the matching crc32 from engine logs
```

JWT generation (64 hex characters + newline, 32 bytes decoded):

```bash
mkdir -p secrets
openssl rand -hex 32 > secrets/jwt.hex
chmod 0600 secrets/jwt.hex
# path is secrets/jwt.hex (≠13/6 — not jwt/); mounted :ro at /jwt/jwt.hex
```

## Image and digest

| Field | Value |
|---|---|
| Image | `ethereum/client-go:v1.17.5` |
| Pin rule | **Exact tag only.** Never `:stable` or `:latest` — both resolve today and both move under a running stack. |
| Resolved digest (V-3) | `ethereum/client-go@sha256:523d3ba26623a619e912019068dc2784f02934070ac46bdae4d5b9df0d917814` |
| Recorded | 2026-08-07 |
| `geth` binary | `/usr/local/bin/geth` (ships in the image; Alpine 3.24 base) |
| IPC path | `/data/geth.ipc` under `--datadir=/data` (healthcheck attaches here) |

Re-check digest after a re-pull:

```bash
docker pull ethereum/client-go:v1.17.5
docker image inspect ethereum/client-go:v1.17.5 --format '{{index .RepoDigests 0}}'
```

If the digest changes for the same tag, treat it as an upstream re-push: re-verify
`geth attach` / IPC before trusting the healthcheck, and update this table.

Healthcheck shape (eth_syncing == false, not "port open"):

```text
geth attach --exec 'eth.syncing == false' /data/geth.ipc | grep -q true
```

`start_period: 120s`, `retries: 40` — a restoring or catching-up EL is
**unhealthy but running**, not restart-looped. Engine depends on `el` with
`condition: service_started` (ADR P3-14), never `service_healthy`.

## `-38002` and fork-choice ordering

**Error:** JSON-RPC `-38002` from the EL on `engine_forkchoiceUpdatedV3` when the
fork-choice state triple is inconsistent (e.g. `safeBlockHash` is not equal to
and not an ancestor of `headBlockHash`, or finalized is not on the head chain).

**The ordering MUST is ours.** The Engine API requires consensus-layer clients to
respect the order of the corresponding fork-choice update events. Concurrent or
stale `forkchoiceUpdated` calls produce **head flapping on the EL** that reads as
a fork-choice bug on our side (R-13). Enforcement lives in the fcU driver
(single-flight / monotonic sequence — CC-33), not in geth.

**Where the symptom appears:** in **geth's logs** (and as a JSON-RPC error body
from the EL), not as a local proto-array assertion. If you see `-38002` while
debugging head instability, check emission order and the safe/finalized/head
triple **before** re-litigating fork-choice scoring.

`-38006` (invalid forkchoice state / related EL rejections of the triple) is the
same class: typed and counted on our side, never retried blindly.
