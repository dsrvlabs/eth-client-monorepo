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

## Observed auth failures

Evidence from CC-30b against a real `ethereum/client-go:v1.17.5` with the
compose `el` flag set (`--authrpc.addr=0.0.0.0`, `--authrpc.vhosts=el`,
`--authrpc.jwtsecret=/jwt/jwt.hex`). The table under **Container-auth trap** is
the theory; this section is what geth actually sent.

### HTTP 401 bodies (JWT path — token problem)

Triggered with a **correct** `Host` (or an IP Host) and a deliberate claim fault.
Bodies are plain text, not JSON-RPC error objects.

| Trigger | HTTP | Body (literal) | Our typed error | Metric |
|---|---|---|---|---|
| `iat = now − 120 s` | 401 | `stale token` | `EngineError::Http401` | `cc_engine_errors_total{code="http_401"}` |
| `iat = now + 120 s` | 401 | `future token` | `EngineError::Http401` | `cc_engine_errors_total{code="http_401"}` |
| No `Authorization` header | 401 | `missing token` | `EngineError::Http401` | `cc_engine_errors_total{code="http_401"}` |
| Token without `iat` claim | 401 | `missing issued-at` | `EngineError::Http401` | `cc_engine_errors_total{code="http_401"}` |
| Malformed / wrong-key JWT | 401 | JWT library error string | `EngineError::Http401` | `cc_engine_errors_total{code="http_401"}` |

geth enforces `jwtExpiryTimeout = 60 s` **both ways** — a client that only tests
one direction discovers the other during a clock-drift incident. Both bodies are
asserted literally in `services/engine/tests/auth_container.rs`
(`jwt_iat_stale`, `jwt_iat_future`).

### HTTP 403 body (vhost path — not a JWT problem)

Triggered with a **valid** token and a `Host:` hostname that is **not** in
`--authrpc.vhosts`. geth's `virtualHostHandler` serves any **IP** Host
unconditionally but validates hostnames.

| Trigger | HTTP | Body (literal) | Our typed error | Metric |
|---|---|---|---|---|
| Valid JWT, `Host: not-in-vhosts.invalid` | 403 | `invalid host specified` | `EngineError::Http403` | `cc_engine_errors_total{code="http_403"}` |

`http_401` does **not** increment on this path. The body is logged at `error!`
**verbatim** (not paraphrased) so this runbook stays greppable against the log
line. Test: `vhost_rejected_is_403_not_401`.

### Connection refused (addr path — not HTTP)

With `--authrpc.addr` left at geth's default `localhost`, a peer on the `cc`
network cannot connect: the engine call fails as
`EngineError::Transport` / `cc_engine_errors_total{code="transport"}` — **not**
`http_401`, **not** `http_403`. Restoring `--authrpc.addr=0.0.0.0` makes the
call succeed again. (CC-39 /2 negative half; recorded at bring-up, not as a
standing CI flip of the compose flag.)

### crc32 pair (one grep, both sides)

```bash
docker compose logs 2>&1 | grep -i crc32
# healthy — exactly two lines, identical crc32 values:
#   el     | … Loaded JWT secret file path=/jwt/jwt.hex crc32=0x…
#   engine | … Loaded JWT secret file path=/jwt/jwt.hex crc32=0x…
```

Engine prints the same message shape as geth (`Loaded JWT secret file` +
`path=` + `crc32=0x…`) so a single case-insensitive grep settles R-6.

**Negative form:** with `secrets/` swapped for an empty directory (and the
`:ro` mount still present), geth either generates its own secret (writable
path) or fails louder on write; our engine **aborts before bind** on the
missing/unreadable secret (CC-30a). The two crc32 values then **differ** (or
engine never starts) — that is the evidence the grep is a real check.
