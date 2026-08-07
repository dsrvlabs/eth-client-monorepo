# Self-devnet (CC-2Ja + CC-2Jd)

Phase 2 has no block production, so the **publisher** (CC-2Jd) replays a
**pre-generated** chain of signed blocks and data-column sidecars onto gossip
at slot cadence. This directory holds generator parameters, the compose
topology, runner scripts, and the static anchor.

## Topology

```text
devnet network (cc-devnet)
  ├─ publisher   cc-p2p --publish-fixture  (holds all 128 columns; cgc=128)
  ├─ node-a      peer under test — default peers with everything
  ├─ node-b      second peer so gossip has a real mesh
  └─ anchor      nginx serving genesis + CC-19 endpoints
```

| Service | Host metrics / HTTP | Notes |
|---------|---------------------|--------|
| **publisher** | `http://127.0.0.1:19102/metrics` | R-7 independent reference; scrape `cc_p2p_gossip_messages_total` |
| **node-a** | `http://127.0.0.1:19112/metrics` | Booking (a) assertions land here |
| **node-b** | `http://127.0.0.1:19122/metrics` | Mesh participant |
| **anchor** | `http://127.0.0.1:18080/` | Checkpoint bootstrap without a public provider |

## What is committed

| Path | Purpose |
|------|---------|
| `devnet.toml` | Generator parameters (seed, slot count ≥512, BPO epochs, blob cycle, slot time) |
| `expected-manifest.json` | Expected half of the reproducibility record |
| `compose.yml` | Self-devnet topology (CC-2Jd) |
| `up.sh` / `down.sh` / `smoke.sh` / `faults.sh` | Runner + docker fault primitives |
| `anchor/nginx.conf` | Static CC-19 path map |
| `out/.gitignore` | Keeps the output directory; ignores generated bulk |
| `README.md` | This file |

## What is generated (gitignored)

```
devnet/out/
  config.yaml          # FULU_FORK_EPOCH: 0, BLOB_SCHEDULE @ 5 & 10, scaled disparity
  genesis.ssz          # BeaconState (Fulu)
  keys/                # Deterministic BLS keys from the seed (CC-2Ja)
  chain/slot_NNNNNN/   # block.ssz + column_XXX.ssz + meta.json
  manifest.json        # Full reproducibility record (incl. generator git SHA)
  node_keys/*.key      # Deterministic secp256k1 per container (CC-2Jd)
  bootnodes.txt        # ENRs — consumed by CC-21c's devnet profile
  multiaddrs*.txt      # Static dial lists for the mesh
  peer_ids.txt         # role → PeerId (two up.sh runs must match)
  anchor/              # nginx document root (spec.json, genesis.json, genesis.ssz)
```

A 512-slot fixture with 128 columns per block is multi-gigabyte. Never commit
`devnet/out/chain/`.

### `manifest.json` git-SHA convention (CC-2Ja)

Every regeneration records `generator_git_sha` in `manifest.json`. Scenario
runners and soak notes must quote that field so "the fixture changed" is never
a silent explanation for a changed result.

## Quick start

Always use `./devnet/up.sh` (not raw `docker compose`). It **builds first**, then stamps
`CC_DEVNET_GENESIS_TIME` to `now + grace` so cold image builds cannot finish the
publisher's catch-up before the mesh is up.

```bash
# Short fixture for a local smoke (optional):
export CC_DEVNET_SLOT_COUNT=16
export CC_DEVNET_MAX_SLOTS=12
export CC_DEVNET_SMOKE_SLOT_N=4

./devnet/up.sh          # gen keys + bootnodes + build + genesis-after-build + up
./devnet/smoke.sh       # M2.1 wire gate
./devnet/faults.sh exercise-once   # offline-gap recovery without restart
./devnet/down.sh
```

Host-published ports bind **127.0.0.1 only** (metrics + anchor). Libp2p is not
published to the host — mesh stays on the `cc-devnet` docker network.

Regenerate the full 512-slot fixture only:

```bash
cargo run -p cc-devnet-gen --release -- --config devnet/devnet.toml
```

## Bootnodes (deterministic)

`up.sh` derives each container's secp256k1 secret as
`SHA-256("cc-devnet-v1:" || role)` and writes:

- `devnet/out/node_keys/{publisher,node-a,node-b}.key`
- `devnet/out/bootnodes.txt` (ENR base64, one per role)
- `devnet/out/peer_ids.txt`

**Two `up.sh` runs produce the same peer ids.** CC-21c's self-devnet profile
reads `bootnodes.txt` unchanged.

### Scenario-configurable peer set (node-a)

| Mode | How |
|------|-----|
| Default (mesh) | `multiaddrs.node-a.txt` lists publisher + node-b |
| Publisher-only | `CC_NODE_A_PEERS=publisher ./devnet/up.sh` (copies the publisher-only file) |

Assert on `cc_p2p_peers{direction=…}` after connect.

## Publisher (`--publish-fixture`)

- Forces conceptual **`cgc = 128`**: subscribes to **all 128** column subnets
  plus `beacon_block`.
- Replays `devnet/out/chain` at slot wall-clock cadence
  (`SECONDS_PER_SLOT`, default 3 s on this devnet).
- Loads every sidecar into an in-memory store used for **ByRoot / ByRange**
  answers once the CC-23a codec lands; store-layer by-root/by-range is unit-tested
  in `fault_mode` today.
- Fault kinds:
  - default / `none` → **plain publisher** (no-op relay)
  - `withhold-column` → parses, returns **not implemented** (CC-2Jb)
  - `misbehave` → parses, returns **not implemented** (CC-2Jc)

```bash
# Emit keys/bootnodes only:
cargo run -p cc-p2p -- --emit-bootnodes devnet/out
```

## Fault primitives (`faults.sh`)

| Primitive | Mechanism |
|-----------|-----------|
| `offline-gap <container> <minutes>` | `docker network disconnect` → sleep → `connect` |
| `pause <container> <seconds>` | `docker pause` / `unpause` |
| `restart <container>` | `docker restart` |
| `exercise-once` | Runs each once against `node-a` (acceptance) |

Example (clause 4 rehearsal uses 10 minutes; CI uses sub-minute):

```bash
./devnet/faults.sh offline-gap node-a 10
./devnet/faults.sh pause node-a 30
./devnet/faults.sh restart node-a
```

## Anchor endpoints (CC-19)

| Path | Body |
|------|------|
| `/genesis.ssz` | raw genesis state SSZ |
| `/eth/v1/config/spec` | JSON `{ "data": { … } }` from generated config |
| `/eth/v1/beacon/genesis` | JSON genesis time + GVR |
| `/eth/v2/debug/beacon/states/genesis` | genesis SSZ + `Eth-Consensus-Version: fulu` |
| `/eth/v2/debug/beacon/states/finalized` | same (devnet genesis == finalized) |

Node-a bootstrapping from the anchor is asserted once the chain side is wired
at M2.2; this issue proves the endpoints themselves.

## Parameters of note (generator)

- **`SECONDS_PER_SLOT=3`** — BPO epochs 5 and 10 arrive in ~16 minutes wall
  time instead of ~64. `MAXIMUM_GOSSIP_CLOCK_DISPARITY` scales with slot time.
- **`blobs_per_block_cycle`** — non-zero and varies so R-4 blob traffic and
  withheld-column scenarios always have columns to act on.
- **Proposers** come from the real EIP-7917 lookahead; headers are BLS-signed.

## Unit tests

```bash
cargo test -p cc-p2p fault_mode
cargo test -p cc-devnet-gen
```

## DAG

`cc-devnet-gen` may depend only on
`{cc-types, cc-crypto, cc-state-transition, cc-config}` (plus ordinary
workspace crates). It must never take `cc-proto` or `cc-libp2p`.

`cc-p2p` may take `cc-libp2p` (CC-2K). The publisher is a **mode of `cc-p2p`**,
not a separate binary.
