# Phase 4 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton opened by
**CC-4B** (Amendment 5) with the `OQ-1` foreign-peer probe section only; later
issues append their own sections (full skeleton at M4.3 / `CC-4Cb`).

| Section | Owner |
|---|---|
| `## OQ-1 — foreign-peer probe` | **CC-4B** |

## OQ-1 — foreign-peer probe

**Owner:** CC-4B  
**Date:** 2026-08-08  
**Tool:** `bin/serve-probe` (`cc-serve-probe`) — own varint/snappy/result-byte
codec against `cc-libp2p` transport (Noise + yamux). Does not link
`services/p2p` or `cc-store`.

### Status

**`OQ-1: NOT_RUN`** — real Hoodi peer multiaddrs were not reachable from this
agent environment as a one-shot dial list.

**Blocker (exact):**

1. Public Hoodi beacon HTTP endpoints reachable from this host
   (`beaconstate-hoodi.chainsafe.io`, similar) return placeholder / empty peer
   tables without dialable multiaddrs (no `/ip4/…/tcp/…/p2p/…` rows).
2. `bin/serve-probe` intentionally has **no** discv5 / ENR resolution path
   (allowed workspace edges are only `{cc-libp2p, cc-types, cc-config}`); it
   dials a multiaddr it is given. Bootnode ENRs in `config/p2p.toml` therefore
   cannot be turned into TCP multiaddrs inside this binary.
3. No pre-collected set of live Hoodi peer multiaddrs was available in-repo for
   Lighthouse / Prysm / Nimbus / Teku / Grandine.

**Do not invent peer rows.** The table below is the schema for a later operator
run; it is empty until a real dial list is available.

### Peer table (schema; empty while NOT_RUN)

One row per peer. Fill when OQ-1 is re-run from a network that can dial Hoodi
participants (5–10 peers × Lighthouse / Prysm / Nimbus / Teku / Grandine).

| Date | Implementation (`agent_version`) | Multiaddr | Advertised `earliest_available_slot` | Head slot | Blocks just above eas | Columns just above eas | Positive pass | Negative pass | Notes |
|---|---|---|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — | — | — | **NOT_RUN** — see blocker above |

### Codec validation against five foreign implementations (ADR P4-12)

| Implementation | Handshake success count | Codec validated |
|---|---|---|
| Lighthouse | 0 / target 5–10 | **NOT_RUN** |
| Prysm | 0 / target 5–10 | **NOT_RUN** |
| Nimbus | 0 / target 5–10 | **NOT_RUN** |
| Teku | 0 / target 5–10 | **NOT_RUN** |
| Grandine | 0 / target 5–10 | **NOT_RUN** |

The probe binary and local dual-swarm tests exercise Status v2 + ByRange
framing end-to-end; foreign-implementation handshake counts remain debt until
OQ-1 is re-run.

### Local substitute (negative-side criterion proved)

While OQ-1 is NOT_RUN, CC-4B records a **local stub-peer** proof of the
negative-side criterion that must not be softened:

```text
cargo test -p cc-serve-probe --locked --test negative_stub \
  stub_empty_success_below_window_fails_naming_slot
```

| Check | Result |
|---|---|
| Stub peer returns **empty success** (zero chunks) below advertised eas | Probe exits non-zero |
| Failure reason **names the exact slot** | **PASS** (see test) |
| Cooperating stub (code 3 below eas; block above) | Both sides pass |
| Unreachable multiaddr | Non-zero exit; error names transport / dial |

### §5.6 outcome sentence

**Not selected — OQ-1 NOT_RUN.** Until foreign-peer rows exist, the M4.1 branch
decision among (1) ship as designed / (2) ship behind
`p2p.advertise_block_floor = false` / (3) ship as designed with the observed
distribution recorded **cannot** be made from live data. Default engineering
posture remains: store holds the full window under every outcome; the advertise
boolean stays open until OQ-1 is filled. **This line is residual debt for the
next operator with Hoodi dial access.**

### Command shape for the later fill-in

```text
cc-serve-probe \
  --peer '/ip4/<host>/tcp/<port>/p2p/<peer_id>' \
  --fork-digest 0x<4-byte-hex> \
  --slots 1000 \
  --below 100 \
  --columns 0,1,2,3 \
  --json /tmp/serve-probe-<peer>.json
```

`--fork-digest` is required and never derived. Record `agent_version` from the
probe's identify observation (JSON / stderr Status line), advertised
`earliest_available_slot`, and block/column results just above it.
