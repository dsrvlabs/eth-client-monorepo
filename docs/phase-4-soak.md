# Phase 4 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton opened by
**CC-4B** (Amendment 5) with the `OQ-1` foreign-peer probe section only; later
issues append their own sections (full skeleton at M4.3 / `CC-4Cb`).

| Section | Owner |
|---|---|
| `## OQ-1 — foreign-peer probe` | **CC-4B** |
| `## Snapshot terms` | **CC-42** |
| `## V-10 / Clause 3 (M4.2 entry)` | **CC-42** |

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

## Snapshot terms

**Owner:** CC-42  
**Date:** 2026-08-08  
**Machine:** Apple M4 Pro, 24 GB RAM, macOS aarch64 (dev machine)  
**Metric families:** `cc_storage_snapshot_seconds{phase=replay|serialize|write|load}`,
`cc_storage_snapshot_bytes`, `cc_storage_snapshot_ring_depth`,
`cc_storage_replay_divergence_total`  
**Bucket boundary:** `cc_storage_snapshot_seconds` has an exact **5.0** boundary
(§10.2) so the cadence conditional is a count, not an interpolation.

### Measured BeaconState size (CC-42 /4)

| Field | Value |
|---|---|
| Source | Hoodi fixture cache `~/.cache/cc-hoodi-fixtures/3649472/beacon_state.ssz` (CC-10b pin) |
| Slot | **3649472** |
| **Byte count (uncompressed SSZ)** | **205 205 311** (~195.7 MiB) |
| **Validator-set size** | **1 455 439** |
| Compression | **none** (ADR P4-14) — stored length equals SSZ length |

The design does **not** depend on a 150–200 MB estimate; the numbers above are
what the cadence and disk model use from this measurement.

### Three terms measured separately (CC-42 /3 / OQ-P4-4)

| Term | Phase label | What | Measured | Notes |
|---|---|---|---|---|
| **(a)** | `replay` | Epoch-transition time during storage own-replay | **605.29 ms / epoch** (mean wall) | Reference: Phase 1 mid-gate mean wall. Own-replay now runs real `state_transition` / `process_slots` (`NoVerification` + always-Valid EL stub). Continuous-chain `phase=replay` samples accumulate on live write-behind. |
| **(b)** | `serialize` + `write` | SSZ serialize + P2 write | **serialize 0.053 s** (Hoodi state); write = one P2 put+evict | Unit criterion CC-42/2: P0 commit p99 during **8 MiB** P2 snapshot writes within 10 % of baseline (writer P0-priority). **Residual:** full ~200 MB Hoodi P2 write wall is soak-only (not the 10 % commit criterion). |
| **(c)** | `load` | SSZ deserialize + tree-hash-cache rebuild | **2.97 s** (warm) / **4.80 s** (cold first sample) | Both ≤ **5.0 s**. Command: `cargo test -p cc-storage --bins term_c_hoodi -- --nocapture` |

### Cadence conditional (executed in this issue)

| Check | Value |
|---|---|
| Term (c) vs 5.0 s boundary | **4.80 s max observed ≤ 5.0** |
| Decision | **Keep `storage.snapshot_epochs = 32`** |
| Fallback (if term (c) > 5.0) | Drop to **16** and re-derive CC-45 /6 — **not taken** |

`OQ-P4-4` is closed by the term-(c) measurement above.

### Ring and divergence (unit-level)

| Check | Result |
|---|---|
| Fifth snapshot evicts oldest; `cc_storage_snapshot_ring_depth` stays 4 | **PASS** (`cc-store` + `cc-storage` ring tests) |
| Divergence guard: corrupt expected root → fatal + both roots + counter +1 | **PASS** (`replay::tests::divergence_guard_fatal_logs_and_increments_exactly_one`) |
| Positive path counter at 0 | **PASS** (`replay::tests::positive_snapshot_zero_divergence`) |
| Commit p99 during snapshot within 10 % of baseline | **PASS** (micro-bench `commit_latency_during_snapshot_within_10_percent`) |
| Uncompressed (`grep` production body free of flate/zstd/snap) | **PASS** |

### Config

```text
storage.snapshot_epochs = 32
storage.snapshot_ring   = 4
```

## V-10 / Clause 3 (M4.2 entry)

**Owner:** CC-42  
**Date:** 2026-08-08  

### V-10 — mid-gate and CC-1H

| Field | Value |
|---|---|
| Command | `cargo test -p cc-chain --test offline_replay cc1h_mid_gate_with_fork_choice_clone -- --nocapture` |
| Recorded in | `docs/phase-1-soak.md` (mid-gate section) |
| Result **today** (this worktree's recorded soak) | **PASS** — max epoch wall **774.81 ms** &lt; 1000 ms bar |
| Hash share | mean **21.0 %**, max **25.2 %** |
| **CC-1H promoted?** | **No** — mid gate closed without promoting hierarchical state |
| Implication for CC-42 | Cheap diffing (`hdiff`) remains **unavailable** (§3.3); cadence is the only lever. `CC-4K` stays unscheduled. |

### Phase 1 Clause 3 number (D-12)

| Metric | Bar | Recorded | Status |
|---|---|---|---|
| Epoch p95 (mid-gate max wall used as the available number) | ≤ 1000 ms | **774.81 ms** max wall | **PASS** (from phase-1-soak; not re-run in this agent session as a fresh p95 histogram) |
| `process_block` p95 | ≤ 400 ms | See phase-1-soak / engine-latency — mid-gate path is epoch ST + FC clone, not a live `process_block` p95 series | **Recorded reference; not re-instrumented here** |

**D-12 inversion (stated):** Phase 1's **Clause 3 gates CC-42** (and CC-46); Phase 1's **Clause 2** (24 h soak) **does not**. A snapshot cadence chosen against an unmeasured baseline is a guess; a 24 h soak that a restart would void is circular with Phase 4's purpose.

**What was used instead of a fresh Clause 3 NOT_RUN:** the committed mid-gate table in `docs/phase-1-soak.md` (max wall 774.81 ms) plus this issue's term-(c) measurement closing OQ-P4-4. If a later operator re-runs the mid-gate and sees a regression above 1000 ms, re-derive term 4 of §3.4 and re-check the 60 s restart bar margin.
