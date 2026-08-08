# Phase 4 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton opened by
**CC-4B** (Amendment 5) with the `OQ-1` foreign-peer probe section; **CC-4Cb**
lands the remaining named-section headers (empty) plus the D-12 gating record.
Later issues fill numbers only — the instrument measures the runs; it is not a
run (D-10).

| Section | Owner |
|---|---|
| `## OQ-1 — foreign-peer probe` | **CC-4B** |
| `## Engine falsifier — both layouts` | **CC-40b** |
| `## Snapshot terms` | **CC-42** |
| `## Phase 1 clause gating (D-12)` | **CC-4Cb** |
| `## Clause 1 — restart trials` | **CC-45c** |
| `## Clause 2 — cursor fallback` | **CC-44b** / **CC-48** / **CC-47a** |
| `## Clause 3 — compressed-retention plateau (discharging)` | **CC-4Cc** |
| `## Clause 3 — Hoodi confirmation (non-discharging)` | **CC-4Cd** |
| `## Clauses 4, 5, 6, 7 — Hoodi` | **CC-4Cd** |
| `## CC-4G — cgc rehearsal` | **CC-4G** |
| `## Machine and environment` | **CC-4Cd** |
| `## Run record` | **CC-4Cb** skeleton; numbers **CC-4Cd** |
| `## Clause table` | **CC-4Cb** skeleton; numbers **CC-4Cd** |

Report instrument: `bash scripts/soak-report.sh --phase 4` (CC-4Cb / §10.6).
Venues are the closed set `hoodi | self-devnet | self-devnet-compressed |
in-process-double | dev-machine`. Confirmation rows carry
`confirmation, non-discharging`.

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

## Engine falsifier — both layouts

**Owner:** CC-40b  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Numbers land in this section
only.

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

## Phase 1 clause gating (D-12)

**Owner:** CC-4Cb  
**Date:** 2026-08-08  
**Milestone:** M4.3

### Decision (not an oversight)

**D-12's inversion is recorded here as a decision, not an oversight.**

| Phase 1 clause | Gates Phase 4? | Scope | Rationale |
|---|---|---|---|
| **Clause 3** (epoch p95 ≤ 1000 ms; `process_block` p95 ≤ 400 ms) | **Yes — hard gate** | **`CC-42` and `CC-46a` only** — and on **nothing else** | Both issues add work to the same slot budget; a snapshot cadence (or prune path) chosen against an unmeasured baseline is a guess |
| **Clause 2** (≥ 24 h Hoodi soak) | **No — not a gate at all** | — | A 24 h soak that a restart would void is precisely the thing Phase 4 removes the need for; requiring it first is **circular** |

### Phase 1 Clause 3 reading as of M4.3

| Source | Metric | Bar | Recorded | Status |
|---|---|---|---|---|
| `docs/phase-1-soak.md` mid-gate (V-10) | Epoch wall (stand-in for p95) | ≤ 1000 ms | **774.81 ms** max wall | **PASS** |
| mid-gate path | `process_block` p95 | ≤ 400 ms | See phase-1-soak / engine-latency — mid-gate path is epoch ST + FC clone, not a live `process_block` p95 series | **Recorded reference; not re-instrumented at M4.3** |
| CC-1H | Hierarchical state promoted? | — | **No** | mid gate closed without promoting hierarchical state |

**Commands used (V-10 re-read at M4.2 / M4.3 entry):**

```text
cargo test -p cc-chain --test offline_replay cc1h_mid_gate_with_fork_choice_clone -- --nocapture
```

**M4.2 entry (CC-42):** Clause 3 number below bar → CC-42 proceeded.  
**M4.3 entry (CC-46a):** same recorded mid-gate number → CC-46a proceeded.  
No fresh Clause 3 histogram was re-run in those agent sessions; the committed
`docs/phase-1-soak.md` mid-gate table is the authority. If a later operator
re-runs the mid-gate and sees a regression above 1000 ms, re-derive term 4 of
§3.4 and re-check the 60 s restart bar margin.

**What was used instead of a fresh Clause 3 `NOT_RUN`:** the committed mid-gate
table (max wall 774.81 ms) plus CC-42's term-(c) measurement closing OQ-P4-4.
Phase 1's **Clause 2** was **not** required at either entry.

### Implication for cheap diffing

`CC-1H` remains unpromoted → cheap diffing (`hdiff`) remains **unavailable**
(§3.3); cadence is the only lever. `CC-4K` stays unscheduled.

## Clause 1 — restart trials

**Owner:** CC-45c  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Numbers land in this section
only. Branch A (Hoodi stack with Phase 3's EL) discharges; branch B records the
literal string `partial — no EL in the restart set` and does **not** discharge.

## Clause 2 — cursor fallback

**Owner:** CC-44b / CC-48 / CC-47a  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Three stages (D-14); only the
third discharges.

### M4.2 attribution

**Owner:** CC-44b  
**Status:** empty.

### M4.4 hole recorded

**Owner:** CC-48 / CC-45b  
**Status:** empty.

### M4.5 hole closed

**Owner:** CC-47a  
**Status:** empty.

## Clause 3 — compressed-retention plateau (discharging)

**Owner:** CC-4Cc  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Discharging venue:
`self-devnet-compressed`. The plateau run itself is CC-4Cc — this section
receives numbers only.

## Clause 3 — Hoodi confirmation (non-discharging)

**Owner:** CC-4Cd  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Row marked
`confirmation, non-discharging`. Must not be merged with the compressed-retention
discharging row.

## Clauses 4, 5, 6, 7 — Hoodi

**Owner:** CC-4Cd  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Full-window serve-probe
(blocks + columns), negative side, and advertisement=served.

## CC-4G — cgc rehearsal

**Owner:** CC-4G  
**Status:** empty skeleton (Amendment 5 / CC-4Cb).

## Machine and environment

**Owner:** CC-4Cd  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Machine spec + V-6 `df -h`.

## Run record

**Owner:** CC-4Cb skeleton; numbers **CC-4Cd**  
**Status:** skeleton only — it measures the runs; it is not a run (D-10).

| Field | Value |
|---|---|
| Phase | 4 |
| Instrument | `bash scripts/soak-report.sh --phase 4` |
| Plateau run | **CC-4Cc** (not this issue) |
| Hoodi week | **CC-4Cd** (not this issue) |
| Restart trials | **CC-45c** (not this issue) |
| Git SHA | _TBD at fill-in_ |
| Start / end | _TBD_ |
| Venue(s) | closed set per clause row |

## Clause table

**Owner:** CC-4Cb (script) / CC-4Cd (numbers)  
**Generated by:** `bash scripts/soak-report.sh --phase 4`  
**Status:** skeleton — paste script output here after a live or harness-backed
run. Columns: `clause | venue | measured | threshold | verdict`.

| Clause | Venue | Measured | Threshold | Verdict |
|---|---|---|---|---|
| _TBD_ | _TBD_ | **NOT_RUN** | — | **NOT_RUN** |
