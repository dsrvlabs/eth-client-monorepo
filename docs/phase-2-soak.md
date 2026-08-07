# Phase 2 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton created by
**CC-21a**; run-record / clause-table fields filled by later issues (CC-29b
template completion, CC-29c numbers). **No invented numbers.**

| Section | Owner |
|---|---|
| `## CC-21/1 ENR sequence` | CC-21a |
| `## IDONTWANT A/B` | CC-22c |
| `## KZG benchmark` | CC-24b (cross-ref `docs/kzg-benchmark.md`) |
| `## Non-Hoodi clauses` | CC-2Jd / CC-2Jb / CC-2Jc / CC-26b / CC-2A |
| `## M2.1 Hoodi hold` | CC-21c |
| `## CC-27/5 verdict latency` | CC-27c |
| `## Run record` | CC-29b skeleton; CC-29c numbers |
| `## Clause table` | CC-29b skeleton; CC-29c numbers |

## CC-21/1 ENR sequence

**Owner:** CC-21a  
**Date:** 2026-08-07  
**discv5 version:** **0.11.0** (workspace pin, `libp2p` feature; CC-2K)  
**Method:** unit/integration test against a real `Discv5` handle constructed on
loopback listen config (`127.0.0.1:0`), **never started** (no UDP bind, no
bootnodes, no network). Throwaway secp256k1 key generated in-process — does not
create `./data/node_key`.

**Command:**

```text
cargo nextest run -p cc-p2p --test enr_seq_probe
```

### Outcome

**`enr_insert` bumps and re-signs.**

| Check | Result |
|---|---|
| `Discv5::enr_insert("cgc", &v)` strictly increases `local_enr().seq()` | **PASS** |
| Resulting ENR `verify()` against its own public key | **PASS** |
| `EnrManager::apply` two-field batch → `seq` + **exactly one** | **PASS** |
| Rebuild-and-replace strategy (fallback path) same three properties | **PASS** (implemented; not selected as default) |

**Strategy shipped in `EnrManager`:** `EnrSeqStrategy::EnrInsert` — single-field
`apply` calls `Discv5::enr_insert`; multi-field batches coalesce via
insert-then-`set_seq(old+1)` on the local ENR replacement path so one logical
change is one sequence bump (§6.2). The rebuild-and-replace fallback remains
available as `EnrSeqStrategy::RebuildAndReplace` if a future discv5 rev regressed
A-P2-4.

**A-P2-4 verdict:** confirmed for discv5 0.11.0 on 2026-08-07.

## IDONTWANT A/B

**Owner:** CC-22c  
**Status:** `_NOT_RUN_` — delta not measured.

## KZG benchmark

**Owner:** CC-24b  
**Status:** `_NOT_RUN_` — see `docs/kzg-benchmark.md` when filled.

## Non-Hoodi clauses

**Owner:** CC-2Jd / CC-2Jb / CC-2Jc / CC-26b / CC-2A (one subsection each, by clause)  
**Status:** `_NOT_RUN_`

## M2.1 Hoodi hold

**Owner:** CC-21c  
**Status:** `_NOT_RUN_` — code path landed (discv5 task, V-4 bootnodes in
`config/p2p.toml` retrieved **2026-08-07** from eth-clients/hoodi
`metadata/bootstrap_nodes.yaml`). Live cold-start (≥ 25 peers / 10 min, then
60 min hold on `cc_p2p_peers{direction}` + `cc_p2p_peers_custody_compatible`)
and D-8 promotion verdict remain operator runs.

**V-4 bootnode list:** see `config/p2p.toml` `[discovery].boot_nodes` (retrieval
date in-file). Self-devnet reads `devnet/out/bootnodes.txt` via
`boot_nodes_file`.

## CC-27/5 verdict latency

**Owner:** CC-27c  
**Date:** 2026-08-07  
**Metric:** `cc_p2p_verdict_latency_seconds` — p95 at exact `le=0.1` bucket
boundary (CC-29a).

| Check | Result |
|---|---|
| Gossip-verify fast path in `services/chain/src/import.rs` | **LANDED** — cheap checks emit ACCEPT before `on_block` / ST |
| In-process: Verdict arrives while transition stalled 2 s | **PASS** (`cargo test -p cc-chain --test gossip_verify_fast_path`) |
| Self-devnet p95 ≤ 100 ms over ≥ 100 blocks | **`_NOT_RUN_`** — record `histogram_quantile` / `le="0.1"` count here after M2.2 self-devnet |

**Method (when run):** scrape `cc_p2p_verdict_latency_seconds_bucket{le="0.1"}` and
total count over a self-devnet window of ≥ 100 imported blocks; p95 ≤ 100 ms is
a counting question at the exact boundary, not an interpolation.

## Booking (c) — DA-blind head follow (M2.2 exit criterion 9)

**Owner:** CC-22d  
**Status:** `_NOT_RUN_` — code path landed (block + column validators, single
report site, seen/pending, inclusion-proof cache). Operator discharge: self-devnet
with CC-2Jd publisher replaying the fixture, `AlwaysAvailable` still in place,
`bin/driver` still present; assert head follows over gossip alone via
`CC_DEVNET_SMOKE_HEAD_FOLLOW=1 devnet/smoke.sh`.

## Run record

**Owner:** CC-29b (skeleton) / CC-29c (numbers)  
**Status:** **`NOT_RUN`** — no Phase 2 Hoodi soak yet. Placeholders only; do not
treat any `_NOT_RUN_` cell as a pass.

### Fields (fill from a single continuous process)

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | Process start timestamp (UTC) | `_NOT_RUN_` | Wall-clock when the running binary started |
| 1b | Git SHA of the running binary | `_NOT_RUN_` | Pin from build info |
| 1c | libp2p git rev | `_NOT_RUN_` | Same pin as `docs/p2p-dependencies.md` / `cc_libp2p::LIBP2P_GIT_REV` |
| 2 | Peer-set-stable timestamp (UTC) | `_NOT_RUN_` | Opens the steady-state window (§12.2) |
| 2b | Steady-state window end (UTC) | `_NOT_RUN_` | ≥ 24 h after field 2 |
| 3a | Hour-2 RSS (KiB) | `_NOT_RUN_` | |
| 3b | Hour-24 RSS (KiB) | `_NOT_RUN_` | |
| 3c | `cc_p2p_cache_occupancy_bytes` hour-2 / hour-24 | `_NOT_RUN_` | OQ-P2-4 |
| 4 | Machine spec | `_NOT_RUN_` | A-P2-8 |
| 5 | Hoodi soak vs self-devnet | `_NOT_RUN_` | concurrent **or** serial (D-6 default: serial) |
| 6 | Bootnode list + V-4 retrieval date | `_NOT_RUN_` | |
| 7 | Per-slot series path | `_NOT_RUN_` | peers, custody-compatible, head lag, RSS, load |
| 8 | V-2 fork check result | `_NOT_RUN_` | which forks checked |
| 9 | Stream reconnect / worker panic / rate-limit log | `_NOT_RUN_` | timestamps |

### Events log

| Timestamp (UTC) | Event | Detail |
|---|---|---|
| — | *(empty until soak)* | |

## Clause table

**Owner:** CC-29b (skeleton) / CC-29c (numbers)  
**Status:** **`NOT_RUN`**

| Clause | Venue | Measured | Threshold | Pass/Fail |
|---|---|---|---|---|
| 1 · healthy peer count 24 h | Hoodi | `_NOT_RUN_` | min peers ≥ 25 **and** custody-compatible ≥ 8 | `_NOT_RUN_` |
| 2 · DA-gated import | Hoodi | `_NOT_RUN_` | imported non-trivial; no deferred head ancestry | `_NOT_RUN_` |
| 3 · head lag ≤ 1 typical | Hoodi | `_NOT_RUN_` | bucket `le=1` ≥ 0.95 (catch-up excluded) | `_NOT_RUN_` |
| 4 · 10-minute gap recovery | self-devnet | `_NOT_RUN_` | back to head within 32 slots | `_NOT_RUN_` |
| 5 · withheld column | adversarial harness | `_NOT_RUN_` | deferred then recovered | `_NOT_RUN_` |
| 6 · scoring penalises | adversarial harness | `_NOT_RUN_` | penalty reason + score crosses −4000 | `_NOT_RUN_` |
| CC-2A · BPO | self-devnet | `_NOT_RUN_` | topic-set change count == 2; peers retained | `_NOT_RUN_` |
| R-5 cross-check | Hoodi | `_NOT_RUN_` | `{recovered}` vs `{deferred}` over 24 h | `_NOT_RUN_` |
