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

### Clause 5 — withheld column (CC-2Jb)

**Owner:** CC-2Jb  
**Venue:** adversarial harness (self-devnet + publisher `--fault-mode withhold-column`)  
**Date:** 2026-08-07  
**Status:** code path landed; full compose discharge via `devnet/scenarios/withheld-column.sh`

| Check | Result |
|---|---|
| Control run (fault off) first | **LANDED** — scenario runs control before fault (R-7) |
| R-4 non-zero commitment count | **PASS** (unit + runtime refuse) |
| Withheld ⊆ node-a sampled set | **PASS** (refuse-to-start + unit) |
| Publish seam skips withheld indices | **PASS** (`decide_column_publish` / `fault_mode` unit) |
| By-root refuse until flag flips | **PASS** (`decide_by_root_column_serve` + flag unit) |
| R-7 publisher metric surface (zero withheld subnet, non-zero others) | scenario asserts when compose path run |
| Both halves one run: deferred then recovered + head advance | scenario records; full DA path needs production peer (see R-5) |
| Second run withholding 2 of 8 | `CC_WITHHOLD_MULTI=1` |
| Venue column present (not Hoodi) | **yes** — adversarial harness |

**R-5 limitations (run notes):** node-a had **one peer** (publisher-only static list);
the withholding peer was **our own publisher**; withholding was deterministic
because of both. Do not read this row off a Hoodi soak.

**Commands:**

```text
cargo test -p cc-p2p --lib fault_mode
cargo test -p cc-p2p --lib by_root_withhold_seam
CC_SKIP_DOCKER=1 ./devnet/scenarios/withheld-column.sh
./devnet/scenarios/withheld-column.sh
```

### Clause 6 — scoring penalises misbehaving peers (CC-2Jc)

**Owner:** CC-2Jc  
**Venue:** adversarial harness (self-devnet + `--fault-mode misbehave:<kind>`)  
**Status:** **code READY** / live run `_NOT_RUN_`

Kinds map to attributable `cc_p2p_peer_penalty_total{reason}` labels:

| Kind | Mechanism | Penalty reason | Δ |
|---|---|---|---|
| `invalid-column` | mutated KZG proof on gossip | `gossip_invalid` | −10 |
| `malformed` | truncated/corrupt sidecar bytes | `gossip_invalid` | −10 |
| `spam` | multi-publish + over-limit req/resp | `rate_limit` | −5 |
| `custody-refuse` | by-root refuse while cgc covers | `custody_unserved` | −15 |
| `stall-reqresp` | first byte after TTFB+1s | `reqresp_fault` | −5 |

**Control runs (R-7):** same scenario with `--fault-mode none` must pass **first** for every kind.

| Kind | Control (fault off) | Fault run | Emissions to cross −4000 | Elapsed slots | App disconnect before GossipSub? | Pass/Fail |
|---|---|---|---|---|---|---|
| `invalid-column` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` |
| `malformed` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` |
| `spam` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` |
| `custody-refuse` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` |
| `stall-reqresp` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` | `_NOT_RUN_` |

**Assertions when run:**
- Each kind attributes **only** its own `reason` label (no cross-contamination).
- Misbehaving publisher's `cc_p2p_peer_score` **crosses the −4000 bucket** (or timeout recorded as R-3 finding).
- `malformed`: `cc_p2p_worker_panics_total` stays 0.
- `spam`: `cc_p2p_reqresp_ratelimit_total` increments; publisher sees error response (not silent drop).
- `custody-refuse`: publisher advertises covering `cgc`; non-custody peers are **not** penalised for the same non-response.
- Publisher independent reference: its `cc_p2p_reqresp_inbound_total` shows node-a requests arriving.
- R-3 cross-check: `cc_p2p_peers_below_threshold{threshold}` for **honest** node-b recorded (non-zero with no induced fault is self-isolation).
- `behavioural` (P7) label still has a producer (GossipSub-observed); no kind injects it (D-5).

**Unit coverage (landed):** `cargo test -p cc-p2p --lib fault_mode` + `reqresp::columns` seam tests for custody-refuse / stall-reqresp decisions.

### Clause 4 · 10-minute gap recovery (CC-26b)

**Owner:** CC-26b  
**Venue (discharging):** self-devnet  
**Booking:** (e) — `faults.sh offline-gap node-a 10` (CC-2Jd docker `network disconnect` primitive)  
**Status:** **`_NOT_RUN_`** — planner + no-DA-bypass unit path landed; live self-devnet discharge waits on M2.3 exit / operator run (CC-2Jb ideally green for full booking matrix; clause 4 itself needs only the planner + offline-gap primitive).

| Field | Value |
|---|---|
| Venue | self-devnet |
| Measured recovery (slots) | `_NOT_RUN_` |
| Threshold | back to head within **32 slots** |
| Parent-linkage walk | `_NOT_RUN_` |
| Every backfilled block DA-gated (`cc_p2p_da_outcome_total`) | `_NOT_RUN_` |
| Pass/Fail | `_NOT_RUN_` |

### Clause 4 confirmation · Hoodi (R-5, non-discharging)

**Owner:** CC-26b  
**Venue:** Hoodi-connected compose stack (serial with self-devnet — D-6 / R-8)  
**Status:** **`_NOT_RUN_`** — confirmation only; **does not discharge** clause 4. Ten-minute disconnect then by-range backfill of ~50 real blocks + columns from real peers.

| Field | Value |
|---|---|
| Venue | Hoodi |
| Kind | `confirmation, non-discharging` |
| Measured | `_NOT_RUN_` |
| Pass/Fail | `_NOT_RUN_` (failure is still a blocker) |

### CC-2A · BPO transition (booking g)

**Owner:** CC-2A  
**Venue:** **self-devnet** (generated `BLOB_SCHEDULE` with `bpo_1_epoch: 5`, `bpo_2_epoch: 10`)  
**Date:** 2026-08-07  
**Status:** code path **LANDED** / live self-devnet regression **`_NOT_RUN_`**

| Check | Result |
|---|---|
| V-3: `BLOB_SCHEDULE` re-read vs live Hoodi head | **PASS** (unit) — head ≈ epoch **114 273** on 2026-08-07; BPO1=52480 / BPO2=54016 both past; **no pending real-network BPO** |
| Boundary known in advance from schedule | **PASS** (`cargo test -p cc-p2p --test bpo_transition`) |
| Overlap window both sets inside / one outside | **PASS** (unit) |
| `set_topic_params` before `subscribe` on next digest | **PASS** (recording stub) |
| `nfd` / `next_fork_epoch` track BPO; `next_fork_version` unchanged | **PASS** (Hoodi schedule) |
| ENR eth2+nfd coalesce → `seq` +1 at boundary; idempotent re-apply | **PASS** |
| Status v2 switches at boundary; old peer not disconnect before / may after | **PASS** |
| Two-boundary walk (epochs 5 & 10) Steady→Overlap→Drain×2 | **PASS** (unit stand-in for booking g) |
| Live self-devnet: topic-set change count == 2; peers retained; no zero-rate slot | **`_NOT_RUN_`** |
| Venue column (not Hoodi) | **yes** — self-devnet |

**soak-report expression (when run):** topic-set change count == 2; peers retained; no zero-rate slot on `cc_p2p_gossip_messages_total{topic=~".*beacon_block"}` across either boundary.

**OQ-5 limitation (run notes — not papered over):** the self-devnet covers the *mechanism* under our control (digest change, resubscription, `nfd`, peer retention). Heterogeneous peers transitioning at slightly different times is **not** covered — every peer on it runs our code. Do not claim equivalence to a multi-client BPO.

**Commands:**

```text
cargo test -p cc-p2p --test bpo_transition
cargo clippy -p cc-p2p --all-targets -- -D warnings
# live (operator): devnet/scenarios/bpo.sh  — NOT_RUN
```

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
**Status:** **`NOT_RUN`** — fill from `scripts/soak-report.sh --phase2` against a real
window. A clause read by eye off a Grafana panel does **not** discharge it.
Empty placeholders only; **no invented numbers**.

| Clause | Venue | Measured | Threshold | Pass/Fail |
|---|---|---|---|---|
| 1 · healthy peer count 24 h | Hoodi | `_NOT_RUN_` | `min_over_time` peers ≥ 25 **and** custody ≥ 8 | `_NOT_RUN_` |
| 2 · DA-gated import | Hoodi | `_NOT_RUN_` | imported non-trivial; no deferred head ancestry | `_NOT_RUN_` |
| 3 · head lag ≤ 1 typical | Hoodi | `_NOT_RUN_` | bucket `le=1` ≥ 0.95 (catch-up excluded via peer-set-stable) | `_NOT_RUN_` |
| 4 · 10-minute gap recovery | self-devnet | `_NOT_RUN_` | back to head within 32 slots; parent walk clean; DA-gated (`cc_p2p_da_outcome_total`) | `_NOT_RUN_` |
| 4 · Hoodi confirmation (R-5) | Hoodi | `_NOT_RUN_` | ~50 real blocks+columns by-range; **non-discharging** | `_NOT_RUN_` |
| 5 · withheld column | adversarial harness | `_NOT_RUN_` | deferred then recovered + head advance | `_NOT_RUN_` |
| 6 · scoring penalises | adversarial harness | `_NOT_RUN_` | penalty reason + score crosses −4000 bucket | `_NOT_RUN_` |
| CC-2A · BPO | self-devnet | `_NOT_RUN_` | topic-set change count == 2; peers retained; no zero-rate `beacon_block` | `_NOT_RUN_` |
| R-5 cross-check | Hoodi | `_NOT_RUN_` | `{recovered}` vs `{deferred}` over 24 h; zero-recovered + non-zero deferred = harness-only recovery | `_NOT_RUN_` |

### Method (CC-29b)

- Clause 1: **`min_over_time`**, not an average — one dip below threshold fails.
- Clauses 3 and 6: **bucket fractions at exact boundaries** (`le=1`, `le=-4000`), not quantile interpolation.
- Clause 3 catch-up exclusion keyed on **`peer_set_stable_unix`** from the sampler meta; pre-stable lag reported separately. Missing that timestamp refuses the report.
- Venue is a column on every row so a non-Hoodi clause cannot be read off a Hoodi run.

### Sampler / report commands (fill-in)

```bash
# Per-slot series (peers, custody, head lag, RSS, load) + peer-set-stable meta.
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv \
  --p2p-metrics-url http://127.0.0.1:9102/metrics

# Capture p2p histogram/counter scrape pair after peer-set-stable, then at window end:
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-start.txt
# … ≥ 24 h steady-state …
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-end.txt

# Optional harness discharge results (clauses 4/5/6/CC-2A) as JSON.
# bash scripts/soak-report.sh --phase2 …
bash scripts/soak-report.sh --phase2 \
  --samples            soak-samples.csv \
  --run-meta           soak-samples.meta \
  --p2p-metrics-start  p2p-metrics-start.txt \
  --p2p-metrics-end    p2p-metrics-end.txt \
  --harness-json       harness-results.json \
  --out                clause-table.md
# optional: --write  → replace this file's ## Clause table section

# Fixture self-test (no live stack; CI / pre-soak):
bash scripts/soak-report.sh --self-test
```
