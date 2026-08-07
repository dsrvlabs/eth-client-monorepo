# Engine latency accounting and the `CC-1H` verdict (CC-3C)

| Field | Value |
|---|---|
| **Issue** | CC-3C |
| **Date** | 2026-08-07 (UTC) / 2026-08-08 (KST) |
| **Tree** | `feature/cc-3c-latency-verdict` on top of `origin/develop` @ `6b39f16` (engine wired, CC-32b) |
| **Purpose** | Four latency numbers, real-Hoodi encode, mid-gate side-by-side vs G-A, named Trigger A/B/C verdict for G-B |

---

## Dev-machine spec (CC-3C /5)

| Item | Value |
|---|---|
| Host | MacBook Pro (Mac16,8) |
| CPU | Apple M4 Pro, 14 cores (10P + 4E) |
| RAM | 24 GB |
| Disk | APPLE SSD AP1024Z, Solid State, Apple Fabric |
| OS / arch | macOS / aarch64 |
| Toolchain | rustc 1.97.1 (workspace pin) |

**Machine scheduling (R-7).** These numbers were taken while other worktrees and a compose stack (`chain`/`engine`/`el`/…) were still on the host. Load average and disk activity at run start are recorded below; they **violate** the exclusive-machine rule stated in the issue. Treat tails as upper bounds, not as a clean baseline for Phase 4 disk-growth comparison.

| Run | loadavg (1m / 5m / 15m) at start | Disk notes |
|---|---|---|
| Encode + wiremock spans | ~56 / 53 / 42 | concurrent compiles / compose |
| Mid-gate run 1 | ~48 / 51 / 49 | concurrent load |
| Mid-gate run 2 | ~57 / 53 / 49 | concurrent load |
| Mid-gate run 3 | ~52 / 52 / 49 | concurrent load |

Disk queue depth is not exposed as Linux `iostat -x` on this host; `iostat` KB/t and tps were non-zero throughout (SSD, mixed interactive load).

---

## G-A — without engine (transcribed, not re-measured)

**Source:** `/tmp/grok-plan-summary-68b80287-cc-30a.md` (CC-30a summary).  
**Commit named there:** `25ec132` on branch `feature/cc-30a-transport-offline` (G-A entry at M3.2).  
**Amendment 8:** this column is a **transcription**. It **cannot be retaken after `CC-32b` without unwiring the engine**, which nobody will do under schedule pressure. Re-measuring now would produce a "with engine" number wearing a "without" label.

**Machine (G-A):** macos aarch64 / Apple M4 Pro / 24 GB  
**Command × 3:** `cargo nextest run -p cc-chain -E 'test(cc1h_mid_gate_with_fork_choice_clone)'`

| Run | max_wall_ms | mean_wall_ms | mean_hash_share% | mean_fc_clone_ms | real s | result |
|-----|-------------|--------------|------------------|------------------|--------|--------|
| 1 | 655.91 | 644.19 | 23.4 | 69.37 | 9.57 | PASS (mid gate closed) |
| 2 | 905.93 | 760.20 | 20.4 | 105.48 | 10.48 | PASS (mid gate closed) |
| 3 | 897.41 | 766.05 | 20.5 | 107.67 | 10.49 | PASS (mid gate closed) |

**p95 gap (G-A wontfix, transcribed):** `offline_replay` / mid-gate does **not** emit `p95(cc_chain_process_block_seconds)` or `p95(cc_chain_process_block_local_seconds)`. Pre-engine those series would agree if measured on the import path; wall / hash / fc_clone + machine are the committed G-A record.

---

## CC-3C /1 — SSZ→JSON hex encode on a real Hoodi payload

**Command:**

```text
cargo nextest run -p cc-engine -E 'test(encode_real_hoodi_payload)' --nocapture
```

| Field | Value |
|---|---|
| **Source** | `beacon.hoodi.ethpandaops.io/eth/v2/beacon/blocks/head` → `/tmp/hoodi-head-ethpandaops.ssz` (raw SSZ; `eth-consensus-version: fulu`) |
| **Beacon slot** | 3659006 |
| **EL block number** | **3372154** |
| Signed block SSZ | 14 969 B |
| Payload SSZ | 5 562 B |
| Transaction count | 30 |
| **Transaction-list bytes** | **4 192** |
| **ExecutionPayloadV3 JSON bytes** | **11 780** |
| newPayloadV4 params JSON | 11 857 B |

### Encode wall clock (`encode_execution_payload_v3`, 25 samples, warm discarded)

| Quantile | ms |
|---|---:|
| p50 | **0.1155** |
| mean | 0.1109 |
| p95 | **0.1228** |

### `cc_engine_encode_seconds` window (SSZ decode + full params build, 25 samples)

| Quantile | ms |
|---|---:|
| p50 | **0.1217** |
| p95 | **0.1245** |

**Read of the number.** This Hoodi head is a **quiet** block (~4 KB of transactions → ~12 KB of hex JSON). The Architecture §6.2 concern — ~1.5 MB of transactions → ~3 MB of hex — is **not** present in this capture. The encode path is real (production `encode_execution_payload_v3` / `build_new_payload_v4_params`); the absolute cost on this payload is **sub-millisecond** and will scale roughly with transaction-list bytes. A synthetic uniform payload was **not** used.

---

## CC-3C /2 — four numbers: (1) call, (2) request, (3) encode, hop = (1)−(2)−(3)

> **Read this first (proxy vs production).** The table below is a **process-local Instant
> proxy**, not live Prometheus quantiles from `cc_chain_engine_call_seconds` /
> `cc_engine_request_seconds` / `cc_engine_encode_seconds`. The “hop” row is
> **not** a measured `chain→engine` gRPC hop: it is the residual of three
> independent offline loops (encode / HTTP / inclusive) in one process, so
> `(1) ≈ (2)+(3)` by construction and any leftover is scheduling noise. Do
> **not** cite the 0.05 ms residual as a gRPC hop for topology or G-B.

### Live process histograms (compose stack on this host)

```text
curl -s localhost:9101/metrics | grep -c 'cc_chain_process_block_local_seconds_bucket'  # → 14
curl -s localhost:9101/metrics | grep -c 'cc_chain_engine_call_seconds_bucket'            # → 14
curl -s localhost:9101/metrics | grep -c 'cc_chain_process_block_seconds_bucket'          # → 14
```

`PROCESS_BLOCK_BUCKETS` has **13** finite boundaries (const reused at CC-3Aa). Exposition also emits `le="+Inf"` → **14** lines. The three families are **bucket-for-bucket comparable** (same ladder).

At measurement time the live stack only had **seed** observations (`_count = 1`, `_sum = 0.0`) on:

- `cc_chain_engine_call_seconds`
- `cc_engine_request_seconds{method="newPayloadV4"}`
- `cc_engine_encode_seconds{method="newPayloadV4"}`
- `cc_chain_process_block_seconds` / `cc_chain_process_block_local_seconds`

So production p50/p95 from those histogram families are **not available**. The offline table is a stand-in that exercises the same encode + ordered-lane HTTP code paths (`new_payload_v4` + wiremock EL) until an exclusive-machine import path fills the three families.

### Offline span samples — process-local proxy (wiremock EL, real Hoodi payload, 20 samples after warm)

**Command:**

```text
cargo nextest run -p cc-engine -E 'test(latency_spans_wiremock_new_payload)' --nocapture
```

| # | Span | What was timed | p50 (ms) | p95 (ms) |
|---|---|---|---:|---:|
| **(1)** | Inclusive `new_payload_v4` (encode + HTTP) | Instant around in-process adapter — **proxy** for `cc_chain_engine_call_seconds` **minus gRPC** | 0.2276 | 0.2638 |
| **(2)** | Ordered-lane HTTP only | Instant around `transport.call` (stub params, not full ~12 KB body) — **proxy** for `cc_engine_request_seconds{method="newPayloadV4"}` | 0.0851 | 0.0976 |
| **(3)** | SSZ decode + params build | Instant around decode + `build_new_payload_v4_params` — **proxy** for `cc_engine_encode_seconds{method="newPayloadV4"}` | 0.0900 | 0.0953 |
| **residual** | **(1) − (2) − (3)** | Process-local overhead ballpark — **not** chain→engine gRPC | **0.0525** | **0.0709** |

Arithmetic (p50): `0.2276 − 0.0851 − 0.0900 = 0.0525` ms.  
Arithmetic (p95): `0.2638 − 0.0976 − 0.0953 = 0.0709` ms.

**Method notes (why this is not the production hop):**

1. **No gRPC.** Engine method adapter + wiremock EL share one process; `EngineApiClient` → engine gRPC is absent.
2. **Independent loops.** (1), (2), and (3) are separate sample sets, not nested spans of one call; residual is distribution mismatch + noise (clamped at ≥ 0), not a fourth instrumented span that reconciles by construction.
3. **HTTP body.** (2) used stub `params` (`[{}, [], "0x00", []]`), not the full encoded Hoodi payload body, so request work is understated vs a real newPayload.
4. **Live fill still required.** A true hop needs non-seed `cc_chain_engine_call_seconds` on chain with the engine peer, then hop = (1) − (2) − (3) from those three histograms.

### Geth's own `newPayload` timing (independent EL reference)

```text
curl -s localhost:6060/debug/metrics/prometheus | grep -i newpayload
```

This geth build exposes **no** `*newpayload*` series. Closest available:

| Metric | Notes |
|---|---|
| `rpc_duration_all` | p50 ≈ 23 µs units as published by geth summary; **not** method-scoped to `engine_newPayloadV4` |
| `engine_getblobs_*` | present, idle (0) |
| `rpc_duration_eth_syncing_success` | upcheck path only |

**Recorded:** independent geth `newPayload` method timing was **not** available from `--metrics` under this compose EL image; EL-side reference for (2) is deferred to a geth build that exports per-method `engine_newPayload*` timers (CC-39 /7 surface) or to the acceptance window.

---

## Comparison baselines — labelled, never adopted (CC-3C /3)

| Source | Figure | Label |
|---|---|---|
| reth published means | 42.9 ms → 32.4 ms | **reth, on mainnet, with a different state backend** |
| reth published P90s | 72.4 ms → 53.1 ms | same |
| geth issue [#28317](https://github.com/ethereum/go-ethereum/issues/28317) | `engine_newPayload` blocking **over a minute** during database compaction | reason the Engine API timeout is **8 s**, not 1 s |

**An order of magnitude, not a target.**  
**Tail, not mean, is what breaks the slot.**

The criterion for this issue is a measurement **recorded**, never a bar **hit**.

---

## CC-3C /4 — mid-gate side-by-side (with engine wired)

**Command × 3 (engine wired in tree via CC-32b):**

```text
cargo nextest run -p cc-chain -E 'test(cc1h_mid_gate_with_fork_choice_clone)'
```

**The mid-gate path does not call `newPayload`.** It is `process_slots` + state clone + `process_justification_and_finalization` from the Hoodi anchor. "With engine wired" means the **codebase** has the production `EngineApiClient` seam (CC-32b); the mid-gate test itself still uses no EL. The deliverable is the **margin** vs G-A, not a pass — though all three runs **passed** the 1000 ms bar on this machine.

### With engine (this issue)

| Run | max_wall_ms | mean_wall_ms | mean_hash_share% | mean_fc_clone_ms | result |
|-----|-------------|--------------|------------------|------------------|--------|
| 1 | 608.10 | 593.20 | 24.8 | 58.49 | PASS |
| 2 | 629.03 | 605.08 | 24.8 | 58.56 | PASS |
| 3 | 605.52 | 584.35 | 25.1 | 55.99 | PASS |

Epoch series (run 2, representative):

| epoch | from → to | wall_ms | hash_ms | hash% | fc_clone_ms |
|---:|---|---:|---:|---:|---:|
| 0 | 3649504 → 3649536 | 598.88 | 150.45 | 25.1 | 67.72 |
| 1 | 3649536 → 3649568 | 594.69 | 150.92 | 25.4 | 55.94 |
| 2 | 3649568 → 3649600 | 629.03 | 149.99 | 23.8 | 56.98 |
| 3 | 3649600 → 3649632 | 604.91 | 150.30 | 24.8 | 55.88 |
| 4 | 3649632 → 3649664 | 597.88 | 148.98 | 24.9 | 56.26 |

### Side-by-side summary

| Column | max of max_wall_ms | mean of mean_wall_ms | mean hash share | mid-gate bar (1000 ms) |
|---|---:|---:|---:|---|
| **Without engine (G-A)** | 905.93 | 723.48 | ~21.4 % | PASS × 3 |
| **With engine wired (CC-3C)** | 629.03 | 594.21 | ~24.9 % | PASS × 3 |
| **Margin (with − without)** | **−276.90** | **−129.27** | +~3.5 pp | both closed |

**Plain statement:** the mid-gate test **passes** on this machine under the with-engine tree; the PRD/Architecture "fails 3/3" snapshot is **stale** relative to G-A and this re-run. The **deliverable is the margin**, not a pass: with-engine mid-gate walls are **no worse** than G-A (slightly better under the concurrent load of this session). Because mid-gate never enters `process_execution_payload`, a well-behaved ~50 ms `newPayload` tail is **not** folded into these walls — that cost only appears on the import path.

---

## Trigger A discriminator (computed, not eyeballed)

§6.4: Trigger A is the exclusive number **inside** 400 ms while the inclusive number is **outside**.

| Quantity | 400 ms boundary | Source |
|---|---|---|
| `p95(cc_chain_process_block_local_seconds)` | **not measured** (live seed only; mid-gate does not observe this family) | chain metrics / import path |
| `p95(cc_chain_process_block_seconds)` | **not measured** (same) | chain metrics / import path |
| Mid-gate epoch wall (proxy for process_epoch class) | **inside** 1000 ms bar (max 629 ms with engine; max 906 ms G-A) | this document |

**Which side of 400 ms:** neither process_block p95 is available as a real quantile. The Trigger A signature — local ≤ 400 ms **and** inclusive > 400 ms — is **not** observed. Mid-gate has no engine call, so local ≡ inclusive for that path by construction.

---

## Verdict

**Trigger B** — `CC-1H` is **not** the answer on this evidence: mid-gate is closed with and without the engine (max wall **629 ms** with / **906 ms** G-A, both under the 1000 ms bar), hash share is ~**25 %** of epoch wall with steady `hash_ms` ≈ 150 ms (no exclusive-inside / inclusive-outside split). The Trigger A discriminator on `p95(process_block_local)` vs `p95(process_block)` against **400 ms** was **not computed as quantiles** (series seed-only / absent from mid-gate); A is ruled out by absence of the A signature on the measured path, not by placing two measured p95s on either side of 400 ms. This is a **provisional** B for G-B schedule pressure: if a later exclusive import path shows exclusive ≤ 400 ms and inclusive > 400 ms, revise to A. Promoting `CC-1H` under the present shape would spend a large increment on the wrong problem (hash/clone path debt stays Phase 1's, not a Phase 3 engine-budget buy-back).

**Trigger C (not yet decidable — re-read at M3.5 exit)** remains open for the production soft-deadline ratio over the acceptance window (`CC-3Ac`).

---

## OQ-P3-8 — is the `chain→engine` gRPC hop worth revisiting topology?

| Item | Value |
|---|---|
| Process-local residual (1)−(2)−(3) offline proxy | p50 **0.05 ms** / p95 **0.07 ms** — **setup noise, not gRPC** |
| Live histogram hop from `cc_chain_engine_call_seconds` − request − encode | **not available** (all three families seed-only) |
| Topology revisit on **latency** alone? | **No** — residual is noise-scale; even a multi-ms true hop is small next to EL tails and epoch work |
| Settled by this residual? | **No** — do not treat 0.05 ms as proof the umbrella hop is cheap |

**A-P3-7 counter-argument (named so this is not a pure latency answer):** linking the Engine API client into `chain` would put the project's **first credential (JWT)** and an **HTTP client** inside the consensus process. That security/process-boundary argument is independent of hop latency and is the standing reason to keep the engine service separate until a **live** hop is measured. **Revisit topology only with non-seed (1)/(2)/(3) histograms in hand**; today's process-local residual neither justifies collapsing the umbrella nor fully closes the latency half of OQ-P3-8.

---

## How to re-run

```text
# Encode on real Hoodi payload (optional: CC_3C_HOODI_BLOCK_SSZ=/path/to/block.ssz)
cargo nextest run -p cc-engine -E 'test(encode_real_hoodi_payload)' --nocapture

# Offline span arithmetic (wiremock EL)
cargo nextest run -p cc-engine -E 'test(latency_spans_wiremock_new_payload)' --nocapture

# Mid-gate × 3 (with engine wired in tree)
cargo nextest run -p cc-chain -E 'test(cc1h_mid_gate_with_fork_choice_clone)' --nocapture

# Bucket comparability on a live chain metrics port
curl -s localhost:9101/metrics | grep -c 'cc_chain_process_block_local_seconds_bucket'
curl -s localhost:9101/metrics | grep -c 'cc_chain_engine_call_seconds_bucket'
curl -s localhost:9101/metrics | grep -c 'cc_chain_process_block_seconds_bucket'
```

---

## Out of scope (recorded)

- Acting on the verdict (G-B at M3.4 entry).
- Optimising `canonical_root()` (Phase 1 debt).
- New metric families (declared at CC-3Aa; observations owned elsewhere).
- SSZ Engine API containers (CC-3H).
- Grafana (CC-3G).
- Acceptance-window Trigger C numbers (CC-3Ac).
