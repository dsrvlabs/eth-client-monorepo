# S0 exit note

Living record for the S0 exit criteria. Later issues append; they do not rewrite
prior conclusions.

---

## E0.4 — R-11 executed (`S0-A-30`)

**Conclusion: confirms a live ship-blocker.**

Executed 2026-08-16 against the committed Hoodi pin
(`crates/types/tests/fixtures/hoodi-anchor.toml`, slot `3649472`) via the
S0a-A-01 harness (`crates/state-transition/tests/support/anchor.rs`).
Command:

```text
HOODI_FIXTURES_CACHE=$HOME/.cache/cc-hoodi-fixtures \
  cargo test -p cc-state-transition --test r11_process_block \
  hoodi_decoded_state_process_block_roundtrip -- --nocapture
```

Wall ~7.0 s. Result:

| Arm | Constructor | `process_block` |
|---|---|---|
| Committed pair (same-slot post-state + pin block) | `load_anchor` → `from_ssz_bytes_with` | `BlockError::BlockSlotNotNewer` |
| Successor of the decoded Hoodi state, cache empty | same raw decode | **`BlockError::CachePoisoned`** |
| Same successor, cache filled | `from_ssz_bytes_hydrated` | **Ok** |

The pin is a post-state pair (CC-18d): `state.slot == block.slot`, so
re-applying `signed_beacon_block.ssz` dies in `process_block_header` and never
reaches `process_sync_aggregate`. That is fixture topology, not a downgrade of
P0-19. Restore and replay apply *subsequent* blocks to an SSZ-decoded state —
the successor arm.

The successor is a valid next-slot block (empty operations, infinity
sync-aggregate, `AcceptEngine`) built from the decoded Hoodi registry and
`current_sync_committee`. `process_sync_aggregate` resolves all 512 real
committee pubkeys through `PubkeyIndexMap` before reading bits ([Q3] §5), so
an empty cache is sufficient to fire `CachePoisoned`.

**Do not quote P0-19's severity outside this repo as merely a code-path
trace.** It has now been executed on a real Hoodi decoded state.

Do **not** infer P1-A/22 or P1-A/23 attribution from this result. That is
R-13 observation (b) / `S0-A-31`.

---

## R-13 (b) — instrumented restore (`S0-A-31`)

**Conclusion: restore reaches `end_stream`. P0-19 is real *and* P1-A/22 and
P1-A/23 are independent bugs. Escalate for re-disposition** to
`patch @ S0 → deleted @ S2` **by explicit decision** (`[PRD]` §6). Deleting
them at S2 without that decision is the wrong call.

Executed 2026-08-16 against the committed Hoodi pin
(`crates/types/tests/fixtures/hoodi-anchor.toml`, slot `3649472`,
`beacon_state.ssz` 205_205_311 B, 1_455_439 validators) via
`handle_restore_accumulated` — the same `RestoreGate` + `apply_restore_set` +
`end_stream` path as `RestoreFromStore`. Replay set: one successor of the pin
(slot `3649473`, empty operations, infinity sync-aggregate), streamed twice —
`DEFERRED` then `AVAILABLE` — so the DA-deferred drop and `process_block` are
both on the path. Command:

```text
HOODI_FIXTURES_CACHE=$HOME/.cache/cc-hoodi-fixtures \
  cargo test -p cc-chain --features s0-a-31-observe --test r13_restore_observation \
  hoodi_restore_observation_where_it_fails -- --nocapture
```

`--features s0-a-31-observe` compiles the raw-decode arm and the in-process
trace. Production `apply_restore_set` (feature off) decodes only through
`from_ssz_bytes_hydrated`. Wall ~14.5 s (load 4.9 s + hydrated 4.9 s + raw
4.6 s). Instrumentation targets: decode, `on_block`, `RestoreGate::end_stream`,
DA-deferred drop.

| Arm | Decode | `on_block` `DEFERRED` | DA-deferred drop | `on_block` `AVAILABLE` / `process_block` | `end_stream` |
|---|---|---|---|---|---|
| Production (hydrated) | `hydrated=true`, cache=`1455439` | `Deferred(DataUnavailable)` | **reached** | **reached** — `state root mismatch` (synthetic successor) | **reached** |
| Raw (omit top-up) | `hydrated=false`, cache=`0` | `Deferred(DataUnavailable)` | **reached** | **reached** — **`cache poisoned`** | **reached** |

`on_block` checks DA **before** `state_transition` (`on_block.rs` step 1). A
stored-`DEFERRED` restore block therefore hits the P1-A/23 drop **without**
`process_block` and **without** `CachePoisoned`. The Available successor still
runs `process_block`: raw decode reproduces P0-19 on the restore path; hydrated
decode passes the cache and dies later at the post-state root check. In both
arms `RestoreInFlightGuard` still calls `end_stream`. Two `end_stream` rows
per arm are the two `Drop` guards (`handle_restore_accumulated` +
`apply_restore_set_blocking` join), not two applies.

This is not inferable from `S0-A-30`. That issue only ran standalone
`process_block` on a decoded Hoodi state. It never entered `RestoreGate`, never
called `end_stream`, and never exercised the DA-first restore loop that reaches
the deferred drop.

P1-A/22 and P1-A/23 are **not patched here** (`deleted @ S2`, `[PRD]` R-8).
`end_stream` already `notify_waiters()` (S0-A-28); /22 independence is *the
site is reached after `process_block` fails*, not a fresh reproduction of the
bootstrap hang. /23 remains the unused `deferred_roots` re-drive gap.
`S2-J-02` must not delete them on a "P0-19 was the common cause" claim.

---

## E0.7 — §9.0 A/B baseline (`S0-B-19`)

**Conclusion: S0 baseline recorded. Topology A == Topology B == this commit.
Same-commit T0 vs T≥1h is not the dangerous case.**

Executed 2026-08-15 against commit `e2ad297072d07ab00cb328e5e12549d9c210ba8d`
(`feature/s0-b-19-ab-baseline`). S0 moves nothing ([ARCH] §9.1); both
topologies are the same six-binary stack. Procedure still followed: `make build`
(all six `target/debug/cc-*` present), six-service compose
`COMPOSE_PROJECT_NAME=s0b19-ab-baseline` + self-devnet `cc-devnet` against the
same 64-slot fixture, wall clock **3632 s** (explicit `--wait 3610` plus scrape
overhead). EL `service_started` (ADR P3-14); EL health stayed `unhealthy`
(Hoodi snap). Raw record:
[`docs/s0-e07-ab-baseline.txt`](../../docs/s0-e07-ab-baseline.txt).

| Clock | UTC | unix |
|---|---|---|
| T0 (after healthy) | 2026-08-15T17:35:01Z | 1786815301 |
| T≥1h | 2026-08-15T18:35:33Z | 1786818933 |

Six services healthy for the whole window (`chain`, `p2p`, `attestation`,
`engine`, `beacon-api`, `storage`). Self-devnet publisher published 64
`beacon_block`s; node-a and node-b each accepted 64. Mesh stayed up after the
fixture ended.

**Family 1** `cc_chain_import_total{result=*}` on `:9101` (there is no
`cc_chain_import_result` series). T≥1h baseline. The six-service stack was
**not** on the self-devnet mesh: these zeros (and Family 2 histogram seed
count=1) are an **idle/unwired S0 baseline**. E1 p2p→chain is dead; the
self-devnet is p2p-only. Later stages must **not** treat this as a loaded
import or head-lag distribution to match.

| result | T0 | T≥1h |
|---|---:|---:|
| imported | 0 | 0 |
| duplicate | 0 | 0 |
| deferred | 0 | 0 |
| deferred_engine | 0 | 0 |
| unknown_parent | 0 | 0 |
| invalid | 0 | 0 |

**Family 2** `cc_p2p_head_lag_slots_bucket` on six-service `:9102` and
self-devnet node-a `:19112`, plus `cc_chain_head_lag_slots` on `:9101`. Every
histogram bucket is the seed observation **1** at T0 and T≥1h; gauge is **0**.
`cc_p2p_head_lag_slots_count=1` (seed only; no further `observe`).

**Family 3** §2.2 overflow (exact names found). All **0** at T0 and T≥1h:

| series | T≥1h |
|---|---:|
| `cc_chain_import_rejected_backpressure_total` | 0 |
| `cc_chain_event_publish_dropped_total` | 0 |
| `cc_chain_da_pending_dropped_total` | 0 |
| `cc_chain_pending_engine_dropped_total` | 0 |
| `cc_engine_fastpath_dropped_total` | 0 |
| `cc_engine_fcu_dropped_stale_total` | 0 |
| `cc_storage_shard_dropped_total{class=*}` (5 classes) | 0 |
| `cc_storage_writer_chunk_dropped_total{class=*}` (8 classes) | 0 |
| `cc_chain_subscribers` | 0 |

No `*terminated*` series and no `cc_grpc_requests_total{code="8"}`
(RESOURCE_EXHAUSTED) series existed on any scrape.

T0 vs T≥1h: family1_nonzero_delta=0, family2_nonzero_delta=0,
family3_nonzero_delta=0. **dangerous_case=no** — family 3 did not move, so
§9.0/4 (family 3 nonzero while 1–2 stay zero) did not fire. Later stages
(`S1-A-19` …) diff against the T≥1h absolute numbers, not against this T0
delta — and must not treat those Family 1 zeros / Family 2 seed count=1 as a
loaded import or head-lag distribution (six-service stack was not on the
self-devnet mesh; E1 p2p→chain is dead; self-devnet is p2p-only).

---

## Other S0 exit criteria

| # | Status |
|---|---|
| E0.1 | pending / carried from S0a |
| E0.2 | pending this note (landed work is `S0-A-09`) |
| E0.3 | pending (`S0-A-11`) |
| **E0.4** | **recorded above — confirms a live ship-blocker** |
| E0.5 | pending (`S0-A-32`) |
| E0.6 | pending (`S0-A-25`, `S0-A-33`) |
| **E0.7** | **recorded above — S0 A/B baseline (same-commit T0 vs T≥1h)** |
| E0.8 | pending (`S0-B-20`) |
| E0.9 | pending (`S0-B-18`) |
| R-13 (b) | **recorded above — restore reaches `end_stream`; /22 and /23 are independent** |
