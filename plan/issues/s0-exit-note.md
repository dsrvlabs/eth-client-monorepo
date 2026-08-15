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

## Other S0 exit criteria

| # | Status |
|---|---|
| E0.1 | pending / carried from S0a |
| E0.2 | pending this note (landed work is `S0-A-09`) |
| E0.3 | pending (`S0-A-11`) |
| **E0.4** | **recorded above — confirms a live ship-blocker** |
| E0.5 | pending (`S0-A-32`) |
| E0.6 | pending (`S0-A-25`, `S0-A-33`) |
| E0.7 | pending (`S0-B-19`) |
| E0.8 | pending (`S0-B-20`) |
| E0.9 | pending (`S0-B-18`) |
| R-13 (b) | **recorded above — restore reaches `end_stream`; /22 and /23 are independent** |
