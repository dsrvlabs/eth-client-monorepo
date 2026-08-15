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
| R-13 (b) | pending (`S0-A-31`); **not** discharged by E0.4 |
