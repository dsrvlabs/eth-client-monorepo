# Phase 1 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section.

| Section | Owner |
|---|---|
| `## CC-1H early gate` | CC-13d |
| `## OQ-3 — comptests` | CC-15a |
| `## CC-1H mid gate` | CC-18d |
| `## Run record` | CC-1Ac skeleton / CC-1Ad numbers |
| `## Timing` | CC-1Ac skeleton / CC-1Ad numbers |

## CC-1H early gate

**Date:** 2026-08-07  
**Machine:** Apple M4 Pro, 24 GB RAM, macOS aarch64 (dev machine)  
**Method:** `process_slots` across **5** epoch boundaries from the committed
Hoodi anchor state (slot `3649472`, state root
`0x2d4f2b8d81bcb72c3556846a532fca7679e9349b0dc37c9c0812f383daa41c55`), empty
slots only. Cache warmed with one `canonical_root()` before measurement.
Hash share = wall time spent inside `measured_canonical_root` /
epoch wall time.

**Command:**

```text
cargo test -p cc-state-transition --test cc1h_early_gate -- --ignored --nocapture
```

| epoch | from → to | wall (ms) | hash (ms) | hash share | root calls |
|---:|---|---:|---:|---:|---:|
| 0 | 3649472 → 3649504 | 689.91 | 52.15 | 7.6 % | 32 |
| 1 | 3649504 → 3649536 | 522.69 | 143.26 | 27.4 % | 32 |
| 2 | 3649536 → 3649568 | 524.27 | 143.53 | 27.4 % | 32 |
| 3 | 3649568 → 3649600 | 530.35 | 144.92 | 27.3 % | 32 |
| 4 | 3649600 → 3649632 | 533.97 | 142.26 | 26.6 % | 32 |

| Aggregate | Value |
|---|---|
| max wall | **689.91 ms** |
| mean wall | 560.24 ms |
| mean hash share | **23.3 %** |
| max hash share | 27.4 % |

### Threshold verdict

**&lt; 700 ms** (max epoch wall 689.91 ms). Proceed with flat-struct state backing.
CC-1H remains a P2 contingency and is **closed for the early gate**.

### Attribution verdict

**&lt; 25 % hashing** (mean 23.3 %). The epoch figure is transition-side arithmetic
dominant, not `canonical_root()`-bound. **CC-1H must not be triggered** on this
number — milhouse would not address the cost. If later gates regress, respond
with a per-handler profile plus CC-1I batching rather than a state-backing swap.

### Explicit decision

**CC-1H is not promoted.** Early gate **closed**; re-measure only at the mid
gate (CC-18d) if the mid-gate path is exercised, or if R-3 early-warning
signals change. M1.2 may exit without landing CC-1H.

## OQ-3 — comptests

*(CC-15a — empty until filled.)*

## CC-1H mid gate

*(CC-18d — empty until filled.)*

## Run record

*(CC-1Ac skeleton / CC-1Ad numbers — empty until filled.)*

## Timing

*(CC-1Ac skeleton / CC-1Ad numbers — empty until filled.)*
