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

**Date:** 2026-08-07  
**Artifact:** `comptests.tar.gz` (pinned digest
`a4319cd2e0253433022b3261ed53ee20c3bccb33517454ddce1a895280ac4e4e` in
`spec-vectors.lock`, tag `v1.7.0-alpha.13`).  
**Method:** list archive contents; inspect Fulu fork-choice material against
the Clause 1 suite table (`mainnet.tar.gz` / `minimal.tar.gz` →
`tests/<preset>/fulu/fork_choice/…`).

### Directories inspected

| Path in archive | Kind |
|---|---|
| `tests/minimal/fulu/fork_choice_compliance/` | **Pre-generated vector cases** (SSZ-snappy + `steps.yaml`) |
| `tests/minimal/{altair,bellatrix,capella,deneb,electra,gloas}/fork_choice_compliance/` | Same suite for other forks (out of Phase 1 Fulu scope) |
| `tests/formats/fork_choice/` | Shared step-format docs (same as standard `fork_choice`) |
| `tests/generators/compliance_runners/fork_choice/` | Generator sources (MiniZinc models, yaml configs) |
| `tests/core/pyspec/…/test/fulu/fork_choice/` | Pyspec *source* generators only (not runnable vectors) |
| `tests/formats/fast_confirmation/` + phase0 pyspec | **Not** Fulu fork-choice (separate format; no Fulu emission) |

No `tests/mainnet/**/fork_choice_compliance` paths exist in this artifact
(minimal preset only).

### Fulu emission under `fork_choice_compliance`

Handlers and case counts (each case is a `pyspec_tests/<name>/` directory with
`steps.yaml`, `anchor_state.ssz_snappy`, `anchor_block.ssz_snappy`, and step
payloads):

| Handler | Cases |
|---|---:|
| `attester_slashing_test` | 128 |
| `block_cover_test` | 192 |
| `block_tree_test` | 512 |
| `block_weight_test` | 256 |
| `invalid_message_test` | 128 |
| `shuffling_test` | 256 |
| **Total Fulu** | **1472** |

**Case shape (for CC-15c's runner):** identical to standard
`fork_choice` format (`tests/formats/fork_choice/README.md`): ordered
`steps.yaml` with keys `tick`, `block` (+ optional `valid`), `attestation`,
`attester_slashing`, and `checks` (head / justified / finalized /
`proposer_boost_root` / time). Sample step keys observed across handlers:
`tick`, `block`, `attestation`, `attester_slashing`, `checks`. No Fulu-only
step kind beyond what the mainnet/minimal `fork_choice` runner already must
dispatch.

### Verdict

**In scope for CC-15c's runner — Clause 1's suite table gains a row.**

These are **additional** fork-choice cases the standard
`tests/<preset>/fulu/fork_choice/…` walk (from `mainnet.tar.gz` /
`minimal.tar.gz`) will **not** cover: different handler path
(`fork_choice_compliance` vs `fork_choice`), different generation method
(constraint-based compliance generator), and they live only in
`comptests.tar.gz`. They are **not** mere duplicates of the pyspec
`fork_choice` suite.

**Consequence for Clause 1 / CC-15c:**

| Suite | Artifact | Preset(s) | Owner | Notes |
|---|---|---|---|---|
| `fork_choice` | `mainnet.tar.gz`, `minimal.tar.gz` | both | CC-15c | existing Clause 1 row |
| **`fork_choice_compliance`** | **`comptests.tar.gz`** | **minimal only** | **CC-15c** | **new row — same step runner, different suite root** |

`fork_choice` may not be declared green (CC-15c) before this section is
honoured: the runner must either execute `fork_choice_compliance` under Fulu
minimal or explicitly skiplist it with a reviewed owner. Recommended path:
one runner implementation, two suite roots
(`…/fulu/fork_choice` and `…/fulu/fork_choice_compliance`).

## CC-1H mid gate

*(CC-18d — empty until filled.)*

## Run record

*(CC-1Ac skeleton / CC-1Ad numbers — empty until filled.)*

## Timing

*(CC-1Ac skeleton / CC-1Ad numbers — empty until filled.)*
