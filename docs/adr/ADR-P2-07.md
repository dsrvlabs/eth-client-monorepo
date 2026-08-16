# ADR-P2-07 — Column-family scoring weight applies regardless of `cgc`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-21d, CC-22c
- **Citations:** 7 sites — `services/p2p/src/discovery/cgc_hook.rs:32-33,249-258`; `services/p2p/src/gossip/scoring.rs:69-70,97-99,124-128,568-574,729-730`; `docs/p2p-scoring.md:54-55`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Gossipsub topic scores are weighted per family. The column family
(`data_column_sidecar_{id}`) is special: the number of live topics equals
`sampling_size`, which moves with `cgc` via `set_custody_group_count`. If
each column topic kept a fixed weight, raising `cgc` would raise the
family's contribution to `max_positive_score` and warp every other
family's relative weight. If the family total moved with `cgc`, a supernode
and a `cgc = 4` node would not be in the same score space.

Phase 2 pinned the **family total** at `WEIGHT_COLUMN_FAMILY = 0.5` and
spread it: per-topic weight is `0.5 / sampling_size` (`scoring.rs:69-70,124-128`).
`family_weight_sum()` counts the column family **once** as 0.5, independent
of `sampling_size` (`scoring.rs:97-99`). The `cgc` hook recomputes that
per-topic weight and `set_topic_params` for every live column topic
(`cgc_hook.rs:6-7,32-33,249-258`). Which concrete topics receive the
params is the sparse custody set, not the dense prefix `0..sampling_size`
(`scoring.rs:568-574`). `docs/p2p-scoring.md` is generated from the same
struct and restates the invariant.

This record is the family-total invariant. Topic P3 / P3b weights staying
**0** is ADR-P2-10 (a (b) row; not this file).

## Decision

**The column-family total topic weight is 0.5 at every `cgc` /
`sampling_size`.**

Per-topic column weight is `0.5 / sampling_size` (`sampling_size.max(1)`).
`set_custody_group_count` must recompute that weight and apply it to every
live / to-be-live column topic **before** subscribe. Assert
`column_family_total(sampling_size) ≈ 0.5` (epsilon `1e-12`). Do not let
`cgc` change `family_weight_sum()`.

## Consequences

What this makes easy:

- A `cgc` raise does not retune `TopicScoreCap` or P6/P7 weights that are
  derived from `max_positive_score`.
- The generated scoring doc and `column_family_total_is_half` are mechanical
  checks of one number.

What this makes hard:

- Per-topic column weight is derived, not a table literal. A reviewer who
  greps for `0.0625` is looking at `sampling_size = 8`, not at the decision.
- Changing `WEIGHT_COLUMN_FAMILY` is a global score-space change, not a
  column-only tweak.

What this forbids:

- A fixed per-column topic weight that makes the family total track `cgc`.
- Counting `0.5 * sampling_size` in `family_weight_sum()`.
- Applying new column `topic_params` after subscribe, or applying them to
  the dense prefix instead of the sampled set.

## Alternatives considered

**Fixed per-topic column weight (e.g. 0.5 / 8 always).** Rejected. At
`cgc` 128 the family would dominate `max_positive_score`; at `cgc` 4 it
would under-weight columns relative to `beacon_block`.

**Family total scales with `sampling_size`.** Rejected. It makes
`max_positive_score` a function of local custody and breaks cross-node
score comparability.

## Refactor impact

**Survives.** P0-17a / S3 may write non-zero P3/P3b weights (ADR-P2-10);
that does not license moving the column family total. `cgc` hook step 2
stays "recompute `0.5 / sampling_size`, then `set_topic_params`".
