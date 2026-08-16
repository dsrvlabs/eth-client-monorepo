# ADR-P3-11 — Invalidation weight removal reuses `remove_invalidated_subtree_weight`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-08, CC-34a, CC-35a, CC-35b
- **Citations:** 5 sites — `crates/fork-choice/src/invalidation_walk.rs:17-19,291-304`; `crates/fork-choice/src/invalidation.rs:8-9`; `crates/fork-choice/src/execution_status.rs:7-13,117-136`; `crates/fork-choice/src/proto_array.rs:633`; numeric assertion at `invalidation_walk.rs:954`
- **Provenance:** re-derived from code (2026-08-16)

## Context

§4.4 weight handling is two parts, and **only both together** are
correct (`execution_status.rs:7-13`):

1. At invalidation time, subtract `invalidBlock.weight` (read
   **before** any zeroing) **once** from each strict ancestor of the
   subtree root, then zero the subtree.
2. Suppress future upward propagation for `Invalid` nodes inside
   `ProtoArray::apply_score_changes`.

Walking strict ancestors from *every* node in the subtree
over-subtracts by the subtree's node count. That is the trap
`remove_invalidated_subtree_weight` is written to avoid
(`execution_status.rs:128-132`). A second, walk-local copy of the
arithmetic would drift from the unit tests that prove both halves.

The backwards walk (CC-35b) already knows the oldest invalidated
root. It should call the same function once, not invent a second
zeroing loop.

## Decision

The invalidation walk **reuses**
`remove_invalidated_subtree_weight` (`invalidation_walk.rs:17-19,291-304`).

After the parent-ward walk and descendant pass, weight removal runs
**once** on the oldest invalidated root (`subtree_root`). That call
zeros the whole subtree and subtracts the pre-zero weight from each
strict ancestor. CC-35 /7 proves both halves of §4.4 together after
a completed walk.

`invalidation.rs` (CC-35a, the three `latestValidHash` cases) uses
the same function for its descendant invalidation
(`invalidation.rs:8-9`). There is one implementation.

Part 2 stays in `apply_score_changes`: Invalid nodes contribute
zero upward and hold weight (`proto_array.rs:633`). The walk does
not reimplement that.

Sibling branches of `invalidBlock` are never touched
(`invalidation.rs:5`).

## Consequences

What this makes easy:

- One numeric invariant, one function, tests on both the walk and
  the LVH cases (`invalidation_walk.rs:954`).
- Future vote/boost deltas cannot resurrect an invalidated subtree
  (part 2).

What this makes hard:

- A "simpler" walk that zeros nodes inline is a defect even if the
  numbers look right on a small fixture — it will over-subtract on
  a deep subtree.

What this forbids:

- A second weight-removal implementation in the walk, in chain
  service, or in a "quick invalidate this head" helper.
- Subtracting each invalidated node's weight from ancestors
  (the over-subtract trap).
- Zeroing *before* reading `invalidBlock.weight`.

## Alternatives considered

**Inline zeroing in the walk, keep `remove_invalidated_subtree_weight`
for CC-35a only.** Rejected: the citing comment exists so both
halves are proven together after a completed walk.

**Subtract per-node weights up the ancestor chain.** Rejected:
`invalidBlock.weight` is already the subtree total; per-node
subtraction over-counts.

None further recorded.

## Refactor impact

**Survives.** S1/S2 do not move proto-array arithmetic. A later
invalidation-from-fcU wrapper must call this function (or
`apply_invalidation`) once, with floors — not dual-call it
(`invalidation_walk.rs:27-28`).
