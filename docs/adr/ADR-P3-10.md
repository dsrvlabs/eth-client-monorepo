# ADR-P3-10 — Optimistic status is proto-array bookkeeping only

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-08, CC-34a, CC-34c, CC-3B
- **Citations:** 7 sites — `crates/fork-choice/src/execution_status.rs:3-5,72-76,730-733`; `crates/fork-choice/src/proto_array.rs:149-151,735-738`; `services/chain/src/metrics.rs:304`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Optimistic sync needs to know which blocks are `NOT_VALIDATED` and
whether the *node* is optimistic. A parallel `HashSet<Root>` of
optimistic blocks is the obvious cache — and a second source of truth
that leaks across finalization, diverges on invalidation, and cannot
answer "is the fork choice itself optimistic when every viable branch
is INVALIDATED?"

CC-34 /9 is discharged by **not having that set**.

## Decision

`ProtoNode.execution_status` is the **single** source of truth for
optimistic bookkeeping (`execution_status.rs:3-5`). There is **no
parallel optimistic-root set**.

- `is_optimistic(store, root)` is derived from the node's
  `execution_status == Optimistic`. It consults **no** parallel set
  and returns `None` if the root has no proto-array node
  (`execution_status.rs:72-82`).
- `optimistic_node_count()` is a scan of the proto-array
  (`proto_array.rs:735-748`). `cc_chain_optimistic_nodes` reads that
  scan (`metrics.rs:304`).
- After many optimistic imports and finalization, optimistic state
  lives only in the **finalization-pruned proto-array**. The test
  `no_unbounded_optimistic_structure` is the leak check
  (`execution_status.rs:730-733`).

Node-level `is_optimistic_node` is also derived (CC-34c), not stored:

```text
is_optimistic_node()  ≜  is_optimistic(head_root)   // branch 1
                      ||  !any_viable_branch()       // branch 2
```

Branch 2 is the one implementations forget: a tree with no viable
branch is optimistic even when `find_head` cannot name a head
(`execution_status.rs:84-115`).

`ExecutionStatus` itself is four values (`Valid` / `Invalid` /
`Optimistic` / `Irrelevant`), mapped from the five-value seam enum
(ADR-P3-04) by `from_payload_status`.

## Consequences

What this makes easy:

- Finalization prune *is* optimistic GC. There is nothing else to
  leak.
- Invalidation (ADR-P3-11) flips `execution_status` and the derived
  predicates follow.

What this makes hard:

- Occupancy is O(n) in proto-array length. CC-3C's numbers would
  justify an index; any such index must be a **pure derivation** of
  the node field (see `invalidation.rs:11-17` for the same rule on
  execution hashes).

What this forbids:

- A sidecar `HashSet` / bitmap / metric-only cache of optimistic
  roots that can disagree with `ProtoNode.execution_status`.
- Treating "no head" as "not optimistic" (drops branch 2).
- Persisting a parallel optimistic set in `cc-store`.

## Alternatives considered

**Parallel optimistic-root set, updated next to proto-array.**
Rejected: CC-34 /9 and the citing module exist so there is nothing
to leak. Two writers is the bug.

**Store `is_optimistic_node` as a flag on `Store`.** Rejected: it
would desync on invalidation / prune. Derive it after `get_head`.

None further recorded.

## Refactor impact

**Survives.** S2's durable seed stores proto-array / scalars
(ADR-P4-06), not a second optimistic set. Metric reshape (§7.3)
keeps `cc_chain_optimistic_nodes` as a derived gauge.
