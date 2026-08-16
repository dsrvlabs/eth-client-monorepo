# ADR-P1-12 — State residency is four pinned roles plus a 64-block body ring

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, CC-18b
- **Citations:** 2 sites in the §10.4 census — `services/chain/src/residency.rs:1,8-18,32-35`; prune surface at `crates/fork-choice/src/store.rs:514-519`; carried at `plan/issues/s2-fold-storage.md` S2-A-01
- **Provenance:** re-derived from code (2026-08-16)

## Context

A proto-array that can name a head is useless if the post-state is gone
and cannot be replayed inside the slot budget. Keeping every post-state
is an OOM. The store therefore needs an explicit residency policy:
which states stay pinned, how a shallow reorg reconstructs the rest,
and what happens when the gap is too wide.

## Decision

In-memory residency is **four named pinned roles** plus a **64-block
body ring** (`residency.rs:1,32-35`).

Pinned roles (max 4 by default; `chain.max_resident_states`):

| Role | Why |
|---|---|
| head | every import's parent, pulled-up tip, query |
| anchor / finalized | replay origin of last resort |
| previous epoch-boundary | Phase 6 target lookups; shallow-reorg origin |
| scratch | in-flight import working copy |

Adding a fifth requires naming a fifth role (`residency.rs:18`).

The body ring (`DEFAULT_BODY_RING_CAPACITY = 64`) retains recently
imported bodies for shallow-reorg replay. A root that is neither
resident nor re-derivable from the ring is a `ReorgGap`, counted, not
a panic (`residency.rs:57-63,86-87`).

`Store::retain_block_states` is the prune surface: keep at most the
pinned roles; **headers and proto-array stay** so fork-choice identity
is intact (`store.rs:514-519`). Restoring a known state via
`put_block_state` does not bump `mutation_counter`.

Invariant: every proto-array node that can become head has a post-state
that is either resident or re-derivable by replay from a resident
ancestor within the slot budget (`residency.rs:5-7`).

## Consequences

What this makes easy:

- Import and `get_head` run against a bounded RAM budget.
- Shallow reorgs replay from the ring instead of checkpoint-sync.

What this makes hard:

- A reorg deeper than 64 blocks (or past the pinned ancestor) is a
  recorded gap. Callers must not assume every proto-array root has a
  live `BeaconState`.

What this forbids:

- An unbounded `block_states` map as the residency policy.
- Dropping headers / proto-array nodes to "free" a state.
- Adding an anonymous fifth resident slot without naming a role.

## Alternatives considered

**Keep every post-state.** Rejected: mainnet states are 150–200 MB; the
citing module exists to bound them.

**Pin only head + finalized.** Rejected: epoch-boundary and scratch are
named because import and Phase-6 lookups need them; stuffing those into
"head" hides the budget.

**Replay from durable storage instead of a body ring.** That is a later
owner (Phase 4 / S2 `StateProvider`). The Phase-1 ring stays until a
typed durable path actually serves the same invariant.

## Refactor impact

**Survives; interacts with [q2].** S2 must carry this ADR intact
(`S2-A-01`). Milhouse changes *how* a state is cloned, not *which*
states stay resident. A durable `StateProvider` may reimplement
`get_state`; it does not delete the four roles or the "re-derivable or
gap" rule.
