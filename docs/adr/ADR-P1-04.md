# ADR-P1-04 — Cached state-root path on `BeaconState`

- **Status:** proposed · revisit at S4a · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 1 (CC-1H seam; milhouse is S4a)
- **Issues:** S1-B-16, S4a-08
- **Citations:** 1 site — `crates/types/src/state/accessors.rs:987-1003` (`commit` at `:983-988`)
- **Provenance:** re-derived from code (2026-08-16) — records the live cached path as an R-17 deferral; milhouse supersedes it at S4a

This is **ADR-P1-04**. `[ARCH]` §10.4 class **(b)**: *"milhouse changes this
([q2]) — supersede at S4a."* `S4a-08` is the scheduled revisit. This file is
the placeholder that makes the S2 entry gate satisfiable (R-17).

## Context

`BeaconState::canonical_root` is the **hot** state-root path (`accessors.rs:990-1003`).
It `commit()`s list backends, `recompute_caches()`, and returns
`caches.field_roots.cached_root`, falling back to `TreeHash::tree_hash_root`
only if the cache is empty. `TreeHash::tree_hash_root` stays the **cold**
uncached path (`ssz_static`, tests).

`commit()` is a no-op under `ssz_types` and is already documented as the
milhouse hook (`accessors.rs:983-988`): under milhouse it will call
`apply_updates()` on every list. `[q2]` measured that the accessor surface
is already milhouse-shaped and that `commit()` is invoked at the top of
every state-root computation.

The cache lives **on** `BeaconState` (`StateCaches`, including list-hash
and field-root maps). That is the Phase-1 decision: one object, one cached
root, one cold fallback. It is also what milhouse replaces. milhouse's
tree *is* the hash cache; descendant states share subtree hashes; clone
becomes O(1). Keeping a hand-rolled `cached_root` beside a persistent
merkle tree is a second cache that will drift.

`[ARCH]` §4.2 / §5.5: the pubkey cache must leave `BeaconState` **before**
milhouse (S2, P0-19/3), or an O(1) list clone is immediately negated by an
O(V) map copy. That move is not this record. This record is only the
state-root path.

## Decision

**Until S4a, `canonical_root` is the cached state-root path.** Callers that
need the live root go through it (after `commit()`). Do not teach
production import / epoch / restore paths to call
`TreeHash::tree_hash_root` on `BeaconState` as the hot path. Do not move
the field-root cache off `BeaconState` in an S1–S3 "cleanup."

`commit()` stays a no-op under `ssz_types` and stays the milhouse
`apply_updates()` hook. Its signature does not change in this record.

**Revisit at S4a (`S4a-08`).** milhouse ([q2], CC-1H) supersedes the
hand-rolled cache. The S4a write-up records what `canonical_root` becomes
(almost certainly `commit()` + milhouse's own root) and marks this file
`superseded`. If milhouse is declined, this file is re-accepted and the
cache stays. Skipping `S4a-08` would make this `proposed` a fudge rather
than a mechanism.

## Consequences

What this makes easy:

- Hot hashing has one function and one cache.
- milhouse lands behind an existing `commit()` / `canonical_root` pair
  instead of a new API.
- `ssz_static` keeps an honest cold path.

What this makes hard:

- `StateCaches` is cloned with the state. That is why P0-19/3 must leave
  `BeaconState` before S4a; this record does not fix that.
- Reviewers must not treat `cached_root` as the S4 hash model.

What this forbids:

- A second production state-root API alongside `canonical_root`.
- Deleting the cache in S1–S3 "because milhouse is coming."
- Closing the S2 entry gate by marking this `accepted`.
- Citing this file as the milhouse design. That design is `[q2]` +
  `S4a-08`.

## Alternatives considered

**Cold `tree_hash_root` everywhere.** Rejected. Phase 1 already pays for
the field-root cache; import and epoch processing are on the cached path
by construction.

**Adopt milhouse now.** Rejected. S4a owns the swap; S2 must first move
the pubkey cache (`[ARCH]` §4.2). This record is not a licence to pull
CC-1H forward.

**Write nothing until S4a.** Rejected. R-17: the S2 gate needs a resolving
document with an explicit revisit.

## Refactor impact

**Supersede at S4a.** milhouse changes the cache.

| Stage | What happens to this record |
|---|---|
| S1 | This file. No accessor change. |
| S2 | Pubkey cache leaves `BeaconState` (P0-19/3). That is a prerequisite, not a supersession of this path. |
| S3 | Untouched. |
| S4a | **`S4a-08` revisits this ADR** and supersedes it if milhouse lands. |
