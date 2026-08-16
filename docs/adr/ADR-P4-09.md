# ADR-P4-09 — Cold keys by slot (and index); hot keys keep the root

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-43a, CC-43b
- **Citations:** 7 sites — `crates/store/src/blocks.rs:3,167`; `crates/store/src/columns.rs:3,500,545,1258`; `crates/store/src/split.rs:172`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Above the split, several roots may exist at one slot (canonical plus
unfinalized siblings). Below the split, migration has deleted
unfinalized siblings (CC-41 step 2); one slot has one block. Column
serve-by-range is specified as `(slot, column_index)` order. Keeping the
root in the cold key would make every ByRange a prefix scan over a
40-byte key and would store a root the region no longer needs.

## Decision

**Hot** keys include the root: blocks `(slot, root)` 40 B; columns
`(slot, root, idx)` 42 B. **Cold** keys drop the root: blocks `slot`
8 B (one row per slot); columns `(slot, idx)` 10 B — the spec
`(slot, column_index)` order. Reverse indexes (`block_slot_by_root`,
`column_slot_by_root`) stay available for ByRoot. Migration re-keys
exactly that way (`split.rs:172-174`). A cold column lookup is
`(slot, idx)`; a mismatched root still returns the row
(`columns.rs:1258`). Values stay opaque SSZ.

## Consequences

What this makes easy:

- Cold ByRange is a key-order walk. A column shard's native order is
  the spec response order.
- Migration is a mechanical re-key plus sibling delete, not a
  re-encode.

What this makes hard:

- Cold is root-blind. A caller that wants "this root at this slot" in
  the cold region must go through the reverse index or accept that the
  slot has one body.
- Putting a second block at a cold slot is a `KeyCollision`.

What this forbids:

- Cold block keys of `(slot, root)`, or cold column keys that still
  carry the root.
- Decoding consensus containers in `cc-store` to "make the key
  richer."
- A migration that copies the hot key layout into the cold table.

## Alternatives considered

**Cold keys keep the root, like hot.** Rejected: after sibling delete
the root is redundant, and it breaks the spec column response order as
the key order.

## Refactor impact

**Survives.** Schema does not change at S2 (`[ARCH]` §9.1 S2 rollback
story). Typed ingest still writes through these key layouts.
