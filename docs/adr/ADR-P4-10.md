# ADR-P4-10 — Column shards are 32 epochs; block shards are 256

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-46b
- **Citations:** 8 sites — `crates/store/src/keys.rs:10,13`; `crates/store/src/blocks.rs:16`; `crates/store/src/columns.rs:17`; `crates/store/src/schema.rs:60`; `docs/storage-schema.md:36`; `services/storage/src/prune/shards.rs:1,231`
- **Provenance:** re-derived from code (2026-08-16)

## Context

redb 4.1.0 `open_table` elides the table-name lifetime to `'static`, so
shard names are interned (`docs/storage-engine.md` R-14). Each shard is
one table (`columns_{ddddd}`, `blocks_{ddddd}`). Shard width **is** the
prune cadence: a tick retires one table with `drop_table` rather than
thousands of B-tree deletes (`prune/shards.rs:1`). Columns prune every
32 epochs; blocks prune every 256 epochs (CC-4A floor). A 32-epoch
*block* shard could not drop until the 256-epoch tick, so it would only
add interned names. `[PRD]` P0-18 is interned table-name exhaustion at
~30 days uptime — this width choice is the growth rate of that pool.

## Decision

Fix column shard width at **32 epochs** and block shard width at
**256 epochs** (`keys.rs:10-14`). Name tables with a zero-padded
five-digit id. Keep the intern pool capped (`MAX_INTERNED_TABLE_NAMES =
512`). Reconcile `table_names()` against the registry at open;
unregistered names fail (I-shards). Do not introduce a 32-epoch block
shard.

## Consequences

What this makes easy:

- One prune tick = one `drop_table`. A 128-slot ByRange spans at most
  two shards under either width (CC-43 /4).
- Column-pass size is ~8 200 keys per 32-epoch shard, chunked on P2
  (`ADR-P4-04`).

What this makes hard:

- The interned-name ceiling is a function of uptime / width. P0-18
  (cap, reuse, or a different naming scheme) is S2 work that **interacts
  with this record**; it does not silently change the widths.

What this forbids:

- Equalising both classes to 32 epochs "for symmetry."
- Runtime-built names that skip the intern pool / registry.
- Growing `MAX_INTERNED_TABLE_NAMES` as the fix for P0-18 without a
  new record.

## Alternatives considered

**32-epoch shards for both classes.** Rejected: block prune is 256
epochs; a 32-epoch block table cannot retire on its own tick.

**Key-prefix in one table instead of shards.** Available as the flat
falsifier layout (`docs/storage-engine.md`); not the production schema.
Shards stay tables.

## Refactor impact

**Survives; interacts with [PRD] P0-18** (interned table-name
exhaustion). Widths move with `cc-store`. S2's scale fixes may change
how names are interned or retired; they must not quietly rewrite 32 /
256.
