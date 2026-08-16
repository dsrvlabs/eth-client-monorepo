# ADR-P4-01 — redb behind an engine seam; a fjall substitution replaces one file

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-40b
- **Citations:** 2 sites — `crates/store/src/engine/mod.rs:4`; `docs/storage-engine.md:5`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing.** `[ARCH]` §4.1's "who owns redb" rules
assume this seam: one inherent API, body in one file, no trait / `dyn`.

## Context

Phase 4 needed an embedded store that is `unsafe_code = "deny"`-compatible
and that survived a documented falsifier (`bin/store-bench`, both sharded
and flat layouts, `docs/storage-engine.md`). The callers (`cc-store`
tables, `services/storage` writer) must not name a vendor. A trait or
`dyn Engine` would tax every get/put and would invite a second impl before
one was needed. The substitution that *was* contemplated is fjall, and
only if a redb major fails the falsifier.

## Decision

Keep **one inherent `Engine` API** in `crates/store/src/engine/`. No trait,
no generic, no `dyn`. The body lives in `engine/redb.rs`. Adopted version
is **redb 4.1.0**. A fjall (or other) substitution **replaces that one
file** and keeps the inherent API. A newer 4.x minor may be taken without
redesign; a major must re-run the falsifier on both layouts. `cc-store`
still names no consensus container.

## Consequences

What this makes easy:

- Callers depend on `Engine` / `Batch` / `ReadTxn`, not on `redb::`.
- A backend swap is a one-file change plus the falsifier, not a trait
  migration.
- `[ARCH]` §4.1 can say "exactly one `redb::Database` handle" and still
  mean "exactly one `Engine`" if the body file changes.

What this makes hard:

- There is no second production backend to A/B at runtime. Tests do not
  get a mock engine via `dyn`.
- `TableDefinition` name lifetime is an engine-body detail (intern pool,
  R-14); it must not leak into the schema.

What this forbids:

- Spreading `redb::` types through `crates/store` tables or
  `services/storage`.
- Introducing a trait / `dyn` "to make fjall easier" before a substitution
  is actually taken.
- Taking a redb major, or fjall, without re-running `bin/store-bench`.
- Sharing this `Engine` with the slashing-protection file (`ADR-R-05`).

## Alternatives considered

**A trait + `dyn Engine` now.** Rejected in the module docs: the API is
inherent; substitution replaces one file.

**fjall as the adopted engine.** Not taken. Both falsifier layouts passed
on redb 4.1.0; `docs/storage-engine.md` keeps redb and records fjall as
the substitution, not the default.

## Refactor impact

**Survives and is load-bearing** — `[ARCH]` §4.1. S2 moves the *opener*
into `bin/beacon-core` and the writer into `crates/storage-core`; it does
not delete the seam or put `redb::` in `chain-core`. Only
`crates/storage-core` may name `cc-store`. The slashing crate at S5 takes
redb **directly**, not via this `Engine`.
