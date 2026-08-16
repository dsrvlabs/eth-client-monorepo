# ADR-05 — `buf breaking` at `FILE` category; messages never move file; renames are breaking

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 0 (workspace-wide; survives to S5, then mostly moot)
- **Issues:** S1-B-07, CC-03
- **Citations:** `docs/contracts.md:4,13-16,69-87`; `proto/buf.yaml:34-38`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-05**. Evolution rules live in `docs/contracts.md`; this file is
the record those rules cite.

## Context

`proto/` is the wire contract every service binary depends on. Additive RPCs
on an existing service are cheap. Moving a published message, renaming a
field, or renaming a file is not — consumers pin file identity and field
names under the strictest buf category.

`proto/buf.yaml` already sets `breaking.use: [FILE]` (`:34-36`).
`docs/contracts.md:4` points evolution at Architecture §3.3 / §3.5 and this
id. The FILE-category section (`docs/contracts.md:69-87`) is the operational
list reviewers actually hit too late if it is only a comment in `buf.yaml`.

## Decision

**`breaking.use: [FILE]`.** That is the strictest buf category. Consequences
that are easy to discover too late, written as standing rules:

| Rule | Consequence |
|---|---|
| A published message **never moves to another file** | Even within the same package, even with no wire change. Tidying `chain.proto` into `chain_service.proto` + `events.proto` by moving existing messages is a breaking change. |
| **Renaming a field is breaking** | Wire-compatible does not matter under `FILE`. |
| **Renaming or deleting a file is breaking** | File identity is part of the contract surface. |

Standing design:

- **One `.proto` file per package in Phase 0**, named after the package's
  service (e.g. `eth/chain/v1/chain.proto`).
- **New messages may go in new files** within the same package.
- **Existing messages never move.**
- Field numbers **1–15** for hot-path / frequently-set fields; **16+**
  otherwise. Deleted fields get `reserved` for **both** number and name.
  Field numbers are **never reused**.

Phase 1 additions land in files that already exist; splits stay open only
for new surface. Adding RPCs to an existing service is **not** a
`FILE`-category break (`docs/contracts.md:103-105`).

The sanctioned escape hatch for a deliberate break is the GitHub PR label
**`buf skip breaking`**, pull_request events only, with every affected
downstream consumer named in the PR. It is not a workaround for a missing
baseline. Push events always run breaking.

Vendored `google.rpc` under `third_party` is ignored for lint and breaking
(ADR-P1-14). That ignore is path-scoped; a violation under `eth/` still
fails.

## Consequences

What this makes easy:

- A move or rename of a published message is a labelled, reviewed break,
  not a tidy-up that CI green-washes.
- New messages can still land in new files without a label.
- Additive RPCs on `ChainService` (and later services) stay unlabelled.

What this makes hard:

- File-level tidying of `chain.proto` / `p2p.proto` / `storage.proto` is a
  break even when the wire is unchanged.
- Anyone who wants `WIRE` or `PACKAGE` to make refactors cheaper has to
  contradict this file, not just edit `buf.yaml`.

What this forbids:

- Moving a published message between files.
- Reusing a field number, or deleting a field without `reserved` name and
  number.
- Turning breaking off in the workflow because a baseline failed to
  resolve (R-2).
- Treating `buf skip breaking` as the path for a missing baseline.

## Alternatives considered

**`WIRE` or `PACKAGE` category.** Weaker. Would allow file moves and some
renames that `FILE` rejects. Rejected in the live `buf.yaml` and in
`docs/contracts.md:13-16`. The house chose the strictest category while
the topology is still many gRPC contracts.

**Allow message moves within a package.** Rejected. FILE treats file
identity as part of the surface; the tidy-up is the break.

## Refactor impact

**Survives to S5**, then mostly moot as the remaining internal contracts
are deleted with their services. Until a package is deleted, `FILE` still
governs it. S1–S4 must not "clean up" proto layout by moving published
messages.
