# ADR-P3-01 — Phase 3 adds no workspace member

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-07, CC-3K
- **Citations:** `docs/phase-3-acceptance.md` (Member count / CC-3K /8 row); `Cargo.toml:3-33`
- **Provenance:** re-derived from code (2026-08-16) — **count corrected**

This is **ADR-P3-01**. The decision is the Phase 3 membership freeze. The
integer next to it is a snapshot, not the decision.

## Context

CC-3K /8 gated Phase 3 close on a workspace member count.
`docs/phase-3-acceptance.md` recorded **16** packages and the sentence
*Phase 3 adds no workspace member* (≠13/1), with the sixteen names
`cc-attestation` … `cc-types`. That 16 was right at Phase 3 close:
CC-28 had already retired `bin/driver` (17→16) *before* Phase 3, and
Phase 3 itself added no crate.

The same cell was then left standing as if 16 were still the live
workspace. It is not. `[ARCH]` §10.4 already flagged the cited integer
as stale (it said 20 after the Phase-4 store DAG). The live
`[workspace].members` list in `Cargo.toml` is **23** path packages
(`cargo metadata --format-version 1 --no-deps | jq '.packages | length'`
agrees). Later admissions, none of them Phase 3:

| Step | Count | Members added |
|---|---:|---|
| Phase 3 close (CC-28 already applied) | 16 | — |
| Phase 4 store DAG | 16 → 19 | `cc-store`, `cc-store-bench`, `cc-serve-probe` |
| CC-4J offline store tool | 19 → 20 | `cc-store-tool` (`bin/cc-store`) |
| S0-A-13 scheduler | 20 → 21 | `cc-scheduler` |
| S1-A-01 Engine API crate | 21 → 22 | `cc-engine-api` |
| S1-A-07 typed handles | 22 → 23 | `cc-seam` |

The decision ("Phase 3 adds none") is still true. The integer 16 is a
close-of-phase measurement. Leaving it as the standing count makes the
next reader think later admissions are a Phase 3 regression.

## Decision

**Phase 3 adds no workspace member.** That freeze is what CC-3K /8
enforced. It does **not** freeze the workspace at 16 forever.

The standing count is the live `[workspace].members` length. Today that
is **23**. The Phase 3 close names remain the historical sixteen. The
phase-3-acceptance Member-count cell is corrected in the same change as
this file so the cited integer matches the tree.

Later stages add members only by their own issues (store DAG, S0
scheduler, S1 engine-api / seam). Those additions are not a violation of
this ADR and are not a reason to rewrite Phase 3 as having added them.

## Consequences

What this makes easy:

- Phase 3 reviews stay scoped to engine/p2p/chain contracts, not a new
  crate.
- The close-of-phase 16 and the live 23 can both be true.

What this makes hard:

- Citing this ADR as "the workspace has 16 members" is a defect. The
  decision is the Phase 3 freeze, not a standing census.

What this forbids:

- A Phase 3 commit that appends a `[workspace].members` row.
- Quietly leaving a stale integer in the phase-3 acceptance table as if
  it were still measured.
- Reading a later admission (`cc-store`, `cc-scheduler`, `cc-engine-api`,
  `cc-seam`, …) as a Phase 3 policy change.

## Alternatives considered

**Treat 16 as a standing invariant and reject later members.** Rejected.
Phase 4's store DAG and S0/S1 crate extractions are scheduled work. The
freeze was "Phase 3 adds none," not "the repo never grows."

**Leave the phase doc at 16 and only write this ADR.** Rejected.
`[ARCH]` §10.4 and `S1-B-07` require the count in
`docs/phase-3-acceptance.md` to be fixed at the live site.

## Refactor impact

**Survives as a Phase 3 historical freeze.** S1+ membership growth is
expected and already landed. Do not use this file to block `cc-wire`
(S3) or `crates/storage-core` (S2). Do not "fix" those later counts by
rewriting Phase 3 as having included them.
