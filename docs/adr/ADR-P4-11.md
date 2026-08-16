# ADR-P4-11 — A soak clause is not discharged until the discharging stage is recorded

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4 (soak methodology)
- **Issues:** S1-B-07, CC-47a, CC-44b, CC-48
- **Citations:** `docs/phase-4-soak.md:373-407,1067-1069,1082`; `plan/issues/s3b-acceptance-windows.md:300-304`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-P4-11**. It is a soak-methodology record. It is **not**
`[ARCH]` §10.1 ⟡ D-14 (the ADR-corpus census). The D-14 token in
`docs/phase-4-soak.md` is the clause-2 three-stage rule.

## Context

Phase 4 clause 2 (cursor fallback) is three stages, three owners, three
venues (`docs/phase-4-soak.md:373-377`):

| Stage | Owner | Venue | What it proves |
|---|---|---|---|
| attribution | CC-44b | in-process-double | `cc_storage_stream_reconnect_total` reasons |
| hole recorded | CC-48 / CC-45b | self-devnet | hole durable in `ServeWindow.holes` |
| hole closed | CC-47a | self-devnet | parent-linkage walk empties holes |

Implemented-and-unit-tested is not a soak. The tree already has
`GapTrigger::ServeWindowHoles` and the below-anchor `PutBackfillBatch`
path (`services/p2p/src/backfill/`, `services/storage/src/backfill.rs`,
`crates/store/src/backfill_progress.rs`). That is necessary and not
sufficient. A live self-devnet run that (1) ages the cursor past the
ring, (2) fills via `PutBackfillBatch` not re-import through chain,
(3) completes the parent-linkage walk, (4) records
`cc_storage_stream_reconnect_total{reason="cursor_too_old"} == 1` was
**not** executed. No bar numbers were invented.

`soak-report.sh` encodes the same rule: clause 2 emits three stages;
**the clause is discharged only when the third is present**
(`docs/phase-4-soak.md:1082`). The first two rows are stage-only
(D-14). Merging stages, or treating instrument smoke as PASS, is how a
clause goes green without the walk.

## Decision

**A clause is not discharged until the discharging evidence is recorded
in the named soak section.** For clause 2 that evidence is the third
stage (hole closed), with both run (a) and run (b) rows. Unit tests,
harness self-tests, and `NOT_RUN` instrument smoke do not discharge.

No bar numbers are invented. `NOT_RUN` stays `NOT_RUN` until the live
run is written down. A partial (attribution without hole-closed, or
three-of-four bars on another clause) is still **not discharged**; name
the missing stage or failing bar.

This is soak methodology for every Phase 4 clause that cites D-14 /
ADR-P4-11. It is not a licence to mark clause 2 discharged because the
write path exists.

## Consequences

What this makes easy:

- A later reader can tell implementation from discharge.
- `soak-report.sh` and the clause table stay honest: missing third
  stage → not discharged.

What this makes hard:

- Shipping the backfill write path cannot close M4.5. Someone still has
  to run the self-devnet walk and paste the rows.

What this forbids:

- Inventing PASS / 20/20 / hole-closed rows without the run.
- Treating the first two clause-2 stages as a discharge.
- Merging clause 2 with another clause's section to green-wash a gap.
- Conflating this D-14 with `[ARCH]` §10.1 ⟡ D-14 (corpus size).

## Alternatives considered

**Discharge on unit tests + implemented write path.** Rejected by the
live soak doc: the path is implemented and the clause is still
`NOT_RUN`. That is the point of the third stage.

**Discharge when any one of the three stages is present.** Rejected.
`soak-report.sh` and `s3b-acceptance-windows.md` say only the third
stage discharges.

## Refactor impact

**Survives as a soak-methodology record.** S2 changes the column write
path; it does not change "no invented numbers" or "only the discharging
stage discharges." S3b re-runs these clauses for real; this file is
what stops a wiring issue from ticking clause 2 green on the way.
