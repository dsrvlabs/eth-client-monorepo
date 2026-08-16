# ADR reconciliation table

**M11 baseline: 748 occurrences** (541 `Architecture §` + 207 ADR). Unit:
*occurrences*, not lines (`[ARCH]` §10.1, `[PLAN]` X-5).

**58 cited ids** from the enumerated `[ARCH]` §10.4 table: **(a) 43 · (b) 12 ·
(c) 3**. (`[ARCH]` §10.4's totals line and ⟡ D-13 say 42/12/4; `[PLAN]` X-1 —
this file follows the enumerated rows, which is the artifact the work is done
against.)

This is a file, not a section of `plan/architecture.md`, so `S1-B-06` can
resolve against it. Classification: **(a)** re-derivable from the citing code ·
**(b)** needs a decision recorded · **(c)** stale — delete the citation.

The eight never-cited numbering gaps (`P1-01`, `P1-02`, `P1-03`, `P1-06`,
`P2-01`, `P2-03`, `P2-12`, `P4-02`) are **not rows here**. They need no ADR;
see [`README.md`](README.md). Their resolving token `uncited` is README-only.

## Parse contract

The GFM table whose header is `| id | bucket | status | resolving |` is the
data. A later ratchet (S2 / ⟡ D-13, not this file's S1-B-06 gate) should:

1. Take body rows of that table (skip the header and the `---` separator).
2. Split on `|` and strip cells. No cell contains `|`.
3. `id` is the hyphenated canonical form (`ADR-P3-02`).
4. `bucket` is `a`, `b`, or `c`. Every cited id has exactly one. No
   unclassified rows.
5. `status` is a token from the vocabulary below. R-17 is represented by
   appending `; revisit at Sn` (example: `proposed; revisit at S2`).
6. `resolving` is a repo-relative path or `-` (no document yet). The token
   `uncited` is README-only (never-cited numbering gaps); it does not appear
   in this table.

`scripts/check-adr-resolver.sh` (S1-B-06) reads the `id` column as an
allowlist (plus `docs/adr/ADR-*.md` filenames). It does not check bucket,
status, or resolving, and an unwritten row still resolves.

### Status vocabulary

| status | meaning |
|---|---|
| `unwritten (a)` | (a) row; ADR body not written (`S1-B-07`…`S1-B-10`) |
| `unwritten (b)` | (b) row; decision not recorded (`S1-B-11`…`S1-B-16`) |
| `stale (c)` | (c) row; citation to delete (`S1-B-17`) |
| `deleted` | (c) row; code citation removed; no ADR (subject gone or premise deleted) |
| `accepted` | document exists; `Status: accepted` |
| `proposed` | document exists; `Status: proposed` |
| `proposed; revisit at Sn` | R-17 form; `Sn` is the owning stage |
| `superseded` | document exists; superseded (optionally `; superseded-by ADR-R-NN`) |

Empty stub rows (`unwritten (a)` / `unwritten (b)` / `stale (c)`, `resolving: -`)
are correct until the owning `S1-B-07`…`S1-B-17` issue lands the document or
deletes the citation.

## Table

| id | bucket | status | resolving |
|---|---|---|---|
| ADR-04 | a | accepted | docs/adr/ADR-04.md |
| ADR-05 | a | accepted | docs/adr/ADR-05.md |
| ADR-06 | a | accepted | docs/adr/ADR-06.md |
| ADR-07 | b | unwritten (b) | - |
| ADR-09 | b | unwritten (b) | - |
| ADR-11 | a | accepted | docs/adr/ADR-11.md |
| ADR-12 | a | accepted | docs/adr/ADR-12.md |
| ADR-P1-04 | b | unwritten (b) | - |
| ADR-P1-05 | a | accepted | docs/adr/ADR-P1-05.md |
| ADR-P1-07 | a | accepted | docs/adr/ADR-P1-07.md |
| ADR-P1-08 | a | accepted | docs/adr/ADR-P1-08.md |
| ADR-P1-09 | a | accepted | docs/adr/ADR-P1-09.md |
| ADR-P1-10 | a | accepted | docs/adr/ADR-P1-10.md |
| ADR-P1-11 | b | unwritten (b) | - |
| ADR-P1-12 | a | accepted | docs/adr/ADR-P1-12.md |
| ADR-P1-13 | c | deleted | - |
| ADR-P1-14 | a | accepted | docs/adr/ADR-P1-14.md |
| ADR-P1-15 | a | accepted | docs/adr/ADR-P1-15.md |
| ADR-P2-02 | a | accepted | docs/adr/ADR-P2-02.md |
| ADR-P2-04 | a | accepted | docs/adr/ADR-P2-04.md |
| ADR-P2-05 | a | accepted | docs/adr/ADR-P2-05.md |
| ADR-P2-06 | a | accepted | docs/adr/ADR-P2-06.md |
| ADR-P2-07 | a | accepted | docs/adr/ADR-P2-07.md |
| ADR-P2-08 | a | accepted | docs/adr/ADR-P2-08.md |
| ADR-P2-09 | a | accepted | docs/adr/ADR-P2-09.md |
| ADR-P2-10 | b | unwritten (b) | - |
| ADR-P2-11 | b | unwritten (b) | - |
| ADR-P2-13 | b | accepted | docs/adr/ADR-P2-13.md |
| ADR-P2-14 | a | accepted | docs/adr/ADR-P2-14.md |
| ADR-P3-01 | a | accepted | docs/adr/ADR-P3-01.md |
| ADR-P3-02 | b | unwritten (b) | - |
| ADR-P3-03 | a | accepted | docs/adr/ADR-P3-03.md |
| ADR-P3-04 | a | accepted | docs/adr/ADR-P3-04.md |
| ADR-P3-05 | a | accepted | docs/adr/ADR-P3-05.md |
| ADR-P3-06 | c | deleted | - |
| ADR-P3-07 | a | accepted | docs/adr/ADR-P3-07.md |
| ADR-P3-08 | a | accepted | docs/adr/ADR-P3-08.md |
| ADR-P3-09 | a | accepted | docs/adr/ADR-P3-09.md |
| ADR-P3-10 | a | accepted | docs/adr/ADR-P3-10.md |
| ADR-P3-11 | a | accepted | docs/adr/ADR-P3-11.md |
| ADR-P3-12 | a | accepted | docs/adr/ADR-P3-12.md |
| ADR-P3-13 | a | accepted | docs/adr/ADR-P3-13.md |
| ADR-P3-14 | b | unwritten (b) | - |
| ADR-P3-15 | b | unwritten (b) | - |
| ADR-P3-16 | b | superseded; superseded-by ADR-R-03 | docs/adr/ADR-R-03.md |
| ADR-P4-01 | a | accepted | docs/adr/ADR-P4-01.md |
| ADR-P4-03 | b | accepted | docs/adr/ADR-P4-03.md |
| ADR-P4-04 | a | accepted | docs/adr/ADR-P4-04.md |
| ADR-P4-05 | a | accepted | docs/adr/ADR-P4-05.md |
| ADR-P4-06 | a | accepted | docs/adr/ADR-P4-06.md |
| ADR-P4-07 | c | superseded; superseded-by ADR-R-02 | docs/adr/ADR-P4-07.md |
| ADR-P4-08 | a | accepted | docs/adr/ADR-P4-08.md |
| ADR-P4-09 | a | accepted | docs/adr/ADR-P4-09.md |
| ADR-P4-10 | a | accepted | docs/adr/ADR-P4-10.md |
| ADR-P4-11 | a | accepted | docs/adr/ADR-P4-11.md |
| ADR-P4-12 | a | accepted | docs/adr/ADR-P4-12.md |
| ADR-P4-13 | a | accepted | docs/adr/ADR-P4-13.md |
| ADR-P4-14 | a | accepted | docs/adr/ADR-P4-14.md |

## Refactor records (not in the 58)

`ADR-R-*` is out of the S1-B-06 extractor (`[ARCH]` §10.3). These files
resolve by filename. They are not census rows: do not add them to the table
above (that table is the 58 cited ids). This section is not a second
`| id | bucket | status | resolving |` table.

S1-B-18 lands [`ADR-R-01.md`](ADR-R-01.md) — Status: accepted. Records the
R-1 discharge order (types land, transport later). Does not supersede a
§10.4 row.
