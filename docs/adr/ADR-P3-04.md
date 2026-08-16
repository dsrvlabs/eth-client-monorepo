# ADR-P3-04 — Five `PayloadStatus` variants match the five `PayloadStatusV1` statuses

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-08, CC-14, CC-32, CC-34
- **Citations:** 1 site — `crates/state-transition/src/engine_seam.rs:35`; enum at `engine_seam.rs:33-53`; aliases at `engine_seam.rs:55-64`; alias test at `engine_seam.rs:286-321`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Engine API `PayloadStatusV1` has five statuses. Fork-choice
bookkeeping only needs the spec aliases (`NOT_VALIDATED`,
`INVALIDATED`) plus `Valid`. Collapsing at the seam would lose the
metric surface (syncing vs accepted, invalid vs invalid-block-hash)
and force every observer to re-parse a string.

## Decision

`PayloadStatus` has **exactly five variants**, matching the five wire
values (`engine_seam.rs:33-53`):

| Variant | Spec alias | Meaning |
|---|---|---|
| `Valid` | — | EL accepted the payload |
| `Invalid { latest_valid_hash }` | `INVALIDATED` | EL rejected the payload |
| `Syncing` | `NOT_VALIDATED` | requisite data missing |
| `Accepted` | `NOT_VALIDATED` | well-formed, not canonical |
| `InvalidBlockHash` | `INVALIDATED` | hash rejected; LVH always none |

`is_not_validated()` ≜ `Syncing | Accepted`.
`is_invalidated()` ≜ `Invalid | InvalidBlockHash`.
The aliases are mutually exclusive; `Valid` is in neither
(`engine_seam.rs:55-64,286-321`).

Fork-choice maps these onto four `ExecutionStatus` values
(`Valid` / `Optimistic` / `Invalid` / `Irrelevant`) in ADR-P3-10.
The **seam** keeps all five so metrics and CC-35's LVH path can see
the wire value.

## Consequences

What this makes easy:

- Metrics can label `SYNCING` vs `ACCEPTED` without a second channel.
- `InvalidBlockHash` cannot accidentally carry a `latest_valid_hash`.

What this makes hard:

- A sixth Engine-API status is a seam change, not a `#[non_exhaustive]`
  surprise.

What this forbids:

- A three-value seam enum (`Valid` / `Optimistic` / `Invalid`) that
  drops the wire distinction.
- Treating `Accepted` as `Valid` or `Syncing` as a transport error.

## Alternatives considered

**Collapse to the spec aliases at the seam.** Rejected in the citing
comment: fork-choice aliases are a *view*; the metric surface needs
all five.

None further recorded.

## Refactor impact

**Survives.** The S1 move into `cc-engine-api` carries the five-value
enum. Mapping onto `ExecutionStatus` stays in `cc-fork-choice`.
