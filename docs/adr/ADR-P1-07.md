# ADR-P1-07 — Transition errors classify exhaustively onto gossip verdicts

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1 (taxonomy); Phase 2 consumes it
- **Issues:** S1-B-08, CC-22
- **Citations:** 1 site in the §10.4 census — `crates/state-transition/src/error.rs:1`; live map at `error.rs:9-17,275-318` and the total test at `error.rs:329-436`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Phase 2 gossip needs ACCEPT / REJECT / IGNORE, plus a third house class for
"our bug — never a network-facing descore." `BlockError` is the transition's
public error. If classification is a catch-all or a later `match` in p2p, a
new variant silently becomes IGNORE or REJECT and a peer is punished for a
cache poison or a transport fault.

Architecture §5.3 put the map next to the enum.

## Decision

`BlockError::gossip_class()` is the Phase-2 map. It is **exhaustive with no
catch-all arm**: adding a variant without classifying it is a compile
failure (`error.rs:2-3,278-279`).

Three classes (`error.rs:11-17`):

| `GossipClass` | Meaning | Examples |
|---|---|---|
| `Reject` | Provably invalid; sender at fault | slot/header mismatches, bad signature, `InvalidPayload`, `Engine(InvalidPayload)`, blob-bound exceeded |
| `Ignore` | Cannot judge yet, or already known | `UnknownParent`, `FutureSlot`, `AlreadyKnown`, `NotDescendedFromFinalized`, `DataNotAvailable` |
| `Internal` | Our bug or resource limit — **must never** reach a network-facing verdict | `CachePoisoned`, `StateNotResident`, `Engine(Transport(_))`, nested `NotYetImplemented` |

`EngineError::InvalidPayload` is REJECT. `EngineError::Transport` is
INTERNAL. Those two must not collapse.

The unit test `gossip_class_is_total_no_catchall` samples every variant
(`error.rs:332`). Adding a variant without updating the test also fails
the build.

## Consequences

What this makes easy:

- p2p scores off a house enum, not a stringly `reason`.
- A new transition error cannot ship unclassified.

What this makes hard:

- Every new `BlockError` / `EngineError` / `OperationError` variant is a
  gossip-policy change and must pick a class in the same PR.

What this forbids:

- A `_ =>` arm on `gossip_class`.
- Treating `Engine(Transport)` or `CachePoisoned` as REJECT.
- Re-deriving the map in the gossip validator.

## Alternatives considered

**Classify in the p2p crate from `Display` / reason strings.** Rejected:
the citing module exists so the map is next to the enum and is exhaustive.

**Two classes (valid / invalid) and drop Internal.** Rejected: Internal is
how a resource limit or our bug avoids becoming a peer penalty (SEC-12a).

None further recorded.

## Refactor impact

**Survives.** S1/S3 gossip wiring consumes this map; it does not replace
it. A new transition error still has to land a `GossipClass` here.
