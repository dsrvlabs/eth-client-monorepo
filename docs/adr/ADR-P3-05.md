# ADR-P3-05 — Engine-unavailable requeue is a separate map from `pending_da`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-08, CC-36a, CC-38
- **Citations:** 2 sites in the §10.4 census — `services/chain/src/pending_engine.rs:1,8-14,26-29`; timeout path at `services/chain/src/engine_client.rs:17-21`; contrast `services/chain/src/da.rs:22-25`; single-variable test at `services/p2p/src/engine_stream/inject.rs:1200-1201`; `[ARCH]` §7.2; `plan/prd.md` row 14; `plan/issues/s2-fold-storage.md` S2-A-01
- **Provenance:** re-derived from code (2026-08-16)

## Context

`on_block` can defer for two different reasons: data-availability not
yet satisfied, and the execution engine unavailable (transport /
deadline). Those waits have different clocks. DA is bounded by the p2p
recovery ladder (~3.3 slots worst case) and times out at **4 slots**.
An EL restart is a container restart (`docker compose restart el` ~
96 s) and needs **8 slots**.

One map with one timeout cannot tell those stories apart. CC-38 /7's
timeout-ordering test is a single-variable check only if the maps stay
separate (`inject.rs:1200-1201`). Closing OQ-P3-10 was this split.

A black-holed engine without a deadline parks the whole consensus core
(`engine_client.rs:17-21`). The deadline turns that into
`Deferred(ExecutionEngineUnavailable)`, which must land in *this* map
— `[ARCH]` §7.2's acceptance test asserts the deferral actually
reaches it.

## Decision

Engine-unavailable requeue lives in **`pending_engine`**, a **separate
map** from `pending_da` (`pending_engine.rs:1-14`).

| Map | Bound | Timeout | Trigger |
|---|---:|---:|---|
| `pending_engine` | 64 | **8 slots** (96 s) | `Deferred(ExecutionEngineUnavailable)` |
| `pending_da` | 64 | **4 slots** | `Deferred(DataUnavailable)` |

Offline → Online on the engine state watch re-drives `pending_engine`
only. DA expiry / `DataAvailable` re-drives `pending_da` only.

Bounds: count + slot-bounded timeout + drop counter. Oldest evicted
on overflow. Refresh of the same root replaces the payload and keeps
position (`pending_engine.rs:52-65,102-108`).

This map **survives S1** (the in-process engine can still be down or
past deadline). `[PRD]` row 14: `pending_engine` still parks a
re-encode; F2 (arrival bytes) is a one-line swap, not a map merge.

## Consequences

What this makes easy:

- An EL bounce does not evict DA-waiters on the 4-slot clock, and a
  slow column recovery does not sit on the 8-slot engine clock.
- The §7.2 black-hole test has one place to look:
  `pending_engine` occupancy / drops, not a mixed queue.

What this makes hard:

- Two eviction metrics, two expire paths. A "just use pending_da"
  helper is a defect.

What this forbids:

- Merging the maps or giving them one timeout.
- Parking engine-unavailable blocks in `pending_da` (or the reverse).
- Treating a transport timeout as `Imported` / optimistic *instead of*
  parking here (unless a superseding ADR says so).

## Alternatives considered

**One pending map, max(4, 8) timeout.** Rejected: it fails
OQ-P3-10 and makes CC-38 /7 a two-variable test. An EL restart would
also hold DA slots.

**No requeue — drop the block and wait for gossip retry.** Rejected:
the point of the deadline is to *unpark the core*, not to forget the
block. Re-drive on Online is the recovery.

None further recorded.

## Refactor impact

**Survives.** S1 keeps the map (deadlines still produce this deferral).
S2 extracts it with `chain-core` (`S2-A-01` — carry intact). §7.2's
acceptance test still asserts a black-holed engine lands here.
