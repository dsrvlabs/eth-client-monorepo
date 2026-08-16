# ADR-P3-02 — Engine dials p2p; engine is not a health peer

- **Status:** superseded · superseded-by: ADR-R-04 · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3 (topology still live in the six-service tree; liveness implication void at S1)
- **Issues:** S1-B-12, S1-A-16, CC-32b, CC-38a
- **Citations:** 11 census sites — `proto/eth/p2p/v1/p2p.proto:21`; `config/chain.toml:57`; `config/engine.toml:9`; `services/engine/src/inject.rs:3,66`; `services/chain/src/main.rs:190,433`; `services/chain/src/core.rs:809`; `crates/engine-api/src/config.rs:133,196,389`
- **Provenance:** re-derived from code (2026-08-16); liveness implication superseded by ADR-R-04 at `S1-B-12`

This is **ADR-P3-02**. `[ARCH]` §10.4 class **(b)**: *engine dials p2p; engine
is deliberately not a health peer*. §7.1 shows **this is why a parked core
reports green**. Defensible for a separate engine process; the green reading
is void once liveness is a core no-op.

`[ARCH]` §10.4 / §10.5 route the successor to **ADR-R-03** and say R-03
*partially supersedes* this id. That assignment is wrong. **ADR-R-03 is the
JWT isolation restatement** (`S1-B-11`: only `cc-engine-api` may declare a
signer; `cc-chain` is not grandfathered). R-03 correctly refused this half.
The live liveness record is **ADR-R-04** (`S1-A-16`: after a core is
installed, aggregate SERVING requires a recent `probe_core_liveness`). This
file is superseded-by **ADR-R-04**, not R-03. Do not put engine health into
the JWT ADR.

## Context

Phase 3 added `EngineStream` (CC-38a). Engine is the client; p2p is the
server (`p2p.proto:21-24`; `inject.rs`). The fast path is an accelerator:
sidecars go up, the subscription set and the column-triggered fetch come
down. A p2p restart must not take engine `NOT_SERVING`.

Separately, chain talks to `EngineService` over a gRPC URI (CC-32b). When
engine is down, imports surface engine-unavailable; chain stays SERVING
(`config/chain.toml:57-59`). Putting either URI under `[peers]` would grow
a health edge and cycle or invert the DAG (ADR-07 roots health at chain).

Both URIs are therefore **plain config keys**, not `[peers]` entries
(`engine.toml:9-12`; `EngineTransportConfig.p2p_uri`; chain `engine_uri`).
Engine's only health peer is chain. Chain has none.

That exclusion, plus the health service answering from a tokio task while
the consensus core is a dedicated OS thread (ADR-P1-09), is why a parked
core reported green (`[ARCH]` §7.1, [PRD] R-4 / M8). A black-holed engine
parked the core via an undeadlined `block_on`; compose still saw SERVING.

`S1-A-16` wired `probe_core_liveness` into aggregate `""`. Parked-core-green
is no longer an acceptable reading of this decision.

## Decision

**Recorded (Phase 3, still the live topology while engine is a process).**
Engine dials p2p. `p2p_uri` and `engine_uri` are plain config, not `[peers]`.
Do not add engine as a compose / bootstrap health peer. Do not flip EL
health. A p2p restart must not make engine `NOT_SERVING`. An engine-down
must not make chain `NOT_SERVING`.

**Superseded (the liveness implication).** Excluding engine from the health
DAG is **not** a license for a parked core to stay SERVING. After a core is
installed, liveness is the deadline-bounded core no-op (`ADR-R-04` /
`S1-A-16`). Aggregate `""` reflects that probe, not process-up.

Do not "fix" parked-core-green by adding `engine` to chain's `[peers]`, or
`p2p` to engine's `[peers]`. That would cycle the DAG and restart the wrong
container. The successor folded engine-*edge* health into the core probe
rather than excluding the core from health entirely (`[ARCH]` §10.5's
intended clause (2), filed under the wrong id).

## Consequences

What this made easy:

- The six-service health DAG stayed acyclic (ADR-07).
- Engine's fast path survived a p2p bounce.
- Chain stayed SERVING when engine was down; imports deferred
  (`pending_engine`, ADR-P3-05).

What this made hard:

- A parked core was invisible to `grpc-health-probe`. That is no longer
  acceptable (`ADR-R-04`).
- Anyone who wants engine on the health DAG has to contradict the live
  topology *and* this file's "do not flip EL / `[peers]`" remainder.

What this forbids:

- Treating parked-core-green as still in force.
- Citing this id as a reason not to run `probe_core_liveness`.
- Folding the successor into `ADR-R-03` (JWT).
- Adding `p2p` or `engine` under `[peers]` "because this ADR is
  superseded."

## Alternatives considered

**Supersede with ADR-R-03, as `[ARCH]` §10.4 / §10.5 wrote.** Rejected.
R-03 is JWT isolation. `S1-B-11` already refused to take this half.
Misusing that id would collide two decisions the way X-3 / X-6 collided
R-04 / R-05.

**Add engine as a health peer now that the probe exists.** Rejected.
`S1-A-16` does not flip EL health. Engine is still a process. A peer edge
would cycle the DAG (ADR-07) and restart engine for a core park, or
restart chain for an engine bounce — the failure the exclusion existed
to avoid.

**Leave this id unwritten and let ADR-R-04 stand alone.** Rejected.
Class (b) needs the Phase-3 decision on disk. R-04 is the liveness
wiring; this file is the supersession of the exclusion-as-green reading.

**Wait until engine is in-process (`S1-A-06` / compose collapse).**
Rejected as a reason to keep parked-core-green. The probe landed at
`S1-A-16`. The (b) row's coupling is that probe, not the process merge.

## Refactor impact

**Liveness implication deleted at S1-A-16 / S1-B-12. Topology remains
until the engine process goes away.**

| Stage | What happens to this record |
|---|---|
| Phase 3 | Engine dials p2p; URIs not under `[peers]`. Parked core reports green. |
| S1-A-16 | Probe wired. Parked-core-green is void. **Landed.** |
| S1-B-12 | This file. Successor is ADR-R-04, not ADR-R-03. No production change. |
| S1-A-06 / S2 | Engine constructor / compose collapse. Plain-config URIs die with the hop. Do not revive `[peers]` edges to "replace" them. |
| S3+ | Citing this id to keep aggregate SERVING on a parked core is a defect against ADR-R-04. |
