# ADR-P3-14 — EL compose dependency is `service_started`, never `service_healthy`

- **Status:** proposed · revisit at S2 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 3 (EL joins compose)
- **Issues:** S1-B-16, S2-J-01
- **Citations:** 4 sites — `docker-compose.yml:113-117,217-219`; `docs/el-runbook.md:71`; `docs/running.md:438`
- **Provenance:** re-derived from code (2026-08-16) — records the live EL start condition; re-decide when compose collapses at S2

This is **ADR-P3-14**. `[ARCH]` §10.4 class **(b)**: *"EL dependency is
`service_started`, never `service_healthy`"* and *"compose collapses to 2
services at S2; re-decide the EL start ordering then."* This file is the
R-17 placeholder.

## Context

Phase 3 adds geth as compose service `el` (`docker-compose.yml:175-219`,
exact tag `ethereum/client-go:v1.17.5`). Its healthcheck is
`eth.syncing == false` via `geth attach` over IPC (`:217-219`,
`docs/el-runbook.md:63-71`) — **synced**, not "the port is open."
`start_period: 120s`, `retries: 40`: a snap-syncing or restoring EL is
**unhealthy but running** for minutes to hours. That is intentional.
Compose must not restart-loop it.

`engine` is the only consensus service that `depends_on: el`. The
condition is **`service_started`**, never `service_healthy`
(`docker-compose.yml:113-117`). Phase A of acceptance exercises the
client **while** the EL catches up (`docs/running.md:435-440`). Waiting
on `service_healthy` would block the stack for the entire sync.

Consensus-to-consensus health is the opposite default: p2p / attestation /
engine / storage / beacon-api wait on `chain: service_healthy`. This
record is only the **EL** edge. `devnet/compose.yml`'s `service_started`
rows are fixture/publisher ordering, not this decision.

S2 folds the six consensus services toward `beacon-core` + EL
(`S2-J-01`). The compose graph that this condition sits in goes away.
Whether `beacon-core` should wait for a *started* EL, a *healthy* EL, or
neither (connect and defer — `pending_engine`) is a new question on a
two-service graph.

## Decision

**Until S2, `engine` depends on `el` with `condition: service_started`.
It does not wait for `service_healthy`.**

Keep EL health as `eth_syncing == false`, not a TCP probe. Do not flip
the engine→el condition to `service_healthy` to "make the stack wait for
sync." Phase A and soak start against an unsynced EL.

**Revisit at S2.** When compose collapses (`S2-J-01`), re-decide EL start
ordering on the two-service graph. Options belong in the S2 write-up:
keep `service_started`; wait for `service_healthy`; or drop the compose
dependency and let the engine client defer (`pending_engine` /
S0-A-27 deadlines). This file stays `proposed` until that revisit.

## Consequences

What this makes easy:

- The six-service stack comes up as soon as geth's process is running.
- A catching-up EL reports `unhealthy` without taking `engine` down
  with it.
- Phase A can talk Engine API during sync.

What this makes hard:

- `engine` may start before authrpc is accepting. That is a connect
  retry / deferral problem, not a compose-health problem. Do not
  "fix" it by switching this condition.
- Operators watching `compose ps` see `el` unhealthy for a long time
  while the rest is green. That is the design.

What this forbids:

- `depends_on: el: service_healthy` on the Phase-3 stack.
- Weakening the EL healthcheck to "port open" so that
  `service_healthy` becomes cheap enough to wait on.
- Closing the S2 entry gate by marking this `accepted`.

## Alternatives considered

**`service_healthy` (wait for `eth_syncing == false`).** Rejected for
Phase 3. Sync is minutes to hours; the stack would not start; Phase A
could not run.

**No `depends_on: el` at all.** A real alternative (engine retries until
the EL exists). Not taken for Phase 3: `service_started` is enough to
order the first boot without coupling to sync. Reconsider at S2.

**TCP healthcheck + `service_healthy`.** Rejected. CC-39/8: health is
sync, not bind. A listening authrpc on a syncing geth is not "healthy."

## Refactor impact

**Revisit at S2.** Compose collapses; re-decide EL start ordering.

| Stage | What happens to this record |
|---|---|
| S1 | This file. No compose change. |
| S2 | **Revisit** on the `beacon-core` + EL graph (`S2-J-01`). Supersede or re-accept. |
| S3+ | A leftover `service_healthy` wait on EL sync in the two-service compose is a defect unless the S2 write-up chose it. |
