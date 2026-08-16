# ADR-P3-08 — `eth_syncing` is the upcheck; it never rides the ordered lane

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-10, CC-36
- **Citations:** 1 site — `services/engine/src/methods/eth_syncing.rs:5`
- **Provenance:** re-derived from code (2026-08-16)

## Context

The Engine API client has two concerns that must not share a queue: payload
notification (`newPayload` / `fcU` on the ordered lane) and EL health. Health
is `eth_syncing` — a non-Engine JSON-RPC method that is nevertheless in
`ADVERTISED_CAPABILITIES` (`services/engine/src/capabilities.rs:25-29`). A
stalled probe that sat on the same HTTP lane as `newPayload` would hold the
attestation deadline hostage for the 30 s mock stall the CC-36 /2 test
constructs (`eth_syncing.rs:94-179`). The four-state machine
(`services/engine/src/state.rs`) consults this probe to decide whether ordered
calls are admitted (`admits_el_call` is Online-only).

## Decision

Issue `eth_syncing` on the **upcheck lane** with the 1 s `eth_syncing`
timeout. Never put it on the ordered lane. A stalled health probe must not
block `newPayload`. Classify `false` as not-syncing and every other JSON
value as syncing; feed that outcome into the four-state machine. Keep
`eth_syncing` in the advertised capability set.

## Consequences

What this makes easy:

- A hung EL health endpoint times out on its own lane; `newPayload` still
  completes inside the ordered-lane timeout.
- The health machine has one probe and one classification (`false` vs
  everything else). Auth failures on the probe are terminal `AuthFailed`.

What this makes hard:

- Two lanes and two timeouts must stay wired. Collapsing them "to simplify
  the transport" re-creates the CC-36 /2 stall.
- `ADVERTISED_CAPABILITIES` is not an Engine-only list; capability
  negotiation includes this non-Engine method ([PRD] P2-E/2).

What this forbids:

- Running `eth_syncing` on `Lane::Ordered`.
- Treating a stalled upcheck as a reason to delay or drop `newPayload`.
- Dropping `eth_syncing` from the advertised set without a new record.

## Alternatives considered

None recorded at the time. The live tree records the lane split and the stall
test; it does not record a rejected alternative.

## Refactor impact

**Survives.** Interacts with [PRD] P2-E/2 (`ADVERTISED_CAPABILITIES` includes
non-Engine `eth_syncing`). The method file moves with `methods/` at `S1-A-05`;
the lane split must move verbatim.
