# ADR-P2-10 — Topic P3/P3b scoring weight is 0 on every topic

- **Status:** proposed · revisit at S3 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 2 (CC-22c / OQ-P2-3 deferred)
- **Issues:** S1-B-16, S3a-B-02, P0-17a
- **Citations:** 1 site — `services/p2p/src/gossip/scoring.rs:5-6` (construction `:363-371`; tests `:815-821`; generated `docs/p2p-scoring.md:7`)
- **Provenance:** re-derived from code (2026-08-16) — records the P3/P3b zero as an R-17 deferral; P0-17a ends it at S3

This is **ADR-P2-10**. `[ARCH]` §10.4 class **(b)**: *"gossip topic scoring
weight **0** on every topic; `OQ-P2-3` deferred"* and *"[PRD] P0-17a wires
§5.6 scoring at S3 — the deferral ends; record the new weights."* The
§10.4 one-liner means **P3 / P3b**. Family topic weights (`WEIGHT_BEACON_BLOCK
= 0.5`, column family 0.5, …) are **ADR-P2-07**, already accepted.

## Context

`build_scoring_config` is the single shipped scoring definition
(`scoring.rs:1-14`). P1, P2, and P4 are populated from the §5.6 table.
**P3 (`mesh_message_deliveries_weight`) and P3b
(`mesh_failure_penalty_weight`) are 0 on every family**, with decay / cap
/ threshold / window also zeroed (`scoring.rs:247-255,363-371`). The
module comment and the generated doc say so (`scoring.rs:5-6`,
`docs/p2p-scoring.md:7`). Tests lock every family's P3/P3b at 0.0
(`scoring.rs:815-821`).

That is **OQ-P2-3 deferred**: mesh-delivery and mesh-failure terms are
specified in §5.6 but not turned on. Invalid-message (P4) still scores.
A peer is not penalised by P3 for failing to deliver on the mesh, so the
W3 Phase-2 clause (*scoring penalty crossing the −4000
`GossipThreshold` bucket*) is not reachable from mesh-delivery failure
while these weights stay 0 (`S3a-B-02`).

P0-17a at S3 (`S3a-B-02`) wires §5.6 scoring for real — subscribe has a
production sender (`S3a-B-01`) and the deferred P3/P3b (and any other
still-zero §5.6 term this record owns) get their table values. The
deferral ends by **writing the new numbers into this ADR**, not by
leaving a comment at `scoring.rs:6`.

Column-family **total** 0.5 at every `cgc` is ADR-P2-07. App-score
disconnect is ADR-P2-09. Neither is this file.

## Decision

**Until S3, P3 and P3b weight 0 on every topic.**

Do not ship non-zero `mesh_message_deliveries_weight` or
`mesh_failure_penalty_weight` in an S1 or S2 PR. Do not "fix" W3 by
turning P3 on locally. Family topic weights and P1/P2/P4 stay under
their own records / the existing constructor.

**Revisit at S3 (`S3a-B-02` / P0-17a).** The deferral ends. Record the
new §5.6 P3/P3b weights (and any other term P0-17a changes) in this
file, or supersede it with a write-up that contains the table. Flip
status only as part of that edit. This file stays `proposed` until then.

## Consequences

What this makes easy:

- Gossipsub mesh-delivery noise cannot graylist a peer via P3 before
  subscribe is even wired.
- ADR-P2-07 can hold the family-total invariant without fighting a
  moving P3 contribution to `max_positive_score`.
- S3 has one place to write the new weights (`S3a-B-02` names this
  file).

What this makes hard:

- W3's −4000 bucket is not exercisable from P3 while this stands.
- Operators reading `docs/p2p-scoring.md` see P3/P3b columns of 0 and
  may think scoring is "off." P1/P2/P4 are not off.

What this forbids:

- Non-zero P3/P3b before the S3 revisit.
- Changing `WEIGHT_*` family totals in the name of P0-17a (that is
  ADR-P2-07).
- Disconnecting on gossip score because P3 is zero (ADR-P2-09).
- Closing the S2 entry gate by marking this `accepted`.

## Alternatives considered

**Ship §5.6 P3/P3b now.** Rejected. OQ-P2-3 deferred them; P0-17a's
subscribe sender is not production yet (`host.rs` dead island). Scoring
penalties without a live subscribe path are untestable numbers.

**Zero every topic weight, including P1/P2/P4 / family weights.** Not
what the code does, and not this decision. `[ARCH]` §10.4's shorthand is
the P3/P3b deferral.

**Leave the zero undocumented until S3.** Rejected. R-17.

## Refactor impact

**Revisit at S3.** P0-17a wires §5.6; record the new weights.

| Stage | What happens to this record |
|---|---|
| S1 | This file. Constructor stays at weight 0. |
| S2 | Untouched. |
| S3 | **`S3a-B-02` revisits this ADR** and writes the §5.6 P3/P3b weights. |
