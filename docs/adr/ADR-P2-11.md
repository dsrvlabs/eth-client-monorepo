# ADR-P2-11 — `ColumnSidecar` on `P2pStream` is contract-only; no producer

- **Status:** proposed · revisit at S2 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 2 (oneof reserved for Phase 4 storage; S2 deletes the path)
- **Issues:** S1-B-16, S2-A-04, S2-A-05, S2-A-07
- **Citations:** 1 site — `proto/eth/p2p/v1/p2p.proto:100-101` (message `:123-130`)
- **Provenance:** re-derived from code (2026-08-16) — records the contract-only arm; supersede at S2 with ADR-R-02

This is **ADR-P2-11**. `[ARCH]` §10.4 class **(b)**: *"`ColumnSidecar` on the
stream is contract-only in Phase 2; no producer"* and *"S2 changes the column
path entirely (§4.3); supersede with ADR-R-02."* This file is the R-17
placeholder.

## Context

`P2pToChain` carries a `ColumnSidecar` oneof arm (`p2p.proto:95-103`).
The comment at `:100` is the decision: **defined for Phase 4 storage; no
Phase 2 producer.** The message is SSZ bytes plus fork, block root,
column index, subnet (`:123-130`). It is a reservation so Phase 4 could
be additive on the existing stream.

Chain will **relay** a received arm into the event bus as `DATA_COLUMN`
without decoding (`services/chain/src/p2p_stream.rs:17-18`, ADR-P4-03).
That is a server behaviour, not a producer. Production p2p does not
construct `P2pToChain.column`. Engine injects columns on **`EngineStream`**
(`InjectColumns`), a different contract (E8), not this oneof.

The reserved arm is how columns were supposed to become durable: p2p →
chain stream → event ring → storage write-behind. `[ARCH]` §4.3 verified
that path as the three-link durability bug (undecoded SSZ, evicting
ring, `CURSOR_TOO_OLD` gap-fill). S2 deletes it. Columns go
`chain-core` → `ArchiveWrite::ingest_columns` as a typed `ColumnBatch`.
They never enter the ring. ADR-R-02 (`S2-A-07`) is the supersession:
`beacon-core` owns redb; the event bus is not a data plane.

## Decision

**Until S2, `ColumnSidecar` on `P2pStream` stays contract-only. Do not
add a production producer.**

Do not start publishing `P2pToChain.column` from `services/p2p` in S1
"to exercise storage." Do not treat the oneof as the S2 ingest API.
Chain may keep the relay so a test can push an arm; that is not a
producer.

**Revisit at S2.** Supersede this file with **ADR-R-02** when the typed
column ingest lands (`S2-A-04` / `S2-A-05` / `S2-A-07`). After
supersession the oneof is historical; deleting it is a proto change
owned by that stage, not by this record. This file stays `proposed`
until then.

## Consequences

What this makes easy:

- Phase 2 did not have to invent a column RPC. The arm is a name and a
  comment.
- S2 can delete a reservation instead of a live producer.
- ADR-P4-03 (relay without decode) stays a separate (b) row (`S1-B-14`).

What this makes hard:

- Anyone grepping `ColumnSidecar` will find a proto message and a relay
  and assume a path exists. It does not, on the production p2p side.
- Tests that inject the arm can look like a producer. They are not.

What this forbids:

- A production `P2pToChain.column` sender before the S2 supersession.
- Using this oneof as the post-S2 archive ingest.
- Closing the S2 entry gate by marking this `accepted`.

## Alternatives considered

**Produce the arm now (p2p decodes gossip columns and sends them on
`P2pStream`).** Rejected. That would make the event ring a live column
data plane — the §4.3 class S2 exists to delete — and it would ship a
producer the week before the path is removed.

**Remove the oneof in S1.** Rejected. Breaking proto for a reservation
that S2 will supersede is churn. S1-B-17's stale-citation work is a
different id.

**Wait for ADR-R-02 and write nothing.** Rejected. R-17.

## Refactor impact

**Supersede at S2 with ADR-R-02.** The column path is replaced, not
tuned.

| Stage | What happens to this record |
|---|---|
| S1 | This file. No producer. |
| S2 | **Revisit.** Typed ingest + ADR-R-02 supersede this. Mark `superseded; superseded-by ADR-R-02`. |
| S3+ | A new `P2pToChain.column` sender is a defect against ADR-R-02. |
