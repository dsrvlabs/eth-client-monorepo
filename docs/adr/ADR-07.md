# ADR-07 — p2p dials chain; the health DAG roots at chain

- **Status:** proposed · revisit at S3 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 1 (direction of E1; health DAG root)
- **Issues:** S1-B-16, S2-A-07, S3a
- **Citations:** 1 site — `proto/eth/chain/v1/chain.proto:36-37` (representative). Direction also at `services/chain/src/p2p_stream.rs:3`; health DAG at `docker-compose.yml:62-64,82-84,110-112,142-144,164-166`
- **Provenance:** re-derived from code (2026-08-16) — records the live direction as an R-17 deferral, not as a final topology

This is **ADR-07**. `[ARCH]` §10.4 class **(b)**: *"decided again at S3 — direction is
meaningless once E1 may be in-process. Supersede with ADR-R-02."* This file is the
placeholder that makes the S2 entry gate satisfiable (R-17). It is not the S3
decision.

## Context

`P2pStream` is a bidirectional RPC on `ChainService`. The comment at
`chain.proto:36-37` states the direction: **p2p dials; chain is the server.**
That is what is implemented (`services/p2p/src/chain_stream/client.rs` opens
`ChainServiceClient`; `services/chain/src/p2p_stream.rs:3` serves). The health
DAG is built on the same fact: every consensus compose service
`depends_on: chain: service_healthy` (`docker-compose.yml` p2p / attestation /
engine / storage / beacon-api). Chain has no health peer. The DAG is acyclic
because clients point at chain and chain does not point back.

`[ARCH]` §2.3 E1 is this edge. Today it is a gRPC stream. S1 types it as
`cc-seam::{ChainIngress, P2pEgress}`. **The transport impl is selected at S3**
(§2.5 / R-14): in-process or a surviving IPC. Client-vs-server is a property
of a stream between two processes. Once both ends may live in one process,
"who dials" is not a topology question.

`[ARCH]` §10.5's **ADR-R-02** (created at S2, `S2-A-07`) lists *"the direction
half of ADR-07"* among what it supersedes. That is the S2 half of the chain
(archive ownership / event bus not a data plane). It is **not** the S3
re-decision of E1's transport. Do not treat ADR-R-02 landing as this record
becoming `accepted`.

## Decision

**Until S3, p2p dials chain. Chain is the gRPC server. The health DAG roots at
chain.**

Do not invert the stream (chain dials p2p) to "fix" a compose cycle. Do not
add a chain → p2p health edge. Do not introduce a `cc-chain` → `cc-p2p` or
`cc-p2p` → `cc-chain` call that is not routed through `cc-seam` (`[ARCH]`
§2.5 check).

**Revisit at S3.** When E1's transport impl is selected, re-decide this
record. If the impl is in-process, client/server direction is deleted with
the stream; record that and point `superseded-by` at the S3 write-up (or at
ADR-R-02 if that file absorbed the direction half and S3 has nothing left to
say). If the impl is still IPC, re-state who dials and whether the health
DAG still roots at a chain-shaped process.

This file stays `proposed` until that revisit. Do not flip it to `accepted`
to close a gate.

## Consequences

What this makes easy:

- The live tree and the compose DAG stay consistent: one server, many
  clients, no cycle.
- S1 can type E1 without pretending the gRPC direction is the end state.
- S2 reviewers have a named document to supersede rather than a comment on
  `chain.proto:36`.

What this makes hard:

- Anyone who wants chain to dial p2p (or a mutual health pair) before S3
  has to supersede this file, not edit the proto comment.
- A parked chain still takes the DAG red. That is intended today and is a
  different record (`ADR-P3-02` / M8) once the core can be parked while
  the health task answers.

What this forbids:

- Treating "p2p dials chain" as a load-bearing invariant of the S3
  topology.
- Closing the S2 entry gate by marking this `accepted`.
- Growing a chain ↔ p2p health edge, or a direct crate edge, "because S3
  will delete it anyway."

## Alternatives considered

**Invert now (chain dials p2p).** Rejected. It would cycle the health DAG
(`p2p depends_on chain` today) and move a working client for no S1/S2 gain.
E1's transport is not selected yet.

**Declare the direction deleted at S1 when the seam lands.** Rejected. The
gRPC stream still exists through S2. Deleting the words while the RPC
remains is the silent-deferral R-17 forbids.

**Wait for ADR-R-02 and write nothing.** Rejected. ADR-R-02 is created at
S2 and is about archive ownership and overflow policy. The S2 entry gate
needs a resolving document *now*.

## Refactor impact

**Revisit at S3.** Direction is meaningless once E1 may be in-process.

| Stage | What happens to this record |
|---|---|
| S1 | This file. No code change. Seam typing does not flip who dials. |
| S2 | ADR-R-02 (`S2-A-07`) may supersede the *direction half* as part of folding storage. The S3 re-decision remains. |
| S3 | **This ADR is revisited.** Record the chosen E1 impl. Supersede or re-accept. |
| S3+ | A leftover "p2p must dial chain" comment on an in-process path is a defect against the S3 write-up. |
