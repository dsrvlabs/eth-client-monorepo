# ADR-P2-06 — Snappy framing is a gossipsub `DataTransform`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09
- **Citations:** 1 site — `crates/libp2p/src/snappy.rs:1-19,41-70`
- **Provenance:** re-derived from code (2026-08-16)

## Context

Ethereum gossip payloads are snappy-framed. The decode has to run where
gossipsub hands the raw bytes over — inside the swarm task — so an
80 MB-expanding payload would stall every other swarm event if the
decompressor were unbounded. Phase 2 put snappy behind gossipsub's
`DataTransform` trait and wrapped the decoder in `Read::take` so the
uncompressed output cannot exceed `GOSSIP_MAX_SIZE` (10 MiB).

`SnappyTransform` is the production transform (`snappy.rs:1-19`). Inbound:
`FrameDecoder` + `take(max + 1)`, reject if `len > max`. Outbound: reject
if the uncompressed payload already exceeds `max`, then `FrameEncoder`.
Per-container SSZ bounds are a later check (CC-22b); this record is the
framing ceiling.

## Decision

**Gossipsub compression is a `DataTransform`, not a manual
pre-/post-process at each publish or validate site.**

Use snappy **framing** (`snap::read::FrameDecoder` /
`snap::write::FrameEncoder`), not the raw block codec. Apply one
uncompressed ceiling (`GOSSIP_MAX_SIZE = 10 MiB` in production) on both
directions. Decompression stays inside the swarm task and stays bounded
by `Read::take`.

## Consequences

What this makes easy:

- Every gossip topic gets the same framing and the same ceiling.
- An expanding payload is `InvalidData` at the transform, not a
  multi-megabyte `Vec` in a validator.
- S3 can move the type under `cc-wire` without changing the contract.

What this makes hard:

- The transform runs on the swarm task (ADR-P2-02). The ceiling is what
  keeps that legal. Raising `GOSSIP_MAX_SIZE` is a swarm-latency decision,
  not a "just a constant" edit.
- Callers must not snappy-frame a second time before `publish`.

What this forbids:

- Unbounded snappy decode on the gossip path.
- A per-topic ad-hoc compressor that bypasses `DataTransform`.
- Treating the 10 MiB ceiling as a per-container SSZ bound — those are
  CC-22b, applied after this transform.

## Alternatives considered

**Decode in the gossip validation worker, not in the transform.** Rejected.
Gossipsub would still hold the raw frame, and an unbounded decode would
run later rather than not at all. The bound has to sit at the first
expansion.

**Raw snappy block format.** Rejected. The spec wire is framed.

## Refactor impact

**Survives.** Moves under `cc-wire`'s orbit at S3 (`[ARCH]` §10.4). The
`DataTransform` + `Read::take` contract does not change when the crate
does.
