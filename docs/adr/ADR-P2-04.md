# ADR-P2-04 — Only BLOCK is chain-authoritative

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-08, CC-22d, CC-2B, CC-2D
- **Citations:** 5 sites in the §10.4 census — `services/chain/src/p2p_stream.rs:12-14,684-689`; `services/p2p/src/gossip/validate/block.rs:1`; `services/p2p/src/gossip/validate/operations.rs:7`; `services/p2p/src/gossip/validate/sync.rs:3`; `services/engine/src/fastpath/mod.rs:120`; `[ARCH]` §2.5 / §10.4; live seam at `crates/seam/src/lib.rs:163-198`
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. It is why `[ARCH]` §2.5's `ChainIngress` is
**narrow**. Widening the ingress so chain becomes the gossip authority for
attestations, sync messages, operations, or columns contradicts this file.

## Context

Phase 2 has one live consensus object on the p2p↔chain stream: the
beacon block. Attestation / sync / operation pools are Phase 5/6.
Column sidecars are a DAS/relay concern, not a second fork-choice
input. If chain accepted every `GossipObject` kind as something it
must judge, the core thread would grow pools it does not own and
`ChainIngress` would become a general gossip bus.

p2p already runs the local-stage checks for those kinds (structure,
timing, dedup, signatures). A second, chain-side verdict would either
duplicate that work or silently override it.

## Decision

**Only `ObjectKind::Block` is chain-authoritative.**

- **BLOCK** (`beacon_block`): p2p does local stage (size, SSZ, timing,
  `(slot, proposer)` dedup) and **forwards** to chain. Consensus stage
  (parent, proposer index, finalized descent, transition) is chain's
  (`block.rs:1-6`). Engine names this owner `TriggerOwner::ChainBlock`
  (`fastpath/mod.rs:120-122`).
- **Sync / operations:** **p2p-authoritative**. REJECT never reaches
  chain (`operations.rs:7-10`; `sync.rs:3-6`). A validated operation
  is scored and forwarded on gossip and **not stored** (pools are
  Phase 5).
- **Non-block kinds on `P2pStream`:** chain **discards** them with a
  counter and replies IGNORE. No pool state is retained
  (`p2p_stream.rs:12-14,684-689`). They travel up the stream only so
  Phase 5/6 can later attach pools.

**This is why `ChainIngress` is narrow** (`crates/seam/src/lib.rs:194-198`):

| Method | Why it exists | Not authority for |
|---|---|---|
| `submit_gossip` | BLOCK verdicts (and only those chain must judge) | operations / sync / columns as consensus objects |
| `notify_data_available` | DA sampling signal; re-drives `pending_da` | proof of availability |
| `submit_column_sidecar` | relay onto the event bus (ADR-P4-03) | column validity — p2p/DAS owns that |

The trait does not grow `submit_attestation` / `submit_operation` /
`submit_sync` as chain-authoritative methods. Column sidecar on the
ingress is a **relay**, not a second authoritative kind.

## Consequences

What this makes easy:

- The core import lane sees blocks (and DA/column *signals*), not a
  mixed gossip taxonomy.
- p2p can REJECT a bad exit / slashing / sync message without a core
  round-trip.
- S1's `ChainIngress` stays two consensus verbs plus one relay.
  Reviewers can see a new method as a spec change.

What this makes hard:

- Phase 5/6 pools attach on the p2p side (or as new, named decisions).
  They do not appear by "just forwarding everything to chain."

What this forbids:

- Chain treating attestation / aggregate / sync / operations /
  columns as chain-authoritative gossip kinds (scoring, store, or
  REJECT from the core).
- Widening `ChainIngress` into a generic `submit_gossip(any kind) →
  chain verdict` bus so that p2p can stop being authoritative for
  non-blocks.
- Dropping p2p-side REJECT for operations/sync because "chain will
  catch it."
- Letting `submit_column_sidecar` become a fork-choice input.

## Alternatives considered

**Forward every `GossipObject` to chain and let the core classify.**
Rejected in `p2p_stream.rs:684-689`: Phase 2 has no pools; the discard
+ IGNORE is the recorded behaviour.

**Make chain authoritative for columns too.** Rejected: column
validity is p2p/DAS (KZG, custody, subnet). Chain relays SSZ without
decoding (ADR-P4-03) and owns only the *block-branch* DA trigger.

**Delay `ChainIngress` until Phase 5 so it can be "complete."**
Rejected: S1 needs a typed E1 now. Completeness here means *narrow
and honest*, not *every future kind*.

## Refactor impact

**Survives and is load-bearing.**

| Stage | What happens to this record |
|---|---|
| S1 | `ChainIngress` is typed around this split. Transport impls (`InProcess` / `Ipc`) do not add kinds. |
| S2 | Storage fold does not make columns chain-authoritative. Typed ingest (ADR-R-02 / P4-03 successor) is still not a gossip verdict. |
| S3 | M9 enters at `ChainIngress` with a **block**. |
| Phase 5/6 | Pools may consume operations/sync. That is a new owner, not a silent widening of chain authority. |
