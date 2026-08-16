# ADR-P2-05 — `ChainView` is chain-owned and pushed to p2p

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 2
- **Issues:** S1-B-09, CC-27b
- **Citations:** 2 sites — `services/p2p/src/chain_stream/view.rs:1-4`; `proto/eth/p2p/v1/p2p.proto:169-176` (seam restatement: `crates/seam/src/lib.rs:117,215-220`)
- **Provenance:** re-derived from code (2026-08-16)

## Context

Status, the slot clock, column gossip validation, and the gap detector all
need the same consensus snapshot: slot, epoch, head, justified, finalized,
and (on epoch / full ticks) proposer lookahead. If p2p polled chain for those
fields, every consumer would grow its own head cache and its own notion of
"now". Phase 2 made the view **chain-owned and pushed**.

The wire type is `ChainView` on the p2p stream (`p2p.proto:169-176`). Cadence
is explicit:

| `view_kind` | When | Fields |
|---|---|---|
| `VIEW_KIND_SLOT_TICK` (1) | every slot | 1–8 |
| `VIEW_KIND_EPOCH_TICK` (2) | every epoch | 1–8 + 11–13 |
| `VIEW_KIND_HEAD_CHANGE` (3) | head change | 1–8 |
| `VIEW_KIND_FULL` (4) | `StreamHello` / reconnect | 1–13 |

p2p stores the latest push in an `ArcSwap` (`ChainViewStore`). One writer —
the stream client — many readers. Readers pointer-load; they never dial
chain (`view.rs:1-4,20-58`). `has_view()` is a store helper (false on the
default view). Handshake **does not read it**: `local_status` loads the
store and builds Status from whatever is there (`handshake.rs:99-101`,
`status.rs:105-117`). Until the first chain push that is the default proto
view (empty roots / zero slots). `has_view()` does not keep a pre-push
Status off the wire.

S1 restates the same push as `P2pEgress::update_view`
(`crates/seam/src/lib.rs:215-220`): an `ArcSwap` store, never blocks,
never fails, **not a p2p self-write**.

## Decision

**Chain is the sole writer of `ChainView`. p2p is a subscriber.**

Publish on the four cadences above. p2p installs each push into one
`ArcSwap<ChainView>` and every local consumer reads that handle. Do not add
a second head poller. Do not let p2p invent slot / head / finalized to
"keep Status alive" before the first push.

`update_view` is the in-process form of the same push. Substance is
unchanged: chain-owned, pointer-load, no backpressure.

## Consequences

What this makes easy:

- Status, the clock, column validation, and gap detection share one snapshot.
- S1's `P2pEgress::update_view` is a rename of the store, not a redesign.
- A reconnect is one `VIEW_KIND_FULL` snapshot, not a cache rebuild.

What this makes hard:

- Live Status is a store load, not a `has_view()` gate. Before the first
  chain push, `build_local_status` emits the default (zeroed) view.
  Clock config is a bootstrap stand-in only (`service.rs:281-282`).
- Adding a field to the view is a proto / seam change, not a p2p-local
  cache tweak.

What this forbids:

- A p2p-owned head poller or a second `ChainView` writer.
- p2p self-writing the store to paper over a missing push.
- Treating `update_view` as a blocking or fallible seam method.

## Alternatives considered

**p2p polls `GetHead` / Status from chain on a timer.** Rejected in the
module docs (`view.rs:4`, "Do not add a second head poller — §16/3"). It
duplicates chain's head cache and races the slot tick.

**Each consumer holds its own copy and refreshes independently.** Rejected.
That is how slot, Status, and the column validator drift by a slot.

## Refactor impact

**Survives in substance.** Becomes `P2pEgress::update_view` (`[ARCH]` §2.1 /
§10.4) — still an `ArcSwap` store. The gRPC `ChainView` message may die with
the stream; the ownership rule does not.
