# ADR-P3-07 — p2p publishes only custody-sampled columns; engine injects only subscribed indices

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-09, CC-37b, CC-38b
- **Citations:** 9 sites — `proto/eth/p2p/v1/p2p.proto:258-261`; `services/p2p/src/engine_stream/mod.rs:3-11`; `services/p2p/src/engine_stream/subscription.rs:1-12`; `services/p2p/src/engine_stream/inject.rs:701-716`; `services/engine/src/fastpath/filter.rs:1-23,110-117`; `services/engine/src/fastpath/mod.rs:469-470`
- **Provenance:** re-derived from code (2026-08-16)

## Context

A Fulu node custodians / samples a sparse subset of the 128 column
subnets (`cgc = 4` → `sampling_size = 8` on Hoodi). The EL path can
assemble **all 128** sidecars from `engine_getBlobs`. Publishing the
unsubscribed 120 would put ~5.6 MB on the engine→p2p hop instead of
~353 KB, and it would put the node on subnets it did not subscribe to.

`fulu/p2p-interface.md` says publish **if and only if** subscribed.
`das-core.md`'s cross-seeding SHOULD is written for reconstruction from
50 %+ of columns, **not** for the EL path; the two are not reconciled at
`v1.7.0-alpha.13`. Phase 3 takes the **narrow** reading and does not
publish to unsubscribed subnets on the strength of that SHOULD
(`filter.rs:4-11`).

The filter has two halves:

1. **Engine (before the process boundary).**
   `filter_subscribed` reads the subscription set **before** the outbound
   `InjectColumns` message is constructed (`filter.rs:13-17,110-117`;
   `fastpath/mod.rs:469-470`). Remainder is **dropped, not queued**
   (`filter.rs:19-23`). Empty set ⇒ publish nothing (fail-closed). Engine
   never invents `0..8`; the set is custody-sampled indices from p2p
   (`filter.rs:36-43`).
2. **p2p (publish-iff-subscribed).**
   `SubscriptionSet` on the stream (`p2p.proto:258-261`) carries those
   indices + `cgc`. p2p produces it on `EngineHello` and on every `cgc`
   change (`subscription.rs:1-12`). Inject step 4 publishes only if
   `sub.is_subscribed(column_index)` (`inject.rs:701-716`;
   `engine_stream/mod.rs:11`). An unsubscribed inject is counted and
   dropped — engine should never have sent it.

The `trusted_local` KZG-skip on the same inject path is **not** this
record (ADR-P3-15 / `S1-B-13`).

## Decision

**p2p publishes only its custody-sampled column indices. Engine injects
only subscribed indices, filtered before the outbound message exists.**

- p2p is the source of truth for the set (`LocalSubscription` /
  wire `SubscriptionSet`). It sends the set on hello and on every
  `set_custody_group_count`.
- Engine expands nothing from `cgc` alone. It filters assembled sidecars
  against the last set **before** constructing `InjectColumns`.
- Unsubscribed sidecars are dropped. No queue of "the other 120".
- p2p's inject path is a second close of the same filter, not a different
  policy: unsubscribed ⇒ do not `publish_column`.
- `{subscribed="false"}` on `cc_engine_sidecars_published_total` stays at
  zero.

## Consequences

What this makes easy:

- The engine→p2p hop stays 8 sidecars, not 128, on the production
  custody size.
- A `cgc` change moves both halves: p2p rebuilds the set, engine's next
  filter uses it.
- S1 folding engine in-process does not license publishing the remainder
  "because the hop is gone". The spec reading is subscribe-iff, not
  "the gRPC was expensive".

What this makes hard:

- Reconstruction / cross-seeding of unsubscribed columns is not this
  node's EL-path job. Recovery is the by-root ladder (CC-25), not
  over-publish.
- An empty subscription is a silent publish-nothing, not a default
  `0..8`.

What this forbids:

- Engine inventing a default column set (`0..8` or `0..cgc`).
- Queuing unsubscribed sidecars "until we subscribe".
- Publishing an injected (or locally-assembled) sidecar whose index is
  not in the current set.
- Taking `das-core.md`'s cross-seeding SHOULD as permission to ignore
  `fulu/p2p-interface.md`'s iff-subscribed rule on the EL path.

## Alternatives considered

**Publish all 128 on the EL path because das-core SHOULD cross-seed.**
Rejected (`filter.rs:4-11`). The SHOULD is a reconstruction rule, not an
EL-publish rule, and the two docs are not reconciled.

**Queue the unsubscribed 120 until `cgc` rises.** Rejected
(`filter.rs:19-23`). A 120-sidecar-per-block queue is a memory leak with
a plausible-sounding justification.

**Filter only on the p2p side, send all 128 across EngineStream.**
Rejected. At `cgc = 4` that is a 16× hot-path copy for bytes that must
not be published.

**Engine derives indices from `cgc` locally (`0..sampling_size`).**
Rejected (`filter.rs:36-38`). Custody sampling is `node_id`-dependent;
a dense prefix is the wrong set.

## Refactor impact

**Survives.** After S1 the engine half is in-process (`[ARCH]` §2.3 E8
becomes a `P2pEgress` method). The filter still runs **before** the
sidecar is offered to gossip, and p2p still refuses to publish an
unsubscribed index. Do not drop the engine-side filter as a "dead
process-boundary optimisation".
