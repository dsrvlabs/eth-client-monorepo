# ADR-P4-12 — The wire prober has its own codec, deliberately independent

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4 (survives and is **load-bearing**; [ARCH] §9.1 S3)
- **Issues:** S1-B-07, CC-4B, S0-B-07, P0-06
- **Citations:** `bin/serve-probe/src/lib.rs:1-4`; `bin/serve-probe/src/codec.rs:1-4`; `bin/serve-probe/src/protocols.rs:70-72`; `bin/serve-probe/Cargo.toml:22-26`; `scripts/check-crate-dag.sh:291`; `docs/phase-4-soak.md:35-37,68-76`; `plan/architecture.md` §1.4 `cc-wire` / §9.1 S3; `plan/prd.md` P0-06
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-P4-12**. It is why **[PRD] P0-06 exists**. A later stage
that "simplifies" the probe onto the node's codec, or onto `cc-wire`,
deletes the only independent falsifier the serve window has. Write the
contradiction here, or do not do it.

## Context

`bin/serve-probe` (`cc-serve-probe`) is a wire-only serve-window prober.
It speaks Status v2 and the ByRange / ByRoot block and column protocols
against a foreign (or our own) libp2p transport. Its job is to fail
when *our node* is off-spec, not to agree with our node.

That job is impossible if the probe and the node share an encoder. P0-06
is the existence proof: the prober reproduced the **same** bogus 4-byte
`BlocksByRoot` offset as `services/p2p`, so probe and node agreed with
each other and both diverged from spec — defeating the stated purpose
(*independent so a shared bug cannot make the probe pass against our
own node*). `S0-B-07` fixed the probe **from the spec, in a separate
PR**, not by copying the node's fix. Copying `S0-B-05` across would
have re-created the coupling this record forbids.

The live crate already encodes the independence:

- `lib.rs:1-4` — own varint + snappy-frame + result-byte codec against
  `cc-libp2p` transport (Noise + yamux). **Does not link
  `services/p2p`.**
- `codec.rs:1-4` — intentionally independent of
  `services/p2p::reqresp::codec`.
- `protocols.rs:70-72` — SSZ limits are **copied, not shared**, with
  `cc_p2p::reqresp::Protocol::request_limits` and
  `cc_libp2p::request_limits`.
- `Cargo.toml:22-26` / `check-crate-dag.sh:291` — allowed workspace
  edges are only `{cc-libp2p, cc-types, cc-config}`. No `cc-p2p`, no
  `cc-store`, no future `cc-wire`.

`[ARCH]` §1.4 introduces `cc-wire` at S3 as *one* SSZ+snappy codec
because today's three hand-rolled copies already disagree. The same
row says **`bin/serve-probe` must not take that dependency** — this
ADR is the reason. §9.1 S3 **does not delete** the probe's codec when
it deletes the other two copies.

Foreign-implementation handshake counts (M2e / `docs/phase-4-soak.md:68`)
are the external falsifier. They remain `NOT_RUN`. Local dual-swarm
tests and the negative stub do not replace them, and they do not
justify sharing a codec "until OQ-1 runs."

## Decision

**The wire prober keeps an independent codec for the life of the
binary.** Independence is a DAG fact and a source fact, not a comment.

1. **`cc-serve-probe` must not depend on `cc-p2p`, `services/p2p`,
   `cc-store`, or `cc-wire`.** The `check-crate-dag.sh` row stays
   `{cc-libp2p, cc-types, cc-config}`. When `cc-wire` lands (`S3a-A-02`),
   the same PR adds an explicit prohibition in the style of S1-A-01.
   Taking `cc-wire` is a defect against this file, not a tidy-up.

2. **Do not share modules with the node.** `request_limits`, framing,
   and SSZ layouts in the probe are copies. A constant that must change
   in both places is changed in both places, from the spec, in separate
   reviews. A shared `const` crate that both link is the P0-06 shape
   with extra steps.

3. **Do not fix the probe by copying the node's encoder.** P0-06 exists
   because that is what happened. A probe bug is a **separate PR
   derived from the spec** (`S0-B-07` / `[PLAN]` README). The node's
   fix is evidence, not a source.

4. **Do not delete this codec at S3.** `[ARCH]` §9.1 S3 deletes
   `services/p2p/src/reqresp/` and the duplicate `request_limits` in
   `crates/libp2p`. It **does not** delete `bin/serve-probe/src/codec.rs`.

The transport (Noise + yamux via `cc-libp2p`) is shared on purpose: the
probe must dial the same stack. The *codec* is what must not be shared.

## Consequences

What this makes easy:

- A shared framing bug cannot make the probe pass against our node.
- P0-06 / M2e stay meaningful: foreign peers and an independent encoder
  are the falsifier, not a round-trip through our own `encode_request`.
- S3 can introduce `cc-wire` without silently recruiting the probe as
  a second client of the same crate.

What this makes hard:

- Three (then two) codec implementations. Limits and SSZ layouts will
  drift; CI and M2e are how drift is caught, not a shared module.
- Every probe wire fix is a second, spec-derived patch.

What this forbids:

- `cc-serve-probe` taking `cc-wire`, `cc-p2p`, `services/p2p`, or
  `cc-store`.
- "Fix the probe by importing the node's encoder / limits / SSZ types."
- Deleting `bin/serve-probe/src/codec.rs` when S3 consolidates the
  node's copies.
- Treating a green probe-vs-our-node test as codec validation (that is
  the P0-06 failure shape).
- A `pub use` of probe codec types from `cc-wire` or the reverse.

## Alternatives considered

**One codec (`cc-wire`) for node and probe.** The S3 simplification.
**Rejected.** It is exactly the coupling P0-06 documented. `[ARCH]`
§1.4 and §9.1 S3 already carve the probe out of that consolidation.
Taking `cc-wire` requires a superseding ADR that explains how a shared
bug can no longer green-wash our node. This file is not that ADR.

**Share only `request_limits`, keep framing independent.** Rejected by
the live `protocols.rs:70-72` comment and by P0-06's sibling (empty
`ColumnsByRoot` offset): the bug class is "copied the wrong layout from
ourselves." Shared limits re-create it for the next constant.

**Drop the probe and rely on foreign peers only.** Rejected. OQ-1 is
`NOT_RUN`; the local negative stub is what currently proves
below-window empty-success fails. The probe stays. Its codec stays
independent.

**Fix P0-06 by copying `S0-B-05` into the probe.** Rejected at
`S0-B-07`: that is the coupling. The probe fix was a separate,
spec-derived PR (`5eebbbb`).

## Refactor impact

**Survives and is load-bearing.**

| Stage | What happens to this record |
|---|---|
| S0 | P0-06 / row 31 discharged by `S0-B-07` **from the spec**. Independence kept. |
| S1 | Untouched. No `cc-wire` yet. DAG row stays `{cc-libp2p, cc-types, cc-config}`. |
| S2 | Untouched. Probe still must not take `cc-store`. |
| S3 / `S3a-A-02` | `cc-wire` is born. **This file forbids the probe taking it.** Add the dag prohibition in that PR. Delete the other two codec copies; **keep this one.** |
| S3b / M2e | Foreign-implementation handshake is the external falsifier. A pass against our own encoder still proves nothing. |
| S4+ | Independence remains. A "just depend on `cc-wire`" cleanup is a defect against this record. |
