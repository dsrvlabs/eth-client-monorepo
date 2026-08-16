# ADR-P3-13 — Soft deadline is `ATTESTATION_DUE_BPS` × slot duration, as a counter

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 3
- **Issues:** S1-B-10, S1-A-15, CC-3Aa
- **Citations:** 5 sites — `services/engine/src/metrics.rs:8,28,445,486,534`
- **Provenance:** re-derived from code (2026-08-16)

## Context

The attestation due time is not 4.0 s. It is
`ATTESTATION_DUE_BPS × SLOT_DURATION_MS / 10_000`, derived at runtime
(`crates/engine-api/src/config.rs:215-252`): 3 999.6 ms on Hoodi
(3 333 × 12 000), ≈ 3 000 ms under a Gloas-like 2 500 bps. A compile-time
histogram boundary of `4.0` on `cc_engine_request_seconds` would silently
become wrong at the next fork (`services/engine/src/metrics.rs:8-13`). The
operator multiplier scales the five transport timeouts only — never this
deadline (`config.rs:48`). Exceeding it warns and counts; it never aborts.
`[ARCH]` §7.2 names this substitution as the precedent for S1's one-slot
liveness probe (`S1-A-15`).

## Decision

Substitute the soft deadline at runtime from `ATTESTATION_DUE_BPS` and
`SLOT_DURATION_MS`. Observe misses on
`cc_engine_soft_deadline_exceeded_total` — a **counter**, not a `4.0`
bucket on `cc_engine_request_seconds`. Do not configure the soft deadline
as a transport timeout. Do not derive it from `SECONDS_PER_SLOT`.

## Consequences

What this makes easy:

- Hoodi and a shorter-due fork share one formula. Dashboards read a
  counter that means "missed the current due time," not "crossed 4 s."
- S1's core-liveness probe can reuse the same substitution (`[ARCH]` §7.2).

What this makes hard:

- Request-latency histograms no longer have an exact attestation boundary.
  Fastpath histograms may still mark 4.0 for other reasons; that is not
  this deadline.

What this forbids:

- Baking `4.0` into `REQUEST_SECONDS_BUCKETS` as the attestation due time.
- Scaling the soft deadline by `timeouts.multiplier`.
- Treating a soft-deadline miss as an abort or a transport timeout.
- Deriving the due time from `SECONDS_PER_SLOT`.

## Alternatives considered

**A 4.0 s bucket on `cc_engine_request_seconds`.** Rejected in the module
docs: the due time is runtime-derived and fork-dependent.

**Make the soft deadline the `newPayload` timeout.** Rejected: transport
timeouts are the five spec knobs (default 8 s for `newPayload`); the soft
deadline is a budget signal that never aborts.

## Refactor impact

**Survives.** Relevant to `[ARCH]` §7.2's one-slot liveness deadline.
Metrics move with the engine crate at S1; the formula already lives in
`cc-engine-api`. A later metric reshape (`[ARCH]` §7.3) must keep the
counter and the runtime substitution.
