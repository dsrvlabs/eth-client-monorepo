# ADR-P1-15 — Latency SLOs are histogram bucket fractions, never quantile interpolation

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, CC-1C, CC-1Ad
- **Citations:** 2 sites in the §10.4 census — `services/chain/src/metrics.rs:6`; live buckets at `metrics.rs:3-6,37-58`; soak method at `docs/phase-1-soak.md:339-376`, `docs/running.md:332`, `scripts/soak-report.sh:7-9,19`; CI ceiling at `services/chain/tests/timing.rs:431`; `[ARCH]` §7.3; `plan/issues/s3b-acceptance-windows.md` S3b-W-02
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing**. `[ARCH]` §7.3's metric reshape, S3b's
Hoodi soak, and any new histogram family must preserve it. Reading a
latency SLO off `histogram_quantile` (or any interpolated p95) contradicts
this file.

## Context

Production budgets are **block p95 ≤ 400 ms** and **epoch p95 ≤ 1000 ms**
on the soak machine (`metrics.rs:37-41`; `timing.rs:430-431`). Prometheus
`histogram_quantile` interpolates inside a bucket. That interpolation is
not a measurement: it can pass a soak whose `le=0.4` fraction is below
0.95, or fail one that met the counting test. Architecture §11.2 made
the SLO a **counting question** at an exact boundary.

The two budgeted histograms therefore include those exact bounds in the
bucket list. A reshape that drops `0.4` / `1.0` or that reports
`histogram_quantile(...)` as the clause-3 number deletes the SLO.

## Decision

**Latency SLOs are read off histogram buckets, never quantile
interpolation** (`metrics.rs:3-6`).

Bucket lists **include the budget bounds exactly**:

- `PROCESS_BLOCK_BUCKETS` has **`0.4`** (`metrics.rs:51-54`)
- `PROCESS_EPOCH_BUCKETS` has **`1.0`** (`metrics.rs:56-59`)

`cc_chain_engine_call_seconds` and
`cc_chain_process_block_local_seconds` reuse
`PROCESS_BLOCK_BUCKETS` by reference so the three histograms cannot
drift apart (`metrics.rs:11-13`).

Soak / report method (`docs/phase-1-soak.md:368-376`;
`scripts/soak-report.sh:5-9`):

```text
fraction(le=L) = (bucket[le=L]_end − bucket[le=L]_start) / (count_end − count_start)
Pass  ⇔  fraction ≥ 0.95
```

That *is* "p95 ≤ L". The report states the **margin**, not just
pass/fail. Catch-up is excluded via a scrape pair, not by smoothing.

CI `timing.rs` asserts a **2× loose ceiling** (0.8 s / 2.0 s). That
ceiling is **not** the budget (`metrics.rs:43-47`).

`scripts/soak-report.sh` self-test fails if the report text contains
`histogram_quantile` (`soak-report.sh:3303-3306`).

## Consequences

What this makes easy:

- Clause 3 is reproducible from two OpenMetrics scrapes and a
  subtraction. No PromQL, no interpolation parameter.
- A missing `0.4` / `1.0` bucket is a review-stopper, not a silent
  SLO change.

What this makes hard:

- Dashboard p95 that uses `histogram_quantile` is not the soak number
  and must not be pasted into `docs/phase-1-soak.md`.
- Adding a latency SLO requires putting the threshold **on a bucket
  boundary** first, then writing the fraction test.

What this forbids:

- Declaring block/epoch (or a successor SLO) as a PromQL
  `histogram_quantile` / interpolated percentile.
- Removing or moving the `0.4` / `1.0` boundaries from the budgeted
  histograms.
- Letting `[ARCH]` §7.3's "one work-type enum, derived histogram
  families" drop exact budget bounds or switch the soak to quantiles.
- Treating the CI 2× ceiling as the production bar.

## Alternatives considered

**Prometheus `histogram_quantile(0.95, …)` as the official p95.**
Rejected in the citing module and in `soak-report.sh`: interpolation
inside a bucket is not a counting test, and the self-test forbids the
helper by name.

**Summaries / HDR histograms for "true" p95.** Not recorded as a
house option. A switch off Prometheus histograms is a new ADR; it
does not sneak in under §7.3's reshape.

**Tighter buckets around 0.4 without keeping 0.4 itself.** Rejected:
the SLO *is* the `le=0.4` fraction.

## Refactor impact

**Survives and is load-bearing.**

| Stage | What happens to this record |
|---|---|
| S1 | Untouched. Engine-local histograms keep `PROCESS_BLOCK_BUCKETS`. |
| §7.3 reshape (with `cc-scheduler`) | Queue-time / worker-time families **must** keep exact budget bounds on the block/epoch (and any successor) SLOs. Derived labels are fine; interpolated p95 is not. |
| S3b-W-02 | Hoodi 24 h soak reads clause 3 off this method. |
| S2+ | Storage / head-lag clauses that already say "bucket fraction at `le=…`" are the same rule. |
