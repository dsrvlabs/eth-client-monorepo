# ADR-R-03 — The JWT isolation invariant is re-pointed from a process to an API surface

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 3 (isolation restated at S1; process-level wording was Phase 3)
- **Issues:** S1-B-11, S1-A-01
- **Citations:** `plan/architecture.md` §6.2 / §9.2 / §10.4 `ADR-P3-16` / §10.5; `plan/issues/s1-fold-el-bridge.md` S1-A-01 / S1-B-11 / E1.4; `scripts/check-crate-dag.sh:28-61,186-200,607-638`; `docs/supply-chain.md:121,134`; `docs/phase-3-acceptance.md:605`; `services/chain/src/engine_client.rs:23-24`; `services/engine/src/jwt.rs:5-9,20-39`; `docker-compose.yml:106-108,205`; `crates/engine-api/src/lib.rs`
- **Provenance:** new — records the S1-A-01 dag rule; supersedes `ADR-P3-16`

This is **ADR-R-03**. It supersedes `ADR-P3-16` (*only `cc-engine` may declare an HTTP client or
JWT signer*). It does **not** partially supersede `ADR-P3-02` (engine is deliberately not a
health peer). That half of the `[ARCH]` §10.5 stub is `S1-B-12` and waits on `S1-A-16`.

## Context

Phase 3 isolated the Engine API HTTP client and the HS256 JWT signer in the `cc-engine`
process so that *"the JWT never enters the consensus process"* was a build failure, not a
review comment (`scripts/check-crate-dag.sh:28-31`; `ADR-P3-16` at
`docs/supply-chain.md:121,134`, `docs/phase-3-acceptance.md:605`,
`services/chain/src/engine_client.rs:23-24`). The script named `cc-engine` and treated any
other crate declaring `reqwest` / `hyper` / `hyper-util` / `jsonwebtoken` / `hmac` /
`sha2-jwt` as a defect.

S1 folds that bridge into the consensus process. `S1-A-01` admitted `crates/engine-api`
(`cc-engine-api`) and re-pointed `http_or_jwt_allowed()` at it in the same change (E1.4).
The Phase-3 sentence is therefore **false as written**: the named crate is no longer
`cc-engine`, and after `S1-A-03` the signer lives in the same process as fork-choice.
Leaving the old wording in force would keep the script green while the invariant it exists
to protect quietly ceased to hold (`[ARCH]` §6.2).

The credential cannot stay out of the process. The guarantee has to be restated as a
property of the **API surface**, not of a process boundary. The load-bearing half of that
restatement is negative: **`cc-chain` is not added to the JWT grandfather list**
(`[ARCH]` §9.2). Adding it would delete the invariant rather than re-point it.

`jwt.rs` and the HTTP transport have not moved yet. They stay in `services/engine` until
`S1-A-03` / `S1-A-06`. A rule that named only `cc-engine-api` today would fail the live
tree for the duration of that move.

## Decision

**Only `cc-engine-api` may declare a JWT signer or an Engine API HTTP client**, and no type
it exports may carry, expose, or `Debug`-print secret material.

`check-crate-dag.sh`'s `http_or_jwt_allowed()` names `cc-engine-api` first. The forbidden
set is unchanged: HTTP `reqwest` / `hyper` / `hyper-util`; JWT `jsonwebtoken` / `hmac` /
`sha2-jwt`. The rule is about *declaring* the dependency; transitive `hyper` under `tonic`
is unaffected.

**Transitional `cc-engine`.** Until `S1-A-06` deletes the old binary surface, `cc-engine`
may still declare HTTP and JWT. The signer and transport still live in
`services/engine` (`S1-A-03` moves `jwt.rs`; `S1-A-02` moves the transport; `S1-A-06`
makes `main.rs` a thin constructor). After `S1-A-06` that allowance is removed. Do not
treat the transitional row as a second permanent home.

**`cc-chain` is not grandfathered for JWT.** HTTP grandfather stays `{cc-chain,
cc-bootstrap}` and is HTTP-only — checkpoint-sync / test `hyper` (CC-15 / CC-28) and the
bootstrap metrics server. A `jsonwebtoken` / `hmac` / `sha2-jwt` edge on either crate is a
build failure. The workspace root may pin the crates; that is not a client declaration.

**Secret-material discipline moves with `jwt.rs`, unchanged.** `JwtSecret` keeps its
hand-written `Debug` printing `Jwt(<redacted>)` (`jwt.rs:8-9,28-39`). File discipline stays
mode `0600`, `..` rejected, max 4 KiB, abort before bind (`jwt.rs:5-7,20-25`). The secret
is bind-mounted read-only and never `COPY`'d into an image (`docker-compose.yml:106-108,205`).
When `jwt.rs` lands in `cc-engine-api`, a grep rule asserts no `pub` item in that crate
names `JwtSecret`. That grep is `S1-A-03`; this record is what forbids shipping the move
without it.

`sha2` is not in the forbidden set. `cc-crypto` (and `cc-chain` for
`kzg_commitment_to_versioned_hash`) legitimately hash. The JWT rule is the signer, not
hashing.

This record does **not** decide engine's place in the health DAG. `ADR-P3-02` remains in
force until `S1-B-12`.

## Consequences

What this makes easy:

- A JWT signer or Engine API HTTP client appearing outside `cc-engine-api` (and, until
  `S1-A-06`, transitional `cc-engine`) is a build failure. The process-level sentence is
  gone; the mechanical guarantee is not.
- Folding the engine bridge does not require parking the credential on `cc-chain`.
- Citation sites that still say *only `cc-engine`* (`docs/supply-chain.md:121,134` and the
  script's `ADR P3-16` tokens) resolve here. They can be retouched when those files move
  for another reason.

What this makes hard:

- Anyone who wants `cc-chain` to sign Engine JWT has to contradict this file, not just
  widen `http_or_jwt_allowed()`.
- The transitional `cc-engine` row must be deleted in `S1-A-06`. Forgetting that leaves a
  second allowed signer after the files have moved.
- `JwtSecret` cannot become a convenient `pub` re-export of `cc-engine-api`. Callers get
  an opaque sign/attach surface, not the bytes.

What this forbids:

- Adding `cc-chain` (or any other consensus crate) to the JWT grandfather list.
- Declaring `reqwest` / `hyper` / `hyper-util` / `jsonwebtoken` / `hmac` / `sha2-jwt` on
  a crate that is not `cc-engine-api` or, until `S1-A-06`, `cc-engine` — except the
  HTTP-only grandfather for `cc-chain` and `cc-bootstrap`.
- A derived `Debug` on a type that holds the raw key, or a `pub` `JwtSecret` on
  `cc-engine-api` after the move.
- Keeping the engine in its own process *in order to* preserve Phase-3 process isolation.

## Alternatives considered

**Keep the engine in its own process so the JWT never enters consensus.** Rejected. It
re-creates the deadline-less hop ([PRD] P0-15) for a guarantee that the API-surface rule
preserves more cheaply. The process boundary was never what made the Engine-API edge
reference-quality — `JwtSecret`'s redacting `Debug` and the 0600 / abort-before-bind file
discipline were (`[ARCH]` §6.2).

**Add `cc-chain` to the JWT grandfather list when the signer becomes in-process.**
Rejected. That is the `[ARCH]` §9.2 S1 prohibition. It deletes the invariant. Chain
already has HTTP for checkpoint-sync; that is not a precedent for holding the signer.

**Name only `cc-engine-api` immediately and fail `cc-engine` until the files move.**
Rejected. `S1-A-01` lands the crate and the rule before `S1-A-02..06` move the code. The
signer still lives in `services/engine`. A same-PR fail-closed cut would force the move
into `S1-A-01` and break the "verbatim with tests" review contract.

**Leave `ADR-P3-16` wording and let the script quietly disagree.** Rejected. `S1-A-01`
without this record is half the deliverable (`s1-fold-el-bridge.md` S1-A-01 acceptance 4).
A dag rule that no longer matches its cited decision is the class `[ARCH]` §6.2 exists to
stop.

**Fold `ADR-P3-02` (engine is not a health peer) into this file now.** Rejected for this
issue. That decision is void once the engine is in-process, but the replacement is the
core-liveness probe (`ADR-R-04` / `S1-A-16`). Recording it here before the probe lands
would either invent a health rule or silently claim a supersession `S1-B-12` owns.

## Refactor impact

**Created at S1. Rule landed at `S1-A-01`. This file is the record.**

| Stage | What happens to this record |
|---|---|
| S1-A-01 | `cc-engine-api` admitted; `http_or_jwt_allowed()` names it; `cc-chain` stays off the JWT list. **Landed.** |
| S1-B-11 | This file. No production code change. |
| S1-A-02..05 | Transport, `jwt.rs`, health machine, methods move verbatim. Secret-material grep lands with `jwt.rs` (`S1-A-03`). |
| S1-A-06 | `services/engine` becomes a thin constructor. **Drop the transitional `cc-engine` HTTP/JWT allowance.** Citations into `services/engine/src/jwt.rs` in this file become historical. |
| S1-B-12 | Separate record: `ADR-P3-02` / health-peer supersession. Not a silent amendment of this file. |
| S2+ | Isolation stays an API-surface rule. A JWT declaration on `cc-chain` / `cc-beacon-core` / any later consensus crate is a defect against this record. |
