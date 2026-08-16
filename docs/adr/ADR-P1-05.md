# ADR-P1-05 — KZG verify returns `Result<bool, _>`, not `bool`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1
- **Issues:** S1-B-08, CC-11c
- **Citations:** 1 site in the §10.4 census — `crates/crypto/src/kzg/trait.rs:12`; live contract also at `trait.rs:5-14,135-172`; adapter at `crates/crypto/src/kzg/rust_eth_kzg.rs:5-7,19-24,106-116,194`; `c-kzg` already matches at `crates/crypto/src/kzg/c_kzg.rs:176`
- **Provenance:** re-derived from code (2026-08-16)

## Context

`CellKzg::verify_cell_kzg_proof_batch` is the EIP-7594 cell-proof verdict. Two
backends implement it. `c-kzg` already returns `Result<bool, Error>` with
`Ok(false)` meaning the proof is invalid. `rust_eth_kzg` 0.10.0 returns
`Result<(), Error>` and folds cryptographic failure into `Err`
(`rust_eth_kzg.rs:5-7`). Callers that treat every `Err` as an internal fault
would descore a peer for a well-formed invalid proof, or treat a malformed
setup as a gossip REJECT.

The two outcomes must never be confused. That is the whole decision.

## Decision

The house KZG trait returns `Result<bool, KzgError>`, matching `c-kzg`:

- **`Ok(true)`** — the batch is cryptographically valid
- **`Ok(false)`** — the proof is **invalid** (not an error)
- **`Err`** — malformed input or an internal fault

CC-11c's `rust_eth_kzg` adapter **must** map only
`Error::is_proof_invalid()` to `Ok(false)` and everything else to `Err`
(`rust_eth_kzg.rs:111-116`). A rename that breaks `is_proof_invalid` is a
reviewable bump; the catch-all is `Err`, never `Ok(false)`.

## Consequences

What this makes easy:

- Gossip / DAS scoring can treat `Ok(false)` as peer fault and `Err` as our
  bug or a bad encoding, without a second error taxonomy.
- Swapping backends is an adapter, not a caller rewrite. `c-kzg` already
  speaks this shape (`c_kzg.rs:176`).

What this makes hard:

- Every new backend must normalise its error enum onto the three-way
  verdict. Folding "invalid proof" into `Err` is a defect against this file.

What this forbids:

- A `bool`-only verify API (no way to say "we could not judge").
- Mapping a setup / length / serialization failure to `Ok(false)`.
- Mapping a proof-verification failure to `Err` in the `rust_eth_kzg`
  adapter.

## Alternatives considered

**Return `bool` and panic / unwrap on backend faults.** Rejected in the
citing module: malformed input and an invalid proof are different events.

**Keep each backend's native `Result` and translate at every call site.**
Rejected: CC-11c exists so the translation lives in one adapter.

None further recorded.

## Refactor impact

**Survives.** Backend swaps and the S3 KZG-pool reconnect do not change the
verdict contract. A new DAS verifier that returns `Result<(), _>` still
adapts here.
