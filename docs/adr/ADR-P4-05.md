# ADR-P4-05 — Commit durability is a config knob (`immediate` | `paranoid`)

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-40b
- **Citations:** 2 sites — `crates/store/src/engine/mod.rs:26`; `config/storage.toml:16`
- **Provenance:** re-derived from code (2026-08-16)

## Context

redb's public durability enum is only `{None, Immediate}`. Production
needed a named 2PC setting without exposing `None` on the config surface
(SEC-40b-6). The archive can tolerate the write-behind loss window
(`commit_max_latency_ms = 4000`); a slashing DB cannot
(`[ARCH]` §9.1 S5 / `ADR-R-05`). Those two files must not share this knob.

## Decision

Expose `Durability::{None, Immediate, Paranoid}` on the engine seam.
Map `immediate` → redb `Immediate` (1PC+C, default). Map `paranoid` →
`Immediate` + `set_two_phase_commit(true)`. Reject `none` in
`Durability::parse` / `resolve` so it cannot come from `storage.toml` or
`CC_STORAGE_DURABILITY`. Tests and bulk-load benches construct
`Durability::None` in code. `cc-store` does not read the environment;
the service passes an already-resolved override.

## Consequences

What this makes easy:

- Operators pick 1PC+C or 2PC in one token. Soak can flip
  `CC_STORAGE_DURABILITY=paranoid` without a rebuild.
- The slashing crate can take `Immediate` / `Paranoid` on **its own**
  redb file without importing this config.

What this makes hard:

- `None` exists on the type but is a foot-gun if a new parser path
  forgets `parse`. The named `Config` error is the backstop.

What this forbids:

- `durability = "none"` (or the env equivalent) as a production value.
- Sharing this knob — or this `Engine` — with the slashing-protection
  store (`ADR-R-05`).
- Reading `CC_STORAGE_DURABILITY` inside `cc-store`.

## Alternatives considered

**Only `Immediate`, no 2PC.** Rejected: `Paranoid` is the documented
mapping for operators who want two-phase commit, and it is what
`ADR-R-05` cites as already available.

**Allow `none` in toml for "faster soak."** Rejected (SEC-40b-6): soak
can still construct `None` in a bench binary; production config cannot.

## Refactor impact

**Survives.** `[q4]` / `ADR-R-05` require the slashing DB not share this
knob (`[ARCH]` §9.1 S5). The enum moves with `cc-store`; S2 does not
widen it.
