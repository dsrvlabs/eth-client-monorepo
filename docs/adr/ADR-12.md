# ADR-12 — `figment` is the config loader (TOML file + prefixed env)

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 0 (workspace-wide; touched by [ARCH] §5.4 unknown-key WARN)
- **Issues:** S1-B-07, CC-09a
- **Citations:** `Cargo.toml:83-84`; `crates/config/src/lib.rs:1-18,43-46,155-179,229-247`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-12**. It records the live loader. The unknown-key WARN
capture ([ARCH] §5.4 / P0-02) is a consumer of this loader, not a
replacement for it.

## Context

Every service binary must fail before bind when a required field is
missing or malformed (CC-09/2). Layering a TOML file under
process-environment overrides is the operator contract
(`config/<service>.toml`, then `CC_<SERVICE>_…`, env wins). Nested maps
use double-underscore splitting
(`CC_P2P_PEERS__CHAIN=http://chain:9001` → `peers["chain"]`).

`cc-config` is the only crate permitted to read the environment (D-2).
`RUST_LOG` and `LOG_FORMAT` are resolved here into `log_filter` /
`log_format` and handed to `cc-bootstrap::init` already resolved
(ADR-06). A second loader (`config` crate, hand-rolled toml+env, clap
for file config) would split that chokepoint.

The workspace already pins `figment` 0.10.19 with `env` and `toml`
features under `# Config loader (Architecture §5 / ADR-12, CC-09a)`
(`Cargo.toml:83-84`). `crates/config/src/lib.rs` is the only production
`Figment` construction.

## Decision

**`figment` is the config loader.** `cc-config::load` / `load_from`
build one `Figment` per service (`figment_for`):

1. serialized telemetry defaults
2. `Toml::file_exact(path)` — no parent-directory walk
3. `Env::prefixed("CC_<SERVICE>_").split("__")`
4. bare `RUST_LOG` / `LOG_FORMAT` overlays (D-2)

Extract `ServiceConfig` first so `#[serde(flatten)]` cannot erase field
paths, then extract the per-service type. Missing fields become `Err`
naming the key, the TOML path, and the `CC_<SERVICE>_<FIELD>` env var.

**Not `deny_unknown_fields` on consensus `ChainConfig`.** [ARCH] §5.4
requires unknown keys to be captured and logged at WARN so an upstream
Heze key is visible rather than a hard fail. That capture is a later
touch of this loader, not a reason to leave `figment`.

## Consequences

What this makes easy:

- Every service binary shares one layering story and one error shape.
- Env overrides are prefixed; they do not collide with `RUST_LOG`.

What this makes hard:

- A service that wants a different file format or a different env
  prefix has to extend `cc-config`, not grow a parallel loader.

What this forbids:

- A second production config loader beside `figment` in `cc-config`.
- Service binaries reading `CC_*` / `RUST_LOG` / `LOG_FORMAT` themselves
  (CC-09/3 / `check-no-env-reads.sh`).
- `deny_unknown_fields` as the house answer to unknown consensus-config
  keys ([ARCH] §5.4).

## Alternatives considered

**Hand-rolled TOML + `std::env`.** Rejected in the live crate: figment
already does prefixed env, `__` nesting, and extract errors. Re-deriving
that is how field provenance gets lost (the flatten problem `load_from`
exists to close).

**`deny_unknown_fields` on `ChainConfig`.** Rejected by [ARCH] §5.4:
upstream configs grow keys this client does not yet understand. Capture
and WARN, do not fail closed on a Heze key.

## Refactor impact

**Survives.** [ARCH] §5.4's unknown-key WARN is a change *to* this
loader, not a swap. S1–S5 keep `cc-config` as the env chokepoint.
