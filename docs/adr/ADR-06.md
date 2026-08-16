# ADR-06 — Bootstrap telemetry is the workspace pin of `prometheus-client`, `tracing`, `tokio`, `tower`, `hyper`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 0 (workspace-wide)
- **Issues:** S1-B-07, CC-05a
- **Citations:** `Cargo.toml:91-99`; `crates/bootstrap/src/lib.rs:1-7`; `crates/bootstrap/Cargo.toml:13-27`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-06**. It records the live workspace pin. It does not pick a
new metrics or tracing crate.

## Context

Every service binary goes through `cc-bootstrap`: `init` installs tracing
and the metric registry; `serve` binds gRPC (health, reflection, user
routes), runs the peer prober and `/metrics`, and drains on signal
(`crates/bootstrap/src/lib.rs:1-7`). That crate has to name one telemetry
stack. Two stacks would mean two `/metrics` dialects and two subscriber
init paths.

The workspace already pins that stack under the comment
`# Bootstrap telemetry (Architecture §4 / ADR-06, CC-05a)` at
`Cargo.toml:91-99`. `cc-bootstrap` takes those pins and no others for the
HTTP metrics server and the tracing subscriber (`crates/bootstrap/Cargo.toml:13-27`).

## Decision

**The bootstrap telemetry stack is the workspace pin:**

| Crate | Pin (workspace) | Role |
|---|---|---|
| `prometheus-client` | 0.25.0 | metric registry / `/metrics` |
| `tracing` | 0.1.44 | spans and events |
| `tracing-subscriber` | 0.3.23 (`env-filter`, `json`) | subscriber |
| `tokio` | 1.53.1 (macros, net, rt-multi-thread, signal, time, sync) | runtime |
| `tower` | 0.5.3 (`util`) | service layer |
| `hyper` | 1.11.0 (server, client, http1) | metrics HTTP |
| `hyper-util` | 0.1.20 (`tokio`) | hyper + tokio |

`http-body` / `http-body-util` / `bytes` / `pin-project-lite` / `futures`
ride with that pin as the HTTP body and future stack `cc-bootstrap`
already declares.

`init` does **not** read the environment. `RUST_LOG` / `LOG_FORMAT` are
resolved by `cc-config` (D-2 / ADR-12) and passed in as
`TelemetrySettings`.

A second metrics or tracing crate in a service binary is a new decision,
not a silent extra pin. Append-only workspace.dependencies (CC-1K) still
applies: do not re-sort this block to insert an alternative.

## Consequences

What this makes easy:

- Every service binary gets the same `/metrics` and the same subscriber
  without re-declaring versions.
- A new telemetry crate showing up beside this pin is a review question
  against this file.

What this makes hard:

- Swapping `prometheus-client` for `metrics` / OpenTelemetry, or `hyper`
  for `axum`, is a workspace-wide change, not a `cc-bootstrap` local edit.

What this forbids:

- A second house metrics or tracing stack in a service binary without a
  superseding ADR.
- `cc-bootstrap::init` reading `RUST_LOG` / `LOG_FORMAT` itself.

## Alternatives considered

**None recorded beyond the live pin.** Re-derived. The tree does not
document a rejected metrics crate; inventing one would be a new decision.

## Refactor impact

**Survives.** S1–S5 keep `cc-bootstrap` as the process runtime even as
binaries fold. A later metrics reshape ([ARCH] §7.3) must keep this pin
or supersede this file; it must not silently add a second stack.
