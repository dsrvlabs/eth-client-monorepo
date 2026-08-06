# eth-client-monorepo

Ethereum consensus client monorepo (Rust MSA beacon node). Phase 0 scaffolding.

## Scaffold-time values (reuse in later issues)

These values are recorded here so CC-01a, CC-03, and CC-04 reuse them without drift:

| Value | Resolved | Consumers |
|---|---|---|
| **`<org>/<repo>`** | `dsrvlabs/eth-client-monorepo` | CC-01a (`[workspace.package] repository`), CC-03 (`buf` `breaking_against`, `docs/contracts.md`) |
| **Rust toolchain** | `rustc 1.97.1 (8bab26f4f 2026-07-14)` | CC-01a (`rust-toolchain.toml` `channel`, `[workspace.package] rust-version`), CC-04 (`Dockerfile ARG RUST_VERSION`) |

License: Apache-2.0.
