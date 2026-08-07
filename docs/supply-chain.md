# Supply-chain policy

How this monorepo gates third-party Rust dependencies, and where the gate does
not reach. Owned by **CC-08**; ties to Architecture §9 (`deps` job), **R-5**,
and open question **OQ-7**.

## Gate

| Mechanism | What it enforces |
|---|---|
| **`deny.toml` + `cargo deny check advisories bans licenses sources`** | RUSTSEC advisories, yanked crates, license allow-list, ban list, and source policy (crates.io only unless explicitly allowed). |
| **`Cargo.lock` + `--locked`** | Reproducible builds in CI and the Dockerfile (R-5). No silent registry drift. |
| **`scripts/check-crate-dag.sh`** | Workspace crate dependency DAG (Architecture §2.2). Runs in both `clippy` and `deps` (D-3). |

The `deps` CI job runs the deny checks and the DAG script on every PR and every
push to `main` / `develop`. Once CC-08 has landed, `deps` is a **required**
status check alongside `fmt`, `clippy`, `test`, and `proto`.

## Source policy (R-5)

`[sources]` in `deny.toml` sets `unknown-git = "deny"` and
`unknown-registry = "deny"`. Phase 0 intentionally has **no** git dependencies.

**Phase 2 entry point:** CC-20 will introduce a **git-pinned libp2p**. That pin
must be added to `allow-git` (exact repository URL) at the same time as the
`Cargo.toml` dependency — not by flipping `unknown-git` to warn/allow. The
`deny.toml` comment under `[sources]` names this entry point so the first
libp2p commit is an anticipated allowance, not a mid-phase policy fight.

## Documented coverage gaps (document, do not fix)

`cargo deny` (and crates.io-oriented advisory matching generally) has two
structural blind spots for this project. Both are **accepted and documented**
rather than worked around with false confidence.

### 1. `blst` is `build.rs`-heavy and not analyzable

`blst` (Phase 1, CC-11) compiles bundled C and assembly through `cc` at build
time. Advisory tooling operates on Rust crate graphs and published crate
metadata; it does **not** inspect the C/asm sources that `build.rs` pulls into
the build. A RUSTSEC hit on a pure-Rust crate is meaningful; absence of a hit
on `blst` does **not** mean the native code was reviewed by the same pipeline.

Compensating practice for crypto FFI edges: reviewed `#[allow(unsafe_code)]` at
the wrapper boundary in `crates/crypto`, C toolchain pinned in the Docker
builder (CC-04), and version pins recorded in `[workspace.dependencies]` when
CC-11 lands.

### 2. Git-pinned libp2p is outside crates.io advisory matching

CC-20's git-pinned libp2p (and any other future git source on the allow-list)
is **not** matched the same way as a crates.io release. Advisories keyed to
crates.io versions can miss commits that sit between releases or only exist on
a branch/rev pin.

## Compensating control (partial answer to OQ-7)

For every git-pinned dependency (starting with libp2p at CC-20):

1. **Record the git rev as a pinned constant** — in `Cargo.toml` (`rev = "…"`)
   and, where useful for operators/docs, a named constant in the owning crate
   so the pin is greppable and reviewable without digging through the lockfile
   alone.
2. **Re-review RUSTSEC advisories for git-pinned dependencies at each phase
   boundary** (Phase 0→1, 1→2, 2→3, …). The review is manual relative to the
   pinned rev and upstream advisory DB; it is not discharged by a green
   `cargo deny` run alone.

The **choice of the libp2p rev itself** is CC-20 (Phase 2), not this document.
CC-08 only fixes the policy shape and the re-review obligation so that work
does not reopen the supply-chain gate as a surprise.

## Local usage

```bash
# Install once (CI uses taiki-e/install-action for cargo-deny).
cargo install cargo-deny --locked

cargo deny check advisories bans licenses sources
bash scripts/check-crate-dag.sh
```

A deliberate new git dependency or disallowed license should fail `cargo deny`
locally the same way the `deps` job fails in CI.

## Phase 2 — libp2p git pin (CC-20/4 / CC-2K / OQ-7)

| Field | Value |
|---|---|
| **Repository** | `https://github.com/libp2p/rust-libp2p` |
| **Pinned rev** | `6348a0be4aeb5b48eecf17a5d0aae15ff8239984` |
| **Review date** | 2026-08-07 |
| **Allow-git entry** | `deny.toml` `[sources].allow-git` (exact URL; `unknown-git` stays deny) |

**What was reviewed (this admission):**

- Official `libp2p/rust-libp2p` tip at the pin (commit 2026-08-04), not a private fork.
- Feature set required by Architecture §3 / CC-20: `identify`, `yamux`, `noise`, `dns`,
  `tcp`, `tokio`, `secp256k1`, `macros`, `metrics`, `gossipsub`, `ping`, `request-response`,
  plus `quic` (compiled; transport config-disabled until CC-2F).
- A-P2-3: resolved `libp2p-gossipsub` at this rev is **0.50.0** (≥ 0.48.0, `IDONTWANT`).
- Source policy: pin listed in `allow-git`; no flip of `unknown-git`.
- Declaration rule: only `cc-libp2p` may depend on `libp2p*` (`scripts/check-crate-dag.sh`
  + `deps` CI grep for crates.io-style version requirements outside `crates/libp2p/`).

**Re-review trigger (OQ-7):** any change of the 40-hex `rev` in root `Cargo.toml`, any
addition of a further git source to `allow-git`, and every **phase boundary** (Phase 2→3,
…). Re-review is manual against the pinned rev and the RUSTSEC advisory DB; a green
`cargo deny` run alone does not discharge it (git sources sit outside crates.io advisory
matching — see gap §2 above). Details of the pin also live in `docs/p2p-dependencies.md`.
