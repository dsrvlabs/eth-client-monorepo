# P2P dependencies

Phase 2 networking pins and supply-chain notes for the libp2p edge. Owned by
**CC-2K** (workspace admission); crate content is **CC-20a**.

## libp2p git pin (CC-20/1)

| Field | Value |
|---|---|
| **Repository** | `https://github.com/libp2p/rust-libp2p` |
| **Rev (40-hex)** | `6348a0be4aeb5b48eecf17a5d0aae15ff8239984` |
| **Resolution date** | 2026-08-07 |
| **Commit date (upstream)** | 2026-08-04 (`deps: bump rand_core from 0.6.4 to 0.10.1 (#6582)`) |
| **Greppable constant** | `cc_libp2p::LIBP2P_GIT_REV` (must match this rev and root `Cargo.toml`) |

**Reason for the pin.** The crates.io facade crate (`libp2p` 0.56.x) lags the protocol
crates by a long margin; Lighthouse and other production clients git-pin rust-libp2p for
that reason. We pin an explicit 40-hex `rev` on the official repository (not a fork) so
the graph is reproducible under `--locked` and so `cargo deny` / `allow-git` can name a
single reviewed source. Only `cc-libp2p` may declare a `libp2p*` dependency; the pin
lives in `[workspace.dependencies]` and is consumed solely from `crates/libp2p`.

**Feature set (workspace pin):** `identify`, `yamux`, `noise`, `dns`, `tcp`, `tokio`,
`secp256k1`, `macros`, `metrics`, `gossipsub`, **`quic`**. QUIC is compiled from day one
so enabling the transport later (CC-2F) is a config change, not a dependency change or
rev re-review.

**Rev-change policy (OQ-7).** Any change to the 40-hex `rev` in root `Cargo.toml` is a
supply-chain event: update `LIBP2P_GIT_REV`, this document, and the Phase 2 section of
`docs/supply-chain.md` in the same commit; run the manual RUSTSEC/git re-review described
there. `scripts/check-crate-dag.sh` fails if docs or the crate constant drift from
`Cargo.toml`. `cargo deny` `allow-git` is URL-only — it does **not** pin the rev; the
DAG pin-integrity check does.

### Hybrid pin (git protocol crates + crates.io identity)

The resolved graph is **hybrid**, matching upstream rust-libp2p facade design:

| Source | What |
|---|---|
| **Git** (`rev` above) | Facade `libp2p` and monorepo protocol crates (`libp2p-gossipsub`, `libp2p-swarm`, `libp2p-quic`, …) |
| **crates.io** | `libp2p-identity`, `multiaddr` (and other registry deps the facade pulls) |

So identity **does** participate in crates.io advisory matching; swarm/gossipsub/quic code
at the pin does **not**. Operators must not assume “entire libp2p stack is git-only.”
`scripts/check-crate-dag.sh` requires every **direct** workspace `libp2p*` dependency and
the resolved facade package `libp2p` to use the workspace git rev; monorepo git
`libp2p-*` packages must share that rev; crates.io `libp2p-identity` remains allowed as
this hybrid.

## A-P2-3 — GossipSub / IDONTWANT

| Field | Value |
|---|---|
| **Resolved `libp2p-gossipsub` version** | **0.50.0** (via the git pin above) |
| **IDONTWANT floor** | `libp2p-gossipsub` ≥ **0.48.0** |
| **Verdict** | **PASS** — 0.50.0 ≥ 0.48.0 |

GossipSub v1.2 `IDONTWANT` landed in 0.48.0. Behavioural measurement is CC-22/3; this
section is the version gate only.

## Deviations

- **discv5 dual identity (deferred to CC-21\*).** Workspace pins `discv5` 0.11.0 with
  feature `libp2p` (pulls crates.io `libp2p-identity` / `multiaddr`). No member depends
  on it yet (absent from `Cargo.lock`). First consumer must resolve under `--locked`,
  assert a single `libp2p-identity` version, and extend this section if dual-source
  pressure appears.

<!-- CC-23b cgc-policy and §3.2 libp2p-ping vs Ethereum-ping naming notes append here. -->
