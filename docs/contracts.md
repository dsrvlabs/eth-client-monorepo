# Protocol contracts

Governance for `proto/` — the wire contracts every service binary depends on.
Owned by CC-03; evolution rules come from Architecture §3.3 and §3.5 (ADR-05).

## Repository

| Field | Value |
|---|---|
| **org/repo** | `dsrvlabs/eth-client-monorepo` |
| **integration branch** | `develop` (intentional deviation from Architecture §9 `main`-only) |
| **buf workspace root** | `proto/` (`proto/buf.yaml`, v2) |
| **lint** | `DEFAULT` |
| **breaking** | `FILE` (strictest category) |
| **codegen** | `crates/proto/build.rs` via `protox` + `tonic-prost-build` — **not** `buf generate` |

## CI gate (`proto` job)

On every PR and every push to `main` / `develop`, the `proto` job runs:

1. **`bufbuild/buf-action@v1`** with `input: proto`, `lint: true`, `format: true`, and
   label-aware `breaking` (see below).
2. **`scripts/check-no-remodelling.sh`** — rejects any `message` whose name matches a consensus
   container (`BeaconBlock`, `BeaconState`, `Attestation `). Those cross service boundaries as
   `bytes ssz` + metadata only (Architecture §2.2 invariant 2 / §3.2).

### Breaking baseline (`breaking_against`)

| Event | Baseline |
|---|---|
| **push** | `https://github.com/dsrvlabs/eth-client-monorepo.git#branch=develop,ref=<github.event.before>,subdir=proto` — parent of the push on the integration branch |
| **pull_request** | `https://github.com/dsrvlabs/eth-client-monorepo.git#branch=<github.base_ref>,subdir=proto` — tip of the PR base (usually `develop`) |

Checkout uses `fetch-depth: 2` so a local `HEAD~1` baseline remains available if needed.
Push and PR baselines both pin **`subdir=proto`** so the comparison root matches the buf workspace.

If a run genuinely cannot resolve a baseline, use the escape hatch below — **never** turn the
check off in the workflow (R-2).

## Escape hatch: `buf skip breaking`

The sanctioned way to land a deliberate breaking contract change is the GitHub PR label
**`buf skip breaking`**.

**Scope:** the label is evaluated only on **pull_request** events. On **push**, breaking always
runs (`breaking` is forced on). Applying or removing the label re-triggers CI because the
workflow listens to `pull_request` types `labeled` and `unlabeled`.

When the label is present on a PR, the `proto` job skips the breaking step. Use it only when the
break is intentional and reviewed:

- **Legitimate in Phase 1** when CC-1E / CC-1F land the real contract shapes — those issues exist
  precisely so later changes stay additive.
- **Not legitimate** as a workaround for a missing baseline, a mis-set `breaking_against`, or a
  refactor that could have been additive.

**PR description must name every affected downstream consumer** (services, clients, generated
stubs, external callers). A labelled PR without that list is incomplete.

## Field-numbering convention

- **1–15** for hot-path and frequently-set fields (one-byte protobuf tags).
- **16+** for everything else.
- Deleted fields get a `reserved` statement for **both** the field number **and** the name.
- Field numbers are **never reused**.

## FILE-category evolution rules (ADR-05)

`breaking.use: [FILE]` is the strictest buf category. Operational consequences that are easy to
discover too late:

| Rule | Consequence |
|---|---|
| A published message **never moves to another file** | Even within the same package, even with no wire change. Tidying `chain.proto` into `chain_service.proto` + `events.proto` by moving existing messages is a breaking change. |
| **Renaming a field is breaking** | Wire-compatible does not matter under `FILE`. |
| **Renaming or deleting a file is breaking** | File identity is part of the contract surface. |

Standing design:

- **One `.proto` file per package in Phase 0**, named after the package's service
  (e.g. `eth/chain/v1/chain.proto`).
- **New messages may go in new files** within the same package.
- **Existing messages never move.**

Phase 1 additions land in files that already exist; splits stay open only for new surface.

## Local checks

```bash
cd proto && buf lint
cd proto && buf format --diff --exit-code
bash scripts/check-no-remodelling.sh
```
