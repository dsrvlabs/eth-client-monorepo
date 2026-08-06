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
# Prefer develop (integration branch). Issue text sometimes says main; both work once published.
cd proto && buf breaking --against '.git#branch=develop,subdir=proto'
bash scripts/check-no-remodelling.sh
```

---

## Milestone-gate flow (R-1)

**Chosen flow:** the pipeline lands work on the **`develop` integration branch** (direct push of green
tips; one commit per issue is fine). At each milestone gate the green `develop` tip is
**fast-forwarded into `main`**. Required status checks run against the tip under test (on `develop`
continuously, and again on the milestone push path as CI is configured).

| Decision | Value | Why |
|---|---|---|
| Integration branch | `develop` | Intentional deviation from Architecture §9 `main`-only; keeps `main` as the milestone-release line |
| How work lands on `develop` | Direct push of issue commits (or short-lived PR if preferred operationally) | Matches Phase 0 one-commit-per-issue velocity |
| Milestone promotion | Fast-forward `develop` → `main` | Avoids merge commits; tip must already be green |
| PR-required on protected branch | **OFF** | If "Require a pull request before merging" is ON, fast-forward push is rejected regardless of green checks — that silently breaks this flow |
| Required checks (CC-07/1) | exactly `fmt`, `clippy`, `test`, `proto` | `deps` joins at M0.4 (CC-08); `vectors` becomes required in Phase 1 |

Do **not** leave both "PR-per-milestone" and "fast-forward into main" live without writing the choice
down. This document picks fast-forward; operators must keep "Require a pull request before merging"
disabled on the protected integration branch (`develop`). The same PR-required=OFF rule applies if
`main` is later given the same required-check set for the milestone push.

---

## Branch protection (`develop`)

Applied by CC-07b on the integration branch. **Required status checks** (sorted):

```text
["clippy", "fmt", "proto", "test"]
```

**Require a pull request before merging:** OFF (`required_pull_request_reviews` is null).

### Operator: verify

```bash
gh api repos/dsrvlabs/eth-client-monorepo/branches/develop/protection \
  --jq '.required_status_checks.contexts | sort'
# expect: ["clippy","fmt","proto","test"]

gh api repos/dsrvlabs/eth-client-monorepo/branches/develop/protection \
  --jq '.required_pull_request_reviews'
# expect: null  (PR-before-merge OFF)
```

### Operator: apply / re-apply (classic branch protection API)

Needs admin (or maintain with suitable permissions) on `dsrvlabs/eth-client-monorepo`. If the
automated agent lacks admin, run this manually — do not weaken the required set or turn PR-required on.

```bash
gh api -X PUT repos/dsrvlabs/eth-client-monorepo/branches/develop/protection \
  -H "Accept: application/vnd.github+json" \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["clippy", "fmt", "proto", "test"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "required_linear_history": false,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": false,
  "lock_branch": false,
  "allow_fork_syncing": false
}
EOF
```

Notes:

- `required_pull_request_reviews: null` is the half that keeps fast-forward push viable (R-1).
- `strict: false` — do not require the tip to be "up to date" with another base in a way that
  blocks direct push of an already-green integration tip.
- `enforce_admins: false` — admins can still emergency-push; prefer not to rely on that.
- Optional later: the same payload with `branches/main` if milestone promotion should also be
  check-gated at the `main` tip (not required for CC-07b; this issue protects `develop`).

### Operator: prove fast-forward still works after protection

With PR-required OFF, a fast-forward of a tip that already has green `fmt`/`clippy`/`test`/`proto`
into the protected branch must succeed (admin/write as appropriate):

```bash
# example shape only — run when a real milestone tip is ready
git fetch origin
git checkout main
git merge --ff-only origin/develop
git push origin main
```

For day-to-day Phase 0 work, the analogous proof is a normal green push to `develop` after the
required checks have been observed on that SHA (CI `push` to `develop` continues to run the jobs).

---

## Gate proof (CC-07b / CC-03/2)

Five configured jobs are not a gate until each has been observed **rejecting a real violation**.
Demos use **throwaway branches + draft PRs** against `develop` (workflow `pull_request` +
`push` to `main`/`develop` only). Throwaway branches and PRs are closed/deleted after recording;
**no demo commit may land on `develop` or `main`** (R-9).

### Procedure (re-run if proofs are lost)

For each demo: branch from `origin/develop` → introduce **one** deliberate violation → push → open
draft PR → wait for CI failure → record job name, violation, failing run URL → close PR → delete
remote branch.

| # | Job | Violation to introduce | Local preview |
|---|---|---|---|
| 1 | `fmt` | Unformatted Rust (e.g. smashed whitespace in `crates/types/src/lib.rs`) | `cargo fmt --all --check` |
| 2 | `clippy` | (a) `clippy::needless_clone` (or any warn lint under `-D warnings`); (b) optional second observation: illegal workspace edge so `scripts/check-crate-dag.sh` fails inside the clippy job (D-3) | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` and `bash scripts/check-crate-dag.sh` |
| 3 | `test` | Deliberately failing unit test **on the throwaway branch only** (workspace may still have no committed tests — R-9) | `cargo nextest run --workspace --locked` |
| 4 | `proto` (lint) | e.g. `FIELD_LOWER_SNAKE_CASE` via a `BadCamelCase` field in a published message | `cd proto && buf lint` |
| 5 | `proto` (breaking) | **Field rename** on an already-published message (FILE category). Must fail **locally and in CI** (CC-03/2) | `cd proto && buf breaking --against '.git#branch=develop,subdir=proto'` |

### Recorded demonstrations (2026-08-06)

All five were run as draft PRs #1–#5 from `throwaway/demo-fail-*` branches; PRs closed and remote
branches deleted after observation. None of the failing commits are on `develop`/`main`.

| # | Job | Violation | Failing run URL |
|---|---|---|---|
| 1 | `fmt` | Deliberate unformatted `crates/types` helper | https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073815370 (`fmt` failed; other required jobs green) |
| 2 | `clippy` | `needless_clone` + illegal `cc-types → cc-crypto` DAG edge | https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073826787 (`clippy` failed) |
| 3 | `test` | `assert_eq!(2 + 2, 5)` unit test in `crates/types` | https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073846471 (`test` failed) |
| 4 | `proto` / lint | `BuildInfo.BadCamelCase` → `FIELD_LOWER_SNAKE_CASE` | https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073850011 (`proto` failed); local `buf lint` reported the same rule |
| 5 | `proto` / breaking | Field rename `BuildInfo.git_sha` → `git_commit` | https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073853519 (`proto` failed). Local: `buf breaking --against '.git#branch=develop,subdir=proto'` exited non-zero (`Field "3" … changed name from "git_sha" to "git_commit"`). |

**Operator note:** re-running demos is always allowed (and required if run URLs rot or the workflow
surface changes). Keep demo commits off `develop`/`main`.

### Warm-run wall clock (CC-07/4)

Required jobs `fmt`, `clippy`, `test`, `proto` on a warm `develop` push (after cache population):

| Job | Duration (approx.) |
|---|---|
| `fmt` | ~26 s |
| `clippy` | ~17 s |
| `proto` | ~5 s |
| `test` | ~81 s |

- **Run:** https://github.com/dsrvlabs/eth-client-monorepo/actions/runs/31073736967  
  (`ci(proto): add buf lint, breaking, and remodelling gate` on `develop`, conclusion success)
- **Parallel wall clock** (earliest required-job start → latest required-job finish): **~1 m 29 s**
  (05:18:39Z → 05:20:08Z) — **under 10 minutes**.
- `vectors` is non-required in Phase 0 and is excluded from this measurement (it dominated that run
  at ~10 m because of cold vector fetch/cache; not part of the CC-07/1 required set).
