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

## ChainService (CC-18a)

Additive RPCs on the existing `eth.chain.v1.ChainService` (Architecture §7.7). Adding RPCs
to an existing service is **not** a `FILE`-category break — `buf breaking` must pass with **no**
`buf skip breaking` label (CC-18/5).

| RPC | Kind | Purpose |
|---|---|---|
| `ImportBlock` | unary | Import a `SignedBeaconBlock` (SSZ bytes) and return a first-class verdict |
| `GetHead` | unary | Head root/slot plus justified/finalized checkpoints (served from `ArcSwap` in CC-18b) |
| `SubscribeEvents` | server-streaming | Event bus with resume cursor; consumers must be idempotent |
| `GetCommitteeShuffling` | unary | Packed epoch shuffling + `dependent_root` from head state (CC-1F, **served**) |
| `GetValidatorPubkeys` | unary | Registry pubkeys by index range/list, bound 256 (CC-1F, **served**) |

CC-1E (`ApplyAttestations`) lands in its own issue — not pre-declared here.

### `ImportBlock`

**Request** — parent plan shape, verbatim (re-plan §Contracts):

| Field | Type | Notes |
|---|---|---|
| `ssz` | `bytes` | `SignedBeaconBlock`, SSZ-encoded. Consensus bodies never re-modelled as proto. |
| `fork` | `uint32` | Fork version tag |
| `root` | `bytes` | Pre-computed `hash_tree_root` — **dedup probe only**; server recomputes on miss (ADR-P1-10) |
| `source` | `eth.common.v1.Source` | `GOSSIP` / `REQRESP` / `API` |

**Response** — `ImportBlockVerdict` + `reason` string:

| Verdict | Meaning |
|---|---|
| `IMPORTED` | New block accepted into the store / fork-choice path |
| `DUPLICATE` | Root already known; transition **not** re-run |
| `DEFERRED_DA` | Parent known but data-availability gate not yet satisfied (Phase 2 fills this in) |
| `UNKNOWN_PARENT` | First-class **verdict**, not an error — drives the driver walk-back (CC-1A/1) |
| `INVALID` | Failed validation; `reason` carries a short explanation |

gRPC status errors (not verdicts) used by the implementation issues:

| Status | When |
|---|---|
| `INVALID_ARGUMENT` | Supplied `root` ≠ true `hash_tree_root` after decode |
| `RESOURCE_EXHAUSTED` | Import command channel full after `send_timeout` |
| `FAILED_PRECONDITION` + `NOT_BOOTSTRAPPED` | Called before checkpoint bootstrap completes (CC-19) |

### `GetHead`

| Field | Type |
|---|---|
| `head_root` | `bytes` |
| `head_slot` | `uint64` |
| `justified` | `Checkpoint { epoch, root }` |
| `finalized` | `Checkpoint { epoch, root }` |

`Checkpoint` here is the fork-choice checkpoint **identity** (epoch + root), not a re-model of a
consensus body. Blocks still cross service boundaries only as `bytes ssz`.

### `SubscribeEvents`

**Request cursor** (`Cursor`, optional — unset means "start live, no replay"):

| Field | Type | Role |
|---|---|---|
| `session_id` | `uint64` | Per-process random id; a stale session is rejected (ADR-P1-11) |
| `seq` | `uint64` | Authoritative resume point (replay from `seq + 1`) |
| `slot` | `uint64` | Validated against the ring entry; human-readable identity |
| `root` | `bytes` | Validated against the ring entry; **idempotency key** for consumers |

**Event**:

| Field | Type |
|---|---|
| `seq` | `uint64` |
| `slot` | `uint64` |
| `root` | `bytes` — carries identity; consumers must be idempotent |
| `kind` | `EventKind` — `HEAD`, `CHAIN_REORG`, `FINALIZED_CHECKPOINT`, `BLOCK_IMPORTED` |
| `payload` | `bytes` — kind-specific, opaque to the bus |

**Bounds** (config defaults; CC-18c implements):

| Bound | Default | Behaviour on breach |
|---|---|---|
| Event ring | **1024** events | Older entries evicted; resubscribe with that cursor → `CURSOR_TOO_OLD` |
| Per-subscriber queue | **256** deep | Slow consumer: stream terminated with `RESOURCE_EXHAUSTED`; reconnect + cursor replay |

**`FAILED_PRECONDITION` + `google.rpc.ErrorInfo` reasons** (Architecture §7.3 / §7.6):

| `reason` | Condition | Consumer action |
|---|---|---|
| `CURSOR_TOO_OLD` | `cursor.seq + 1 < ring.front().seq` (evicted) | Fall back to `GetHead`, resubscribe with no cursor |
| `CURSOR_UNKNOWN_SESSION` | `cursor.session_id != ring.session_id` (server restarted) | Same, plus re-bootstrap assumptions |

Domain convention: `"eth.chain.v1"`. Construction helper and the tonic 0.14 detail-attachment
shape live in `cc-proto` (`status_with_error_info` / `error_info_from_status`):

1. `ErrorInfo { reason, domain, … }`
2. `prost_types::Any { type_url: "type.googleapis.com/google.rpc.ErrorInfo", value: encode }`
3. `google.rpc.Status { code, message, details: [any] }` → `encode_to_vec`
4. `tonic::Status::with_details(code, message, bytes)` → `grpc-status-details-bin` trailer

### `GetCommitteeShuffling` (CC-1F)

**Served** for the head state's **current and next epoch** only (Architecture §7.7 / §16/6). Both
handlers read through the core thread's single FIFO `Query` command — no second copy of the state is
held on the gRPC side (§7.1).

**Request:**

| Field | Type | Notes |
|---|---|---|
| `epoch` | `uint64` | Must be head `current` or `current + 1` |

**Response:**

| Field | Type | Notes |
|---|---|---|
| `shuffled_indices` | `repeated uint64` | Packed committee assignment — active validators in shuffled order; committees are contiguous slices of this list (same layout as the head state's `ShufflingCache`) |
| `dependent_root` | `bytes` | Decision root used as the **cache key** (§5.4). See stability rules below. |
| `epoch` | `uint64` | Echo of the served epoch |
| `committees_per_slot` | `uint64` | So the caller can slice committees without a second query |

**`dependent_root` stability (Phase 5 caching):**

| Case | Value | Stable within epoch? |
|---|---|---|
| **Current** epoch (and any epoch whose `start_slot(epoch) − 1` is historical) | Block root at `start_slot(epoch) − 1` (true decision root) | Yes |
| **Next** epoch mid-epoch (dependent slot still current/future) | **Current** epoch's true decision root (`start_slot(current) − 1`) — provisional but **stable** for the whole current epoch on a branch | Yes — does **not** advance with head slot |

Using the current-epoch decision root as the mid-window next-epoch key avoids per-slot cache
thrash while remaining branch-distinguishing for forks that diverged before the current epoch.
The assignment itself is seed-determined (RANDAO / `MIN_SEED_LOOKAHEAD`) and matches
`compute_shuffled_active_indices`; mid-epoch forks that share current-epoch history correctly share
the same next-epoch assignment and the same provisional key.

**gRPC statuses:**

| Status | When |
|---|---|
| `FAILED_PRECONDITION` | Requested epoch is outside `{current, current+1}` |
| `FAILED_PRECONDITION` + `NOT_BOOTSTRAPPED` | Called before checkpoint bootstrap |
| `INTERNAL` | Shuffling / decision-root computation failed on the head state |

**Caching contract:** the `attestation` service (Phase 5) must key its cache by
`(epoch, dependent_root)`. Two competing branches in the same epoch produce different
`dependent_root` values and different assignments — that is why the field exists.

### `GetValidatorPubkeys` (CC-1F)

**Served** unconditionally as a registry read through the head state's validator list
(`PubkeyIndexMap` is the reverse direction; this RPC is index → pubkey).

**Request** — either a contiguous range **or** an explicit list:

| Field | Type | Notes |
|---|---|---|
| `start_index` | `uint64` | Range start (used when `indices` is empty) |
| `count` | `uint64` | Range length (used when `indices` is empty) |
| `indices` | `repeated uint64` | When non-empty, overrides `start_index`/`count` |

**Bound:** at most **256** indices per request. Over that — or a zero-count empty request — is
`INVALID_ARGUMENT`, **never a truncated response** (same discipline as CC-1E's 128-attestation batch
bound and Phase 2's `GetValidatorRecords`).

**Response:**

| Field | Type | Notes |
|---|---|---|
| `indices` | `repeated uint64` | Echo of the resolved indices |
| `pubkeys` | `repeated bytes` | 48-byte BLS public keys, parallel to `indices` |

**gRPC statuses:**

| Status | When |
|---|---|
| `INVALID_ARGUMENT` | Empty request, `count == 0` with empty `indices`, more than 256 indices, or any index out of registry range |
| `FAILED_PRECONDITION` + `NOT_BOOTSTRAPPED` | Called before checkpoint bootstrap |

### Vendored `google.rpc` (ADR-P1-14)

| Path | Role |
|---|---|
| `proto/third_party/google/rpc/status.proto` | `google.rpc.Status` wire envelope for details |
| `proto/third_party/google/rpc/error_details.proto` | `ErrorInfo` and kin |
| `proto/third_party/google/rpc/README.md` | Provenance: upstream, **commit**, fetch date, license |

`buf.yaml` lists `third_party` under both `lint.ignore` and `breaking.ignore` — these files are
not our contract and fail `DEFAULT`/`FILE` by design. The ignore is path-scoped: a deliberate
violation under `eth/` still fails `buf lint` (negative check; re-run any time):

```bash
# Temporary camelCase field under eth/ must fail lint (ignore is not over-broad).
# Do not commit the violation.
cd proto && buf lint   # expect FIELD_LOWER_SNAKE_CASE on eth/… only; third_party silent
```

`crates/proto/build.rs` adds `proto/third_party` to the `protox` include path so imports resolve
as `google/rpc/…`. Well-known types (`google/protobuf/*`) come from `protox`'s built-in resolver.

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
