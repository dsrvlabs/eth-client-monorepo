# ADR-P1-14 — Vendored `google.rpc` protos under `third_party`, excluded from lint/breaking

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 1 (survives to S5)
- **Issues:** S1-B-07, CC-18a
- **Citations:** `crates/proto/build.rs:5-8`; `crates/proto/src/lib.rs:14-16`; `proto/third_party/google/rpc/README.md`; `proto/buf.yaml:3-4,16-17,37-38`; `docs/contracts.md:321-340`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-P1-14**. Cursor and bootstrap failures need
`FAILED_PRECONDITION` + `google.rpc.ErrorInfo{reason}`. That type is not
our contract.

## Context

Phase 0 shipped no BSR dependency and codegen is `protox` over local
files (ADR-04). Attaching `ErrorInfo` reasons (`CURSOR_TOO_OLD`,
`CURSOR_UNKNOWN_SESSION`, …) to `tonic::Status` requires
`google/rpc/{status,error_details}.proto` on the local include path.

Pulling them from the BSR would add `buf.lock` *and* a `buf export`
step. That is the path Architecture §7.6 and
`proto/third_party/google/rpc/README.md` deliberately avoided.

Those two files fail `DEFAULT` / `FILE` by design (`PACKAGE_DIRECTORY_MATCH`,
`PACKAGE_VERSION_SUFFIX`, multi-value `go_package`, …). They are
googleapis' contract, not ours. Putting them under `eth/` would either
fail lint or force a workspace-wide ignore.

## Decision

**Vendor `status.proto` and `error_details.proto` under
`proto/third_party/google/rpc/`.** Provenance is the README: upstream
googleapis/googleapis, commit `02362883cd16428e5f57fa60ca8d8f60dafcdba7`,
fetched 2026-08-06, Apache-2.0. Well-known imports
(`google/protobuf/any.proto`, `duration.proto`) are **not** vendored;
`protox`'s built-in `GoogleFileResolver` supplies them.

`proto/buf.yaml` lists `third_party` under both `lint.ignore` and
`breaking.ignore`. The ignore is path-scoped: a deliberate violation
under `eth/` still fails `buf lint`.

`crates/proto/build.rs` puts `proto/third_party` **first** on the
`protox` include path so the files are named `google/rpc/…` rather than
`third_party/google/rpc/…`. `cc-proto` exposes them as `cc_proto::rpc`
for `ErrorInfo` packing.

Refresh is a curl of those two files at a pinned commit, then an update
of the README SHA. It is not `buf export`.

## Consequences

What this makes easy:

- Callers attach `ErrorInfo` reasons without a BSR or `buf.lock`.
- `buf lint` / `buf breaking` stay honest on `eth/`.
- Codegen stays local-files-only (ADR-04).

What this makes hard:

- A googleapis refresh is a manual two-file curl plus a README SHA
  bump, not a `buf` module bump.

What this forbids:

- Taking `google.rpc` from the BSR as a `buf.lock` dependency.
- Vendoring `google/protobuf/*` well-known types beside these two files.
- Widening `lint.ignore` / `breaking.ignore` from `third_party` to the
  whole workspace.
- Treating `third_party` as house contract surface (ADR-05 does not
  apply to it).

## Alternatives considered

**BSR module + `buf export`.** Rejected: Phase 0 shipped no BSR
dependency; codegen is `protox` over local files. A lockfile and an
export step for two protos is the thing the README exists to avoid.

**Hand-roll an `ErrorInfo` message under `eth.common.v1`.** Rejected:
gRPC clients and `tonic` expect `type.googleapis.com/google.rpc.ErrorInfo`.
A house twin would not decode as the standard detail.

**None further recorded.** Re-derived.

## Refactor impact

**Survives to S5.** As long as any gRPC surface returns
`FAILED_PRECONDITION` + `ErrorInfo`, the vendor tree stays. Deleting the
last such RPC is what retires it, not a buf-module migration.
