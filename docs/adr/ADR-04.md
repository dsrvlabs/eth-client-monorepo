# ADR-04 — Proto codegen is `protox` + `tonic-prost-build` at crate build; generated `.rs` is not checked in

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 0 (workspace-wide; scope shrinks as protos are deleted S1–S5)
- **Issues:** S1-B-07, CC-02
- **Citations:** `crates/proto/build.rs:1-8,30-38`; `crates/proto/src/lib.rs:3-5`; `docs/contracts.md:16`; `proto/buf.yaml:2`
- **Provenance:** re-derived from code (2026-08-16)

This is **ADR-04**. It records the live codegen path. It does not invent a
check-in policy that the tree does not have.

## Context

Every service binary depends on the contracts under `proto/`. Two generation
stories were available: run `buf generate` (or `protoc`) as a separate step and
commit the resulting `.rs`, or generate at `cc-proto` build time from the
checked-in `.proto` tree.

`crates/proto/build.rs` already does the second. It compiles every `.proto`
under `proto/` with pure-Rust `protox`, writes a file-descriptor set for
tonic-reflection, and feeds the descriptors to `tonic_prost_build::configure()`
with `generate_default_stubs(true)`. No `protoc` binary is required in Docker,
CI, or on a dev machine (`build.rs:1-3`). `proto/buf.yaml:2` has no
`buf.gen.yaml` for the same reason: *codegen is `build.rs`*.

`crates/proto/src/lib.rs:3-5` states the complementary half: the source of
truth is the top-level `proto/` tree; code is generated at build time
(`protox` → `tonic_prost_build::compile_fds`); **no generated `.rs` files are
checked in** (CC-02/1). The crate tree is `build.rs` and `src/lib.rs` plus
`include_proto!` modules. `[ARCH]` §10.4's census row that said "generated
`.rs` checked in" is a stale summary of this id; the live citation sites are
the authority.

`docs/contracts.md:16` repeats the negative: codegen is
`crates/proto/build.rs` via `protox` + `tonic-prost-build` — **not**
`buf generate`. buf stays the lint / format / `FILE`-breaking gate (ADR-05).

## Decision

**Generate Rust stubs at `cc-proto` build time with `protox` +
`tonic-prost-build`.** Do not run `buf generate`. Do not require a `protoc`
binary. Do not check generated `.rs` into the repository.

The include path is `proto/` plus `proto/third_party/` so vendored
`google/rpc/{status,error_details}.proto` resolve as `google/rpc/…`
(ADR-P1-14). Well-known `google/protobuf/*` types come from `protox`'s
built-in `GoogleFileResolver`, not a second vendor tree.

Walk the proto tree deterministically (sorted paths, no symlink follow) so
codegen output is stable (CC-02/1). Write `eth_descriptor.bin` from the
compiled file-descriptor set yourself; `compile_fds` does not honour
`file_descriptor_set_path`.

Default method bodies return `UNIMPLEMENTED` so an additive RPC does not
force every service stub to grow an empty handler.

## Consequences

What this makes easy:

- A proto change is a Rust rebuild, not a generated-file PR.
- CI and developer machines need `buf` for lint/breaking and `cargo` for
  stubs — not `protoc`.
- Citation sites that say *not `buf generate`* (`docs/contracts.md:16`,
  `proto/buf.yaml:2`) resolve here.

What this makes hard:

- Reviewers cannot diff generated Rust in git. Behavioural contract review
  is the `.proto` plus `buf breaking` (ADR-05).
- Anyone who wants committed stubs has to contradict this file, not just
  add an `out_dir` under `crates/proto/src/`.

What this forbids:

- `buf generate` / `protoc` as the house codegen path.
- Checking generated `.rs` into `crates/proto/` (or anywhere else) as the
  source of truth.
- A `buf.gen.yaml` that re-introduces a second generator beside `build.rs`.

## Alternatives considered

**`buf generate` (or `protoc`) with generated `.rs` checked in.** The usual
monorepo shape. Rejected in the live tree: it needs a plugin binary, a
`buf.gen.yaml`, and a generated-file review surface. The crate already
generates at build time. `[ARCH]` §10.4's "checked in" wording is not a
decision to restore that shape.

**Check in the generated `.rs` while still running `build.rs`.** Rejected by
`lib.rs:3-5`. Two sources of truth would drift the first time someone
forgets to regenerate.

**None further recorded.** Re-derived; do not invent a third generator.

## Refactor impact

**Survives.** Scope shrinks as gRPC contracts are deleted S1–S5. The
remaining proto packages keep this path until they go. S2/S3 deleting a
`.proto` is not a reason to switch the survivors to `buf generate`.
