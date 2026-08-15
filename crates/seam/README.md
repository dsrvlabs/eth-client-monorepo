# cc-seam

Typed handles and overflow contracts for internal service edges
(`[ARCH]` §2.1).

This crate is traits and types only. Transport impls land later:

- `InProcess` — S1-A-08
- `Ipc` — S1-A-09

**Both impls stay buildable permanently** (`[ARCH]` §9.2). The losing
impl is never deleted at S3; it is demoted to a test fixture so the
conformance suite stays honest.
