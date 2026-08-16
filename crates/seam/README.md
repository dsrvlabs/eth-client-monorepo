# cc-seam

Typed handles and overflow contracts for internal service edges
(`[ARCH]` §2.1).

Transport impls:

- `InProcess` — bounded tokio mpsc + oneshot replies (S1-A-08). Single Hull.
  These channels **are** the live queues; do not wrap them in front of the
  scheduler import lane, the event ring, or `publish_fwd`.
- `Ipc` — wraps today's tonic-over-TCP edge; S3 option is a unix socket +
  `SO_PEERCRED` (S1-A-09). The jittered reconnect loop stays inside this impl.

**Both impls stay buildable permanently** (`[ARCH]` §9.2). The losing
impl is never deleted at S3; it is demoted to a test fixture so the
conformance suite stays honest.
