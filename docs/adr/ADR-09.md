# ADR-09 — Runtime image is `debian:bookworm-slim`, not distroless

- **Status:** proposed · revisit at S3 · superseded-by: — · **Date:** 2026-08-16
- **Phase:** 0–2 (operator debug surface)
- **Issues:** S1-B-16
- **Citations:** 1 site — `Dockerfile:59-61`
- **Provenance:** re-derived from code (2026-08-16) — records the live base image as an R-17 deferral

This is **ADR-09**. `[ARCH]` §10.4 class **(b)**: *"the stated reason expires with
the program; re-decide at S3."* This file is the placeholder (R-17). It is not a
commitment to ship a shell in production forever.

## Context

The runtime stage is `FROM debian:${DEBIAN_RELEASE}-slim` (`Dockerfile:61`),
commented at `:59` as **not distroless** because **Phases 0–2 need
`docker compose exec … sh`**. The image installs `ca-certificates`, drops to
`cc` (uid 10001), and runs a single `ENTRYPOINT`. It is a debug-friendly
slim Debian, not a full desktop, and not a distroless static rootfs.

Distroless (or a scratch + ca-certs image) would remove the shell, the
package manager, and most of the attack surface that is not the service
binary. That is the usual production default. The Phase 0–2 program
rejected it because soak, restart drills, and `compose exec` inspection
were how the services were operated. That reason is tied to a six-service
compose topology and a human-in-the-loop debug loop.

S2 collapses compose toward `beacon-core` + EL (`[ARCH]` §4 / `S2-J-01`).
S3 is the first stage that can treat the node as a product image rather
than a lab. The stated reason expires there. Whether the image stays
Debian is then a production decision, not a soak convenience.

## Decision

**Until S3, the runtime image stays `debian:bookworm-slim` (or the pinned
`DEBIAN_RELEASE`-slim equivalent). It is not distroless.**

Keep a working `sh` so `docker compose exec <svc> sh` remains possible on
the Phase 0–2 stack. Do not switch the runtime `FROM` to distroless /
scratch / wolfi-as-a-shell-less-default in an S1 or S2 PR "for supply
chain."

**Revisit at S3.** Re-decide the runtime base when the six-service debug
loop is no longer the operating model. Candidates belong in the S3
write-up: stay on slim Debian; move to distroless; or split debug vs
release images. This file stays `proposed` until that revisit.

## Consequences

What this makes easy:

- Operators and soak scripts can `compose exec` into a running service
  without a sidecar debug image.
- The runtime `FROM` line has a named owner and a named expiry.

What this makes hard:

- The production image carries a shell and Debian's default userland
  through S2. That is accepted debt, not an oversight.
- Image-CVE scanners will keep reporting packages that exist only because
  of this record.

What this forbids:

- Silent distroless / scratch cutover before the S3 revisit.
- Treating `:59`'s comment as a permanent product requirement.
- Closing the S2 entry gate by marking this `accepted`.

## Alternatives considered

**Distroless now.** Rejected for Phases 0–2. The program's debug loop is
`compose exec`. Removing `sh` would force a second image or host-side
only inspection before the topology has settled.

**Debug image + distroless release, now.** A real alternative, and likely
the S3 shape. Not taken at S1: two images double the pin/digest surface
while compose still has six service builds. Record it for the revisit;
do not invent the split in this file.

**Wait for S3 and write nothing.** Rejected. R-17: an honest deferral is
fine; a silent one is not.

## Refactor impact

**Revisit at S3.** The stated reason expires with the Phase 0–2 program.

| Stage | What happens to this record |
|---|---|
| S1 | This file. No `Dockerfile` change. |
| S2 | Compose collapses; `compose exec` may still be how `beacon-core` is inspected. Do not flip the base in the same PR that folds processes. |
| S3 | **This ADR is revisited.** Choose the production base. Supersede or re-accept. |
