# syntax=docker/dockerfile:1.7
# Multi-stage image for all six service binaries (Architecture §6.1, CC-04).
# BuildKit: shared builder across compose targets; ARG SERVICE selects the runtime binary.

ARG RUST_VERSION=1.97.1
ARG DEBIAN_RELEASE=bookworm

# ── builder ───────────────────────────────────────────────────────────────
# rust:*-bookworm carries the C toolchain (cc, libc-dev) required for blst in Phase 1.
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
ARG RUST_VERSION
WORKDIR /build

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
# Embedded by cc-spec-tests (include_str from crates/spec-tests/src/ → repo root).
COPY spec-vectors.lock ./
# crates/proto/build.rs resolves ../../proto — must be present before cargo build.
COPY proto/ proto/
COPY crates/ crates/
COPY services/ services/
COPY bin/ bin/
# Local-dev TOML defaults (not required for cargo build; shipped to runtime below).
COPY config/ config/

# R-6 / §10 item 1: ARG RUST_VERSION must equal rust-toolchain.toml's channel.
RUN channel="$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)" && \
    test -n "${channel}" && \
    test "${channel}" = "${RUST_VERSION}"

# .dockerignore excludes .git/; pass the SHA as a build arg (crates/bootstrap/build.rs).
ARG CC_GIT_SHA=unknown
ENV CC_GIT_SHA=${CC_GIT_SHA}

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --workspace --release --locked && \
    mkdir -p /out && \
    for b in cc-chain cc-p2p cc-attestation cc-engine cc-beacon-api cc-storage; do \
      cp "target/release/${b}" /out/; \
    done

# ── health probe (arch-aware, checksummed) ────────────────────────────────
FROM debian:${DEBIAN_RELEASE}-slim AS probe
ARG TARGETARCH
ARG HEALTH_PROBE_VERSION=v0.4.54
ARG HEALTH_PROBE_SHA256_AMD64=e25b2f7e50176a909b9036e2c7598d8997b524a87d24af81d92b796e6dfb4897
ARG HEALTH_PROBE_SHA256_ARM64=bc77dba74822d2b00c2585d225b77048cdb044407a0541ebf4c12711c2fdf779
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL -o /grpc-health-probe \
      "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}" && \
    case "${TARGETARCH}" in \
      amd64) echo "${HEALTH_PROBE_SHA256_AMD64}  /grpc-health-probe" ;; \
      arm64) echo "${HEALTH_PROBE_SHA256_ARM64}  /grpc-health-probe" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac | sha256sum -c - && chmod +x /grpc-health-probe

# ── runtime ───────────────────────────────────────────────────────────────
# debian:bookworm-slim (not distroless) — Phases 0–2 need `docker compose exec … sh` (ADR-09).
# WORKDIR /app so cc-config's relative `config/<service>.toml` (Toml::file_exact) resolves.
FROM debian:${DEBIAN_RELEASE}-slim AS runtime
ARG SERVICE
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 cc && \
    useradd --system --uid 10001 --gid 10001 --no-create-home cc
COPY --from=probe /grpc-health-probe /usr/local/bin/grpc-health-probe
# ARG cannot be interpolated into ENTRYPOINT — copy to a fixed path.
COPY --from=builder /out/${SERVICE} /usr/local/bin/service
# Compose overrides via CC_* env; TOML must still exist (figment file_exact fails if missing).
COPY config/ /app/config/
# p2p (and later services) persist relative paths under WORKDIR as user `cc`
# (e.g. config node_key_path = "./data/node_key"). /app is root-owned after
# COPY; create a writable data dir before dropping privileges.
RUN mkdir -p /app/data && chown -R 10001:10001 /app/data
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/service"]
