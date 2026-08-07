# eth-client-monorepo — developer entrypoints
# Mirrors required CI gates (fmt/clippy/test/proto/deps) plus local compose/vectors helpers.
# Toolchain pin lives only in rust-toolchain.toml; do not hardcode the Rust version here.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# ── Cargo ────────────────────────────────────────────────────────────────────
CARGO       ?= cargo
CARGO_FLAGS ?= --locked
CLIPPY_FLAGS ?= --workspace --all-targets --all-features $(CARGO_FLAGS)
NEXTEST_FLAGS ?= --workspace $(CARGO_FLAGS) --profile ci

# ── Services (compose / binaries) ────────────────────────────────────────────
SERVICES := chain p2p attestation engine beacon-api storage

# ── Git SHA for compose build_info ───────────────────────────────────────────
CC_GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPTS := scripts
PROTO   := proto

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} \
		/^[a-zA-Z0-9_.-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ══════════════════════════════════════════════════════════════════════════════
# Build
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: build
build: ## Build workspace (debug)
	$(CARGO) build --workspace $(CARGO_FLAGS)

.PHONY: release
release: ## Build workspace (release)
	$(CARGO) build --workspace --release $(CARGO_FLAGS)

.PHONY: check
check: ## Type-check workspace (no codegen of binaries)
	$(CARGO) check --workspace --all-targets $(CARGO_FLAGS)

# ══════════════════════════════════════════════════════════════════════════════
# Test
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: test
test: ## Run tests with cargo-nextest (CI profile)
	$(CARGO) nextest run $(NEXTEST_FLAGS)

.PHONY: test-cargo
test-cargo: ## Run tests with cargo test (fallback without nextest)
	$(CARGO) test --workspace $(CARGO_FLAGS)

# ══════════════════════════════════════════════════════════════════════════════
# Lint / format (required CI: fmt, clippy)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: fmt
fmt: ## Format all Rust sources
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting (CI: fmt job)
	$(CARGO) fmt --all --check

.PHONY: clippy
clippy: ## Clippy with -D warnings (CI: clippy job)
	$(CARGO) clippy $(CLIPPY_FLAGS) -- -D warnings

.PHONY: lint
lint: fmt-check clippy check-dag check-env ## Local lint suite (fmt + clippy + guards)

# ══════════════════════════════════════════════════════════════════════════════
# Policy guards (scripts/ — also wired into CI)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: check-dag
check-dag: ## Enforce crate dependency DAG (Architecture §2.2)
	bash $(SCRIPTS)/check-crate-dag.sh

.PHONY: check-env
check-env: ## No std::env::var outside crates/config (CC-09/3)
	bash $(SCRIPTS)/check-no-env-reads.sh

.PHONY: check-remodelling
check-remodelling: ## No consensus containers remodelled as protos (CC-02/4)
	bash $(SCRIPTS)/check-no-remodelling.sh

# ══════════════════════════════════════════════════════════════════════════════
# Proto (required CI: proto job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: proto-lint
proto-lint: ## buf lint
	buf lint $(PROTO)

.PHONY: proto-fmt
proto-fmt: ## Format protos with buf
	buf format -w $(PROTO)

.PHONY: proto-fmt-check
proto-fmt-check: ## Check proto formatting
	buf format -d --exit-code $(PROTO)

.PHONY: proto-breaking
proto-breaking: ## FILE-category breaking vs origin/develop (needs fetch)
	buf breaking $(PROTO) --against '.git#branch=origin/develop,subdir=proto'

.PHONY: proto
proto: proto-lint proto-fmt-check check-remodelling ## Local proto suite (lint + format + remodelling)

# ══════════════════════════════════════════════════════════════════════════════
# Supply chain (required CI: deps job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: deny
deny: ## cargo deny advisories bans licenses sources
	$(CARGO) deny check advisories bans licenses sources

.PHONY: deps
deps: deny check-dag ## Supply-chain gate (deny + DAG)

# ══════════════════════════════════════════════════════════════════════════════
# Spec vectors (non-required CI: vectors job; never implicit in cargo)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: vectors
vectors: ## Fetch/verify pinned consensus-spec vectors (~8–10 GB)
	bash $(SCRIPTS)/fetch-spec-vectors.sh

.PHONY: vectors-force
vectors-force: ## Re-download every vector artifact
	bash $(SCRIPTS)/fetch-spec-vectors.sh --force

.PHONY: vectors-layout
vectors-layout: ## Regenerate spec-vectors-layout.md from cache
	bash $(SCRIPTS)/record-vector-layout.sh

# ══════════════════════════════════════════════════════════════════════════════
# Docker / compose (non-required CI: compose job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: compose-build
compose-build: ## Build compose images (sets CC_GIT_SHA)
	CC_GIT_SHA=$(CC_GIT_SHA) docker compose build

.PHONY: compose-up
compose-up: ## Start stack detached
	CC_GIT_SHA=$(CC_GIT_SHA) docker compose up -d

.PHONY: compose-down
compose-down: ## Stop stack
	docker compose down

.PHONY: compose-ps
compose-ps: ## Show compose service status
	docker compose ps

.PHONY: wait-healthy
wait-healthy: ## Wait until all six services are healthy (default 90s)
	bash $(SCRIPTS)/wait-healthy.sh

.PHONY: prove-health
prove-health: ## Mutual-health proof (stop chain → NOT_SERVING → recover)
	bash $(SCRIPTS)/prove-mutual-health.sh

.PHONY: compose
compose: compose-build compose-up wait-healthy ## Build, up, and wait-healthy

.PHONY: compose-proof
compose-proof: compose prove-health ## Full mutual-health proof (compose CI path)

# ══════════════════════════════════════════════════════════════════════════════
# Meta
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: ci
ci: fmt-check clippy check-dag check-env test proto deps ## Required local CI gates (no compose/vectors)

.PHONY: clean
clean: ## cargo clean
	$(CARGO) clean

.PHONY: clean-all
clean-all: clean compose-down ## cargo clean + compose down
