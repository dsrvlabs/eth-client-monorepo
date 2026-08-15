#!/usr/bin/env bash
# S0a-B-05 — resolve buf `breaking_against` from the GitHub event.
#
# The proto job used to hardcode `#branch=develop` on every push. Pushes to
# `main` must compare against `main`. This script is the single source of
# that URL so the rule can be dry-run against workflow event fixtures
# instead of grepping ci.yml.
#
# Usage:
#   bash scripts/proto-breaking-against.sh              # print URL from GITHUB_*
#   bash scripts/proto-breaking-against.sh --self-test  # fixture dry-run
#
# Event inputs (Actions sets these; --self-test sets them per fixture):
#   GITHUB_EVENT_NAME   push | pull_request
#   GITHUB_EVENT_PATH   GitHub event JSON (preferred)
#   GITHUB_REF_NAME     fallback push-target branch when EVENT_PATH is unset
#   GITHUB_EVENT_BEFORE fallback push parent SHA when EVENT_PATH is unset
#   GITHUB_BASE_REF     fallback PR base when EVENT_PATH is unset
#   GITHUB_SERVER_URL   default https://github.com
#   GITHUB_REPOSITORY   default dsrvlabs/eth-client-monorepo
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="${ROOT}/.github/fixtures/proto-breaking"

die() {
  echo "error: $*" >&2
  exit 1
}

repo_git_url() {
  local server="${GITHUB_SERVER_URL:-https://github.com}"
  local repo="${GITHUB_REPOSITORY:-dsrvlabs/eth-client-monorepo}"
  printf '%s/%s.git' "${server%/}" "${repo}"
}

# field: push_branch | before | base_ref
event_field() {
  local field="$1"
  local path="${GITHUB_EVENT_PATH:-}"
  [[ -n "${path}" ]] || return 1
  [[ -f "${path}" ]] || die "GITHUB_EVENT_PATH is not a file: ${path}"
  command -v python3 >/dev/null 2>&1 || die "python3 is required to read GITHUB_EVENT_PATH"
  python3 - "${path}" "${field}" <<'PY'
import json, sys

path, field = sys.argv[1], sys.argv[2]
with open(path) as fh:
    ev = json.load(fh)

if field == "push_branch":
    ref = ev.get("ref") or ""
    prefix = "refs/heads/"
    sys.stdout.write(ref[len(prefix):] if ref.startswith(prefix) else ref)
elif field == "before":
    sys.stdout.write(ev.get("before") or "")
elif field == "base_ref":
    pr = ev.get("pull_request") or {}
    base = pr.get("base") or {}
    sys.stdout.write(base.get("ref") or "")
else:
    sys.stderr.write("error: unknown event field: %s\n" % field)
    sys.exit(2)
PY
}

check_branch() {
  local branch="$1"
  local what="$2"
  [[ -n "${branch}" ]] || die "${what} is empty"
  # `#` and `,` are buf git-ref URL separators; a branch containing them
  # would silently split the against spec.
  case "${branch}" in
    *[,#]*) die "${what} contains buf against-URL metacharacters: ${branch}" ;;
  esac
}

resolve() {
  local event_name="${GITHUB_EVENT_NAME:-}"
  [[ -n "${event_name}" ]] || die "GITHUB_EVENT_NAME is required"

  local branch before
  case "${event_name}" in
    push)
      if [[ -n "${GITHUB_EVENT_PATH:-}" ]]; then
        branch="$(event_field push_branch)" || exit 1
        before="$(event_field before)" || exit 1
      else
        branch="${GITHUB_REF_NAME:-}"
        before="${GITHUB_EVENT_BEFORE:-}"
      fi
      check_branch "${branch}" "push target branch"
      printf '%s#branch=%s,ref=%s,subdir=proto\n' "$(repo_git_url)" "${branch}" "${before}"
      ;;
    pull_request|pull_request_target)
      if [[ -n "${GITHUB_EVENT_PATH:-}" ]]; then
        branch="$(event_field base_ref)" || exit 1
      else
        branch="${GITHUB_BASE_REF:-}"
      fi
      check_branch "${branch}" "PR base branch"
      printf '%s#branch=%s,subdir=proto\n' "$(repo_git_url)" "${branch}"
      ;;
    *)
      die "unsupported event: ${event_name}"
      ;;
  esac
}

self_test() {
  local failed=0

  command -v python3 >/dev/null 2>&1 || die "python3 is required for --self-test"

  run_case() {
    local name="$1" event="$2" fixture="$3" want="$4"
    local got path="${FIXTURE_DIR}/${fixture}"
    [[ -f "${path}" ]] || die "missing fixture: ${path}"
    got="$(
      env -u GITHUB_REF_NAME -u GITHUB_BASE_REF -u GITHUB_EVENT_BEFORE \
        GITHUB_EVENT_NAME="${event}" \
        GITHUB_EVENT_PATH="${path}" \
        GITHUB_SERVER_URL="https://github.com" \
        GITHUB_REPOSITORY="dsrvlabs/eth-client-monorepo" \
        bash "${ROOT}/scripts/proto-breaking-against.sh"
    )"
    if [[ "${got}" != "${want}" ]]; then
      echo "FAIL ${name}" >&2
      echo "  got:  ${got}" >&2
      echo "  want: ${want}" >&2
      failed=1
    else
      echo "ok: ${name}"
    fi
  }

  run_case \
    "push to main compares against main" \
    push push-main.json \
    "https://github.com/dsrvlabs/eth-client-monorepo.git#branch=main,ref=1111111111111111111111111111111111111111,subdir=proto"

  run_case \
    "push to develop compares against develop" \
    push push-develop.json \
    "https://github.com/dsrvlabs/eth-client-monorepo.git#branch=develop,ref=2222222222222222222222222222222222222222,subdir=proto"

  run_case \
    "PR against main compares against main" \
    pull_request pull_request-main.json \
    "https://github.com/dsrvlabs/eth-client-monorepo.git#branch=main,subdir=proto"

  run_case \
    "PR against develop compares against develop" \
    pull_request pull_request-develop.json \
    "https://github.com/dsrvlabs/eth-client-monorepo.git#branch=develop,subdir=proto"

  # Direct falsifier for the old hardcoded develop baseline.
  local main_got
  main_got="$(
    env -u GITHUB_REF_NAME -u GITHUB_BASE_REF -u GITHUB_EVENT_BEFORE \
      GITHUB_EVENT_NAME=push \
      GITHUB_EVENT_PATH="${FIXTURE_DIR}/push-main.json" \
      GITHUB_SERVER_URL="https://github.com" \
      GITHUB_REPOSITORY="dsrvlabs/eth-client-monorepo" \
      bash "${ROOT}/scripts/proto-breaking-against.sh"
  )"
  case "${main_got}" in
    *"#branch=develop,"*)
      echo "FAIL push-main must not emit branch=develop: ${main_got}" >&2
      failed=1
      ;;
    *"#branch=main,"*)
      echo "ok: push-main is not the hardcoded develop baseline"
      ;;
    *)
      echo "FAIL push-main missing branch=main: ${main_got}" >&2
      failed=1
      ;;
  esac

  # Env fallback (no EVENT_PATH) still keys off the push target.
  local env_got
  env_got="$(
    env -u GITHUB_EVENT_PATH \
      GITHUB_EVENT_NAME=push \
      GITHUB_REF_NAME=main \
      GITHUB_EVENT_BEFORE=3333333333333333333333333333333333333333 \
      GITHUB_SERVER_URL="https://github.com" \
      GITHUB_REPOSITORY="dsrvlabs/eth-client-monorepo" \
      bash "${ROOT}/scripts/proto-breaking-against.sh"
  )"
  local env_want="https://github.com/dsrvlabs/eth-client-monorepo.git#branch=main,ref=3333333333333333333333333333333333333333,subdir=proto"
  if [[ "${env_got}" != "${env_want}" ]]; then
    echo "FAIL env fallback push-main" >&2
    echo "  got:  ${env_got}" >&2
    echo "  want: ${env_want}" >&2
    failed=1
  else
    echo "ok: env fallback push-main"
  fi

  if [[ "${failed}" -ne 0 ]]; then
    die "proto breaking baseline fixture dry-run failed"
  fi
  echo "ok: proto breaking baseline fixtures"
}

case "${1:-}" in
  --self-test) self_test ;;
  "" ) resolve ;;
  -h|--help)
    sed -n '2,24p' "$0"
    ;;
  *)
    die "unknown argument: $1 (want --self-test)"
    ;;
esac
