#!/usr/bin/env bash
# scripts/el-snapshot-restore.sh — CC-39b ethPandaOps Hoodi geth snapshot restore
#
# Resolves the live `latest` pointer (V-5), stream-extracts the zstd tarball with
# **no second copy on disk** (CC-39 /4, OQ-P3-5), records block number /
# content-length / restore wall clock / `du -sh` of the result, and optionally
# waits for `eth_syncing == false` after geth is started separately.
#
# Stream-extract form (required; never download-to-file then extract):
#   wget -O - <url> | tar -I zstd -xvf - -C <elstore>
# The live path uses the same wget | zstd stream with **path-safe** member
# checks (assert_safe_member / assert_safe_tarball stream) before any write —
# equivalent operator form above; GNU tar alone does not reject `../` members.
#
# Integrity residual: ethPandaOps does **not** publish a per-snapshot checksum
# (no .sha256 / SHA256SUMS next to snapshot.tar.zst). Trust is TLS + host
# allowlist + path-safe extract; the geth **image** digest is CC-39a / runbook.
#
# Usage:
#   bash scripts/el-snapshot-restore.sh                  # full restore
#   bash scripts/el-snapshot-restore.sh --dry-run        # V-5 only + free-space check
#   bash scripts/el-snapshot-restore.sh --probe-throughput # first ~10 min of download only
#   bash scripts/el-snapshot-restore.sh --record-only PATH # write V-5 JSON without restore
#   bash scripts/el-snapshot-restore.sh --wait-synced    # wait-only (no extract; elstore may be full)
#   bash scripts/el-snapshot-restore.sh --restore --wait-synced  # extract then wait
#
# Environment:
#   ELSTORE              target datadir (default: ${REPO_ROOT}/.data/elstore)
#   EL_HTTP              eth HTTP for eth_syncing (default: http://127.0.0.1:8545)
#   SNAPSHOT_NETWORK     default: hoodi
#   SNAPSHOT_CLIENT      default: geth
#   SNAPSHOT_BASE_URL    default: https://snapshots.ethpandaops.io
#   ALLOWED_SNAPSHOT_HOSTS  comma-separated host allowlist (default: snapshots.ethpandaops.io)
#   EARLY_WARN_SECONDS   throughput sample window (default: 600 = 10 min)
#   REPLAN_HOURS         if projected download alone exceeds this, warn (default: 4)
#   WGET_TRIES           wget --tries (default: 0 = unlimited retries)
#
# Exit codes:
#   0  success
#   1  restore / tool failure
#   2  usage
#   3  early-warning: projected download alone > REPLAN_HOURS (still exit 0 if
#      --probe-throughput only records; full restore aborts unless --force)
#   4  insufficient free disk for stream-extract (compressed alone would not fit)
#   5  unsafe URL (scheme/host) or unsafe tarball member path
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# GNU tar is required for `tar -I zstd` (macOS bsdtar uses -I for inclusion files).
# Prefer Homebrew gnubin if present so the acceptance / operator form works on darwin.
if [[ -d /opt/homebrew/opt/gnu-tar/libexec/gnubin ]]; then
  PATH="/opt/homebrew/opt/gnu-tar/libexec/gnubin:${PATH}"
elif [[ -d /usr/local/opt/gnu-tar/libexec/gnubin ]]; then
  PATH="/usr/local/opt/gnu-tar/libexec/gnubin:${PATH}"
fi
export PATH

NETWORK="${SNAPSHOT_NETWORK:-hoodi}"
CLIENT="${SNAPSHOT_CLIENT:-geth}"
BASE_URL="${SNAPSHOT_BASE_URL:-https://snapshots.ethpandaops.io}"
ALLOWED_SNAPSHOT_HOSTS="${ALLOWED_SNAPSHOT_HOSTS:-snapshots.ethpandaops.io}"
ELSTORE="${ELSTORE:-${REPO_ROOT}/.data/elstore}"
EL_HTTP="${EL_HTTP:-http://127.0.0.1:8545}"
EARLY_WARN_SECONDS="${EARLY_WARN_SECONDS:-600}"
REPLAN_HOURS="${REPLAN_HOURS:-4}"
WGET_TRIES="${WGET_TRIES:-0}"

DRY_RUN=0
PROBE=0
WAIT_SYNCED=0
FORCE_RESTORE=0
FORCE=0
RECORD_ONLY=""
QUIET_TAR=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --probe-throughput) PROBE=1; shift ;;
    --wait-synced) WAIT_SYNCED=1; shift ;;
    --restore) FORCE_RESTORE=1; shift ;;
    --force) FORCE=1; shift ;;
    --record-only) RECORD_ONLY="$2"; shift 2 ;;
    --elstore) ELSTORE="$2"; shift 2 ;;
    --el-http) EL_HTTP="$2"; shift 2 ;;
    --quiet-tar) QUIET_TAR=1; shift ;;
    -h|--help)
      sed -n '2,50p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

log() { echo "el-restore: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

for tool in curl wget tar zstd python3; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

# Refuse if `tar -I zstd` is not GNU-tar form (bsdtar would treat -I as include-file).
if ! tar --version 2>&1 | head -1 | grep -q 'GNU tar'; then
  die "GNU tar required for 'tar -I zstd' (on macOS: brew install gnu-tar; PATH=.../gnubin:\$PATH)"
fi

utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
unix_now() { date -u +%s; }

# ── URL safety: HTTPS + host allowlist ──────────────────────────────────────
# SNAPSHOT_BASE_URL / snapshot URLs may be overridden; only https:// + allowlist.
assert_safe_https_url() {
  local url="$1"
  local label="${2:-URL}"
  case "${url}" in
    https://*) ;;
    *)
      echo "error: ${label} must be https:// (got: ${url})" >&2
      return 5
      ;;
  esac
  # Strip scheme; take host (before / or :port).
  local rest="${url#https://}"
  local host="${rest%%/*}"
  host="${host%%:*}"
  host="$(printf '%s' "${host}" | tr '[:upper:]' '[:lower:]')"
  [[ -n "${host}" ]] || { echo "error: ${label} has empty host" >&2; return 5; }
  local allowed IFS=','
  # shellcheck disable=SC2086
  for allowed in ${ALLOWED_SNAPSHOT_HOSTS}; do
    allowed="$(printf '%s' "${allowed}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
    [[ -z "${allowed}" ]] && continue
    if [[ "${host}" == "${allowed}" ]]; then
      return 0
    fi
  done
  echo "error: ${label} host '${host}' not in ALLOWED_SNAPSHOT_HOSTS=${ALLOWED_SNAPSHOT_HOSTS}" >&2
  return 5
}

assert_safe_https_url "${BASE_URL}" "SNAPSHOT_BASE_URL" || exit $?
# Path segments joined into the snapshot URL — reject surprises.
[[ "${NETWORK}" =~ ^[A-Za-z0-9_-]+$ ]] || die "SNAPSHOT_NETWORK must be [A-Za-z0-9_-]+ (got: ${NETWORK})"
[[ "${CLIENT}" =~ ^[A-Za-z0-9_-]+$ ]] || die "SNAPSHOT_CLIENT must be [A-Za-z0-9_-]+ (got: ${CLIENT})"

# ── Path safety (fetch-spec-vectors assert_safe_tarball, stream form) ───────
# Reject absolute paths and any `..` component on member names / symlink targets.
assert_safe_member() {
  local member="$1"
  local p="${member%/}"
  [[ -z "${p}" ]] && return 0
  case "${p}" in
    /*|~*)
      echo "error: tarball contains absolute path: ${member}" >&2
      return 5
      ;;
  esac
  case "/${p}/" in
    */../*)
      echo "error: tarball contains path traversal: ${member}" >&2
      return 5
      ;;
  esac
  return 0
}

# Validate every member of an on-disk archive (non-stream helper / tests).
assert_safe_tarball() {
  local archive="$1"
  local member any=0
  while IFS= read -r member; do
    any=1
    assert_safe_member "${member}" || return 5
  done < <(tar -I zstd -tf "${archive}")
  if [[ "${any}" -eq 0 ]]; then
    echo "error: tarball is empty: ${archive}" >&2
    return 5
  fi
  return 0
}

# Stream-extract with path checks on every member before write (no second copy).
# Operator-equivalent form (also printed for dry-run / logs):
#   wget -O - <url> | tar -I zstd -xvf - -C <elstore>
# GNU tar alone does not refuse `../` members under -C; we decompress with zstd
# and extract via tarfile with assert_safe_member on name + linkname.
# Python helper written once per process — stdin is the uncompressed tar stream
# (must not use a heredoc on the same python invocation as the pipe; SC2259).
STREAM_EXTRACT_PY=""
write_stream_extract_py() {
  STREAM_EXTRACT_PY="$(mktemp "${TMPDIR:-/tmp}/cc-el-stream-extract.XXXXXX.py")"
  cat >"${STREAM_EXTRACT_PY}" <<'PY'
"""Path-safe streaming tar extract (assert_safe_member on every member)."""
import sys
import tarfile

def safe(name: str) -> bool:
    """Mirror bash assert_safe_member: no absolute / ~, no .. component."""
    if name is None:
        return True
    p = name.rstrip("/")
    if not p:
        return True
    if p.startswith("/") or p.startswith("~"):
        return False
    parts = [x for x in p.replace("\\", "/").split("/") if x != ""]
    if any(part == ".." for part in parts):
        return False
    return True

def fail(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(5)

def main() -> None:
    if len(sys.argv) < 2:
        fail("usage: stream_extract.py DEST [quiet=0|1]")
    dest = sys.argv[1]
    quiet = len(sys.argv) > 2 and sys.argv[2] == "1"
    # Stream mode (r|): no seeking; single pass; no second copy on disk.
    with tarfile.open(fileobj=sys.stdin.buffer, mode="r|") as tf:
        any_member = False
        for m in tf:
            any_member = True
            if not safe(m.name):
                fail(f"tarball contains unsafe member path: {m.name!r}")
            if m.linkname and (m.issym() or m.islnk()) and not safe(m.linkname):
                fail(f"tarball contains unsafe link target: {m.name!r} -> {m.linkname!r}")
            if not quiet:
                print(m.name, file=sys.stderr)
            # Python 3.12+ data filter: defence in depth with assert_safe_member.
            try:
                tf.extract(m, path=dest, filter="data")
            except TypeError:
                tf.extract(m, path=dest)
        if not any_member:
            fail("tarball is empty")

if __name__ == "__main__":
    main()
PY
}

stream_safe_extract() {
  local dest="$1"
  local url="$2"
  local rc quiet_flag=0
  [[ "${QUIET_TAR}" -eq 1 ]] && quiet_flag=1
  if [[ -z "${STREAM_EXTRACT_PY}" || ! -f "${STREAM_EXTRACT_PY}" ]]; then
    write_stream_extract_py
  fi
  log "stream-extract (path-safe): wget -O - ${url} | tar -I zstd -xvf - -C ${dest}"
  set +e
  set -o pipefail
  # shellcheck disable=SC2086
  wget --tries="${WGET_TRIES}" --retry-connrefused -O - "${url}" \
    | zstd -d \
    | python3 "${STREAM_EXTRACT_PY}" "${dest}" "${quiet_flag}"
  rc=$?
  set +o pipefail
  set -e
  return "${rc}"
}

# After extract: every realpath under dest must stay inside dest (symlinks too).
assert_elstore_confined() {
  local dest="$1"
  local dest_real f rp
  dest_real="$(cd "${dest}" && pwd -P)"
  # Files and dirs.
  while IFS= read -r -d '' f; do
    rp="$(realpath "${f}" 2>/dev/null || true)"
    if [[ -z "${rp}" ]]; then
      die "cannot resolve path under elstore: ${f}"
    fi
    case "${rp}" in
      "${dest_real}"|"${dest_real}"/*) ;;
      *)
        echo "error: extracted path escaped elstore: ${f} -> ${rp}" >&2
        exit 5
        ;;
    esac
  done < <(find "${dest}" -print0 2>/dev/null)
  # Symlink targets must resolve inside dest.
  while IFS= read -r -d '' f; do
    rp="$(realpath "${f}" 2>/dev/null || true)"
    if [[ -z "${rp}" ]]; then
      echo "error: symlink does not resolve inside elstore: ${f}" >&2
      exit 5
    fi
    case "${rp}" in
      "${dest_real}"|"${dest_real}"/*) ;;
      *)
        echo "error: symlink escaped elstore: ${f} -> ${rp}" >&2
        exit 5
        ;;
    esac
  done < <(find "${dest}" -type l -print0 2>/dev/null)
}

# ── Wait-only mode (M1): no extract, elstore may already be populated ───────
# Docs step 3: after restore + geth up, only poll eth_syncing.
# Sets globals SYNC_FALSE_UTC / SYNC_FALSE_OUTPUT for the result table.
SYNC_FALSE_UTC=""
SYNC_FALSE_OUTPUT=""
wait_eth_synced() {
  local curl_rc
  log "waiting for eth_syncing == false on ${EL_HTTP} (no extract; elstore may be non-empty)"
  while true; do
    set +e
    SYNC_FALSE_OUTPUT="$(curl -fsS -X POST "${EL_HTTP}" \
      -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}' 2>&1)"
    curl_rc=$?
    set -e
    if [[ "${curl_rc}" -eq 0 ]] && printf '%s' "${SYNC_FALSE_OUTPUT}" | grep -q '"result":false'; then
      SYNC_FALSE_UTC="$(utc_now)"
      log "eth_syncing == false at ${SYNC_FALSE_UTC}"
      log "output: ${SYNC_FALSE_OUTPUT}"
      cat <<EOF

### eth_syncing gate
| Field | Value |
|---|---|
| UTC | **${SYNC_FALSE_UTC}** |
| EL HTTP | \`${EL_HTTP}\` |
| Command | \`curl -sS -X POST ${EL_HTTP} -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}'\` |
| Output | \`${SYNC_FALSE_OUTPUT}\` |
EOF
      return 0
    fi
    log "still syncing (or EL down): ${SYNC_FALSE_OUTPUT:-curl_rc=$curl_rc}"
    sleep 30
  done
}

# --wait-synced alone (or with --el-http / --elstore only) is wait-only.
# Full restore + wait in one shot: --restore --wait-synced.
if [[ "${WAIT_SYNCED}" -eq 1 && "${FORCE_RESTORE}" -eq 0 \
   && "${DRY_RUN}" -eq 0 && "${PROBE}" -eq 0 && -z "${RECORD_ONLY}" ]]; then
  wait_eth_synced
  exit 0
fi

# ── V-5: re-read live snapshot pointer ──────────────────────────────────────
LATEST_URL="${BASE_URL%/}/${NETWORK}/${CLIENT}/latest"
V5_READ_AT="$(utc_now)"
log "V-5: reading ${LATEST_URL} at ${V5_READ_AT}"

BLOCK_NUMBER="$(curl -fsSL --max-time 30 "${LATEST_URL}" | tr -d '[:space:]')"
[[ -n "${BLOCK_NUMBER}" ]] || die "empty latest pointer from ${LATEST_URL}"
[[ "${BLOCK_NUMBER}" =~ ^[0-9]+$ ]] || die "latest pointer not a block number: ${BLOCK_NUMBER}"

SNAPSHOT_URL="${BASE_URL%/}/${NETWORK}/${CLIENT}/${BLOCK_NUMBER}/snapshot.tar.zst"
assert_safe_https_url "${SNAPSHOT_URL}" "snapshot URL" || exit $?

log "V-5: HEADing ${SNAPSHOT_URL}"

HEADER_FILE="$(mktemp "${TMPDIR:-/tmp}/cc-el-restore-hdrs.XXXXXX")"
# shellcheck disable=SC2064
trap 'rm -f "${HEADER_FILE}" "${STREAM_EXTRACT_PY:-}"' EXIT

curl -fsSI --max-time 60 "${SNAPSHOT_URL}" >"${HEADER_FILE}" || die "HEAD failed for ${SNAPSHOT_URL}"

CONTENT_LENGTH="$(
  awk 'BEGIN{IGNORECASE=1} /^content-length:/{gsub(/\r/,""); print $2; exit}' "${HEADER_FILE}"
)"
LAST_MODIFIED="$(
  awk 'BEGIN{IGNORECASE=1} /^last-modified:/{sub(/\r$/,""); sub(/^[^:]+:[[:space:]]*/,""); print; exit}' "${HEADER_FILE}"
)"
[[ -n "${CONTENT_LENGTH}" ]] || die "no content-length in HEAD response"
[[ "${CONTENT_LENGTH}" =~ ^[0-9]+$ ]] || die "content-length not integer: ${CONTENT_LENGTH}"

# Snapshot age note. Pass Last-Modified via env — never splice into a Python heredoc.
AGE_NOTE="last-modified=${LAST_MODIFIED:-unknown}"
if [[ -n "${LAST_MODIFIED}" ]]; then
  LM_EPOCH="$(
    LAST_MODIFIED="${LAST_MODIFIED}" python3 - <<'PY'
import email.utils, os, sys
s = os.environ.get("LAST_MODIFIED", "")
try:
    t = email.utils.parsedate_to_datetime(s)
    print(int(t.timestamp()))
except (TypeError, ValueError, OverflowError, IndexError):
    sys.exit(1)
PY
  )" || LM_EPOCH=""
  if [[ -n "${LM_EPOCH}" && "${LM_EPOCH}" =~ ^[0-9]+$ ]]; then
    AGE_SECS=$(( $(unix_now) - LM_EPOCH ))
    AGE_HOURS="$(awk -v s="${AGE_SECS}" 'BEGIN{printf "%.1f", s/3600}')"
    AGE_NOTE="age≈${AGE_HOURS}h since last-modified (${LAST_MODIFIED}); catch-up cost grows with age"
  fi
fi

# Free disk at target (parent may not exist yet). Portable: df -k → 1K-blocks.
ELSTORE_PARENT="$(dirname "${ELSTORE}")"
mkdir -p "${ELSTORE_PARENT}"
FREE_K="$(df -k "${ELSTORE_PARENT}" | awk 'NR==2{print $4}')"
FREE_BYTES=$((FREE_K * 1024))

CONTENT_GIB="$(awk -v b="${CONTENT_LENGTH}" 'BEGIN{printf "%.2f", b/1024/1024/1024}')"
FREE_GIB="$(awk -v b="${FREE_BYTES}" 'BEGIN{printf "%.2f", b/1024/1024/1024}')"

log "V-5 block_number=${BLOCK_NUMBER}"
log "V-5 content-length=${CONTENT_LENGTH} bytes (${CONTENT_GIB} GiB)"
log "V-5 last-modified=${LAST_MODIFIED:-unknown}"
log "V-5 ${AGE_NOTE}"
log "disk free at ${ELSTORE_PARENT}: ${FREE_BYTES} bytes (${FREE_GIB} GiB)"
log "integrity: no publisher checksum for snapshot.tar.zst (TLS + host allowlist + path-safe extract)"

# Record payload via env only (no unquoted heredoc splice of headers / paths).
RECORD_JSON="$(
  V5_READ_AT="${V5_READ_AT}" \
  LATEST_URL="${LATEST_URL}" \
  BLOCK_NUMBER="${BLOCK_NUMBER}" \
  SNAPSHOT_URL="${SNAPSHOT_URL}" \
  CONTENT_LENGTH="${CONTENT_LENGTH}" \
  CONTENT_GIB="${CONTENT_GIB}" \
  LAST_MODIFIED="${LAST_MODIFIED:-}" \
  AGE_NOTE="${AGE_NOTE}" \
  ELSTORE="${ELSTORE}" \
  FREE_BYTES="${FREE_BYTES}" \
  FREE_GIB="${FREE_GIB}" \
  python3 - <<'PY'
import json, os
print(json.dumps({
  "v5_read_at_utc": os.environ["V5_READ_AT"],
  "latest_url": os.environ["LATEST_URL"],
  "block_number": int(os.environ["BLOCK_NUMBER"]),
  "snapshot_url": os.environ["SNAPSHOT_URL"],
  "content_length_bytes": int(os.environ["CONTENT_LENGTH"]),
  "content_length_gib": float(os.environ["CONTENT_GIB"]),
  "last_modified": os.environ.get("LAST_MODIFIED", ""),
  "age_note": os.environ.get("AGE_NOTE", ""),
  "elstore": os.environ["ELSTORE"],
  "free_bytes_before": int(os.environ["FREE_BYTES"]),
  "free_gib_before": float(os.environ["FREE_GIB"]),
  "checksum": None,
  "checksum_note": "ethPandaOps does not publish snapshot.tar.zst digests; integrity is TLS + host allowlist + path-safe extract",
}, indent=2))
PY
)"

if [[ -n "${RECORD_ONLY}" ]]; then
  printf '%s\n' "${RECORD_JSON}" >"${RECORD_ONLY}"
  log "wrote V-5 record to ${RECORD_ONLY}"
  exit 0
fi

if [[ "${DRY_RUN}" -eq 1 ]]; then
  cat <<EOF
### V-5 dry-run (no restore)
| Field | Value |
|---|---|
| Read at (UTC) | ${V5_READ_AT} |
| latest | \`${LATEST_URL}\` → **${BLOCK_NUMBER}** |
| snapshot URL | \`${SNAPSHOT_URL}\` |
| content-length | **${CONTENT_LENGTH}** bytes (${CONTENT_GIB} GiB) |
| last-modified | ${LAST_MODIFIED:-unknown} |
| age / catch-up | ${AGE_NOTE} |
| elstore | \`${ELSTORE}\` |
| free disk | ${FREE_BYTES} bytes (${FREE_GIB} GiB) |
| host allowlist | \`${ALLOWED_SNAPSHOT_HOSTS}\` |
| publisher checksum | **none published** (residual — TLS + allowlist + path-safe extract) |

Stream-extract (not executed; path-safe member checks apply on live run):
\`\`\`bash
wget --tries=${WGET_TRIES} --retry-connrefused -O - \\
  ${SNAPSHOT_URL} \\
  | tar -I zstd -xvf - -C ${ELSTORE}
du -sh ${ELSTORE}
\`\`\`
EOF
  if (( FREE_BYTES < CONTENT_LENGTH )); then
    log "WARNING: free disk (${FREE_GIB} GiB) < compressed size (${CONTENT_GIB} GiB)"
    exit 4
  fi
  exit 0
fi

# ── Early-warning: first EARLY_WARN_SECONDS of download throughput (R-3) ────
probe_throughput() {
  local sample_secs="$1"
  log "R-3 early-warning: sampling first ${sample_secs}s of download (bytes discarded, no second copy)"
  local t0 t1 elapsed bytes rate_bps projected_s projected_h
  t0="$(unix_now)"
  set +e
  bytes="$(curl -fsS --max-time "${sample_secs}" -o /dev/null -w '%{size_download}' \
    "${SNAPSHOT_URL}" 2>/dev/null)"
  local curl_rc=$?
  set -e
  t1="$(unix_now)"
  elapsed=$((t1 - t0))
  if [[ "${elapsed}" -lt 1 ]]; then elapsed=1; fi
  bytes="${bytes:-0}"
  if [[ "${curl_rc}" -ne 0 && "${curl_rc}" -ne 28 && "${bytes}" -eq 0 ]]; then
    log "R-3: curl failed (rc=${curl_rc}) with zero bytes"
    echo "0 0 ${elapsed} 0"
    return 1
  fi
  rate_bps="$(awk -v b="${bytes}" -v e="${elapsed}" 'BEGIN{printf "%.0f", b/e}')"
  if [[ "${rate_bps}" -eq 0 ]]; then
    log "R-3: zero throughput over ${elapsed}s — network or service problem"
    echo "0 0 ${elapsed} ${bytes}"
    return 1
  fi
  projected_s="$(awk -v total="${CONTENT_LENGTH}" -v r="${rate_bps}" 'BEGIN{printf "%.0f", total/r}')"
  projected_h="$(awk -v s="${projected_s}" 'BEGIN{printf "%.2f", s/3600}')"
  log "R-3: sampled ${bytes} bytes in ${elapsed}s → ${rate_bps} B/s"
  log "R-3: projected download alone ≈ ${projected_h} h (threshold ${REPLAN_HOURS} h)"
  echo "${rate_bps} ${projected_s} ${elapsed} ${bytes}"
  if awk -v h="${projected_h}" -v lim="${REPLAN_HOURS}" 'BEGIN{exit !(h+0 > lim+0)}'; then
    return 0
  fi
  return 1
}

if [[ "${PROBE}" -eq 1 ]]; then
  set +e
  probe_out="$(probe_throughput "${EARLY_WARN_SECONDS}")"
  probe_rc=$?
  set -e
  read -r RATE_BPS PROJECTED_S ELAPSED SAMPLED_BYTES <<<"${probe_out}"
  PROJECTED_H="$(awk -v s="${PROJECTED_S}" 'BEGIN{printf "%.2f", s/3600}')"
  cat <<EOF
### R-3 early-warning probe
| Field | Value |
|---|---|
| Sample window | ${ELAPSED}s (cap ${EARLY_WARN_SECONDS}s) |
| Bytes sampled | ${SAMPLED_BYTES} |
| Throughput | ${RATE_BPS} B/s |
| Projected download | ${PROJECTED_H} h |
| Re-plan threshold | ${REPLAN_HOURS} h |
| Crossed threshold | $([[ "${probe_rc}" -eq 0 ]] && echo YES || echo no) |
EOF
  if [[ "${probe_rc}" -eq 0 ]]; then
    log "R-3: projected download alone past ${REPLAN_HOURS}h — re-plan milestone start"
    exit 3
  fi
  exit 0
fi

# Refuse if free disk cannot hold even the compressed footprint as a lower bound
# on the *uncompressed* extract (no expansion factor assumed).
if (( FREE_BYTES < CONTENT_LENGTH )); then
  log "free disk ${FREE_GIB} GiB < content-length ${CONTENT_GIB} GiB — abort"
  exit 4
fi

# Optional early-warning before committing the multi-hour restore.
if [[ "${FORCE}" -eq 0 && "${EARLY_WARN_SECONDS}" -gt 0 ]]; then
  PRE_SECS=60
  if [[ "${EARLY_WARN_SECONDS}" -lt 60 ]]; then
    PRE_SECS="${EARLY_WARN_SECONDS}"
  fi
  set +e
  probe_out="$(probe_throughput "${PRE_SECS}")"
  probe_rc=$?
  set -e
  read -r RATE_BPS PROJECTED_S ELAPSED SAMPLED_BYTES <<<"${probe_out}"
  PROJECTED_H="$(awk -v s="${PROJECTED_S}" 'BEGIN{printf "%.2f", s/3600}')"
  if [[ "${probe_rc}" -eq 0 ]]; then
    log "R-3: projected download ${PROJECTED_H}h > ${REPLAN_HOURS}h — abort (use --force to override)"
    exit 3
  fi
fi

# ── Stream-extract: no second copy, path-safe members ───────────────────────
mkdir -p "${ELSTORE}"
# Refuse non-empty datadir (would mix snapshot with partial state).
if [[ -n "$(ls -A "${ELSTORE}" 2>/dev/null || true)" ]]; then
  die "elstore not empty: ${ELSTORE} (refuse to mix; empty it or pick another --elstore)"
fi

RESTORE_START_UTC="$(utc_now)"
RESTORE_T0="$(unix_now)"
log "stream-extract start ${RESTORE_START_UTC}"

PROGRESS_LOG="$(mktemp "${TMPDIR:-/tmp}/cc-el-restore-progress.XXXXXX")"
(
  sleep "${EARLY_WARN_SECONDS}"
  now="$(unix_now)"
  du_bytes="$(du -sk "${ELSTORE}" 2>/dev/null | awk '{print $1 * 1024}')"
  echo "t_plus_${EARLY_WARN_SECONDS}s du_bytes=${du_bytes} wall_elapsed=$((now - RESTORE_T0))" >"${PROGRESS_LOG}"
) &
PROGRESS_PID=$!

set +e
stream_safe_extract "${ELSTORE}" "${SNAPSHOT_URL}"
EXTRACT_RC=$?
set -e

kill "${PROGRESS_PID}" 2>/dev/null || true
wait "${PROGRESS_PID}" 2>/dev/null || true

RESTORE_T1="$(unix_now)"
RESTORE_END_UTC="$(utc_now)"
RESTORE_WALL=$((RESTORE_T1 - RESTORE_T0))
RESTORE_WALL_H="$(awk -v s="${RESTORE_WALL}" 'BEGIN{printf "%.2f", s/3600}')"

if [[ "${EXTRACT_RC}" -ne 0 ]]; then
  echo "error: path-safe stream-extract failed (exit ${EXTRACT_RC}) after ${RESTORE_WALL}s" >&2
  exit "${EXTRACT_RC}"
fi

assert_elstore_confined "${ELSTORE}"

# ── du -sh measured once; never invent an uncompressed size (OQ-P3-5) ───────
DU_OUT="$(du -sh "${ELSTORE}")"
DU_HUMAN="$(echo "${DU_OUT}" | awk '{print $1}')"
DU_BYTES="$(du -sk "${ELSTORE}" | awk '{print $1 * 1024}')"

log "restore complete wall=${RESTORE_WALL}s (${RESTORE_WALL_H}h)"
log "du -sh ${ELSTORE} → ${DU_OUT}"

FIRST_WINDOW_NOTE="(no mid-window sample — restore finished before ${EARLY_WARN_SECONDS}s or sampler missed)"
if [[ -s "${PROGRESS_LOG}" ]]; then
  FIRST_WINDOW_NOTE="$(cat "${PROGRESS_LOG}")"
fi
rm -f "${PROGRESS_LOG}"

# Optional: extract then wait (--restore --wait-synced).
if [[ "${WAIT_SYNCED}" -eq 1 ]]; then
  wait_eth_synced
fi

cat <<EOF

### el-snapshot-restore result
| Field | Value |
|---|---|
| V-5 read at (UTC) | ${V5_READ_AT} |
| Snapshot block | **${BLOCK_NUMBER}** |
| content-length | **${CONTENT_LENGTH}** bytes (${CONTENT_GIB} GiB) |
| last-modified | ${LAST_MODIFIED:-unknown} |
| Age / catch-up | ${AGE_NOTE} |
| Publisher checksum | **none** (ethPandaOps does not publish snapshot digests) |
| elstore | \`${ELSTORE}\` |
| Restore start (UTC) | ${RESTORE_START_UTC} |
| Restore end (UTC) | ${RESTORE_END_UTC} |
| Restore wall clock | **${RESTORE_WALL}** s (${RESTORE_WALL_H} h) |
| \`du -sh\` (measured) | **${DU_HUMAN}** (${DU_BYTES} bytes) |
| First-window note | ${FIRST_WINDOW_NOTE} |
| eth_syncing==false (UTC) | ${SYNC_FALSE_UTC:-NOT_WAITED} |
| eth_syncing command output | ${SYNC_FALSE_OUTPUT:-n/a} |
EOF
