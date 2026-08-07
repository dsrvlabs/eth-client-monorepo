#!/usr/bin/env bash
# scripts/phase2-soak-entry-checks.sh — CC-29c entry checks (V-2, V-4)
#
# Operator-facing pre-soak helper. Does **not** start the 24 h window and does
# **not** invent clause numbers. It re-fetches Hoodi network metadata and
# prints what to record in docs/phase-2-soak.md § Run record before the
# 60-minute hold, 2 h rehearsal, or 24 h soak.
#
# Checks:
#   V-2  — re-read eth-clients/hoodi metadata/config.yaml:
#            * any scheduled fork or BPO boundary in the next 48 h?
#            * is GLOAS_FORK_EPOCH still absent (A-P2-9)?
#   V-4  — re-fetch bootstrap_nodes.yaml, print retrieval date + ENR count,
#          optionally diff against config/p2p.toml boot_nodes.
#
# Also prints the command recipe for:
#   * 60-minute entry hold (≥ 25 peers, ≥ 8 custody-compatible)
#   * 2 h rehearsal jobs (peers hold, deferred non-pathological, panics flat)
#   * R-9 corpus refresh from rehearsal capture
#   * soak-report.sh --phase2 after a real window
#
# Usage:
#   bash scripts/phase2-soak-entry-checks.sh
#   bash scripts/phase2-soak-entry-checks.sh --offline   # fixture-only, no network
#   bash scripts/phase2-soak-entry-checks.sh --write-notes PATH
#
# Exit 0 always when checks complete (warnings are not failures). Exit 2 on
# usage / missing tools. Exit 1 if required local fixtures are absent.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OFFLINE=0
WRITE_NOTES=""
HOODI_CONFIG_URL="${HOODI_CONFIG_URL:-https://raw.githubusercontent.com/eth-clients/hoodi/main/metadata/config.yaml}"
HOODI_BOOT_URL="${HOODI_BOOT_URL:-https://raw.githubusercontent.com/eth-clients/hoodi/main/metadata/bootstrap_nodes.yaml}"
LOCAL_CONFIG="${REPO_ROOT}/crates/types/tests/fixtures/hoodi-config.yaml"
LOCAL_P2P_TOML="${REPO_ROOT}/config/p2p.toml"
LOOKAHEAD_HOURS="${SOAK_V2_LOOKAHEAD_HOURS:-48}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --offline) OFFLINE=1; shift ;;
    --write-notes) WRITE_NOTES="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

log() { echo "phase2-entry: $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

command -v python3 >/dev/null 2>&1 || die "required tool not found: python3"
[[ -f "$LOCAL_CONFIG" ]] || die "local Hoodi fixture missing: $LOCAL_CONFIG"
[[ -f "$LOCAL_P2P_TOML" ]] || die "local p2p config missing: $LOCAL_P2P_TOML"

TMPDIR_RUN="$(mktemp -d "${TMPDIR:-/tmp}/cc-phase2-entry.XXXXXX")"
# shellcheck disable=SC2329 # invoked via trap EXIT
cleanup() { rm -rf "$TMPDIR_RUN"; }
trap cleanup EXIT

fetch() {
  local url="$1" dest="$2"
  if [[ "$OFFLINE" -eq 1 ]]; then
    return 1
  fi
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --max-time 30 "$url" -o "$dest" && return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -q -O "$dest" "$url" && return 0
  fi
  return 1
}

# ── V-2: fork / BPO boundary within lookahead ──────────────────────────────
run_v2() {
  local config_path="$1"
  local source_label="$2"
  python3 - "$config_path" "$source_label" "$LOOKAHEAD_HOURS" <<'PY'
import datetime, re, sys
from pathlib import Path

path = Path(sys.argv[1])
label = sys.argv[2]
lookahead_h = int(sys.argv[3])
text = path.read_text(errors="replace")

def yaml_scalar(key):
    # Simple KEY: value (no nested structures).
    m = re.search(r"^" + re.escape(key) + r":\s*(\S+)\s*$", text, re.M)
    return m.group(1) if m else None

def int_or_none(s):
    if s is None:
        return None
    try:
        return int(s, 0)
    except ValueError:
        return None

genesis_time = int_or_none(yaml_scalar("MIN_GENESIS_TIME")) or 0
genesis_delay = int_or_none(yaml_scalar("GENESIS_DELAY")) or 0
seconds_per_slot = int_or_none(yaml_scalar("SECONDS_PER_SLOT")) or 12
slots_per_epoch = 32  # mainnet/hoodi preset; not always in config excerpt
gvr_time = genesis_time + genesis_delay

now = datetime.datetime.now(datetime.timezone.utc)
now_ts = int(now.timestamp())
if gvr_time > 0 and now_ts >= gvr_time:
    slot = (now_ts - gvr_time) // seconds_per_slot
    epoch = slot // slots_per_epoch
else:
    slot = 0
    epoch = 0

horizon_s = lookahead_h * 3600
horizon_epochs = horizon_s // (seconds_per_slot * slots_per_epoch)
epoch_end = epoch + max(horizon_epochs, 1)

# Named fork epochs (regular).
fork_keys = [
    "ALTAIR_FORK_EPOCH",
    "BELLATRIX_FORK_EPOCH",
    "CAPELLA_FORK_EPOCH",
    "DENEB_FORK_EPOCH",
    "ELECTRA_FORK_EPOCH",
    "FULU_FORK_EPOCH",
    "GLOAS_FORK_EPOCH",
]
forks = []
for k in fork_keys:
    v = yaml_scalar(k)
    if v is None:
        forks.append((k, None, "absent"))
    else:
        e = int_or_none(v)
        forks.append((k, e, "present"))

# BLOB_SCHEDULE epochs (BPO boundaries).
bpo = []
for m in re.finditer(r"^\s*-\s*EPOCH:\s*(\d+)\s*$", text, re.M):
    bpo.append(int(m.group(1)))
# Also accept EPOCH: under BLOB_SCHEDULE without list dash (unlikely).
for m in re.finditer(r"^BLOB_SCHEDULE:.*?^([A-Z_]+:)", text, re.M | re.S):
    pass

print(f"### V-2 fork / BPO check")
print(f"")
print(f"| Field | Value |")
print(f"|---|---|")
print(f"| Config source | `{label}` |")
print(f"| Check time (UTC) | {now.strftime('%Y-%m-%dT%H:%M:%SZ')} |")
print(f"| Lookahead | {lookahead_h} h |")
print(f"| Genesis time used | MIN_GENESIS_TIME+GENESIS_DELAY = {gvr_time} |")
print(f"| Approx current slot / epoch | {slot} / {epoch} |")
print(f"| Epoch horizon (exclusive) | {epoch_end} |")
print(f"")
print(f"| Fork key | Epoch | In next {lookahead_h} h? | Notes |")
print(f"|---|---:|---|---|")

any_boundary = False
gloas_absent = True
for k, e, status in forks:
    if k == "GLOAS_FORK_EPOCH":
        if status == "present":
            gloas_absent = False
        print(f"| `{k}` | {e if e is not None else '—'} | n/a | **{'ABSENT (A-P2-9 ok)' if status == 'absent' else 'PRESENT — record and re-evaluate A-P2-9'}** |")
        continue
    if e is None:
        print(f"| `{k}` | — | — | not in config |")
        continue
    if e < epoch:
        in_win = "no (past)"
    elif e >= epoch_end:
        in_win = "no (beyond horizon)"
    else:
        in_win = "**YES**"
        any_boundary = True
    print(f"| `{k}` | {e} | {in_win} | |")

print(f"")
print(f"| BPO (BLOB_SCHEDULE EPOCH) | In next {lookahead_h} h? |")
print(f"|---:|---|")
if not bpo:
    print(f"| *(none found)* | — |")
else:
    for e in bpo:
        if e < epoch:
            in_win = "no (past)"
        elif e >= epoch_end:
            in_win = "no (beyond horizon)"
        else:
            in_win = "**YES**"
            any_boundary = True
        print(f"| {e} | {in_win} |")

print(f"")
print(f"**GLOAS_FORK_EPOCH absent:** {'yes' if gloas_absent else 'NO — present in config'}")
print(f"**Any regular or BPO boundary in window:** {'**YES — land CC-2A first or move the window; record which**' if any_boundary else 'no'}")
print(f"")
if any_boundary:
    sys.exit(10)  # signal to bash for log emphasis; not a hard fail of the helper
sys.exit(0)
PY
}

# ── V-4: bootnode re-fetch ─────────────────────────────────────────────────
run_v4() {
  local boot_path="$1"
  local source_label="$2"
  local retrieval_date
  retrieval_date="$(date -u +%Y-%m-%d)"

  python3 - "$boot_path" "$source_label" "$retrieval_date" "$LOCAL_P2P_TOML" <<'PY'
import re, sys
from pathlib import Path

boot_path = Path(sys.argv[1])
label = sys.argv[2]
retrieval = sys.argv[3]
p2p_toml = Path(sys.argv[4])

text = boot_path.read_text(errors="replace")
# ENR lines: enr:-... or quoted in yaml lists
enrs = re.findall(r"(enr:-[A-Za-z0-9._\-]+)", text)
# de-dupe preserve order
seen = set()
uniq = []
for e in enrs:
    if e not in seen:
        seen.add(e)
        uniq.append(e)

toml = p2p_toml.read_text(errors="replace") if p2p_toml.is_file() else ""
toml_enrs = re.findall(r'"(enr:-[^"]+)"', toml)
toml_set = set(toml_enrs)
fetched_set = set(uniq)
only_remote = sorted(fetched_set - toml_set)
only_local = sorted(toml_set - fetched_set)

# Retrieval date already in p2p.toml comment?
m = re.search(r"Retrieval date:\s*(\d{4}-\d{2}-\d{2})", toml)
local_date = m.group(1) if m else "(not recorded in config/p2p.toml)"

print("### V-4 bootnode re-fetch")
print("")
print("| Field | Value |")
print("|---|---|")
print(f"| Source | `{label}` |")
print(f"| Retrieval date (UTC day) | **{retrieval}** |")
print(f"| ENR count (fetched) | {len(uniq)} |")
print(f"| ENR count (config/p2p.toml) | {len(toml_enrs)} |")
print(f"| Prior retrieval date in p2p.toml | {local_date} |")
print(f"| In remote not in p2p.toml | {len(only_remote)} |")
print(f"| In p2p.toml not in remote | {len(only_local)} |")
print("")
if only_remote or only_local:
    print("**Drift detected** — update `config/p2p.toml` `[discovery].boot_nodes` and the")
    print("`Retrieval date:` comment before the soak if the list is trusted.")
    if only_remote[:3]:
        print("")
        print("Sample remote-only (first 3):")
        for e in only_remote[:3]:
            print(f"- `{e[:72]}…`" if len(e) > 72 else f"- `{e}`")
else:
    print("**No ENR set drift** vs `config/p2p.toml` (order ignored).")
print("")
print("Record field 6 in the run record as: bootnode source URL + retrieval date")
print(f"`{retrieval}`, ENR count {len(uniq)}.")
PY
}

# ── main ───────────────────────────────────────────────────────────────────
REPORT_PARTS=()
log "CC-29c Phase 2 soak entry checks (offline=$OFFLINE)"

# V-2
V2_PATH="$LOCAL_CONFIG"
V2_LABEL="local fixture $LOCAL_CONFIG"
if FETCHED_CFG="$TMPDIR_RUN/hoodi-config.yaml"; fetch "$HOODI_CONFIG_URL" "$FETCHED_CFG"; then
  V2_PATH="$FETCHED_CFG"
  V2_LABEL="$HOODI_CONFIG_URL"
  log "V-2: fetched live Hoodi config"
else
  if [[ "$OFFLINE" -eq 1 ]]; then
    log "V-2: --offline — using local fixture only"
  else
    log "V-2: fetch failed — falling back to local fixture"
  fi
fi

V2_OUT="$TMPDIR_RUN/v2.md"
set +e
run_v2 "$V2_PATH" "$V2_LABEL" >"$V2_OUT"
V2_RC=$?
set -e
cat "$V2_OUT"
if [[ "$V2_RC" -eq 10 ]]; then
  log "WARNING: a fork or BPO boundary falls inside the ${LOOKAHEAD_HOURS}h window"
elif [[ "$V2_RC" -ne 0 ]]; then
  die "V-2 evaluator failed (exit $V2_RC)"
fi
REPORT_PARTS+=("$V2_OUT")

# V-4
V4_PATH=""
V4_LABEL=""
if FETCHED_BOOT="$TMPDIR_RUN/bootstrap_nodes.yaml"; fetch "$HOODI_BOOT_URL" "$FETCHED_BOOT"; then
  V4_PATH="$FETCHED_BOOT"
  V4_LABEL="$HOODI_BOOT_URL"
  log "V-4: fetched live bootnode list"
else
  if [[ "$OFFLINE" -eq 1 ]]; then
    log "V-4: --offline — summarizing config/p2p.toml only (no live list)"
  else
    log "V-4: fetch failed — summarizing config/p2p.toml only"
  fi
  # Synthesize a yaml-ish list from p2p.toml so the printer still works.
  V4_PATH="$TMPDIR_RUN/boot_from_toml.yaml"
  python3 - "$LOCAL_P2P_TOML" "$V4_PATH" <<'PY'
import re, sys
from pathlib import Path
toml = Path(sys.argv[1]).read_text(errors="replace")
out = Path(sys.argv[2])
enrs = re.findall(r'"(enr:-[^"]+)"', toml)
out.write_text("\n".join(f"- {e}" for e in enrs) + "\n")
PY
  V4_LABEL="config/p2p.toml (offline / fetch-failed stand-in)"
fi

V4_OUT="$TMPDIR_RUN/v4.md"
run_v4 "$V4_PATH" "$V4_LABEL" >"$V4_OUT"
cat "$V4_OUT"
REPORT_PARTS+=("$V4_OUT")

# ── operator recipe (always printed) ───────────────────────────────────────
cat <<'EOF'

### Operator recipe (after entry checks pass)

Machine must be clean: no self-devnet, no second `cc-p2p`, no builds, sleep and
automatic updates **disabled** (D-6, R-8).

```bash
# 0) Rig self-test (no live stack) — always safe:
bash scripts/soak-report.sh --self-test

# 1) Pin binary + start Phase 2 six-service Hoodi stack (once; restart voids):
export CC_GIT_SHA="$(git rev-parse --short HEAD)"
# record: git SHA, libp2p rev (docs/p2p-dependencies.md / cc_libp2p::LIBP2P_GIT_REV)
docker compose up -d   # or your soak-machine equivalent
bash scripts/wait-healthy.sh

# 2) 60-minute entry hold (M2.1 re-run on today's peer set). Start sampler:
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-entry-hold.csv \
  --p2p-metrics-url http://127.0.0.1:9102/metrics \
  --duration        3600

# Entry gate: after hold, min_over_time peers ≥ 25 AND custody ≥ 8.
# If either fails, do not open the soak window (D-8, R-2).

# 3) 2 h rehearsal (three jobs) — still not the 24 h proof:
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-rehearsal.csv \
  --p2p-metrics-url http://127.0.0.1:9102/metrics \
  --duration        7200

# During/after rehearsal, confirm:
#   a) cc_p2p_peers holds ≥ 25 for two hours (series, not a single scrape)
#   b) cc_p2p_da_outcome_total{result="deferred"} is non-pathological (A-P2-10)
#   c) cc_p2p_worker_panics_total is flat at zero (any increment → P0, stop)
curl -sS http://127.0.0.1:9102/metrics | grep -E 'cc_p2p_(peers|da_outcome|worker_panics)'

# 4) R-9 — regenerate mutated-valid corpus from rehearsal Hoodi capture (CC-22e):
# ./scripts/corpus-from-capture.sh "$REHEARSAL_CAPTURE_DIR"
# cargo nextest run -p cc-p2p --test hostile_input   # must be green before 24 h

# 5) Open 24 h window only after peer-set-stable (sampler writes peer_set_stable_unix):
# Record field 2 = peer_set_stable timestamp (unrecoverable later).
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-start.txt
bash scripts/soak-sampler.sh \
  --driver-provider "$DRIVER_PROVIDER" \
  --ref-provider    "$REF_PROVIDER" \
  --out             soak-samples.csv \
  --p2p-metrics-url http://127.0.0.1:9102/metrics
# … ≥ 24 h after peer-set-stable …
curl -sS http://127.0.0.1:9102/metrics > p2p-metrics-end.txt

# 6) Emit clause table (clauses 1–3 + R-5 from Hoodi scrapes; 4/5/6/CC-2A via harness-json):
bash scripts/soak-report.sh --phase2 \
  --samples            soak-samples.csv \
  --run-meta           soak-samples.meta \
  --p2p-metrics-start  p2p-metrics-start.txt \
  --p2p-metrics-end    p2p-metrics-end.txt \
  --out                clause-table.md
# optional: --harness-json harness-results.json --write --docs docs/phase-2-soak.md
```

What voids a run (do not continue; restart from zero after a fix):
  p2p swarm-task panic; chain panic/OOM/exit; discovery 5 restarts / 5 min;
  any code change/rebuild/redeploy; machine sleep/reboot/thermal/OS update;
  any self-devnet or second cc-p2p on this machine; fork/BPO boundary in window.

Not voids (record with timestamp): p2p↔chain stream reconnect; worker_panics
increment (report as P0 *with* the run); non-empty swarm_stall_seconds;
persistent deferred rate (keep running; read R-5 breakdown).

EOF

if [[ -n "$WRITE_NOTES" ]]; then
  {
    echo "# Phase 2 soak entry-check notes"
    echo
    echo "Generated by \`scripts/phase2-soak-entry-checks.sh\`."
    echo
    cat "$V2_OUT"
    echo
    cat "$V4_OUT"
  } >"$WRITE_NOTES"
  log "wrote notes to $WRITE_NOTES"
fi

log "entry checks finished (record V-2 / V-4 into docs/phase-2-soak.md § Run record)"
exit 0
