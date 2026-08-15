#!/usr/bin/env bash
# S0-A-12 / P1-B/8 — exactly one fork-schedule walk outside cc-types.
#
# A walk is a *.rs file that either:
#   * compares an epoch against 3+ distinct `*_fork_epoch` fields, or
#   * lists 3+ `*_fork_epoch` field reads inside one array
#
# The authority table lives in crates/types. The single remaining consumer
# walk is services/p2p/src/fork_digest.rs (regular-fork epochs mixed with
# the blob schedule for the next digest boundary). P1-B/6 request_limits
# is out of scope.
#
# Exit non-zero and print every extra/missing hit. Wired into `make lint`
# (S0a-B-04 gate list) and the clippy job.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 - "$ROOT" <<'PY'
import re
import sys
from pathlib import Path

ROOT = Path(sys.argv[1])
ALLOWED = Path("services/p2p/src/fork_digest.rs")
TYPES = Path("crates/types")
SCAN_ROOTS = ("crates", "services", "bin")
FORKS = ("altair", "bellatrix", "capella", "deneb", "electra", "fulu")
FIELD = re.compile(r"\.(" + "|".join(FORKS) + r")_fork_epoch\b")
CMP = re.compile(
    r"(?:>=|<=|>|<)\s*[^\n]*\b(?:" + "|".join(FORKS) + r")_fork_epoch\b"
    r"|"
    r"\b(?:" + "|".join(FORKS) + r")_fork_epoch\b[^\n]*(?:>=|<=|>|<)"
)
ARRAY = re.compile(
    r"\[(?:[^\[\]]*\.(" + "|".join(FORKS) + r")_fork_epoch\b[^\[\]]*){3,}\]",
    re.S,
)
COMMENT = re.compile(r"//.*?$|/\*.*?\*/", re.S | re.M)


def rel(path: Path) -> Path:
    return path.relative_to(ROOT)


def is_walk(text: str) -> bool:
    stripped = COMMENT.sub("", text)
    names = set(FIELD.findall(stripped))
    if len(names) < 3:
        return False
    return bool(CMP.search(stripped) or ARRAY.search(stripped))


hits: list[Path] = []
for base in SCAN_ROOTS:
    root = ROOT / base
    if not root.is_dir():
        continue
    for path in sorted(root.rglob("*.rs")):
        rel_path = rel(path)
        if TYPES in rel_path.parents or rel_path == TYPES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as e:
            print(f"error: failed to read {rel_path}: {e}", file=sys.stderr)
            sys.exit(1)
        if is_walk(text):
            hits.append(rel_path)

if hits != [ALLOWED]:
    print(
        "error: expected exactly one fork-schedule walk outside cc-types "
        f"({ALLOWED}); S0-A-12 / P1-B/8:",
        file=sys.stderr,
    )
    if hits:
        for h in hits:
            print(f"  {h}", file=sys.stderr)
    else:
        print("  (none found)", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: call ChainConfig::fork_version_at_epoch / fork_name_at_epoch; "
        "do not re-walk *_fork_epoch. The remaining walk is "
        "fork_digest.rs regular_fork_epochs (digest boundary + BPO).",
        file=sys.stderr,
    )
    sys.exit(1)

print(f"ok: one fork-schedule walk outside cc-types ({ALLOWED})")
PY
