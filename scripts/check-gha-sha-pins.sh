#!/usr/bin/env bash
# S0a-B-06 / P1-C/1 — GitHub Actions must be pinned to commit SHAs.
# Mutable refs (`@v4`, `@main`, short SHAs) can be retargeted; 40-hex cannot.
# Line-grep of `@v[0-9]` misses folded `uses: >-` and non-v tags; parse `uses` values.
#
# Exit non-zero and print every hit on failure. Standalone so it can later be
# wired into `make ci` (S0a-B-04); also run from the clippy job.
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
WF = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"

if not WF.is_dir():
    print("error: missing .github/workflows", file=sys.stderr)
    sys.exit(1)

# `uses` as a mapping key (list item or bare), optional quotes / space before `:`.
KEY_RE = re.compile(
    r"""^(?P<pad>[ \t]*)(?:-\s+)?(?P<key>uses|["']uses["'])\s*:\s*(?P<rest>.*)$"""
)
FLOW_RE = re.compile(
    r"""[{,]\s*(?:uses|["']uses["'])\s*:\s*(?P<val>[^,\n}]+)"""
)
BLOCK_RE = re.compile(r"^([|>])([+-]?)(?:\d+)?\s*(?:#.*)?$")
SHA40 = re.compile(r"^[0-9a-fA-F]{40}$")
SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def is_comment(line: str) -> bool:
    return line.lstrip(" \t").startswith("#")


def leading_ws(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def parse_quoted(s: str) -> str:
    q = s[0]
    out = []
    i = 1
    while i < len(s):
        c = s[i]
        if c == "\\" and q == '"' and i + 1 < len(s):
            out.append(s[i + 1])
            i += 2
            continue
        if c == q:
            return "".join(out)
        out.append(c)
        i += 1
    return "".join(out)


def unquote(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        return s[1:-1]
    return s


def plain_scalar(rest: str) -> str:
    s = rest.strip()
    if not s:
        return ""
    if s[0] in "'\"":
        return parse_quoted(s)
    m = re.search(r"\s+#", s)
    if m:
        s = s[: m.start()]
    return s.strip()


def read_block(lines: list[str], start: int, key_col: int) -> tuple[str, int]:
    i = start
    chunk: list[str] = []
    while i < len(lines):
        ln = lines[i]
        if not ln.strip():
            i += 1
            continue
        if leading_ws(ln) > key_col:
            chunk.append(ln)
            i += 1
            continue
        break
    parts = []
    for raw in chunk:
        st = raw.strip()
        if not st or st.startswith("#"):
            continue
        parts.append(st)
    return unquote("".join(parts)), i


def extract_uses(path: Path) -> list[tuple[int, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    found: list[tuple[int, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip("\r")
        if is_comment(line):
            i += 1
            continue
        m = KEY_RE.match(line)
        if m:
            lineno = i + 1
            rest = m.group("rest")
            key_col = m.start("key")
            if BLOCK_RE.match(rest.strip()):
                value, i = read_block(lines, i + 1, key_col)
            else:
                value = plain_scalar(rest)
                i += 1
            found.append((lineno, value))
            continue
        for fm in FLOW_RE.finditer(line):
            found.append((i + 1, unquote(plain_scalar(fm.group("val")))))
        i += 1
    return found


def check_uses(value: str) -> str | None:
    value = value.strip()
    if not value:
        return "empty uses"
    if value.startswith("./"):
        return None
    if value.startswith("docker://"):
        _, sep, digest = value.partition("@sha256:")
        if sep != "@sha256:" or not SHA256.fullmatch(digest):
            return f"docker:// uses must be digest-pinned (@sha256:<64-hex>): {value}"
        return None
    if "@" not in value:
        return f"remote uses must pin a 40-hex SHA: {value}"
    ref = value.rsplit("@", 1)[1]
    if not SHA40.fullmatch(ref):
        return f"remote uses must pin a 40-hex SHA, got {ref!r}: {value}"
    return None


def yaml_files() -> list[Path]:
    out: list[Path] = []
    for base in (WF, ACTIONS):
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            if p.is_file() and p.suffix in {".yml", ".yaml"}:
                out.append(p)
    return out


hits: list[str] = []
n = 0
for path in yaml_files():
    for lineno, value in extract_uses(path):
        n += 1
        err = check_uses(value)
        if err:
            hits.append(f"{rel(path)}:{lineno}: {err}")

if hits:
    print(
        "error: GitHub Actions must be pinned to commit SHAs (S0a-B-06):",
        file=sys.stderr,
    )
    print("\n".join(hits), file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: pin remote actions to uses: <action>@<40-hex> # vN.M.P "
        "(local ./ actions allowed)",
        file=sys.stderr,
    )
    sys.exit(1)

print(f"ok: {n} uses: pin(s) are 40-hex SHAs (or local ./) under .github/")
PY
