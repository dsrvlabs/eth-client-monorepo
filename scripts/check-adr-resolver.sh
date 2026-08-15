#!/usr/bin/env bash
# S1-B-06 / [ARCH] §10.1 — ADR citation resolver + Architecture § census.
#
# Extracts every ADR[ -]<id> and Architecture §<n> occurrence. Fails when an
# ADR id is in neither the reconciliation.md `id` column nor a regular
# docs/adr/ADR-*.md file. Unwritten table rows (the 43 (a) stubs) resolve;
# this gate does not demand the bodies and does not check bucket / status /
# resolving. Architecture § hits are census-only — they do not resolve
# under docs/adr/.
#
# The extractor is ADR[ -]P?[0-9]+(-[0-9]+)? so it catches the spaced
# spelling (ADR P3-02). Bare never-cited short ids in docs/adr/README.md
# are not citations. ADR-R-* is out of the regex on purpose: plan/ already
# cites unwritten R-01…R-04 / R-07; extracting them would go red before
# those bodies exist. Landed ADR-R-05.md / ADR-R-06.md still count as
# resolvable by filename.
#
# Usage:
#   bash scripts/check-adr-resolver.sh              # fixtures, then live tree
#   bash scripts/check-adr-resolver.sh --self-test  # fixtures only
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 - "$ROOT" "${1:-}" <<'PY'
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(sys.argv[1])
ARG = sys.argv[2] if len(sys.argv) > 2 else ""
FIXTURE_ROOT = ROOT / "scripts" / "fixtures" / "check-adr-resolver"
ADR_DIR = Path("docs") / "adr"
RECON_NAME = "reconciliation.md"

# [ARCH] §10.1 / docs/adr/README.md. Space or hyphen; P-series and base NN.
# R-series omitted: see script header (unwritten R-01… in plan/).
ADR_RE = re.compile(r"(?<![A-Za-z0-9])ADR[ -](P?[0-9]+(?:-[0-9]+)?)")
ARCH_RE = re.compile(r"(?<![A-Za-z])Architecture §([0-9]+(?:\.[0-9]+)*)")
TABLE_HEADER = re.compile(
    r"^\|\s*id\s*\|\s*bucket\s*\|\s*status\s*\|\s*resolving\s*\|?\s*$",
    re.I,
)
TABLE_SEP = re.compile(r"^\|[\s:|-]+\|?\s*$")
# docs/adr/<id>.md or docs/adr/<id>-<kebab-slug>.md ([ARCH] §10.2 / README).
ADR_FILE_RE = re.compile(
    r"^(ADR-(?:R-[0-9]+|P[0-9]+-[0-9]+|[0-9]+))(?:-[a-z0-9]+)*\.md$",
    re.I,
)

SKIP_DIR_NAMES = frozenset(
    {".git", "target", "node_modules", ".venv", "vendor", ".grok"}
)
SKIP_NAME_PREFIXES = ("architecture-study-", "review-develop-")
SKIP_SUFFIXES = frozenset(
    {
        ".ssz",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".bin",
        ".wasm",
        ".o",
        ".a",
        ".so",
        ".dylib",
        ".rlib",
        ".woff",
        ".woff2",
        ".ttf",
        ".pdf",
        ".zip",
        ".gz",
        ".bz2",
        ".xz",
        ".zst",
    }
)


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    kind: str
    raw: str
    canonical: str


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def rel_to(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def skip_rel(rel: str) -> bool:
    parts = Path(rel).parts
    if parts and parts[0] == "scripts" and "fixtures" in parts:
        return True
    name = Path(rel).name
    if name.startswith(SKIP_NAME_PREFIXES):
        return True
    if Path(rel).suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def is_binary(data: bytes) -> bool:
    return b"\0" in data[:8192]


def parse_reconciliation(path: Path) -> set[str]:
    """Allowlist = `id` column of the 4-column GFM table. Not steps 4–6."""
    if not path.is_file():
        return set()
    ids: set[str] = set()
    lines = path.read_text(encoding="utf-8").splitlines()
    i = 0
    n = len(lines)
    while i < n:
        if not TABLE_HEADER.match(lines[i].strip()):
            i += 1
            continue
        i += 1
        if i < n and TABLE_SEP.match(lines[i].strip()):
            i += 1
        while i < n:
            raw = lines[i].strip()
            if not raw.startswith("|"):
                break
            cells = [c.strip() for c in raw.strip("|").split("|")]
            if cells and cells[0]:
                ids.add(cells[0])
            i += 1
    return ids


def parse_adr_filenames(adr_dir: Path) -> set[str]:
    ids: set[str] = set()
    if not adr_dir.is_dir():
        return ids
    for path in adr_dir.iterdir():
        # Name match on a symlink is not a document under docs/adr/.
        if path.is_symlink() or not path.is_file():
            continue
        m = ADR_FILE_RE.match(path.name)
        if m:
            ids.add(m.group(1))
    return ids


def resolvable_ids(root: Path) -> set[str]:
    return parse_reconciliation(root / ADR_DIR / RECON_NAME) | parse_adr_filenames(
        root / ADR_DIR
    )


def extract_text(text: str, rel: str) -> list[Hit]:
    hits: list[Hit] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        for m in ADR_RE.finditer(line):
            raw = m.group(0)
            hits.append(Hit(rel, lineno, "adr", raw, "ADR-" + m.group(1)))
        for m in ARCH_RE.finditer(line):
            hits.append(Hit(rel, lineno, "arch", m.group(0), m.group(1)))
    return hits


def decode_file(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError as e:
        die(f"failed to read {path}: {e}")
    if is_binary(data):
        return None
    return data.decode("utf-8", errors="replace")


def walk_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d
            for d in dirnames
            if d not in SKIP_DIR_NAMES and not d.startswith(SKIP_NAME_PREFIXES)
        ]
        base = Path(dirpath)
        for name in filenames:
            path = base / name
            rel = rel_to(path, root)
            if skip_rel(rel) or path.is_symlink() or not path.is_file():
                continue
            out.append(path)
    out.sort()
    return out


def git_ls_files(root: Path) -> list[Path] | None:
    try:
        raw = subprocess.check_output(
            [
                "git",
                "-C",
                str(root),
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ],
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return None
    files: list[Path] = []
    for chunk in raw.split(b"\0"):
        if not chunk:
            continue
        rel = chunk.decode("utf-8", errors="surrogateescape")
        rel_path = Path(rel)
        if rel_path.is_absolute() or ".." in rel_path.parts:
            continue
        if skip_rel(rel):
            continue
        path = root / rel
        if path.is_symlink() or not path.is_file():
            continue
        files.append(path)
    return files


def iter_scan_files(root: Path, *, prefer_git: bool) -> list[Path]:
    if prefer_git:
        listed = git_ls_files(root)
        # None = git missing; [] = skip_rel ate the tree. Both fall back so
        # an empty git list is not a green 0-citation live scan.
        if listed:
            return listed
    return walk_files(root)


def collect_hits(root: Path, *, prefer_git: bool) -> tuple[list[Hit], int]:
    files = iter_scan_files(root, prefer_git=prefer_git)
    hits: list[Hit] = []
    for path in files:
        text = decode_file(path)
        if text is None:
            continue
        hits.extend(extract_text(text, rel_to(path, root)))
    hits.sort(key=lambda h: (h.path, h.line, h.kind, h.canonical, h.raw))
    return hits, len(files)


def unresolved_adr(hits: list[Hit], resolved: set[str]) -> list[Hit]:
    return [h for h in hits if h.kind == "adr" and h.canonical not in resolved]


def format_hit(hit: Hit) -> str:
    extra = ""
    if hit.kind == "adr" and hit.raw != hit.canonical:
        extra = f" → {hit.canonical}"
    return f"{hit.path}:{hit.line}: {hit.raw}{extra}"


def check_tree(
    root: Path, *, prefer_git: bool
) -> tuple[list[Hit], list[Hit], set[str], int]:
    resolved = resolvable_ids(root)
    hits, n_files = collect_hits(root, prefer_git=prefer_git)
    return hits, unresolved_adr(hits, resolved), resolved, n_files


def report_unresolved(bad: list[Hit]) -> None:
    print(
        "error: ADR id does not resolve under docs/adr/ (S1-B-06):",
        file=sys.stderr,
    )
    for hit in bad:
        print(f"  {format_hit(hit)}", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: add a row to docs/adr/reconciliation.md or a docs/adr/ADR-*.md "
        "file. Unwritten table rows are valid — do not invent a file. "
        "Never-cited short ids in docs/adr/README.md are not citations "
        "unless written in ADR[ -]<id> form.",
        file=sys.stderr,
    )


def self_test() -> int:
    failed = 0
    fail_dir = FIXTURE_ROOT / "expect-fail"
    pass_dir = FIXTURE_ROOT / "expect-pass"
    required_fail = ("nonexistent-id", "nonexistent-spaced")
    required_pass = ("table-unwritten", "adr-file", "uncited-short")

    if not fail_dir.is_dir() or not pass_dir.is_dir():
        die(f"missing fixture dirs under {rel_to(FIXTURE_ROOT, ROOT)}")

    for name in required_fail:
        if not (fail_dir / name / "cite.md").is_file():
            print(
                f"error: self-test: missing negative fixture {rel_to(fail_dir / name, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
    for name in required_pass:
        if not (pass_dir / name / "cite.md").is_file():
            print(
                f"error: self-test: missing positive fixture {rel_to(pass_dir / name, ROOT)}",
                file=sys.stderr,
            )
            failed = 1

    def expect_fail(name: str) -> None:
        nonlocal failed
        tree = fail_dir / name
        if not (tree / "cite.md").is_file():
            return
        hits, bad, _resolved, _n = check_tree(tree, prefer_git=False)
        if not any(h.kind == "adr" for h in hits):
            print(
                f"error: self-test: extractor found no ADR citation in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
            return
        if not bad:
            print(
                f"error: self-test: expected unresolved ADR id in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
            return
        print(f"ok: self-test {name} is red")

    def expect_pass(name: str, *, want_adr: bool, want_spaced: bool, want_arch: bool) -> None:
        nonlocal failed
        tree = pass_dir / name
        if not (tree / "cite.md").is_file():
            return
        hits, bad, _resolved, _n = check_tree(tree, prefer_git=False)
        if bad:
            print(
                f"error: self-test: expected pass in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            for hit in bad:
                print(f"  {format_hit(hit)}", file=sys.stderr)
            failed = 1
            return
        adr_hits = [h for h in hits if h.kind == "adr"]
        if want_adr and not adr_hits:
            print(
                f"error: self-test: expected an ADR citation in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
            return
        if not want_adr and adr_hits:
            print(
                f"error: self-test: short id treated as a citation in {rel_to(tree, ROOT)}:",
                file=sys.stderr,
            )
            for hit in adr_hits:
                print(f"  {format_hit(hit)}", file=sys.stderr)
            failed = 1
            return
        if want_spaced and not any(h.raw.startswith("ADR ") for h in adr_hits):
            print(
                f"error: self-test: spaced ADR spelling not extracted in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
            return
        if want_arch and not any(h.kind == "arch" for h in hits):
            print(
                f"error: self-test: Architecture § not extracted in {rel_to(tree, ROOT)}",
                file=sys.stderr,
            )
            failed = 1
            return
        print(f"ok: self-test {name} is green")

    expect_fail("nonexistent-id")
    expect_fail("nonexistent-spaced")
    expect_pass("table-unwritten", want_adr=True, want_spaced=True, want_arch=True)
    expect_pass("adr-file", want_adr=True, want_spaced=False, want_arch=False)
    expect_pass("uncited-short", want_adr=False, want_spaced=False, want_arch=False)

    # Lock the OR rule: table-unwritten must have no ADR-*.md; adr-file no table.
    table_tree = pass_dir / "table-unwritten"
    if (table_tree / "cite.md").is_file():
        if parse_adr_filenames(table_tree / ADR_DIR):
            print(
                "error: self-test: table-unwritten must resolve with no ADR-*.md file",
                file=sys.stderr,
            )
            failed = 1
        if not parse_reconciliation(table_tree / ADR_DIR / RECON_NAME):
            print(
                "error: self-test: table-unwritten is missing a reconciliation row",
                file=sys.stderr,
            )
            failed = 1
    file_tree = pass_dir / "adr-file"
    if (file_tree / "cite.md").is_file():
        if parse_reconciliation(file_tree / ADR_DIR / RECON_NAME):
            print(
                "error: self-test: adr-file must resolve with no reconciliation row",
                file=sys.stderr,
            )
            failed = 1
        if not parse_adr_filenames(file_tree / ADR_DIR):
            print(
                "error: self-test: adr-file is missing docs/adr/ADR-*.md",
                file=sys.stderr,
            )
            failed = 1

    n_fail = sum(
        1 for p in fail_dir.iterdir() if p.is_dir() and (p / "cite.md").is_file()
    )
    n_pass = sum(
        1 for p in pass_dir.iterdir() if p.is_dir() and (p / "cite.md").is_file()
    )
    if n_fail < 2:
        print(
            f"error: self-test: need >=2 negative fixtures in {rel_to(fail_dir, ROOT)} (found {n_fail})",
            file=sys.stderr,
        )
        failed = 1
    if n_pass < 3:
        print(
            f"error: self-test: need >=3 positive fixtures in {rel_to(pass_dir, ROOT)} (found {n_pass})",
            file=sys.stderr,
        )
        failed = 1

    if failed:
        die("ADR resolver fixture self-test failed")
    print("ok: ADR resolver fixtures")
    return 0


def compare_live() -> int:
    hits, bad, resolved, n_files = check_tree(ROOT, prefer_git=True)
    adr_hits = [h for h in hits if h.kind == "adr"]
    if n_files == 0 or not adr_hits:
        die(
            "live scan extracted 0 ADR citations "
            f"({n_files} files; empty walker or dead extractor; S1-B-06)"
        )
    if bad:
        report_unresolved(bad)
        return 1
    arch_hits = [h for h in hits if h.kind == "arch"]
    ids = {h.canonical for h in adr_hits}
    print(
        f"ok: ADR resolver — {len(adr_hits)} ADR citations ({len(ids)} ids) "
        f"ok via table id or ADR-*.md ({len(resolved)} resolvable); "
        f"{len(arch_hits)} Architecture § extracted (census only)"
    )
    return 0


def main() -> int:
    if ARG in ("-h", "--help"):
        print(
            "Usage: bash scripts/check-adr-resolver.sh [--self-test]\n"
            "Extract ADR[ -]<id> and Architecture §<n>. Fail unknown ADR ids "
            "(reconciliation.md id column or docs/adr/ADR-*.md). "
            "Architecture § is census-only."
        )
        return 0
    if ARG not in ("", "--self-test"):
        die(f"unknown argument: {ARG} (want --self-test)")

    rc = self_test()
    if ARG == "--self-test":
        return rc
    live = compare_live()
    return live if live else rc


if __name__ == "__main__":
    raise SystemExit(main())
PY
