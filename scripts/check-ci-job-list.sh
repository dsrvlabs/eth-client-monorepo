#!/usr/bin/env bash
# S0a-B-04 / P0-08(d) — `make ci` job list must match ci.yml job ids.
#
# E0.1: assert by diffing the two lists, not by inspection. A job added to
# one side only is the class of drift this gate exists to close.
#
# Usage:
#   bash scripts/check-ci-job-list.sh              # fixtures, live set-diff, then in-memory fmt drop
#   bash scripts/check-ci-job-list.sh --self-test  # fixtures + in-memory fmt drop (no live set-diff)
#
# Makefile source of truth is the `CI_JOBS :=` assignment (not `ci:` prereqs:
# those omit vectors/compose, which stay opt-in for size / docker).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 - "$ROOT" "${1:-}" <<'PY'
import re
import sys
from pathlib import Path

ROOT = Path(sys.argv[1])
ARG = sys.argv[2] if len(sys.argv) > 2 else ""
FIXTURE_ROOT = ROOT / "scripts" / "fixtures" / "check-ci-job-list"
LIVE_MAKEFILE = ROOT / "Makefile"
LIVE_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

ASSIGN_RE = re.compile(r"^CI_JOBS\s*(?:\?|:)?=\s*(.*)$")
JOB_KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*)\s*:")
TOP_JOBS_RE = re.compile(r"^jobs\s*:")


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def strip_make_comment(rest: str) -> str:
    idx = rest.find(" #")
    if idx >= 0:
        rest = rest[:idx]
    return rest.rstrip()


def parse_makefile_jobs(text: str, *, label: str) -> list[str]:
    lines = text.splitlines()
    found: list[str] | None = None
    i = 0
    while i < len(lines):
        raw = lines[i]
        if raw.lstrip().startswith("#"):
            i += 1
            continue
        m = ASSIGN_RE.match(raw)
        if not m:
            i += 1
            continue
        if found is not None:
            die(f"{label}: multiple CI_JOBS assignments")
        chunks: list[str] = []
        rest = m.group(1)
        while True:
            rest = strip_make_comment(rest)
            cont = rest.endswith("\\")
            if cont:
                rest = rest[:-1].rstrip()
            chunks.extend(rest.split())
            i += 1
            if not cont or i >= len(lines):
                break
            rest = lines[i]
        found = chunks
    if found is None:
        die(f"{label}: missing CI_JOBS assignment")
    if not found:
        die(f"{label}: CI_JOBS is empty")
    dups = sorted({j for j in found if found.count(j) > 1})
    if dups:
        die(f"{label}: CI_JOBS has duplicates: {' '.join(dups)}")
    return found


def parse_workflow_jobs(text: str, *, label: str) -> list[str]:
    lines = text.splitlines()
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.lstrip(" \t")
        if not stripped or stripped.startswith("#") or not TOP_JOBS_RE.match(line):
            i += 1
            continue
        i += 1
        jobs: list[str] = []
        job_indent: int | None = None
        while i < n:
            ln = lines[i]
            st = ln.lstrip(" \t")
            if not st or st.startswith("#"):
                i += 1
                continue
            indent = len(ln) - len(ln.lstrip(" \t"))
            if indent == 0:
                break
            if job_indent is None:
                job_indent = indent
            if indent == job_indent:
                m = JOB_KEY_RE.match(st)
                if m:
                    jobs.append(m.group(1))
            i += 1
        if not jobs:
            die(f"{label}: jobs: has no job ids")
        dups = sorted({j for j in jobs if jobs.count(j) > 1})
        if dups:
            die(f"{label}: duplicate job ids: {' '.join(dups)}")
        return jobs
    die(f"{label}: no top-level jobs: key")
    raise AssertionError("unreachable")


def load_pair(directory: Path, *, makefile: str = "Makefile", workflow: str = "ci.yml") -> tuple[list[str], list[str]]:
    mf = directory / makefile
    wf = directory / workflow
    if not mf.is_file():
        die(f"missing {mf.relative_to(ROOT)}")
    if not wf.is_file():
        die(f"missing {wf.relative_to(ROOT)}")
    rel_m = str(mf.relative_to(ROOT))
    rel_w = str(wf.relative_to(ROOT))
    return (
        parse_makefile_jobs(mf.read_text(encoding="utf-8"), label=rel_m),
        parse_workflow_jobs(wf.read_text(encoding="utf-8"), label=rel_w),
    )


def diff_sets(make_jobs: list[str], yml_jobs: list[str]) -> tuple[list[str], list[str]]:
    a, b = set(make_jobs), set(yml_jobs)
    return sorted(a - b), sorted(b - a)


def report_diff(only_make: list[str], only_yml: list[str]) -> None:
    print(
        "error: Makefile CI_JOBS and .github/workflows/ci.yml jobs differ (S0a-B-04):",
        file=sys.stderr,
    )
    if only_make:
        print(f"  only in Makefile: {' '.join(only_make)}", file=sys.stderr)
    if only_yml:
        print(f"  only in ci.yml:   {' '.join(only_yml)}", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: add or remove the job id on both sides; do not edit only one list.",
        file=sys.stderr,
    )


def compare_live() -> int:
    make_jobs, yml_jobs = load_pair(ROOT, makefile="Makefile", workflow=".github/workflows/ci.yml")
    only_make, only_yml = diff_sets(make_jobs, yml_jobs)
    if only_make or only_yml:
        report_diff(only_make, only_yml)
        return 1
    print(f"ok: CI_JOBS matches ci.yml ({len(set(make_jobs))} jobs): {' '.join(sorted(set(make_jobs)))}")
    return 0


def rel_fix(path: Path) -> str:
    return str(path.relative_to(ROOT))


def drop_workflow_job(text: str, job: str) -> str:
    """Remove one `jobs:` entry so AC 2 can run against the live workflow text."""
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    in_jobs = False
    job_indent: int | None = None
    skipping = False
    seen = False
    for line in lines:
        raw = line.rstrip("\n\r")
        st = raw.lstrip(" \t")
        if not st or st.startswith("#"):
            if not skipping:
                out.append(line)
            continue
        indent = len(raw) - len(raw.lstrip(" \t"))
        if TOP_JOBS_RE.match(raw):
            in_jobs = True
            skipping = False
            out.append(line)
            continue
        if indent == 0:
            in_jobs = False
            skipping = False
            out.append(line)
            continue
        if in_jobs:
            if job_indent is None:
                job_indent = indent
            if indent == job_indent:
                m = JOB_KEY_RE.match(st)
                if m:
                    skipping = m.group(1) == job
                    if skipping:
                        seen = True
                    if not skipping:
                        out.append(line)
                    continue
        if not skipping:
            out.append(line)
    if not seen:
        die(f"self-test: live workflow has no {job!r} job to delete")
    return "".join(out)


def self_test() -> int:
    failed = 0
    fail_dir = FIXTURE_ROOT / "expect-fail"
    pass_dir = FIXTURE_ROOT / "expect-pass"

    required_fail = (
        "yaml-missing-fmt",
        "yaml-extra-bonus",
        "makefile-extra-bonus",
    )
    required_pass = ("match",)

    if not fail_dir.is_dir() or not pass_dir.is_dir():
        die(f"missing fixture dirs under {rel_fix(FIXTURE_ROOT)}")

    for name in required_fail:
        if not (fail_dir / name / "ci.yml").is_file() or not (fail_dir / name / "Makefile").is_file():
            print(f"error: self-test: missing negative fixture {rel_fix(fail_dir / name)}", file=sys.stderr)
            failed = 1
    for name in required_pass:
        if not (pass_dir / name / "ci.yml").is_file() or not (pass_dir / name / "Makefile").is_file():
            print(f"error: self-test: missing positive fixture {rel_fix(pass_dir / name)}", file=sys.stderr)
            failed = 1

    def expect_fail(name: str, *, only_make: list[str], only_yml: list[str]) -> None:
        nonlocal failed
        directory = fail_dir / name
        if not directory.is_dir():
            return
        try:
            got_make, got_yml = load_pair(directory)
            got_only_make, got_only_yml = diff_sets(got_make, got_yml)
        except SystemExit as exc:
            print(f"error: self-test: {name} raised: {exc}", file=sys.stderr)
            failed = 1
            return
        if not got_only_make and not got_only_yml:
            print(
                f"error: self-test: expected mismatch in {rel_fix(directory)}",
                file=sys.stderr,
            )
            failed = 1
            return
        if got_only_make != only_make or got_only_yml != only_yml:
            print(f"error: self-test: {name} diff mismatch", file=sys.stderr)
            print(f"  got Makefile-only: {got_only_make} want {only_make}", file=sys.stderr)
            print(f"  got ci.yml-only:   {got_only_yml} want {only_yml}", file=sys.stderr)
            failed = 1
            return
        print(f"ok: self-test {name} is red ({'Makefile' if only_make else 'ci.yml'} extra: {' '.join(only_make or only_yml)})")

    def expect_pass(name: str) -> None:
        nonlocal failed
        directory = pass_dir / name
        if not directory.is_dir():
            return
        try:
            got_make, got_yml = load_pair(directory)
            got_only_make, got_only_yml = diff_sets(got_make, got_yml)
        except SystemExit as exc:
            print(f"error: self-test: {name} raised: {exc}", file=sys.stderr)
            failed = 1
            return
        if got_only_make or got_only_yml:
            print(f"error: self-test: expected match in {rel_fix(directory)}", file=sys.stderr)
            report_diff(got_only_make, got_only_yml)
            failed = 1
            return
        print(f"ok: self-test {name} is green")

    # Direct falsifier for AC 2: drop a job from ci.yml, leave Makefile alone.
    expect_fail("yaml-missing-fmt", only_make=["fmt"], only_yml=[])
    expect_fail("yaml-extra-bonus", only_make=[], only_yml=["bonus"])
    expect_fail("makefile-extra-bonus", only_make=["bonus"], only_yml=[])
    expect_pass("match")

    n_fail = sum(1 for p in fail_dir.iterdir() if p.is_dir() and (p / "ci.yml").is_file())
    n_pass = sum(1 for p in pass_dir.iterdir() if p.is_dir() and (p / "ci.yml").is_file())
    if n_fail < 3:
        print(
            f"error: self-test: need >=3 negative fixtures in {rel_fix(fail_dir)} (found {n_fail})",
            file=sys.stderr,
        )
        failed = 1
    if n_pass < 1:
        print(
            f"error: self-test: need >=1 positive fixture in {rel_fix(pass_dir)} (found {n_pass})",
            file=sys.stderr,
        )
        failed = 1

    if failed:
        die("CI job-list fixture self-test failed")
    print("ok: CI job-list fixtures")
    return 0


def demonstrate_live_fmt_deletion() -> int:
    """AC2 against the live workflow text: drop `fmt`, require Makefile extra.

    Only called when `fmt` is still a live `jobs:` key (after `compare_live`,
    or on `--self-test` when the committed workflow still has it). A real
    delete of `fmt` from ci.yml is reported by `compare_live` / `report_diff`,
    not by this helper dying on a missing job.
    """
    live_make = parse_makefile_jobs(
        LIVE_MAKEFILE.read_text(encoding="utf-8"), label="Makefile"
    )
    live_yml = LIVE_WORKFLOW.read_text(encoding="utf-8")
    live_jobs = parse_workflow_jobs(live_yml, label=".github/workflows/ci.yml")
    if "fmt" not in live_jobs:
        print("ok: skip live fmt-drop demonstration (fmt already absent from ci.yml)")
        return 0
    dropped = drop_workflow_job(live_yml, "fmt")
    dropped_jobs = parse_workflow_jobs(
        dropped, label=".github/workflows/ci.yml (fmt deleted)"
    )
    only_make, only_yml = diff_sets(live_make, dropped_jobs)
    if "fmt" not in only_make:
        print(
            "error: live in-memory fmt deletion did not report a Makefile extra",
            file=sys.stderr,
        )
        report_diff(only_make, only_yml)
        return 1
    extras = [j for j in only_make if j != "fmt"]
    if extras or only_yml:
        # Live lists already drifted; still show that fmt is a Makefile extra.
        print(
            "ok: live in-memory fmt deletion reports only in Makefile: fmt "
            f"(also drifted: Makefile {extras} ci.yml {only_yml})"
        )
    else:
        print("ok: live in-memory fmt deletion reports only in Makefile: fmt")
    return 0


def main() -> int:
    if ARG in ("-h", "--help"):
        print(
            "Usage: bash scripts/check-ci-job-list.sh [--self-test]\n"
            "Diff Makefile CI_JOBS against .github/workflows/ci.yml job ids."
        )
        return 0
    if ARG not in ("", "--self-test"):
        die(f"unknown argument: {ARG} (want --self-test)")

    if not LIVE_MAKEFILE.is_file():
        die(f"missing {LIVE_MAKEFILE}")
    if not LIVE_WORKFLOW.is_file():
        die(f"missing {LIVE_WORKFLOW}")

    rc = self_test()
    if ARG == "--self-test":
        demo = demonstrate_live_fmt_deletion()
        return demo if demo else rc
    # Live set-diff before the in-memory fmt drop so a real delete of `fmt`
    # from ci.yml prints `only in Makefile: fmt` via report_diff (F1).
    live = compare_live()
    if live != 0:
        return live
    demo = demonstrate_live_fmt_deletion()
    return demo if demo else rc


if __name__ == "__main__":
    raise SystemExit(main())
PY
