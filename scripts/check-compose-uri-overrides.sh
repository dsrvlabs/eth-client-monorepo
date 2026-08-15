#!/usr/bin/env bash
# S0-B-02 / P0-07 + P1-A/27 — compose must override the three cross-service
# URIs and mount p2p identity onto storage.
#
# Missing keys make localhost TOML defaults apply in-container: chain dials
# its own 9004 (import dead), engine has no CC_ENGINE_P2P_URI, p2p has no
# CC_P2P_PEERS__STORAGE (serve window fail-closes). Storage without the
# identity mount / CC_STORAGE_NODE_KEY_PATH can never enforce I-node-id.
#
# A Hoodi block-import smoke needs a synced EL and is outside the required
# CI budget. This gate is the smallest check that would have caught the
# missing keys: parse docker-compose.yml (always) and optionally assert the
# same keys inside a running stack (`--runtime`, compose job).
#
# Usage:
#   bash scripts/check-compose-uri-overrides.sh              # fixtures + live file
#   bash scripts/check-compose-uri-overrides.sh --self-test  # fixtures only
#   bash scripts/check-compose-uri-overrides.sh --runtime    # live file + running stack
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 - "$ROOT" "${1:-}" <<'PY'
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(sys.argv[1])
ARG = sys.argv[2] if len(sys.argv) > 2 else ""
LIVE = ROOT / "docker-compose.yml"
FIXTURE_ROOT = ROOT / "scripts" / "fixtures" / "check-compose-uri-overrides"

# Compose-network values — TOML localhost defaults are the defect.
URI_KEYS = (
    ("chain", "CC_CHAIN_ENGINE_URI", "http://engine:9004"),
    ("engine", "CC_ENGINE_P2P_URI", "http://p2p:9002"),
    ("p2p", "CC_P2P_PEERS__STORAGE", "http://storage:9006"),
)
IDENTITY_VOL = "cc-p2p-identity"
IDENTITY_ENV = "CC_STORAGE_NODE_KEY_PATH"
IDENTITY_KEY_SUFFIX = "node_key"

LOCALHOST_RE = re.compile(r"(?i)(127\.0\.0\.1|localhost)")
SERVICE_RE = re.compile(r"^([A-Za-z0-9_-]+)\s*:")
ENV_KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$")
VOL_SHORT_RE = re.compile(
    r"^(?P<src>[^:]+):(?P<tgt>[^:]+)(?::(?P<mode>[^:]+))?$"
)


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def strip_comment(line: str) -> str:
    in_s = in_d = False
    out: list[str] = []
    for c in line:
        if c == "'" and not in_d:
            in_s = not in_s
        elif c == '"' and not in_s:
            in_d = not in_d
        elif c == "#" and not in_s and not in_d:
            break
        out.append(c)
    return "".join(out).rstrip()


def unquote(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        return s[1:-1]
    return s


def leading_ws(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def parse_services(text: str) -> dict[str, dict[str, object]]:
    """Map service name → {env: {k: v}, volumes: [(src, tgt, mode)]}."""
    lines = text.splitlines()
    n = len(lines)
    i = 0
    while i < n:
        raw = strip_comment(lines[i])
        if raw.strip() == "services:":
            i += 1
            break
        i += 1
    else:
        die("no top-level services: key")

    services: dict[str, dict[str, object]] = {}
    svc: str | None = None
    svc_indent: int | None = None
    section: str | None = None
    section_indent: int | None = None

    while i < n:
        raw_line = lines[i]
        raw = strip_comment(raw_line)
        st = raw.strip()
        if not st:
            i += 1
            continue
        indent = leading_ws(raw)
        if indent == 0:
            break
        if svc_indent is None:
            m = SERVICE_RE.match(st)
            if not m:
                i += 1
                continue
            svc_indent = indent
            svc = m.group(1)
            services[svc] = {"env": {}, "volumes": []}
            section = None
            i += 1
            continue
        if indent == svc_indent:
            m = SERVICE_RE.match(st)
            if m:
                svc = m.group(1)
                services[svc] = {"env": {}, "volumes": []}
                section = None
                section_indent = None
            i += 1
            continue
        if svc is None:
            i += 1
            continue
        if section is not None and indent <= (section_indent or indent):
            section = None
            section_indent = None
        if section is None:
            if st.startswith("environment:"):
                section = "environment"
                section_indent = indent
            elif st.startswith("volumes:"):
                section = "volumes"
                section_indent = indent
            i += 1
            continue
        if section == "environment":
            if st.startswith("<<:"):
                i += 1
                continue
            em = ENV_KEY_RE.match(st)
            if em:
                services[svc]["env"][em.group(1)] = unquote(em.group(2))  # type: ignore[index]
        elif section == "volumes" and st.startswith("-"):
            item = unquote(st[1:].strip())
            src, tgt, mode = parse_volume_item(item, lines, i, indent)
            if src:
                services[svc]["volumes"].append((src, tgt, mode))  # type: ignore[union-attr]
        i += 1
    return services


def parse_volume_item(
    item: str, lines: list[str], idx: int, indent: int
) -> tuple[str, str, str]:
    """Short-form `src:tgt[:mode]` or long-form `source:` / `target:` siblings."""
    if item and not item.endswith(":") and ":" in item and not item.startswith("type:"):
        m = VOL_SHORT_RE.match(item)
        if m:
            return m.group("src"), m.group("tgt"), m.group("mode") or ""
        return item, "", ""
    # Long form: `- type: volume` then source:/target: at greater indent.
    src = tgt = mode = ""
    j = idx + 1
    while j < len(lines):
        raw = strip_comment(lines[j])
        st = raw.strip()
        if not st:
            j += 1
            continue
        if leading_ws(raw) <= indent:
            break
        if st.startswith("source:"):
            src = unquote(st.split(":", 1)[1])
        elif st.startswith("target:"):
            tgt = unquote(st.split(":", 1)[1])
        elif st.startswith("read_only:"):
            val = unquote(st.split(":", 1)[1]).lower()
            if val in ("true", "yes"):
                mode = "ro"
        j += 1
    if not src and item.startswith("source:"):
        src = unquote(item.split(":", 1)[1])
    return src, tgt, mode


def check_file(path: Path, *, label: str) -> list[str]:
    if not path.is_file():
        return [f"{label}: missing file"]
    text = path.read_text(encoding="utf-8")
    try:
        services = parse_services(text)
    except SystemExit as exc:
        return [f"{label}: {exc}"]
    errors: list[str] = []
    for svc, key, want in URI_KEYS:
        env = services.get(svc, {}).get("env", {})
        if not isinstance(env, dict):
            errors.append(f"{label}: {svc}: environment is not a mapping")
            continue
        got = env.get(key)
        if got is None:
            errors.append(f"{label}: {svc} missing {key} (want {want})")
            continue
        if LOCALHOST_RE.search(got):
            errors.append(
                f"{label}: {svc}.{key}={got} uses localhost; want compose DNS {want}"
            )
            continue
        if got != want:
            errors.append(f"{label}: {svc}.{key}={got} (want {want})")

    stor = services.get("storage")
    if stor is None:
        errors.append(f"{label}: missing storage service")
        return errors
    env = stor.get("env", {})
    vols = stor.get("volumes", [])
    if not isinstance(env, dict) or not isinstance(vols, list):
        errors.append(f"{label}: storage env/volumes unreadable")
        return errors
    path_val = env.get(IDENTITY_ENV)
    if not path_val:
        errors.append(f"{label}: storage missing {IDENTITY_ENV}")
    elif not path_val.rstrip("/").endswith(IDENTITY_KEY_SUFFIX):
        errors.append(
            f"{label}: storage.{IDENTITY_ENV}={path_val} must end with {IDENTITY_KEY_SUFFIX}"
        )
    identity = [v for v in vols if isinstance(v, tuple) and v[0] == IDENTITY_VOL]
    if not identity:
        errors.append(
            f"{label}: storage has no {IDENTITY_VOL} volume (I-node-id identity mount)"
        )
    else:
        _src, tgt, mode = identity[0]
        if not volume_is_ro(mode):
            errors.append(
                f"{label}: storage {IDENTITY_VOL} mount must be :ro "
                f"(got mode={mode!r})"
            )
        if path_val and tgt and not (
            path_val == tgt or path_val.startswith(tgt.rstrip("/") + "/")
        ):
            errors.append(
                f"{label}: storage.{IDENTITY_ENV}={path_val} is not under mount {tgt}"
            )
    return errors


def volume_is_ro(mode: str) -> bool:
    """True when compose short-form `:ro` or long-form `read_only: true`."""
    parts = {p.strip().lower() for p in (mode or "").split(",") if p.strip()}
    return "ro" in parts


def report(errors: list[str]) -> int:
    if not errors:
        return 0
    print(
        "error: compose URI overrides / identity mount check failed (S0-B-02):",
        file=sys.stderr,
    )
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: chain needs CC_CHAIN_ENGINE_URI=http://engine:9004; "
        "engine needs CC_ENGINE_P2P_URI=http://p2p:9002; "
        "p2p needs CC_P2P_PEERS__STORAGE=http://storage:9006; "
        "storage needs CC_STORAGE_NODE_KEY_PATH plus a read-only "
        "cc-p2p-identity mount (`:ro`).",
        file=sys.stderr,
    )
    return 1


def rel_fix(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def self_test() -> int:
    fail_dir = FIXTURE_ROOT / "expect-fail"
    pass_dir = FIXTURE_ROOT / "expect-pass"
    if not fail_dir.is_dir() or not pass_dir.is_dir():
        die(f"missing fixture dirs under {rel_fix(FIXTURE_ROOT)}")

    required_fail = (
        "missing-chain-engine-uri",
        "missing-engine-p2p-uri",
        "missing-p2p-storage-peer",
        "missing-identity-mount",
        "missing-storage-node-key-path",
        "localhost-chain-engine-uri",
        "rw-identity-mount",
    )
    required_pass = ("match",)
    failed = 0

    for name in required_fail:
        p = fail_dir / name / "docker-compose.yml"
        if not p.is_file():
            print(f"error: self-test: missing negative fixture {rel_fix(p)}", file=sys.stderr)
            failed = 1
    for name in required_pass:
        p = pass_dir / name / "docker-compose.yml"
        if not p.is_file():
            print(f"error: self-test: missing positive fixture {rel_fix(p)}", file=sys.stderr)
            failed = 1

    n_fail = 0
    for d in sorted(fail_dir.iterdir()):
        yml = d / "docker-compose.yml"
        if not yml.is_file():
            continue
        n_fail += 1
        errs = check_file(yml, label=rel_fix(yml))
        if not errs:
            print(f"error: self-test: expected failure in {rel_fix(d)}", file=sys.stderr)
            failed = 1
        else:
            print(f"ok: self-test {d.name} is red")

    n_pass = 0
    for d in sorted(pass_dir.iterdir()):
        yml = d / "docker-compose.yml"
        if not yml.is_file():
            continue
        n_pass += 1
        errs = check_file(yml, label=rel_fix(yml))
        if errs:
            print(f"error: self-test: expected pass in {rel_fix(d)}:", file=sys.stderr)
            for e in errs:
                print(f"  {e}", file=sys.stderr)
            failed = 1
        else:
            print(f"ok: self-test {d.name} is green")

    if n_fail < 7:
        print(
            f"error: self-test: need >=7 negative fixtures in {rel_fix(fail_dir)} (found {n_fail})",
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
        die("compose URI-override fixture self-test failed")
    print("ok: compose URI-override fixtures")
    return 0


def compose_exec(svc: str, args: list[str]) -> tuple[int, str]:
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", svc, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    out = (r.stdout or "").strip()
    if r.returncode != 0 and r.stderr:
        err = r.stderr.strip().replace("\n", " ")
        out = f"{out} ({err})".strip()
    return r.returncode, out


def runtime_check() -> int:
    if subprocess.run(["docker", "compose", "version"], capture_output=True).returncode != 0:
        die("docker compose is required for --runtime")
    errors: list[str] = []
    for svc, key, want in URI_KEYS:
        rc, got = compose_exec(svc, ["printenv", key])
        if rc != 0 or not got or got.startswith("("):
            errors.append(f"runtime: {svc} printenv {key} failed: {got or f'exit {rc}'}")
            continue
        # printenv may include a trailing comment-free value only.
        got = got.splitlines()[0].strip()
        if got != want:
            errors.append(f"runtime: {svc}.{key}={got} (want {want})")
    rc, got = compose_exec("storage", ["printenv", IDENTITY_ENV])
    if rc != 0 or not got:
        errors.append(f"runtime: storage printenv {IDENTITY_ENV} failed: {got or f'exit {rc}'}")
    else:
        got = got.splitlines()[0].strip()
        if not got.rstrip("/").endswith(IDENTITY_KEY_SUFFIX):
            errors.append(f"runtime: storage.{IDENTITY_ENV}={got} must end with {IDENTITY_KEY_SUFFIX}")
        mount_dir = str(Path(got).parent)
        rc2, _ = compose_exec("storage", ["sh", "-c", f"test -d {mount_dir}"])
        if rc2 != 0:
            errors.append(f"runtime: storage identity mount dir missing: {mount_dir}")
        rc3, mounts = compose_exec(
            "storage", ["sh", "-c", f"grep -E ' {mount_dir} ' /proc/mounts || true"]
        )
        if rc3 != 0 or mount_dir not in mounts:
            errors.append(
                f"runtime: storage {mount_dir} is not a mount ({IDENTITY_VOL} identity)"
            )
        else:
            ro = False
            for line in mounts.splitlines():
                parts = line.split()
                if len(parts) >= 4 and parts[1].rstrip("/") == mount_dir.rstrip("/"):
                    opts = {o.strip() for o in parts[3].split(",") if o.strip()}
                    if "ro" in opts:
                        ro = True
            if not ro:
                errors.append(
                    f"runtime: storage {mount_dir} is not mounted :ro "
                    f"({IDENTITY_VOL} identity)"
                )
    if errors:
        return report(errors)
    print(
        "ok: running stack has the three URI overrides and storage identity mount"
    )
    return 0


def main() -> int:
    if ARG in ("-h", "--help"):
        print(
            "Usage: bash scripts/check-compose-uri-overrides.sh "
            "[--self-test|--runtime]\n"
            "Assert P0-07 URI overrides and P1-A/27 identity mount in "
            "docker-compose.yml."
        )
        return 0
    if ARG not in ("", "--self-test", "--runtime"):
        die(f"unknown argument: {ARG} (want --self-test or --runtime)")

    rc = self_test()
    if ARG == "--self-test":
        return rc
    if not LIVE.is_file():
        die(f"missing {LIVE}")
    live_errors = check_file(LIVE, label="docker-compose.yml")
    if live_errors:
        return report(live_errors)
    print(
        "ok: docker-compose.yml has CC_CHAIN_ENGINE_URI, CC_ENGINE_P2P_URI, "
        "CC_P2P_PEERS__STORAGE, and storage identity mount"
    )
    if ARG == "--runtime":
        return runtime_check()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
PY
