#!/usr/bin/env bash
# S0-B-20 / E0.8 — off-host port scan (M4).
#
# A scan from a second host must reach none of 9001–9006, and 9101–9106
# must answer only on 127.0.0.1. A compose diff is not evidence.
#
# --policy is the lint gate (fixtures + docker-compose.yml). It is not E0.8.
# --scan replays compose -p flags onto alpine dummy listeners. It is a
# compose-file regression, not E0.8, and must never print M4=0.
# --live is the E0.8 instrument: the real compose stack must already be
# up (127.0.0.1:910N serving cc_* /metrics). A scanner on an isolated
# docker network (not compose `cc`, not the host loopback netns) probes
# the host's non-loopback addresses. A 0.0.0.0 canary proves the path
# can see a host bind.
#
# Usage:
#   bash scripts/offhost-port-scan.sh                 # fixtures + policy
#   bash scripts/offhost-port-scan.sh --policy        # fixtures + live compose
#   bash scripts/offhost-port-scan.sh --self-test     # fixtures only
#   bash scripts/offhost-port-scan.sh --scan          # dummy alpine replay (not E0.8)
#   bash scripts/offhost-port-scan.sh --live          # E0.8: running compose stack
#   bash scripts/offhost-port-scan.sh --remote HOST   # we are the second host
#   bash scripts/offhost-port-scan.sh --record FILE   # also write the transcript
#
# Two-host procedure (physical second host, or a netns with a veth to LAN):
#   On the compose host, bring the stack up and note LAN_IP.
#   On the second host:
#     bash scripts/offhost-port-scan.sh --remote "$LAN_IP"
#   All twelve ports must be closed. Separately, on the compose host:
#     127.0.0.1:9101–9106 open with cc_* /metrics; 127.0.0.1:9001–9006 closed
#     (`--live` loopback half).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

MODE="default"
RECORD=""
REMOTE=""
IMAGE="${S0B20_IMAGE:-alpine:3.20}"
GRPC_PORTS=(9001 9002 9003 9004 9005 9006)
METRICS_PORTS=(9101 9102 9103 9104 9105 9106)
CANARY_PORTS=(19191 19192 19193 19194 19195)
RUN_ID="s0b20-$$-${RANDOM:-0}"
TARGET_NET="${RUN_ID}-target"
SCAN_NET="${RUN_ID}-scan"
LISTEN_CTR="${RUN_ID}-listen"
CANARY_CTR="${RUN_ID}-canary"
SCAN_CTR="${RUN_ID}-scan"
WORKDIR=""
CLEANED=0
INVOKED="bash scripts/offhost-port-scan.sh $*"

usage() {
  sed -n '16,23p' "$0" | sed 's/^# \{0,1\}//'
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --policy) MODE="policy"; shift ;;
    --self-test) MODE="self-test"; shift ;;
    --scan) MODE="scan"; shift ;;
    --live) MODE="live"; shift ;;
    --remote)
      [[ $# -ge 2 ]] || die "--remote needs a host address"
      MODE="remote"
      REMOTE="$2"
      shift 2
      ;;
    --record)
      [[ $# -ge 2 ]] || die "--record needs a path"
      RECORD="$2"
      shift 2
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if [[ -n "${RECORD}" ]]; then
  mkdir -p "$(dirname "${RECORD}")"
  exec > >(tee "${RECORD}") 2>&1
fi

# Embedded helpers live in a temp copy so the container can mount them.
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/s0b20.XXXXXX")"
export S0B20_WORKDIR="${WORKDIR}"

cleanup() {
  if [[ "${CLEANED}" -eq 1 ]]; then
    return 0
  fi
  CLEANED=1
  if command -v docker >/dev/null 2>&1; then
    docker rm -f "${LISTEN_CTR}" "${CANARY_CTR}" "${SCAN_CTR}" >/dev/null 2>&1 || true
    docker network rm "${TARGET_NET}" "${SCAN_NET}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf "${WORKDIR}"
  fi
}
trap cleanup EXIT

cat >"${WORKDIR}/s0b20.py" <<'PY'
#!/usr/bin/env python3
"""S0-B-20 helpers: compose host-publish policy + TCP listen/scan."""
from __future__ import annotations

import ipaddress
import os
import re
import socket
import sys
import threading
import time
from pathlib import Path

GRPC = tuple(range(9001, 9007))
METRICS = tuple(range(9101, 9107))
LOOPBACK = {"127.0.0.1", "::1"}
SERVICE_RE = re.compile(r"^([A-Za-z0-9_-]+)\s*:")
PORT_ITEM_RE = re.compile(
    r"^(?:"
    r"\[(?P<v6>[^]]+)\]:(?P<v6hp>[0-9]*):(?P<v6cp>[0-9]+)(?:/(?P<v6pr>[a-z0-9]+))?"
    r"|"
    r"(?P<hip>\d+\.\d+\.\d+\.\d+):(?P<hp>[0-9]*):(?P<cp>[0-9]+)(?:/(?P<pr>[a-z0-9]+))?"
    r"|"
    r"(?P<hp2>[0-9]+):(?P<cp2>[0-9]+)(?:/(?P<pr2>[a-z0-9]+))?"
    r"|"
    r"(?P<only>[0-9]+)(?:/(?P<pr3>[a-z0-9]+))?"
    r")$"
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


def parse_short_port(item: str) -> dict[str, object] | None:
    item = item.strip().strip(",")
    if not item or item.endswith(":"):
        return None
    m = PORT_ITEM_RE.match(item)
    if not m:
        return None
    if m.group("v6"):
        host_ip = m.group("v6")
        host_s = m.group("v6hp")
        container = int(m.group("v6cp"))
        proto = m.group("v6pr") or "tcp"
    elif m.group("hip"):
        host_ip = m.group("hip")
        host_s = m.group("hp")
        container = int(m.group("cp"))
        proto = m.group("pr") or "tcp"
    elif m.group("hp2"):
        host_ip = "0.0.0.0"
        host_s = m.group("hp2")
        container = int(m.group("cp2"))
        proto = m.group("pr2") or "tcp"
    else:
        host_ip = "0.0.0.0"
        host_s = m.group("only")
        container = int(m.group("only"))
        proto = m.group("pr3") or "tcp"
    host_port = int(host_s) if host_s else None
    return {
        "host_ip": host_ip,
        "host_port": host_port,
        "container_port": container,
        "proto": proto,
        "raw": item,
    }


def parse_ports(text: str) -> list[dict[str, object]]:
    lines = text.splitlines()
    n = len(lines)
    i = 0
    while i < n:
        if strip_comment(lines[i]).strip() == "services:":
            i += 1
            break
        i += 1
    else:
        die("no top-level services: key")

    ports: list[dict[str, object]] = []
    svc: str | None = None
    svc_indent: int | None = None
    section: str | None = None
    section_indent: int | None = None

    while i < n:
        raw = strip_comment(lines[i])
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
            section = None
            i += 1
            continue
        if indent == svc_indent:
            m = SERVICE_RE.match(st)
            if m:
                svc = m.group(1)
                section = None
                section_indent = None
            i += 1
            continue
        if section is not None and indent <= (section_indent or indent):
            section = None
            section_indent = None
        if section is None:
            if st.startswith("ports:"):
                section = "ports"
                section_indent = indent
                rest = st[len("ports:") :].strip()
                if rest.startswith("["):
                    body = rest.strip("[]")
                    for part in body.split(","):
                        parsed = parse_short_port(unquote(part.strip()))
                        if parsed:
                            parsed["service"] = svc
                            ports.append(parsed)
                    section = None
            i += 1
            continue
        if section == "ports" and st.startswith("-"):
            item = unquote(st[1:].strip())
            parsed = parse_short_port(item)
            if parsed:
                parsed["service"] = svc
                ports.append(parsed)
            else:
                # Long form: consume siblings.
                long = {"service": svc, "host_ip": "0.0.0.0", "proto": "tcp"}
                if item.startswith("target:"):
                    long["container_port"] = int(unquote(item.split(":", 1)[1]))
                elif item.startswith("published:"):
                    long["host_port"] = int(unquote(item.split(":", 1)[1]))
                j = i + 1
                while j < n:
                    raw_j = strip_comment(lines[j])
                    st_j = raw_j.strip()
                    if not st_j:
                        j += 1
                        continue
                    if leading_ws(raw_j) <= indent:
                        break
                    if st_j.startswith("target:"):
                        long["container_port"] = int(unquote(st_j.split(":", 1)[1]))
                    elif st_j.startswith("published:"):
                        long["host_port"] = int(unquote(st_j.split(":", 1)[1]))
                    elif st_j.startswith("host_ip:"):
                        long["host_ip"] = unquote(st_j.split(":", 1)[1])
                    elif st_j.startswith("protocol:"):
                        long["proto"] = unquote(st_j.split(":", 1)[1])
                    j += 1
                if "container_port" in long:
                    long["raw"] = item
                    ports.append(long)
        i += 1
    return ports


def relevant(p: dict[str, object]) -> bool:
    hp = p.get("host_port")
    cp = p.get("container_port")
    proto = str(p.get("proto") or "tcp").lower()
    if proto not in ("tcp", ""):
        return False
    for val in (hp, cp):
        if isinstance(val, int) and (val in GRPC or val in METRICS):
            return True
    return False


def check_ports(ports: list[dict[str, object]], *, label: str) -> list[str]:
    errors: list[str] = []
    metrics_ok: dict[int, bool] = {p: False for p in METRICS}
    for p in ports:
        if not relevant(p):
            continue
        svc = p.get("service") or "?"
        host_ip = str(p.get("host_ip") or "0.0.0.0")
        hp = p.get("host_port")
        cp = p.get("container_port")
        raw = p.get("raw") or ""
        involved = [
            n
            for n in (hp, cp)
            if isinstance(n, int) and (n in GRPC or n in METRICS)
        ]
        for n in involved:
            if n in GRPC:
                errors.append(
                    f"{label}: {svc} publishes gRPC {n} to the host "
                    f"({host_ip}:{hp}->{cp} raw={raw!r})"
                )
            if n in METRICS:
                if host_ip not in LOOPBACK:
                    errors.append(
                        f"{label}: {svc} publishes metrics {n} on {host_ip} "
                        f"(want 127.0.0.1) raw={raw!r}"
                    )
                elif isinstance(hp, int) and hp == n:
                    metrics_ok[n] = True
    for n, ok in metrics_ok.items():
        if not ok:
            errors.append(
                f"{label}: metrics {n} is not published on 127.0.0.1:{n}"
            )
    return errors


def check_file(path: Path, *, label: str) -> list[str]:
    if not path.is_file():
        return [f"{label}: missing file"]
    text = path.read_text(encoding="utf-8")
    try:
        ports = parse_ports(text)
    except SystemExit as exc:
        return [f"{label}: {exc}"]
    return check_ports(ports, label=label)


def compose_publish_flags(path: Path) -> list[str]:
    """docker -p flags for 9001–9006 / 9101–9106 from compose (policy must pass)."""
    flags: list[str] = []
    for p in parse_ports(path.read_text(encoding="utf-8")):
        if not relevant(p):
            continue
        host_ip = str(p.get("host_ip") or "0.0.0.0")
        hp = p.get("host_port")
        cp = p.get("container_port")
        if not isinstance(cp, int):
            continue
        if isinstance(hp, int):
            flags.append(f"{host_ip}:{hp}:{cp}")
        else:
            flags.append(f"{host_ip}::{cp}")
    return flags


def probe(ip: str, port: int, timeout: float = 0.8) -> tuple[str, str]:
    err = ""
    try:
        addr = (ip, port)
        family = socket.AF_INET6 if ":" in ip and not ip.startswith("127.") else socket.AF_INET
        if ip == "::1":
            family = socket.AF_INET6
        s = socket.socket(family, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect(addr)
            return "open", ""
        except OSError as exc:
            err = type(exc).__name__
            return "closed", err
        finally:
            s.close()
    except OSError as exc:
        return "closed", type(exc).__name__ or err


def cmd_listen(ports: list[int]) -> int:
    def serve(port: int) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
        s.listen(32)
        while True:
            try:
                c, _ = s.accept()
                try:
                    c.sendall(b"s0-b-20\n")
                except OSError:
                    pass
                c.close()
            except OSError:
                time.sleep(0.05)

    for p in ports:
        t = threading.Thread(target=serve, args=(p,), daemon=True)
        t.start()
    # Ready marker for the orchestrator.
    print("s0b20-listen-ready " + " ".join(str(p) for p in ports), flush=True)
    threading.Event().wait()
    return 0


def cmd_probe(args: list[str]) -> int:
    # Always exit 0 — the orchestrator reads the printed state.
    # A closed port is the expected E0.8 result, not a helper failure.
    if len(args) < 2:
        die("probe: want IP PORT [PORT...]")
    ip = args[0]
    for raw in args[1:]:
        port = int(raw)
        state, err = probe(ip, port)
        extra = f" {err}" if err else ""
        print(f"probe {ip}:{port} {state}{extra}")
    return 0


def is_loopback(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_loopback
    except ValueError:
        return ip in LOOPBACK or ip.startswith("127.")


def is_link_local(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_link_local
    except ValueError:
        return ip.startswith("169.254.")


def cmd_check_remote(ip: str) -> int:
    """Exit 1 if REMOTE is loopback / link-local / unspecified / multicast."""
    ip = ip.strip()
    if not ip:
        die("empty --remote target")
    host = ip
    if host.startswith("[") and "]" in host:
        host = host[1 : host.index("]")]
    elif host.count(":") == 1 and not host.startswith(":"):
        host = host.split(":", 1)[0]
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        # Hostname: resolve and reject if every A/AAAA is local-only.
        try:
            infos = socket.getaddrinfo(host, None)
        except OSError as exc:
            die(f"--remote {ip!r} did not resolve: {exc}")
        addrs = []
        for info in infos:
            try:
                addrs.append(ipaddress.ip_address(info[4][0]))
            except (ValueError, OSError):
                continue
        if not addrs:
            die(f"--remote {ip!r} resolved to no addresses")
        if all(
            a.is_loopback or a.is_link_local or a.is_unspecified or a.is_multicast
            for a in addrs
        ):
            die(
                f"--remote {ip} resolves only to loopback/link-local "
                f"({', '.join(str(a) for a in addrs)}); refuse localhost M4"
            )
        print(f"ok: --remote {ip} is not loopback/link-local")
        return 0
    if addr.is_loopback or addr.is_link_local or addr.is_unspecified or addr.is_multicast:
        die(
            f"--remote {ip} is {addr} (loopback/link-local/unspecified); "
            "refuse localhost M4"
        )
    print(f"ok: --remote {ip} is not loopback/link-local")
    return 0


def cmd_host_ips() -> int:
    ips: set[str] = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    text = os.popen("ifconfig 2>/dev/null; ip -4 -o addr show 2>/dev/null").read()
    for m in re.finditer(r"\binet[6]?\s+(\d+\.\d+\.\d+\.\d+)", text):
        ips.add(m.group(1))
    keep = []
    for ip in sorted(ips):
        if is_loopback(ip) or is_link_local(ip) or ip.startswith("0."):
            continue
        try:
            addr = ipaddress.ip_address(ip)
            if addr.is_multicast or addr.is_unspecified or addr.is_reserved:
                continue
        except ValueError:
            continue
        keep.append(ip)
    for ip in keep:
        print(ip)
    return 0


def fixtures(root: Path) -> int:
    fx = root / "scripts" / "fixtures" / "offhost-port-scan"
    fail_dir = fx / "expect-fail"
    pass_dir = fx / "expect-pass"
    required_fail = ("published-grpc", "metrics-unbound", "missing-metrics")
    required_pass = ("loopback-metrics",)
    failed = 0
    for name in required_fail:
        p = fail_dir / name / "docker-compose.yml"
        if not p.is_file():
            print(f"error: self-test: missing negative fixture {p}", file=sys.stderr)
            failed = 1
    for name in required_pass:
        p = pass_dir / name / "docker-compose.yml"
        if not p.is_file():
            print(f"error: self-test: missing positive fixture {p}", file=sys.stderr)
            failed = 1
    n_fail = n_pass = 0
    if fail_dir.is_dir():
        for d in sorted(fail_dir.iterdir()):
            yml = d / "docker-compose.yml"
            if not yml.is_file():
                continue
            n_fail += 1
            errs = check_file(yml, label=str(yml.relative_to(root)))
            if not errs:
                print(f"error: self-test: expected failure in {d.name}", file=sys.stderr)
                failed = 1
            else:
                print(f"ok: self-test {d.name} is red")
    if pass_dir.is_dir():
        for d in sorted(pass_dir.iterdir()):
            yml = d / "docker-compose.yml"
            if not yml.is_file():
                continue
            n_pass += 1
            errs = check_file(yml, label=str(yml.relative_to(root)))
            if errs:
                print(f"error: self-test: expected pass in {d.name}:", file=sys.stderr)
                for e in errs:
                    print(f"  {e}", file=sys.stderr)
                failed = 1
            else:
                print(f"ok: self-test {d.name} is green")
    if n_fail < 3:
        print(f"error: self-test: need >=3 negative fixtures (found {n_fail})", file=sys.stderr)
        failed = 1
    if n_pass < 1:
        print(f"error: self-test: need >=1 positive fixture (found {n_pass})", file=sys.stderr)
        failed = 1
    if failed:
        die("off-host port-scan fixture self-test failed")
    print("ok: off-host port-scan fixtures")
    return 0


def cmd_policy(root: Path, paths: list[str]) -> int:
    errors: list[str] = []
    for raw in paths:
        p = Path(raw)
        label = p.name if p.resolve() == (root / "docker-compose.yml").resolve() else str(p)
        errors.extend(check_file(p, label=label))
    if errors:
        print("error: compose host-publish policy failed (S0-B-20):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "hint: do not publish 9001–9006; bind 9101–9106 as 127.0.0.1:910N:910N.",
            file=sys.stderr,
        )
        return 1
    print("ok: docker-compose.yml publishes 9101–9106 on 127.0.0.1 and no 9001–9006")
    return 0


def cmd_publish_flags(path: Path) -> int:
    for flag in compose_publish_flags(path):
        print(flag)
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        die("usage: s0b20.py listen|probe|host-ips|fixtures|policy|publish-flags ...")
    cmd = argv[1]
    rest = argv[2:]
    if cmd == "listen":
        return cmd_listen([int(x) for x in rest])
    if cmd == "probe":
        return cmd_probe(rest)
    if cmd == "check-remote":
        if not rest:
            die("check-remote: want IP")
        return cmd_check_remote(rest[0])
    if cmd == "host-ips":
        return cmd_host_ips()
    if cmd == "fixtures":
        return fixtures(Path(rest[0]))
    if cmd == "policy":
        return cmd_policy(Path(rest[0]), rest[1:])
    if cmd == "publish-flags":
        return cmd_publish_flags(Path(rest[0]))
    die(f"unknown helper command: {cmd}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
PY

helper() {
  python3 "${WORKDIR}/s0b20.py" "$@"
}

echo "s0-b-20 / E0.8 off-host port scan"
echo "date_utc:    $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "uname:       $(uname -a)"
echo "hostname:    $(hostname)"
echo "cwd:         ${ROOT}"
echo "mode:        ${MODE}"
echo "command:     ${INVOKED}"
echo "compose:     ${ROOT}/docker-compose.yml"
echo

# ── fixtures (always, except --remote) ───────────────────────────────────
if [[ "${MODE}" != "remote" ]]; then
  helper fixtures "${ROOT}"
fi

if [[ "${MODE}" == "self-test" ]]; then
  echo "ok: fixture self-test only"
  exit 0
fi

# ── live compose policy ──────────────────────────────────────────────────
if [[ "${MODE}" != "remote" ]]; then
  [[ -f "${ROOT}/docker-compose.yml" ]] || die "missing docker-compose.yml"
  helper policy "${ROOT}" "${ROOT}/docker-compose.yml"
fi

if [[ "${MODE}" == "policy" || "${MODE}" == "default" ]]; then
  echo "ok: compose host-publish policy (no TCP scan)"
  exit 0
fi

# ── TCP scan ─────────────────────────────────────────────────────────────
host_probe() {
  local ip="$1"
  local port="$2"
  local state err
  state="$(helper probe "${ip}" "${port}" | awk '{print $3}')"
  echo "${state}"
}

loopback_metrics_up() {
  local p
  for p in "${METRICS_PORTS[@]}"; do
    [[ "$(host_probe 127.0.0.1 "${p}")" == "open" ]] || return 1
  done
  return 0
}

# Real stack fingerprint: /metrics on each 910N must serve cc_* series.
# The alpine dummy listener accepts TCP but is not Prometheus.
live_stack_fingerprint() {
  local p text
  command -v curl >/dev/null 2>&1 || die "curl is required for --live (cc_* /metrics fingerprint)"
  for p in "${METRICS_PORTS[@]}"; do
    set +e
    text="$(curl -fsS --max-time 2 "http://127.0.0.1:${p}/metrics" 2>/dev/null)"
    set -e
    if [[ -z "${text}" ]] || ! grep -qE '^cc_' <<<"${text}"; then
      echo "error: 127.0.0.1:${p}/metrics is not a live CC process (need cc_* series)" >&2
      return 1
    fi
    echo "live_fingerprint: 127.0.0.1:${p}/metrics has cc_* series"
  done
  return 0
}

if [[ "${MODE}" == "remote" ]]; then
  helper check-remote "${REMOTE}"
  echo "scanner: this host ($(hostname)) is the second host"
  echo "target:  ${REMOTE}"
  fail=0
  for p in "${GRPC_PORTS[@]}" "${METRICS_PORTS[@]}"; do
    line="$(helper probe "${REMOTE}" "${p}")"
    echo "  ${line}"
    if grep -q " open" <<<"${line}"; then
      echo "error: ${REMOTE}:${p} is reachable off-host" >&2
      fail=1
    fi
  done
  if [[ "${fail}" -ne 0 ]]; then
    die "off-host scan found reachable bus/metrics ports on ${REMOTE}"
  fi
  echo "ok: ${REMOTE} answers none of 9001–9006 or 9101–9106 from this host"
  echo "note: --remote does not see the compose host's 127.0.0.1:910N; pair with --live there"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  die "docker is required for the TCP scan (use --policy for the static half)"
fi
if ! docker info >/dev/null 2>&1; then
  die "docker daemon is not running (use --policy for the static half)"
fi

SCAN_MODE="${MODE}"
if [[ "${SCAN_MODE}" == "live" ]]; then
  loopback_metrics_up || die "--live requires 127.0.0.1:9101–9106 open (stack not up)"
  live_stack_fingerprint || die "--live requires cc_* /metrics on 127.0.0.1:9101–9106 (dummy TCP is not the stack)"
elif [[ "${SCAN_MODE}" == "scan" ]]; then
  if loopback_metrics_up; then
    die "--scan cannot bind dummy 910N while the live stack is up; use --live for E0.8"
  fi
  echo "note: --scan is a compose-flag replay onto alpine; it is not E0.8 / not M4=0"
fi

echo "scan_mode: ${SCAN_MODE}"
echo "image:     ${IMAGE}"

docker network create --internal=false "${TARGET_NET}" >/dev/null
docker network create --internal=false "${SCAN_NET}" >/dev/null

# Pick a free canary port on the host.
CANARY=""
for cand in "${CANARY_PORTS[@]}"; do
  if [[ "$(host_probe 127.0.0.1 "${cand}")" == "closed" ]]; then
    CANARY="${cand}"
    break
  fi
done
[[ -n "${CANARY}" ]] || die "no free canary port in ${CANARY_PORTS[*]}"
echo "canary:    0.0.0.0:${CANARY} (path-liveness publish)"

# Listener ports inside the substitute container.
LISTEN_PORTS=("${GRPC_PORTS[@]}" "${METRICS_PORTS[@]}" "${CANARY}")
PUBLISH_ARGS=()
if [[ "${SCAN_MODE}" == "live" ]]; then
  LISTEN_PORTS=("${CANARY}")
  PUBLISH_ARGS+=(-p "0.0.0.0:${CANARY}:${CANARY}")
  docker run -d --name "${CANARY_CTR}" --network "${TARGET_NET}" \
    "${PUBLISH_ARGS[@]}" \
    -v "${WORKDIR}/s0b20.py:/s0b20.py:ro" \
    "${IMAGE}" \
    sh -c "apk add --no-cache python3 >/dev/null && exec python3 /s0b20.py listen ${LISTEN_PORTS[*]}" \
    >/dev/null
  WAIT_CTR="${CANARY_CTR}"
else
  while IFS= read -r flag; do
    [[ -n "${flag}" ]] || continue
    PUBLISH_ARGS+=(-p "${flag}")
  done < <(helper publish-flags "${ROOT}/docker-compose.yml")
  PUBLISH_ARGS+=(-p "0.0.0.0:${CANARY}:${CANARY}")
  echo "publish:   ${PUBLISH_ARGS[*]}"
  docker run -d --name "${LISTEN_CTR}" --network "${TARGET_NET}" \
    "${PUBLISH_ARGS[@]}" \
    -v "${WORKDIR}/s0b20.py:/s0b20.py:ro" \
    "${IMAGE}" \
    sh -c "apk add --no-cache python3 >/dev/null && exec python3 /s0b20.py listen ${LISTEN_PORTS[*]}" \
    >/dev/null
  WAIT_CTR="${LISTEN_CTR}"
fi

# Wait until the listener is ready (apk + bind).
ready=0
for _ in $(seq 1 60); do
  if docker logs "${WAIT_CTR}" 2>&1 | grep -q "s0b20-listen-ready"; then
    ready=1
    break
  fi
  if ! docker inspect -f '{{.State.Running}}' "${WAIT_CTR}" 2>/dev/null | grep -q true; then
    docker logs "${WAIT_CTR}" >&2 || true
    die "listener container exited before becoming ready"
  fi
  sleep 0.5
done
[[ "${ready}" -eq 1 ]] || die "listener did not become ready"

# Host addresses the scanner will target. Exclude docker's host-gateway
# (Docker Desktop may forward 127.0.0.1 publishes there).
HOST_IPS=()
while IFS= read -r _ip; do
  [[ -n "${_ip}" ]] && HOST_IPS+=("${_ip}")
done < <(helper host-ips)
GATEWAY_IP="$(
  docker run --rm --add-host=host.docker.internal:host-gateway "${IMAGE}" \
    sh -c 'getent ahostsv4 host.docker.internal 2>/dev/null || getent hosts host.docker.internal' \
    2>/dev/null | awk '/^[0-9]+\./ {print $1; exit}' || true
)"
TARGETS=()
for ip in "${HOST_IPS[@]}"; do
  if [[ -n "${GATEWAY_IP}" && "${ip}" == "${GATEWAY_IP}" ]]; then
    echo "note: excluding host-gateway ${ip} from off-host targets"
    continue
  fi
  TARGETS+=("${ip}")
done
echo "host_ips:     ${HOST_IPS[*]:-<none>}"
echo "host_gateway: ${GATEWAY_IP:-<none>}"
echo "targets:      ${TARGETS[*]:-<none>}"
[[ ${#TARGETS[@]} -gt 0 ]] || die "no non-loopback host IPv4 to scan from the second netns"

# Scanner on a sibling network — not TARGET_NET, not host netns.
# Install python once, then probe.
SCANNER_IP="$(
  docker run -d --name "${SCAN_CTR}" --network "${SCAN_NET}" \
    --add-host=host.docker.internal:host-gateway \
    -v "${WORKDIR}/s0b20.py:/s0b20.py:ro" \
    "${IMAGE}" \
    sh -c "apk add --no-cache python3 >/dev/null && exec sleep 300" \
    >/dev/null
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${SCAN_CTR}"
)"
# Wait for python.
for _ in $(seq 1 60); do
  if docker exec "${SCAN_CTR}" python3 -c "print(1)" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
docker exec "${SCAN_CTR}" python3 -c "print(1)" >/dev/null 2>&1 \
  || die "scanner python3 never became ready"

echo "scanner_container: ${SCAN_CTR}"
echo "scanner_ip:        ${SCANNER_IP}"
echo "scanner_network:   ${SCAN_NET} (isolated from ${TARGET_NET} and host loopback netns)"
echo "scanner_hostname:  $(docker exec "${SCAN_CTR}" hostname)"
echo

# Canary: at least one off-host target must see the 0.0.0.0 publish.
echo "== canary (must be open off-host, else the scan path is blind) =="
canary_ok=0
CANARY_TARGET=""
for ip in "${TARGETS[@]}"; do
  line="$(docker exec "${SCAN_CTR}" python3 /s0b20.py probe "${ip}" "${CANARY}")"
  echo "  offhost ${line}"
  if grep -q " open" <<<"${line}"; then
    canary_ok=1
    CANARY_TARGET="${ip}"
  fi
done
if [[ "${canary_ok}" -eq 0 ]]; then
  die "canary ${CANARY} not reachable from the scanner on any host IP — cannot claim a closed scan"
fi
echo "ok: scanner on ${SCAN_NET} (${SCANNER_IP}) can see a 0.0.0.0 host publish via ${CANARY_TARGET}:${CANARY}"
echo

echo "== off-host (scanner netns ${SCAN_NET} → host non-loopback) =="
fail=0
for ip in "${TARGETS[@]}"; do
  for p in "${GRPC_PORTS[@]}" "${METRICS_PORTS[@]}"; do
    line="$(docker exec "${SCAN_CTR}" python3 /s0b20.py probe "${ip}" "${p}")"
    echo "  offhost ${line}"
    if grep -q " open" <<<"${line}"; then
      echo "error: ${ip}:${p} is reachable from the scanner netns" >&2
      fail=1
    fi
  done
done

echo
echo "== loopback (host netns; 910N must answer, 900N must not) =="
if [[ "${SCAN_MODE}" == "live" ]] || [[ "${SCAN_MODE}" == "scan" ]] || [[ "${SCAN_MODE}" == "default" ]]; then
  for p in "${METRICS_PORTS[@]}"; do
    line="$(helper probe 127.0.0.1 "${p}")"
    echo "  loopback ${line}"
    if ! grep -q " open" <<<"${line}"; then
      echo "error: 127.0.0.1:${p} did not answer (metrics must be loopback-reachable)" >&2
      fail=1
    fi
  done
  for p in "${GRPC_PORTS[@]}"; do
    line="$(helper probe 127.0.0.1 "${p}")"
    echo "  loopback ${line}"
    if grep -q " open" <<<"${line}"; then
      echo "error: 127.0.0.1:${p} answers; gRPC bus must not be host-published" >&2
      fail=1
    fi
  done
fi

echo
if [[ "${fail}" -ne 0 ]]; then
  if [[ "${SCAN_MODE}" == "live" ]]; then
    die "E0.8 / M4 failed — see probes above"
  fi
  die "compose-flag replay scan failed — see probes above"
fi

if [[ "${SCAN_MODE}" == "live" ]]; then
  echo "verdict: PASS  M4=0  (live compose stack; E0.8)"
  echo "ok: off-host scan from ${SCANNER_IP} on ${SCAN_NET} (not cc, not host loopback netns) reaches none of 9001–9006 or 9101–9106"
  echo "ok: 9101–9106 answer on 127.0.0.1 with cc_* /metrics; 9001–9006 unpublished on the host"
else
  echo "verdict: PASS  compose-flag replay (NOT E0.8 / NOT M4=0)"
  echo "ok: dummy alpine replay of compose -p flags; run --live against a running stack to discharge E0.8"
fi
if [[ -n "${RECORD}" ]]; then
  echo "record: ${RECORD}"
fi
exit 0
