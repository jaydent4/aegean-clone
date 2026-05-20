#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time


BEGIN_MARKER = "# BEGIN AEGEAN NODE HOSTS"
END_MARKER = "# END AEGEAN NODE HOSTS"
TARGET = Path("/etc/hosts")
DOCKER_SUBNET_PREFIX = "10.0.0."
DOCKER_BASE_OCTET = 10
SOURCE_FILES = {
    "docker": "docker_nodes",
    "distributed": "distributed_nodes",
}


def parse_nodes(source_text: str) -> list[tuple[str, str]]:
    nodes = []
    for raw_line in source_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        name, value = line.split(None, 1)
        nodes.append((name, value))
    return nodes


def replace_managed_block(existing: str, block: str) -> str:
    begin = existing.find(BEGIN_MARKER)
    end = existing.find(END_MARKER)

    if begin != -1 and end != -1 and end > begin:
        end += len(END_MARKER)
        prefix = existing[:begin].rstrip()
        suffix = existing[end:].lstrip("\n")
        parts = []
        if prefix:
            parts.append(prefix)
        parts.append(block.rstrip())
        if suffix:
            parts.append(suffix.rstrip())
        return "\n\n".join(parts) + "\n"

    existing = existing.rstrip()
    if not existing:
        return block
    return existing + "\n\n" + block


def docker_host_ip(name: str) -> str:
    if not name.startswith("node"):
        raise ValueError(f"unsupported docker node name: {name}")
    index = int(name.removeprefix("node"))
    return f"{DOCKER_SUBNET_PREFIX}{DOCKER_BASE_OCTET + index}"


def resolve_ipv4(hostname: str) -> str:
    try:
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise RuntimeError(f"failed to resolve {hostname}: {exc}") from exc

    for info in infos:
        address = info[4][0]
        if address:
            return address
    raise RuntimeError(f"failed to resolve an IPv4 address for {hostname}")


def render_snippet(mode: str, source_text: str) -> str:
    lines = []
    for name, value in parse_nodes(source_text):
        if mode == "docker":
            host_ip = docker_host_ip(name)
        else:
            host_ip = resolve_ipv4(value)
        lines.append(f"{host_ip} {name}")
    return "\n".join(lines)


def write_hosts(updated: str) -> None:
    result = subprocess.run(
        ["sudo", "tee", str(TARGET)],
        input=updated,
        text=True,
        stdout=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"failed to update {TARGET} via sudo")


def shell_quote(script: str) -> str:
    return shlex.quote(script)


def build_remote_sync_command(block: str) -> str:
    script = f"""
set -euo pipefail
target=/etc/hosts
tmp="$(mktemp)"
cleanup() {{
  rm -f "$tmp"
}}
trap cleanup EXIT

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "sudo is required when not connected as root" >&2
  exit 1
fi

$SUDO awk -v begin={shell_quote(BEGIN_MARKER)} -v end={shell_quote(END_MARKER)} '
  $0 == begin {{ skip = 1; next }}
  $0 == end {{ skip = 0; next }}
  !skip {{ print }}
' "$target" > "$tmp"

if [ -s "$tmp" ]; then
  printf '\\n\\n' >> "$tmp"
fi

cat <<'EOF_BLOCK' >> "$tmp"
{block.rstrip()}
EOF_BLOCK
printf '\\n' >> "$tmp"
$SUDO cp "$tmp" "$target"
"""
    return f"bash -lc {shell_quote(script)}"


def format_output(stdout: str, stderr: str) -> str:
    combined = "\n".join(part.strip() for part in (stdout, stderr) if part.strip()).strip()
    if not combined:
        return "(no output)"
    lines = combined.splitlines()
    if len(lines) <= 12:
        return combined
    return "\n".join(lines[-12:])


@dataclass(frozen=True)
class RemoteSyncResult:
    name: str
    result: subprocess.CompletedProcess[str]
    elapsed_sec: float


def sync_remote_node(name: str, hostname: str, remote_command: str) -> RemoteSyncResult:
    started_at = time.monotonic()
    result = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            f"gjl@{hostname}",
            remote_command,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    return RemoteSyncResult(name, result, time.monotonic() - started_at)


def sync_remote_nodes(mode: str, source_text: str, block: str) -> int:
    if mode != "distributed":
        return 0

    remote_command = build_remote_sync_command(block)
    failures: list[str] = []
    nodes = parse_nodes(source_text)
    if not nodes:
        print("Updated remote /etc/hosts on 0 nodes")
        return 0

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {
            executor.submit(sync_remote_node, name, hostname, remote_command): name
            for name, hostname in nodes
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                remote_result = future.result()
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{name}: hosts update failed unexpectedly\n{exc}")
                print(f"{name}: failed unexpectedly")
                continue

            result = remote_result.result
            if result.returncode == 0:
                print(f"{remote_result.name}: updated /etc/hosts ({remote_result.elapsed_sec:.1f}s)")
                continue

            failures.append(
                f"{remote_result.name}: hosts update failed (exit {result.returncode})\n"
                f"{format_output(result.stdout, result.stderr)}"
            )
            print(f"{remote_result.name}: failed ({remote_result.elapsed_sec:.1f}s)")

    if failures:
        print(
            "\n\n".join(failures),
            file=sys.stderr,
        )
        return 1

    print(f"Updated remote /etc/hosts on {len(nodes)} nodes")
    return 0


def main() -> int:
    args = sys.argv[1:]
    if not args or args[0] not in {"docker", "distributed"}:
        print(
            "usage: python3 setup/hosts.py [docker|distributed] [--local-only]",
            file=sys.stderr,
        )
        return 1

    mode = args[0]
    local_only = False
    for arg in args[1:]:
        if arg == "--local-only":
            local_only = True
        else:
            print(f"unknown argument: {arg}", file=sys.stderr)
            return 1

    script_dir = Path(__file__).resolve().parent
    source = script_dir / SOURCE_FILES[mode]
    source_text = source.read_text()

    try:
        snippet = render_snippet(mode, source_text)
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    block = f"{BEGIN_MARKER}\n{snippet}\n{END_MARKER}\n"
    existing = TARGET.read_text()
    updated = replace_managed_block(existing, block)

    try:
        write_hosts(updated)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Updated {TARGET} with {mode} config")

    if not local_only:
        return sync_remote_nodes(mode, source_text, block)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
