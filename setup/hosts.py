#!/usr/bin/env python3

from pathlib import Path
import socket
import subprocess
import sys


BEGIN_MARKER = "# BEGIN AEGEAN NODE HOSTS"
END_MARKER = "# END AEGEAN NODE HOSTS"
REMOTE_REPO_PATH = "/app"
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


def sync_remote_nodes(mode: str) -> int:
    if mode != "distributed":
        return 0

    source_text = (Path(__file__).resolve().parent / SOURCE_FILES[mode]).read_text()
    script_path = f"{REMOTE_REPO_PATH}/setup/hosts.py"
    failures = []
    for name, hostname in parse_nodes(source_text):
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                f"gjl@{hostname}",
                "python3",
                script_path,
                mode,
                "--local-only",
            ],
            check=False,
        )
        if result.returncode != 0:
            failures.append(name)

    if failures:
        print(
            f"Failed to update remote /etc/hosts on: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1

    print(f"Updated remote /etc/hosts on {len(parse_nodes(source_text))} nodes")
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
        return sync_remote_nodes(mode)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
