#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import shlex
import subprocess
import sys
import time


BEGIN_MARKER = "# BEGIN AEGEAN NODE CONFIG"
END_MARKER = "# END AEGEAN NODE CONFIG"
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


def render_snippet(mode: str, source_text: str) -> str:
    blocks = []
    for name, value in parse_nodes(source_text):
        if mode == "docker":
            blocks.append(
                "\n".join(
                    [
                        f"Host {name}",
                        "  HostName localhost",
                        f"  Port {value}",
                        "  User root",
                        "  StrictHostKeyChecking no",
                        "  UserKnownHostsFile /dev/null",
                    ]
                )
            )
        else:
            blocks.append(
                "\n".join(
                    [
                        f"Host {name}",
                        f"  HostName {value}",
                        "  User gjl",
                        "  StrictHostKeyChecking no",
                        "  UserKnownHostsFile /dev/null",
                    ]
                )
            )
    return "\n\n".join(blocks)


def shell_quote(script: str) -> str:
    return shlex.quote(script)


def build_remote_sync_command(block: str) -> str:
    script = f"""
set -euo pipefail
target="$HOME/.ssh/config"
mkdir -p "$(dirname "$target")"
tmp="$(mktemp)"
cleanup() {{
  rm -f "$tmp"
}}
trap cleanup EXIT

if [ -f "$target" ]; then
  awk -v begin={shell_quote(BEGIN_MARKER)} -v end={shell_quote(END_MARKER)} '
    $0 == begin {{ skip = 1; next }}
    $0 == end {{ skip = 0; next }}
    !skip {{ print }}
  ' "$target" > "$tmp"
else
  : > "$tmp"
fi

if [ -s "$tmp" ]; then
  printf '\\n\\n' >> "$tmp"
fi

cat <<'EOF_BLOCK' >> "$tmp"
{block.rstrip()}
EOF_BLOCK
printf '\\n' >> "$tmp"
mv "$tmp" "$target"
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


def sync_remote_nodes(mode: str, source_text: str) -> int:
    if mode != "distributed":
        return 0

    snippet = render_snippet(mode, source_text)
    block = f"{BEGIN_MARKER}\n{snippet}\n{END_MARKER}\n"
    remote_command = build_remote_sync_command(block)
    failures: list[str] = []
    nodes = parse_nodes(source_text)
    if not nodes:
        print("Updated remote ~/.ssh/config on 0 nodes")
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
                failures.append(f"{name}: ssh config update failed unexpectedly\n{exc}")
                print(f"{name}: failed unexpectedly")
                continue

            result = remote_result.result
            if result.returncode == 0:
                print(f"{remote_result.name}: updated ~/.ssh/config ({remote_result.elapsed_sec:.1f}s)")
                continue

            failures.append(
                f"{remote_result.name}: ssh config update failed (exit {result.returncode})\n"
                f"{format_output(result.stdout, result.stderr)}"
            )
            print(f"{remote_result.name}: failed ({remote_result.elapsed_sec:.1f}s)")

    if failures:
        print(
            "\n\n".join(failures),
            file=sys.stderr,
        )
        return 1

    print(f"Updated remote ~/.ssh/config on {len(nodes)} nodes")
    return 0


def main() -> int:
    args = sys.argv[1:]
    if not args or args[0] not in {"docker", "distributed"}:
        print(
            "usage: python3 setup/ssh_config.py [docker|distributed] [--local-only]",
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
    target = Path.home() / ".ssh" / "config"
    source_text = source.read_text()

    snippet = render_snippet(mode, source_text)
    block = f"{BEGIN_MARKER}\n{snippet}\n{END_MARKER}\n"
    existing = target.read_text() if target.exists() else ""
    updated = replace_managed_block(existing, block)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(updated)
    print(f"Updated {target} with {mode} config")

    if not local_only:
        return sync_remote_nodes(mode, source_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
