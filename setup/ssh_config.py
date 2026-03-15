#!/usr/bin/env python3

from pathlib import Path
import subprocess
import sys


BEGIN_MARKER = "# BEGIN AEGEAN NODE CONFIG"
END_MARKER = "# END AEGEAN NODE CONFIG"
REMOTE_REPO_PATH = "/app"


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


def sync_remote_nodes(mode: str, source_text: str) -> int:
    if mode != "distributed":
        return 0

    script_path = f"{REMOTE_REPO_PATH}/setup/ssh_config.py"
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
            f"Failed to update remote ~/.ssh/config on: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1

    print(f"Updated remote ~/.ssh/config on {len(parse_nodes(source_text))} nodes")
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
    source = script_dir / f"{mode}_ssh_config"
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
