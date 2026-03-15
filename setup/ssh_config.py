#!/usr/bin/env python3

from pathlib import Path
import sys


BEGIN_MARKER = "# BEGIN AEGEAN NODE CONFIG"
END_MARKER = "# END AEGEAN NODE CONFIG"


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
    for line in source_text.splitlines():
        line = line.strip()
        if not line:
            continue
        name, value = line.split(None, 1)
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


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"docker", "distributed"}:
        print("usage: python3 setup/ssh_config.py [docker|distributed]", file=sys.stderr)
        return 1

    mode = sys.argv[1]
    script_dir = Path(__file__).resolve().parent
    source = script_dir / f"{mode}_ssh_config"
    target = Path.home() / ".ssh" / "config"

    snippet = render_snippet(mode, source.read_text())
    block = f"{BEGIN_MARKER}\n{snippet}\n{END_MARKER}\n"
    existing = target.read_text() if target.exists() else ""
    updated = replace_managed_block(existing, block)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(updated)
    print(f"Updated {target} with {mode} config")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
