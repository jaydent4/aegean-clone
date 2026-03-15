#!/usr/bin/env python3

import argparse
import shlex
import subprocess
import sys


REPO_URL = "https://github.com/ucla-progsoftsys/aegean-clone.git"


def shell_quote(script: str) -> str:
    return shlex.quote(script)


def build_remote_command() -> str:
    body = f"""
set -euo pipefail
if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "sudo is required when not connected as root" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is not installed" >&2
  exit 1
fi

if [ -d /app/.git ]; then
  remote_url="$($SUDO git -C /app remote get-url origin)"
  if [ "$remote_url" != "{REPO_URL}" ]; then
    echo "/app exists but origin is $remote_url, expected {REPO_URL}" >&2
    exit 1
  fi
  $SUDO git -C /app pull --ff-only
elif [ -e /app ]; then
  echo "/app exists but is not a git repository" >&2
  exit 1
else
  parent_dir="$($SUDO dirname /app)"
  $SUDO mkdir -p "$parent_dir"
  $SUDO git clone {shell_quote(REPO_URL)} /app
fi
"""
    return body


def run_host(host: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            host,
            "bash",
            "-lc",
            build_remote_command(),
        ],
        text=True,
        capture_output=True,
    )


def format_output(stdout: str, stderr: str) -> str:
    combined = "\n".join(part.strip() for part in (stdout, stderr) if part.strip()).strip()
    if not combined:
        return "(no output)"
    lines = combined.splitlines()
    if len(lines) <= 12:
        return combined
    return "\n".join(lines[-12:])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ensure /app on node1..nodeN is the aegean-clone repo, pulling or cloning as needed."
    )
    parser.add_argument("count", type=int, help="Operate on node1 through node<count>.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        print("count must be at least 1", file=sys.stderr)
        return 2

    failures: list[str] = []
    for idx in range(1, args.count + 1):
        host = f"node{idx}"
        result = run_host(host)
        if result.returncode == 0:
            print(f"{host}: repo ready at /app")
            continue
        failures.append(
            f"{host}: repo setup failed (exit {result.returncode})\n{format_output(result.stdout, result.stderr)}"
        )

    if failures:
        print("\n\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
