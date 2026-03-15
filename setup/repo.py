#!/usr/bin/env python3

import argparse
from pathlib import Path
import shlex
import subprocess
import sys


REPO_URL = "https://github.com/ucla-progsoftsys/aegean-clone.git"
REPO_ROOT = Path(__file__).resolve().parent.parent


def shell_quote(script: str) -> str:
    return shlex.quote(script)


def build_git_command() -> str:
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

parent_dir="$($SUDO dirname /app)"
$SUDO mkdir -p "$parent_dir"
if [ -e /app ]; then
  $SUDO rm -rf /app
fi
$SUDO git clone {shell_quote(REPO_URL)} /app
"""
    return body


def build_upload_command() -> str:
    body = """
set -euo pipefail
if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "sudo is required when not connected as root" >&2
  exit 1
fi

parent_dir="$($SUDO dirname /app)"
$SUDO mkdir -p "$parent_dir"
if [ -e /app ]; then
  $SUDO rm -rf /app
fi
$SUDO mkdir -p /app
$SUDO tar -xf - -C /app
"""
    return body


def run_host_git(host: str) -> subprocess.CompletedProcess[str]:
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
            build_git_command(),
        ],
        text=True,
        capture_output=True,
    )


def run_host_upload(host: str) -> subprocess.CompletedProcess[str]:
    tar_proc = subprocess.Popen(
        ["tar", "-cf", "-", "."],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    try:
        ssh_proc = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                host,
                "bash",
                "-lc",
                build_upload_command(),
            ],
            stdin=tar_proc.stdout,
            capture_output=True,
        )
    finally:
        if tar_proc.stdout is not None:
            tar_proc.stdout.close()
    tar_stderr = tar_proc.communicate()[1]
    if tar_proc.returncode != 0:
        stderr = tar_stderr.decode() if tar_stderr else "local tar command failed"
        return subprocess.CompletedProcess(ssh_proc.args if "ssh_proc" in locals() else ["ssh", host], 1, "", stderr)
    return subprocess.CompletedProcess(
        ssh_proc.args,
        ssh_proc.returncode,
        ssh_proc.stdout.decode(),
        ((tar_stderr.decode() if tar_stderr else "") + ssh_proc.stderr.decode()),
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
        description="Populate /app on node0..node<count-1> either from GitHub or by uploading the current local checkout."
    )
    parser.add_argument("count", type=int, help="Operate on node0 through node<count-1>.")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Replace /app with this local repo checkout instead of doing a fresh GitHub clone.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        print("count must be at least 1", file=sys.stderr)
        return 2

    failures: list[str] = []
    for idx in range(args.count):
        host = f"node{idx}"
        result = run_host_upload(host) if args.upload else run_host_git(host)
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
