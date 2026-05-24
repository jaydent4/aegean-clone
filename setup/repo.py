#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import time
from typing import Optional


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
$SUDO chown -R "$(id -un):$(id -gn)" /app
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
preserve_dir="$(mktemp -d /tmp/aegean-bin.XXXXXX)"
trap '$SUDO rm -rf "$preserve_dir"' EXIT
if [ -d /app/bin ]; then
  $SUDO cp -a /app/bin "$preserve_dir/bin"
fi
if [ -e /app ]; then
  $SUDO rm -rf /app
fi
$SUDO mkdir -p /app
$SUDO tar -xf - -C /app
if [ -d "$preserve_dir/bin" ]; then
  $SUDO rm -rf /app/bin
  $SUDO cp -a "$preserve_dir/bin" /app/bin
fi
$SUDO chown -R "$(id -un):$(id -gn)" /app
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


def list_upload_paths() -> subprocess.CompletedProcess[list[Path]]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    if result.returncode != 0:
        return subprocess.CompletedProcess(
            result.args,
            result.returncode,
            [],
            result.stderr.decode(),
        )
    paths = [
        Path(raw.decode())
        for raw in result.stdout.split(b"\0")
        if raw
    ]
    return subprocess.CompletedProcess(result.args, 0, paths, "")


def run_host_upload(host: str, upload_paths: list[Path]) -> subprocess.CompletedProcess[str]:
    ssh_proc = subprocess.Popen(
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
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_error = ""
    try:
        assert ssh_proc.stdin is not None
        with tarfile.open(fileobj=ssh_proc.stdin, mode="w|") as archive:
            for rel_path in upload_paths:
                archive.add(REPO_ROOT / rel_path, arcname=rel_path)
    except (BrokenPipeError, OSError, tarfile.TarError) as exc:
        tar_error = f"local tar creation failed: {exc}\n"
    finally:
        if ssh_proc.stdin is not None:
            try:
                ssh_proc.stdin.close()
            except BrokenPipeError as exc:
                if not tar_error:
                    tar_error = f"local tar creation failed: {exc}\n"
            ssh_proc.stdin = None

    stdout, stderr = ssh_proc.communicate()
    if tar_error:
        return subprocess.CompletedProcess(
            ssh_proc.args,
            1,
            stdout.decode(),
            tar_error + stderr.decode(),
        )
    return subprocess.CompletedProcess(
        ssh_proc.args,
        ssh_proc.returncode,
        stdout.decode(),
        stderr.decode(),
    )


def format_output(stdout: str, stderr: str) -> str:
    combined = "\n".join(part.strip() for part in (stdout, stderr) if part.strip()).strip()
    if not combined:
        return "(no output)"
    lines = combined.splitlines()
    if len(lines) <= 12:
        return combined
    return "\n".join(lines[-12:])


@dataclass(frozen=True)
class HostRepoResult:
    host: str
    result: subprocess.CompletedProcess[str]
    elapsed_sec: float


def setup_host(host: str, upload_paths: Optional[list[Path]]) -> HostRepoResult:
    started_at = time.monotonic()
    result = (
        run_host_upload(host, upload_paths)
        if upload_paths is not None
        else run_host_git(host)
    )
    return HostRepoResult(
        host=host,
        result=result,
        elapsed_sec=time.monotonic() - started_at,
    )


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
    hosts = [f"node{idx}" for idx in range(args.count)]
    upload_paths = None

    if args.upload:
        upload_result = list_upload_paths()
        if upload_result.returncode != 0:
            print(format_output("", upload_result.stderr), file=sys.stderr)
            return 1
        upload_paths = upload_result.stdout

    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {executor.submit(setup_host, host, upload_paths): host for host in hosts}
        for future in as_completed(futures):
            host = futures[future]
            try:
                host_result = future.result()
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{host}: repo setup failed unexpectedly\n{exc}")
                print(f"{host}: failed unexpectedly")
                continue

            result = host_result.result
            if result.returncode == 0:
                print(f"{host_result.host}: repo ready at /app ({host_result.elapsed_sec:.1f}s)")
                continue

            failures.append(
                f"{host_result.host}: repo setup failed (exit {result.returncode})\n{format_output(result.stdout, result.stderr)}"
            )
            print(f"{host_result.host}: failed ({host_result.elapsed_sec:.1f}s)")

    if failures:
        print("\n\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
