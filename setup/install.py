#!/usr/bin/env python3

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass


K6_VERSION = "v1.2.3"
GO_VERSION = "1.23.12"


@dataclass(frozen=True)
class Step:
    name: str
    command: str


STEPS = [
    Step(
        "apt proxy config",
        r"""mkdir -p /etc/apt/apt.conf.d && cat > /etc/apt/apt.conf.d/99fixbadproxy <<'EOF'
Acquire::http::Pipeline-Depth 0;
Acquire::http::No-Cache true;
Acquire::BrokenProxy true;
EOF""",
    ),
    Step("apt update", "apt-get update"),
    Step(
        "system packages",
        "DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server ca-certificates wget tar",
    ),
    Step(
        "install oha",
        r"""arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64) asset="oha-linux-amd64" ;;
  arm64) asset="oha-linux-arm64" ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac
wget -O /usr/local/bin/oha "https://github.com/hatoo/oha/releases/latest/download/${asset}"
chmod +x /usr/local/bin/oha
oha --version""",
    ),
    Step(
        "install k6",
        rf"""arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64) asset="k6-{K6_VERSION}-linux-amd64.tar.gz" ;;
  arm64) asset="k6-{K6_VERSION}-linux-arm64.tar.gz" ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac
wget -O /tmp/k6.tar.gz "https://github.com/grafana/k6/releases/download/{K6_VERSION}/${{asset}}"
tar -xzf /tmp/k6.tar.gz -C /tmp
install -m 0755 "/tmp/${{asset%.tar.gz}}/k6" /usr/local/bin/k6
rm -rf /tmp/k6.tar.gz "/tmp/${{asset%.tar.gz}}"
k6 version""",
    ),
    Step(
        "install go",
        rf"""arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64) goarch="amd64" ;;
  arm64) goarch="arm64" ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac
asset="go{GO_VERSION}.linux-${{goarch}}.tar.gz"
wget -O /tmp/go.tar.gz "https://go.dev/dl/${{asset}}"
rm -rf /usr/local/go
tar -C /usr/local -xzf /tmp/go.tar.gz
rm -f /tmp/go.tar.gz
/usr/local/go/bin/go version""",
    ),
    Step("go symlink", "ln -sf /usr/local/go/bin/go /usr/bin/go"),
    Step(
        "configure ssh",
        r"""mkdir -p /run/sshd /root/.ssh
sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PermitEmptyPasswords .*/PermitEmptyPasswords yes/' /etc/ssh/sshd_config
sed -i 's/^#\?UsePAM .*/UsePAM no/' /etc/ssh/sshd_config
ssh-keygen -A""",
    ),
    Step("unlock root", "passwd -d root && passwd -u root || true"),
    Step("app directory", "mkdir -p /app"),
]


def shell_quote(script: str) -> str:
    return shlex.quote(script)


def build_remote_command(step: Step) -> str:
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
$SUDO /bin/bash -lc {shell_quote(step.command)}
"""
    return body


def run_step(host: str, step: Step) -> subprocess.CompletedProcess[str]:
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
            build_remote_command(step),
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


def install_host(host: str) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for step in STEPS:
        result = run_step(host, step)
        if result.returncode == 0:
            continue
        failures.append(
            f"{host}: {step.name} failed (exit {result.returncode})\n{format_output(result.stdout, result.stderr)}"
        )
    return (len(failures) == 0, failures)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install the packages and SSH setup from docker/Dockerfile onto node0..node<count-1> over SSH."
    )
    parser.add_argument("count", type=int, help="Install on node0 through node<count-1>.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        print("count must be at least 1", file=sys.stderr)
        return 2

    failures: list[str] = []
    for idx in range(args.count):
        host = f"node{idx}"
        success, host_failures = install_host(host)
        failures.extend(host_failures)
        if success:
            print(f"{host}: successfully installed all steps")

    if failures:
        print("\n\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
