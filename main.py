import argparse
import json
import logging
import os
import glob
import shlex
import subprocess
import time

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
REMOTE_RPC_SCRIPT = "/app/remote_rpc.py"


def resolve_run_config_paths(run_config_path):
    resolved_run_config_path = os.path.abspath(run_config_path)
    relative_run_config_path = os.path.relpath(resolved_run_config_path, REPO_ROOT)

    runs_dir_name = "runs"
    current_dir = os.path.dirname(resolved_run_config_path)
    while True:
        if os.path.basename(current_dir) == runs_dir_name:
            experiment_dir = os.path.dirname(current_dir)
            architecture_dir = os.path.join(experiment_dir, "architecture")
            if os.path.isdir(architecture_dir):
                return resolved_run_config_path, relative_run_config_path, architecture_dir

        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            break
        current_dir = parent_dir

    raise ValueError(
        f"could not resolve experiment architecture directory for run config: {run_config_path}"
    )


def load_run_config(run_config_path):
    resolved_run_config_path, relative_run_config_path, architecture_dir = resolve_run_config_paths(
        run_config_path
    )

    with open(resolved_run_config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    architecture = data.get("architecture")
    if not architecture:
        raise ValueError("run config must include non-empty 'architecture'")

    run_timeout_seconds = data.get("run_timeout_seconds")
    if not isinstance(run_timeout_seconds, int) or run_timeout_seconds <= 0:
        raise ValueError("run config must include positive integer 'run_timeout_seconds'")

    architecture_path = os.path.normpath(os.path.join(architecture_dir, architecture))
    return resolved_run_config_path, relative_run_config_path, architecture_path, data


def load_experiment_topology(architecture_path):
    with open(architecture_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    services = data.get("services", {})
    nodes = data.get("nodes", {})
    if not services or not nodes:
        raise ValueError("config must include non-empty 'services' and 'nodes'")

    node_names = sorted(nodes.keys())
    client_names = []
    for node_name, node_cfg in nodes.items():
        service_name = node_cfg.get("service")
        if not service_name:
            raise ValueError(f"node {node_name} is missing 'service'")

        service_cfg = services.get(service_name)
        if not service_cfg:
            raise ValueError(f"node {node_name} references unknown service '{service_name}'")

        service_type = service_cfg.get("type")
        if service_type == "client":
            client_names.append(node_name)

    return node_names, sorted(client_names)


def _scp(node_name, remote_path, local_path):
    return subprocess.run(["scp", *_ssh_options(), f"{node_name}:{remote_path}", local_path], check=False)


def list_run_config_paths(runs_dir=None):
    if runs_dir is None:
        runs_dir = os.path.join(REPO_ROOT, "experiment", "runs")

    patterns = [
        os.path.join(runs_dir, "*.json"),
        os.path.join(runs_dir, "*", "*.json"),
    ]

    run_config_paths = []
    for pattern in patterns:
        run_config_paths.extend(glob.glob(pattern))

    return sorted(os.path.abspath(path) for path in run_config_paths)


def create_results_run_dir(relative_run_config_path, results_dir="results"):
    results_root = os.path.join(REPO_ROOT, results_dir)
    run_config_relpath = os.path.relpath(
        os.path.abspath(os.path.join(REPO_ROOT, relative_run_config_path)),
        os.path.join(REPO_ROOT, "experiment", "runs"),
    )
    run_dir = os.path.join(results_root, os.path.splitext(run_config_relpath)[0])
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def collect_logs(run_dir, node_names, client_names):
    logger.info("Collecting logs (%d nodes, %d clients)", len(node_names), len(client_names))

    for name in node_names:
        local_path = os.path.join(run_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)
    logger.info("Log collection complete: %s", run_dir)


def _ssh_options():
    return [
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
    ]


def _ssh(node_name, remote_args, **kwargs):
    return subprocess.run(
        ["ssh", *_ssh_options(), node_name, *remote_args],
        check=False,
        **kwargs,
    )


def _ssh_shell(node_name, command, **kwargs):
    remote_command = f"bash -lc {shlex.quote(command)}"
    return subprocess.run(
        ["ssh", *_ssh_options(), node_name, remote_command],
        check=False,
        **kwargs,
    )


def _remote_rpc(node_name, path, payload=None, timeout=5):
    if payload is None:
        payload = {}

    result = _ssh(
        node_name,
        [
            "python3",
            REMOTE_RPC_SCRIPT,
            path,
            json.dumps(payload),
            str(timeout),
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 2,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"ssh/python failed for {node_name}")

    envelope = json.loads(result.stdout or "{}")
    return envelope.get("payload", {})


def launch_nodes(node_names, config_path):
    logger.info("Launching %d nodes", len(node_names))
    remote_config_path = shlex.quote(f"../{config_path}")
    for name in node_names:
        _ssh_shell(
            name,
            (
                "cd /app/src || exit 1; "
                f"nohup go run . --name {shlex.quote(name)} "
                "--host 0.0.0.0 "
                "--port 8000 "
                f"--config {remote_config_path} "
                "> /tmp/node.log 2>&1 &"
            ),
        )

def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes", len(node_names))
    for name in node_names:
        subprocess.run(["ssh", name, "pkill", "-9", "-f", "go"])


def get_node_ready(node_name):
    payload = _remote_rpc(node_name, "/ready", timeout=5)
    return bool(payload.get("ready", False))


def wait_for_nodes_ready(node_names, timeout=30.0, poll_interval=1.0):
    if not node_names:
        return True

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        all_ready = True
        for name in node_names:
            try:
                if not get_node_ready(name):
                    all_ready = False
            except Exception as exc:  # noqa: BLE001
                logger.info("Node %s not ready yet: %s", name, exc)
                all_ready = False

        if all_ready:
            return True
        time.sleep(poll_interval)

    return False


def run_experiment(config_path):
    _, relative_run_config_path, architecture_path, run_config = load_run_config(config_path)
    node_names, client_names = load_experiment_topology(architecture_path)
    run_dir = create_results_run_dir(relative_run_config_path)
    run_timeout_seconds = run_config["run_timeout_seconds"]

    logger.info("Experiment starting: %s", relative_run_config_path)
    stop_docker_nodes(node_names)

    launch_nodes(node_names, relative_run_config_path)
    logger.info("Waiting for all nodes to become ready")
    all_nodes_ready = wait_for_nodes_ready(node_names, timeout=120.0, poll_interval=1.0)
    if not all_nodes_ready:
        logger.warning("Node readiness timeout after 120s; proceeding anyway")

    logger.info("Waiting for run timeout: %ss", run_timeout_seconds)
    run_start = time.monotonic()
    time.sleep(run_timeout_seconds)
    run_duration_seconds = max(0.0, time.monotonic() - run_start)
    logger.info("Run timeout reached after %.2fs", run_duration_seconds)

    stop_docker_nodes(node_names)
    collect_logs(run_dir, node_names, client_names)
    logger.info("Experiment complete: %s -> %s", relative_run_config_path, run_dir)


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment")
    parser.add_argument("config_path", nargs="?", help="Path to run config JSON")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all configs under experiment/runs and experiment/runs/*",
    )
    args = parser.parse_args()

    if args.all:
        if args.config_path:
            parser.error("config_path cannot be used with --all")
        config_paths = list_run_config_paths()
        if not config_paths:
            parser.error("no run configs found under experiment/runs")
        for config_path in config_paths:
            run_experiment(config_path)
        return

    if not args.config_path:
        parser.error("config_path is required unless --all is used")

    run_experiment(args.config_path)


if __name__ == "__main__":
    main()
