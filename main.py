import argparse
import json
import logging
import os
import shlex
import subprocess
import time
from datetime import datetime

from tqdm import tqdm

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_run_config(run_config_path):
    with open(run_config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    architecture = data.get("architecture")
    if not architecture:
        raise ValueError("run config must include non-empty 'architecture'")

    base_dir = os.path.dirname(run_config_path) or "."
    architecture_path = os.path.normpath(os.path.join(base_dir, "../architecture", architecture))
    return architecture_path, data


def load_experiment_topology(architecture_path):
    with open(architecture_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    services = data.get("services", {})
    nodes = data.get("nodes", {})
    if not services or not nodes:
        raise ValueError("config must include non-empty 'services' and 'nodes'")

    node_names = sorted(nodes.keys())
    client_names = []
    pprof_nodes = []
    for node_name, node_cfg in nodes.items():
        service_name = node_cfg.get("service")
        if not service_name:
            raise ValueError(f"node {node_name} is missing 'service'")

        service_cfg = services.get(service_name)
        if not service_cfg:
            raise ValueError(f"node {node_name} references unknown service '{service_name}'")

        service_type = service_cfg.get("type")
        if service_type in {"client", "oha_client", "k6_client"}:
            client_names.append(node_name)
        if service_type in {"server", "external_service"}:
            pprof_nodes.append(node_name)

    return node_names, sorted(client_names), sorted(pprof_nodes)


def _scp(node_name, remote_path, local_path):
    return subprocess.run(["scp", f"{node_name}:{remote_path}", local_path], check=False)


def create_results_run_dir(results_dir="results"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(results_dir, timestamp)
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def collect_logs(run_dir, node_names, client_names):
    logger.info("Collecting logs (%d nodes, %d clients)", len(node_names), len(client_names))

    logs_dir = os.path.join(run_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    for name in node_names:
        local_path = os.path.join(logs_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)

    traces_dir = os.path.join(run_dir, "traces")
    os.makedirs(traces_dir, exist_ok=True)
    for name in client_names:
        local_path = os.path.join(traces_dir, f"{name}.jsonl")
        _scp(name, "/tmp/client_result.jsonl", local_path)
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

def launch_nodes(node_names, config_path):
    logger.info("Launching %d nodes", len(node_names))
    remote_config_path = f"../{config_path}"
    quoted_config_path = shlex.quote(remote_config_path)
    for name in node_names:
        subprocess.run(["ssh", name, "sh", "-lc",
                        "'cd /app/src && "
                        f"nohup go run . --name {name} --host 0.0.0.0 --port 8000 --config {quoted_config_path} "
                        "> /tmp/node.log 2>&1 &'"])

def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes", len(node_names))
    for name in node_names:
        subprocess.run(["ssh", name, "pkill -9 -f 'go'"])


def get_node_ready(node_name):
    result = _ssh(
        node_name,
        ["curl", "-sS", "-X", "POST", "http://127.0.0.1:8000/ready"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"ssh/curl failed for {node_name}")

    payload = json.loads(result.stdout or "{}")
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


def get_client_progress(client_name):
    result = _ssh(
        client_name,
        ["curl", "-sS", "-X", "POST", "http://127.0.0.1:8000/progress"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"ssh/curl failed for {client_name}")

    payload = json.loads(result.stdout or "{}")
    progress = float(payload.get("progress", 0.0))
    finished = bool(payload.get("finished", False))
    disable_progress_timeout = bool(payload.get("disableProgressTimeout", False))
    return progress, finished, disable_progress_timeout


def wait_for_clients_progress(client_names, poll_interval=1.0, stall_timeout=30.0):
    if not client_names:
        return True

    prev_progress = 0.0
    prev_progress_time = time.monotonic()
    progress_bar = tqdm(total=float(len(client_names)), desc="Clients")

    try:
        while True:
            progress_sum = 0
            all_finished = True
            disable_progress_timeout = False
            for name in client_names:
                try:
                    progress, finished, disable_timeout = get_client_progress(name)
                    progress_sum += progress
                    all_finished = all_finished and finished
                    disable_progress_timeout = disable_progress_timeout or disable_timeout
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to read progress from %s: %s", name, exc)
                    all_finished = False

            progress_bar.update(progress_sum - prev_progress)

            if all_finished:
                progress_bar.close()
                return True

            if progress_sum != prev_progress:
                prev_progress = progress_sum
                prev_progress_time = time.monotonic()
            elif not disable_progress_timeout and time.monotonic() - prev_progress_time > stall_timeout:
                logger.warning(
                    "Progress stalled for over %.1fs (total_progress=%.3f)",
                    stall_timeout,
                    progress_sum,
                )
                return False

            time.sleep(poll_interval)
    finally:
        progress_bar.close()


def start_remote_cpu_profiles(pprof_nodes, cpu_seconds):
    if not pprof_nodes or cpu_seconds <= 0:
        return {}

    logger.info(
        "Starting remote CPU profiles on %d nodes for %ds",
        len(pprof_nodes),
        cpu_seconds,
    )
    commands = {}
    for node_name in pprof_nodes:
        proc = subprocess.Popen(
            [
                "ssh",
                *_ssh_options(),
                node_name,
                "curl",
                "-sS",
                f"http://127.0.0.1:8000/debug/pprof/profile?seconds={cpu_seconds}",
                "-o",
                f"/tmp/{node_name}-cpu.pb.gz",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        commands[node_name] = {
            "proc": proc,
            "stopped_early": False,
        }
    return commands


def stop_remote_cpu_profiles(remote_cpu_commands):
    if not remote_cpu_commands:
        return

    logger.info("Stopping remote CPU profiles")
    for node_name, command in remote_cpu_commands.items():
        proc = command["proc"]
        if proc.poll() is None:
            command["stopped_early"] = True
            proc.terminate()
        else:
            logger.debug("CPU profile already finished for %s", node_name)


def wait_remote_cpu_profiles(remote_cpu_commands, timeout_seconds):
    if not remote_cpu_commands:
        return

    deadline = time.time() + timeout_seconds
    for node_name, command in remote_cpu_commands.items():
        proc = command["proc"]
        stopped_early = command["stopped_early"]
        timeout_left = max(1.0, deadline - time.time())
        try:
            _, stderr = proc.communicate(timeout=timeout_left)
            if proc.returncode != 0:
                message = (stderr or "").strip()
                if stopped_early:
                    logger.info("CPU profile for %s stopped after experiment completion", node_name)
                else:
                    logger.warning("CPU profile collection failed for %s: %s", node_name, message)
        except subprocess.TimeoutExpired:
            proc.kill()
            logger.warning("CPU profile collection timed out for %s", node_name)


def capture_remote_snapshot_profiles(pprof_nodes):
    if not pprof_nodes:
        return

    snapshot_profile_names = ["heap", "allocs", "goroutine", "mutex", "block", "threadcreate"]
    logger.info("Capturing snapshot pprof profiles on %d nodes", len(pprof_nodes))
    for node_name in pprof_nodes:
        for profile_name in snapshot_profile_names:
            _ssh(
                node_name,
                [
                    "curl",
                    "-sS",
                    f"http://127.0.0.1:8000/debug/pprof/{profile_name}",
                    "-o",
                    f"/tmp/{node_name}-{profile_name}.pb.gz",
                ],
            )
        _ssh(
            node_name,
            [
                "curl",
                "-sS",
                "http://127.0.0.1:8000/debug/pprof/goroutine?debug=1",
                "-o",
                f"/tmp/{node_name}-goroutine.txt",
            ],
        )


def collect_pprof(run_dir, pprof_nodes):
    if not pprof_nodes:
        return

    pprof_dir = os.path.join(run_dir, "pprof")
    os.makedirs(pprof_dir, exist_ok=True)

    logger.info("Copying pprof artifacts from %d nodes", len(pprof_nodes))
    suffixes = [
        "cpu.pb.gz",
        "heap.pb.gz",
        "allocs.pb.gz",
        "goroutine.pb.gz",
        "goroutine.txt",
        "mutex.pb.gz",
        "block.pb.gz",
        "threadcreate.pb.gz",
    ]
    for node_name in pprof_nodes:
        for suffix in suffixes:
            _scp(node_name, f"/tmp/{node_name}-{suffix}", os.path.join(pprof_dir, f"{node_name}-{suffix}"))


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment")
    parser.add_argument("config_path", help="Path to run config JSON")
    args = parser.parse_args()

    architecture_path, run_config = load_run_config(args.config_path)
    node_names, client_names, pprof_nodes = load_experiment_topology(architecture_path)
    run_dir = create_results_run_dir()

    cpu_profile_max_seconds = int(
        run_config.get("pprof_cpu_max_seconds", run_config.get("pprof_cpu_seconds", 24 * 60 * 60))
    )
    cpu_profile_wait_slack_seconds = int(run_config.get("pprof_cpu_wait_slack_seconds", 10))
    collect_pprof_enabled = bool(run_config.get("collect_pprof", True))

    logger.info("Experiment starting")
    stop_docker_nodes(node_names)

    launch_nodes(node_names, args.config_path)
    logger.info("Waiting for all nodes to become ready")
    all_nodes_ready = wait_for_nodes_ready(node_names, timeout=120.0, poll_interval=1.0)
    if not all_nodes_ready:
        logger.warning("Node readiness timeout after 120s; proceeding anyway")

    remote_cpu_commands = {}
    if collect_pprof_enabled:
        remote_cpu_commands = start_remote_cpu_profiles(pprof_nodes, cpu_profile_max_seconds)

    logger.info("Waiting for client completion")
    progress_start = time.monotonic()
    completed = wait_for_clients_progress(
        client_names,
        poll_interval=1.0,
        stall_timeout=5.0,
    )
    progress_duration_seconds = max(0.0, time.monotonic() - progress_start)
    logger.info("Client progress duration: %.2fs", progress_duration_seconds)
    if not completed:
        logger.warning("Proceeding after progress timeout")

    if collect_pprof_enabled:
        stop_remote_cpu_profiles(remote_cpu_commands)
        wait_remote_cpu_profiles(
            remote_cpu_commands,
            timeout_seconds=cpu_profile_wait_slack_seconds,
        )
        capture_remote_snapshot_profiles(pprof_nodes)

    stop_docker_nodes(node_names)
    collect_logs(run_dir, node_names, client_names)
    if collect_pprof_enabled:
        collect_pprof(run_dir, pprof_nodes)
    logger.info("Experiment complete")


if __name__ == "__main__":
    main()
