import argparse
import json
import logging
import os
import glob
import re
import shlex
import statistics
import subprocess
import time
from datetime import datetime

import yaml

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
REMOTE_RPC_SCRIPT = "/app/remote_rpc.py"
REMOTE_BINARY_PATH = "/app/bin/aegean-node"
DEFAULT_REMOTE_CPU_PROFILE_PATH = "/tmp/cpu.pprof"
DEFAULT_REMOTE_BLOCK_PROFILE_PATH = "/tmp/block.pprof"
DEFAULT_REMOTE_MUTEX_PROFILE_PATH = "/tmp/mutex.pprof"
DEFAULT_REMOTE_OTEL_TRACE_PATH = "/tmp/otel-traces.json"


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

    data = load_config_file(resolved_run_config_path)

    architecture = data.get("architecture")
    if not architecture:
        raise ValueError("run config must include non-empty 'architecture'")

    run_timeout_seconds = data.get("run_timeout_seconds")
    if not isinstance(run_timeout_seconds, int) or run_timeout_seconds <= 0:
        raise ValueError("run config must include positive integer 'run_timeout_seconds'")

    architecture_path = os.path.normpath(os.path.join(architecture_dir, architecture))
    return resolved_run_config_path, relative_run_config_path, architecture_path, data


def load_experiment_topology(architecture_path):
    data = load_config_file(architecture_path)

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
        os.path.join(runs_dir, "*.yaml"),
        os.path.join(runs_dir, "*.yml"),
        os.path.join(runs_dir, "*", "*.json"),
        os.path.join(runs_dir, "*", "*.yaml"),
        os.path.join(runs_dir, "*", "*.yml"),
    ]

    run_config_paths = []
    for pattern in patterns:
        run_config_paths.extend(glob.glob(pattern))

    return sorted(os.path.abspath(path) for path in run_config_paths)


def create_results_run_dir(relative_run_config_path, results_dir="results", timestamped=False):
    results_root = os.path.join(REPO_ROOT, results_dir)
    run_config_relpath = os.path.relpath(
        os.path.abspath(os.path.join(REPO_ROOT, relative_run_config_path)),
        os.path.join(REPO_ROOT, "experiment", "runs"),
    )
    run_dir = os.path.join(results_root, os.path.splitext(run_config_relpath)[0])
    if timestamped:
        run_dir = os.path.join(run_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def collect_logs(run_dir, node_names, client_names, enable_pprof=True, enable_tracing=True):
    logger.info("Collecting logs (%d nodes, %d clients)", len(node_names), len(client_names))

    for name in node_names:
        local_path = os.path.join(run_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)

    profiled_node = os.environ.get("AEGEAN_PROFILE_NODE", "").strip()
    profile_path = os.environ.get("AEGEAN_CPU_PROFILE_PATH", DEFAULT_REMOTE_CPU_PROFILE_PATH).strip() or DEFAULT_REMOTE_CPU_PROFILE_PATH
    block_profile_path = os.environ.get("AEGEAN_BLOCK_PROFILE_PATH", DEFAULT_REMOTE_BLOCK_PROFILE_PATH).strip() or DEFAULT_REMOTE_BLOCK_PROFILE_PATH
    mutex_profile_path = os.environ.get("AEGEAN_MUTEX_PROFILE_PATH", DEFAULT_REMOTE_MUTEX_PROFILE_PATH).strip() or DEFAULT_REMOTE_MUTEX_PROFILE_PATH
    otel_trace_path = os.environ.get("AEGEAN_OTEL_FILE_PATH", DEFAULT_REMOTE_OTEL_TRACE_PATH).strip() or DEFAULT_REMOTE_OTEL_TRACE_PATH
    if enable_pprof and profiled_node:
        local_profile_path = os.path.join(run_dir, f"{profiled_node}.cpu.pprof")
        _scp(profiled_node, profile_path, local_profile_path)
        _scp(profiled_node, block_profile_path, os.path.join(run_dir, f"{profiled_node}.block.pprof"))
        _scp(profiled_node, mutex_profile_path, os.path.join(run_dir, f"{profiled_node}.mutex.pprof"))
    if enable_tracing:
        for name in node_names:
            _scp(name, otel_trace_path, os.path.join(run_dir, f"{name}.otel.json"))

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


def build_binary(build_node):
    logger.info("Building shared binary on %s", build_node)
    _ssh_shell(
        build_node,
        (
            "mkdir -p /app/bin /app/.gomodcache /tmp/go-build || exit 1; "
            "cd /app/src || exit 1; "
            "export GOMODCACHE=/app/.gomodcache; "
            "export GOCACHE=/tmp/go-build; "
            f"go build -o {shlex.quote(REMOTE_BINARY_PATH)} ."
        ),
    )


def launch_nodes(node_names, config_path, enable_pprof=True, enable_tracing=True):
    logger.info("Launching %d nodes", len(node_names))
    if node_names:
        build_binary(node_names[0])

    remote_config_path = shlex.quote(f"../{config_path}")
    profiled_node = os.environ.get("AEGEAN_PROFILE_NODE", "").strip()
    profile_path = os.environ.get("AEGEAN_CPU_PROFILE_PATH", DEFAULT_REMOTE_CPU_PROFILE_PATH).strip() or DEFAULT_REMOTE_CPU_PROFILE_PATH
    block_profile_path = os.environ.get("AEGEAN_BLOCK_PROFILE_PATH", DEFAULT_REMOTE_BLOCK_PROFILE_PATH).strip() or DEFAULT_REMOTE_BLOCK_PROFILE_PATH
    mutex_profile_path = os.environ.get("AEGEAN_MUTEX_PROFILE_PATH", DEFAULT_REMOTE_MUTEX_PROFILE_PATH).strip() or DEFAULT_REMOTE_MUTEX_PROFILE_PATH
    otel_trace_path = os.environ.get("AEGEAN_OTEL_FILE_PATH", DEFAULT_REMOTE_OTEL_TRACE_PATH).strip() or DEFAULT_REMOTE_OTEL_TRACE_PATH
    for name in node_names:
        profile_env = ""
        if enable_pprof and name == profiled_node:
            profile_env = (
                f"AEGEAN_CPU_PROFILE_PATH={shlex.quote(profile_path)} "
                f"AEGEAN_BLOCK_PROFILE_PATH={shlex.quote(block_profile_path)} "
                f"AEGEAN_MUTEX_PROFILE_PATH={shlex.quote(mutex_profile_path)} "
            )
        telemetry_env = ""
        if enable_tracing:
            telemetry_env = f"AEGEAN_OTEL_FILE_PATH={shlex.quote(otel_trace_path)} "
        else:
            telemetry_env = "AEGEAN_DISABLE_TRACING=1 "
        _ssh_shell(
            name,
            (
                "mkdir -p /app/.gomodcache /tmp/go-build || exit 1; "
                "cd /app/src || exit 1; "
                "export GOMODCACHE=/app/.gomodcache; "
                "export GOCACHE=/tmp/go-build; "
                f"nohup env {telemetry_env}{profile_env}{shlex.quote(REMOTE_BINARY_PATH)} --name {shlex.quote(name)} "
                "--host 0.0.0.0 "
                "--port 8000 "
                f"--config {remote_config_path} "
                "> /tmp/node.log 2>&1 &"
            ),
        )

def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes", len(node_names))
    for name in node_names:
        _ssh_shell(
            name,
            (
                "pkill -TERM -f aegean-node || true; "
                "pkill -TERM -f 'go run \\.' || true; "
                "sleep 1; "
                "pkill -9 -f aegean-node || true; "
                "pkill -9 -f 'go run \\.' || true"
            ),
        )


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


def _parse_k6_duration(value_str, unit_str):
    """Convert a k6 duration value+unit to seconds."""
    v = float(value_str)
    unit = unit_str.lower().strip()
    if unit == "ms":
        return v / 1000.0
    if unit == "µs" or unit == "us":
        return v / 1_000_000.0
    if unit == "s":
        return v
    return v


def parse_client_log(log_path):
    text = open(log_path, "r", encoding="utf-8").read()
    metrics = {}

    # --- Try k6 format first ---
    # http_req_duration line: avg=439.86ms min=38.12ms med=467.06ms max=783.82ms p(90)=695.11ms p(95)=722.34ms
    k6_duration_re = re.search(
        r"http_req_duration\.+:\s+"
        r"avg=([0-9.]+)(ms|s|µs|us)\s+"
        r"min=([0-9.]+)(ms|s|µs|us)\s+"
        r"med=([0-9.]+)(ms|s|µs|us)\s+"
        r"max=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(90\)=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(95\)=([0-9.]+)(ms|s|µs|us)",
        text,
    )
    if k6_duration_re:
        metrics["average_sec"] = _parse_k6_duration(k6_duration_re.group(1), k6_duration_re.group(2))
        metrics["fastest_sec"] = _parse_k6_duration(k6_duration_re.group(3), k6_duration_re.group(4))
        metrics["p50"] = _parse_k6_duration(k6_duration_re.group(5), k6_duration_re.group(6))
        metrics["slowest_sec"] = _parse_k6_duration(k6_duration_re.group(7), k6_duration_re.group(8))
        metrics["p90"] = _parse_k6_duration(k6_duration_re.group(9), k6_duration_re.group(10))
        metrics["p95"] = _parse_k6_duration(k6_duration_re.group(11), k6_duration_re.group(12))

    # http_reqs line: 5708   362.383422/s
    k6_reqs_re = re.search(r"http_reqs\.+:\s+(\d+)\s+([0-9.]+)/s", text)
    if k6_reqs_re:
        metrics["total_requests"] = int(k6_reqs_re.group(1))
        metrics["requests_sec"] = float(k6_reqs_re.group(2))

    # http_req_failed line: 0.00%  0 out of 5708
    k6_failed_re = re.search(r"http_req_failed\.+:\s+([0-9.]+)%", text)
    if k6_failed_re:
        metrics["success_rate"] = 100.0 - float(k6_failed_re.group(1))

    # iteration_duration for additional percentiles
    k6_iter_re = re.search(
        r"iteration_duration\.+:\s+"
        r"avg=([0-9.]+)(ms|s|µs|us)\s+"
        r"min=([0-9.]+)(ms|s|µs|us)\s+"
        r"med=([0-9.]+)(ms|s|µs|us)\s+"
        r"max=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(90\)=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(95\)=([0-9.]+)(ms|s|µs|us)",
        text,
    )

    if metrics:
        return metrics

    # --- Fallback: oha-style format ---
    for key, pattern in [
        ("success_rate", r"Success rate:\s+([0-9.]+)%"),
        ("total_sec", r"Total:\s+([0-9.]+) sec"),
        ("slowest_sec", r"Slowest:\s+([0-9.]+) sec"),
        ("fastest_sec", r"Fastest:\s+([0-9.]+) sec"),
        ("average_sec", r"Average:\s+([0-9.]+) sec"),
        ("requests_sec", r"Requests/sec:\s+([0-9.]+)"),
    ]:
        m = re.search(pattern, text)
        if m:
            metrics[key] = float(m.group(1))

    percentile_re = re.compile(r"^\s*([0-9.]+)%\s+in\s+([0-9.]+)\s+sec", re.MULTILINE)
    for m in percentile_re.finditer(text):
        pct = m.group(1)
        label = "p" + pct.replace(".", "_").rstrip("0").rstrip("_")
        metrics[label] = float(m.group(2))

    histogram = []
    hist_re = re.compile(r"^\s*([0-9.]+)\s+sec\s+\[(\d+)\]", re.MULTILINE)
    for m in hist_re.finditer(text):
        histogram.append([float(m.group(1)), int(m.group(2))])
    if histogram:
        metrics["histogram"] = histogram

    status_codes = {}
    status_re = re.compile(r"^\s*\[(\d+)\]\s+(\d+)\s+responses", re.MULTILINE)
    for m in status_re.finditer(text):
        status_codes[m.group(1)] = int(m.group(2))
    if status_codes:
        metrics["status_codes"] = status_codes

    return metrics


def aggregate_runs(per_run_metrics):
    scalar_keys = set()
    for m in per_run_metrics:
        for k, v in m.items():
            if isinstance(v, (int, float)):
                scalar_keys.add(k)

    aggregated = {}
    for key in sorted(scalar_keys):
        values = [m[key] for m in per_run_metrics if key in m]
        if not values:
            continue
        aggregated[key] = {
            "mean": round(statistics.mean(values), 4),
            "median": round(statistics.median(values), 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "stdev": round(statistics.stdev(values), 4) if len(values) > 1 else 0.0,
            "values": values,
        }
    return aggregated


def run_experiment(config_path, enable_pprof=True, enable_tracing=True, timestamped=False):
    _, relative_run_config_path, architecture_path, run_config = load_run_config(config_path)
    node_names, client_names = load_experiment_topology(architecture_path)
    run_dir = create_results_run_dir(relative_run_config_path, timestamped=timestamped)
    run_timeout_seconds = run_config["run_timeout_seconds"]

    logger.info("Experiment starting: %s", relative_run_config_path)
    stop_docker_nodes(node_names)

    launch_nodes(node_names, relative_run_config_path, enable_pprof=enable_pprof, enable_tracing=enable_tracing)
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
    time.sleep(3)  # wait for ports to be released before next run
    collect_logs(run_dir, node_names, client_names, enable_pprof=enable_pprof, enable_tracing=enable_tracing)
    logger.info("Experiment complete: %s -> %s", relative_run_config_path, run_dir)
    return run_dir, client_names


def run_experiment_n_times(config_path, n, enable_pprof=True, enable_tracing=True):
    logger.info("Running experiment %d times: %s", n, config_path)
    run_dirs = []
    per_run_metrics = []
    client_names = []

    for i in range(n):
        logger.info("=== Run %d/%d ===", i + 1, n)
        run_dir, client_names = run_experiment(
            config_path, enable_pprof=enable_pprof, enable_tracing=enable_tracing,
            timestamped=True,
        )
        run_dirs.append(run_dir)

        for client_name in client_names:
            log_path = os.path.join(run_dir, f"{client_name}.log")
            if os.path.isfile(log_path):
                metrics = parse_client_log(log_path)
                per_run_metrics.append(metrics)
                break

    aggregated = aggregate_runs(per_run_metrics)

    result = {
        "num_runs": n,
        "run_dirs": run_dirs,
        "config": config_path,
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "per_run": per_run_metrics,
        "aggregated": aggregated,
    }

    _, relative_run_config_path, _, _ = load_run_config(config_path)
    results_run_dir = create_results_run_dir(relative_run_config_path, timestamped=False)
    output_path = os.path.join(results_run_dir, "aggregated_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    logger.info("Aggregated results written to %s", output_path)
    return output_path


def load_config_file(path):
    with open(path, "r", encoding="utf-8") as f:
        if path.endswith((".yaml", ".yml")):
            data = yaml.safe_load(f)
        else:
            data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"config file must contain an object: {path}")
    return data


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment")
    parser.add_argument("config_path", nargs="?", help="Path to run config YAML or JSON")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all configs under experiment/runs and experiment/runs/*",
    )
    parser.add_argument(
        "--disable-pprof",
        action="store_true",
        help="Disable pprof env injection and pprof artifact collection.",
    )
    parser.add_argument(
        "--disable-tracing",
        action="store_true",
        help="Disable tracing env injection and otel artifact collection.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to run the experiment (default: 1). Results are aggregated into aggregated_results.json.",
    )
    args = parser.parse_args()

    enable_pprof = not args.disable_pprof
    enable_tracing = not args.disable_tracing

    if args.all:
        if args.config_path:
            parser.error("config_path cannot be used with --all")
        config_paths = list_run_config_paths()
        if not config_paths:
            parser.error("no run configs found under experiment/runs")
        for config_path in config_paths:
            if args.runs > 1:
                run_experiment_n_times(config_path, args.runs, enable_pprof=enable_pprof, enable_tracing=enable_tracing)
            else:
                run_experiment(config_path, enable_pprof=enable_pprof, enable_tracing=enable_tracing)
        return

    if not args.config_path:
        parser.error("config_path is required unless --all is used")

    if args.runs > 1:
        output_path = run_experiment_n_times(
            args.config_path, args.runs, enable_pprof=enable_pprof, enable_tracing=enable_tracing,
        )
        print(f"Aggregated results: {output_path}")
    else:
        run_experiment(args.config_path, enable_pprof=enable_pprof, enable_tracing=enable_tracing)


if __name__ == "__main__":
    main()
