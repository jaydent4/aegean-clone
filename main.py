import argparse
import json
import logging
import os
import glob
import re
import shutil
import shlex
import statistics
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.message import EmailMessage

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

BINARY_SOURCE_NODE = None
BINARY_READY_NODES = set()

HOTEL_LOCAL_REDIS_SERVICES = {
    "geo",
    "profile",
    "rate",
    "recommendation",
    "reservation",
    "search",
    "user",
}

MEDIA_LOCAL_REDIS_SERVICES = {
    "compose_review",
    "movie_id",
    "movie_review",
    "rating",
    "review_storage",
    "user",
    "user_review",
}

SOCIAL_LOCAL_REDIS_SERVICES = {
    "home_timeline",
    "post_storage",
    "social_graph",
    "user_timeline",
}


def send_completion_email(to_address, subject, body):
    sendmail_path = shutil.which("sendmail") or "/usr/sbin/sendmail"
    if not os.path.exists(sendmail_path):
        logger.warning("Could not send completion email: sendmail not found")
        return False

    from_address = os.environ.get("AEGEAN_EMAIL_FROM", "aegean@localhost")
    message = EmailMessage()
    message["From"] = from_address
    message["To"] = to_address
    message["Subject"] = subject
    message.set_content(body)

    result = subprocess.run(
        [sendmail_path, "-t", "-oi"],
        input=message.as_bytes(),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        logger.warning("Could not send completion email to %s: %s", to_address, stderr or result.returncode)
        return False

    logger.info("Completion email sent to %s", to_address)
    return True


def send_run_complete_email(to_address, completed_items, started_at):
    elapsed_seconds = time.monotonic() - started_at
    subject = "Aegean experiment complete"
    lines = [
        "Aegean experiment run complete.",
        "",
        f"Completed at: {datetime.now().isoformat(timespec='seconds')}",
        f"Elapsed: {elapsed_seconds:.1f}s",
    ]
    if completed_items:
        lines.extend(["", "Outputs:"])
        lines.extend(f"- {item}" for item in completed_items)
    send_completion_email(to_address, subject, "\n".join(lines) + "\n")


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

    nested_eo_request_quorum_timeout_ms = data.get("nested_eo_request_quorum_timeout_ms")
    if (
        nested_eo_request_quorum_timeout_ms is not None
        and (
            not isinstance(nested_eo_request_quorum_timeout_ms, int)
            or nested_eo_request_quorum_timeout_ms <= 0
        )
    ):
        raise ValueError(
            "run config field 'nested_eo_request_quorum_timeout_ms' must be a positive integer"
        )

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
    node_services = {}
    for node_name, node_cfg in nodes.items():
        service_name = node_cfg.get("service")
        if not service_name:
            raise ValueError(f"node {node_name} is missing 'service'")

        service_cfg = services.get(service_name)
        if not service_cfg:
            raise ValueError(f"node {node_name} references unknown service '{service_name}'")

        service_type = service_cfg.get("type")
        node_services[node_name] = service_name
        if service_type == "client":
            client_names.append(node_name)

    return node_names, sorted(client_names), node_services


def _scp(node_name, remote_path, local_path):
    return subprocess.run(["scp", *_ssh_options(), f"{node_name}:{remote_path}", local_path], check=False)


def _scp_to(local_path, node_name, remote_path):
    return subprocess.run(["scp", *_ssh_options(), local_path, f"{node_name}:{remote_path}"], check=False)


def _run_for_nodes(node_names, task, action):
    if not node_names:
        return {}

    results = {}
    failures = []
    with ThreadPoolExecutor(max_workers=len(node_names)) as executor:
        futures = {executor.submit(task, name): name for name in node_names}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures.append((name, exc))

    if failures:
        details = "; ".join(f"{name}: {exc}" for name, exc in failures)
        raise RuntimeError(f"{action} failed for {details}")

    return results


def list_run_config_paths(runs_dir=None):
    if runs_dir is None:
        runs_dir = os.path.join(REPO_ROOT, "experiment", "runs")

    patterns = [
        os.path.join(runs_dir, "**", "*.json"),
        os.path.join(runs_dir, "**", "*.yaml"),
        os.path.join(runs_dir, "**", "*.yml"),
    ]

    run_config_paths = []
    for pattern in patterns:
        run_config_paths.extend(glob.glob(pattern, recursive=True))

    return sorted(os.path.abspath(path) for path in run_config_paths)


def get_results_run_dir(relative_run_config_path, results_dir="results", timestamped=False):
    results_root = os.path.join(REPO_ROOT, results_dir)
    run_config_relpath = os.path.relpath(
        os.path.abspath(os.path.join(REPO_ROOT, relative_run_config_path)),
        os.path.join(REPO_ROOT, "experiment", "runs"),
    )
    run_dir = os.path.join(results_root, os.path.splitext(run_config_relpath)[0])
    if timestamped:
        run_dir = os.path.join(run_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
    return run_dir


def create_results_run_dir(relative_run_config_path, results_dir="results", timestamped=False):
    run_dir = get_results_run_dir(relative_run_config_path, results_dir=results_dir, timestamped=timestamped)
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def result_exists_for_run_config(config_path, runs=1):
    _, relative_run_config_path, _, _ = load_run_config(config_path)
    run_dir = get_results_run_dir(relative_run_config_path, timestamped=False)
    if runs > 1:
        return os.path.isfile(os.path.join(run_dir, "aggregated_results.json")), run_dir
    if not os.path.isdir(run_dir):
        return False, run_dir
    with os.scandir(run_dir) as entries:
        return any(entries), run_dir


def collect_logs(run_dir, node_names, client_names, enable_pprof=False, enable_tracing=False):
    logger.info("Collecting logs (%d nodes, %d clients, %d workers)", len(node_names), len(client_names), len(node_names))

    def copy_node_log(name):
        local_path = os.path.join(run_dir, f"{name}.log")
        return _scp(name, "/tmp/node.log", local_path)

    _run_for_nodes(node_names, copy_node_log, "collect logs")

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
        def copy_otel_trace(name):
            return _scp(name, otel_trace_path, os.path.join(run_dir, f"{name}.otel.json"))

        _run_for_nodes(node_names, copy_otel_trace, "collect traces")

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
    logger.info("Building binary on %s", build_node)
    result = _ssh_shell(
        build_node,
        (
            "mkdir -p /app/bin /app/.gomodcache /tmp/go-build || exit 1; "
            "cd /app/src || exit 1; "
            "export GOMODCACHE=/app/.gomodcache; "
            "export GOCACHE=/tmp/go-build; "
            f"go build -buildvcs=false -o {shlex.quote(REMOTE_BINARY_PATH)} ."
        ),
    )
    if result.returncode != 0:
        raise RuntimeError(f"failed to build shared binary on {build_node}")


def distribute_binary(build_node, node_names):
    targets = [name for name in node_names if name != build_node]
    if not targets:
        return

    logger.info("Distributing binary from %s to %d node(s) with %d worker(s)", build_node, len(targets), len(targets))
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_binary = os.path.join(tmp_dir, "aegean-node")
        result = _scp(build_node, REMOTE_BINARY_PATH, local_binary)
        if result.returncode != 0:
            raise RuntimeError(f"failed to copy binary from {build_node}")

        def distribute_to_node(name):
            result = _ssh_shell(name, "mkdir -p /app/bin")
            if result.returncode != 0:
                raise RuntimeError(f"failed to create binary directory on {name}")
            result = _scp_to(local_binary, name, REMOTE_BINARY_PATH)
            if result.returncode != 0:
                raise RuntimeError(f"failed to copy binary to {name}")
            result = _ssh_shell(name, f"chmod +x {shlex.quote(REMOTE_BINARY_PATH)}")
            if result.returncode != 0:
                raise RuntimeError(f"failed to mark binary executable on {name}")

        _run_for_nodes(targets, distribute_to_node, "distribute binary")


def ensure_binaries_ready(node_names):
    global BINARY_SOURCE_NODE

    missing_nodes = [name for name in node_names if name not in BINARY_READY_NODES]
    if not missing_nodes:
        logger.info("Binary already available on %d node(s); skipping build", len(node_names))
        return

    if BINARY_SOURCE_NODE is None:
        BINARY_SOURCE_NODE = node_names[0]
        build_binary(BINARY_SOURCE_NODE)
        BINARY_READY_NODES.add(BINARY_SOURCE_NODE)

    distribute_binary(BINARY_SOURCE_NODE, missing_nodes)
    BINARY_READY_NODES.update(missing_nodes)


def node_local_redis_env(run_config, service_name):
    env_parts = []
    needs_local_redis = False

    if "hotel_redis_enable" in run_config:
        enabled = bool(run_config.get("hotel_redis_enable")) and (
            service_name in HOTEL_LOCAL_REDIS_SERVICES or service_name.startswith("recommendation_")
        )
        env_parts.append(f"HOTEL_REDIS_ENABLE={'1' if enabled else '0'}")
        if enabled:
            env_parts.append("HOTEL_REDIS_ADDR=127.0.0.1:6379")
            needs_local_redis = True

    if "media_redis_enable" in run_config:
        enabled = bool(run_config.get("media_redis_enable")) and service_name in MEDIA_LOCAL_REDIS_SERVICES
        env_parts.append(f"MEDIA_REDIS_ENABLE={'1' if enabled else '0'}")
        if enabled:
            env_parts.append("MEDIA_REDIS_ADDR=127.0.0.1:6379")
            needs_local_redis = True

    if "social_redis_enable" in run_config:
        enabled = bool(run_config.get("social_redis_enable")) and service_name in SOCIAL_LOCAL_REDIS_SERVICES
        env_parts.append(f"SOCIAL_REDIS_ENABLE={'1' if enabled else '0'}")
        if enabled:
            env_parts.append("SOCIAL_REDIS_ADDR=127.0.0.1:6379")
            needs_local_redis = True

    return needs_local_redis, " ".join(env_parts)


def launch_nodes(
    node_names,
    node_services,
    config_path,
    run_config,
    enable_pprof=False,
    enable_tracing=False,
    enable_logging=False,
):
    logger.info("Launching %d nodes with %d worker(s)", len(node_names), len(node_names))
    if node_names:
        ensure_binaries_ready(node_names)

    remote_config_path = shlex.quote(f"../{config_path}")
    profiled_node = os.environ.get("AEGEAN_PROFILE_NODE", "").strip()
    profile_path = os.environ.get("AEGEAN_CPU_PROFILE_PATH", DEFAULT_REMOTE_CPU_PROFILE_PATH).strip() or DEFAULT_REMOTE_CPU_PROFILE_PATH
    block_profile_path = os.environ.get("AEGEAN_BLOCK_PROFILE_PATH", DEFAULT_REMOTE_BLOCK_PROFILE_PATH).strip() or DEFAULT_REMOTE_BLOCK_PROFILE_PATH
    mutex_profile_path = os.environ.get("AEGEAN_MUTEX_PROFILE_PATH", DEFAULT_REMOTE_MUTEX_PROFILE_PATH).strip() or DEFAULT_REMOTE_MUTEX_PROFILE_PATH
    otel_trace_path = os.environ.get("AEGEAN_OTEL_FILE_PATH", DEFAULT_REMOTE_OTEL_TRACE_PATH).strip() or DEFAULT_REMOTE_OTEL_TRACE_PATH
    def launch_node(name):
        service_name = node_services.get(name, "")
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
        logging_env = f"AEGEAN_ENABLE_LOGGING={'1' if enable_logging else '0'} "
        redis_needed, redis_env = node_local_redis_env(run_config, service_name)
        redis_setup = (
            "sudo pkill -TERM -x redis-server || true; "
            "sleep 1; "
            "sudo pkill -9 -x redis-server || true; "
        )
        if redis_needed:
            redis_data_dir = f"/tmp/aegean-redis-{name}"
            redis_setup += (
                f"mkdir -p {shlex.quote(redis_data_dir)} || exit 1; "
                f"redis-server --bind 127.0.0.1 --port 6379 "
                f"--dir {shlex.quote(redis_data_dir)} "
                "--appendonly yes "
                "--logfile /tmp/redis.log "
                "--daemonize yes || exit 1; "
            )
        redis_env_prefix = (redis_env + " ") if redis_env else ""
        result = _ssh_shell(
            name,
            (
                "mkdir -p /app/.gomodcache /tmp/go-build || exit 1; "
                "cd /app/src || exit 1; "
                "export GOMODCACHE=/app/.gomodcache; "
                "export GOCACHE=/tmp/go-build; "
                f"{redis_setup}"
                f"nohup env {logging_env}{telemetry_env}{profile_env}{redis_env_prefix}"
                f"{shlex.quote(REMOTE_BINARY_PATH)} --name {shlex.quote(name)} "
                "--host 0.0.0.0 "
                "--port 8000 "
                f"--config {remote_config_path} "
                "> /tmp/node.log 2>&1 &"
            ),
        )
        if result.returncode != 0:
            raise RuntimeError(f"failed to launch node {name}")

    _run_for_nodes(node_names, launch_node, "launch nodes")


def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes with %d worker(s)", len(node_names), len(node_names))

    def stop_node(name):
        return _ssh_shell(
            name,
            (
                "pkill -TERM -f aegean-node || true; "
                "pkill -TERM -f 'go run \\.' || true; "
                "pkill -TERM -x redis-server || true; "
                "sleep 1; "
                "pkill -9 -f aegean-node || true; "
                "pkill -9 -f 'go run \\.' || true"
                "; "
                "pkill -9 -x redis-server || true"
            ),
        )

    _run_for_nodes(node_names, stop_node, "stop nodes")


def get_node_ready(node_name):
    payload = _remote_rpc(node_name, "/ready", timeout=5)
    return bool(payload.get("ready", False))


def wait_for_nodes_ready(node_names, timeout=30.0, poll_interval=1.0):
    if not node_names:
        return True

    deadline = time.monotonic() + timeout
    with ThreadPoolExecutor(max_workers=len(node_names)) as executor:
        while time.monotonic() < deadline:
            all_ready = True
            futures = {executor.submit(get_node_ready, name): name for name in node_names}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    if not future.result():
                        all_ready = False
                except Exception as exc:  # noqa: BLE001
                    logger.info("Node %s not ready yet: %s", name, exc)
                    all_ready = False

            if all_ready:
                return True
            time.sleep(poll_interval)

    return False


def get_client_status(node_name):
    payload = _remote_rpc(node_name, "/status", timeout=5)
    return {
        "request_logic_started": bool(payload.get("request_logic_started", False)),
        "request_logic_completed": bool(payload.get("request_logic_completed", False)),
    }


def wait_for_clients_complete(client_names, timeout=30.0, poll_interval=1.0):
    if not client_names:
        return True

    deadline = time.monotonic() + timeout
    with ThreadPoolExecutor(max_workers=len(client_names)) as executor:
        while time.monotonic() < deadline:
            all_completed = True
            futures = {executor.submit(get_client_status, name): name for name in client_names}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    status = future.result()
                    if not status["request_logic_completed"]:
                        all_completed = False
                except Exception as exc:  # noqa: BLE001
                    logger.info("Client %s status unavailable yet: %s", name, exc)
                    all_completed = False

            if all_completed:
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


def run_experiment(
    config_path,
    enable_pprof=False,
    enable_tracing=False,
    enable_logging=False,
    timestamped=False,
):
    _, relative_run_config_path, architecture_path, run_config = load_run_config(config_path)
    node_names, client_names, node_services = load_experiment_topology(architecture_path)
    run_dir = create_results_run_dir(relative_run_config_path, timestamped=timestamped)
    run_timeout_seconds = run_config["run_timeout_seconds"]

    logger.info("Experiment starting: %s", relative_run_config_path)
    stop_docker_nodes(node_names)

    launch_nodes(
        node_names,
        node_services,
        relative_run_config_path,
        run_config,
        enable_pprof=enable_pprof,
        enable_tracing=enable_tracing,
        enable_logging=enable_logging,
    )
    logger.info("Waiting for all nodes to become ready")
    all_nodes_ready = wait_for_nodes_ready(node_names, timeout=120.0, poll_interval=1.0)
    if not all_nodes_ready:
        logger.warning("Node readiness timeout after 120s; proceeding anyway")

    run_start = time.monotonic()
    if client_names:
        client_wait_timeout = max(30.0, float(run_timeout_seconds) * 2.0)
        logger.info(
            "Waiting for %d client(s) to finish request logic (safety timeout %.0fs)",
            len(client_names),
            client_wait_timeout,
        )
        all_clients_completed = wait_for_clients_complete(
            client_names,
            timeout=client_wait_timeout,
            poll_interval=1.0,
        )
        run_duration_seconds = max(0.0, time.monotonic() - run_start)
        if all_clients_completed:
            logger.info("All clients finished after %.2fs", run_duration_seconds)
        else:
            logger.warning(
                "Timed out waiting for clients after %.2fs; stopping experiment anyway",
                run_duration_seconds,
            )
    else:
        logger.info("No client nodes found; falling back to run timeout: %ss", run_timeout_seconds)
        time.sleep(run_timeout_seconds)
        run_duration_seconds = max(0.0, time.monotonic() - run_start)
        logger.info("Run timeout reached after %.2fs", run_duration_seconds)

    stop_docker_nodes(node_names)
    time.sleep(3)  # wait for ports to be released before next run
    collect_logs(run_dir, node_names, client_names, enable_pprof=enable_pprof, enable_tracing=enable_tracing)
    logger.info("Experiment complete: %s -> %s", relative_run_config_path, run_dir)
    return run_dir, client_names


def run_experiment_n_times(
    config_path,
    n,
    enable_pprof=False,
    enable_tracing=False,
    enable_logging=False,
):
    logger.info("Running experiment %d times: %s", n, config_path)
    run_dirs = []
    per_run_metrics = []
    client_names = []

    for i in range(n):
        logger.info("=== Run %d/%d ===", i + 1, n)
        run_dir, client_names = run_experiment(
            config_path,
            enable_pprof=enable_pprof,
            enable_tracing=enable_tracing,
            enable_logging=enable_logging,
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


def run_config_is_skipped(config_path):
    data = load_config_file(config_path)
    skipped = data.get("skipped", False)
    if not isinstance(skipped, bool):
        raise ValueError(f"run config 'skipped' field must be boolean when present: {config_path}")
    return skipped


def run_config_paths(
    config_paths,
    runs,
    enable_pprof=False,
    enable_tracing=False,
    enable_logging=False,
    resume=False,
):
    completed_items = []
    for config_path in config_paths:
        if run_config_is_skipped(config_path):
            logger.info("Skipping config marked skipped=true: %s", config_path)
            continue

        if resume:
            result_exists, result_path = result_exists_for_run_config(config_path, runs=runs)
            if result_exists:
                if runs > 1:
                    result_path = os.path.join(result_path, "aggregated_results.json")
                logger.info("Skipping existing result for %s: %s", config_path, result_path)
                completed_items.append(result_path)
                continue

        if runs > 1:
            output_path = run_experiment_n_times(
                config_path,
                runs,
                enable_pprof=enable_pprof,
                enable_tracing=enable_tracing,
                enable_logging=enable_logging,
            )
            completed_items.append(output_path)
        else:
            run_dir, _ = run_experiment(
                config_path,
                enable_pprof=enable_pprof,
                enable_tracing=enable_tracing,
                enable_logging=enable_logging,
            )
            completed_items.append(run_dir)
    return completed_items


def resolve_config_targets(config_targets, parser):
    config_paths = []
    all_run_config_paths = None
    for config_target in config_targets:
        resolved_config_target = os.path.abspath(config_target)
        if os.path.isdir(resolved_config_target):
            dir_config_paths = list_run_config_paths(resolved_config_target)
            if not dir_config_paths:
                parser.error(f"no run configs found under {resolved_config_target}")
            config_paths.extend(dir_config_paths)
        elif os.path.isfile(resolved_config_target):
            config_paths.append(resolved_config_target)
        else:
            try:
                config_re = re.compile(config_target)
            except re.error as exc:
                parser.error(f"config_path is not an existing file/directory or valid regex: {config_target} ({exc})")

            if all_run_config_paths is None:
                all_run_config_paths = list_run_config_paths()

            regex_matches = [
                path for path in all_run_config_paths
                if config_re.search(os.path.relpath(path, REPO_ROOT)) or config_re.search(path)
            ]
            if not regex_matches:
                parser.error(f"regex matched no run configs: {config_target}")
            config_paths.extend(regex_matches)
    return config_paths


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment")
    parser.add_argument(
        "config_paths",
        nargs="*",
        help="Paths to run config YAML/JSON files, directories of run configs, or regexes matching run config paths",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all configs under experiment/runs and experiment/runs/*",
    )
    parser.add_argument(
        "--enable-pprof",
        action="store_true",
        help="Enable pprof env injection and pprof artifact collection.",
    )
    parser.add_argument(
        "--enable-tracing",
        action="store_true",
        help="Enable tracing env injection and otel artifact collection.",
    )
    parser.add_argument(
        "--enable-logging",
        action="store_true",
        help="Enable Aegean node logs and Raft module logs. Disabled by default.",
    )
    parser.add_argument(
        "--dir",
        dest="config_dir",
        help="Run every config YAML or JSON file under the provided directory recursively. Equivalent to passing the directory as a config_path.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to run the experiment (default: 1). Results are aggregated into aggregated_results.json.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip configs whose expected result already exists.",
    )
    parser.add_argument(
        "--email",
        help="Send a completion email to this address after all requested runs finish.",
    )
    args = parser.parse_args()

    enable_pprof = args.enable_pprof
    enable_tracing = args.enable_tracing
    enable_logging = args.enable_logging
    started_at = time.monotonic()
    config_paths = []

    if args.runs < 1:
        parser.error("--runs must be at least 1")

    if args.all:
        if args.config_paths:
            parser.error("config_paths cannot be used with --all")
        if args.config_dir:
            parser.error("--dir cannot be used with --all")
        config_paths = list_run_config_paths()
        if not config_paths:
            parser.error("no run configs found under experiment/runs")
    else:
        if args.config_dir and args.config_paths:
            parser.error("config_paths cannot be used with --dir")
        config_targets = [args.config_dir] if args.config_dir else args.config_paths
        if not config_targets:
            parser.error("at least one config_path is required unless --all or --dir is used")
        config_paths = resolve_config_targets(config_targets, parser)

    completed_items = run_config_paths(
        config_paths,
        args.runs,
        enable_pprof=enable_pprof,
        enable_tracing=enable_tracing,
        enable_logging=enable_logging,
        resume=args.resume,
    )

    if args.runs > 1 and len(completed_items) == 1 and not args.all and not args.config_dir:
        print(f"Aggregated results: {completed_items[0]}")

    if args.email:
        send_run_complete_email(args.email, completed_items, started_at)


if __name__ == "__main__":
    main()
