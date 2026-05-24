#!/usr/bin/env python3
"""Find a QPS whose k6 p90 latency falls inside a target band."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_LOWER_QPS = 0
DEFAULT_UPPER_QPS = 10_000
DEFAULT_MIN_P90_SECONDS = 0.2
DEFAULT_MAX_P90_SECONDS = 0.4
DEFAULT_NODE_COUNT = 26
BOUNDARY_CONFIG_NAME = "qps_boundary.yaml"
DEFAULT_REPO_SCRIPT = "eval/repo.py" if (REPO_ROOT / "eval" / "repo.py").is_file() else "setup/repo.py"

K6_QPS_YAML_RE = re.compile(r"^(\s*k6_qps\s*:\s*)([^#\n]*?)(\s*(?:#.*)?)$", re.MULTILINE)
THROUGHPUT_RE = re.compile(r"http_reqs\.*:\s+\d+\s+([0-9.]+)/s")
LATENCY_RE = re.compile(
    r"http_req_duration\.*:.*?\bmed=([0-9.]+)(µs|us|ms|s)\b.*?\bp\(90\)=([0-9.]+)(µs|us|ms|s)\b"
)


@dataclass(frozen=True)
class Metrics:
    throughput: float
    p90_seconds: float
    sources: tuple[Path, ...]


@dataclass(frozen=True)
class Trial:
    qps: int
    throughput: float
    p90_seconds: float
    sources: tuple[Path, ...]


class BoundaryError(RuntimeError):
    pass


class K6MetricsParseError(BoundaryError):
    pass


def duration_to_seconds(value: str, unit: str) -> float:
    duration = float(value)
    if unit in ("µs", "us"):
        return duration / 1_000_000.0
    if unit == "ms":
        return duration / 1_000.0
    return duration


def parse_metrics_text(text: str, source: Path) -> Metrics | None:
    throughput_match = THROUGHPUT_RE.search(text)
    latency_match = LATENCY_RE.search(text)
    if not throughput_match or not latency_match:
        return None

    return Metrics(
        throughput=float(throughput_match.group(1)),
        p90_seconds=duration_to_seconds(latency_match.group(3), latency_match.group(4)),
        sources=(source,),
    )


def combine_metrics(metrics: list[Metrics]) -> Metrics:
    if not metrics:
        raise BoundaryError("no metrics to combine")
    if len(metrics) == 1:
        return metrics[0]

    # Multiple k6 client logs cannot be merged into an exact p90 from summaries.
    # Sum realized throughput and use the highest client p90 as a conservative boundary signal.
    return Metrics(
        throughput=sum(metric.throughput for metric in metrics),
        p90_seconds=max(metric.p90_seconds for metric in metrics),
        sources=tuple(source for metric in metrics for source in metric.sources),
    )


def set_k6_qps(config_path: Path, qps: int) -> None:
    suffix = config_path.suffix.lower()
    text = config_path.read_text(encoding="utf-8")

    if suffix == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise BoundaryError(f"config file must contain a JSON object: {config_path}")
        if "k6_qps" not in data:
            raise BoundaryError(f"config file does not contain k6_qps: {config_path}")
        data["k6_qps"] = qps
        config_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        return

    new_text, replacements = K6_QPS_YAML_RE.subn(rf"\g<1>{qps}\g<3>", text, count=1)
    if replacements != 1:
        raise BoundaryError(f"config file does not contain a top-level k6_qps line: {config_path}")
    config_path.write_text(new_text, encoding="utf-8")


def resolve_results_dir(config_path: Path) -> Path:
    config_path = config_path.resolve()
    current = config_path.parent
    while True:
        if current.name == "runs" and (current.parent / "architecture").is_dir():
            rel_to_runs = config_path.relative_to(current)
            return REPO_ROOT / "results" / rel_to_runs.with_suffix("")

        parent = current.parent
        if parent == current:
            raise BoundaryError(f"could not find enclosing experiment/runs directory for {config_path}")
        current = parent


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def qps_config_paths(run_dir: Path) -> list[Path]:
    return sorted(path for path in run_dir.glob("qps_*.yaml") if path.is_file())


def pick_source_qps_config(run_dir: Path) -> Path:
    candidates = qps_config_paths(run_dir)
    if not candidates:
        raise BoundaryError(f"{display_path(run_dir)} must contain at least one qps_*.yaml file")

    non_boundary = [path for path in candidates if path.name != BOUNDARY_CONFIG_NAME]
    return non_boundary[0] if non_boundary else candidates[0]


def create_boundary_config(run_dir: Path) -> Path:
    source_path = pick_source_qps_config(run_dir)
    boundary_path = run_dir / BOUNDARY_CONFIG_NAME
    if source_path != boundary_path:
        shutil.copy2(source_path, boundary_path)
    print(
        f"Boundary config: copied {display_path(source_path)} -> {display_path(boundary_path)}",
        flush=True,
    )
    return boundary_path


def resolve_boundary_config_path(config_arg: str) -> Path:
    path = Path(config_arg).expanduser().resolve()
    if path.is_dir():
        return create_boundary_config(path)
    if path.is_file():
        return path
    raise BoundaryError(f"config path does not exist or is not a file/directory: {path}")


def run_streaming_command(cmd: list[str], *, cwd: Path) -> list[str]:
    print(f"$ {shlex.join(cmd)}", flush=True)
    tail: deque[str] = deque(maxlen=80)

    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        tail.append(line.rstrip("\n"))

    returncode = process.wait()
    if returncode != 0:
        tail_text = "\n".join(tail)
        raise BoundaryError(
            f"command failed with exit code {returncode}: {shlex.join(cmd)}"
            + (f"\nLast output lines:\n{tail_text}" if tail_text else "")
        )

    return list(tail)


def prepare_remote_repo(args: argparse.Namespace) -> None:
    repo_script = getattr(args, "repo_script", DEFAULT_REPO_SCRIPT)
    run_streaming_command(["git", "add", "."], cwd=REPO_ROOT)
    run_streaming_command([args.python_bin, repo_script, str(args.node_count), "--upload"], cwd=REPO_ROOT)


def run_main_config(args: argparse.Namespace, config_path: Path) -> None:
    cmd = [args.python_bin, "main.py"]
    if getattr(args, "_binary_ready", False):
        cmd.append("--skip-binary-build")
    cmd.append(str(config_path))

    run_streaming_command(cmd, cwd=REPO_ROOT)
    args._binary_ready = True


def parse_fresh_result_metrics(result_dir: Path, started_at: float) -> Metrics:
    if not result_dir.is_dir():
        raise BoundaryError(f"result directory was not created: {result_dir}")

    metrics: list[Metrics] = []
    for log_path in sorted(result_dir.glob("*.log")):
        try:
            if log_path.stat().st_mtime < started_at - 2.0:
                continue
            metric = parse_metrics_text(log_path.read_text(encoding="utf-8", errors="replace"), log_path)
        except OSError:
            continue
        if metric is not None:
            metrics.append(metric)

    if not metrics:
        fresh_logs = sorted(
            log_path
            for log_path in result_dir.glob("*.log")
            if log_path.stat().st_mtime >= started_at - 2.0
        )
        fresh_log_list = ", ".join(display_path(path) for path in fresh_logs) or "none"
        raise K6MetricsParseError(
            "could not parse k6 http_reqs/http_req_duration metrics from fresh logs "
            f"in {display_path(result_dir)}; fresh logs: {fresh_log_list}"
        )

    return combine_metrics(metrics)


def run_trial(args: argparse.Namespace, config_path: Path, result_dir: Path, qps: int) -> Trial:
    print(f"\n=== Testing k6_qps={qps} ===", flush=True)
    set_k6_qps(config_path, qps)

    prepare_remote_repo(args)

    started_at = time.time()
    run_main_config(args, config_path)
    metrics = parse_fresh_result_metrics(result_dir, started_at)

    trial = Trial(
        qps=qps,
        throughput=metrics.throughput,
        p90_seconds=metrics.p90_seconds,
        sources=metrics.sources,
    )
    print(
        "Result: "
        f"qps={trial.qps}, throughput={trial.throughput:.2f}/s, "
        f"p90={trial.p90_seconds:.3f}s, "
        f"sources={', '.join(display_path(path) for path in trial.sources)}",
        flush=True,
    )
    return trial


def distance_from_target_band(trial: Trial, min_p90: float, max_p90: float) -> float:
    if trial.p90_seconds < min_p90:
        return min_p90 - trial.p90_seconds
    if trial.p90_seconds > max_p90:
        return trial.p90_seconds - max_p90
    return 0.0


def find_boundary(args: argparse.Namespace) -> int:
    config_path = resolve_boundary_config_path(args.config_path)
    result_dir = resolve_results_dir(config_path)
    lower = args.lower
    upper = args.upper
    tested: set[int] = set()
    trials: list[Trial] = []

    max_steps = args.max_steps
    if max_steps is None:
        max_steps = math.ceil(math.log2(upper - lower + 1)) + 1 if upper >= lower else 0

    print(
        f"Searching {display_path(config_path)} for p90 in "
        f"[{args.min_p90:.3f}s, {args.max_p90:.3f}s] with qps bounds [{lower}, {upper}]",
        flush=True,
    )
    print(f"Result logs will be read from {display_path(result_dir)}", flush=True)

    steps = 0
    while lower <= upper and steps < max_steps:
        qps = (lower + upper) // 2
        if qps in tested:
            break

        tested.add(qps)
        steps += 1
        try:
            trial = run_trial(args, config_path, result_dir, qps)
        except K6MetricsParseError as exc:
            upper = qps - 1
            print(
                "Could not parse k6 metrics at "
                f"qps={qps}; treating load as too high and lowering upper bound to {upper}. "
                f"Details: {exc}",
                flush=True,
            )
            continue
        trials.append(trial)

        if args.min_p90 <= trial.p90_seconds <= args.max_p90:
            print(
                "\nFound matching QPS: "
                f"qps={trial.qps}, throughput={trial.throughput:.2f}/s, "
                f"p90={trial.p90_seconds:.3f}s",
                flush=True,
            )
            return 0

        if trial.p90_seconds < args.min_p90:
            lower = qps + 1
            print(f"p90 below target; raising lower bound to {lower}", flush=True)
        else:
            upper = qps - 1
            print(f"p90 above target; lowering upper bound to {upper}", flush=True)

    if trials:
        best = min(trials, key=lambda trial: distance_from_target_band(trial, args.min_p90, args.max_p90))
        set_k6_qps(config_path, best.qps)
        print(
            "\nNo QPS in the target band was found. Closest trial: "
            f"qps={best.qps}, throughput={best.throughput:.2f}/s, "
            f"p90={best.p90_seconds:.3f}s",
            flush=True,
        )
    else:
        print("\nNo trials were run.", flush=True)
    return 2


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Binary-search k6_qps until p90 latency is inside a target band.",
    )
    parser.add_argument(
        "config_path",
        help=(
            "Run config YAML/JSON file containing k6_qps, or a run directory "
            "containing qps_*.yaml. Directories are tuned via qps_boundary.yaml."
        ),
    )
    parser.add_argument("--lower", type=int, default=DEFAULT_LOWER_QPS, help="Inclusive lower QPS bound.")
    parser.add_argument("--upper", type=int, default=DEFAULT_UPPER_QPS, help="Inclusive upper QPS bound.")
    parser.add_argument(
        "--min-p90",
        type=float,
        default=DEFAULT_MIN_P90_SECONDS,
        help="Minimum acceptable p90 latency in seconds.",
    )
    parser.add_argument(
        "--max-p90",
        type=float,
        default=DEFAULT_MAX_P90_SECONDS,
        help="Maximum acceptable p90 latency in seconds.",
    )
    parser.add_argument(
        "--node-count",
        type=int,
        default=DEFAULT_NODE_COUNT,
        help="Node count passed to setup/repo.py.",
    )
    parser.add_argument(
        "--python",
        dest="python_bin",
        default="python",
        help="Python executable used for setup/repo.py and main.py.",
    )
    parser.add_argument(
        "--repo-script",
        default=DEFAULT_REPO_SCRIPT,
        help=f"Repo upload script path relative to the repo root. Defaults to {DEFAULT_REPO_SCRIPT}.",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Maximum binary-search trials. Defaults to enough steps for the QPS range.",
    )
    args = parser.parse_args(argv)

    if args.lower < 0 or args.upper < 0:
        parser.error("--lower and --upper must be non-negative")
    if args.lower > args.upper:
        parser.error("--lower must be less than or equal to --upper")
    if args.min_p90 <= 0 or args.max_p90 <= 0:
        parser.error("--min-p90 and --max-p90 must be positive seconds")
    if args.min_p90 > args.max_p90:
        parser.error("--min-p90 must be less than or equal to --max-p90")
    if args.node_count <= 0:
        parser.error("--node-count must be positive")
    if args.max_steps is not None and args.max_steps <= 0:
        parser.error("--max-steps must be positive when provided")
    if not (REPO_ROOT / args.repo_script).is_file():
        parser.error(f"--repo-script does not exist: {args.repo_script}")

    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        return find_boundary(args)
    except BoundaryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
