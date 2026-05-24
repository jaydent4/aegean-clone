#!/usr/bin/env python3
"""Find a boundary QPS, then gather Fibonacci-spaced lower-QPS runs."""

from __future__ import annotations

import argparse
import math
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from find_boundary import (
    DEFAULT_LOWER_QPS,
    DEFAULT_MAX_P90_SECONDS,
    DEFAULT_MIN_P90_SECONDS,
    DEFAULT_NODE_COUNT,
    DEFAULT_UPPER_QPS,
    BoundaryError,
    K6MetricsParseError,
    Trial,
    display_path,
    distance_from_target_band,
    parse_fresh_result_metrics,
    resolve_results_dir,
    run_streaming_command,
    set_k6_qps,
)


REPO_ROOT = Path(__file__).resolve().parent
BOUNDARY_CONFIG_NAME = "qps_boundary.yaml"
DEFAULT_REPO_SCRIPT = "eval/repo.py" if (REPO_ROOT / "eval" / "repo.py").is_file() else "setup/repo.py"


class GatherError(RuntimeError):
    pass


@dataclass(frozen=True)
class BoundaryResult:
    qps: int
    trial: Trial


def resolve_run_dir(run_dir_arg: str) -> Path:
    run_dir = Path(run_dir_arg).expanduser().resolve()
    if not run_dir.is_dir():
        raise GatherError(f"run directory does not exist or is not a directory: {run_dir}")
    return run_dir


def qps_config_paths(run_dir: Path) -> list[Path]:
    return sorted(path for path in run_dir.glob("qps_*.yaml") if path.is_file())


def pick_source_qps_config(run_dir: Path) -> Path:
    candidates = qps_config_paths(run_dir)
    if not candidates:
        raise GatherError(f"{display_path(run_dir)} must contain at least one qps_*.yaml file")

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


def run_trial(args: argparse.Namespace, config_path: Path, result_dir: Path, qps: int) -> Trial:
    print(f"\n=== Boundary trial k6_qps={qps} ===", flush=True)
    set_k6_qps(config_path, qps)

    run_streaming_command(["git", "add", "."], cwd=REPO_ROOT)
    run_streaming_command([args.python_bin, args.repo_script, str(args.node_count), "--upload"], cwd=REPO_ROOT)

    started_at = time.time()
    run_streaming_command([args.python_bin, "main.py", str(config_path)], cwd=REPO_ROOT)
    metrics = parse_fresh_result_metrics(result_dir, started_at)

    trial = Trial(
        qps=qps,
        throughput=metrics.throughput,
        p90_seconds=metrics.p90_seconds,
        sources=metrics.sources,
    )
    print(
        "Boundary trial result: "
        f"qps={trial.qps}, throughput={trial.throughput:.2f}/s, "
        f"p90={trial.p90_seconds:.3f}s, "
        f"sources={', '.join(display_path(path) for path in trial.sources)}",
        flush=True,
    )
    return trial


def find_boundary_qps(args: argparse.Namespace, config_path: Path) -> BoundaryResult:
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
    print(f"Boundary result logs will be read from {display_path(result_dir)}", flush=True)

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
                "\nFound boundary QPS: "
                f"qps={trial.qps}, throughput={trial.throughput:.2f}/s, "
                f"p90={trial.p90_seconds:.3f}s",
                flush=True,
            )
            set_k6_qps(config_path, trial.qps)
            return BoundaryResult(qps=trial.qps, trial=trial)

        if trial.p90_seconds < args.min_p90:
            lower = qps + 1
            print(f"p90 below target; raising lower bound to {lower}", flush=True)
        else:
            upper = qps - 1
            print(f"p90 above target; lowering upper bound to {upper}", flush=True)

    if trials:
        best = min(trials, key=lambda trial: distance_from_target_band(trial, args.min_p90, args.max_p90))
        set_k6_qps(config_path, best.qps)
        message = (
            "no QPS in the target band was found; closest trial was "
            f"qps={best.qps}, throughput={best.throughput:.2f}/s, p90={best.p90_seconds:.3f}s"
        )
    else:
        message = "no boundary trials were run"
    raise GatherError(message)


def clear_non_boundary_qps_configs(run_dir: Path, boundary_path: Path) -> None:
    removed = []
    for path in qps_config_paths(run_dir):
        if path.resolve() == boundary_path.resolve():
            continue
        path.unlink()
        removed.append(path)

    if removed:
        print(
            "Removed old QPS configs: "
            + ", ".join(display_path(path) for path in removed),
            flush=True,
        )
    else:
        print("No old QPS configs to remove.", flush=True)


def fibonacci_numbers():
    previous = 0
    current = 1
    yield previous
    while True:
        yield current
        previous, current = current, previous + current


def gathered_qps_values(boundary_qps: int) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    fib_sum = 0

    for fib in fibonacci_numbers():
        fib_sum += fib
        raw_qps = ((100 - fib_sum) / 100.0) * boundary_qps
        if raw_qps <= 0:
            break

        qps = int(round(raw_qps))
        if qps <= 0 or qps in seen:
            continue

        values.append(qps)
        seen.add(qps)

    return values


def create_gather_config(boundary_path: Path, qps: int) -> Path:
    config_path = boundary_path.with_name(f"qps_{qps}.yaml")
    shutil.copy2(boundary_path, config_path)
    set_k6_qps(config_path, qps)
    return config_path


def run_gather_config(args: argparse.Namespace, config_path: Path) -> None:
    print(f"\n=== Gathering {display_path(config_path)} ===", flush=True)
    run_streaming_command(["git", "add", "."], cwd=REPO_ROOT)
    run_streaming_command([args.python_bin, args.repo_script, str(args.node_count), "--upload"], cwd=REPO_ROOT)
    run_streaming_command([args.python_bin, "main.py", str(config_path)], cwd=REPO_ROOT)


def gather_runs(args: argparse.Namespace, boundary_path: Path, boundary_qps: int) -> None:
    qps_values = gathered_qps_values(boundary_qps)
    if not qps_values:
        raise GatherError(f"boundary QPS {boundary_qps} produced no positive gather QPS values")

    print(
        f"Gathering Fibonacci-spaced QPS values from boundary {boundary_qps}: "
        + ", ".join(str(qps) for qps in qps_values),
        flush=True,
    )

    for qps in qps_values:
        config_path = create_gather_config(boundary_path, qps)
        run_gather_config(args, config_path)


def remove_boundary_config(boundary_path: Path | None) -> bool:
    if boundary_path is None or not boundary_path.exists():
        return True
    try:
        boundary_path.unlink()
    except OSError as exc:
        print(f"error: could not remove {display_path(boundary_path)}: {exc}", file=sys.stderr)
        return False

    print(f"\nRemoved boundary config {display_path(boundary_path)}", flush=True)
    return True


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
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


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find a boundary qps_*.yaml config, replace the directory's QPS configs, and gather lower-QPS runs.",
    )
    parser.add_argument("run_dir", help="Directory containing qps_*.yaml run configs.")
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
        help="Node count passed to the repo upload script.",
    )
    parser.add_argument(
        "--python",
        dest="python_bin",
        default="python",
        help="Python executable used for the repo upload script and main.py.",
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
    validate_args(args, parser)
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    boundary_path: Path | None = None
    exit_code = 0
    try:
        run_dir = resolve_run_dir(args.run_dir)
        boundary_path = create_boundary_config(run_dir)
        boundary_result = find_boundary_qps(args, boundary_path)
        clear_non_boundary_qps_configs(run_dir, boundary_path)
        gather_runs(args, boundary_path, boundary_result.qps)
    except (BoundaryError, GatherError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        exit_code = 1

    if not remove_boundary_config(boundary_path):
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
