#!/usr/bin/env python3
"""Gather Fibonacci-spaced lower-QPS runs from an existing boundary QPS."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from find_boundary import (
    DEFAULT_NODE_COUNT,
    K6_QPS_YAML_RE,
    BoundaryError,
    display_path,
    set_k6_qps,
)
from find_boundary_and_gather import (
    BOUNDARY_CONFIG_NAME,
    DEFAULT_REPO_SCRIPT,
    GatherError,
    clear_non_boundary_qps_configs,
    gather_runs,
    qps_config_paths,
    remove_boundary_config,
    remove_boundary_result,
    resolve_run_dir,
)


REPO_ROOT = Path(__file__).resolve().parent


def read_k6_qps(config_path: Path) -> int:
    text = config_path.read_text(encoding="utf-8")
    if config_path.suffix.lower() == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise GatherError(f"config file must contain a JSON object: {display_path(config_path)}")
        raw_qps = data.get("k6_qps")
    else:
        match = K6_QPS_YAML_RE.search(text)
        if match is None:
            raise GatherError(f"config file does not contain a top-level k6_qps line: {display_path(config_path)}")
        raw_qps = match.group(2).strip()

    try:
        qps = int(raw_qps)
    except (TypeError, ValueError) as exc:
        raise GatherError(f"k6_qps must be an integer in {display_path(config_path)}: {raw_qps!r}") from exc
    if qps <= 0:
        raise GatherError(f"k6_qps must be positive in {display_path(config_path)}: {qps}")
    return qps


def pick_boundary_source(run_dir: Path, explicit_path: str | None, boundary_qps: int | None) -> Path:
    if explicit_path is not None:
        config_path = Path(explicit_path).expanduser().resolve()
        if not config_path.is_file():
            raise GatherError(f"boundary config does not exist or is not a file: {config_path}")
        return config_path

    boundary_path = run_dir / BOUNDARY_CONFIG_NAME
    if boundary_path.is_file():
        return boundary_path

    candidates = qps_config_paths(run_dir)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise GatherError(f"{display_path(run_dir)} must contain at least one qps_*.yaml file")
    if boundary_qps is not None:
        return candidates[0]
    raise GatherError(
        f"{display_path(run_dir)} contains multiple qps_*.yaml files; "
        "pass --boundary-config or create qps_boundary.yaml"
    )


def prepare_boundary_config(args: argparse.Namespace, run_dir: Path) -> tuple[Path, int]:
    source_path = pick_boundary_source(run_dir, args.boundary_config, args.boundary_qps)
    boundary_path = run_dir / BOUNDARY_CONFIG_NAME
    if source_path != boundary_path:
        shutil.copy2(source_path, boundary_path)
        print(
            f"Boundary config: copied {display_path(source_path)} -> {display_path(boundary_path)}",
            flush=True,
        )
    else:
        print(f"Boundary config: using {display_path(boundary_path)}", flush=True)

    boundary_qps = args.boundary_qps if args.boundary_qps is not None else read_k6_qps(boundary_path)
    set_k6_qps(boundary_path, boundary_qps)
    return boundary_path, boundary_qps


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.boundary_qps is not None and args.boundary_qps <= 0:
        parser.error("--boundary-qps must be positive when provided")
    if args.node_count <= 0:
        parser.error("--node-count must be positive")
    if not (REPO_ROOT / args.repo_script).is_file():
        parser.error(f"--repo-script does not exist: {args.repo_script}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create and run Fibonacci-spaced lower-QPS configs from a boundary QPS.",
    )
    parser.add_argument("run_dir", help="Directory containing qps_*.yaml run configs.")
    parser.add_argument(
        "--boundary-config",
        help=(
            "Config to copy to qps_boundary.yaml before gathering. "
            "Defaults to qps_boundary.yaml when present, otherwise the only qps_*.yaml in run_dir."
        ),
    )
    parser.add_argument(
        "--boundary-qps",
        type=int,
        default=None,
        help="Boundary QPS to gather below. Defaults to k6_qps from the boundary config.",
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
    args = parser.parse_args(argv)
    validate_args(args, parser)
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    boundary_path: Path | None = None
    exit_code = 0
    try:
        run_dir = resolve_run_dir(args.run_dir)
        boundary_path, boundary_qps = prepare_boundary_config(args, run_dir)
        clear_non_boundary_qps_configs(run_dir, boundary_path)
        gather_runs(args, boundary_path, boundary_qps)
    except (BoundaryError, GatherError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        exit_code = 1

    if not remove_boundary_result(boundary_path):
        exit_code = 1
    if not remove_boundary_config(boundary_path):
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
