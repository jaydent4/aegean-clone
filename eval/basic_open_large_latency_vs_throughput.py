#!/usr/bin/env python3

import argparse
from pathlib import Path

from latency_vs_throughput_utils import (
    DEFAULT_FILENAME,
    DEFAULT_RESULTS_ROOT,
    SeriesSpec,
    generate_comparison_plot,
)


WORKLOAD_NAME = "basic_open_large"
DEFAULT_MIN_QPS = 250
DEFAULT_MAX_QPS = 400


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot basic_open_large latency against realized throughput."
    )
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--filename", default=DEFAULT_FILENAME)
    parser.add_argument("--min-qps", type=int, default=DEFAULT_MIN_QPS)
    parser.add_argument("--max-qps", type=int, default=DEFAULT_MAX_QPS)
    args = parser.parse_args()

    output_path = generate_comparison_plot(
        title="Basic Open Large Latency vs Realized Throughput",
        output_path=args.results_root / WORKLOAD_NAME / args.filename,
        series_specs=[
            SeriesSpec("Aegean", args.results_root / WORKLOAD_NAME),
            SeriesSpec("Aegean+EO", args.results_root / f"{WORKLOAD_NAME}_eo"),
            SeriesSpec("PBEO", args.results_root / f"{WORKLOAD_NAME}_pbeo"),
            SeriesSpec("Unreplicated", args.results_root / f"{WORKLOAD_NAME}_unreplicated"),
        ],
        min_offered_qps=args.min_qps,
        max_offered_qps=args.max_qps,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
