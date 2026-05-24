#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

from plot_utils import (
    collect_data,
    collect_data_aggregated,
    plot_latency,
    plot_latency_aggregated,
    plot_throughput,
    plot_throughput_aggregated,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "basic_open_large_aegean"
QPS_DIR_RE = re.compile(r"qps_(\d+)$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--aggregated", action="store_true",
                        help="Use aggregated multi-run data (expects qps_*/*/node0.log)")
    args = parser.parse_args()

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    throughput_path = results_dir / "throughput_vs_qps.png"
    latency_path = results_dir / "latency_vs_qps.png"

    if args.aggregated:
        rows = collect_data_aggregated(results_dir, "qps_*/*/node0.log", QPS_DIR_RE)
        plot_throughput_aggregated(rows, throughput_path, "QPS", "Throughput vs QPS")
        plot_latency_aggregated(rows, latency_path, "QPS", "Latency vs QPS")
    else:
        rows = collect_data(results_dir, "qps_*/node0.log", QPS_DIR_RE)
        plot_throughput(rows, throughput_path, "QPS", "Throughput vs QPS")
        plot_latency(rows, latency_path, "QPS", "Latency vs QPS")

    print(f"Wrote {throughput_path}")
    print(f"Wrote {latency_path}")


if __name__ == "__main__":
    main()
