#!/usr/bin/env python3

import re
from pathlib import Path

from plot_utils import collect_data, plot_latency, plot_throughput


DEFAULT_RESULTS_DIR = Path(
    "/Users/jasonliu/Documents/VSCode/aegean-clone/results/basic_open_large_req"
)

QPS_DIR_RE = re.compile(r"qps_(\d+)$")


def main() -> None:
    rows = collect_data(DEFAULT_RESULTS_DIR, "qps_*/node0.log", QPS_DIR_RE)
    DEFAULT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    throughput_path = DEFAULT_RESULTS_DIR / "throughput_vs_qps.png"
    latency_path = DEFAULT_RESULTS_DIR / "latency_vs_qps.png"

    plot_throughput(rows, throughput_path, "QPS", "Throughput vs QPS")
    plot_latency(rows, latency_path, "QPS", "Latency vs QPS")

    print(f"Wrote {throughput_path}")
    print(f"Wrote {latency_path}")


if __name__ == "__main__":
    main()
