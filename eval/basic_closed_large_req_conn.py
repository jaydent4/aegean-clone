#!/usr/bin/env python3

import re
from pathlib import Path

from plot_utils import collect_data, plot_latency, plot_throughput


DEFAULT_RESULTS_DIR = Path(
    "/Users/jasonliu/Documents/VSCode/aegean-clone/results/basic_closed_large_req_conn"
)

CONN_DIR_RE = re.compile(r"conn_(\d+)$")


def main() -> None:
    rows = collect_data(DEFAULT_RESULTS_DIR, "conn_*/node0.log", CONN_DIR_RE)
    DEFAULT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    throughput_path = DEFAULT_RESULTS_DIR / "throughput_vs_connections.png"
    latency_path = DEFAULT_RESULTS_DIR / "latency_vs_connections.png"

    plot_throughput(rows, throughput_path, "Connections", "Throughput vs Connections")
    plot_latency(rows, latency_path, "Connections", "Latency vs Connections")

    print(f"Wrote {throughput_path}")
    print(f"Wrote {latency_path}")


if __name__ == "__main__":
    main()
