#!/usr/bin/env python3

import argparse
from pathlib import Path

from latency_vs_throughput_utils import (
    DEFAULT_FILENAME,
    DEFAULT_RESULTS_ROOT,
    generate_workload_plot,
)


WORKLOAD_NAME = "worker_8"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot worker_8 latency against realized throughput."
    )
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--filename", default=DEFAULT_FILENAME)
    args = parser.parse_args()

    output_path = generate_workload_plot(
        WORKLOAD_NAME,
        results_root=args.results_root,
        filename=args.filename,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
