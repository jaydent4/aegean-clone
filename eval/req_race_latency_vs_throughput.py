#!/usr/bin/env python3

import argparse
from pathlib import Path

from latency_vs_throughput_utils import (
    DEFAULT_RESULTS_ROOT,
    SeriesSpec,
    generate_comparison_plot,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot req_race latency against realized throughput for Aegean and Aegean+EO."
    )
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument(
        "--filename",
        default="req_race_direct_vs_eo_latency_vs_throughput.png",
    )
    args = parser.parse_args()

    output_path = generate_comparison_plot(
        title="Req Race Latency vs Realized Throughput",
        output_path=args.results_root / "req_race_aegean" / args.filename,
        series_specs=[
            SeriesSpec("Aegean", args.results_root / "req_race_aegean"),
            SeriesSpec("Aegean+EO", args.results_root / "req_race_aegean_eo"),
            SeriesSpec("Unreplicated", args.results_root / "req_race_unreplicated"),
        ],
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
