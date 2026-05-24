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
        description="Plot non-idempotent external service latency against realized throughput for Aegean and Aegean+EO."
    )
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument(
        "--filename",
        default="nonidem_external_srv_direct_vs_eo_latency_vs_throughput.png",
    )
    args = parser.parse_args()

    output_path = generate_comparison_plot(
        title="Non-Idempotent External Service Latency vs Realized Throughput",
        output_path=args.results_root / "nonidem_external_srv_aegean" / args.filename,
        series_specs=[
            SeriesSpec("Aegean", args.results_root / "nonidem_external_srv_aegean"),
            SeriesSpec("Aegean+EO", args.results_root / "nonidem_external_srv_aegean_eo"),
            SeriesSpec("Unreplicated", args.results_root / "nonidem_external_srv_unreplicated"),
        ],
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
