#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt

from plot_utils import parse_metrics_log


REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_ROOT = REPO_ROOT / "results"
OUTPUT_PATH = RESULTS_ROOT / "write_medium" / "latency_vs_throughput.png"

SERIES = [
    ("Aegean", RESULTS_ROOT / "write_medium_aegean", [5, 10, 15, 20, 22], "#08306b"),
    ("PBEO", RESULTS_ROOT / "write_medium_pbeo", [5, 10, 15, 20, 22, 24], "#238b45"),
]


def load_points(series_dir: Path, offered_qps_values: list[int]) -> list[tuple[int, float, float, float]]:
    points = []
    for offered_qps in offered_qps_values:
        log_path = series_dir / f"qps_{offered_qps}" / "node0.log"
        if not log_path.is_file():
            raise FileNotFoundError(log_path)
        throughput, median_ms, p90_ms = parse_metrics_log(log_path)
        points.append((offered_qps, throughput, median_ms, p90_ms))
    return points


def main() -> int:
    fig, ax = plt.subplots(figsize=(8.5, 5.4))

    for label, series_dir, offered_qps_values, color in SERIES:
        points = load_points(series_dir, offered_qps_values)
        throughputs = [point[1] for point in points]
        medians = [point[2] for point in points]
        p90s = [point[3] for point in points]

        ax.plot(
            throughputs,
            medians,
            marker="o",
            linewidth=2.4,
            markersize=6,
            color=color,
            label=f"{label} Median",
        )
        ax.plot(
            throughputs,
            p90s,
            marker="s",
            linewidth=2.4,
            markersize=5,
            linestyle=":",
            color=color,
            label=f"{label} P90",
        )

    ax.set_xlabel("Realized Throughput (req/s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Write Medium Latency vs Realized Throughput")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 1000)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(frameon=True, loc="upper left")
    fig.tight_layout()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_PATH, dpi=220)
    plt.close(fig)
    print(f"Wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
