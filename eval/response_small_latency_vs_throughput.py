#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt

from plot_utils import parse_metrics_log


REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_ROOT = REPO_ROOT / "results"
OUTPUT_PATH = RESULTS_ROOT / "response_small_aegean" / "latency_vs_throughput.png"

SERIES = [
    (
        "Aegean",
        RESULTS_ROOT / "response_small_aegean",
        [1000, 1500, 2000, 2500, 3000],
        "#08306b",
    ),
    (
        "Aegean+EO",
        RESULTS_ROOT / "response_small_aegean_eo",
        [500, 1000, 1500, 2000, 2500],
        "#6baed6",
    ),
    (
        "PBEO",
        RESULTS_ROOT / "response_small_pbeo",
        [1500, 3000, 5500, 6000, 6300],
        "#238b45",
    ),
    (
        "Unreplicated",
        RESULTS_ROOT / "response_small_unreplicated",
        [3000, 6000, 8000, 9000],
        "#555555",
    ),
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
        is_unreplicated = label == "Unreplicated"

        ax.plot(
            throughputs,
            medians,
            marker="o",
            linewidth=3.2 if is_unreplicated else 2.4,
            markersize=7 if is_unreplicated else 6,
            color=color,
            label=f"{label} Median",
            zorder=4 if is_unreplicated else 2,
        )
        ax.plot(
            throughputs,
            p90s,
            marker="s",
            linewidth=3.2 if is_unreplicated else 2.4,
            markersize=6 if is_unreplicated else 5,
            linestyle=":",
            color=color,
            label=f"{label} P90",
            zorder=4 if is_unreplicated else 2,
        )

    ax.set_xlabel("Realized Throughput (req/s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Response Small Latency vs Realized Throughput")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 400)
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
