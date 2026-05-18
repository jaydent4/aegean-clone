from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required. Install with: pip install matplotlib"
    ) from exc

from plot_utils import parse_metrics_log


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_ROOT = REPO_ROOT / "results"
DEFAULT_FILENAME = "latency_vs_throughput.png"


@dataclass(frozen=True)
class MetricPoint:
    offered_qps: int
    throughput: float
    median_ms: float
    p90_ms: float


QPS_DIR_RE = re.compile(r"qps_(\d+)$")
SERIES_COLORS = {
    "Aegean": "#08306b",
    "Aegean+EO": "#6baed6",
    "PBEO": "#238b45",
    "Unreplicated": "#555555",
}
SERIES_SUFFIXES = [
    ("Aegean", "_aegean"),
    ("Aegean+EO", "_aegean_eo"),
    ("PBEO", "_pbeo"),
    ("Unreplicated", "_unreplicated"),
]


def humanize_workload_name(workload_name: str) -> str:
    return workload_name.replace("_", " ").title()


def series_label(series_dir: Path, workload_name: str) -> str:
    for label, suffix in SERIES_SUFFIXES:
        if series_dir.name == f"{workload_name}{suffix}":
            return label
    if series_dir.name == f"{workload_name}_eo":
        return "Aegean+EO"
    if series_dir.name == f"{workload_name}_unreplicated":
        return "Unreplicated"
    return "Aegean"


def existing_series_dirs(results_root: Path, workload_name: str) -> list[Path]:
    candidates = [
        *(results_root / f"{workload_name}{suffix}" for _, suffix in SERIES_SUFFIXES),
        results_root / workload_name,
        results_root / f"{workload_name}_eo",
    ]
    return [path for path in candidates if path.is_dir()]


def load_series_points(
    series_dir: Path,
    *,
    min_offered_qps: int | None = None,
    max_offered_qps: int | None = None,
    min_throughput: float | None = None,
    max_throughput: float | None = None,
    drop_collapse_tolerance: float | None = None,
) -> list[MetricPoint]:
    points: list[MetricPoint] = []

    for log_path in sorted(series_dir.glob("qps_*/node0.log")):
        qps_match = QPS_DIR_RE.match(log_path.parent.name)
        if not qps_match:
            continue
        offered_qps = int(qps_match.group(1))
        if min_offered_qps is not None and offered_qps < min_offered_qps:
            continue
        if max_offered_qps is not None and offered_qps > max_offered_qps:
            continue
        try:
            throughput, median_ms, p90_ms = parse_metrics_log(log_path)
        except ValueError:
            continue
        if min_throughput is not None and throughput < min_throughput:
            continue
        if max_throughput is not None and throughput > max_throughput:
            continue
        points.append(
            MetricPoint(
                offered_qps=offered_qps,
                throughput=throughput,
                median_ms=median_ms,
                p90_ms=p90_ms,
            )
        )

    if not points:
        raise ValueError(f"No complete client logs found under {series_dir}")

    points.sort(key=lambda point: point.offered_qps)
    if drop_collapse_tolerance is not None:
        filtered: list[MetricPoint] = []
        peak_throughput = 0.0
        for point in points:
            if peak_throughput > 0 and point.throughput < peak_throughput * drop_collapse_tolerance:
                continue
            filtered.append(point)
            peak_throughput = max(peak_throughput, point.throughput)
        points = filtered

    return points


def collect_series(results_root: Path, workload_name: str) -> dict[str, list[MetricPoint]]:
    series: dict[str, list[MetricPoint]] = {}

    for series_dir in existing_series_dirs(results_root, workload_name):
        try:
            points = load_series_points(series_dir)
        except ValueError:
            continue
        series[series_label(series_dir, workload_name)] = points

    if not series:
        raise ValueError(f"No complete result sets found for {workload_name}")

    return series


def plot_latency_vs_throughput(
    series: dict[str, list[MetricPoint]],
    output_path: Path,
    title: str,
) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 5.4))

    for label, points in series.items():
        throughput = [point.throughput for point in points]
        median_ms = [point.median_ms for point in points]
        p90_ms = [point.p90_ms for point in points]
        color = SERIES_COLORS.get(label)
        is_unreplicated = label == "Unreplicated"

        ax.plot(
            throughput,
            median_ms,
            marker="o",
            linewidth=3.2 if is_unreplicated else 2.4,
            markersize=7 if is_unreplicated else 6,
            color=color,
            label=f"{label} Median",
            zorder=4 if is_unreplicated else 2,
        )
        ax.plot(
            throughput,
            p90_ms,
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
    ax.set_title(title)
    ax.set_xlim(left=0)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(frameon=True, loc="upper left")
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def output_path_for(results_root: Path, workload_name: str, filename: str) -> Path:
    return results_root / workload_name / filename


def generate_workload_plot(
    workload_name: str,
    *,
    results_root: Path = DEFAULT_RESULTS_ROOT,
    filename: str = DEFAULT_FILENAME,
    min_offered_qps: int | None = None,
    max_offered_qps: int | None = None,
    min_throughput: float | None = None,
    max_throughput: float | None = None,
    drop_collapse_tolerance: float | None = None,
) -> Path:
    series: dict[str, list[MetricPoint]] = {}
    for series_dir in existing_series_dirs(results_root, workload_name):
        try:
            points = load_series_points(
                series_dir,
                min_offered_qps=min_offered_qps,
                max_offered_qps=max_offered_qps,
                min_throughput=min_throughput,
                max_throughput=max_throughput,
                drop_collapse_tolerance=drop_collapse_tolerance,
            )
        except ValueError:
            continue
        if points:
            series[series_label(series_dir, workload_name)] = points
    if not series:
        raise ValueError(f"No complete result sets found for {workload_name}")
    output_path = output_path_for(results_root, workload_name, filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_latency_vs_throughput(
        series,
        output_path,
        f"{humanize_workload_name(workload_name)} Latency vs Realized Throughput",
    )
    return output_path


@dataclass(frozen=True)
class SeriesSpec:
    label: str
    series_dir: Path


def collect_named_series(
    series_specs: Iterable[SeriesSpec],
    *,
    min_offered_qps: int | None = None,
    max_offered_qps: int | None = None,
    min_throughput: float | None = None,
    max_throughput: float | None = None,
    drop_collapse_tolerance: float | None = None,
) -> dict[str, list[MetricPoint]]:
    series: dict[str, list[MetricPoint]] = {}

    for series_spec in series_specs:
        if not series_spec.series_dir.is_dir():
            print(f"Skipping {series_spec.label}: missing directory {series_spec.series_dir}")
            continue
        try:
            points = load_series_points(
                series_spec.series_dir,
                min_offered_qps=min_offered_qps,
                max_offered_qps=max_offered_qps,
                min_throughput=min_throughput,
                max_throughput=max_throughput,
                drop_collapse_tolerance=drop_collapse_tolerance,
            )
        except ValueError as exc:
            print(f"Skipping {series_spec.label}: {exc}")
            continue
        series[series_spec.label] = points

    if not series:
        raise ValueError("No complete result sets found for requested series")

    return series


def generate_comparison_plot(
    *,
    title: str,
    output_path: Path,
    series_specs: Iterable[SeriesSpec],
    min_offered_qps: int | None = None,
    max_offered_qps: int | None = None,
    min_throughput: float | None = None,
    max_throughput: float | None = None,
    drop_collapse_tolerance: float | None = None,
) -> Path:
    series = collect_named_series(
        series_specs,
        min_offered_qps=min_offered_qps,
        max_offered_qps=max_offered_qps,
        min_throughput=min_throughput,
        max_throughput=max_throughput,
        drop_collapse_tolerance=drop_collapse_tolerance,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_latency_vs_throughput(series, output_path, title)
    return output_path
