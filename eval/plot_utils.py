import re
import statistics
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required. Install with: pip install matplotlib"
    ) from exc


THROUGHPUT_RE = re.compile(r"http_reqs\.*:\s+\d+\s+([0-9.]+)/s")
LATENCY_RE = re.compile(
    r"http_req_duration\.*:.*?\bmed=([0-9.]+)(µs|us|ms|s)\b.*?\bp\(90\)=([0-9.]+)(µs|us|ms|s)\b"
)


def duration_to_ms(value: str, unit: str) -> float:
    duration = float(value)
    if unit in ("µs", "us"):
        return duration / 1000
    if unit == "s":
        return duration * 1000
    return duration


def extract_throughput_and_latency(text: str, *, source: str = "<text>") -> tuple[float, float, float]:
    throughput_match = THROUGHPUT_RE.search(text)
    latency_match = LATENCY_RE.search(text)

    if not throughput_match:
        raise ValueError(f"Could not find http_reqs throughput in {source}")
    if not latency_match:
        raise ValueError(f"Could not find median/p90 latency in {source}")

    throughput = float(throughput_match.group(1))
    median_ms = duration_to_ms(latency_match.group(1), latency_match.group(2))
    p90_ms = duration_to_ms(latency_match.group(3), latency_match.group(4))
    return throughput, median_ms, p90_ms


def parse_metrics_log(log_path: Path) -> tuple[float, float, float]:
    return extract_throughput_and_latency(log_path.read_text(), source=str(log_path))


def parse_metric_log(log_path: Path, x_axis_re: re.Pattern[str]) -> tuple[float, float, float, float]:
    text = log_path.read_text()

    x_axis_match = x_axis_re.search(log_path.parent.name)

    if not x_axis_match:
        raise ValueError(f"Could not parse x-axis value from directory name: {log_path.parent.name}")

    x_axis_value = float(x_axis_match.group(1))
    throughput, median_ms, p90_ms = parse_metrics_log(log_path)
    return x_axis_value, throughput, median_ms, p90_ms


def collect_data(
    results_dir: Path, directory_glob: str, x_axis_re: re.Pattern[str]
) -> list[tuple[float, float, float, float]]:
    rows = []
    for log_path in sorted(results_dir.glob(directory_glob)):
        rows.append(parse_metric_log(log_path, x_axis_re))

    if not rows:
        raise ValueError(f"No node0.log files found under {results_dir}")

    rows.sort(key=lambda row: row[0])
    return rows


def collect_data_aggregated(
    results_dir: Path, directory_glob: str, x_axis_re: re.Pattern[str]
) -> list[tuple[float, float, float, float, float, float, float]]:
    """Collect from multiple runs per x-axis point and aggregate.

    Expects: qps_400/20260327_142649/node0.log
    The directory_glob should match the inner logs, e.g. "qps_*/*/node0.log".

    Returns: (x_value, throughput_mean, throughput_stdev,
              median_mean, median_stdev, p90_mean, p90_stdev)
    """
    grouped: dict[float, list[tuple[float, float, float]]] = {}
    for log_path in sorted(results_dir.glob(directory_glob)):
        x_axis_dir = log_path.parent.parent.name
        x_axis_match = x_axis_re.search(x_axis_dir)
        if not x_axis_match:
            continue

        try:
            throughput, median_ms, p90_ms = parse_metrics_log(log_path)
        except ValueError:
            continue

        x_val = float(x_axis_match.group(1))

        grouped.setdefault(x_val, []).append((throughput, median_ms, p90_ms))

    if not grouped:
        raise ValueError(f"No log files found under {results_dir}")

    rows = []
    for x_val in sorted(grouped):
        samples = grouped[x_val]
        tp = [s[0] for s in samples]
        med = [s[1] for s in samples]
        p90 = [s[2] for s in samples]
        rows.append((
            x_val,
            statistics.mean(tp),
            statistics.stdev(tp) if len(tp) > 1 else 0.0,
            statistics.mean(med),
            statistics.stdev(med) if len(med) > 1 else 0.0,
            statistics.mean(p90),
            statistics.stdev(p90) if len(p90) > 1 else 0.0,
        ))

    return rows


def plot_throughput(rows, output_path: Path, x_label: str, title: str, yerr=None) -> None:
    x_values = [row[0] for row in rows]
    throughput = [row[1] for row in rows]

    plt.figure(figsize=(8, 5))
    if yerr is not None:
        plt.errorbar(x_values, throughput, yerr=yerr, marker="o", linewidth=2, capsize=4)
    else:
        plt.plot(x_values, throughput, marker="o", linewidth=2)
    plt.xlabel(x_label)
    plt.ylabel("Throughput (req/s)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def plot_latency(rows, output_path: Path, x_label: str, title: str,
                 yerr_median=None, yerr_p90=None) -> None:
    x_values = [row[0] for row in rows]
    median_ms = [row[2] for row in rows]
    p90_ms = [row[3] for row in rows]

    plt.figure(figsize=(8, 5))
    if yerr_median is not None:
        plt.errorbar(x_values, median_ms, yerr=yerr_median, marker="o", linewidth=2, capsize=4, label="Median")
    else:
        plt.plot(x_values, median_ms, marker="o", linewidth=2, label="Median")
    if yerr_p90 is not None:
        plt.errorbar(x_values, p90_ms, yerr=yerr_p90, marker="o", linewidth=2, capsize=4, label="P90")
    else:
        plt.plot(x_values, p90_ms, marker="o", linewidth=2, label="P90")
    plt.xlabel(x_label)
    plt.ylabel("Latency (ms)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def plot_throughput_aggregated(
    rows: list[tuple[float, float, float, float, float, float, float]],
    output_path: Path, x_label: str, title: str,
) -> None:
    simple_rows = [(r[0], r[1]) for r in rows]
    yerr = [r[2] for r in rows]
    plot_throughput(simple_rows, output_path, x_label, title, yerr=yerr)


def plot_latency_aggregated(
    rows: list[tuple[float, float, float, float, float, float, float]],
    output_path: Path, x_label: str, title: str,
) -> None:
    simple_rows = [(r[0], None, r[3], r[5]) for r in rows]
    yerr_median = [r[4] for r in rows]
    yerr_p90 = [r[6] for r in rows]
    plot_latency(simple_rows, output_path, x_label, title,
                 yerr_median=yerr_median, yerr_p90=yerr_p90)
