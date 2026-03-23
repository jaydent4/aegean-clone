import re
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required to generate plots. Install it with `python3 -m pip install matplotlib`."
    ) from exc


THROUGHPUT_RE = re.compile(r"http_reqs\.*:\s+\d+\s+([0-9.]+)/s")
LATENCY_RE = re.compile(r"http_req_duration\.*:.*?\bmed=([0-9.]+)ms\b.*?\bp\(90\)=([0-9.]+)ms\b")


def parse_metric_log(log_path: Path, x_axis_re: re.Pattern[str]) -> tuple[float, float, float, float]:
    text = log_path.read_text()

    x_axis_match = x_axis_re.search(log_path.parent.name)
    throughput_match = THROUGHPUT_RE.search(text)
    latency_match = LATENCY_RE.search(text)

    if not x_axis_match:
        raise ValueError(f"Could not parse x-axis value from directory name: {log_path.parent.name}")
    if not throughput_match:
        raise ValueError(f"Could not find http_reqs throughput in {log_path}")
    if not latency_match:
        raise ValueError(f"Could not find median/p90 latency in {log_path}")

    x_axis_value = float(x_axis_match.group(1))
    throughput = float(throughput_match.group(1))
    median_ms = float(latency_match.group(1))
    p90_ms = float(latency_match.group(2))
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


def plot_throughput(
    rows: list[tuple[float, float, float, float]],
    output_path: Path,
    x_label: str,
    title: str,
) -> None:
    x_values = [row[0] for row in rows]
    throughput = [row[1] for row in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(x_values, throughput, marker="o", linewidth=2)
    plt.xlabel(x_label)
    plt.ylabel("Throughput (req/s)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def plot_latency(
    rows: list[tuple[float, float, float, float]],
    output_path: Path,
    x_label: str,
    title: str,
) -> None:
    x_values = [row[0] for row in rows]
    median_ms = [row[2] for row in rows]
    p90_ms = [row[3] for row in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(x_values, median_ms, marker="o", linewidth=2, label="Median")
    plt.plot(x_values, p90_ms, marker="o", linewidth=2, label="P90")
    plt.xlabel(x_label)
    plt.ylabel("Latency (ms)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
