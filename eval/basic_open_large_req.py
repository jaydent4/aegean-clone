#!/usr/bin/env python3

import re
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required to generate plots. Install it with `python3 -m pip install matplotlib`."
    ) from exc


DEFAULT_RESULTS_DIR = Path(
    "/Users/jasonliu/Documents/VSCode/aegean-clone/results/basic_open_large_req"
)

QPS_DIR_RE = re.compile(r"qps_(\d+)$")
THROUGHPUT_RE = re.compile(r"http_reqs\.*:\s+\d+\s+([0-9.]+)/s")
LATENCY_RE = re.compile(r"http_req_duration\.*:.*?\bmed=([0-9.]+)ms\b.*?\bp\(90\)=([0-9.]+)ms\b")


def parse_node0_log(log_path: Path) -> tuple[float, float, float]:
    text = log_path.read_text()

    qps_match = QPS_DIR_RE.search(log_path.parent.name)
    throughput_match = THROUGHPUT_RE.search(text)
    latency_match = LATENCY_RE.search(text)

    if not qps_match:
        raise ValueError(f"Could not parse qps from directory name: {log_path.parent.name}")
    if not throughput_match:
        raise ValueError(f"Could not find http_reqs throughput in {log_path}")
    if not latency_match:
        raise ValueError(f"Could not find median/p90 latency in {log_path}")

    qps = float(qps_match.group(1))
    throughput = float(throughput_match.group(1))
    median_ms = float(latency_match.group(1))
    p90_ms = float(latency_match.group(2))
    return qps, throughput, median_ms, p90_ms


def collect_data(results_dir: Path) -> list[tuple[float, float, float, float]]:
    rows = []
    for log_path in sorted(results_dir.glob("qps_*/node0.log")):
        qps, throughput, median_ms, p90_ms = parse_node0_log(log_path)
        rows.append((qps, throughput, median_ms, p90_ms))

    if not rows:
        raise ValueError(f"No node0.log files found under {results_dir}")

    rows.sort(key=lambda row: row[0])
    return rows


def plot_throughput(rows: list[tuple[float, float, float, float]], output_path: Path) -> None:
    qps = [row[0] for row in rows]
    throughput = [row[1] for row in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(qps, throughput, marker="o", linewidth=2)
    plt.xlabel("QPS")
    plt.ylabel("Throughput (req/s)")
    plt.title("Throughput vs QPS")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def plot_latency(rows: list[tuple[float, float, float, float]], output_path: Path) -> None:
    qps = [row[0] for row in rows]
    median_ms = [row[2] for row in rows]
    p90_ms = [row[3] for row in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(qps, median_ms, marker="o", linewidth=2, label="Median")
    plt.plot(qps, p90_ms, marker="o", linewidth=2, label="P90")
    plt.xlabel("QPS")
    plt.ylabel("Latency (ms)")
    plt.title("Latency vs QPS")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def main() -> None:
    rows = collect_data(DEFAULT_RESULTS_DIR)
    DEFAULT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    throughput_path = DEFAULT_RESULTS_DIR / "throughput_vs_qps.png"
    latency_path = DEFAULT_RESULTS_DIR / "latency_vs_qps.png"

    plot_throughput(rows, throughput_path)
    plot_latency(rows, latency_path)

    print(f"Wrote {throughput_path}")
    print(f"Wrote {latency_path}")


if __name__ == "__main__":
    main()
