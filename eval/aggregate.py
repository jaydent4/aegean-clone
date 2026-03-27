#!/usr/bin/env python3
"""
Aggregate results from multiple experiment runs.

Usage:
    python3 eval/aggregate.py <aggregated_results.json>
    python3 eval/aggregate.py <run_dir1> <run_dir2> ... [--client node0] [--output-dir <dir>]
"""

import argparse
import json
import os
import re
import statistics
from pathlib import Path


def _parse_k6_duration(value_str, unit_str):
    """Convert a k6 duration value+unit to seconds."""
    v = float(value_str)
    unit = unit_str.lower().strip()
    if unit == "ms":
        return v / 1000.0
    if unit in ("µs", "us"):
        return v / 1_000_000.0
    return v


def parse_client_log(log_path):
    text = Path(log_path).read_text()
    metrics = {}

    # --- Try k6 format first ---
    k6_duration_re = re.search(
        r"http_req_duration\.+:\s+"
        r"avg=([0-9.]+)(ms|s|µs|us)\s+"
        r"min=([0-9.]+)(ms|s|µs|us)\s+"
        r"med=([0-9.]+)(ms|s|µs|us)\s+"
        r"max=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(90\)=([0-9.]+)(ms|s|µs|us)\s+"
        r"p\(95\)=([0-9.]+)(ms|s|µs|us)",
        text,
    )
    if k6_duration_re:
        metrics["average_sec"] = _parse_k6_duration(k6_duration_re.group(1), k6_duration_re.group(2))
        metrics["fastest_sec"] = _parse_k6_duration(k6_duration_re.group(3), k6_duration_re.group(4))
        metrics["p50"] = _parse_k6_duration(k6_duration_re.group(5), k6_duration_re.group(6))
        metrics["slowest_sec"] = _parse_k6_duration(k6_duration_re.group(7), k6_duration_re.group(8))
        metrics["p90"] = _parse_k6_duration(k6_duration_re.group(9), k6_duration_re.group(10))
        metrics["p95"] = _parse_k6_duration(k6_duration_re.group(11), k6_duration_re.group(12))

    k6_reqs_re = re.search(r"http_reqs\.+:\s+(\d+)\s+([0-9.]+)/s", text)
    if k6_reqs_re:
        metrics["total_requests"] = int(k6_reqs_re.group(1))
        metrics["requests_sec"] = float(k6_reqs_re.group(2))

    k6_failed_re = re.search(r"http_req_failed\.+:\s+([0-9.]+)%", text)
    if k6_failed_re:
        metrics["success_rate"] = 100.0 - float(k6_failed_re.group(1))

    if metrics:
        return metrics

    # --- Fallback: oha-style format ---
    for key, pattern in [
        ("success_rate", r"Success rate:\s+([0-9.]+)%"),
        ("total_sec", r"Total:\s+([0-9.]+) sec"),
        ("slowest_sec", r"Slowest:\s+([0-9.]+) sec"),
        ("fastest_sec", r"Fastest:\s+([0-9.]+) sec"),
        ("average_sec", r"Average:\s+([0-9.]+) sec"),
        ("requests_sec", r"Requests/sec:\s+([0-9.]+)"),
    ]:
        m = re.search(pattern, text)
        if m:
            metrics[key] = float(m.group(1))

    percentile_re = re.compile(r"^\s*([0-9.]+)%\s+in\s+([0-9.]+)\s+sec", re.MULTILINE)
    for m in percentile_re.finditer(text):
        pct = m.group(1)
        label = "p" + pct.replace(".", "_").rstrip("0").rstrip("_")
        metrics[label] = float(m.group(2))

    histogram = []
    hist_re = re.compile(r"^\s*([0-9.]+)\s+sec\s+\[(\d+)\]", re.MULTILINE)
    for m in hist_re.finditer(text):
        histogram.append([float(m.group(1)), int(m.group(2))])
    if histogram:
        metrics["histogram"] = histogram

    return metrics


def aggregate_scalar_metrics(per_run_metrics):
    scalar_keys = set()
    for m in per_run_metrics:
        for k, v in m.items():
            if isinstance(v, (int, float)):
                scalar_keys.add(k)

    aggregated = {}
    for key in sorted(scalar_keys):
        values = [m[key] for m in per_run_metrics if key in m]
        if not values:
            continue
        aggregated[key] = {
            "mean": round(statistics.mean(values), 4),
            "median": round(statistics.median(values), 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "stdev": round(statistics.stdev(values), 4) if len(values) > 1 else 0.0,
            "values": values,
        }
    return aggregated


def load_from_json(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["per_run"], data.get("aggregated", {}), data.get("num_runs", len(data["per_run"]))


def load_from_run_dirs(run_dirs, client_name="node0"):
    per_run = []
    for run_dir in run_dirs:
        log_path = os.path.join(run_dir, f"{client_name}.log")
        if not os.path.isfile(log_path):
            log_path = os.path.join(run_dir, "logs", f"{client_name}.log")
        if not os.path.isfile(log_path):
            print(f"Warning: no log found for {client_name} in {run_dir}, skipping")
            continue
        per_run.append(parse_client_log(log_path))

    if not per_run:
        raise SystemExit(f"No client logs found in any run directory for {client_name}")

    aggregated = aggregate_scalar_metrics(per_run)
    return per_run, aggregated, len(per_run)


def print_summary_table(aggregated, num_runs):
    """Print a text summary of key metrics."""
    print(f"\n{'='*60}")
    print(f"  Aggregated Summary ({num_runs} runs)")
    print(f"{'='*60}")

    fmt_row = "  {:<20s} {:>10s} {:>10s} {:>10s} {:>10s}"
    print(fmt_row.format("Metric", "Mean", "Stdev", "Min", "Max"))
    print(f"  {'-'*60}")

    display_keys = [
        ("requests_sec", "Throughput (req/s)", 1),
        ("average_sec", "Avg Latency (ms)", 1000),
        ("p50", "p50 (ms)", 1000),
        ("p90", "p90 (ms)", 1000),
        ("p95", "p95 (ms)", 1000),
        ("p99", "p99 (ms)", 1000),
    ]
    for key, label, scale in display_keys:
        if key not in aggregated:
            continue
        a = aggregated[key]
        print(fmt_row.format(
            label,
            f"{a['mean'] * scale:.2f}",
            f"{a['stdev'] * scale:.2f}",
            f"{a['min'] * scale:.2f}",
            f"{a['max'] * scale:.2f}",
        ))
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate results from multiple experiment runs",
    )
    parser.add_argument(
        "inputs", nargs="+",
        help="Path to aggregated_results.json OR multiple run directories",
    )
    parser.add_argument(
        "--client", default="node0",
        help="Client node name to read logs from (default: node0)",
    )
    parser.add_argument(
        "--output-dir", "-o", default=None,
        help="Directory to write aggregated JSON (default: same directory as input)",
    )
    args = parser.parse_args()

    # Determine if input is a single JSON or multiple directories
    if len(args.inputs) == 1 and args.inputs[0].endswith(".json"):
        per_run, aggregated, num_runs = load_from_json(args.inputs[0])
        default_output = os.path.dirname(args.inputs[0]) or "."
    else:
        per_run, aggregated, num_runs = load_from_run_dirs(args.inputs, args.client)
        default_output = "."

    output_dir = args.output_dir or default_output
    os.makedirs(output_dir, exist_ok=True)

    print_summary_table(aggregated, num_runs)

    # Save aggregated JSON if loaded from directories
    if len(args.inputs) > 1 or not args.inputs[0].endswith(".json"):
        out_json = os.path.join(output_dir, "aggregated_results.json")
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({"num_runs": num_runs, "per_run": per_run, "aggregated": aggregated}, f, indent=2)
        print(f"Wrote {out_json}")

    print("Done!")


if __name__ == "__main__":
    main()
