#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime
from math import floor, ceil


def parse_ts(record):
    ts = record.get("timestamp")
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        try:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            return datetime.fromisoformat(ts).timestamp()
        except Exception:
            pass
    payload = record.get("payload")
    if isinstance(payload, dict):
        p_ts = payload.get("timestamp")
        if isinstance(p_ts, (int, float)):
            return float(p_ts)
    return None


def normalize_actual(actual, expected):
    if isinstance(actual, dict) and not isinstance(expected, dict):
        if "read_value" in actual and len(actual.keys() & {"read_value", "request_id", "status"}) == len(actual):
            return actual.get("read_value")
        if "value" in actual and len(actual.keys()) == 1:
            return actual.get("value")
    return actual


def values_equal(expected, actual):
    norm_actual = normalize_actual(actual, expected)
    return expected == norm_actual


def percentile(values, p):
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    k = (p / 100) * (len(values) - 1)
    f = floor(k)
    c = ceil(k)
    if f == c:
        return values[int(k)]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


def load_records(path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def analyze_trace(path):
    records = load_records(path)
    requests = []
    responses = []
    by_id = {}

    for rec in records:
        r_type = rec.get("type")
        req_id = rec.get("request_id")
        if req_id is None:
            continue
        entry = by_id.setdefault(req_id, {"requests": [], "responses": []})
        if r_type == "request":
            requests.append(rec)
            entry["requests"].append(rec)
        elif r_type == "response":
            responses.append(rec)
            entry["responses"].append(rec)

    mismatches = []
    for req_id, entry in by_id.items():
        reqs = sorted(entry["requests"], key=parse_ts)
        if not reqs:
            continue
        expected = reqs[0].get("expected_result")
        for resp in entry["responses"]:
            actual = resp.get("actual_result")
            if not values_equal(expected, actual):
                mismatches.append((reqs[0], resp))

    count_mismatches = []
    if len(requests) != len(responses):
        for req_id, entry in by_id.items():
            if len(entry["requests"]) != len(entry["responses"]):
                count_mismatches.append((req_id, entry))

    latencies = []
    for req_id, entry in by_id.items():
        req_ts = None
        resp_ts = None
        if entry["requests"]:
            req_ts = min(filter(lambda x: x is not None, (parse_ts(r) for r in entry["requests"])), default=None)
        if entry["responses"]:
            resp_ts = min(filter(lambda x: x is not None, (parse_ts(r) for r in entry["responses"])), default=None)
        if req_ts is not None and resp_ts is not None:
            latency = resp_ts - req_ts
            if latency >= 0:
                latencies.append(latency)

    return {
        "requests": requests,
        "responses": responses,
        "mismatches": mismatches,
        "count_mismatches": count_mismatches,
        "latencies": latencies,
    }


def format_json(obj):
    return json.dumps(obj, sort_keys=True)


def main():
    parser = argparse.ArgumentParser(description="Analyze experiment trace results")
    parser.add_argument("results_folder", help="Folder name under experiment/results or a full path")
    args = parser.parse_args()

    folder = args.results_folder
    if not os.path.isabs(folder):
        candidate = os.path.join(os.path.dirname(__file__), "results", folder)
        if os.path.isdir(candidate):
            folder = candidate
        elif os.path.isdir(folder):
            folder = os.path.abspath(folder)
        else:
            print(f"Results folder not found: {args.results_folder}", file=sys.stderr)
            sys.exit(1)
    traces_dir = os.path.join(folder, "traces")
    if not os.path.isdir(traces_dir):
        print(f"Traces folder not found: {traces_dir}", file=sys.stderr)
        sys.exit(1)

    trace_files = sorted(
        f for f in (os.path.join(traces_dir, name) for name in os.listdir(traces_dir))
        if os.path.isfile(f)
    )

    for trace_path in trace_files:
        result = analyze_trace(trace_path)
        print(f"Trace: {os.path.basename(trace_path)}")

        if result["mismatches"]:
            print("Mismatched expected_result vs actual_result (request_id):")
            for req, _resp in result["mismatches"]:
                print(f"request_id={req.get('request_id')}")
        else:
            print("Mismatched expected_result vs actual_result: none")

        if result["count_mismatches"]:
            print("Request/response count mismatches (request_id):")
            for req_id, entry in result["count_mismatches"]:
                print(f"request_id={req_id} requests={len(entry['requests'])} responses={len(entry['responses'])}")
        else:
            print("Request/response count mismatches: none")

        latencies = result["latencies"]
        if latencies:
            p25 = percentile(latencies, 25)
            p50 = percentile(latencies, 50)
            p75 = percentile(latencies, 75)
            p99 = percentile(latencies, 99)
            print("Latency (seconds, request -> first response):")
            print(f"p25={p25:.6f} p50={p50:.6f} p75={p75:.6f} p99={p99:.6f}")
        else:
            print("Latency: n/a")

        print("")


if __name__ == "__main__":
    main()
