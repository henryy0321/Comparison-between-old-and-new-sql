#!/usr/bin/env python3
"""Create a summary CSV and comparison charts from benchmark_results.csv."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
INPUT_FILE = RESULTS / "benchmark_results.csv"
SUMMARY_FILE = RESULTS / "summary.csv"
CHART_DIR = RESULTS / "charts"
METRICS = {
    "throughput_ops_sec": "Throughput (operations/second)",
    "p95_latency_ms": "p95 latency (ms)",
    "average_cpu_percent": "Average CPU usage (%)",
    "average_memory_mib": "Average memory usage (MiB)",
}


def read_rows():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"No results found at {INPUT_FILE}. Run scripts/run_benchmark.py first."
        )
    with INPUT_FILE.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main():
    rows = read_rows()
    groups = defaultdict(lambda: defaultdict(list))
    group_keys = ("database", "dataset_records", "workload", "clients")

    for row in rows:
        key = tuple(row.get(field, "") for field in group_keys)
        for metric in METRICS:
            number = to_float(row.get(metric))
            if number is not None:
                groups[key][metric].append(number)

    summary_rows = []
    for key, metrics in sorted(groups.items()):
        item = dict(zip(group_keys, key))
        for metric in METRICS:
            values = metrics.get(metric, [])
            item[metric] = round(mean(values), 4) if values else ""
        summary_rows.append(item)

    RESULTS.mkdir(parents=True, exist_ok=True)
    with SUMMARY_FILE.open("w", newline="", encoding="utf-8") as file:
        fields = [*group_keys, *METRICS.keys()]
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary_rows)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is required for charts. Run: python -m pip install -r requirements.txt"
        ) from exc

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    datasets = sorted({row["dataset_records"] for row in summary_rows}, key=int)
    chart_count = 0

    for dataset in datasets:
        for metric, axis_label in METRICS.items():
            fig, ax = plt.subplots(figsize=(9, 5.5))
            plotted = False
            for database in sorted({r["database"] for r in summary_rows}):
                for workload in sorted({r["workload"] for r in summary_rows}):
                    selected = [
                        r for r in summary_rows
                        if r["dataset_records"] == dataset
                        and r["database"] == database
                        and r["workload"] == workload
                        and to_float(r.get(metric)) is not None
                    ]
                    selected.sort(key=lambda r: int(r["clients"]))
                    if not selected:
                        continue
                    x_values = [int(r["clients"]) for r in selected]
                    y_values = [float(r[metric]) for r in selected]
                    ax.plot(x_values, y_values, marker="o", label=f"{database} - {workload}")
                    plotted = True

            if not plotted:
                plt.close(fig)
                continue
            ax.set_title(f"{axis_label} — {int(dataset):,} records")
            ax.set_xlabel("Concurrent clients")
            ax.set_ylabel(axis_label)
            ax.set_xticks(sorted({int(r["clients"]) for r in summary_rows if r["dataset_records"] == dataset}))
            ax.grid(True, alpha=0.25)
            ax.legend(fontsize=8)
            fig.tight_layout()
            output = CHART_DIR / f"{metric}_{dataset}.png"
            fig.savefig(output, dpi=160)
            plt.close(fig)
            chart_count += 1

    print(f"Summary saved: {SUMMARY_FILE}")
    print(f"Charts created: {chart_count} in {CHART_DIR}")
    print("Use summary.csv and the charts when writing your Results and Discussion section.")


if __name__ == "__main__":
    main()
