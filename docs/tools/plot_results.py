#!/usr/bin/env python3

"""
Run examples:
  uv run --with matplotlib --with pandas --python 3.14+gil docs/experiments/labelset_store/plot_results.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import os

DPI=300

def plot_csvs(file_paths, output_plot):
    plt.figure(figsize=(12, 7), dpi=DPI)

    for path in file_paths:
        if not os.path.exists(path):
            print(f"Warning: File {path} not found. Skipping.")
            continue

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"Error reading {path}: {e}")
            continue

        if df.empty or "timestamp" not in df.columns or "rss" not in df.columns:
            print(f"Warning: {path} is empty or missing required columns. Skipping.")
            continue

        # Convert RSS to MiB
        rss_mib = df["rss"] / (1024 * 1024)

        # X axis: time since start (s)
        relative_times = df["timestamp"] - df["timestamp"].iloc[0]

        label = os.path.basename(path).replace(".csv", "")
        plt.plot(relative_times, rss_mib, label=label)

    plt.xlabel("Time since start (s)")
    plt.ylabel("RSS Memory (MiB)")
    plt.title("Memory Usage (RSS) Comparison")
    plt.legend()
    plt.grid(True)
    plt.savefig(output_plot, dpi=DPI)
    print(f"Plot saved to {output_plot}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Plot RSS memory usage from CSV files."
    )
    parser.add_argument("files", nargs="*", help="CSV files to plot")
    parser.add_argument(
        "--output", default="comparison_plot.png", help="Output plot file"
    )

    args = parser.parse_args()

    if not args.files:
        # Default files if none provided
        results_dir = "docs/experiments/labelset_store/results"
        args.files = [
            os.path.join(results_dir, "flat_interned.csv"),
            os.path.join(results_dir, "key_set_dict_encoded.csv"),
        ]

    plot_csvs(args.files, args.output)
