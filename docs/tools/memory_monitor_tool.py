#!/usr/bin/env python3

"""
Run examples:
  uv run --with psutil --with matplotlib --python 3.14+gil docs/experiments/labelset_store/memory_monitor_tool.py $INGESTER_PID --interval 1 --csv "$CSV_FILE" --plot "$PLOT_FILE"
"""

import time
import argparse
import csv
import sys
from dataclasses import dataclass
from typing import List, Optional, Iterable, Callable
import psutil

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


@dataclass
class MemStats:
    rss_min: Optional[int] = None
    rss_max: Optional[int] = None
    rss_avg: Optional[float] = None
    uss_min: Optional[int] = None
    uss_max: Optional[int] = None
    uss_avg: Optional[float] = None
    pss_min: Optional[int] = None
    pss_max: Optional[int] = None
    pss_avg: Optional[float] = None


@dataclass
class _MetricAgg:
    min_v: Optional[int] = None
    max_v: Optional[int] = None
    sum_v: int = 0
    n: int = 0

    def add(self, v: int) -> None:
        if self.min_v is None or v < self.min_v:
            self.min_v = v
        if self.max_v is None or v > self.max_v:
            self.max_v = v
        self.sum_v += v
        self.n += 1

    def avg(self) -> Optional[float]:
        return self.sum_v / self.n if self.n > 0 else None


class MemoryMonitor:
    def __init__(self, interval_s: float):
        self.interval_s = max(interval_s, 0.001)
        self.history = []

    def monitor(self, pid: int, csv_file: str, plot_file: str):
        print(f"Monitoring PID: {pid} at {self.interval_s}s intervals")
        rss_agg, uss_agg, pss_agg = _MetricAgg(), _MetricAgg(), _MetricAgg()

        try:
            p = psutil.Process(pid)
            with open(csv_file, "w", newline="") as f:
                writer = csv.writer(f)
                header = ["timestamp", "rss"]

                # Check what info is available
                info = p.memory_full_info()
                have_uss = hasattr(info, "uss")
                have_pss = hasattr(info, "pss")

                if have_uss:
                    header.append("uss")
                if have_pss:
                    header.append("pss")
                writer.writerow(header)

                while p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                    try:
                        info = p.memory_full_info()
                        ts = time.time()
                        row = [ts, info.rss]

                        rss_agg.add(info.rss)

                        current_data = {"ts": ts, "rss": info.rss}

                        if have_uss:
                            uss_val = getattr(info, "uss")
                            row.append(uss_val)
                            uss_agg.add(uss_val)
                            current_data["uss"] = uss_val
                        if have_pss:
                            pss_val = getattr(info, "pss")
                            row.append(pss_val)
                            pss_agg.add(pss_val)
                            current_data["pss"] = pss_val

                        writer.writerow(row)
                        self.history.append(current_data)

                        time.sleep(self.interval_s)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        break
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} not found.")
            sys.exit(1)
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user.")

        print(f"Monitoring finished. Data saved to {csv_file}")

        if self.history:
            if HAS_MATPLOTLIB:
                # Let's plot only RSS
                self.plot(plot_file, have_uss=False, have_pss=False)
            else:
                print("Matplotlib not installed, skipping plot.")
        else:
            print("No data collected to plot.")

        stats = MemStats(
            rss_min=rss_agg.min_v,
            rss_max=rss_agg.max_v,
            rss_avg=rss_agg.avg(),
            uss_min=uss_agg.min_v,
            uss_max=uss_agg.max_v,
            uss_avg=uss_agg.avg(),
            pss_min=pss_agg.min_v,
            pss_max=pss_agg.max_v,
            pss_avg=pss_agg.avg(),
        )
        print("\nSummary Statistics (bytes):")
        print(f"RSS: min={stats.rss_min}, max={stats.rss_max}, avg={stats.rss_avg}")
        if have_uss:
            print(f"USS: min={stats.uss_min}, max={stats.uss_max}, avg={stats.uss_avg}")
        if have_pss:
            print(f"PSS: min={stats.pss_min}, max={stats.pss_max}, avg={stats.pss_avg}")

    def plot(self, plot_file: str, have_uss: bool, have_pss: bool):
        timestamps = [d["ts"] - self.history[0]["ts"] for d in self.history]
        rss = [d["rss"] / (1024 * 1024) for d in self.history]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, rss, label="RSS")

        if have_uss:
            uss = [d["uss"] / (1024 * 1024) for d in self.history]
            plt.plot(timestamps, uss, label="USS")
        if have_pss:
            pss = [d["pss"] / (1024 * 1024) for d in self.history]
            plt.plot(timestamps, pss, label="PSS")

        plt.xlabel("Time (s)")
        plt.ylabel("Memory Usage (MiB)")
        plt.title("Memory Usage Over Time")
        plt.legend()
        plt.grid(True)
        plt.savefig(plot_file)
        print(f"Plot saved to {plot_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor memory usage of a specific process."
    )
    parser.add_argument("pid", type=int, help="PID of the process to monitor")
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Sampling interval in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="memory_usage.csv",
        help="Output CSV file (default: memory_usage.csv)",
    )
    parser.add_argument(
        "--plot",
        type=str,
        default="memory_usage.png",
        help="Output plot file (default: memory_usage.png)",
    )

    args = parser.parse_args()

    monitor = MemoryMonitor(args.interval)
    monitor.monitor(args.pid, args.csv, args.plot)


if __name__ == "__main__":
    main()
