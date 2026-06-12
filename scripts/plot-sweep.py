#!/usr/bin/env python3
"""
plot-sweep.py — visualise a rate sweep from sweep.sh.

Produces a dual-axis chart:
  - Left  Y: measured throughput (k msg/s)   — solid blue line
  - Right Y: average p99 latency (ms)        — dashed orange line
  - X: target rate (msgs/sec)

A vertical marker shows the "knee": the lowest rate that reaches
90% of peak measured throughput — the most efficient operating point.

Usage:
    python3 scripts/plot-sweep.py --data logs/sweep.csv [--out sweep.png] [--title "..."]
"""

import argparse
import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def find_knee(rates, throughput):
    """Return the rate at which measured throughput first hits 90% of its peak."""
    peak = max(throughput)
    for r, t in zip(rates, throughput):
        if t >= 0.90 * peak:
            return r
    return rates[-1]


def main():
    ap = argparse.ArgumentParser(description="Plot rate sweep results")
    ap.add_argument("--data",  required=True, help="sweep.csv from sweep.sh")
    ap.add_argument("--out",   default="sweep.png", help="output PNG path")
    ap.add_argument("--title", default="kafka-lite rate sweep")
    args = ap.parse_args()

    df = pd.read_csv(args.data)
    if df.empty:
        sys.exit("error: sweep CSV is empty")

    df = df.sort_values("rate_target")
    rates = df["rate_target"].tolist()
    thr   = (df["avg_msgs_per_sec"] / 1000).tolist()   # k msg/s
    p99   = df["avg_p99_ms"].tolist()

    knee = find_knee(rates, thr)

    fig, ax1 = plt.subplots(figsize=(10, 6), layout="constrained")
    ax2 = ax1.twinx()

    # Throughput line
    l1, = ax1.plot(rates, thr, color="#2196F3", linewidth=2.2,
                   marker="o", markersize=7, label="measured throughput (k msg/s)")
    ax1.set_xlabel("target rate (msgs/sec)", fontsize=12)
    ax1.set_ylabel("measured throughput (k msg/s)", color="#2196F3", fontsize=12)
    ax1.tick_params(axis="y", labelcolor="#2196F3")
    ax1.set_ylim(bottom=0)

    # P99 latency line
    l2, = ax2.plot(rates, p99, color="#FF9800", linewidth=2.2, linestyle="--",
                   marker="s", markersize=7, label="p99 latency (ms)")
    ax2.set_ylabel("avg p99 latency (ms)", color="#FF9800", fontsize=12)
    ax2.tick_params(axis="y", labelcolor="#FF9800")
    ax2.set_ylim(bottom=0)

    # Knee annotation
    ax1.axvline(knee, color="gray", linestyle=":", linewidth=1.5, alpha=0.8)
    knee_thr = thr[rates.index(knee)]
    ax1.annotate(
        f"knee\n{knee:,} msg/s\n{knee_thr:.1f}k achieved",
        xy=(knee, knee_thr),
        xytext=(knee * 1.05, knee_thr * 0.75),
        fontsize=9,
        color="gray",
        arrowprops=dict(arrowstyle="->", color="gray", lw=1.2),
    )

    # Peak annotation
    peak_thr = max(thr)
    peak_r   = rates[thr.index(peak_thr)]
    ax1.annotate(
        f"peak\n{peak_thr:.1f}k msg/s",
        xy=(peak_r, peak_thr),
        xytext=(peak_r * 0.75, peak_thr * 1.05),
        fontsize=9,
        color="#2196F3",
        arrowprops=dict(arrowstyle="->", color="#2196F3", lw=1.2),
    )

    ax1.legend(handles=[l1, l2], loc="upper left", fontsize=10)
    ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.xticks(rotation=30, ha="right")

    fig.suptitle(args.title, fontsize=13, fontweight="bold")
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"saved: {args.out}")


if __name__ == "__main__":
    main()
