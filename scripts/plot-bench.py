#!/usr/bin/env python3
"""
plot-bench.py — visualise kafka-lite throughput benchmark results.

Usage:
    python3 scripts/plot-bench.py --data logs/bench.csv [--events logs/chaos-events.csv] \
        [--out bench.png] [--title "my run"]
"""

import argparse
import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec


def load_data(path):
    df = pd.read_csv(path)
    required = {"elapsed_s", "msgs_per_sec", "mb_per_sec", "errs_per_sec",
                "consume_lag", "p50_ms", "p99_ms"}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"error: {path} is missing columns: {missing}")
    return df


def load_events(path):
    if not path:
        return pd.DataFrame(columns=["elapsed_s", "event", "type", "num"])
    try:
        df = pd.read_csv(path)
        if df.empty or "elapsed_s" not in df.columns:
            return pd.DataFrame(columns=["elapsed_s", "event", "type", "num"])
        return df
    except Exception:
        return pd.DataFrame(columns=["elapsed_s", "event", "type", "num"])


def outage_windows(events):
    """Return list of (kill_s, restart_s, label) tuples for shading."""
    kills = {}
    windows = []
    for _, row in events.iterrows():
        key = (row["type"], int(row["num"]))
        if row["event"] == "kill":
            kills[key] = row["elapsed_s"]
        elif row["event"] == "restart" and key in kills:
            label = f"{row['type']} {int(row['num'])}"
            windows.append((kills.pop(key), row["elapsed_s"], label))
    return windows


def permanent_kills(events):
    """Return sorted list of (elapsed_s, broker_num) for kill_permanent events."""
    perm = events[events["event"] == "kill_permanent"].copy()
    perm = perm.sort_values("elapsed_s")
    return [(row["elapsed_s"], int(row["num"])) for _, row in perm.iterrows()]


def main():
    ap = argparse.ArgumentParser(description="Plot kafka-lite benchmark results")
    ap.add_argument("--data",    required=True, help="per-second CSV from -csv flag")
    ap.add_argument("--events",  default=None,  help="chaos events CSV (optional)")
    ap.add_argument("--brokers", type=int, default=0,
                    help="total broker count for 'N remaining' annotations on permanent kills")
    ap.add_argument("--out",     default="bench.png", help="output PNG path")
    ap.add_argument("--title",   default="kafka-lite throughput benchmark")
    args = ap.parse_args()

    df      = load_data(args.data)
    t = df["elapsed_s"]
    x_max = t.max() + 1

    events  = load_events(args.events)
    # Drop events outside the data window — the chaos/degrade loop may have kept
    # running after the job finished, and out-of-range text annotations cause
    # bbox_inches="tight" to produce a very wide image.
    if not events.empty:
        events = events[events["elapsed_s"] <= x_max].copy()
    windows = outage_windows(events)
    perm    = permanent_kills(events)

    fig = plt.figure(figsize=(14, 10), layout="constrained")
    fig.suptitle(args.title, fontsize=14, fontweight="bold")
    gs = GridSpec(3, 1, figure=fig, hspace=0.45)

    # ── Panel 1: Throughput & Errors ──────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0])
    ax1r = ax1.twinx()

    ax1.plot(t, df["msgs_per_sec"] / 1000, color="#2196F3", linewidth=1.8,
             label="throughput (k msg/s)")
    ax1r.fill_between(t, df["errs_per_sec"], alpha=0.45, color="#F44336",
                      label="errors/s")
    ax1r.plot(t, df["errs_per_sec"], color="#F44336", linewidth=0.8, alpha=0.7)

    ax1.set_ylabel("throughput (k msg/s)", color="#2196F3")
    ax1r.set_ylabel("errors / s", color="#F44336")
    ax1.tick_params(axis="y", labelcolor="#2196F3")
    ax1r.tick_params(axis="y", labelcolor="#F44336")
    ax1.set_xlim(0, x_max)
    ax1.set_ylim(bottom=0)
    ax1r.set_ylim(bottom=0)
    ax1.set_title("Throughput & Errors", fontsize=11)
    ax1.set_xlabel("")

    # ── Panel 2: Latency ──────────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[1], sharex=ax1)

    ax2.plot(t, df["p50_ms"], color="#4CAF50", linewidth=1.8, label="p50")
    ax2.plot(t, df["p99_ms"], color="#FF9800", linewidth=1.8,
             linestyle="--", label="p99")
    ax2.set_ylabel("latency (ms)")
    ax2.set_ylim(bottom=0)
    ax2.set_title("Produce Latency", fontsize=11)
    ax2.legend(loc="upper right", fontsize=9)

    # ── Panel 3: Consumer Lag ─────────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[2], sharex=ax1)

    lag = df["consume_lag"].clip(lower=0)
    ax3.fill_between(t, lag, alpha=0.4, color="#9C27B0")
    ax3.plot(t, lag, color="#9C27B0", linewidth=1.2)
    ax3.set_ylabel("consume lag (msgs)")
    ax3.set_ylim(bottom=0)
    ax3.set_xlabel("elapsed time (s)")
    ax3.set_title("Consumer Lag", fontsize=11)

    # ── Chaos event overlays ──────────────────────────────────────────────────
    for ax in (ax1, ax2, ax3):
        for kill_s, restart_s, label in windows:
            ax.axvspan(kill_s, restart_s, alpha=0.12, color="red", zorder=0)
            ax.axvline(kill_s,    color="red",   linestyle="--", linewidth=1, alpha=0.7)
            ax.axvline(restart_s, color="green", linestyle="--", linewidth=1, alpha=0.7)

    # ── Permanent-kill overlays (degradation mode) ────────────────────────────
    total_brokers = args.brokers if args.brokers > 0 else (max(n for _, n in perm) if perm else 0)
    for i, (t_kill, num) in enumerate(perm):
        for ax in (ax1, ax2, ax3):
            ax.axvline(t_kill, color="darkred", linestyle="--", linewidth=1.5, alpha=0.85)
        remaining = total_brokers - (i + 1)
        label = f"↓broker-{num}"
        if remaining >= 0:
            label += f"\n({remaining} left)"
        ax1.text(t_kill + 0.3, ax1.get_ylim()[1] * 0.88,
                 label, color="darkred", fontsize=7, va="top")

    # Annotate kill/restart lines on the top panel only.
    kill_patch    = mpatches.Patch(color="red",     alpha=0.5, label="kill window")
    restart_patch = mpatches.Patch(color="green",   alpha=0.5, label="restart")
    perm_patch    = mlines.Line2D([0], [0], color="darkred", linestyle="--",
                                  linewidth=1.5, label="permanent kill")

    # Add kill/restart labels above the top panel.
    for kill_s, restart_s, label in windows:
        ax1.text(kill_s + 0.2, ax1.get_ylim()[1] * 0.92,
                 f"↓{label}", color="red", fontsize=7.5, va="top")
        ax1.text(restart_s + 0.2, ax1.get_ylim()[1] * 0.80,
                 f"↑{label}", color="green", fontsize=7.5, va="top")

    handles1, labels1   = ax1.get_legend_handles_labels()
    handles1r, labels1r = ax1r.get_legend_handles_labels()
    extra_patches = []
    extra_labels  = []
    if windows:
        extra_patches += [kill_patch, restart_patch]
        extra_labels  += ["kill window", "restart"]
    if perm:
        extra_patches.append(perm_patch)
        extra_labels.append("permanent kill")
    ax1.legend(handles1 + handles1r + extra_patches,
               labels1 + labels1r + extra_labels,
               loc="upper left", fontsize=9)

    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"saved: {args.out}")


if __name__ == "__main__":
    main()
