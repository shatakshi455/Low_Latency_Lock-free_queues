#!/usr/bin/env python3
"""
Plot benchmark results from bench_results.csv.

Produces a 3×3 grid of throughput charts:
  Rows    = message sizes  (8 / 64 / 256 bytes)
  Columns = queue depths   (64 / 256 / 1024)
  Each cell = grouped bars: consumer count × queue type

Usage:
    python3 plot.py [bench_results.csv] [output.png]
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Configuration ────────────────────────────────────────────────────────────

MSG_SIZES    = [8, 64, 256]
QUEUE_DEPTHS = [64, 256, 1024]
CONSUMERS    = [1, 2, 4, 8]

QUEUE_TYPES  = ["mutex", "spsc", "spmc"]
LABELS       = {"mutex": "Mutex", "spsc": "SPSC (lock‑free)", "spmc": "SPMC (lock‑free)"}
COLORS       = {"mutex": "#ef4444", "spsc": "#22c55e", "spmc": "#3b82f6"}
EDGE_COLORS  = {"mutex": "#b91c1c", "spsc": "#15803d", "spmc": "#1d4ed8"}

# ── Styling ──────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.facecolor":   "#0f172a",
    "axes.facecolor":     "#1e293b",
    "axes.edgecolor":     "#334155",
    "axes.labelcolor":    "#e2e8f0",
    "axes.titleweight":   "bold",
    "axes.titlesize":     11,
    "axes.titlecolor":    "#f1f5f9",
    "axes.grid":          True,
    "grid.color":         "#334155",
    "grid.alpha":         0.5,
    "xtick.color":        "#94a3b8",
    "ytick.color":        "#94a3b8",
    "text.color":         "#e2e8f0",
    "legend.facecolor":   "#1e293b",
    "legend.edgecolor":   "#475569",
    "legend.labelcolor":  "#e2e8f0",
    "legend.fontsize":    8,
    "font.family":        "sans-serif",
    "font.size":          9,
})

# ── Load data ────────────────────────────────────────────────────────────────

csv_path = sys.argv[1] if len(sys.argv) > 1 else "bench_results.csv"
out_path = sys.argv[2] if len(sys.argv) > 2 else "benchmark_chart.png"

df = pd.read_csv(csv_path)

# ── Build figure ─────────────────────────────────────────────────────────────

fig, axes = plt.subplots(
    len(MSG_SIZES), len(QUEUE_DEPTHS),
    figsize=(16, 11),
    sharey="row",
    constrained_layout=True,
)

fig.suptitle(
    "Lock‑Free Queue Throughput Comparison",
    fontsize=18, fontweight="bold", color="#f8fafc", y=1.01,
)

bar_width = 0.22

for row, ms in enumerate(MSG_SIZES):
    for col, qd in enumerate(QUEUE_DEPTHS):
        ax = axes[row, col]
        subset = df[(df["msg_bytes"] == ms) & (df["queue_depth"] == qd)]

        x = np.arange(len(CONSUMERS))

        for i, qt in enumerate(QUEUE_TYPES):
            throughputs = []
            for cc in CONSUMERS:
                match = subset[(subset["queue_type"] == qt) &
                               (subset["consumers"] == cc)]
                throughputs.append(
                    match["throughput_mops"].values[0] if len(match) > 0 else 0
                )

            bars = ax.bar(
                x + i * bar_width,
                throughputs,
                bar_width * 0.88,
                label=LABELS[qt] if row == 0 and col == 0 else "",
                color=COLORS[qt],
                edgecolor=EDGE_COLORS[qt],
                linewidth=0.6,
                alpha=0.9,
                zorder=3,
            )

            # Annotate non-zero bars with value
            for bar, val in zip(bars, throughputs):
                if val > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + ax.get_ylim()[1] * 0.01,
                        f"{val:.1f}",
                        ha="center", va="bottom",
                        fontsize=6, color="#94a3b8",
                    )

        ax.set_xticks(x + bar_width)
        ax.set_xticklabels([str(c) for c in CONSUMERS])
        ax.set_xlabel("Consumers")

        if col == 0:
            ax.set_ylabel("Throughput (Mops/s)")

        ax.set_title(f"msg={ms}B  ·  depth={qd}")
        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=False))
        ax.set_axisbelow(True)

# ── Shared legend ────────────────────────────────────────────────────────────

handles = [
    plt.Rectangle((0, 0), 1, 1, fc=COLORS[qt], ec=EDGE_COLORS[qt], lw=0.8)
    for qt in QUEUE_TYPES
]
fig.legend(
    handles, [LABELS[qt] for qt in QUEUE_TYPES],
    loc="upper center",
    ncol=3,
    fontsize=11,
    frameon=True,
    fancybox=True,
    shadow=True,
    bbox_to_anchor=(0.5, 1.0),
)

# ── Second pass: update y-limits and re-annotate with correct scale ──────────

for row, ms in enumerate(MSG_SIZES):
    for col, qd in enumerate(QUEUE_DEPTHS):
        ax = axes[row, col]
        # Add some headroom for annotations
        ymax = ax.get_ylim()[1]
        ax.set_ylim(0, ymax * 1.15)

# ── Save ─────────────────────────────────────────────────────────────────────

fig.savefig(out_path, dpi=180, bbox_inches="tight",
            facecolor=fig.get_facecolor(), edgecolor="none")
print(f"✓ Chart saved to {out_path}")

# Also save a latency summary chart
fig2, axes2 = plt.subplots(1, 3, figsize=(16, 5), constrained_layout=True)
fig2.suptitle(
    "Median Latency (p50) — queue depth = 1024",
    fontsize=15, fontweight="bold", color="#f8fafc", y=1.02,
)

for col, ms in enumerate(MSG_SIZES):
    ax = axes2[col]
    subset = df[(df["msg_bytes"] == ms) & (df["queue_depth"] == 1024)]

    x = np.arange(len(CONSUMERS))

    for i, qt in enumerate(QUEUE_TYPES):
        lats = []
        for cc in CONSUMERS:
            match = subset[(subset["queue_type"] == qt) &
                           (subset["consumers"] == cc)]
            lats.append(
                match["lat_p50_ns"].values[0] if len(match) > 0 else 0
            )

        ax.bar(
            x + i * bar_width,
            lats,
            bar_width * 0.88,
            label=LABELS[qt] if col == 0 else "",
            color=COLORS[qt],
            edgecolor=EDGE_COLORS[qt],
            linewidth=0.6,
            alpha=0.9,
            zorder=3,
        )

    ax.set_xticks(x + bar_width)
    ax.set_xticklabels([str(c) for c in CONSUMERS])
    ax.set_xlabel("Consumers")
    if col == 0:
        ax.set_ylabel("p50 Latency (ns)")
    ax.set_title(f"msg = {ms}B")
    ax.set_axisbelow(True)
    ymax = ax.get_ylim()[1]
    ax.set_ylim(0, ymax * 1.12)

fig2.legend(
    handles, [LABELS[qt] for qt in QUEUE_TYPES],
    loc="upper center", ncol=3, fontsize=11,
    frameon=True, fancybox=True, shadow=True,
    bbox_to_anchor=(0.5, 1.0),
)

latency_path = out_path.replace(".png", "_latency.png")
fig2.savefig(latency_path, dpi=180, bbox_inches="tight",
             facecolor=fig2.get_facecolor(), edgecolor="none")
print(f"✓ Latency chart saved to {latency_path}")
