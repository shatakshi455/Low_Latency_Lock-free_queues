#!/usr/bin/env python3
"""bench/plot.py — Generate throughput charts from bench_results.csv."""

import csv
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("Error: matplotlib is required. Install with: pip install matplotlib",
          file=sys.stderr)
    sys.exit(1)


def load_csv(path: Path) -> list[dict]:
    """Load bench_results.csv and return list of row dicts."""
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "queue_type": row["queue_type"],
                "msg_bytes":  int(row["msg_bytes"]),
                "consumers":  int(row["consumers"]),
                "ops":        int(row["ops"]),
                "median_mops": float(row["median_mops"]),
            })
    return rows


# ─── Chart 1: Throughput vs Message Size (1 consumer) ────────────────────────

def plot_throughput_vs_msgsize(rows: list[dict], out_path: Path):
    """Bar-grouped chart: throughput at 1 consumer for each queue type."""
    # Filter to 1-consumer rows only; pick one mutex variant
    queue_labels = {
        "spsc_lockfree": "SPSC (lock-free)",
        "spmc_lockfree": "SPMC (lock-free)",
        "mutex_spsc":    "Mutex (baseline)",
    }

    # Collect data: queue_type -> {msg_bytes: median_mops}
    data = defaultdict(dict)
    for r in rows:
        if r["consumers"] != 1:
            continue
        qt = r["queue_type"]
        if qt not in queue_labels:
            continue
        data[qt][r["msg_bytes"]] = r["median_mops"]

    msg_sizes = sorted({r["msg_bytes"] for r in rows})

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = {
        "spsc_lockfree": "#2ecc71",
        "spmc_lockfree": "#3498db",
        "mutex_spsc":    "#e74c3c",
    }
    markers = {
        "spsc_lockfree": "o",
        "spmc_lockfree": "s",
        "mutex_spsc":    "^",
    }

    x_labels = [f"{s}B" for s in msg_sizes]
    x_pos = list(range(len(msg_sizes)))

    for qt, label in queue_labels.items():
        vals = [data[qt].get(s, 0) for s in msg_sizes]
        ax.plot(x_pos, vals, marker=markers[qt], label=label,
                color=colors[qt], linewidth=2, markersize=8)

    ax.set_xlabel("Message Size", fontsize=12)
    ax.set_ylabel("Throughput (Mops/s)", fontsize=12)
    ax.set_title("Throughput vs Message Size  (1 consumer)", fontsize=14,
                 fontweight="bold")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels)
    ax.legend(fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ─── Chart 2: Throughput vs Consumer Count (64B messages) ────────────────────

def plot_throughput_vs_consumers(rows: list[dict], out_path: Path):
    """Line chart: throughput scaling with consumer count at 64B."""
    msg_target = 64

    # SPSC baseline (1 consumer only)
    spsc_val = None
    for r in rows:
        if (r["queue_type"] == "spsc_lockfree" and
                r["msg_bytes"] == msg_target and r["consumers"] == 1):
            spsc_val = r["median_mops"]
            break

    # SPMC and Mutex-MPMC lines
    queue_labels = {
        "spmc_lockfree": "SPMC (lock-free)",
        "mutex_mpmc":    "Mutex (MPMC)",
    }
    colors = {
        "spmc_lockfree": "#3498db",
        "mutex_mpmc":    "#e74c3c",
    }
    markers = {
        "spmc_lockfree": "o",
        "mutex_mpmc":    "s",
    }

    data = defaultdict(dict)
    for r in rows:
        if r["msg_bytes"] != msg_target:
            continue
        qt = r["queue_type"]
        if qt in queue_labels:
            data[qt][r["consumers"]] = r["median_mops"]

    consumer_counts = sorted({r["consumers"] for r in rows
                              if r["msg_bytes"] == msg_target and
                              r["queue_type"] in queue_labels})

    fig, ax = plt.subplots(figsize=(10, 6))

    for qt, label in queue_labels.items():
        vals = [data[qt].get(nc, 0) for nc in consumer_counts]
        ax.plot(consumer_counts, vals, marker=markers[qt], label=label,
                color=colors[qt], linewidth=2, markersize=8)

    if spsc_val is not None:
        ax.axhline(y=spsc_val, color="#2ecc71", linestyle="--", linewidth=1.5,
                   label=f"SPSC baseline ({spsc_val:.1f} Mops/s)")

    ax.set_xlabel("Number of Consumers", fontsize=12)
    ax.set_ylabel("Throughput (Mops/s)", fontsize=12)
    ax.set_title("Throughput vs Consumer Count  (64B messages)", fontsize=14,
                 fontweight="bold")
    ax.set_xticks(consumer_counts)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    repo = Path(__file__).resolve().parent.parent
    csv_path = repo / "bench_results.csv"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found. Run 'make run-bench' first.",
              file=sys.stderr)
        sys.exit(1)

    rows = load_csv(csv_path)

    plot_throughput_vs_msgsize(rows, repo / "throughput_vs_msgsize.png")
    plot_throughput_vs_consumers(rows, repo / "throughput_vs_consumers.png")

    print("Done.")


if __name__ == "__main__":
    main()
