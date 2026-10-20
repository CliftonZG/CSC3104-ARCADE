#!/usr/bin/env python3
"""Turn load-probe reports into a summary table and report-ready charts.

Owner: Tan Yan Ting (Phase 2: horizontal scalability & resource profiling).

Usage:  python test/load/plot_results.py test/load/results/<run-dir>
Input:  <run-dir>/pairs-<N>.json   (written by run-load.ps1 / run-load.sh)
Output: <run-dir>/summary.csv, summary.md and fig1..fig3 *.png
        (matplotlib is optional: without it only the tables are written)
"""
import csv
import glob
import json
import os
import re
import sys

# Fixed categorical order (validated palette, light mode): identity never cycles.
PRIMARY = "#2a78d6"  # slot 1 - primary pods
BACKUP = "#eb6834"   # slot 2 - standby pods
REDIS = "#1baf7a"    # slot 3 - shared Redis
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

COLUMNS = [
    ("pairs", "Pairs"),
    ("pairs_complete", "Pairs measured"),
    ("pair_cpu_mean", "CPU / pair (mcores, mean)"),
    ("pair_cpu_p95", "CPU / pair (mcores, p95)"),
    ("pair_rss_mean", "RSS / pair (MiB, mean)"),
    ("pair_rss_p95", "RSS / pair (MiB, p95)"),
    ("primary_cpu_mean", "Primary CPU (mcores)"),
    ("backup_cpu_mean", "Standby CPU (mcores)"),
    ("primary_rss_mean", "Primary RSS (MiB)"),
    ("backup_rss_mean", "Standby RSS (MiB)"),
    ("fleet_cpu", "Fleet CPU (mcores)"),
    ("fleet_rss", "Fleet RSS (MiB)"),
    ("moves_per_sec", "Moves/s (fleet)"),
    ("move_p50", "Move latency p50 (ms)"),
    ("move_p95", "Move latency p95 (ms)"),
    ("move_p99", "Move latency p99 (ms)"),
    ("redis_p50", "Redis commit p50 (ms)"),
    ("redis_p99", "Redis commit p99 (ms)"),
    ("redis_cpu", "Redis CPU (mcores)"),
    ("redis_mem", "Redis memory (MiB)"),
    ("redis_ops", "Redis ops/s"),
]


def load(run_dir):
    rows = []
    for path in glob.glob(os.path.join(run_dir, "pairs-*.json")):
        if not re.search(r"pairs-\d+\.json$", path):
            continue
        with open(path, encoding="utf-8") as f:
            text = f.read().strip()
        if not text:
            print(f"skip empty {path}", file=sys.stderr)
            continue
        r = json.loads(text)
        redis = r.get("redis") or {}
        rows.append({
            "pairs": r["pairs_requested"],
            "pairs_complete": r["pairs_complete"],
            "pair_cpu_mean": r["pair_cpu_millicores"]["mean"],
            "pair_cpu_p95": r["pair_cpu_millicores"]["p95"],
            "pair_rss_mean": r["pair_rss_mib"]["mean"],
            "pair_rss_p95": r["pair_rss_mib"]["p95"],
            "primary_cpu_mean": r["primary_cpu_millicores"]["mean"],
            "backup_cpu_mean": r["backup_cpu_millicores"]["mean"],
            "primary_rss_mean": r["primary_rss_mib"]["mean"],
            "backup_rss_mean": r["backup_rss_mib"]["mean"],
            "fleet_cpu": r["pair_cpu_millicores"]["sum"],
            "fleet_rss": r["pair_rss_mib"]["sum"],
            "moves_per_sec": r["total_moves_per_sec"],
            "move_p50": r["move_latency_ms"]["p50"],
            "move_p95": r["move_latency_ms"]["p95"],
            "move_p99": r["move_latency_ms"]["p99"],
            "redis_p50": r["redis_commit_ms"]["p50"],
            "redis_p99": r["redis_commit_ms"]["p99"],
            "redis_cpu": redis.get("cpu_millicores", 0),
            "redis_mem": redis.get("used_memory_mib", 0),
            "redis_ops": redis.get("ops_per_sec", 0),
        })
    return sorted(rows, key=lambda x: x["pairs"])


def fmt(v):
    return f"{v:.2f}" if isinstance(v, float) else str(v)


def write_tables(rows, run_dir):
    with open(os.path.join(run_dir, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([c for c, _ in COLUMNS])
        for r in rows:
            w.writerow([r[c] for c, _ in COLUMNS])
    with open(os.path.join(run_dir, "summary.md"), "w", encoding="utf-8") as f:
        # Transposed: one column per fleet size reads better in a report.
        f.write("| Metric | " + " | ".join(f"{r['pairs']} pairs" for r in rows) + " |\n")
        f.write("|---|" + "---:|" * len(rows) + "\n")
        for key, label in COLUMNS[1:]:
            f.write(f"| {label} | " + " | ".join(fmt(r[key]) for r in rows) + " |\n")


def style(ax, title, ylabel):
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
    ax.set_ylabel(ylabel, color=MUTED, fontsize=9)
    ax.set_xlabel("Concurrent match pairs", color=MUTED, fontsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8)


def plot(rows, run_dir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed: wrote tables only (pip install matplotlib)", file=sys.stderr)
        return
    if not rows:
        return
    plt.rcParams.update({"font.family": "sans-serif", "figure.dpi": 150})
    labels = [str(r["pairs"]) for r in rows]
    xs = list(range(len(rows)))

    # Fig 1: per-pair footprint, stacked primary + standby (small multiples, one axis each).
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.6))
    for ax, (p_key, b_key, unit, title) in zip(axes, [
        ("primary_cpu_mean", "backup_cpu_mean", "millicores", "CPU per Primary/Standby pair"),
        ("primary_rss_mean", "backup_rss_mean", "MiB", "Resident memory per pair"),
    ]):
        prim = [r[p_key] for r in rows]
        back = [r[b_key] for r in rows]
        ax.bar(xs, prim, width=0.55, color=PRIMARY, label="Primary", edgecolor="white", linewidth=1)
        ax.bar(xs, back, width=0.55, bottom=prim, color=BACKUP, label="Standby", edgecolor="white", linewidth=1)
        for x, p, b in zip(xs, prim, back):
            ax.annotate(f"{p + b:.1f}", (x, p + b), textcoords="offset points", xytext=(0, 3),
                        ha="center", fontsize=8, color=INK)
        ax.set_xticks(xs, labels)
        style(ax, title, unit)
    handles, names = axes[0].get_legend_handles_labels()
    fig.legend(handles, names, frameon=False, fontsize=8, loc="upper right", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(os.path.join(run_dir, "fig1_per_pair_footprint.png"))
    plt.close(fig)

    # Fig 2: fleet totals vs pairs, with the ideal linear-scaling line from the smallest run.
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.6))
    pairs = [r["pairs"] for r in rows]
    for ax, (key, unit, title) in zip(axes, [
        ("fleet_cpu", "millicores", "Total gameserver CPU"),
        ("fleet_rss", "MiB", "Total gameserver memory"),
    ]):
        vals = [r[key] for r in rows]
        ax.plot(pairs, vals, color=PRIMARY, linewidth=2, marker="o", markersize=5, label="Measured")
        if pairs[0] > 0:
            per = vals[0] / pairs[0]
            ax.plot(pairs, [per * p for p in pairs], color=MUTED, linewidth=1, linestyle="--", label="Linear from smallest run")
        ax.set_xticks(pairs)
        style(ax, title, unit)
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(run_dir, "fig2_fleet_scaling.png"))
    plt.close(fig)

    # Fig 3: latency as the fleet grows (Redis is the shared component).
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.6))
    ax = axes[0]
    ax.plot(pairs, [r["move_p50"] for r in rows], color=PRIMARY, linewidth=2, marker="o", markersize=5, label="p50")
    ax.plot(pairs, [r["move_p99"] for r in rows], color=BACKUP, linewidth=2, marker="o", markersize=5, label="p99")
    ax.set_xticks(pairs)
    style(ax, "Move latency incl. Redis commit", "ms")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    ax = axes[1]
    ax.plot(pairs, [r["redis_cpu"] for r in rows], color=REDIS, linewidth=2, marker="o", markersize=5)
    ax.set_xticks(pairs)
    style(ax, "Shared Redis CPU", "millicores")
    fig.tight_layout()
    fig.savefig(os.path.join(run_dir, "fig3_latency_and_redis.png"))
    plt.close(fig)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run_dir = sys.argv[1]
    rows = load(run_dir)
    if not rows:
        sys.exit(f"no pairs-*.json reports in {run_dir}")
    write_tables(rows, run_dir)
    plot(rows, run_dir)
    print(f"wrote summary.csv, summary.md and charts to {run_dir}")


if __name__ == "__main__":
    main()
