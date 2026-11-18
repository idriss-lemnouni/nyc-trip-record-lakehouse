"""Render the measured NYC TLC source reconciliation and storage comparison."""
from argparse import ArgumentParser
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

PALETTE = ("#3c63b7", "#007b76")
INK = "#162a3a"
MUTED = "#506379"
REASONS = {
    "missing_or_negative_total": "Missing or negative total",
    "duration_outside_0_to_6_hours": "Duration outside (0, 6 h]",
    "pickup_outside_source_month": "Pickup outside source month",
    "invalid_timestamp": "Invalid timestamp",
    "invalid_zone": "Invalid taxi zone",
    "invalid_distance": "Invalid distance",
}


def setup(title, caption, panels=1):
    figure, axes = plt.subplots(1, panels, figsize=(10.2, 4.8), squeeze=False)
    figure.set_facecolor("#fcfdfe")
    figure.subplots_adjust(left=0.14, right=0.96, top=0.78, bottom=0.22, wspace=0.34)
    figure.text(0.055, 0.93, title, fontsize=19, fontweight="bold", color=INK)
    figure.text(0.055, 0.055, caption, fontsize=9, color=MUTED)
    for axis in axes[0]:
        axis.set_facecolor(figure.get_facecolor())
        axis.spines[["top", "right"]].set_visible(False)
        axis.set_axisbelow(True)
        axis.grid(axis="y", color="#cedbe5")
    return figure, axes[0]


def plot(metrics, out):
    out.mkdir(parents=True, exist_ok=True)
    sources = metrics["sources"]
    months = [item["month"] for item in sources]
    eligible = [item["eligible_rows"] for item in sources]
    excluded = [item["source_rows"] - item["eligible_rows"] for item in sources]
    figure, (axis,) = setup("Spark reconciles every TLC source row",
                           "Yellow Taxi Parquet · first matching quality rule determines quarantine")
    axis.bar(months, eligible, color=PALETTE[1], label="Eligible")
    axis.bar(months, excluded, bottom=eligible, color="#d98a4d", label="Quarantined")
    axis.set_ylabel("Source rows")
    axis.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    axis.legend(loc="upper right", frameon=False)
    axis.set_ylim(0, max(item["source_rows"] for item in sources) * 1.28)
    for x, item in enumerate(sources):
        axis.text(x, item["source_rows"] + 50000, f'{item["source_rows"]:,}', ha="center", color=INK)
    figure.savefig(out / "source-reconciliation.png", dpi=175)
    plt.close(figure)

    observed = [reason for reason in REASONS if any(reason in item["quarantine"] for item in sources)]
    figure, (axis,) = setup("Quarantine is dominated by fare totals",
                           "Counts use the first failing rule · log scale shows rare month mismatches")
    figure.subplots_adjust(left=0.26)
    width = 0.32
    for idx, item in enumerate(sources):
        values = [item["quarantine"].get(reason, 0) for reason in observed]
        offsets = [pos + (idx - (len(sources) - 1) / 2) * width for pos in range(len(observed))]
        axis.barh(offsets, values, height=width, color=PALETTE[idx], label=item["month"])
        for offset, value in zip(offsets, values):
            if value:
                axis.text(value * 1.09, offset, f"{value:,}", va="center", fontsize=9, color=INK)
    axis.set_yticks(range(len(observed)), [REASONS[reason] for reason in observed])
    axis.invert_yaxis()
    axis.set_xscale("log")
    axis.set_xlim(10, max(max(item["quarantine"].values()) for item in sources) * 4.6)
    axis.grid(False, axis="y")
    axis.grid(True, axis="x", color="#cedbe5")
    axis.set_xlabel("Quarantined rows (log scale)")
    axis.legend(frameon=False, loc="lower right")
    figure.savefig(out / "quarantine-rules.png", dpi=175)
    plt.close(figure)

    benchmark = metrics["benchmark"]
    layouts = [benchmark["flat"], benchmark["partitioned"]]
    layouts_short = ["Flat Parquet", "By service day"]
    day = metrics["identity"]["benchmark_day"]
    figure, (storage, timing) = setup(
        "Day partitioning narrows candidate files",
        f"{day} · both returned {benchmark['rows']:,} trips · catalogued file sizes and three local query runs",
        panels=2,
    )
    storage.set_ylabel("Candidate Parquet (MB)")
    sizes = [layout["candidate_files_for_day"]["bytes"] / 1_000_000 for layout in layouts]
    storage.bar(layouts_short, sizes, color=PALETTE, width=0.56)
    storage.set_ylim(0, max(sizes) * 1.32)
    for idx, (size, layout) in enumerate(zip(sizes, layouts)):
        count = layout["candidate_files_for_day"]["files"]
        storage.text(idx, size + max(sizes) * 0.035, f"{size:.1f} MB · {count} file{'s' if count != 1 else ''}",
                     ha="center", fontsize=9, color=INK)
    timing.set_ylabel("Wall time (s)")
    timing.set_xticks([0, 1], layouts_short)
    for idx, (color, layout) in enumerate(zip(PALETTE, layouts)):
        times = layout["local_query_seconds"]
        timing.scatter([idx - 0.1, idx, idx + 0.1], times, color=color, s=26)
        level = layout["local_median_seconds"]
        timing.hlines(level, idx - 0.23, idx + 0.23, color=color, linewidth=2)
    timing.set_xlim(-0.5, 1.5)
    timing.set_ylim(0, max(max(item["local_query_seconds"]) for item in layouts) * 1.21)
    figure.savefig(out / "partition-pruning.png", dpi=175)
    plt.close(figure)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, default=Path("reports/spark-metrics.json"))
    parser.add_argument("--out", type=Path, default=Path("figures/results"))
    args = parser.parse_args()
    plot(json.loads(args.metrics.read_text(encoding="utf-8")), args.out)
