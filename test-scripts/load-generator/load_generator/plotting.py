from datetime import datetime
import os
from matplotlib import pyplot as plt


def _find_global_start(eps_by_job: dict[str, list[list[tuple[datetime, float]]]]) -> datetime:
    earliest = None
    for all_series in eps_by_job.values():
        for series in all_series:
            if series:
                ts = series[0][0]
                if earliest is None or ts < earliest:
                    earliest = ts
    return earliest


def _value_at_mark(seconds: list[float], values: list[float], mark: float) -> tuple[float, float] | None:
    if not seconds:
        return None
    best_i = min(range(len(seconds)), key=lambda i: abs(seconds[i] - mark))
    return seconds[best_i], values[best_i]


def plot_test_results(
    output_dir: str,
    eps_by_job: dict[str, list[list[tuple[datetime, float]]]],
    rebalance_timestamps_by_job: dict[str, list[datetime]]
) -> None:
    jobs = list(eps_by_job.keys())
    jobs_count = len(jobs)
    if jobs_count == 0:
        return

    global_start = _find_global_start(eps_by_job)

    fig, axs = plt.subplots(jobs_count, 1, figsize=(12, 4 * jobs_count), sharex=True)
    if jobs_count == 1:
        axs = [axs]

    for i, job in enumerate(jobs):
        ax = axs[i]
        all_series = eps_by_job[job]

        for idx, series in enumerate(all_series):
            seconds = [(ts - global_start).total_seconds() for ts, _ in series]
            values = [value for _, value in series]
            line, = ax.plot(seconds, values, linewidth=1.5, label=f"replica {idx + 1}")
            for mark in (150, 450):
                point = _value_at_mark(seconds, values, mark)
                if point is None:
                    continue
                x, y = point
                ax.annotate(
                    f"{y:.0f}",
                    xy=(x, y),
                    xytext=(0, 6),
                    textcoords="offset points",
                    ha="center",
                    fontsize=8,
                    color=line.get_color(),
                )
        ax.set_ylabel("Consumed msg/s")
        ax.set_title(f"Throughput for {job}")

        rebalance_added_to_legend = False
        if job in rebalance_timestamps_by_job:
            for timestamp in rebalance_timestamps_by_job[job]:
                offset = (timestamp - global_start).total_seconds()
                label = "rebalance" if not rebalance_added_to_legend else None
                ax.axvline(x=offset, color='r', linestyle='--', alpha=0.5, label=label)
                rebalance_added_to_legend = True

        ax.legend(loc='upper right')

    axs[-1].set_xlabel("Elapsed time (s)")
    fig.tight_layout()

    fig.savefig(output_dir)
    print(f"Plot saved to {output_dir}")
    plt.close(fig)
