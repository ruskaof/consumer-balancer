from datetime import datetime
import os
from matplotlib import pyplot as plt


def plot_test_results(
    output_dir: str,
    eps_by_job: dict[str, list[tuple[datetime, float]]],
    rebalance_timestamps_by_job: dict[str, list[datetime]]
) -> None:
    jobs = list(eps_by_job.keys())
    jobs_count = len(jobs)
    if jobs_count == 0:
        return

    fig, axs = plt.subplots(jobs_count, 1, figsize=(12, 4 * jobs_count), sharex=True)
    if jobs_count == 1:
        axs = [axs]

    for i, job in enumerate(jobs):
        ax = axs[i]
        results = eps_by_job[job]
        timestamps = [timestamp for timestamp, _ in results]
        values = [value for _, value in results]

        ax.plot(timestamps, values, linewidth=1.5, label=job)
        ax.set_ylabel("Consumed msg/s")
        ax.set_title(f"Throughput for {job}")

        if job in rebalance_timestamps_by_job:
            for timestamp in rebalance_timestamps_by_job[job]:
                ax.axvline(x=timestamp, color='r', linestyle='--', alpha=0.5)

        ax.legend(loc='upper right')

    axs[-1].set_xlabel("Time")
    fig.tight_layout()


    fig.savefig(output_dir)
    print(f"Plot saved to {output_dir}")
    plt.close(fig)
