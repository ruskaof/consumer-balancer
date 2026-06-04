"""Figures for the performance report.

Two figures map onto the library's two value propositions:

* ``{prefix}.png`` — **Experiment A** (assignment quality). Imbalance ratio,
  p99 end-to-end latency, and peak consumer lag, each as a function of load
  skew, one line per strategy with 95% confidence bands. The headline result.
* ``{prefix}.shift.png`` — **Experiment B** (proactive rebalance). Peak consumer
  lag over time as the hot partitions shift at controlled instants, showing how
  quickly each strategy recovers balance.
"""

from __future__ import annotations

from datetime import datetime

from matplotlib import pyplot as plt

# Stable label/style per job so the two figures are visually consistent.
JOB_STYLE = {
    "listener-roundrobin": ("RoundRobin (count-balanced)", "tab:blue", "o"),
    "listener-cooperative-sticky": ("CooperativeSticky (sticky)", "tab:orange", "s"),
    "listener-balanced": ("Load-aware (this library)", "tab:green", "^"),
}

_SWEEP_PANELS = [
    ("imbalance", "Load imbalance\n(max / mean consumer load)"),
    ("p99_latency_ms", "p99 end-to-end\nlatency (ms)"),
    ("max_lag", "Peak consumer\nlag (records)"),
]


def _label(job: str) -> str:
    return JOB_STYLE.get(job, (job, None, None))[0]


def _color(job: str):
    return JOB_STYLE.get(job, (job, None, None))[1]


def _marker(job: str) -> str:
    return JOB_STYLE.get(job, (job, None, "o"))[2]


def plot_skew_sweep(
    output_prefix: str,
    skews: list[float],
    aggregated: dict[str, dict[str, dict[str, list[float]]]],
) -> str:
    """Experiment A: metric vs. skew, one line per strategy, with 95% CI bars."""
    jobs = list(aggregated.keys())
    fig, axs = plt.subplots(len(_SWEEP_PANELS), 1, figsize=(9, 3.2 * len(_SWEEP_PANELS)), sharex=True)
    if len(_SWEEP_PANELS) == 1:
        axs = [axs]

    for ax, (metric, ylabel) in zip(axs, _SWEEP_PANELS):
        for job in jobs:
            means = aggregated[job][metric]["mean"]
            cis = aggregated[job][metric]["ci95"]
            yerr = [c if c == c else 0.0 for c in cis]  # NaN CI (n<2) -> no bar
            ax.errorbar(
                skews, means, yerr=yerr,
                label=_label(job), color=_color(job), marker=_marker(job),
                capsize=3, linewidth=1.6,
            )
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        if metric == "imbalance":
            ax.axhline(y=1.0, color="grey", linestyle=":", alpha=0.7, label="perfect balance")

    axs[0].set_title("Effect of load skew on consumer-group balance")
    axs[0].legend(loc="upper left", fontsize=8)
    axs[-1].set_xlabel("Load skew (Zipf exponent s) — 0 = uniform, higher = more skewed")
    fig.tight_layout()

    path = f"{output_prefix}.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"Skew-sweep figure saved to {path}")
    return path


def plot_load_shift(
    output_prefix: str,
    start: datetime,
    lag_series_by_job: dict[str, list[tuple[datetime, float]]],
    phase_boundaries_seconds: list[float],
    rebalance_timestamps_by_job: dict[str, list[datetime]],
) -> str:
    """Experiment B: peak consumer lag over time, with load-shift markers."""
    fig, ax = plt.subplots(figsize=(11, 5))

    for job, series in lag_series_by_job.items():
        if not series:
            continue
        seconds = [(ts - start).total_seconds() for ts, _ in series]
        values = [v for _, v in series]
        ax.plot(seconds, values, label=_label(job), color=_color(job), linewidth=1.8)
        for ts in rebalance_timestamps_by_job.get(job, []):
            ax.axvline(x=(ts - start).total_seconds(), color=_color(job), linestyle=":", alpha=0.25)

    shift_labeled = False
    for boundary in phase_boundaries_seconds:
        label = "load shift" if not shift_labeled else None
        ax.axvline(x=boundary, color="red", linestyle="--", alpha=0.6, label=label)
        shift_labeled = True

    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Peak consumer lag (records)")
    ax.set_title("Recovery after controlled load shifts (lower and faster is better)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    path = f"{output_prefix}.shift.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"Load-shift figure saved to {path}")
    return path
