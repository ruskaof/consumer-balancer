"""Performance-test orchestrator for the load-aware partition assignor.

The harness answers a single hypothesis with two experiments:

    H: load-aware assignment lowers the most-loaded consumer's load (and the
       resulting lag/latency) relative to count-based assignors when, and only
       when, the per-partition load is skewed.

* Experiment A (static skew sweep) varies the Zipf skew exponent and measures
  steady-state imbalance, p99 latency and peak lag for each strategy. It shows
  *whether and by how much* the library helps, including the control (uniform
  load) where it must not hurt.
* Experiment B (controlled load shift) moves the hot partitions at known times
  and measures how fast each strategy recovers, exercising the library's
  proactive-rebalance trigger.

Both experiments compare three consumer groups consuming the same topic
simultaneously, so they see identical input: RoundRobin (count-balanced),
CooperativeSticky (sticky), and the load-aware assignor.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import numpy as np
from kafka import KafkaProducer

from .config import Config
from .metrics import (
    area_under_curve,
    imbalance_from_assignment,
    imbalance_from_consumed,
)
from .plotting import plot_load_shift, plot_skew_sweep
from .prometheus_client import PrometheusClient
from .report import aggregate_sweep, write_metadata
from .workload import produce_constant_load, zipf_partition_rates

# Time to wait after a production window so Prometheus scrapes the final samples
# before we query them (must exceed the scrape interval).
SETTLE_SECONDS = 15


def _measure_window(client: PrometheusClient, job: str, start_ts: float, end_ts: float,
                    offered_rates: dict[int, float]) -> dict:
    assignment = client.get_assignment(job, start_ts, end_ts)
    imbalance = imbalance_from_assignment(assignment, offered_rates)
    imbalance_source = "offered-load"
    if imbalance is None:
        consumed = client.get_consumed_rate_by_instance(job, start_ts, end_ts)
        imbalance = imbalance_from_consumed(consumed)
        imbalance_source = "consumed-rate-fallback"
    return {
        "job": job,
        "imbalance": imbalance if imbalance is not None else float("nan"),
        "imbalance_source": imbalance_source,
        "p99_latency_ms": client.get_e2e_p99_millis(job, start_ts, end_ts),
        "max_lag": client.get_max_lag(job, start_ts, end_ts),
        "rebalances": client.get_rebalance_count(job, start_ts, end_ts),
    }


def run_experiment_a(cfg: Config, producer, client: PrometheusClient) -> dict:
    raw_records: list[dict] = []
    for skew_index, skew in enumerate(cfg.skew_exponents):
        for rep in range(cfg.repetitions):
            rng = np.random.default_rng([cfg.seed, skew_index, rep])
            offered_rates = zipf_partition_rates(
                cfg.partitions_count, skew, cfg.target_total_msgs_per_sec, rng)

            print(f"[A] skew={skew} rep={rep + 1}/{cfg.repetitions} "
                  f"(warmup {cfg.warmup_seconds}s + steady {cfg.steady_seconds}s)")
            produce_constant_load(
                producer, cfg.topic, offered_rates,
                duration_seconds=cfg.warmup_seconds + cfg.steady_seconds)
            window_end = time.time()
            window_start = window_end - cfg.steady_seconds

            time.sleep(SETTLE_SECONDS)
            for job in cfg.jobs:
                record = _measure_window(client, job, window_start, window_end, offered_rates)
                record.update({"skew": skew, "rep": rep})
                raw_records.append(record)
                print(f"    {job}: imbalance={record['imbalance']:.2f} "
                      f"({record['imbalance_source']}), p99={record['p99_latency_ms']:.0f}ms, "
                      f"max_lag={record['max_lag']:.0f}, rebalances={record['rebalances']:.0f}")

    aggregated = aggregate_sweep(cfg.jobs, cfg.skew_exponents, raw_records)
    plot_skew_sweep(cfg.output_prefix, cfg.skew_exponents, aggregated)
    return {
        "skew_exponents": cfg.skew_exponents,
        "repetitions": cfg.repetitions,
        "jobs": cfg.jobs,
        "aggregated": aggregated,
        "raw_records": raw_records,
    }


def run_experiment_b(cfg: Config, producer, client: PrometheusClient) -> dict:
    b_start = datetime.now()
    b_start_ts = b_start.timestamp()
    phase_offsets: list[float] = []

    for phase in range(cfg.shift_phases):
        rng = np.random.default_rng([cfg.seed, 1000, phase])
        offered_rates = zipf_partition_rates(
            cfg.partitions_count, cfg.shift_skew, cfg.target_total_msgs_per_sec, rng)
        phase_offsets.append(time.time() - b_start_ts)
        print(f"[B] phase {phase + 1}/{cfg.shift_phases} (skew={cfg.shift_skew}, "
              f"{cfg.shift_phase_seconds}s) — hot partitions reshuffled")
        produce_constant_load(
            producer, cfg.topic, offered_rates, duration_seconds=cfg.shift_phase_seconds)

    time.sleep(SETTLE_SECONDS)
    b_end_ts = time.time()

    lag_series_by_job = {
        job: client.get_max_lag_series(job, b_start_ts, b_end_ts) for job in cfg.jobs
    }
    rebalance_ts_by_job = {
        job: client.get_rebalance_timestamps(job, b_start_ts, b_end_ts) for job in cfg.jobs
    }

    # Load shifts happen at the start of every phase after the first.
    shift_boundaries = phase_offsets[1:]

    # Recovery metric: area under the lag curve in each phase (lower = recovers faster).
    recovery = {}
    for job, series in lag_series_by_job.items():
        elapsed = [((ts.timestamp() - b_start_ts), v) for ts, v in series]
        per_phase = []
        for phase in range(cfg.shift_phases):
            lo = phase_offsets[phase]
            hi = phase_offsets[phase + 1] if phase + 1 < len(phase_offsets) else (b_end_ts - b_start_ts)
            phase_points = [(x, y) for x, y in elapsed if lo <= x <= hi]
            per_phase.append(area_under_curve(phase_points))
        recovery[job] = per_phase

    plot_load_shift(cfg.output_prefix, b_start, lag_series_by_job, shift_boundaries, rebalance_ts_by_job)

    return {
        "skew": cfg.shift_skew,
        "phases": cfg.shift_phases,
        "phase_seconds": cfg.shift_phase_seconds,
        "shift_boundaries_seconds": shift_boundaries,
        "lag_area_under_curve_by_phase": recovery,
        "lag_series": {
            job: [[ts.isoformat(), v] for ts, v in series]
            for job, series in lag_series_by_job.items()
        },
    }


def main() -> None:
    cfg = Config()
    client = PrometheusClient(cfg.prometheus_url)
    producer = KafkaProducer(bootstrap_servers=cfg.bootstrap_servers)
    library_version = os.getenv("LIBRARY_VERSION", "unknown")

    print("Operating regime:")
    for key, value in cfg.regime().items():
        print(f"  {key}: {value}")

    experiment_a = run_experiment_a(cfg, producer, client) if "A" in cfg.experiment else None
    experiment_b = run_experiment_b(cfg, producer, client) if "B" in cfg.experiment else None

    write_metadata(cfg.output_prefix, cfg.as_dict(), library_version, experiment_a, experiment_b)
    producer.close()


if __name__ == "__main__":
    main()
