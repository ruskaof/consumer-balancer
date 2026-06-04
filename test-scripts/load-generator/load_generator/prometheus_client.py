"""Thin Prometheus query client for the performance harness.

Only metrics that are reliably exported by the test listeners are used:

* ``kafka_consumer_fetch_manager_records_consumed_rate`` (per instance) — already
  emitted by Spring Kafka + Micrometer; consumed throughput.
* ``kafka_consumer_fetch_manager_records_lag`` (per instance, per partition) —
  used both as the lag signal and, via its label set, to recover *which*
  partitions each consumer instance currently owns (the assignment).
* ``kafka_consumer_fetch_manager_records_lag_max`` (per instance) — peak backlog.
* ``e2e_latency_seconds_bucket`` — custom timer registered by the listener, read
  with ``histogram_quantile`` for a correctly-aggregated p99.
* ``kafka_consumer_coordinator_rebalance_total`` — rebalance count (the cost
  side of the comparison).
"""

from __future__ import annotations

from datetime import datetime

import requests


class PrometheusClient:
    def __init__(self, host: str):
        self.range_url = f"http://{host}/api/v1/query_range"
        self.query_url = f"http://{host}/api/v1/query"

    # --- low-level ----------------------------------------------------------
    def _range(self, query: str, start_ts: float, end_ts: float, step: str = "15s") -> list[dict]:
        response = requests.get(
            self.range_url,
            params={"query": query, "start": start_ts, "end": end_ts, "step": step},
        )
        response.raise_for_status()
        return response.json()["data"]["result"]

    def _instant(self, query: str, at_ts: float) -> list[dict]:
        response = requests.get(self.query_url, params={"query": query, "time": at_ts})
        response.raise_for_status()
        return response.json()["data"]["result"]

    @staticmethod
    def _mean_of_series(series: dict) -> float:
        values = [float(point[1]) for point in series.get("values", [])]
        return sum(values) / len(values) if values else 0.0

    # --- assignment / load --------------------------------------------------
    def get_assignment(self, job: str, start_ts: float, end_ts: float) -> dict[str, set[int]]:
        """Recover ``{consumer: {partition ids}}`` from the per-partition lag metric.

        A consumer only reports a records-lag series for partitions it owns, so
        the label set of this metric reveals the live assignment. We key by
        ``client_id`` when present (one per consumer thread/member — the unit the
        library actually balances) and fall back to ``instance`` (the pod)
        otherwise. Returns an empty dict if the per-partition metric is
        unavailable, in which case the caller falls back to a throughput-based
        imbalance estimate.
        """
        query = f'kafka_consumer_fetch_manager_records_lag{{job="{job}"}}'
        assignment: dict[str, set[int]] = {}
        for series in self._range(query, start_ts, end_ts):
            metric = series["metric"]
            consumer = metric.get("client_id") or metric.get("instance")
            partition = metric.get("partition")
            if consumer is None or partition is None:
                continue
            assignment.setdefault(consumer, set()).add(int(partition))
        return assignment

    def get_consumed_rate_by_instance(self, job: str, start_ts: float, end_ts: float) -> dict[str, float]:
        query = f'sum(kafka_consumer_fetch_manager_records_consumed_rate{{job="{job}"}}) by (instance)'
        result: dict[str, float] = {}
        for series in self._range(query, start_ts, end_ts):
            instance = series["metric"].get("instance", "unknown")
            result[instance] = self._mean_of_series(series)
        return result

    # --- outcome metrics ----------------------------------------------------
    def get_max_lag(self, job: str, start_ts: float, end_ts: float) -> float:
        """Peak (over the window) of the most-backlogged consumer's lag."""
        query = f'max(kafka_consumer_fetch_manager_records_lag_max{{job="{job}"}})'
        peak = 0.0
        for series in self._range(query, start_ts, end_ts):
            for point in series.get("values", []):
                peak = max(peak, float(point[1]))
        return peak

    def get_e2e_p99_millis(self, job: str, start_ts: float, end_ts: float) -> float:
        window = max(1, int(end_ts - start_ts))
        query = (
            f"histogram_quantile(0.99, sum(rate(e2e_latency_seconds_bucket{{job=\"{job}\"}}[{window}s])) by (le))"
        )
        results = self._instant(query, end_ts)
        if not results:
            return float("nan")
        value = float(results[0]["value"][1])
        return value * 1000.0 if value == value else float("nan")  # NaN-safe

    def get_rebalance_count(self, job: str, start_ts: float, end_ts: float) -> float:
        window = max(1, int(end_ts - start_ts))
        query = f"sum(increase(kafka_consumer_coordinator_rebalance_total{{job=\"{job}\"}}[{window}s]))"
        results = self._instant(query, end_ts)
        if not results:
            return 0.0
        return float(results[0]["value"][1])

    # --- time series (used by the load-shift figure) ------------------------
    def get_max_lag_series(self, job: str, start_ts: float, end_ts: float) -> list[tuple[datetime, float]]:
        query = f'max(kafka_consumer_fetch_manager_records_lag_max{{job="{job}"}})'
        result = self._range(query, start_ts, end_ts)
        if not result:
            return []
        return [
            (datetime.fromtimestamp(float(point[0])), float(point[1]))
            for point in result[0].get("values", [])
        ]

    def get_rebalance_timestamps(self, job: str, start_ts: float, end_ts: float) -> list[datetime]:
        query = f'sum(increase(kafka_consumer_coordinator_rebalance_total{{job="{job}"}}[1m]))'
        times: list[datetime] = []
        for series in self._range(query, start_ts, end_ts):
            for point in series.get("values", []):
                if float(point[1]) > 0:
                    times.append(datetime.fromtimestamp(float(point[0])))
        return times
