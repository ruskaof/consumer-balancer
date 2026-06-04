"""Experiment configuration for the load-aware assignor performance test.

Every parameter is read from an environment variable with a documented default,
so a run is fully described (and reproduced) by its environment plus the random
seed. The defaults below are the "local / paper" profile; the Docker CI profile
in ``docker/test-env.properties`` overrides them with shorter windows.

Why these knobs exist
----------------------
The benefit of load-aware assignment depends on three things, so the harness
makes all three first-class, tunable inputs instead of magic numbers:

* **Skew** of the per-partition load (``SKEW_EXPONENTS``). Modelled as a Zipf
  distribution; ``s = 0`` is uniform (the control), larger ``s`` is more skewed.
* **Granularity**: how much load sits in a single (indivisible) partition
  relative to a consumer's capacity. Governed by ``PARTITIONS_COUNT`` together
  with the consumer count and the per-message service time.
* **Utilisation**: the offered load relative to aggregate consumer capacity
  (``TARGET_TOTAL_MSGS_PER_SEC`` vs. ``LISTENER_PROCESSING_COST_MICROS`` and the
  consumer count). See :meth:`Config.regime` for the derived utilisation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_float_list(name: str, default: str) -> list[float]:
    raw = os.getenv(name, default)
    return [float(part) for part in raw.split(",") if part.strip() != ""]


@dataclass
class Config:
    # --- Connectivity -------------------------------------------------------
    bootstrap_servers: str = field(default_factory=lambda: _env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic: str = field(default_factory=lambda: _env_str("TOPIC", "test-topic"))
    prometheus_url: str = field(default_factory=lambda: _env_str("PROMETHEUS_URL", "localhost:9090"))
    output_prefix: str = field(default_factory=lambda: _env_str("GRAPH_OUTPUT_DIR", "test-out/result-default"))

    # --- Topology -----------------------------------------------------------
    partitions_count: int = field(default_factory=lambda: _env_int("PARTITIONS_COUNT", 32))
    # consumer threads per group = replicas * concurrency; used only for regime diagnostics.
    consumer_replicas: int = field(default_factory=lambda: _env_int("CONSUMER_REPLICAS", 4))
    listener_concurrency: int = field(default_factory=lambda: _env_int("LISTENER_CONCURRENCY", 2))
    processing_cost_micros: int = field(default_factory=lambda: _env_int("LISTENER_PROCESSING_COST_MICROS", 0))

    # --- Load ---------------------------------------------------------------
    target_total_msgs_per_sec: float = field(default_factory=lambda: _env_float("TARGET_TOTAL_MSGS_PER_SEC", 1400.0))

    # --- Reproducibility ----------------------------------------------------
    seed: int = field(default_factory=lambda: _env_int("SEED", 42))

    # --- Which experiments to run ("A", "B", or "AB") -----------------------
    experiment: str = field(default_factory=lambda: _env_str("EXPERIMENT", "AB").upper())

    # --- Experiment A: static skew sweep ------------------------------------
    skew_exponents: list[float] = field(default_factory=lambda: _env_float_list("SKEW_EXPONENTS", "0.0,0.5,1.0,1.5"))
    repetitions: int = field(default_factory=lambda: _env_int("REPETITIONS", 3))
    warmup_seconds: int = field(default_factory=lambda: _env_int("WARMUP_SECONDS", 60))
    steady_seconds: int = field(default_factory=lambda: _env_int("STEADY_SECONDS", 60))

    # --- Experiment B: controlled load shift --------------------------------
    shift_skew: float = field(default_factory=lambda: _env_float("SHIFT_SKEW", 1.0))
    shift_phases: int = field(default_factory=lambda: _env_int("SHIFT_PHASES", 3))
    shift_phase_seconds: int = field(default_factory=lambda: _env_int("SHIFT_PHASE_SECONDS", 120))

    # --- Consumer groups under test (job label -> human description) --------
    jobs: list[str] = field(default_factory=lambda: [
        "listener-roundrobin",
        "listener-cooperative-sticky",
        "listener-balanced",
    ])

    @property
    def consumer_threads(self) -> int:
        return max(1, self.consumer_replicas * self.listener_concurrency)

    def thread_capacity_msgs_per_sec(self) -> float:
        """Max messages/sec one consumer thread can process given the service time."""
        if self.processing_cost_micros <= 0:
            return float("inf")
        return 1_000_000.0 / self.processing_cost_micros

    def regime(self) -> dict:
        """Derived, human-readable description of the operating regime.

        Printed at startup so a reviewer can see *why* the chosen parameters
        should (or should not) reveal a difference between strategies, without
        having to reverse-engineer it from the raw numbers.
        """
        capacity = self.thread_capacity_msgs_per_sec()
        aggregate_capacity = capacity * self.consumer_threads
        mean_thread_load = self.target_total_msgs_per_sec / self.consumer_threads
        utilisation = (
            self.target_total_msgs_per_sec / aggregate_capacity
            if aggregate_capacity not in (0.0, float("inf"))
            else 0.0
        )
        return {
            "consumer_threads_per_group": self.consumer_threads,
            "thread_capacity_msgs_per_sec": capacity,
            "aggregate_capacity_msgs_per_sec": aggregate_capacity,
            "offered_total_msgs_per_sec": self.target_total_msgs_per_sec,
            "mean_thread_load_msgs_per_sec": mean_thread_load,
            "mean_utilisation": utilisation,
            "note": (
                "A consumer saturates (and accrues lag) when its assigned load exceeds "
                "thread_capacity. load-aware keeps every consumer near mean_thread_load; "
                "count-based assignors leave the unlucky consumer above it under skew."
            ),
        }

    def as_dict(self) -> dict:
        d = asdict(self)
        d["regime"] = self.regime()
        return d
