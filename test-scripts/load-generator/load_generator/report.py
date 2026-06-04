"""Build the machine-readable result artifact (``*.metadata.json``).

This file is what the scientific article cites: it contains the seed, every
configuration parameter, the derived operating regime, and the aggregated
metrics (mean and 95% CI half-width) for each strategy at each skew level, plus
the raw per-repetition records so the tables and figures can be regenerated
without re-running the experiment.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

from .metrics import mean_ci95


def _json_safe(value):
    """Replace NaN/inf floats with None so the artifact is valid JSON.

    NaN appears legitimately (e.g. a 95% CI half-width with a single repetition,
    or p99 latency when no samples were recorded). ``json.dump`` would emit the
    invalid ``NaN`` token, which breaks strict parsers like ``jq`` and pandas.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value

# Metrics aggregated across repetitions for the skew sweep (Experiment A).
SWEEP_METRICS = ("imbalance", "p99_latency_ms", "max_lag", "rebalances")


def aggregate_sweep(
    jobs: list[str],
    skews: list[float],
    raw_records: list[dict],
) -> dict[str, dict[str, dict[str, list[float]]]]:
    """Aggregate raw per-(skew, rep, job) records into mean ± CI per job/metric.

    Returns ``{job: {metric: {"mean": [...per skew...], "ci95": [...]}}}`` with
    one entry per skew level, in the order given by ``skews``.
    """
    aggregated: dict[str, dict[str, dict[str, list[float]]]] = {}
    for job in jobs:
        aggregated[job] = {m: {"mean": [], "ci95": []} for m in SWEEP_METRICS}
        for skew in skews:
            cell = [
                r for r in raw_records
                if r["job"] == job and r["skew"] == skew
            ]
            for metric in SWEEP_METRICS:
                mean, ci = mean_ci95([r[metric] for r in cell])
                aggregated[job][metric]["mean"].append(mean)
                aggregated[job][metric]["ci95"].append(ci)
    return aggregated


def write_metadata(
    prefix: str,
    config: dict,
    library_version: str,
    experiment_a: dict | None,
    experiment_b: dict | None,
) -> str:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "library_version": library_version,
        "seed": config.get("seed"),
        "config": config,
        "experiment_a_static_skew_sweep": experiment_a,
        "experiment_b_load_shift": experiment_b,
    }
    path = f"{prefix}.metadata.json"
    with open(path, "w") as fh:
        json.dump(_json_safe(payload), fh, indent=2, default=str, allow_nan=False)
    print(f"Metadata written to {path}")
    return path
