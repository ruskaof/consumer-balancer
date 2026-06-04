"""Metric computation and statistical aggregation.

The headline metric is the **load imbalance ratio**

    imbalance = (load of the most-loaded consumer) / (mean consumer load)

which is exactly the quantity the library's ``ThresholdTrigger`` optimises. A
value of 1.0 is perfect balance; round-robin/sticky assignors rise above 1.0
under skew while load-aware stays near it. We compute it two ways:

* **Offered-load imbalance (preferred, noise-free).** Using the live assignment
  (which partitions each consumer owns) and the *offered* per-partition rates we
  generated, we compute each consumer's load analytically. This is independent
  of runtime noise and consumer saturation, so it cleanly isolates assignment
  quality.
* **Consumed-rate imbalance (fallback).** If the per-partition assignment metric
  is unavailable, we fall back to max/mean of per-instance consumed throughput.
  Note this *understates* imbalance for a saturated consumer (its consumed rate
  is capped at capacity), so it is a conservative estimate; the lag and latency
  metrics capture the suppressed excess.
"""

from __future__ import annotations

import math
import statistics

# Two-sided 95% Student-t critical values by degrees of freedom (n-1).
# Falls back to the normal approximation (1.96) for larger samples.
_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    15: 2.131, 20: 2.086, 30: 2.042,
}


def imbalance_from_assignment(
    assignment: dict[str, set[int]],
    offered_rates: dict[int, float],
) -> float | None:
    """max/mean consumer load using the live assignment and offered rates."""
    if not assignment:
        return None
    loads = [
        sum(offered_rates.get(p, 0.0) for p in partitions)
        for partitions in assignment.values()
    ]
    mean = sum(loads) / len(loads)
    if mean <= 0:
        return None
    return max(loads) / mean


def imbalance_from_consumed(consumed_by_instance: dict[str, float]) -> float | None:
    """Conservative fallback: max/mean of per-instance consumed throughput."""
    loads = [v for v in consumed_by_instance.values()]
    if not loads:
        return None
    mean = sum(loads) / len(loads)
    if mean <= 0:
        return None
    return max(loads) / mean


def mean_ci95(values: list[float]) -> tuple[float, float]:
    """Return (mean, half-width of the 95% confidence interval).

    The half-width is NaN for n < 2 (a single sample has no spread). Reviewers
    can read mean ± half-width directly off the figures and table.
    """
    clean = [v for v in values if v is not None and not math.isnan(v)]
    n = len(clean)
    if n == 0:
        return float("nan"), float("nan")
    mean = sum(clean) / n
    if n < 2:
        return mean, float("nan")
    sd = statistics.stdev(clean)
    t = _T95.get(n - 1, 1.96)
    return mean, t * sd / math.sqrt(n)


def area_under_curve(series: list[tuple[float, float]]) -> float:
    """Trapezoidal integral of a (seconds, value) series.

    Used for Experiment B: the area under the consumer-lag curve after a load
    shift quantifies how much backlog a strategy accumulated before recovering.
    Lower is better; a strategy that never recovers integrates without bound.
    """
    if len(series) < 2:
        return 0.0
    total = 0.0
    for (x0, y0), (x1, y1) in zip(series, series[1:]):
        total += (x1 - x0) * (y0 + y1) / 2.0
    return total
