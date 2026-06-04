"""Synthetic, reproducible Kafka workload generation.

The load model is a **Zipf distribution** over partition popularity: a small
number of partitions carry most of the load, the standard model for skewed key
popularity in messaging and caching systems. A single exponent ``s`` spans the
whole regime (``s = 0`` uniform, larger ``s`` more skewed), which is what lets
the harness *sweep* skew instead of hand-picking one arbitrary distribution.

Two design choices keep the experiment honest:

* **Seeded random placement.** The Zipf weights are assigned to partition ids
  through a seeded random permutation. Count-based assignors (round-robin) are
  sensitive to *where* the hot partitions land, so randomising placement (and
  repeating with different seeds) measures their expected behaviour rather than
  a lucky or unlucky layout.
* **Realised rate == assignor input.** We produce each partition at exactly the
  rate implied by its weight. The assignor reads per-partition load from
  Prometheus (the broker offset rate), so the load we generate *is* the signal
  the library balances on. There is no hidden mismatch between the experiment
  and what the library sees.
"""

from __future__ import annotations

import time

import numpy as np


def zipf_partition_rates(
    num_partitions: int,
    exponent: float,
    total_msgs_per_sec: float,
    rng: np.random.Generator,
) -> dict[int, float]:
    """Return ``{partition_id: target_msgs_per_sec}`` for a Zipf(exponent) load.

    ``exponent == 0`` yields a uniform distribution (the control). The weights
    sum to ``total_msgs_per_sec``. The mapping of weights to partition ids is a
    random permutation drawn from ``rng`` so hot partitions are not always the
    low-numbered ones.
    """
    ranks = np.arange(1, num_partitions + 1, dtype=float)
    raw = 1.0 / np.power(ranks, exponent)
    weights = raw / raw.sum()

    permutation = rng.permutation(num_partitions)
    rates: dict[int, float] = {}
    for rank_index in range(num_partitions):
        partition_id = int(permutation[rank_index])
        rates[partition_id] = float(weights[rank_index] * total_msgs_per_sec)
    return rates


def produce_constant_load(
    producer,
    topic: str,
    partition_rates: dict[int, float],
    duration_seconds: float,
    tick_seconds: float = 0.1,
) -> None:
    """Produce messages so that each partition receives its target rate.

    Each message payload is the producer's wall-clock send time in epoch
    milliseconds, which the consumer reads back to compute end-to-end latency.
    A fractional accumulator per partition makes the long-run rate exact even
    when ``rate * tick_seconds`` is below one message per tick.
    """
    partitions = sorted(partition_rates)
    accumulators = {p: 0.0 for p in partitions}

    start = time.time()
    next_tick = start
    while time.time() - start < duration_seconds:
        payload = str(int(time.time() * 1000)).encode()
        for p in partitions:
            accumulators[p] += partition_rates[p] * tick_seconds
            count = int(accumulators[p])
            if count <= 0:
                continue
            accumulators[p] -= count
            for _ in range(count):
                producer.send(topic, value=payload, partition=p)

        next_tick += tick_seconds
        sleep_for = next_tick - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
    producer.flush()
