# consumer-balancer

Load-aware Kafka consumer partition assignment driven by per-partition **weights** (default: Prometheus), plus optional **proactive rebalance** when an elected group member detects load imbalance.

Built-in Kafka assignors such as `RangeAssignor` and `RoundRobinAssignor` balance **partition count** (and sticky strategies minimize movement). They do not use external **per-partition load** signals. This library assigns partitions with a greedy **least-loaded** strategy using those weights, which reduces the worst consumer load when weights are skewed.

## Modules

| Module | Purpose |
|--------|---------|
| `consumer-balancer-core` | `LoadAwarePartitionAssignor`, Prometheus client, balancing, triggers |
| `consumer-balancer-spring-boot-starter` | Spring Boot auto-configuration |
| `test-listener` | Example Spring Boot app |

## Quickstart (Gradle)

Add the starter (publish to Maven Local or your repository as needed):

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-spring-boot-starter:1.0-SNAPSHOT")
}
```

Minimal configuration:

```yaml
spring:
  kafka:
    consumer:
      group-id: my-group
      properties:
        partition.assignment.strategy: io.github.ruskaof.balancer.LoadAwarePartitionAssignor

consumer-balancer:
  prometheus:
    weight-query-template: 'sum(rate(kafka_topic_partition_current_offset{topic=~"%s"}[1m])) by (topic, partition)'
    host: localhost
    port: 9090
```

Example using a hypothetical bytes metric instead:

```yaml
consumer-balancer:
  prometheus:
    weight-query-template: 'sum(rate(kafka_consumer_fetch_bytes_total{topic=~"%s"}[1m])) by (topic, partition)'
```

Placeholder `%s` is replaced with a `|`‑separated, regex‑escaped list of subscribed topic names for the instant query.

`consumer-balancer.prometheus.*` is merged into `assignor.load-aware.prometheus.*` for the assignor. You can still set `assignor.load-aware.prometheus.*` under `spring.kafka.consumer.properties` explicitly; those values take precedence over `consumer-balancer` defaults where applicable.

## Configuration reference (`consumer-balancer`)

| Property | Default | Description |
|----------|---------|-------------|
| `consumer-balancer.enabled` | `true` | Master switch for balancer auto-configuration. |
| `consumer-balancer.proactive-rebalance-enabled` | `true` | When `true`, one elected consumer runs the threshold trigger and may call `enforceRebalance()` on listener containers. |
| `consumer-balancer.rebalance-load-imbalance-threshold` | `1.1` | Proactive rebalance when `(max member load) / (optimal max load) > threshold` (see `ThresholdTrigger`). |
| `consumer-balancer.prometheus.weight-query-template` | — | **Required** when using the default `PrometheusWeightService`: PromQL with `%s`. Series must include `topic` and `partition` labels. |
| `consumer-balancer.prometheus.scheme` | `http` | Prometheus URL scheme. |
| `consumer-balancer.prometheus.host` | `localhost` | Prometheus host. |
| `consumer-balancer.prometheus.port` | `9090` | Prometheus port. |
| `consumer-balancer.prometheus.connect-timeout` | `10s` | HTTP connect timeout. |
| `consumer-balancer.prometheus.request-timeout` | `30s` | HTTP request timeout (per query). |
| `consumer-balancer.coordinator.election-interval` | `30s` | How often coordinator election runs. |
| `consumer-balancer.coordinator.trigger-check-interval` | `30s` | How often the coordinator evaluates the rebalance trigger. |

Assignor keys (merged from `consumer-balancer.prometheus` when not set in YAML):

- `assignor.load-aware.prometheus.weight-query-template` (required for load-aware assignor)
- `assignor.load-aware.prometheus.host`
- `assignor.load-aware.prometheus.port`
- `assignor.load-aware.prometheus.scheme`
- `assignor.load-aware.prometheus.connect-timeout-ms`
- `assignor.load-aware.prometheus.request-timeout-ms`

## Custom weight store

Implement `io.github.ruskaof.balancer.weight.WeightService` and expose it as a Spring `@Bean`. The default `PrometheusWeightService` + `PrometheusClient` beans are omitted when a `WeightService` bean is present.

Optionally provide your own `io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder` (or `TemplatedKafkaRatePromqlBuilder`) for custom PromQL while still using Prometheus.

## Operations

- Your PromQL must return series with `topic` and `partition` labels so weights can be mapped to `TopicPartition`.
- If load-aware assignment throws, `LoadAwarePartitionAssignor` falls back to Kafka’s `RoundRobinAssignor`.

## Build

This repository is built with **Gradle on the PATH** (not the wrapper), for example:

```bash
gradle test
```

## Performance evaluation (Docker)

The performance harness is designed to be defensible for a scientific write-up:
every parameter is justified, the run is reproducible from a seed, and the
metrics measured are the ones the library actually optimises.

### Hypothesis

> Load-aware assignment lowers the most-loaded consumer's load — and the lag and
> latency that follow from it — relative to count-based assignors **when, and
> only when, the per-partition load is skewed**.

Three consumer groups consume the same topic simultaneously (so they see
identical input): `RoundRobin` (count-balanced baseline), `CooperativeSticky`
(sticky baseline), and `LoadAwarePartitionAssignor` (the treatment).

### Workload model: why these parameters

Per-partition load is modelled as a **Zipf distribution** with exponent `s` —
the standard model for skewed key/partition popularity. A single knob spans the
whole regime: `s = 0` is uniform (the control, where the library must not hurt),
larger `s` is more skewed. Zipf weights are mapped to partition ids through a
**seeded random permutation**, so count-based assignors are measured over the
*expected* placement of hot partitions rather than one lucky/unlucky layout;
repetitions with different seeds give 95% confidence intervals.

The benefit depends on the **operating regime**, which the harness prints at
startup and records in the metadata:

```
thread capacity   = 1e6 / LISTENER_PROCESSING_COST_MICROS   (msg/s per consumer thread)
aggregate capacity= thread capacity × CONSUMER_REPLICAS × LISTENER_CONCURRENCY
mean utilisation  = TARGET_TOTAL_MSGS_PER_SEC / aggregate capacity
```

A consumer accrues lag once its assigned load exceeds its thread capacity. The
test listener simulates a fixed per-message service time (`LockSupport.parkNanos`,
configured by `LISTENER_PROCESSING_COST_MICROS`) precisely so that an overloaded
consumer falls behind — without this, an idle no-op consumer can never reveal a
difference between strategies. Choose `TARGET_TOTAL_MSGS_PER_SEC` and the service
time so that mean utilisation is moderate (~0.3–0.6): low enough that load-aware
keeps every consumer below capacity, high enough that the consumer holding the
hot partitions under a count-based assignor crosses it.

### Metrics

| Metric | What it shows | Source |
|--------|---------------|--------|
| **Imbalance ratio** = max / mean consumer load | Assignment quality, noise-free (computed from the live assignment + offered rates). This is exactly what `ThresholdTrigger` optimises. `1.0` is perfect balance. | per-partition `records-lag` labels + offered rates |
| **p99 end-to-end latency** | Production-user impact: the producer embeds its send time, the consumer records `now − sent` into a percentile-histogram timer. | `e2e_latency_seconds_bucket` via `histogram_quantile` |
| **Peak consumer lag** | Backlog on the most-loaded consumer. | `kafka_consumer_fetch_manager_records_lag_max` |
| **Rebalance count** | The *cost* side of the comparison (load-aware buys balance by triggering rebalances). | `kafka_consumer_coordinator_rebalance_total` |

### Experiments and outputs

- **Experiment A — static skew sweep.** Holds a fixed skewed load and sweeps
  `SKEW_EXPONENTS`, measuring steady-state metrics per strategy.
  → `test-out/result-default.png`: imbalance, p99 latency and peak lag vs. skew,
  one line per strategy with 95% CI bands.
- **Experiment B — controlled load shift.** Reshuffles the hot partitions at
  known instants and measures recovery (area under the lag curve per phase),
  exercising the proactive-rebalance trigger.
  → `test-out/result-default.shift.png`: peak lag over time with shift markers.
- `test-out/result-default.metadata.json`: seed, all parameters, the derived
  regime, and the aggregated metrics (mean ± 95% CI) plus raw per-repetition
  records — cite this in the paper and regenerate tables/figures from it.

### Running

```bash
# CI / demo profile (short windows, single repetition):
docker compose --env-file docker/test-env.properties -f docker/docker-compose.yaml up --abort-on-container-exit
```

**Paper profile.** For publishable numbers, increase the statistical and
temporal budget (these are the code defaults in `load_generator/config.py`):
`SKEW_EXPONENTS=0.0,0.5,1.0,1.5`, `REPETITIONS=5` (or more), `WARMUP_SECONDS=60`,
`STEADY_SECONDS=60`. Warmup must exceed the coordinator
`trigger-check-interval` (default 30s) so the proactive rebalance has time to
act. Override any value in `docker/test-env.properties`.

## License

See [LICENSE](LICENSE).
