# consumer-balancer

Load-aware Kafka consumer partition assignment driven by per-partition **weights** (default: events/sec measured from partition end offsets via Kafka's AdminClient), plus optional **proactive rebalance** when an elected group member detects load imbalance.

Built-in Kafka assignors such as `RangeAssignor` and `RoundRobinAssignor` balance **partition count** (and sticky strategies minimize movement). They do not use **per-partition load** signals. This library assigns partitions with a greedy **least-loaded** strategy using those weights, which reduces the worst consumer load when weights are skewed.

## Modules

| Module | Purpose |
|--------|---------|
| `consumer-balancer-core` | `LoadAwarePartitionAssignor`, weight stores (Kafka offset-rate, Prometheus), balancing, triggers |
| `consumer-balancer-spring-boot-starter` | Spring Boot auto-configuration |
| `test-listener` | Example Spring Boot app |

## Requirements

- Spring Boot **4.0+** (Spring Framework 7, Spring for Apache Kafka 4.0, Apache Kafka clients 4.1+)
- Java **21+**

## Quickstart (Gradle)

Both modules are published to [Maven Central](https://central.sonatype.com/artifact/io.github.ruskaof/consumer-balancer-spring-boot-starter). Most users only need the starter, which pulls in `consumer-balancer-core` transitively:

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-spring-boot-starter:5.0.1")
}
```

Using the assignor without Spring Boot? Depend on the core module directly:

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-core:5.0.1")
}
```

Minimal configuration — no weight backend needed, the default **offset-rate** weight store measures each partition's events/sec from end-offset growth through the Kafka AdminClient:

```yaml
spring:
  kafka:
    consumer:
      group-id: my-group
      properties:
        partition.assignment.strategy: io.github.ruskaof.balancer.LoadAwarePartitionAssignor
```

Optionally tune the measurement window:

```yaml
consumer-balancer:
  offset-rate:
    rate-interval: 5m   # default 1m
```

## Weight stores

A weight store (`WeightService`) supplies the per-partition load weights that drive both the assignor and the proactive rebalance trigger. Pick one with `consumer-balancer.weight-store`, or [define your own bean](#custom-weight-store).

### `offset-rate` (default) — Kafka end-offset rates

Snapshots partition end offsets through the Kafka AdminClient — on every weight computation and in a background thread every `sample-interval` — and weighs each partition by its offset growth over the last `rate-interval`, i.e. its **produce rate in events/sec**. Works out of the box against the same cluster (and with the same security settings) as the consumer.

Notes:

- The very first assignment after startup has no offset history yet, so every partition gets the default weight `1.0` (a count-balanced assignment); weights kick in once two snapshots at least ~`rate-interval` apart exist. With proactive rebalance on (the default), the group converges to a load-aware assignment automatically.
- Each instance measures independently from the same source (broker end offsets), so no shared metrics infrastructure is required.
- Weights reflect the **produce** rate. If your per-event processing cost varies wildly per partition, consider the Prometheus store with a cost-based metric, or a custom `WeightService`.

### `prometheus` — PromQL weight query

Set `consumer-balancer.weight-store: prometheus` to load weights from a Prometheus-compatible backend instead:

```yaml
consumer-balancer:
  weight-store: prometheus
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

The Prometheus store works with any backend that serves the Prometheus query API. For VictoriaMetrics, set `consumer-balancer.prometheus.path-prefix` to `/prometheus` (single-node) or `/select/<accountID>/prometheus` (cluster vmselect):

```yaml
consumer-balancer:
  prometheus:
    host: vmselect
    port: 8481
    path-prefix: /select/0/prometheus
```

## Bean wiring

The starter injects the application context's `WeightService`, `BalanceService` and (when proactive rebalance is enabled) `MemberIdTracker` beans into Spring Boot's auto-configured consumer factory under the `assignor.load-aware.*` keys, so the assignor uses exactly the same collaborators as the rebalance trigger. Values set explicitly under `spring.kafka.consumer.properties.assignor.load-aware.*` win over the injected beans.

If you define your own `ConsumerFactory` bean, Boot's factory customizers do not run for it — set the `assignor.load-aware.*` keys on your factory yourself (the `BalancerConsumerFactoryCustomizer` bean can be applied manually).

## Configuration reference (`consumer-balancer`)

| Property | Default | Description |
|----------|---------|-------------|
| `consumer-balancer.enabled` | `true` | Master switch for balancer auto-configuration. |
| `consumer-balancer.proactive-rebalance-enabled` | `true` | When `true`, one elected consumer runs the threshold trigger and may call `enforceRebalance()` on listener containers. |
| `consumer-balancer.rebalance-load-imbalance-threshold` | `1.1` | Proactive rebalance when `(max member load) / (optimal max load) > threshold` (see `ThresholdTrigger`). |
| `consumer-balancer.weight-store` | `offset-rate` | Built-in weight store to auto-configure: `offset-rate` or `prometheus`. Ignored when a custom `WeightService` bean is defined. |
| `consumer-balancer.offset-rate.rate-interval` | `1m` | Window over which end-offset growth is turned into an events/sec weight. |
| `consumer-balancer.offset-rate.sample-interval` | `rate-interval / 4` | How often end offsets are sampled in the background (default clamped between `1s` and `30s`). |
| `consumer-balancer.prometheus.weight-query-template` | — | **Required** when `weight-store=prometheus`: PromQL with `%s`. Series must include the topic and partition labels (see below). |
| `consumer-balancer.prometheus.topic-label` | `topic` | Label on the weight-query series that carries the topic name. |
| `consumer-balancer.prometheus.partition-label` | `partition` | Label on the weight-query series that carries the partition number. |
| `consumer-balancer.prometheus.scheme` | `http` | Prometheus URL scheme. |
| `consumer-balancer.prometheus.host` | `localhost` | Prometheus host. |
| `consumer-balancer.prometheus.port` | `9090` | Prometheus port. |
| `consumer-balancer.prometheus.path-prefix` | *(empty)* | Path prefix prepended to `/api/v1/query` for Prometheus-API-compatible backends, e.g. `/prometheus` (single-node VictoriaMetrics) or `/select/<accountID>/prometheus` (VictoriaMetrics cluster). |
| `consumer-balancer.prometheus.authorization` | *(none)* | Optional `Authorization` header value sent with every query, e.g. `Bearer <token>` or `Basic <base64>`. |
| `consumer-balancer.prometheus.connect-timeout` | `10s` | HTTP connect timeout. |
| `consumer-balancer.prometheus.request-timeout` | `30s` | HTTP request timeout (per query). |
| `consumer-balancer.coordinator.election-interval` | `30s` | How often coordinator election runs. |
| `consumer-balancer.coordinator.trigger-check-interval` | `30s` | How often the coordinator evaluates the rebalance trigger. |

## Assignor configuration (consumer configs)

`LoadAwarePartitionAssignor` reads its collaborators from the Kafka consumer configs. Each of these keys accepts an **instance** (when the config map is built programmatically), a **`Class`**, or a **fully-qualified class name** (instantiated via its public no-arg constructor; implementations of Kafka's `Configurable` receive the consumer configs):

- `assignor.load-aware.weight-service` — `WeightService` used for assignment. When absent, the default selected by `assignor.load-aware.weight-store` is built.
- `assignor.load-aware.balance-service` — `BalanceService` used for assignment (default: `SortingRoundRobinBalanceService`).
- `assignor.load-aware.member-id-tracker` — optional `MemberIdTracker` that receives this consumer's member id after every rebalance (needed for proactive rebalance).

Weight-store keys, used **only** when `assignor.load-aware.weight-service` is not set (the Spring Boot starter covers this case by injecting the `WeightService` bean instead):

- `assignor.load-aware.weight-store` — `offset-rate` (default) or `prometheus`.
- `assignor.load-aware.offset-rate.rate-interval-ms` (default: `60000`)
- `assignor.load-aware.offset-rate.sample-interval-ms` (default: a quarter of the rate interval, clamped between 1 and 30 seconds)

The offset-rate default builds its own AdminClient by reusing the consumer's `bootstrap.servers` and security configs, so it usually needs no extra keys at all. That admin client and its sampler thread (daemon) live until JVM exit — Kafka offers no hook to close reflectively created assignor collaborators; if you need explicit lifecycle control, pass a pre-built `KafkaOffsetRateWeightService` instance via `assignor.load-aware.weight-service` and close it yourself.

Prometheus keys, required when `assignor.load-aware.weight-store` is set to `prometheus`:

- `assignor.load-aware.prometheus.weight-query-template`
- `assignor.load-aware.prometheus.topic-label` (default: `topic`)
- `assignor.load-aware.prometheus.partition-label` (default: `partition`)
- `assignor.load-aware.prometheus.host`
- `assignor.load-aware.prometheus.port`
- `assignor.load-aware.prometheus.scheme`
- `assignor.load-aware.prometheus.path-prefix`
- `assignor.load-aware.prometheus.authorization`
- `assignor.load-aware.prometheus.connect-timeout-ms`
- `assignor.load-aware.prometheus.request-timeout-ms`

Plain-Java example with a custom weight source and member tracking for proactive rebalance:

```java
MemberIdTracker tracker = new MemberIdTracker();
WeightService weights = new MyDatabaseWeightService(dataSource);

Map<String, Object> configs = new HashMap<>();
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "my-group");
configs.put("partition.assignment.strategy", LoadAwarePartitionAssignor.class.getName());
configs.put("assignor.load-aware.weight-service", weights);    // or a class name
configs.put("assignor.load-aware.member-id-tracker", tracker); // optional

var consumer = new KafkaConsumer<>(configs, new StringDeserializer(), new ByteArrayDeserializer());
// For proactive rebalance, hand the same tracker to the election:
// new CoordinatorElection.Builder()
//         .setMemberIdsSupplier(() -> tracker.getCurrentMemberIds("my-group"))
//         ...
```

> Kafka logs a "supplied but isn't a known config" warning for these custom keys — that is harmless.

## Custom weight store

Implement `io.github.ruskaof.balancer.weight.WeightService` and expose it as a Spring `@Bean`. Both built-in weight stores back off when a `WeightService` bean is present, and the bean drives **both** the `LoadAwarePartitionAssignor` and the proactive `ThresholdTrigger`. The same override mechanism applies to `BalanceService`.

The returned map is treated as a lookup over the requested partitions: requested partitions that are missing (or mapped to `null`/non-finite values) fall back to the default weight `1.0`, and entries for partitions that were not requested are ignored.

Optionally provide your own `io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder` (or `TemplatedKafkaRatePromqlBuilder`) for custom PromQL while still using Prometheus.

## Operations

- With the default offset-rate store, the first assignment after an instance becomes group leader uses default weights (no offset history yet) and is effectively count-balanced; subsequent assignments and proactive-trigger checks use measured rates. Partitions whose end offset went backwards (e.g. a recreated topic) fall back to the default weight `1.0` until fresh history accrues.
- With the Prometheus store, your PromQL must be an **instant vector** query returning series with `topic` and `partition` labels so weights can be mapped to `TopicPartition`. If your metrics use different label names (e.g. `kafka_topic`), set `consumer-balancer.prometheus.topic-label` / `partition-label` accordingly — remember the `by (...)` clause of the query must keep those labels. Partitions without a sample — including `NaN`/`Inf` samples — get the default weight `1.0`.
- Partitions are assigned only to members subscribed to their topic, so groups whose members subscribe to different topic sets are handled correctly.
- Proactive rebalance requires a group id in `spring.kafka.consumer.group-id`; only the listener containers of that group receive `enforceRebalance()`.
- If load-aware assignment throws, `LoadAwarePartitionAssignor` falls back to Kafka’s `RoundRobinAssignor`.
- `LoadAwarePartitionAssignor` is a **client-side** assignor, so it applies only under the *classic* consumer group protocol (`group.protocol=classic`, the default on Kafka 4.x). If you opt into the new KIP-848 protocol (`group.protocol=consumer`), partitions are assigned broker-side and this assignor is bypassed — along with the member-id tracking that proactive rebalance relies on.

## Build

This repository is built with **Gradle on the PATH** (not the wrapper), for example:

```bash
gradle test
```

## Performance test (Docker)

End-to-end run compares **RoundRobin** vs **load-aware** consumers with skewed synthetic load. It writes:

- `test-out/result-default.png` — throughput rebalance spikes when Micrometer exposes `kafka_consumer_*rebalance*` counters.


```bash
docker compose --env-file docker/test-env.properties -f docker/docker-compose.yaml up --abort-on-container-exit
```

## License

See [LICENSE](LICENSE).
