# consumer-balancer

Load-aware Kafka consumer partition assignment driven by per-partition **weights** (default: events/sec measured from partition end offsets via Kafka's AdminClient), plus optional **proactive rebalance** when an elected group member detects load imbalance.

Built-in Kafka assignors such as `RangeAssignor` and `RoundRobinAssignor` balance **partition count** (and sticky strategies minimize movement). They do not use **per-partition load** signals, and they treat every consumer as independent even when several consumers are threads of one application instance. This library assigns partitions with a greedy **least-loaded** strategy using those weights, evening traffic across **application instances** (pods/JVMs) first and across the consumers inside each instance second — see [Instance-aware balancing](#instance-aware-balancing).

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
    implementation("io.github.ruskaof:consumer-balancer-spring-boot-starter:7.0.0")
}
```

Using the assignor without Spring Boot? Depend on the core module directly:

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-core:7.0.0")
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

- The very first assignment after startup has no offset history yet, so every partition gets the default weight `1.0` (a count-balanced assignment); weights kick in once two snapshots at least ~`rate-interval` apart exist. With proactive rebalance on (the default), the group converges to a load-aware assignment automatically, once the imbalance has held for `consumer-balancer.rebalance-min-violated-checks` checks.
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

## Instance-aware balancing

One application instance (a pod, a JVM) usually runs **several** consumers — with Spring, `listener containers × spring.kafka.listener.concurrency` group members. Those members share the instance's CPU, so balancing per member is not enough: two heavy members could land in one pod, and when the group has **more members than partitions** a plain member-level assignor leaves arbitrary members — and therefore arbitrary pods — idle.

The assignor therefore balances in two levels:

1. Every member reports an **instance id** to the group leader (through subscription userData). Members sharing an id form one instance.
2. Partitions are placed heaviest-first onto the eligible **instance** with the lowest total load, then onto the least-loaded member **inside** that instance. Zero-weight partitions spread by count at both levels.

The result: instances receive equal traffic regardless of how many members each runs, heavy partitions never pile onto one pod while another idles, and with fewer partitions than members every instance still gets its fair share (`floor(P/I)`–`ceil(P/I)` partitions across `I` instances).

The instance id resolves in this order:

1. `consumer-balancer.instance-id` property (or the `assignor.load-aware.instance-id` consumer config) — set it when you want stable, human-readable instance labels (e.g. the pod name) in the leader's assignment logs;
2. otherwise a **random id generated once per JVM** — every consumer in the JVM shares it, and distinct JVMs never collide, even on one machine.

A member whose userData carries no readable instance id (e.g. an older library version during a rolling upgrade) is treated as its own single-member instance, so mixed-version groups keep working and converge once the rollout completes.

The proactive `ThresholdTrigger` compares **instance-level** loads too — see [Proactive rebalance](#proactive-rebalance) for how it approximates instances and why it is deliberately slow to act.

## Proactive rebalance

One elected member (the coordinator) checks the group every `consumer-balancer.coordinator.trigger-check-interval` (default `30s`) and may force a rebalance. **Every proactive rebalance stops the whole group**, so the trigger is built to under-react rather than over-react.

It has to be, because it watches the group from the *outside*, through the AdminClient, and that view is only an approximation of what the assignor sees:

- **instances** — the AdminClient cannot read subscription userData, so members are grouped by their broker-observed client host. Whenever each JVM has its own address (one pod = one IP in Kubernetes) that is exactly the assignor's per-JVM grouping; several JVMs per machine or host-network pods make the two disagree.
- **subscriptions** — the AdminClient cannot see them either, so every member counts as eligible for every topic in the group. That matches the instance-level load being compared as long as every instance runs the whole set of listeners, which is the normal case for identical replicas.
- **weights** — the coordinator measures them itself, while the assignment it judges was computed from the group leader's own, equally valid, measurements taken at a different moment.

Each of those can make the computed optimum unreachable — and since the assignor is deterministic, an unreachable optimum asks for the same useless rebalance on every check. Three guards keep that from becoming a rebalance storm:

1. **Stable groups only.** While the group is rebalancing, the AdminClient reports partial or previous-generation assignments. Judging those would fire again on the rebalance the trigger has just caused, which is a self-sustaining loop. Non-stable checks are skipped entirely and do not even count toward the hysteresis below.
2. **Hysteresis.** The imbalance must show up on `consumer-balancer.rebalance-min-violated-checks` checks *of one unchanged assignment* (default `2`) before it counts as real rather than as a noisy weight sample. A check that finds the group balanced *decays* that count by one instead of resetting it: right after the load moves, the weight window still spans the load being replaced, so the ratio drifts back and forth across the threshold — a strict reset would restart the count over and over exactly when the trigger is most needed. Moving a partition does reset it, because a streak about one assignment says nothing about another.
3. **Cooldown with backoff.** Two rebalances are never closer together than `consumer-balancer.rebalance-cooldown` (default `10m`) — whether or not the previous one changed the assignment, which is what bounds the cost when the trigger and the assignor disagree. Every rebalance that does not bring the group within the threshold doubles the cooldown up to `consumer-balancer.rebalance-max-cooldown` (default `2h`), with a warning naming the likely causes; the cooldown returns to its base as soon as the group is seen balanced again.

**How long a correction takes** is the sum of three things, and the defaults assume load that drifts over tens of minutes:

```
offset-rate rate-interval          60s   the weights must catch up to the new load
+ check-interval x min-violated-checks    60s   the imbalance must be confirmed
+ whatever is left of the cooldown
```

So a genuine, sustained imbalance is corrected in roughly two minutes, while a disagreement the assignor cannot resolve costs one rebalance and then fades to one attempt every two hours. If your load moves faster than that, shorten `offset-rate.rate-interval` and `coordinator.trigger-check-interval` first — they are what the detection latency is actually made of; `rebalance-min-violated-checks` buys little, because the weight store already averages over its own window and consecutive checks are correlated samples of it.

Note that `consumer-balancer.rebalance-load-imbalance-threshold` stays tight (`1.1`) on purpose. It is tempting to raise it as a storm guard, but the cooldown backoff already bounds what a false positive costs, whereas a raised threshold silently loses real corrections — one badly placed hot partition often shows up as only a 10–20% instance-level skew.

## Multiple Kafka clusters

The whole balancer stack is per-cluster: an admin client, a weight store, a `MemberIdTracker`, a coordinator election and a trigger all belong to exactly one cluster. The starter auto-configures that stack for Spring Boot's auto-configured consumer; a second cluster needs a second set, hand-wired next to the second `ConsumerFactory` and `KafkaListenerContainerFactory` you already define for it (exactly as with plain Spring for Apache Kafka). Every bean of the proactive path backs off when the application defines its own (`@ConditionalOnMissingBean`), so you can also replace pieces of the auto-configured stack instead of adding to it.

One thing does **not** follow from the group id: **which containers belong to which cluster.** Applications normally reuse the same group id on every cluster, so a rebalance initiator selecting containers by group id alone would rebalance every cluster whenever one of them is imbalanced. Scope it by listener id:

```yaml
consumer-balancer:
  listener-ids: [orders, payments]   # the @KafkaListener ids that consume from the auto-configured cluster
```

and give the second cluster's stack its own initiator:

```java
@Bean
CoordinatorManager.RebalanceInitiator clusterBRebalanceInitiator(KafkaListenerEndpointRegistry registry) {
    return ContainerRegistryRebalanceInitiator.withListenerIds(
            registry, "my-group", List.of("ordersOnClusterB"));
}
```

Listener ids are the only stable, publicly readable identity a `MessageListenerContainer` carries besides its group id, which is why they are the hook. Give the listeners explicit ids (`@KafkaListener(id = "orders", ...)`) — the generated ones are positional and not stable across refactorings. For anything else, `ContainerRegistryRebalanceInitiator` also accepts an arbitrary `Predicate<MessageListenerContainer>`.

Give each cluster its own `MemberIdTracker` too (one per `ConsumerFactory`): the tracker keys member ids by group id, so sharing one instance between clusters that reuse a group id would pool member ids from both.

## Bean wiring

The starter injects the application context's `WeightService`, `BalanceService` and (when proactive rebalance is enabled) `MemberIdTracker` beans into Spring Boot's auto-configured consumer factory under the `assignor.load-aware.*` keys, so the assignor uses exactly the same collaborators as the rebalance trigger. Values set explicitly under `spring.kafka.consumer.properties.assignor.load-aware.*` win over the injected beans.

If you define your own `ConsumerFactory` bean, Boot's factory customizers do not run for it — set the `assignor.load-aware.*` keys on your factory yourself (the `BalancerConsumerFactoryCustomizer` bean can be applied manually).

Every bean of the proactive path — `MemberIdTracker`, `RebalanceTrigger`, `CoordinatorManager.RebalanceInitiator`, `CoordinatorManager`, `CoordinatorManagerLifecycle` — is `@ConditionalOnMissingBean`, so defining your own replaces the auto-configured one rather than colliding with it. That is what makes a [second cluster's stack](#multiple-kafka-clusters) wirable by hand.

## Configuration reference (`consumer-balancer`)

| Property | Default | Description |
|----------|---------|-------------|
| `consumer-balancer.enabled` | `true` | Master switch for balancer auto-configuration. |
| `consumer-balancer.proactive-rebalance-enabled` | `true` | When `true`, one elected consumer runs the threshold trigger and may call `enforceRebalance()` on listener containers. |
| `consumer-balancer.rebalance-load-imbalance-threshold` | `1.1` | Proactive rebalance when `(max instance load) / (optimal max instance load) > threshold` (see [Proactive rebalance](#proactive-rebalance)). |
| `consumer-balancer.instance-id` | *(auto)* | Application-instance id shared by every consumer in this JVM; members reporting the same id are balanced as one instance. Default: a random id generated once per JVM. |
| `consumer-balancer.rebalance-min-violated-checks` | `2` | Trigger checks that must see the imbalance on one unchanged assignment before a rebalance is fired; a balanced check decays the count by one. `1` fires on first sight. |
| `consumer-balancer.rebalance-cooldown` | `10m` | Minimum time between two proactive rebalances, regardless of whether the previous one changed anything. `0` disables the cooldown and its backoff. |
| `consumer-balancer.rebalance-max-cooldown` | `2h` | Ceiling for the cooldown after it has been doubled by rebalances that did not restore balance. Must not be shorter than `rebalance-cooldown`. |
| `consumer-balancer.listener-ids` | *(empty)* | Listener container ids the proactive rebalance may touch; empty means every registered container of the group id. Set it when several Kafka clusters share the group id — see [Multiple Kafka clusters](#multiple-kafka-clusters). |
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

Plus one plain-string key:

- `assignor.load-aware.instance-id` — the application-instance id this consumer reports to the group leader (see [Instance-aware balancing](#instance-aware-balancing)). Default: a random id generated once per JVM.

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

Implement `io.github.ruskaof.balancer.weight.WeightService` and expose it as a Spring `@Bean`. Both built-in weight stores back off when a `WeightService` bean is present, and the bean drives **both** the `LoadAwarePartitionAssignor` and the proactive `ThresholdTrigger`. The same override mechanism applies to `BalanceService` (`computeOptimalAssignment(Collection<GroupMember>, Map<TopicPartition, Double>)` — each `GroupMember` carries its member id, instance id and subscribed topics).

The returned map is treated as a lookup over the requested partitions: requested partitions that are missing (or mapped to `null`/non-finite values) fall back to the default weight `1.0`, and entries for partitions that were not requested are ignored.

Optionally provide your own `io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder` (or `TemplatedKafkaRatePromqlBuilder`) for custom PromQL while still using Prometheus.

## Operations

- With the default offset-rate store, the first assignment after an instance becomes group leader uses default weights (no offset history yet) and is effectively count-balanced; subsequent assignments and proactive-trigger checks use measured rates. Partitions whose end offset went backwards (e.g. a recreated topic) fall back to the default weight `1.0` until fresh history accrues.
- With the Prometheus store, your PromQL must be an **instant vector** query returning series with `topic` and `partition` labels so weights can be mapped to `TopicPartition`. If your metrics use different label names (e.g. `kafka_topic`), set `consumer-balancer.prometheus.topic-label` / `partition-label` accordingly — remember the `by (...)` clause of the query must keep those labels. Partitions without a sample — including `NaN`/`Inf` samples — get the default weight `1.0`.
- Partitions are assigned only to members subscribed to their topic, so groups whose members subscribe to different topic sets are handled correctly.
- With more members than partitions, partitions spread evenly across **instances** (some members inside each instance stay idle); with fewer instances than partitions than members, every instance carries a near-equal share of the measured traffic. Instances receive equal traffic regardless of their member counts — an instance running fewer threads gets the same load on fewer, busier members.
- The threshold trigger approximates instances by the broker-observed client host of each member. Whenever each JVM has its own address (one pod = one IP in Kubernetes), that induces exactly the assignor's per-JVM grouping. When instances share an address (several JVMs per machine, host-network pods), or when instances run *different* sets of listeners, the trigger's view and the assignor's diverge; the cooldown backoff then fades the useless rebalances out and logs a warning naming the cause. See [Proactive rebalance](#proactive-rebalance).
- The trigger only judges a **stable** group, so a check landing during a rebalance is skipped rather than acted on. Expect `ThresholdTrigger skipped ... group state is PREPARING_REBALANCE` at debug level around every rebalance.
- Proactive rebalance requires a group id in `spring.kafka.consumer.group-id`; only the listener containers of that group — narrowed by `consumer-balancer.listener-ids` when set — receive `enforceRebalance()`. When no container matches, a warning is logged instead of silently doing nothing.
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
