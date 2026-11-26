# Configuration

[Quick start](../README.md#quick-start) · [Spring Boot properties](#spring-boot-properties) · [Weight stores](#weight-stores) · [Several groups](#several-consumer-groups) · [Multiple clusters](#multiple-kafka-clusters) · [Plain Java](#kafka-consumer-properties)

## Spring Boot properties

All keys in these tables start with `consumer-balancer.`. For example, the full key for the weight window is `consumer-balancer.offset-rate.rate-interval`.

| Property | Default | Effect |
| --- | --- | --- |
| `enabled` | `true` | Enable balancer auto-configuration, including metrics. |
| `instance-id` | Generated per JVM | Group consumers that share an application instance. Use a distinct ID for each instance. |
| `weight-store` | `offset-rate` | Choose `offset-rate` or `prometheus`. A `WeightService` bean overrides this choice. |
| `offset-rate.rate-interval` | `1m` | Window for estimating records/s from partition end offsets. |
| `offset-rate.sample-interval` | One quarter of the rate interval | Background sampling frequency. The derived default ranges from 1s to 30s. |
| `cross-group-balancing-enabled` | `false` | Include load from other groups in the same cluster registry. |

### Proactive rebalance settings

| Property | Default | Effect |
| --- | --- | --- |
| `proactive-rebalance-enabled` | `true` | Allow one coordinator per registered group to request a rebalance. Set to `false` for assignment only. |
| `coordinator.election-interval` | `30s` | How often each instance checks who should coordinate the group. |
| `coordinator.trigger-check-interval` | `30s` | How often the coordinator evaluates the trigger. |
| `rebalance-load-imbalance-threshold` | `1.1` | Trigger when the current peak instance load divided by the proposed peak exceeds this ratio. |
| `rebalance-min-violated-checks` | `2` | Violations required on one assignment. A balanced check subtracts one from the count. |
| `rebalance-cooldown` | `10m` | Minimum interval between proactive rebalances. Zero disables the cooldown and backoff. |
| `rebalance-max-cooldown` | `2h` | Cap on backoff after ineffective rebalances. Must be at least `rebalance-cooldown`. |
| `listener-ids` | Empty | Restrict rebalance requests for the auto-registered group to these Spring listener IDs. Empty selects all its containers. |

### Prometheus settings

| Property | Default | Effect |
| --- | --- | --- |
| `prometheus.weight-query-template` | Required for the default PromQL builder | Instant-vector query containing `%s` for the subscribed topics. |
| `prometheus.topic-label` | `topic` | Result label containing the topic name. |
| `prometheus.partition-label` | `partition` | Result label containing the partition number. |
| `prometheus.scheme` | `http` | URL scheme. |
| `prometheus.host` | `localhost` | Prometheus API host. |
| `prometheus.port` | `9090` | Prometheus API port. |
| `prometheus.path-prefix` | Empty | Prefix before `/api/v1/query`. For VictoriaMetrics use `/prometheus` (single node) or `/select/<accountID>/prometheus` (cluster). |
| `prometheus.authorization` | Unset | Value of the HTTP `Authorization` header. |
| `prometheus.connect-timeout` | `10s` | HTTP connection timeout. |
| `prometheus.request-timeout` | `30s` | Timeout for one query. |

## Weight stores

**Kafka end offsets.** The default store samples offsets on demand and in the background through Kafka's AdminClient. Each JVM keeps its own history for the partitions requested by any group in its registry. A partition needs two usable samples at least 500 ms apart. While the window fills, the rate uses the available history. A missing sample or a backwards offset (for example, after recreating a topic) gives that partition the default weight of `1.0`.

**Prometheus.** Use a query that returns one series per topic and partition. This example uses an exported end-offset metric. If you collect processing time, you can use the rate of a cumulative processing-seconds metric with the same labels to account for differences in record cost.

```yaml
consumer-balancer:
  weight-store: prometheus
  prometheus:
    host: localhost
    port: 9090
    weight-query-template: 'sum by (topic, partition) (rate(kafka_topic_partition_current_offset{topic=~"%s"}[1m]))'
```

The builder replaces `%s` with the regex-escaped topic names joined by `|`. Keep both identifying labels in the result when aggregating. If your exporter uses other labels, change the query and the corresponding label properties. A `KafkaRatePromqlBuilder` bean can supply the query programmatically.

**Custom weights.** Expose a [WeightService](../consumer-balancer-core/src/main/java/io/github/ruskaof/balancer/weight/WeightService.java) bean. The assignor and threshold trigger both use it. Missing, null or non-finite weights fall back to `1.0`. The library ignores entries outside the requested partition set.

To replace the placement algorithm, supply a [BalanceService](../consumer-balancer-core/src/main/java/io/github/ruskaof/balancer/balance/BalanceService.java) bean. The two-argument `computeOptimalAssignment` receives members and partition weights. With cross-group balancing enabled, implement the three-argument overload as well. Its last argument contains the other groups' load per instance.

## Several consumer groups

The starter registers `spring.kafka.consumer.group-id` if set. Register each additional group once with the same `ConsumerGroupBalancers` bean. For listeners managed by Spring, pass the group's ID and the endpoint registry.

```java
balancers.register("payments",
        ContainerRebalanceInitiator.of("payments", endpointRegistry));
```

For a consumer factory you create yourself, apply the registry's `assignorConfigs()` before creating consumers. Those configs install the assignor and pass it the same services and tracker as the group's monitor. Boot applies its customizer only to its auto-configured factory, where explicit consumer properties take precedence.

```java
Map<String, Object> props = new HashMap<>(consumerProperties);
props.put(ConsumerConfig.GROUP_ID_CONFIG, "payments");
props.putAll(balancers.assignorConfigs());
var factory = new DefaultKafkaConsumerFactory<String, byte[]>(props);
```

If your application manages its own containers, pass a supplier of the group's current containers to `ContainerRebalanceInitiator.of(groupId, supplier)`. The supplier runs on each rebalance, so it can return replacements. To set a different threshold for a group, use the group builder in place of `register(groupId, initiator)`.

```java
balancers.group("payments")
        .rebalanceInitiator(initiator)
        .imbalanceThreshold(1.3)
        .register();
```

Use `.trigger(...)` on the builder to replace a group's threshold trigger. For the auto-registered group, a unique `RebalanceTrigger` or `RebalanceInitiator` bean replaces its respective default. Spring starts registered groups with the context and stops monitoring before listener containers shut down. A group registered after startup begins monitoring on registration.

### Balancing combined load across groups

Set `consumer-balancer.cross-group-balancing-enabled=true` or call `.crossGroupBalancing(true)` on the plain Java registry builder. When placing partitions for one group, the assignor starts each instance at the load it already carries for other registered groups. Each decision moves only the current group's partitions. If two groups consume the same partition, its weight counts twice.

Before starting consumers, register the same groups and enable the setting on every participating instance. Use one registry per cluster and pass its `assignorConfigs()` to each consumer factory. Each instance needs a local consumer in every participating nonempty group to receive its topology. Use comparable weight units across groups.

The monitors coordinate proactive requests on one instance. A request reserves a correction until acknowledged or until the 30-second reservation expires. Concurrent Kafka rebalances can still occur. If another group's topology or weights are unavailable, ordinary assignment uses the current group's weights alone and proactive checks wait. With proactive rebalance disabled, combined load still influences ordinary assignment.

## Multiple Kafka clusters

Each cluster needs its own registry, AdminClient, weight store and member tracker. The starter creates these for the cluster in `spring.kafka.*` and manages the lifecycle and metrics of additional registry beans. For a second cluster, declare its beans alongside its consumer factory.

```java
@Bean(defaultCandidate = false)
AdminClient clusterBAdminClient() {
    return AdminClient.create(clusterBAdminProperties());
}

@Bean(defaultCandidate = false, destroyMethod = "close")
KafkaOffsetRateWeightService clusterBWeightService(@Qualifier("clusterBAdminClient") AdminClient admin) {
    return new KafkaOffsetRateWeightService(admin, Duration.ofMinutes(1));
}

@Bean(destroyMethod = "close")
ConsumerGroupBalancers clusterBBalancers(
        @Qualifier("clusterBAdminClient") AdminClient admin,
        @Qualifier("clusterBWeightService") WeightService weights,
        KafkaListenerEndpointRegistry endpoints) {
    var balancers = ConsumerGroupBalancers.builder()
            .adminClient(admin)
            .weightService(weights)
            .tags(Map.of("cluster", "b"))
            .build();
    balancers.register("orders", ContainerRebalanceInitiator.of("orders", endpoints)
            .onlyListenerIds(List.of("ordersOnClusterB")));
    return balancers;
}
```

Pass `clusterBBalancers.assignorConfigs()` to cluster B's factory. The `defaultCandidate = false` declarations keep the first cluster's default beans available. Select its registry with `@Qualifier(BalancerAutoConfiguration.BALANCERS_BEAN_NAME)`.

When clusters reuse group IDs, scope the initiator on **each** cluster to explicit listener IDs. The example restricts cluster B to `ordersOnClusterB`. Set `consumer-balancer.listener-ids` for the auto-configured cluster. For other selection criteria use `ContainerRebalanceInitiator.filter(...)`.

## Kafka consumer properties

For the core library, set `partition.assignment.strategy=io.github.ruskaof.balancer.LoadAwarePartitionAssignor`. The library-specific keys below start with `assignor.load-aware.`.

| Property | Value |
| --- | --- |
| `instance-id` | Application-instance ID, shared by consumers on that instance. Defaults to a random ID generated once per JVM. |
| `weight-service` | Custom `WeightService`. Takes precedence over `weight-store`. |
| `balance-service` | Custom placement algorithm. Defaults to `SortingRoundRobinBalanceService`. |
| `member-id-tracker` | Shared `MemberIdTracker` for proactive monitoring and static-member restart recovery. |
| `cross-group-load-service` | Shared service supplied by the registry when cross-group balancing is enabled. Requires the shared tracker. |
| `weight-store` | `offset-rate` (default) or `prometheus`. |
| `offset-rate.rate-interval-ms` | Rate window in milliseconds. Default `60000`. |
| `offset-rate.sample-interval-ms` | Sampling interval in milliseconds. Defaults to a quarter of the window, bounded to 1–30 seconds. |
| `prometheus.host`, `prometheus.port`, `prometheus.weight-query-template` | All three are required when the core assignor constructs a Prometheus store. |
| `prometheus.topic-label`, `prometheus.partition-label`, `prometheus.scheme`, `prometheus.path-prefix`, `prometheus.authorization` | Same meanings and defaults as the Spring Boot settings. |
| `prometheus.connect-timeout-ms`, `prometheus.request-timeout-ms` | Timeouts in milliseconds. Defaults are `10000` and `30000`. |

Service and tracker keys accept objects, classes or fully qualified class names. Classes need a public no-arg constructor. Implementations of Kafka's `Configurable` receive the consumer config map. Kafka may log these library-specific keys as unknown.

When the assignor builds the offset-rate store, it reuses the consumer's bootstrap servers and compatible security properties. Its AdminClient and daemon sampler live for the lifetime of the JVM. For explicit shutdown, pass a store instance that your application owns.

### Proactive rebalance in plain Java

Build a registry per cluster and pass its configs to each consumer. The registry invokes your rebalance initiator on a coordinator thread. Hand the request to the consumer's poll thread, which owns the `KafkaConsumer`. In this example, `running` and `handle(record)` belong to the application.

```java
String groupId = "orders";
Map<String, Object> props = new HashMap<>();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

try (var admin = AdminClient.create(props);
     var weights = new KafkaOffsetRateWeightService(admin, Duration.ofMinutes(1));
     var balancers = ConsumerGroupBalancers.builder().adminClient(admin).weightService(weights).build()) {
    var rebalanceRequested = new AtomicBoolean();
    balancers.register(groupId, () -> rebalanceRequested.set(true));
    props.putAll(balancers.assignorConfigs());

    try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
        consumer.subscribe(List.of("orders"));
        balancers.start();
        while (running.get()) {
            if (rebalanceRequested.getAndSet(false)) {
                consumer.enforceRebalance();
            }
            consumer.poll(Duration.ofMillis(250)).forEach(record -> handle(record));
        }
    }
}
```

For manual wiring, share one `MemberIdTracker` between the assignor, `ThresholdTrigger` and `CoordinatorElection.Builder.setMemberIdTracker(tracker)`. The coordinator manager uses the election's tracker. In 10.x this replaces `InstanceUserData`, `GroupInstanceUserData`, `MemberIdTracker.recordInstanceIds/getInstanceIds` and `CoordinatorElection.Builder.setMemberIdsSupplier`. See the [group upgrade procedure](../README.md#upgrading-monitoring-metadata).
