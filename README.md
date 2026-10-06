# consumer-balancer

Load-aware Kafka consumer partition assignment driven by per-partition **weights** (default: events/sec measured from partition end offsets via Kafka's AdminClient), plus optional **proactive rebalance** when an elected group member detects load imbalance.

Built-in Kafka assignors such as `RangeAssignor` and `RoundRobinAssignor` balance **partition count** (and sticky strategies minimize movement). They do not use **per-partition load** signals, and they treat every consumer as independent even when several consumers are threads of one application instance. This library assigns partitions with a greedy **least-loaded** strategy using those weights, evening traffic across **application instances** (pods/JVMs) first and across the consumers inside each instance second — see [Instance-aware balancing](#instance-aware-balancing).

## Modules

| Module | Purpose |
|--------|---------|
| `consumer-balancer-core` | `LoadAwarePartitionAssignor`, weight stores (Kafka offset-rate, Prometheus), balancing, triggers |
| `consumer-balancer-spring-boot-starter` | Spring Boot auto-configuration, Micrometer metrics |
| `test-listener` | Example Spring Boot app |

## Requirements

- Spring Boot **4.0+** (Spring Framework 7, Spring for Apache Kafka 4.0, Apache Kafka clients 4.1+)
- Java **21+**

## Quickstart (Gradle)

Both modules are published to [Maven Central](https://central.sonatype.com/artifact/io.github.ruskaof/consumer-balancer-spring-boot-starter). Most users only need the starter, which pulls in `consumer-balancer-core` transitively:

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-spring-boot-starter:10.1.0")
}
```

Using the assignor without Spring Boot? Depend on the core module directly:

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-core:10.1.0")
}
```

Minimal configuration — no weight backend needed, the default **offset-rate** weight store measures each partition's events/sec from end-offset growth through the Kafka AdminClient:

```yaml
spring:
  kafka:
    consumer:
      group-id: my-group
```

The starter sets `partition.assignment.strategy` of Boot's consumer factory to `LoadAwarePartitionAssignor`. A strategy you configure yourself under `spring.kafka.consumer.properties.partition.assignment.strategy` wins.

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
- One store serves every consumer group of the cluster: each partition keeps its own history, and the background sampler covers every partition any group has asked about. A partition stops being sampled only after it failed three samples in a row that other partitions survived (typically a deleted topic).
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

Every member must report the current subscription format. Missing or unreadable metadata causes round-robin assignment and disables proactive monitoring for that assignment; instance grouping is never guessed. Version 10 introduces a breaking protocol change; see [Upgrading monitoring metadata](#upgrading-monitoring-metadata).

By default, the leader sends a topology snapshot containing application-instance ids and actual topic subscriptions to **one monitoring consumer**. Every other consumer receives a small acknowledgement containing the monitor identity and its own expected instance id, topics and request revision. The monitor's `MemberIdTracker` combines that snapshot with the AdminClient's current assignments, so `ThresholdTrigger` uses the same instance grouping and subscription constraints as the assignor. With [cross-group balancing](#balancing-combined-load-across-groups) enabled, one consumer per application instance receives the full topology so other groups can account for that instance's load.

## Proactive rebalance

One designated member (the coordinator) checks the group every `consumer-balancer.coordinator.trigger-check-interval` (default `30s`) and may force a rebalance. By default, the assignor selects the smallest typed identity among consumers advertising monitoring support: `s:<group.instance.id>` for static members, or `m:<memberId>` for dynamic members. With cross-group balancing enabled, it orders candidates by application-instance id first, then typed member identity, keeping the registered groups' monitors on the same instance. Static identities preserve ownership when broker member ids change, and an idle consumer can still monitor. `CoordinatorElection` verifies that this designated consumer is live, belongs to this JVM, and has a current topology. **Every proactive rebalance stops the whole group**, so the trigger is built to under-react rather than over-react.

The monitor validates its topology before judging the group:

- **instances and subscriptions** — the AdminClient cannot read these from subscription userData. The snapshot supplies both, including subscriptions of idle members, and the trigger only considers placements allowed by those subscriptions. If the snapshot is unavailable, stale, or does not match the live membership, the check is **skipped**. Grouping does not depend on client addresses, which several pods can share.
- **metadata size** — the aggregate budget for added assignment metadata is **512 KiB**, covering the full snapshot and all member acknowledgements, including the additional snapshot copies when cross-group balancing is enabled. Common instance ids and subscription sets are deduplicated inside the snapshot. If the budget is exceeded, normal partition assignment continues and a warning explains why proactive monitoring is unavailable. Kafka's limits on native subscriptions, assignments and group metadata still apply separately.
- **weights** — the coordinator measures them itself, while the assignment it judges was computed from the group leader's own, equally valid, measurements taken at a different moment. Right after an instance becomes coordinator, its weight store may have no history for the group yet; a check with no usable weight at all is **skipped** rather than judged on partition counts, and the next check normally has weights.

Weight measurements taken at different times can still make the monitor and assignor disagree about the best assignment. Three guards bound repeated rebalance requests:

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

### Static membership and restarts

Kafka's per-consumer `group.instance.id` is separate from the application-instance id shared by consumers in a JVM. An explicit `consumer-balancer.instance-id` remains optional. With the default automatic id, a restarted JVM generates a new application-instance id even if its static consumer identities stay the same.

Kafka may replay a static consumer's cached assignment without running the assignor. Each consumer checks its acknowledgement against its current application-instance id and topics. A mismatch requests a fresh assignment, including when the restarted consumer is not the monitor. Requests are coalesced per JVM and group and retried with backoff; they wait while the group is rebalancing. A static restart can therefore cause an additional group rebalance to refresh instance grouping. A matching acknowledgement ends recovery. Missing or corrupt metadata and an exceeded metadata budget do not create an automatic refresh loop.

Every explicit rebalance request advances a revision included in subscription metadata before asking the consumer to rejoin. This lets Kafka observe a changed subscription even when the monitoring consumer is not Kafka's group leader. Ordinary polling does not change the revision.

### Upgrading monitoring metadata

**Version 10.0.0 requires an interrupted, coordinated upgrade of each consumer group.** The Kafka assignor protocol name changes from `load-aware` to `load-aware-v2`; the Java assignor class and `assignor.load-aware.*` configuration keys stay the same. Old subscription and assignment formats are no longer supported. Configure `LoadAwarePartitionAssignor` as the group's only assignment strategy during this migration; an additional shared strategy would allow Kafka to select a different assignor instead.

1. Stop all old consumers in the group. For Kubernetes, use a deployment procedure that stops every old pod before starting the new consumers; a rolling deployment that waits for new consumers to become ready can stall.
2. Wait for Kafka to report that the group has no members. Static memberships remain until their session timeouts expire unless explicitly removed through Kafka's member-removal API. Stopping pods alone does not clear them immediately.
3. Start all consumers on version 10.0.0 with the same `group.id` and committed offsets. Do not delete the group or reset its offsets. Verify that the group becomes stable and a monitor is active.

Starting new consumers while old memberships remain can raise `InconsistentGroupProtocolException`. Kafka classifies this error as non-retriable, so affected consumers may need restarting after the old memberships are gone. The interruption includes any wait for static session expiry. Rolling back across this protocol boundary requires the same stop-and-start procedure.

`InstanceUserData`, `GroupInstanceUserData`, `MemberIdTracker.recordInstanceIds/getInstanceIds`, and `CoordinatorElection.Builder.setMemberIdsSupplier` have been removed. Use `ConsumerGroupBalancers` (also used by the starter) to wire ownership, subscription revisions and recovery together; manual core integrations need the shared tracker described below. Automatic application-instance IDs and ordinary static-member restart recovery remain supported within version 10.

## Several consumer groups

Out of the box, the starter balances the group of `spring.kafka.consumer.group-id`. An application running several independent consumer groups — each with its own topics, `ConsumerFactory` and listener containers, possibly created by the application itself rather than by `@KafkaListener` — registers every group with the auto-configured `ConsumerGroupBalancers` bean instead. `spring.kafka.consumer.group-id` does not have to be set; without it, no group is registered automatically.

Two things connect a group to the balancer: its consumer factory carries the balancer's assignor configs — `partition.assignment.strategy` included; to list further assignors, e.g. while migrating a group, put your own strategy after them — and the group is registered with what forces its rebalance when the proactive trigger fires:

```java
@Configuration
class ConsumerGroupsConfig {

    ConsumerGroupsConfig(ConsumerGroupBalancers balancers, List<GroupSpec> specs) {
        for (GroupSpec spec : specs) {
            Map<String, Object> props = new HashMap<>(spec.consumerProps());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, spec.groupId());
            props.putAll(balancers.assignorConfigs()); // the assignor, its weight store, balance service, member tracker
            var factory = new DefaultKafkaConsumerFactory<>(props);

            var manager = new MyContainerManager(factory, spec); // creates and replaces its containers on topic rescans
            balancers.register(spec.groupId(), ContainerRebalanceInitiator.of(spec.groupId(), manager::containers));
        }
    }
}
```

Everything else follows from the registration:

- **Lifecycle** — the group's coordinator election starts with the context (or at once, when registered later) and stops before Spring's listener containers do.
- **Settings** — every `consumer-balancer.*` property applies to registered groups too. Override them per group with `balancers.group("payments").rebalanceInitiator(initiator).imbalanceThreshold(1.3).register()`; `.trigger(...)` replaces the `ThresholdTrigger` altogether.
- **Metrics** — every group gets its own [meters](#metrics) tagged `group=<id>`, including groups registered at runtime.
- **Weights** — one weight store serves all groups; the offset-rate store keeps a history per partition, so groups never cost each other their measurements.

`ContainerRebalanceInitiator.of(groupId, supplier)` calls the supplier on every proactive rebalance, so it always reaches the containers that exist at that moment, replaced ones included. `ContainerRebalanceInitiator.of(groupId, registry)` does the same for a `ListenerContainerRegistry` such as the `KafkaListenerEndpointRegistry`. Any other `RebalanceInitiator` (a functional interface) works too, but only `ContainerRebalanceInitiator` reports the `rebalance.*` meters.

A group can be registered once per registry — a second registration of the same group id is rejected, since two coordinator elections for one group would double every rebalance.

Without Spring, build the registry yourself — see the [plain-Java example](#assignor-configuration-consumer-configs).

### Balancing combined load across groups

By default, every consumer group is balanced independently. To account for the sum of an instance's partition weights across groups, opt in:

```yaml
consumer-balancer:
  cross-group-balancing-enabled: true
```

For plain Java, enable it on the shared registry:

```java
ConsumerGroupBalancers balancers = ConsumerGroupBalancers.builder()
        .adminClient(admin)
        .weightService(weights)
        .crossGroupBalancing(true)
        .build();
```

Use the same enabled setting and register the same groups on every participating application instance **before starting consumers**, then give every consumer factory the registry's `assignorConfigs()`. Each participating instance must run at least one local consumer in every scoped, nonempty group to receive its topology; registering group names alone does not deliver those snapshots. Consumers sharing an application instance must share its instance id; the default per-JVM id already does this. The scope is all groups registered with that registry on one Kafka cluster. Separate registries and clusters do not contribute load to each other.

When placing a group's partitions or considering its proactive rebalance, the balancer includes the load already assigned to each instance by the other registered groups. A group can therefore receive an uneven allocation across instances when that improves their combined load. Weights must use comparable units across groups; consuming the same topic partition in two groups counts twice because both groups perform the work. Each decision moves only the current group's partitions.

Monitors are selected by application-instance id, then typed member identity, so the groups' monitors share one instance and registry. That registry serializes proactive corrections, reserving a decision until a fresh assignment acknowledges it. A reservation expires after 30 seconds if the decision has not completed, allowing recovery when it could not be executed. Concurrent natural rebalances and startup remain best effort: Kafka does not apply the groups' assignments as one atomic operation.

The existing threshold, hysteresis and cooldown still apply. If another group is rebalancing, or its topology or required weights are unavailable, the proactive check is skipped. Assignment falls back to ordinary per-group load-aware placement until that information is available, including during initial startup. This mode adds AdminClient reads for other groups and sends each group's full topology once per application instance; the aggregate assignment-metadata budget remains **512 KiB**. With `proactive-rebalance-enabled: false`, combined load still influences ordinary Kafka rebalances, but the library does not initiate automatic corrections.

## Multiple Kafka clusters

A `ConsumerGroupBalancers` registry belongs to exactly one cluster: its admin client, weight store and `MemberIdTracker` all do. The starter auto-configures the registry of the cluster `spring.kafka.*` points at; a second cluster needs a second registry, declared next to the second `ConsumerFactory` you already define for it (exactly as with plain Spring for Apache Kafka). Every `ConsumerGroupBalancers` bean is started, stopped and measured by the starter, and the auto-configured one stays in place next to yours:

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
        KafkaListenerEndpointRegistry registry) {
    ConsumerGroupBalancers balancers = ConsumerGroupBalancers.builder()
            .adminClient(admin)
            .weightService(weights)
            .tags(Map.of("cluster", "b")) // tells the clusters apart in the metrics
            .build();
    balancers.register("my-group", ContainerRebalanceInitiator.of("my-group", registry)
            .onlyListenerIds(List.of("ordersOnClusterB")));
    return balancers;
}
```

Put `clusterBBalancers.assignorConfigs()` into cluster B's consumer factory. Declare cluster B's admin client and weight store with `defaultCandidate = false`: a plain `WeightService` bean would [replace](#custom-weight-store) the auto-configured store of the first cluster. Once there are several registries, inject the auto-configured one with `@Qualifier(BalancerAutoConfiguration.BALANCERS_BEAN_NAME)`.

One thing does **not** follow from the group id: **which containers belong to which cluster.** Applications normally reuse the same group id on every cluster, so a rebalance initiator selecting containers by group id alone would rebalance every cluster whenever one of them is imbalanced. Scope each initiator by listener id, as above for cluster B, and the auto-registered group with:

```yaml
consumer-balancer:
  listener-ids: [orders, payments]   # the @KafkaListener ids that consume from the auto-configured cluster
```

Listener ids are the only stable, publicly readable identity a `MessageListenerContainer` carries besides its group id, which is why they are the hook. Give the listeners explicit ids (`@KafkaListener(id = "orders", ...)`) — the generated ones are positional and not stable across refactorings. For anything else, `ContainerRebalanceInitiator.filter(...)` accepts an arbitrary `Predicate<MessageListenerContainer>`.

Never share a registry, or its `MemberIdTracker`, between clusters: the tracker keys member ids and instance ids by group id, so clusters that reuse a group id would pool member ids from both and let one cluster's mapping overwrite the other's.

## Bean wiring

The auto-configured `ConsumerGroupBalancers` bean is built from the context's `WeightService`, `BalanceService` and `MemberIdTracker` beans plus the `consumer-balancer.*` properties. Its `assignorConfigs()` — `partition.assignment.strategy`, plus those collaborators and the instance id under the `assignor.load-aware.*` keys — are injected into Spring Boot's auto-configured consumer factory, so the consumer uses `LoadAwarePartitionAssignor` with exactly the same collaborators as the rebalance trigger. Values set explicitly under `spring.kafka.consumer.properties.*` win over the injected ones.

If you define your own `ConsumerFactory` bean, Boot's factory customizers do not run for it — put `balancers.assignorConfigs()` into its configs yourself (or apply the `BalancerConsumerFactoryCustomizer` bean to it), as in [Several consumer groups](#several-consumer-groups).

The group of `spring.kafka.consumer.group-id` is registered with a `ThresholdTrigger` and a `ContainerRebalanceInitiator` over the `KafkaListenerEndpointRegistry`. A single `RebalanceTrigger` or `RebalanceInitiator` bean in the context replaces the respective default for that group; the `WeightService`, `BalanceService` and `MemberIdTracker` beans are `@ConditionalOnMissingBean` as well.

## Metrics

With Micrometer on the classpath and a `MeterRegistry` bean in the context — which is what `spring-boot-starter-actuator` plus a registry backend gives you — the starter binds the balancer's meters automatically. There is nothing to configure and no new dependency: Micrometer is optional for this library (absent from its POM), and without it the metrics auto-configuration backs off entirely. `consumer-balancer.enabled=false` turns the meters off together with everything else; individual meters can be suppressed with ordinary Micrometer meter filters.

Every meter is prefixed `consumer.balancer.`. The meters of a consumer group are tagged with `group` — one set per group registered with a `ConsumerGroupBalancers` registry, including groups registered at runtime (Prometheus renders e.g. `consumer_balancer_trigger_evaluations_total{group="my-group",outcome="fired"}`). The `offset.rate.*` meters describe the weight store every group of a registry shares, so they carry no `group` tag. A registry's `tags(...)`, e.g. `cluster=b` for a [second cluster](#multiple-kafka-clusters), are added to all of its meters.

Every `ConsumerGroupBalancers` bean is measured automatically. A registry built outside the Spring context is bound with one call: `ConsumerBalancerMetrics.of(balancers).bindTo(meterRegistry)`.

| Meter | Type | Tags | Meaning |
|-------|------|------|---------|
| `trigger.imbalance.ratio` | gauge | | `(max instance load) / (optimal max instance load)` from the last evaluation that computed it — the value the threshold trigger judges. `NaN` until first computed. |
| `trigger.imbalance.threshold` | gauge | | The configured `rebalance-load-imbalance-threshold`, so dashboards can plot the ratio against its limit. |
| `trigger.instance.load` | gauge | `assignment` = `current` \| `optimal` | Load of the most loaded instance under the current assignment, and what it would carry under the optimal one. |
| `trigger.members` | gauge | | Group members at the last judged evaluation. |
| `trigger.instances` | gauge | | Application instances observed at the last judged evaluation. |
| `trigger.partitions` | gauge | | Assigned partitions at the last judged evaluation. |
| `trigger.weights.defaulted` | gauge | | Partitions whose weight fell back to the default at the last judged evaluation — a weight-store data-quality signal. |
| `trigger.violated.checks` | gauge | | Current [hysteresis](#proactive-rebalance) streak. |
| `trigger.cooldown` | gauge (seconds) | | Effective cooldown between fires, including backoff doubling. |
| `trigger.last.fired` | gauge (epoch seconds) | | When the trigger last fired on this instance; `NaN` until the first fire. `time() - x` is the age in PromQL. |
| `trigger.evaluations` | counter | `outcome` = `fired` \| `balanced` \| `awaiting_hysteresis` \| `cooldown_suppressed` \| `group_not_stable` \| `instances_unknown` \| `cross_group_load_unavailable` \| `weights_unknown` \| `no_members` \| `error` | Trigger evaluations by outcome. `cross_group_load_unavailable` means another group's correction is pending or its assignments, topology or weights cannot yet be used. `weights_unknown` means no partition had a usable weight, so the group was not judged — expected once after an instance becomes coordinator, a problem with the weight store when it keeps growing. `error` counts the failures the trigger otherwise only logs (broken admin client, weight store) — worth alerting on. A check cancelled because the coordinator role moved or the application stopped is not an evaluation and is not counted. |
| `trigger.evaluation.duration` | timer | | Wall time of evaluations (group describe, weight fetch, optimal-assignment computation). |
| `coordinator` | gauge | | `1` on the instance currently holding the coordinator role, `0` everywhere else. Exactly one instance per group should report `1`. |
| `rebalance.initiations` | counter | `result` = `enforced` \| `no_match` | Proactive rebalance initiations. `no_match` means no listener container matched the group and filter — the rebalance had no effect, check `listener-ids` or the container supplier; worth alerting on. |
| `rebalance.containers.enforced` | counter | | Listener containers that received `enforceRebalance()`, across all initiations. |
| `offset.rate.sample.errors` | counter | | Background end-offset samples in which at least one partition failed. While these persist, weights degrade toward the default and balancing quality silently drops. |
| `offset.rate.tracked.partitions` | gauge | | Partitions the offset-rate sampler currently tracks. |

The trigger only evaluates on the elected coordinator, so on every other instance the `trigger.*` meters keep their initial values (`NaN`/`0`) — and after losing the role an instance keeps its last observations. Aggregate across instances with `max by (group)`, or join on `consumer_balancer_coordinator == 1`. The trigger meters describe the default `ThresholdTrigger`, and the `rebalance.*` meters a `ContainerRebalanceInitiator`; a group using a custom trigger or initiator drops the respective meters, the rest keep working.

## Configuration reference (`consumer-balancer`)

| Property | Default | Description |
|----------|---------|-------------|
| `consumer-balancer.enabled` | `true` | Master switch for balancer auto-configuration. |
| `consumer-balancer.proactive-rebalance-enabled` | `true` | When `true`, one elected consumer per registered group runs the threshold trigger and may call `enforceRebalance()` on listener containers. When `false`, registered groups are balanced by the assignor only. |
| `consumer-balancer.cross-group-balancing-enabled` | `false` | Include other registered groups' load on each instance when assigning partitions and considering proactive rebalances. Applies within one registry and Kafka cluster; see [Balancing combined load across groups](#balancing-combined-load-across-groups). |
| `consumer-balancer.rebalance-load-imbalance-threshold` | `1.1` | Proactive rebalance when `(max instance load) / (optimal max instance load) > threshold` (see [Proactive rebalance](#proactive-rebalance)). |
| `consumer-balancer.instance-id` | *(auto)* | Application-instance id shared by every consumer in this JVM; members reporting the same id are balanced as one instance. Default: a random id generated once per JVM. |
| `consumer-balancer.rebalance-min-violated-checks` | `2` | Trigger checks that must see the imbalance on one unchanged assignment before a rebalance is fired; a balanced check decays the count by one. `1` fires on first sight. |
| `consumer-balancer.rebalance-cooldown` | `10m` | Minimum time between two proactive rebalances, regardless of whether the previous one changed anything. `0` disables the cooldown and its backoff. |
| `consumer-balancer.rebalance-max-cooldown` | `2h` | Ceiling for the cooldown after it has been doubled by rebalances that did not restore balance. Must not be shorter than `rebalance-cooldown`. |
| `consumer-balancer.listener-ids` | *(empty)* | Listener container ids the proactive rebalance of the `spring.kafka.consumer.group-id` group may touch; empty means every registered container of that group. Set it when several Kafka clusters share the group id — see [Multiple Kafka clusters](#multiple-kafka-clusters). Programmatically registered groups choose their containers through their own initiator. |
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
- `assignor.load-aware.member-id-tracker` — optional `MemberIdTracker` that receives local member identities and monitoring acknowledgements, plus the full topology on the designated monitor. Required for proactive monitoring and static-restart recovery; use the same tracker in the assignor, election, trigger and coordinator manager.

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

Plain-Java example with a custom weight source and proactive rebalance — a `ConsumerGroupBalancers` registry wires the election and the trigger to the same `MemberIdTracker` the assignor reports to:

```java
AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", "localhost:9092"));
ConsumerGroupBalancers balancers = ConsumerGroupBalancers.builder()
        .adminClient(admin)
        .weightService(new MyDatabaseWeightService(dataSource))
        .build();

Map<String, Object> configs = new HashMap<>();
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "my-group");
configs.putAll(balancers.assignorConfigs()); // the assignor, its weight service, balance service, member tracker

var consumer = new KafkaConsumer<>(configs, new StringDeserializer(), new ByteArrayDeserializer());
// Called on the coordinator's thread when the group should rebalance; KafkaConsumer is not
// thread-safe, so hand the request to the polling thread, which calls consumer.enforceRebalance().
balancers.register("my-group", () -> rebalanceRequested.set(true));
balancers.start();
// ... on shutdown: balancers.close(); admin.close();
```

`ConsumerGroupBalancers` is the recommended entry point for proactive rebalance. If you construct the core components manually, pass one shared `MemberIdTracker` instance in the consumer configs and to `ThresholdTrigger`, and call `CoordinatorElection.Builder.setMemberIdTracker(tracker)`. The election builder requires this tracker. `CoordinatorManager(election, trigger, initiator, checkIntervalMs)` automatically uses the election's tracker. This shared state connects monitor ownership to the delivered topology, updates subscription revisions, and enables recovery on nonmonitor JVMs.

> Kafka logs a "supplied but isn't a known config" warning for these custom keys — that is harmless.

## Custom weight store

Implement `io.github.ruskaof.balancer.weight.WeightService` and expose it as a Spring `@Bean`. Both built-in weight stores back off when a `WeightService` bean is present, and the bean drives **both** the `LoadAwarePartitionAssignor` and the proactive `ThresholdTrigger`. The same override mechanism applies to `BalanceService` (`computeOptimalAssignment(Collection<GroupMember>, Map<TopicPartition, Double>)` — each `GroupMember` carries its member id, instance id and subscribed topics).

To support cross-group balancing, a custom `BalanceService` must also implement `computeOptimalAssignment(Collection<GroupMember>, Map<TopicPartition, Double>, Map<String, Double>)`. The third argument is the other groups' baseline load by application-instance id, counted once per instance. The default overload rejects a nonempty baseline map so an implementation cannot silently ignore that load; the built-in `SortingRoundRobinBalanceService` supports it.

The returned map is treated as a lookup over the requested partitions: requested partitions that are missing (or mapped to `null`/non-finite values) fall back to the default weight `1.0`, and entries for partitions that were not requested are ignored.

Optionally provide your own `io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder` (or `TemplatedKafkaRatePromqlBuilder`) for custom PromQL while still using Prometheus.

## Operations

- With the default offset-rate store, the first assignment after an instance becomes group leader uses default weights (no offset history yet) and is effectively count-balanced; subsequent assignments and proactive-trigger checks use measured rates. Partitions whose end offset went backwards (e.g. a recreated topic) fall back to the default weight `1.0` until fresh history accrues. The assignment log (see [Reading the assignment log](#reading-the-assignment-log)) names this situation as the `all N partitions had no usable weight` factor.
- With the Prometheus store, your PromQL must be an **instant vector** query returning series with `topic` and `partition` labels so weights can be mapped to `TopicPartition`. If your metrics use different label names (e.g. `kafka_topic`), set `consumer-balancer.prometheus.topic-label` / `partition-label` accordingly — remember the `by (...)` clause of the query must keep those labels. Partitions without a sample — including `NaN`/`Inf` samples — get the default weight `1.0`.
- Partitions are assigned only to members subscribed to their topic, so groups whose members subscribe to different topic sets are handled correctly.
- With more members than partitions, partitions spread evenly across **instances** (some members inside each instance stay idle); with fewer instances than partitions than members, every instance carries a near-equal share of the measured traffic. Instances receive equal traffic regardless of their member counts — an instance running fewer threads gets the same load on fewer, busier members.
- The threshold trigger uses the assignor's topology for both instance grouping and actual topic subscriptions. An unavailable or stale topology pauses monitoring until a valid snapshot arrives. Check assignor logs for protocol or metadata-budget warnings if no monitor becomes active. See [Proactive rebalance](#proactive-rebalance) and [Static membership and restarts](#static-membership-and-restarts).
- The trigger only judges a **stable** group, so a check landing during a rebalance is skipped rather than acted on. Expect `ThresholdTrigger skipped ... group state is PREPARING_REBALANCE` at debug level around every rebalance.
- Proactive rebalance covers the group of `spring.kafka.consumer.group-id` and every group [registered](#several-consumer-groups) with a `ConsumerGroupBalancers` registry; without the property, only registered groups are balanced proactively, and an INFO line at startup says so. Only the listener containers of a group — narrowed by `consumer-balancer.listener-ids` or the initiator's own filter — receive `enforceRebalance()`. When no container matches, a warning is logged instead of silently doing nothing.
- If load-aware assignment throws, `LoadAwarePartitionAssignor` falls back to Kafka’s `RoundRobinAssignor`: the cause is logged as a warning, followed by a `Round-robin fallback distribution ...` INFO line showing how many partitions each instance received. On this path partition weights are ignored entirely — an instance running twice the members receives roughly twice the partitions.
- `LoadAwarePartitionAssignor` is a **client-side** assignor, so it applies only under the *classic* consumer group protocol (`group.protocol=classic`, the default on Kafka 4.x). If you opt into the new KIP-848 protocol (`group.protocol=consumer`), partitions are assigned broker-side and this assignor is bypassed — along with the member-id tracking that proactive rebalance relies on.

### Reading the assignment log

After every rebalance the **group leader** — the one consumer that runs the assignor, not necessarily the proactive-rebalance coordinator — logs the distribution it just computed, through the `io.github.ruskaof.balancer.LoadAwarePartitionAssignor` logger at INFO:

```
Load-aware assignment computed [instances=3, members=6, partitions=12, totalWeight=1050, defaultedWeights=3]:
  instance pod-a: load=500.0 (47.6% of total), partitions=1, members=2
  instance pod-b: load=300.0 (28.6% of total), partitions=6, members=2
  instance pod-c: load=250.0 (23.8% of total), partitions=5, members=2
  skew: max instance load 500.0 (pod-a) is +42.9% above the ideal even share 350.0
  unevenness factors:
  - partition orders-0 alone weighs 500.0, more than the ideal even share 350.0 — a perfectly even distribution is impossible
  - 3 of 12 partitions had no usable weight and got the default 1.0; their real load is invisible to this assignment
```

Each `instance` line is the traffic that instance is *expected* to carry under the measured weights, with its share of the total. `skew` compares the most loaded instance against the ideal even share (`totalWeight / instances`) — note this is **not** the `consumer.balancer.trigger.imbalance.ratio` metric, which compares the *current* assignment against the *optimal* one; the skew line describes the freshly computed assignment itself. The `unevenness factors` name what the balancer detected about why the distribution cannot, or deliberately does not, come out flatter:

- `all N partitions had no usable weight ...` — the weight store has no data yet (normal right after startup). Wait one `offset-rate.rate-interval` (or fix the Prometheus query); the proactive trigger then corrects the balance.
- `N of M partitions had no usable weight ...` — some partitions are invisible to the weight store. Check that your PromQL covers every subscribed topic, or that the offset-rate store has sampled long enough.
- `... defaulted partitions were placed as if nearly free` — measured weights dwarf the default `1.0`, so a busy-but-unmeasured partition may land anywhere, including on an already-loaded instance.
- `every partition weight is zero ...` — the store reports no traffic at all; partitions were spread by count.
- `partition X alone weighs ...` — one partition exceeds the ideal per-instance share, so **no** assignment can be even. Only more partitions (or a different partition key) can fix this; the balancer already minimized the damage.
- `partitions of K topic(s) could only go to a subset of instances ...` — subscription topology constrains placement. If those topics carry real load, run their listeners on more instances.
- `more instances (I) than partitions (P) ...` — some instances necessarily receive nothing.
- `instances run different member counts ...` — instances receive equal load regardless of member count (by design), so members of smaller instances run hotter.

For the full member-by-member table with every partition's weight, raise the same logger to DEBUG:

```yaml
logging:
  level:
    io.github.ruskaof.balancer.LoadAwarePartitionAssignor: DEBUG
```

```
Load-aware assignment detail [instances=3, members=6, partitions=12]:
  pod-a/consumer-a1-7f3e: load=500.0, partitions=[orders-0=500.0]
  pod-a/consumer-a2-9c1b: load=0.0, partitions=[]
  pod-b/consumer-b1-11aa: load=150.0, partitions=[orders-1=50.0, orders-4=50.0, payments-0=50.0]
```

**Still uneven on your dashboards even though the log looks balanced?** The log shows *expected* load at assignment time, under the configured weight metric — the gap is usually in the metric, not the balancing:

- The weight may not be proportional to real cost: the default offset-rate store measures **produced events/sec**, and events of different topics can cost very different CPU. A Prometheus query measuring what you actually care about (e.g. per-partition processing time) balances better.
- Weights are a trailing average measured *before* the rebalance; traffic that shifts afterwards is not reflected until the next trigger check fires a rebalance. Compare with `consumer.balancer.trigger.imbalance.ratio` for the live view.

## Build

This repository is built with **Gradle on the PATH** (not the wrapper), for example:

```bash
gradle test
```

The core unit suite includes a synthetic group of **500 static consumers across 100 application instances and 500 partitions**. It verifies metadata delivery and a proactive trigger decision without creating that many broker resources. A separate metadata test verifies approximately linear growth when member count doubles. These are functional checks, not a broker capacity or throughput benchmark.

## Performance test (Docker)

End-to-end run compares **RoundRobin** vs **load-aware** consumers with skewed synthetic load. It writes:

- `test-out/result-default.png` — throughput rebalance spikes when Micrometer exposes `kafka_consumer_*rebalance*` counters.


```bash
docker compose --env-file docker/test-env.properties -f docker/docker-compose.yaml up --abort-on-container-exit
```

## License

See [LICENSE](LICENSE).
