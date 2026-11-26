# consumer-balancer

A Kafka partition assignor for consumer groups where some partitions receive much more traffic than others. It uses each partition's incoming record rate to spread work across application instances, then across the consumers inside each instance. The Spring Boot starter can also request a rebalance when traffic shifts.

## Table of contents

- [Quick start](#quick-start) ([Spring Boot](#spring-boot) · [Plain Java](#plain-java))
- [How partitions are assigned](#instance-aware-balancing)
- [When the group rebalances](#proactive-rebalance)
- [Upgrading a group to 10.x](#upgrading-monitoring-metadata)
- [Configuration and troubleshooting](#configuration-and-troubleshooting)
- [Build and performance tests](#build-and-performance-tests)

## Quick start

Use Java 21+ and Kafka's classic consumer group protocol (`group.protocol=classic`). With `group.protocol=consumer`, the broker assigns partitions and bypasses client-side assignors. This project builds against Kafka clients 4.1.2 and, for the starter, Spring Boot 4.0.7 with Spring Kafka 4.0.6.

### Spring Boot

Add the starter to your Gradle dependencies. It includes the core library. [Maven Central](https://central.sonatype.com/artifact/io.github.ruskaof/consumer-balancer-spring-boot-starter)

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-spring-boot-starter:10.1.0")
}
```

Point your listener at Kafka and set its consumer group in `application.yml`. Use the same group ID on each replica.

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: my-group
```

The starter installs `LoadAwarePartitionAssignor` on Boot's consumer factory and registers `my-group` for proactive rebalancing. An explicit `partition.assignment.strategy` on the factory takes precedence. For a listener using this configuration, see the [example application](test-listener/src/main/java/io/github/ruskaof/test/listener/TestListenerApplication.java).

On the first rebalance, look for `Load-aware assignment computed` in the group leader's INFO log. With the default weight store, a fresh instance uses weight `1.0` for each partition until it has two usable end-offset samples. Later assignments use the measured rates.

### Plain Java

Add the core library and set the assignment strategy in your Kafka consumer properties.

```kotlin
dependencies {
    implementation("io.github.ruskaof:consumer-balancer-core:10.1.0")
}
```

```properties
partition.assignment.strategy=io.github.ruskaof.balancer.LoadAwarePartitionAssignor
```

This enables load-aware placement on Kafka rebalances. To have the library initiate rebalances as well, wire a [plain Java registry](docs/configuration.md#proactive-rebalance-in-plain-java).

## Instance-aware balancing

In a synthetic example, four partitions receive 80, 40, 30 and 10 records/s. With two pods subscribed to the topic, the assignor puts the 80-record/s partition on one pod and the other three on the second. Each pod receives 80 records/s, even though they own different numbers of partitions.

The group leader places the heaviest partition first. For each partition it chooses the least-loaded instance with a subscriber to that topic, then the least-loaded subscriber on that instance. Zero-weight partitions are distributed by count. A single hot partition stays on one consumer, which limits how evenly the group can share the work.

Consumers in the same JVM share a generated instance ID. Set `consumer-balancer.instance-id` to a unique value per application instance, such as a pod name, if you want to recognize it in the logs. Groups balance independently by default. Applications running several groups on the same pods can enable [cross-group balancing](docs/configuration.md#balancing-combined-load-across-groups).

The default weight store estimates incoming records/s from Kafka end-offset growth over a one-minute window. If records vary in processing cost, a Prometheus query or a custom `WeightService` can supply a weight such as processing time per partition. See [weight stores](docs/configuration.md#weight-stores).

## Proactive rebalance

Traffic can move to a different partition while every consumer stays in the group. By default, one consumer checks the group every 30 seconds. It compares the busiest instance's current load with the highest instance load under the assignment the balancer would choose using the same weights. For example, if a single hot partition sets the peak in both assignments, moving it would gain nothing.

The default trigger requests a rebalance when the ratio exceeds `1.1` on two checks of the same assignment. A balanced check reduces the violation count by one. Checks pause while the group is rebalancing, its membership snapshot is stale, or all weights are unavailable. A proactive rebalance pauses consumption across the group. The default cooldown is 10 minutes and backs off to at most 2 hours after repeated ineffective rebalances.

To apply weights only when Kafka initiates a rebalance, set `consumer-balancer.proactive-rebalance-enabled=false`. For other policies, see the [threshold, periodic and composite triggers](docs/triggers/README.md).

## Upgrading monitoring metadata

**Upgrading from a pre-10 release to 10.x requires downtime for each consumer group.** Version 10 changed the Kafka assignor protocol name from `load-aware` to `load-aware-v2`. Configure `LoadAwarePartitionAssignor` as the group's only strategy during the migration.

1. Stop all old consumers in the group before starting any new ones.
2. Wait until Kafka reports zero members. For static consumers, wait for session expiry or remove the old memberships through Kafka's member-removal API.
3. Start the 10.x consumers with the same group ID and committed offsets. Verify that the group becomes stable and, if proactive rebalancing is enabled, that a monitor is active.

A consumer started too early can fail with `InconsistentGroupProtocolException` and may need a restart once the old memberships clear. Use the same stop-and-start procedure to roll back across the protocol change. Manual core integrations also need the [10.x tracker wiring](docs/configuration.md#proactive-rebalance-in-plain-java).

## Configuration and troubleshooting

| I want to… | Where to look |
| --- | --- |
| Find a property or change a default | [Spring Boot properties](docs/configuration.md#spring-boot-properties) · [Kafka consumer properties](docs/configuration.md#kafka-consumer-properties) |
| Use Prometheus or an application metric for weights | [Weight stores and query example](docs/configuration.md#weight-stores) |
| Register more listeners, groups or clusters | [Several consumer groups](docs/configuration.md#several-consumer-groups) · [Multiple Kafka clusters](docs/configuration.md#multiple-kafka-clusters) |
| Understand an uneven assignment | [Reading the assignment log](docs/monitoring.md#reading-the-assignment-log) |
| Find out why a rebalance fired or was skipped | [Trigger outcomes and Micrometer metrics](docs/monitoring.md#metrics) |

## Build and performance tests

Run the unit tests from the repository root with JDK 21 and Gradle (`8.14.4` in CI).

```bash
gradle test
```

The Docker test runs round-robin, cooperative-sticky and load-aware consumer groups against the same synthetic workload. It shifts traffic between partitions and writes a per-instance throughput chart to `test-out/result-default.png`. See the [benchmark and sample chart](docs/perftest/README.md).

```bash
docker compose --env-file docker/test-env.properties -f docker/docker-compose.yaml up --abort-on-container-exit
```

[Contributing](CONTRIBUTING.md) · [Apache License 2.0](LICENSE)
