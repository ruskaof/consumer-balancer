# Monitoring and troubleshooting

[Assignment logs](#reading-the-assignment-log) · [Monitor ownership](#monitor-ownership) · [Static membership](#static-membership-and-restarts) · [Micrometer metrics](#metrics)

## Reading the assignment log

After a rebalance, the Kafka group leader logs the assignment at INFO under `io.github.ruskaof.balancer.LoadAwarePartitionAssignor`. The leader and the proactive monitor may be different consumers. This synthetic log excerpt illustrates three partitions with weights of 90, 20 and 10.

```text
Load-aware assignment computed [instances=2, members=2, partitions=3, totalWeight=120.0, defaultedWeights=0]:
  instance pod-a: load=90.0 (75.0% of total), partitions=1, members=1
  instance pod-b: load=30.0 (25.0% of total), partitions=2, members=1
  skew: max instance load 90.0 (pod-a) is +50.0% above the ideal even share 60.0
```

The `skew` line compares the busiest instance with the arithmetic mean (`120 / 2 = 60`). One partition alone weighs 90, so any placement has a peak of at least 90. The trigger compares the current peak with the peak under its proposed placement, giving a ratio of `90 / 90 = 1.0` for this distribution.

The `unevenness factors` lines explain what constrained the assignment.

| Observation | What to check |
| --- | --- |
| All or some weights defaulted | A fresh offset-rate store needs two usable observations per partition. For Prometheus, check that the query returns a finite value for each topic and partition. The fallback weight of `1.0` can make an unmeasured partition look cheap. |
| All weights are zero | The store reports no traffic. The assignor spreads these partitions by count. |
| One partition outweighs the average | That partition sets a lower bound on the peak. Check the partition key if most traffic lands on one key. |
| A topic can run on only a subset of instances | Check where listeners for that topic are deployed. |
| Some consumers are idle | Each partition can have only one consumer in a group. The assignor balances across eligible instances before distributing work among their consumers. |
| Loads look even in the log but CPU differs on the dashboard | The log uses weights from assignment time. Check whether traffic has shifted or some records are more expensive to process. A [custom weight](configuration.md#weight-stores) can represent processing cost. |
| `Round-robin fallback distribution` | Load-aware assignment failed. The preceding warning contains the cause. Check it for missing or unreadable subscription metadata. |

To see each member's partitions and individual weights, enable DEBUG on the assignor.

```yaml
logging:
  level:
    io.github.ruskaof.balancer.LoadAwarePartitionAssignor: DEBUG
```

## Monitor ownership

The leader sends the group's instance IDs and subscriptions to one monitoring consumer. Each other member receives an acknowledgement with the monitor's identity and its own expected instance ID, topics and request revision. The monitor checks that snapshot against current Kafka membership before evaluating the assignment. Even a member with no assigned partitions can monitor.

Among members advertising monitoring support, the leader chooses the smallest typed identity (`s:<group.instance.id>` for static members and `m:<memberId>` for dynamic members). With cross-group balancing enabled, the leader orders candidates by application-instance ID first, then member identity, and sends one full snapshot per participating instance.

The library limits the combined size of snapshots and acknowledgements to 512 KiB per assignment. If that budget is exceeded, partition assignment continues and the leader logs why monitoring is unavailable. Kafka's own metadata limits also apply.

### Static membership and restarts

Kafka's `group.instance.id` identifies one static **consumer**. The library's `consumer-balancer.instance-id` groups all consumers on an **application instance**. By default the latter changes when the JVM restarts.

Kafka can replay a static member's cached assignment. If its acknowledged instance ID or topics no longer match, the consumer requests a fresh assignment. This can cause one additional group rebalance after a restart. Requests from the same JVM and group are coalesced, wait for a stable group and retry with backoff. A matching acknowledgement ends recovery. See the [10.x upgrade procedure](../README.md#upgrading-monitoring-metadata) when crossing the protocol boundary.

## Metrics

With Micrometer on the classpath and a `MeterRegistry` bean, the starter registers meters for every `ConsumerGroupBalancers` bean. The names below all start with `consumer.balancer.`. Proactively monitored groups have a `group` tag. The shared offset-rate store has one set of meters per registry. Registry tags such as `cluster=b` apply to both.

| Meter | Type | What it reports |
| --- | --- | --- |
| `coordinator` | Gauge | `1` on the instance monitoring this group and `0` elsewhere. |
| `trigger.imbalance.ratio` | Gauge | Peak instance load under the current assignment divided by the peak under the proposed assignment. `NaN` until a ratio is available. |
| `trigger.imbalance.threshold` | Gauge | Configured ratio threshold. |
| `trigger.instance.load` | Gauge | Peak instance load by `assignment` tag (`current` or `optimal`). |
| `trigger.members`, `trigger.instances`, `trigger.partitions` | Gauges | Group size at the last evaluation that measured it. |
| `trigger.weights.defaulted` | Gauge | Partitions that fell back to weight `1.0`. |
| `trigger.violated.checks` | Gauge | Accumulated imbalance checks for the assignment. |
| `trigger.cooldown` | Time gauge | Effective cooldown, including backoff. |
| `trigger.last.fired` | Gauge | Epoch seconds of the last trigger fire on this instance. `NaN` before the first fire. |
| `trigger.evaluations` | Counter | Evaluations by `outcome` (see below). |
| `trigger.evaluation.duration` | Timer | Time spent describing the group, fetching weights and evaluating placement. |
| `rebalance.initiations` | Counter | Requests by `result`. `enforced` means a container matched. For `no_match`, check listener IDs and the container supplier. |
| `rebalance.containers.enforced` | Counter | Total listener containers that received `enforceRebalance()`. |
| `offset.rate.sample.errors` | Counter | Background samples in which at least one partition failed. |
| `offset.rate.tracked.partitions` | Gauge | Partitions tracked by the offset sampler. |

Read trigger gauges from the instance where `consumer_balancer_coordinator == 1`. An instance that loses the role retains its last observations. `trigger.*` meters come from `ThresholdTrigger`, and `rebalance.*` meters from `ContainerRebalanceInitiator`. For a registry outside the Spring context, call `ConsumerBalancerMetrics.of(balancers).bindTo(meterRegistry)`.

### Trigger outcomes

For example, Prometheus exposes the fire counter as `consumer_balancer_trigger_evaluations_total{group="my-group",outcome="fired"}`.

| Outcome | Meaning |
| --- | --- |
| `fired` | The trigger requested a rebalance. |
| `balanced` | The ratio was within threshold or every weight was zero. |
| `awaiting_hysteresis` | The ratio exceeded the threshold. More checks are needed to confirm the imbalance. |
| `cooldown_suppressed` | Imbalance was confirmed during the cooldown. |
| `group_not_stable` | Kafka reported a transition or an unknown group state. |
| `instances_unknown` | The membership snapshot was unavailable or stale. |
| `cross_group_load_unavailable` | Another group's correction is pending, or its assignments, topology or weights are unavailable. |
| `weights_unknown` | No partition had a usable weight. This can happen while a new monitor collects samples. If it persists, check the weight store. |
| `no_members` | The group was empty or no member had an assignment. |
| `error` | Evaluation failed. Check the logged exception. |
