package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.instance.InstanceIdResolver;
import io.github.ruskaof.balancer.instance.InstanceUserData;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the assignor delegates to {@link BalanceService} with a sanitized weight
 * map covering exactly the partitions being assigned, and with each member's subscribed
 * topics.
 */
class LoadAwarePartitionAssignorTest {

    @Test
    void assignMatchesGreedyBalanceWhenWeightsAreProvided() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        WeightService weights = partitions -> {
            Map<TopicPartition, Double> m = new HashMap<>();
            for (TopicPartition tp : partitions) {
                m.put(tp, tp.partition() == 0 ? 50.0 : 1.0);
            }
            return m;
        };
        BalanceService balance = new SortingRoundRobinBalanceService();

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, weights,
                LoadAwareAssignorConfig.BALANCE_SERVICE, balance));

        String topic = "t";
        Map<String, Integer> partitionsPerTopic = Map.of(topic, 3);
        Map<String, Subscription> subscriptions = subscriptions(Map.of(
                "a", List.of(topic),
                "b", List.of(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        Map<TopicPartition, Double> w = weights.computeWeights(Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)));
        Map<String, List<TopicPartition>> expected = balance.computeOptimalAssignment(
                List.of(
                        new GroupMember("a", "a", Set.of(topic)),
                        new GroupMember("b", "b", Set.of(topic))),
                w);

        assertEquals(expected, assignment);
    }

    @Test
    void assignsPartitionsOnlyToSubscribedMembers() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        AtomicReference<Collection<GroupMember>> capturedMembers = new AtomicReference<>();
        BalanceService capturingBalance = (members, weights) -> {
            capturedMembers.set(members);
            return new SortingRoundRobinBalanceService().computeOptimalAssignment(members, weights);
        };

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.BALANCE_SERVICE, capturingBalance));

        Map<String, Integer> partitionsPerTopic = Map.of("t1", 1, "t2", 2);
        Map<String, Subscription> subscriptions = subscriptions(Map.of(
                "a", List.of("t1"),
                "b", List.of("t1", "t2")));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(
                Map.of("a", Set.of("t1"), "b", Set.of("t1", "t2")),
                capturedMembers.get().stream().collect(
                        Collectors.toMap(GroupMember::memberId, GroupMember::subscribedTopics)),
                "load-aware path must run and receive each member's subscribed topics");
        assertTrue(assignment.get("a").stream().allMatch(tp -> tp.topic().equals("t1")),
                "member 'a' did not subscribe to t2 but was assigned: " + assignment.get("a"));
        Set<TopicPartition> allAssigned = new HashSet<>();
        assignment.values().forEach(allAssigned::addAll);
        assertEquals(Set.of(
                new TopicPartition("t1", 0),
                new TopicPartition("t2", 0),
                new TopicPartition("t2", 1)), allAssigned);
    }

    @Test
    void subscriptionUserDataCarriesConfiguredInstanceId() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.INSTANCE_ID, "pod-1"));

        InstanceUserData.Decoded decoded =
                InstanceUserData.decode(assignor.subscriptionUserData(Set.of("t")));

        assertTrue(decoded.ok());
        assertEquals("pod-1", decoded.instanceId());
    }

    @Test
    void subscriptionUserDataFallsBackToAutoInstanceId() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of()));

        InstanceUserData.Decoded decoded =
                InstanceUserData.decode(assignor.subscriptionUserData(Set.of("t")));

        assertTrue(decoded.ok());
        assertEquals(InstanceIdResolver.autoInstanceId(), decoded.instanceId());
    }

    @Test
    void subscriptionAdvertisesMonitoringOnlyWhenATrackerIsConfigured() {
        LoadAwarePartitionAssignor plain = configuredAssignor();
        assertFalse(MonitoringProtocol.readSubscription(plain.subscriptionUserData(Set.of("t"))).monitoring());

        MemberIdTracker tracker = new MemberIdTracker();
        LoadAwarePartitionAssignor monitored = new LoadAwarePartitionAssignor();
        monitored.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.MEMBER_ID_TRACKER, tracker,
                "group.id", "g"));
        var initial = MonitoringProtocol.readSubscription(monitored.subscriptionUserData(Set.of("t")));
        assertTrue(initial.monitoring());
        assertEquals(InstanceIdResolver.autoInstanceId(), initial.instanceId());
        assertEquals(initial, MonitoringProtocol.readSubscription(monitored.subscriptionUserData(Set.of("t"))),
                "ordinary subscription reads must not change the request revision");

        tracker.prepareRebalance("g");
        var requested = MonitoringProtocol.readSubscription(monitored.subscriptionUserData(Set.of("t")));
        assertTrue(requested.revision() > initial.revision(), "an explicit rejoin must change subscription metadata");
        assertEquals(requested, MonitoringProtocol.readSubscription(monitored.subscriptionUserData(Set.of("t"))));
    }

    @Test
    void assignGroupsMembersByReportedInstanceId() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        AtomicReference<Collection<GroupMember>> capturedMembers = new AtomicReference<>();
        BalanceService capturingBalance = (members, weights) -> {
            capturedMembers.set(members);
            return new SortingRoundRobinBalanceService().computeOptimalAssignment(members, weights);
        };
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.BALANCE_SERVICE, capturingBalance));

        Map<String, Subscription> subscriptions = new TreeMap<>();
        subscriptions.put("a1", new Subscription(List.of("t"), InstanceUserData.encode("pod-a")));
        subscriptions.put("a2", new Subscription(List.of("t"), InstanceUserData.encode("pod-a")));
        subscriptions.put("b1", new Subscription(List.of("t"), InstanceUserData.encode("pod-b")));

        assignor.assign(Map.of("t", 2), subscriptions);

        assertEquals(
                Map.of("a1", "pod-a", "a2", "pod-a", "b1", "pod-b"),
                capturedMembers.get().stream().collect(
                        Collectors.toMap(GroupMember::memberId, GroupMember::instanceId)));
    }

    @Test
    void assignTreatsMembersWithoutReadableInstanceIdAsTheirOwnInstances() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        AtomicReference<Collection<GroupMember>> capturedMembers = new AtomicReference<>();
        BalanceService capturingBalance = (members, weights) -> {
            capturedMembers.set(members);
            return new SortingRoundRobinBalanceService().computeOptimalAssignment(members, weights);
        };
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.BALANCE_SERVICE, capturingBalance));

        Map<String, Subscription> subscriptions = new TreeMap<>();
        subscriptions.put("ok", new Subscription(List.of("t"), InstanceUserData.encode("pod-a")));
        subscriptions.put("nullData", new Subscription(List.of("t"), null));
        subscriptions.put("emptyData", new Subscription(List.of("t"), ByteBuffer.allocate(0)));
        subscriptions.put("garbage", new Subscription(List.of("t"), ByteBuffer.wrap(new byte[]{7})));

        Map<String, List<TopicPartition>> assignment = assignor.assign(Map.of("t", 4), subscriptions);

        assertEquals(
                Map.of("ok", "pod-a", "nullData", "nullData", "emptyData", "emptyData", "garbage", "garbage"),
                capturedMembers.get().stream().collect(
                        Collectors.toMap(GroupMember::memberId, GroupMember::instanceId)),
                "members without a readable instance id must fall back to their member id");
        assertEquals(4, assignment.values().stream().mapToInt(List::size).sum(),
                "every partition must still be assigned");
    }

    @Test
    void backfillsDefaultWeightsWhenWeightServiceReturnsSubset() {
        AtomicReference<Map<TopicPartition, Double>> capturedWeights = new AtomicReference<>();
        LoadAwarePartitionAssignor assignor = configureCapturing(
                partitions -> Map.of(), capturedWeights);

        Map<String, List<TopicPartition>> assignment = assignor.assign(
                Map.of("t", 3), subscriptions(Map.of("a", List.of("t"))));

        assertEquals(
                Map.of(
                        new TopicPartition("t", 0), 1.0,
                        new TopicPartition("t", 1), 1.0,
                        new TopicPartition("t", 2), 1.0),
                capturedWeights.get(),
                "missing weights must be backfilled with the default");
        assertEquals(3, assignment.get("a").size(), "every partition must be assigned");
    }

    @Test
    void dropsWeightEntriesForPartitionsNotBeingAssigned() {
        AtomicReference<Map<TopicPartition, Double>> capturedWeights = new AtomicReference<>();
        LoadAwarePartitionAssignor assignor = configureCapturing(
                partitions -> Map.of(new TopicPartition("t", 99), 100.0), capturedWeights);

        Map<String, List<TopicPartition>> assignment = assignor.assign(
                Map.of("t", 2), subscriptions(Map.of("a", List.of("t"))));

        assertEquals(
                Set.of(new TopicPartition("t", 0), new TopicPartition("t", 1)),
                capturedWeights.get().keySet(),
                "stale weight entries must not enter the assignment");
        assertFalse(assignment.get("a").contains(new TopicPartition("t", 99)));
    }

    @Test
    void sanitizesNonFiniteWeightsBeforeBalancing() {
        AtomicReference<Map<TopicPartition, Double>> capturedWeights = new AtomicReference<>();
        LoadAwarePartitionAssignor assignor = configureCapturing(
                partitions -> Map.of(
                        new TopicPartition("t", 0), Double.NaN,
                        new TopicPartition("t", 1), Double.POSITIVE_INFINITY,
                        new TopicPartition("t", 2), 3.0),
                capturedWeights);

        assignor.assign(Map.of("t", 3), subscriptions(Map.of("a", List.of("t"))));

        assertEquals(
                Map.of(
                        new TopicPartition("t", 0), 1.0,
                        new TopicPartition("t", 1), 1.0,
                        new TopicPartition("t", 2), 3.0),
                capturedWeights.get(),
                "non-finite weights must fall back to the default");
    }

    @Test
    void fallsBackToRoundRobinWhenWeightServiceThrows() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> {
                    throw new IllegalStateException("weight backend unreachable");
                }));

        Map<String, Integer> partitionsPerTopic = Map.of("t", 3);
        Map<String, Subscription> subscriptions = subscriptions(Map.of(
                "a", List.of("t"),
                "b", List.of("t")));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(
                new RoundRobinAssignor().assign(partitionsPerTopic, subscriptions),
                assignment,
                "a failing weight service must degrade to plain round-robin, not fail the rebalance");
    }

    @Test
    void fallsBackToRoundRobinWhenBalanceServiceThrows() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.BALANCE_SERVICE, (BalanceService) (members, weights) -> {
                    throw new IllegalStateException("balance service broke");
                }));

        Map<String, Integer> partitionsPerTopic = Map.of("t", 4);
        Map<String, Subscription> subscriptions = subscriptions(Map.of(
                "a", List.of("t"),
                "b", List.of("t")));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(
                new RoundRobinAssignor().assign(partitionsPerTopic, subscriptions),
                assignment,
                "a failing balance service must degrade to plain round-robin, not fail the rebalance");
    }

    @Test
    void configureBuildsOffsetRateDefaultsWhenNoWeightServiceConfigured() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        assignor.configure(Map.of("bootstrap.servers", "127.0.0.1:9092"));

        assertInstanceOf(SortingRoundRobinBalanceService.class, getField(assignor, "balanceService"));
        KafkaOffsetRateWeightService weightService =
                assertInstanceOf(KafkaOffsetRateWeightService.class, getField(assignor, "weightService"));
        try (weightService) {
            assertEquals(KafkaOffsetRateWeightService.DEFAULT_RATE_INTERVAL, weightService.getRateInterval());
        }
    }

    @Test
    void configurePassesIntervalsToOffsetRateDefaults() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        assignor.configure(Map.of(
                "bootstrap.servers", "127.0.0.1:9092",
                LoadAwareAssignorConfig.OFFSET_RATE_RATE_INTERVAL_MS, "120000",
                LoadAwareAssignorConfig.OFFSET_RATE_SAMPLE_INTERVAL_MS, "5000"));

        KafkaOffsetRateWeightService weightService =
                assertInstanceOf(KafkaOffsetRateWeightService.class, getField(assignor, "weightService"));
        try (weightService) {
            assertEquals(Duration.ofMinutes(2), weightService.getRateInterval());
            assertEquals(Duration.ofSeconds(5), weightService.getSampleInterval());
        }
    }

    @Test
    void configureFailsWithoutWeightServiceOrBootstrapServers() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> assignor.configure(Map.of()));

        assertTrue(e.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void configureFailsOnUnknownWeightStore() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> assignor.configure(Map.of(LoadAwareAssignorConfig.WEIGHT_STORE, "graphite")));

        assertTrue(e.getMessage().contains("graphite"));
        assertTrue(e.getMessage().contains(LoadAwareAssignorConfig.WEIGHT_STORE_OFFSET_RATE));
    }

    @Test
    void configureBuildsPrometheusDefaultsWhenPrometheusStoreSelected() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_STORE, LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS,
                LoadAwareAssignorConfig.PROMETHEUS_HOST, "localhost",
                LoadAwareAssignorConfig.PROMETHEUS_PORT, "9090",
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "sum(rate(kafka_messages_total{topic=~\"%s\"}[1m])) by (topic, partition)"));

        assertInstanceOf(PrometheusWeightService.class, getField(assignor, "weightService"));
        assertInstanceOf(SortingRoundRobinBalanceService.class, getField(assignor, "balanceService"));

        PrometheusWeightService weightService = (PrometheusWeightService) getField(assignor, "weightService");
        assertEquals("topic", weightService.getTopicLabel());
        assertEquals("partition", weightService.getPartitionLabel());
    }

    @Test
    void configurePassesCustomTopicAndPartitionLabelsToPrometheusDefaults() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_STORE, LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS,
                LoadAwareAssignorConfig.PROMETHEUS_HOST, "localhost",
                LoadAwareAssignorConfig.PROMETHEUS_PORT, "9090",
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "sum(rate(kafka_messages_total{kafka_topic=~\"%s\"}[1m])) by (kafka_topic, kafka_partition)",
                LoadAwareAssignorConfig.PROMETHEUS_TOPIC_LABEL, "kafka_topic",
                LoadAwareAssignorConfig.PROMETHEUS_PARTITION_LABEL, "kafka_partition"));

        PrometheusWeightService weightService = (PrometheusWeightService) getField(assignor, "weightService");
        assertEquals("kafka_topic", weightService.getTopicLabel());
        assertEquals("kafka_partition", weightService.getPartitionLabel());
    }

    @Test
    void configureFailsWithoutPrometheusHostWhenPrometheusStoreSelected() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> assignor.configure(Map.of(
                        LoadAwareAssignorConfig.WEIGHT_STORE, LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS)));

        assertTrue(e.getMessage().contains(LoadAwareAssignorConfig.PROMETHEUS_HOST));
    }

    @Test
    void configureFailsWithClearMessageOnInvalidTimeout() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> assignor.configure(Map.of(
                        LoadAwareAssignorConfig.WEIGHT_STORE, LoadAwareAssignorConfig.WEIGHT_STORE_PROMETHEUS,
                        LoadAwareAssignorConfig.PROMETHEUS_HOST, "localhost",
                        LoadAwareAssignorConfig.PROMETHEUS_PORT, "9090",
                        LoadAwareAssignorConfig.PROMETHEUS_CONNECT_TIMEOUT_MS, "abc",
                        LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE, "x{topic=~\"%s\"}")));

        assertTrue(e.getMessage().contains(LoadAwareAssignorConfig.PROMETHEUS_CONNECT_TIMEOUT_MS));
    }

    @Test
    void onAssignmentReportsMemberIdToConfiguredTracker() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        MemberIdTracker tracker = new MemberIdTracker();
        WeightService weights = partitions -> Map.of();

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, weights,
                LoadAwareAssignorConfig.MEMBER_ID_TRACKER, tracker));

        Assignment assignment = new Assignment(List.of());
        assignor.onAssignment(assignment, new ConsumerGroupMetadata("g", 1, "m-1", Optional.empty()));
        assertEquals(Set.of("m-1"), tracker.getCurrentMemberIds("g"));

        // A changed member id replaces the previously reported one.
        assignor.onAssignment(assignment, new ConsumerGroupMetadata("g", 2, "m-2", Optional.empty()));
        assertEquals(Set.of("m-2"), tracker.getCurrentMemberIds("g"));
    }

    @Test
    void assignSendsTheTopologyOnlyToTheMonitorAndAcknowledgesEveryMember() {
        LoadAwarePartitionAssignor assignor = configuredAssignor();

        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(instanceSubscriptions()));

        Map<String, MonitoringProtocol.Member> expected = Map.of(
                "m:a1", new MonitoringProtocol.Member("pod-a", Set.of("t")),
                "m:a2", new MonitoringProtocol.Member("pod-a", Set.of("t")),
                "m:b1", new MonitoringProtocol.Member("pod-b", Set.of("t")));
        assertEquals(Set.of("a1", "a2", "b1"), assignment.groupAssignment().keySet());
        var owner = MonitoringProtocol.readAssignment(assignment.groupAssignment().get("a1").userData());
        assertNotNull(owner);
        assignment.groupAssignment().forEach((memberId, memberAssignment) -> {
            var decoded = MonitoringProtocol.readAssignment(memberAssignment.userData());
            assertNotNull(decoded, memberId + " received no readable topology acknowledgement");
            assertEquals("m:a1", decoded.ownerIdentity());
            assertEquals(owner.snapshotId(), decoded.snapshotId());
            assertEquals(expected.get("m:" + memberId).instanceId(), decoded.instanceId());
            assertEquals(Set.of("t"), decoded.topics());
            assertEquals(memberId.equals("a1") ? expected : Map.of(), decoded.members());
        });
    }

    @Test
    void everyMemberGetsItsOwnViewOfTheEncodedMapping() {
        LoadAwarePartitionAssignor assignor = configuredAssignor();

        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(instanceSubscriptions()));

        // The protocol serializer consumes the buffer it is handed, so draining one member's
        // copy must leave the others untouched.
        ByteBuffer drained = assignment.groupAssignment().get("a1").userData();
        drained.get(new byte[drained.remaining()]);

        assertEquals(0, drained.remaining(), "the drained buffer is the one that was read");
        for (String memberId : List.of("a2", "b1")) {
            ByteBuffer other = assignment.groupAssignment().get(memberId).userData();
            assertEquals(0, other.position(), memberId + " shares its buffer position with another member");
            assertNotNull(MonitoringProtocol.readAssignment(other), memberId + " lost its metadata");
        }
    }

    @Test
    void assignSendsTheInstanceIdsEvenWhenItFallsBackToRoundRobin() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> {
                    throw new IllegalStateException("weight backend unreachable");
                }));

        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(instanceSubscriptions()));

        // The mapping comes from the subscriptions, not from the assignment, so a degraded
        // assignment must not also blind the proactive trigger.
        assertEquals(Set.of("m:a1", "m:a2", "m:b1"), MonitoringProtocol.readAssignment(
                assignment.groupAssignment().get("a1").userData()).members().keySet());
    }

    @Test
    void assignSendsNoInstanceIdsWhenTheGroupIsTooLargeForTheBudget() {
        LoadAwarePartitionAssignor assignor = configuredAssignor();
        Map<String, Subscription> subscriptions = new TreeMap<>();
        // Long member ids reach the budget at a member count the test can still build.
        String padding = "x".repeat(2000);
        for (int i = 0; i < 300; i++) {
            subscriptions.put("member-" + i + "-" + padding,
                    new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-" + i, true, 0)));
        }

        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(subscriptions));

        assertEquals(300, assignment.groupAssignment().size(), "every member must still be assigned");
        assignment.groupAssignment().values().forEach(a ->
                assertNull(a.userData(), "a mapping over the budget must not be sent at all"));
    }

    @Test
    void onAssignmentMakesTheDeliveredTopologyAvailableToTheTracker() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        MemberIdTracker tracker = new MemberIdTracker();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of(),
                LoadAwareAssignorConfig.MEMBER_ID_TRACKER, tracker,
                LoadAwareAssignorConfig.INSTANCE_ID, "pod-a"));
        assignor.subscriptionUserData(Set.of("t"));
        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(instanceSubscriptions()));
        assignor.onAssignment(
                assignment.groupAssignment().get("a1"),
                new ConsumerGroupMetadata("g", 7, "a1", Optional.empty()));

        var liveMembers = List.of(liveMember("a1", Optional.empty()), liveMember("a2", Optional.empty()),
                liveMember("b1", Optional.empty()));
        List<GroupMember> resolved = tracker.resolveMembers("g", liveMembers);
        assertNotNull(resolved);
        assertEquals(Map.of("a1", "pod-a", "a2", "pod-a", "b1", "pod-b"), resolved.stream()
                .collect(Collectors.toMap(GroupMember::memberId, GroupMember::instanceId)));
    }

    @Test
    void monitoringOwnershipUsesStaticIdentityAndDoesNotRequireAssignedPartitions() {
        LoadAwarePartitionAssignor assignor = configuredAssignor();
        Subscription owner = new Subscription(List.of("idle"), MonitoringProtocol.subscription("pod-a", true, 7));
        owner.setGroupInstanceId(Optional.of("static-a"));
        Subscription worker = new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-b", true, 8));
        worker.setGroupInstanceId(Optional.of("static-b"));
        Subscription unsupported = new Subscription(List.of("t"), InstanceUserData.encode("pod-c"));
        unsupported.setGroupInstanceId(Optional.of("static-0"));
        GroupAssignment assignment = assignor.assign(cluster("t", 2), new GroupSubscription(
                Map.of("z-owner", owner, "a-worker", worker, "first-unsupported", unsupported)));

        assertTrue(assignment.groupAssignment().get("z-owner").partitions().isEmpty());
        var topology = MonitoringProtocol.readAssignment(assignment.groupAssignment().get("z-owner").userData());
        assertNotNull(topology);
        assertEquals("s:static-a", topology.ownerIdentity());
        assertEquals(7, topology.revision());
        assertEquals(Set.of("idle"), topology.members().get("s:static-a").topics());
        assertEquals(Set.of("t"), topology.members().get("s:static-b").topics());
        assertEquals(8, MonitoringProtocol.readAssignment(
                assignment.groupAssignment().get("a-worker").userData()).revision());
        assertTrue(MonitoringProtocol.readAssignment(
                assignment.groupAssignment().get("first-unsupported").userData()).members().isEmpty());
    }

    @Test
    void noMonitoringMetadataIsSentWithoutACapableMonitor() {
        GroupAssignment assignment = configuredAssignor().assign(cluster("t", 2), new GroupSubscription(Map.of(
                "a", new Subscription(List.of("t"), InstanceUserData.encode("pod-a")),
                "b", new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-b", false, 0)))));
        assignment.groupAssignment().values().forEach(member -> assertNull(member.userData()));
    }

    @Test
    void fiveHundredMembersAndPartitionsFitTheBudgetAndMetadataGrowsLinearly() {
        GroupAssignment small = scaleAssignment(250);
        GroupAssignment required = scaleAssignment(500);
        assertEquals(500, required.groupAssignment().size());
        assertEquals(500, required.groupAssignment().values().stream().mapToInt(a -> a.partitions().size()).sum());
        long largeBytes = metadataBytes(required);
        long smallBytes = metadataBytes(small);
        assertTrue(largeBytes < MonitoringProtocol.MAX_TOTAL_BYTES);
        assertTrue(largeBytes >= smallBytes * 1.9 && largeBytes <= smallBytes * 2.1,
                "doubling group size should approximately double metadata: " + smallBytes + " -> " + largeBytes);
        List<MonitoringProtocol.Assignment> topologies = required.groupAssignment().values().stream()
                .map(a -> MonitoringProtocol.readAssignment(a.userData()))
                .peek(a -> assertNotNull(a, "every member must receive a readable acknowledgement"))
                .filter(a -> !a.members().isEmpty()).toList();
        assertEquals(1, topologies.size());
        assertEquals(500, topologies.getFirst().members().size());
        assertEquals(100, topologies.getFirst().members().values().stream()
                .map(MonitoringProtocol.Member::instanceId).distinct().count());
    }

    @Test
    void onAssignmentWithoutTrackerIsNoOp() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of()));

        assertDoesNotThrow(() -> assignor.onAssignment(
                new Assignment(List.of()),
                new ConsumerGroupMetadata("g", 1, "m-1", Optional.empty())));
    }

    private static LoadAwarePartitionAssignor configureCapturing(
            WeightService weightService,
            AtomicReference<Map<TopicPartition, Double>> capturedWeights) {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        BalanceService capturingBalance = (members, weights) -> {
            capturedWeights.set(weights);
            return new SortingRoundRobinBalanceService().computeOptimalAssignment(members, weights);
        };
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService,
                LoadAwareAssignorConfig.BALANCE_SERVICE, capturingBalance));
        return assignor;
    }

    private static LoadAwarePartitionAssignor configuredAssignor() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, (WeightService) partitions -> Map.of()));
        return assignor;
    }

    /** Two members in pod-a, one in pod-b — the case the trigger has to see as two instances. */
    private static Map<String, Subscription> instanceSubscriptions() {
        Map<String, Subscription> subscriptions = new TreeMap<>();
        subscriptions.put("a1", new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-a", true, 0)));
        subscriptions.put("a2", new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-a", true, 0)));
        subscriptions.put("b1", new Subscription(List.of("t"), MonitoringProtocol.subscription("pod-b", true, 0)));
        return subscriptions;
    }

    private static org.apache.kafka.clients.admin.MemberDescription liveMember(
            String memberId, Optional<String> staticId) {
        return new org.apache.kafka.clients.admin.MemberDescription(memberId, staticId, "client", "host",
                new org.apache.kafka.clients.admin.MemberAssignment(Set.of()));
    }

    private static GroupAssignment scaleAssignment(int memberCount) {
        Map<String, Subscription> subscriptions = new TreeMap<>();
        for (int i = 0; i < memberCount; i++) {
            var subscription = new Subscription(List.of("t"),
                    MonitoringProtocol.subscription(String.format("pod-%04d", i / 5), true, 0));
            subscription.setGroupInstanceId(Optional.of(String.format("consumer-%04d", i)));
            subscriptions.put(String.format("member-%04d", i), subscription);
        }
        return configuredAssignor().assign(cluster("t", memberCount), new GroupSubscription(subscriptions));
    }

    private static long metadataBytes(GroupAssignment assignment) {
        return assignment.groupAssignment().values().stream().mapToLong(a -> {
            assertNotNull(a.userData(), "the metadata budget must not disable proactive monitoring");
            return a.userData().remaining();
        }).sum();
    }

    private static Cluster cluster(String topic, int partitionCount) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new PartitionInfo(topic, i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("c", List.of(node), partitions, Set.of(), Set.of());
    }

    private static Map<String, Subscription> subscriptions(Map<String, List<String>> topicsByMember) {
        Map<String, Subscription> subscriptions = new TreeMap<>();
        ByteBuffer userData = ByteBuffer.allocate(0);
        topicsByMember.forEach((member, topics) ->
                subscriptions.put(member, new Subscription(topics, userData)));
        return subscriptions;
    }

    private static Object getField(Object target, String name) throws Exception {
        Field f = LoadAwarePartitionAssignor.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }
}
