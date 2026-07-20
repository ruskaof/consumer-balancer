package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.instance.InstanceIdResolver;
import io.github.ruskaof.balancer.instance.InstanceUserData;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
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
