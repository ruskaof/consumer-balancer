package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the assignor delegates to {@link BalanceService} using the same
 * greedy placement
 * as {@link SortingRoundRobinBalanceService} when weights are supplied.
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
        Map<String, Subscription> subscriptions = new TreeMap<>();
        ByteBuffer userData = ByteBuffer.allocate(0);
        subscriptions.put("a", new Subscription(List.of(topic), userData));
        subscriptions.put("b", new Subscription(List.of(topic), userData));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        Map<TopicPartition, Double> w = weights.computeWeights(Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)));
        Map<String, List<TopicPartition>> expected = balance.computeOptimalAssignment(subscriptions.keySet(), w);

        assertEquals(expected, assignment);
    }

    @Test
    void configureBuildsPrometheusDefaultsWhenNoWeightServiceConfigured() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        assignor.configure(Map.of(
                LoadAwareAssignorConfig.PROMETHEUS_HOST, "localhost",
                LoadAwareAssignorConfig.PROMETHEUS_PORT, "9090",
                LoadAwareAssignorConfig.PROMETHEUS_WEIGHT_QUERY_TEMPLATE,
                "sum(rate(kafka_messages_total{topic=~\"%s\"}[1m])) by (topic, partition)"));

        assertInstanceOf(PrometheusWeightService.class, getField(assignor, "weightService"));
        assertInstanceOf(SortingRoundRobinBalanceService.class, getField(assignor, "balanceService"));
    }

    @Test
    void configureFailsWithoutWeightServiceOrPrometheusConfigs() {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> assignor.configure(Map.of()));

        assertTrue(e.getMessage().contains(LoadAwareAssignorConfig.PROMETHEUS_HOST));
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

    private static Object getField(Object target, String name) throws Exception {
        Field f = LoadAwarePartitionAssignor.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }
}
