package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.weight.WeightService;
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
    void assignMatchesGreedyBalanceWhenWeightsAreProvided() throws Exception {
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();

        WeightService weights = partitions -> {
            Map<TopicPartition, Double> m = new HashMap<>();
            for (TopicPartition tp : partitions) {
                m.put(tp, tp.partition() == 0 ? 50.0 : 1.0);
            }
            return m;
        };
        BalanceService balance = new SortingRoundRobinBalanceService();

        setField(assignor, "weightService", weights);
        setField(assignor, "balanceService", balance);

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

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = LoadAwarePartitionAssignor.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
