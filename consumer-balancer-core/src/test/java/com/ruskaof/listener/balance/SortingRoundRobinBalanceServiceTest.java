package com.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demonstrates that greedy least-loaded assignment lowers the worst consumer
 * load compared to
 * Kafka's {@link RoundRobinAssignor} when partition weights are skewed (hot
 * partitions).
 */
class SortingRoundRobinBalanceServiceTest {

    private final SortingRoundRobinBalanceService loadAware = new SortingRoundRobinBalanceService();

    @Test
    void greedyLowersMaxLoadVersusRoundRobinWhenWeightsAreSkewed() {
        Set<String> members = new TreeSet<>(List.of("c0", "c1", "c2"));
        String topic = "orders";
        int numPartitions = 6;
        Map<TopicPartition, Double> weights = new HashMap<>();
        for (int p = 0; p < numPartitions; p++) {
            TopicPartition tp = new TopicPartition(topic, p);
            double w = (p < 2) ? 100.0 : 1.0;
            weights.put(tp, w);
        }

        Map<String, List<TopicPartition>> greedy = loadAware.computeOptimalAssignment(members, weights);
        Map<String, List<TopicPartition>> roundRobin = roundRobinAssignment(topic, numPartitions, members);

        double greedyMax = maxMemberLoad(greedy, weights);
        double rrMax = maxMemberLoad(roundRobin, weights);

        assertTrue(
                greedyMax < rrMax,
                () -> String.format(
                        "Greedy max load (%.2f) should be < round-robin max load (%.2f). "
                                + "Greedy=%s RR=%s weights=%s",
                        greedyMax, rrMax, greedy, roundRobin, weights));
    }

    private static double maxMemberLoad(
            Map<String, List<TopicPartition>> assignment,
            Map<TopicPartition, Double> weights) {
        return assignment.values().stream()
                .mapToDouble(parts -> parts.stream().mapToDouble(weights::get).sum())
                .max()
                .orElse(0);
    }

    /**
     * Same topic subscription for all members; uses Kafka's
     * {@link RoundRobinAssignor} for a fair baseline.
     */
    private static Map<String, List<TopicPartition>> roundRobinAssignment(
            String topic,
            int numPartitions,
            Set<String> members) {
        Map<String, Integer> partitionsPerTopic = Map.of(topic, numPartitions);
        Map<String, Subscription> subscriptions = new TreeMap<>();
        ByteBuffer userData = ByteBuffer.allocate(0);
        for (String m : members) {
            subscriptions.put(m, new Subscription(List.of(topic), userData));
        }
        RoundRobinAssignor rr = new RoundRobinAssignor();
        return rr.assign(partitionsPerTopic, subscriptions);
    }
}
