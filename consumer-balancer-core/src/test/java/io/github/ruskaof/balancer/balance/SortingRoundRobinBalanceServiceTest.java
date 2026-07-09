package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the greedy least-loaded placement: it lowers the worst consumer load compared to
 * Kafka's {@link RoundRobinAssignor} when partition weights are skewed, and it assigns a
 * partition only to members subscribed to its topic.
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

        Map<String, List<TopicPartition>> greedy =
                loadAware.computeOptimalAssignment(homogeneous(members, topic), weights);
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

    @Test
    void assignsPartitionsOnlyToSubscribedMembers() {
        Map<String, Set<String>> subscribedTopics = Map.of(
                "c0", Set.of("a"),
                "c1", Set.of("a", "b"));
        Map<TopicPartition, Double> weights = Map.of(
                new TopicPartition("a", 0), 10.0,
                new TopicPartition("b", 0), 100.0,
                new TopicPartition("b", 1), 1.0);

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(subscribedTopics, weights);

        assertEquals(
                List.of(new TopicPartition("b", 0), new TopicPartition("b", 1)),
                assignment.get("c1").stream()
                        .filter(tp -> tp.topic().equals("b"))
                        .sorted(Comparator.comparingInt(TopicPartition::partition))
                        .toList(),
                "only c1 subscribes to topic 'b', so it must receive every 'b' partition");
        assertTrue(assignment.get("c0").stream().allMatch(tp -> tp.topic().equals("a")));

        Set<TopicPartition> allAssigned = new HashSet<>();
        assignment.values().forEach(allAssigned::addAll);
        assertEquals(weights.keySet(), allAssigned, "every partition must be assigned exactly once");
    }

    @Test
    void failsWhenNoMemberSubscribesToAPartitionsTopic() {
        Map<String, Set<String>> subscribedTopics = Map.of("c0", Set.of("a"));
        Map<TopicPartition, Double> weights = Map.of(new TopicPartition("b", 0), 1.0);

        assertThrows(IllegalStateException.class,
                () -> loadAware.computeOptimalAssignment(subscribedTopics, weights));
    }

    @Test
    void returnsEmptyListsForAllMembersWhenThereAreNoPartitions() {
        Map<String, List<TopicPartition>> assignment = loadAware.computeOptimalAssignment(
                Map.of("c0", Set.of("a"), "c1", Set.of("a")), Map.of());

        assertEquals(Map.of("c0", List.of(), "c1", List.of()), assignment);
    }

    @Test
    void failsWithoutMembers() {
        assertThrows(IllegalArgumentException.class,
                () -> loadAware.computeOptimalAssignment(Map.of(), Map.of()));
    }

    private static Map<String, Set<String>> homogeneous(Set<String> members, String topic) {
        Map<String, Set<String>> subscribedTopics = new HashMap<>();
        for (String member : members) {
            subscribedTopics.put(member, Set.of(topic));
        }
        return subscribedTopics;
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
