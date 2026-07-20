package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the two-level greedy least-loaded placement: load is evened across instances first
 * (heavy partitions never co-locate in one instance while another idles), across the members
 * of each instance second, and a partition goes only to members subscribed to its topic.
 * Members that are each their own instance behave like plain member-level greedy.
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
                loadAware.computeOptimalAssignment(singletonInstances(members, topic), weights);
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
        List<GroupMember> members = List.of(
                new GroupMember("c0", "c0", Set.of("a")),
                new GroupMember("c1", "c1", Set.of("a", "b")));
        Map<TopicPartition, Double> weights = Map.of(
                new TopicPartition("a", 0), 10.0,
                new TopicPartition("b", 0), 100.0,
                new TopicPartition("b", 1), 1.0);

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        assertEquals(
                List.of(new TopicPartition("b", 0), new TopicPartition("b", 1)),
                assignment.get("c1").stream()
                        .filter(tp -> tp.topic().equals("b"))
                        .sorted(Comparator.comparingInt(TopicPartition::partition))
                        .toList(),
                "only c1 subscribes to topic 'b', so it must receive every 'b' partition");
        assertTrue(assignment.get("c0").stream().allMatch(tp -> tp.topic().equals("a")));

        assertEveryPartitionAssignedOnce(assignment, weights);
    }

    @Test
    void spreadsZeroWeightPartitionsEvenlyAcrossMembers() {
        Set<String> members = new TreeSet<>(List.of("c0", "c1", "c2"));
        String topic = "orders";
        Map<TopicPartition, Double> weights = new HashMap<>();
        for (int p = 0; p < 6; p++) {
            weights.put(new TopicPartition(topic, p), 0.0);
        }

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(singletonInstances(members, topic), weights);

        assignment.forEach((member, partitions) -> assertEquals(
                2, partitions.size(),
                () -> "each member should get 2 of the 6 zero-weight partitions: " + assignment));

        assertEveryPartitionAssignedOnce(assignment, weights);
    }

    @Test
    void spreadsZeroWeightPartitionsByCountWithoutDisturbingWeightedPlacement() {
        Set<String> members = new TreeSet<>(List.of("c0", "c1"));
        String topic = "orders";
        Map<TopicPartition, Double> weights = new HashMap<>();
        weights.put(new TopicPartition(topic, 0), 10.0);
        weights.put(new TopicPartition(topic, 1), 10.0);
        for (int p = 2; p < 6; p++) {
            weights.put(new TopicPartition(topic, p), 0.0);
        }

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(singletonInstances(members, topic), weights);

        assignment.forEach((member, partitions) -> {
            assertEquals(3, partitions.size(),
                    () -> "each member should hold 3 of the 6 partitions: " + assignment);
            assertEquals(10.0, partitions.stream().mapToDouble(weights::get).sum(),
                    () -> "each member should carry one weighted partition: " + assignment);
        });
    }

    @Test
    void spreadsPartitionsAcrossInstancesWhenMembersOutnumberPartitions() {
        String topic = "orders";
        List<GroupMember> members = List.of(
                new GroupMember("a1", "pod-a", Set.of(topic)),
                new GroupMember("a2", "pod-a", Set.of(topic)),
                new GroupMember("a3", "pod-a", Set.of(topic)),
                new GroupMember("b1", "pod-b", Set.of(topic)),
                new GroupMember("b2", "pod-b", Set.of(topic)),
                new GroupMember("b3", "pod-b", Set.of(topic)));
        Map<TopicPartition, Double> weights = new HashMap<>();
        for (int p = 0; p < 4; p++) {
            weights.put(new TopicPartition(topic, p), 1.0);
        }

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        Map<String, Integer> partitionsPerInstance = countPerInstance(assignment, members);
        assertEquals(Map.of("pod-a", 2, "pod-b", 2), partitionsPerInstance,
                () -> "4 partitions across 2 instances of 3 members must land 2 per instance: " + assignment);
        assignment.forEach((member, partitions) -> assertTrue(partitions.size() <= 1,
                () -> "within an instance, partitions should go to distinct members: " + assignment));
        assertEveryPartitionAssignedOnce(assignment, weights);
    }

    @Test
    void avoidsCoLocatingHeavyPartitionsInOneInstance() {
        String topic = "orders";
        List<GroupMember> members = List.of(
                new GroupMember("a1", "pod-a", Set.of(topic)),
                new GroupMember("a2", "pod-a", Set.of(topic)),
                new GroupMember("b1", "pod-b", Set.of(topic)),
                new GroupMember("b2", "pod-b", Set.of(topic)));
        Map<TopicPartition, Double> weights = Map.of(
                new TopicPartition(topic, 0), 100.0,
                new TopicPartition(topic, 1), 100.0,
                new TopicPartition(topic, 2), 1.0,
                new TopicPartition(topic, 3), 1.0);

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        Map<String, Double> instanceLoads = loadPerInstance(assignment, members, weights);
        assertEquals(Map.of("pod-a", 101.0, "pod-b", 101.0), instanceLoads,
                () -> "each instance must carry exactly one heavy partition: " + assignment);
    }

    @Test
    void balancesInstanceLoadsRegardlessOfMemberCounts() {
        String topic = "orders";
        List<GroupMember> members = List.of(
                new GroupMember("a1", "pod-a", Set.of(topic)),
                new GroupMember("b1", "pod-b", Set.of(topic)),
                new GroupMember("b2", "pod-b", Set.of(topic)),
                new GroupMember("b3", "pod-b", Set.of(topic)));
        Map<TopicPartition, Double> weights = Map.of(
                new TopicPartition(topic, 0), 100.0,
                new TopicPartition(topic, 1), 50.0,
                new TopicPartition(topic, 2), 30.0,
                new TopicPartition(topic, 3), 20.0);

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        Map<String, Double> instanceLoads = loadPerInstance(assignment, members, weights);
        assertEquals(Map.of("pod-a", 100.0, "pod-b", 100.0), instanceLoads,
                () -> "instances get equal traffic even with different member counts: " + assignment);
    }

    @Test
    void spreadsZeroWeightPartitionsEvenlyAcrossInstances() {
        String topic = "orders";
        List<GroupMember> members = List.of(
                new GroupMember("a1", "pod-a", Set.of(topic)),
                new GroupMember("b1", "pod-b", Set.of(topic)),
                new GroupMember("b2", "pod-b", Set.of(topic)));
        Map<TopicPartition, Double> weights = new HashMap<>();
        for (int p = 0; p < 6; p++) {
            weights.put(new TopicPartition(topic, p), 0.0);
        }

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        Map<String, Integer> partitionsPerInstance = countPerInstance(assignment, members);
        assertEquals(Map.of("pod-a", 3, "pod-b", 3), partitionsPerInstance,
                () -> "zero-weight partitions must spread count-evenly across instances: " + assignment);
        assertTrue(Math.abs(assignment.get("b1").size() - assignment.get("b2").size()) <= 1,
                () -> "zero-weight partitions must spread count-evenly within an instance: " + assignment);
        assertEveryPartitionAssignedOnce(assignment, weights);
    }

    @Test
    void routesTopicsOnlyToInstancesWithSubscribedMembers() {
        List<GroupMember> members = List.of(
                new GroupMember("a1", "pod-a", Set.of("a")),
                new GroupMember("b1", "pod-b", Set.of("a", "b")));
        Map<TopicPartition, Double> weights = Map.of(
                new TopicPartition("a", 0), 10.0,
                new TopicPartition("b", 0), 100.0,
                new TopicPartition("b", 1), 1.0);

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, weights);

        assertEquals(
                Set.of(new TopicPartition("b", 0), new TopicPartition("b", 1)),
                assignment.get("b1").stream().filter(tp -> tp.topic().equals("b")).collect(
                        HashSet::new, HashSet::add, HashSet::addAll),
                "only pod-b has a member subscribed to 'b', so it takes every 'b' partition "
                        + "no matter how loaded it already is");
        assertEquals(List.of(new TopicPartition("a", 0)), assignment.get("a1"));
    }

    @Test
    void singleInstanceGroupBehavesLikeMemberLevelGreedy() {
        Set<String> memberIds = new TreeSet<>(List.of("c0", "c1", "c2"));
        String topic = "orders";
        Map<TopicPartition, Double> weights = new HashMap<>();
        for (int p = 0; p < 6; p++) {
            weights.put(new TopicPartition(topic, p), (p < 2) ? 100.0 : 1.0);
        }
        List<GroupMember> oneInstance = memberIds.stream()
                .map(id -> new GroupMember(id, "the-only-pod", Set.of(topic)))
                .toList();

        Map<String, List<TopicPartition>> oneInstanceAssignment =
                loadAware.computeOptimalAssignment(oneInstance, weights);
        Map<String, List<TopicPartition>> singletonAssignment =
                loadAware.computeOptimalAssignment(singletonInstances(memberIds, topic), weights);

        assertEquals(singletonAssignment, oneInstanceAssignment,
                "with all members in one instance, the instance level degenerates to member-level greedy");
    }

    @Test
    void failsWhenNoMemberSubscribesToAPartitionsTopic() {
        List<GroupMember> members = List.of(new GroupMember("c0", "c0", Set.of("a")));
        Map<TopicPartition, Double> weights = Map.of(new TopicPartition("b", 0), 1.0);

        assertThrows(IllegalStateException.class,
                () -> loadAware.computeOptimalAssignment(members, weights));
    }

    @Test
    void returnsEmptyListsForAllMembersWhenThereAreNoPartitions() {
        List<GroupMember> members = List.of(
                new GroupMember("c0", "pod-a", Set.of("a")),
                new GroupMember("c1", "pod-a", Set.of("a")));

        Map<String, List<TopicPartition>> assignment =
                loadAware.computeOptimalAssignment(members, Map.of());

        assertEquals(Map.of("c0", List.of(), "c1", List.of()), assignment);
    }

    @Test
    void failsWithoutMembers() {
        assertThrows(IllegalArgumentException.class,
                () -> loadAware.computeOptimalAssignment(List.of(), Map.of()));
    }

    @Test
    void failsOnDuplicateMemberIds() {
        List<GroupMember> members = List.of(
                new GroupMember("c0", "pod-a", Set.of("a")),
                new GroupMember("c0", "pod-b", Set.of("a")));

        assertThrows(IllegalArgumentException.class,
                () -> loadAware.computeOptimalAssignment(members, Map.of()));
    }

    /** Every member as its own instance: plain member-level balancing. */
    private static List<GroupMember> singletonInstances(Collection<String> memberIds, String topic) {
        return memberIds.stream()
                .map(id -> new GroupMember(id, id, Set.of(topic)))
                .toList();
    }

    private static void assertEveryPartitionAssignedOnce(
            Map<String, List<TopicPartition>> assignment,
            Map<TopicPartition, Double> weights) {
        List<TopicPartition> allAssigned = assignment.values().stream().flatMap(List::stream).toList();
        assertEquals(weights.keySet(), new HashSet<>(allAssigned),
                "every partition must be assigned");
        assertEquals(weights.size(), allAssigned.size(), "no partition may be assigned twice");
    }

    private static Map<String, Integer> countPerInstance(
            Map<String, List<TopicPartition>> assignment, List<GroupMember> members) {
        Map<String, Integer> counts = new HashMap<>();
        for (GroupMember member : members) {
            counts.merge(member.instanceId(), assignment.get(member.memberId()).size(), Integer::sum);
        }
        return counts;
    }

    private static Map<String, Double> loadPerInstance(
            Map<String, List<TopicPartition>> assignment,
            List<GroupMember> members,
            Map<TopicPartition, Double> weights) {
        Map<String, Double> loads = new HashMap<>();
        for (GroupMember member : members) {
            double load = assignment.get(member.memberId()).stream().mapToDouble(weights::get).sum();
            loads.merge(member.instanceId(), load, Double::sum);
        }
        return loads;
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
