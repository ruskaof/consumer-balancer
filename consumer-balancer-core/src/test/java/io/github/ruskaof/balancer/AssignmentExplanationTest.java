package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.AssignmentExplanation.InstanceLine;
import io.github.ruskaof.balancer.balance.GroupMember;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins the content and format of the assignment explanation the assignor logs: the
 * per-instance distribution lines, the skew line, and the exact wording and canonical order
 * of every detected unevenness factor. Formats are pinned by one golden string per event
 * kind; the factor rules are pinned through {@code reasons()}.
 */
class AssignmentExplanationTest {

    @Test
    void reportsPerInstanceLinesSortedByInstanceIdWithGoldenInfoSummary() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "orders"), member("m-b1", "pod-b", "orders")),
                Map.of(
                        tp("orders", 0), 30.0,
                        tp("orders", 1), 30.0,
                        tp("orders", 2), 20.0,
                        tp("orders", 3), 20.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("orders", 0), tp("orders", 3)),
                        "m-b1", List.of(tp("orders", 1), tp("orders", 2))));

        assertEquals(
                List.of(
                        new InstanceLine("pod-a", 1, 2, 50.0),
                        new InstanceLine("pod-b", 1, 2, 50.0)),
                explanation.instances());
        assertEquals(List.of(), explanation.reasons());
        assertEquals("""
                Load-aware assignment computed [instances=2, members=2, partitions=4, totalWeight=100.0, defaultedWeights=0]:
                  instance pod-a: load=50.0 (50.0% of total), partitions=2, members=1
                  instance pod-b: load=50.0 (50.0% of total), partitions=2, members=1
                  skew: max instance load 50.0 (pod-a) is +0.0% above the ideal even share 50.0
                  unevenness factors: none detected""",
                explanation.infoSummary());
    }

    @Test
    void reportsAllDefaultedWeightsAsCountBalanced() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t"), member("m-b1", "pod-b", "t")),
                Map.of(tp("t", 0), 1.0, tp("t", 1), 1.0, tp("t", 2), 1.0),
                3,
                Map.of(
                        "m-a1", List.of(tp("t", 0), tp("t", 2)),
                        "m-b1", List.of(tp("t", 1))));

        assertEquals(
                List.of("all 3 partitions had no usable weight and got the default 1.0"
                        + " — the distribution is by partition count, not load"
                        + " (expected on the first assignment after startup, before the weight store has history)"),
                explanation.reasons());
    }

    @Test
    void reportsPartiallyDefaultedWeights() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t"), member("m-b1", "pod-b", "t")),
                Map.of(
                        tp("t", 0), 50.0,
                        tp("t", 1), 50.0,
                        tp("t", 2), 1.0,
                        tp("t", 3), 45.0),
                1,
                Map.of(
                        "m-a1", List.of(tp("t", 0), tp("t", 2)),
                        "m-b1", List.of(tp("t", 1), tp("t", 3))));

        assertEquals(
                List.of("1 of 4 partitions had no usable weight and got the default 1.0;"
                        + " their real load is invisible to this assignment"),
                explanation.reasons());
    }

    @Test
    void flagsDefaultsDwarfedByMeasuredWeights() {
        List<GroupMember> members =
                List.of(member("m-a1", "pod-a", "t"), member("m-b1", "pod-b", "t"));
        Map<String, List<TopicPartition>> assignment = Map.of(
                "m-a1", List.of(tp("t", 0)),
                "m-b1", List.of(tp("t", 1), tp("t", 2)));

        AssignmentExplanation atFactor = AssignmentExplanation.explain(
                members,
                Map.of(tp("t", 0), 100.0, tp("t", 1), 99.0, tp("t", 2), 1.0),
                1,
                assignment);
        assertEquals(
                List.of(
                        "1 of 3 partitions had no usable weight and got the default 1.0;"
                                + " their real load is invisible to this assignment",
                        "the default weight 1.0 is negligible next to the heaviest measured weight 100.0"
                                + " (>=100x), so defaulted partitions were placed as if nearly free"),
                atFactor.reasons());

        AssignmentExplanation belowFactor = AssignmentExplanation.explain(
                members,
                Map.of(tp("t", 0), 99.9, tp("t", 1), 99.9, tp("t", 2), 1.0),
                1,
                assignment);
        assertEquals(
                List.of("1 of 3 partitions had no usable weight and got the default 1.0;"
                        + " their real load is invisible to this assignment"),
                belowFactor.reasons());
    }

    @Test
    void flagsHeaviestPartitionAboveIdealShare() {
        List<GroupMember> members =
                List.of(member("m-a1", "pod-a", "orders"), member("m-b1", "pod-b", "orders"));

        AssignmentExplanation dominated = AssignmentExplanation.explain(
                members,
                Map.of(tp("orders", 0), 500.0, tp("orders", 1), 120.0, tp("orders", 2), 80.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("orders", 1), tp("orders", 2)),
                        "m-b1", List.of(tp("orders", 0))));
        assertEquals(
                List.of("partition orders-0 alone weighs 500.0, more than the ideal even share 350.0"
                        + " — a perfectly even distribution is impossible"),
                dominated.reasons());

        AssignmentExplanation atBound = AssignmentExplanation.explain(
                members,
                Map.of(tp("orders", 0), 350.0, tp("orders", 1), 200.0, tp("orders", 2), 150.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("orders", 1), tp("orders", 2)),
                        "m-b1", List.of(tp("orders", 0))));
        assertEquals(List.of(), atBound.reasons(),
                "a partition weighing exactly the ideal share does not make evenness impossible");
    }

    @Test
    void flagsTopicsConstrainedToInstanceSubset() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a1", "pod-a", "orders", "payments", "audit", "idle"),
                        member("m-b1", "pod-b", "orders", "payments"),
                        member("m-c1", "pod-c", "orders")),
                Map.of(
                        tp("orders", 0), 50.0,
                        tp("orders", 1), 50.0,
                        tp("payments", 0), 40.0,
                        tp("audit", 0), 30.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("audit", 0)),
                        "m-b1", List.of(tp("payments", 0), tp("orders", 0)),
                        "m-c1", List.of(tp("orders", 1))));

        assertEquals(
                List.of("partitions of 2 topic(s) could only go to a subset of instances:"
                        + " audit (1 of 3 instances, weight 30.0), payments (2 of 3 instances, weight 40.0)"),
                explanation.reasons(),
                "a topic every instance subscribes to, or one without assigned partitions, is not a constraint");
    }

    @Test
    void capsListedConstrainedTopicsAtFive() {
        Map<TopicPartition, Double> weights = new HashMap<>();
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        List<TopicPartition> exclusive = new ArrayList<>();
        for (int i = 1; i <= 7; i++) {
            weights.put(tp("t" + i, 0), 10.0);
            exclusive.add(tp("t" + i, 0));
        }
        weights.put(tp("shared", 0), 10.0);
        assignment.put("m-a1", exclusive);
        assignment.put("m-b1", List.of(tp("shared", 0)));

        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a1", "pod-a", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "shared"),
                        member("m-b1", "pod-b", "shared")),
                weights,
                0,
                assignment);

        assertEquals(
                List.of("partitions of 7 topic(s) could only go to a subset of instances:"
                        + " t1 (1 of 2 instances, weight 10.0), t2 (1 of 2 instances, weight 10.0),"
                        + " t3 (1 of 2 instances, weight 10.0), t4 (1 of 2 instances, weight 10.0),"
                        + " t5 (1 of 2 instances, weight 10.0), and 2 more"),
                explanation.reasons());
    }

    @Test
    void flagsUnequalMemberCountsPerInstance() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a1", "pod-a", "t"),
                        member("m-a2", "pod-a", "t"),
                        member("m-a3", "pod-a", "t"),
                        member("m-b1", "pod-b", "t")),
                Map.of(
                        tp("t", 0), 25.0,
                        tp("t", 1), 25.0,
                        tp("t", 2), 25.0,
                        tp("t", 3), 25.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("t", 0)),
                        "m-a2", List.of(tp("t", 1)),
                        "m-a3", List.of(),
                        "m-b1", List.of(tp("t", 2), tp("t", 3))));

        assertEquals(
                List.of("instances run different member counts (1-3); each instance still receives"
                        + " an equal load share by design, so members of smaller instances run hotter"),
                explanation.reasons());
    }

    @Test
    void showsEmptyInstancesAndFlagsMoreInstancesThanPartitions() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a", "pod-a", "t"),
                        member("m-b", "pod-b", "t"),
                        member("m-c", "pod-c", "t"),
                        member("m-d", "pod-d", "t"),
                        member("m-e", "pod-e", "t")),
                Map.of(tp("t", 0), 10.0, tp("t", 1), 10.0, tp("t", 2), 10.0),
                0,
                Map.of(
                        "m-a", List.of(tp("t", 0)),
                        "m-b", List.of(tp("t", 1)),
                        "m-c", List.of(tp("t", 2)),
                        "m-d", List.of(),
                        "m-e", List.of()));

        assertEquals(
                List.of(
                        "partition t-0 alone weighs 10.0, more than the ideal even share 6.0"
                                + " — a perfectly even distribution is impossible",
                        "more instances (5) than partitions (3) — instance(s) [pod-d, pod-e] received nothing"),
                explanation.reasons(),
                "with more instances than partitions, perfect evenness is genuinely impossible too");
        assertEquals(new InstanceLine("pod-d", 1, 0, 0.0), explanation.instances().get(3));
        assertEquals(new InstanceLine("pod-e", 1, 0, 0.0), explanation.instances().get(4));
    }

    @Test
    void reportsAllZeroWeightsAsCountSpread() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t"), member("m-b1", "pod-b", "t")),
                Map.of(tp("t", 0), 0.0, tp("t", 1), 0.0, tp("t", 2), 0.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("t", 0), tp("t", 2)),
                        "m-b1", List.of(tp("t", 1))));

        assertEquals(
                List.of("every partition weight is zero — partitions were spread by count, not load"
                        + " (the weight store currently reports no traffic)"),
                explanation.reasons());
        assertTrue(explanation.infoSummary().contains("skew: n/a (no measured load)"));
        assertFalse(explanation.infoSummary().contains("% of total"),
                "shares of a zero total are meaningless and must be omitted");
    }

    @Test
    void ordersCombinedReasonsCanonically() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a1", "pod-a", "orders", "audit"),
                        member("m-a2", "pod-a", "orders"),
                        member("m-b1", "pod-b", "orders")),
                Map.of(
                        tp("orders", 0), 500.0,
                        tp("orders", 1), 120.0,
                        tp("audit", 0), 1.0),
                1,
                Map.of(
                        "m-a1", List.of(tp("orders", 1), tp("audit", 0)),
                        "m-a2", List.of(),
                        "m-b1", List.of(tp("orders", 0))));

        assertEquals(
                List.of(
                        "1 of 3 partitions had no usable weight and got the default 1.0;"
                                + " their real load is invisible to this assignment",
                        "the default weight 1.0 is negligible next to the heaviest measured weight 500.0"
                                + " (>=100x), so defaulted partitions were placed as if nearly free",
                        "partition orders-0 alone weighs 500.0, more than the ideal even share 310.5"
                                + " — a perfectly even distribution is impossible",
                        "partitions of 1 topic(s) could only go to a subset of instances:"
                                + " audit (1 of 2 instances, weight 1.0)",
                        "instances run different member counts (1-2); each instance still receives"
                                + " an equal load share by design, so members of smaller instances run hotter"),
                explanation.reasons());
    }

    @Test
    void singleInstanceReportsNaSkew() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t"), member("m-a2", "pod-a", "t")),
                Map.of(tp("t", 0), 10.0, tp("t", 1), 20.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("t", 1)),
                        "m-a2", List.of(tp("t", 0))));

        assertEquals(List.of(), explanation.reasons());
        assertTrue(explanation.infoSummary().contains("skew: n/a (single instance)"));
    }

    @Test
    void handlesAssignmentWithNoPartitions() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t"), member("m-b1", "pod-b", "t")),
                Map.of(),
                0,
                Map.of("m-a1", List.of(), "m-b1", List.of()));

        assertEquals(List.of(), explanation.reasons());
        assertTrue(explanation.infoSummary().contains("partitions=0"));
        assertTrue(explanation.infoSummary().contains("skew: n/a (no measured load)"));
        assertTrue(explanation.infoSummary().contains("unevenness factors: none detected"));
    }

    @Test
    void toleratesCustomBalanceServiceOutputAndFlagsUnassignedPartitions() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(member("m-a1", "pod-a", "t")),
                Map.of(tp("t", 0), 10.0, tp("t", 1), 5.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("t", 0), tp("x", 9)),
                        "ghost", List.of()));

        assertEquals(
                List.of(
                        new InstanceLine("ghost", 1, 0, 0.0),
                        new InstanceLine("pod-a", 1, 2, 11.0)),
                explanation.instances(),
                "unknown members count as their own instance; unknown partitions count at the default weight");
        assertEquals(
                List.of(
                        "1 partition(s) were not assigned to any member — unexpected;"
                                + " check the configured balance service: [t-1]",
                        "partition t-0 alone weighs 10.0, more than the ideal even share 7.5"
                                + " — a perfectly even distribution is impossible",
                        "partitions of 1 topic(s) could only go to a subset of instances:"
                                + " t (1 of 2 instances, weight 15.0)"),
                explanation.reasons());
    }

    @Test
    void debugTableListsEveryMemberWithPartitionWeights() {
        AssignmentExplanation explanation = AssignmentExplanation.explain(
                List.of(
                        member("m-a1", "pod-a", "orders", "payments"),
                        member("m-a2", "pod-a", "orders", "payments"),
                        member("m-b1", "pod-b", "orders", "payments")),
                Map.of(
                        tp("orders", 0), 500.0,
                        tp("orders", 1), 120.0,
                        tp("payments", 0), 30.0),
                0,
                Map.of(
                        "m-a1", List.of(tp("payments", 0), tp("orders", 1)),
                        "m-a2", List.of(),
                        "m-b1", List.of(tp("orders", 0))));

        assertEquals("""
                Load-aware assignment detail [instances=2, members=3, partitions=3]:
                  pod-a/m-a1: load=150.0, partitions=[orders-1=120.0, payments-0=30.0]
                  pod-a/m-a2: load=0.0, partitions=[]
                  pod-b/m-b1: load=500.0, partitions=[orders-0=500.0]""",
                explanation.debugTable());
    }

    @Test
    void fallbackSummaryCountsPartitionsPerInstance() {
        String summary = AssignmentExplanation.fallbackSummary(
                List.of(
                        member("m-a1", "pod-a", "t"),
                        member("m-a2", "pod-a", "t"),
                        member("m-b1", "pod-b", "t")),
                Map.of(
                        "m-a1", List.of(tp("t", 0), tp("t", 1)),
                        "m-a2", List.of(tp("t", 2)),
                        "m-b1", List.of(tp("t", 3))));

        assertEquals(
                "Round-robin fallback distribution (partition weights were ignored;"
                        + " see the preceding warning for the cause):"
                        + " pod-a=3 partitions (2 members), pod-b=1 partition (1 member)",
                summary);
    }

    @Test
    void formatsNumbersDeterministically() {
        assertEquals("1210", AssignmentExplanation.fmt(1210.0));
        assertEquals("1000", AssignmentExplanation.fmt(1000.0));
        assertEquals("350.0", AssignmentExplanation.fmt(350.0));
        assertEquals("1.0", AssignmentExplanation.fmt(1.0));
        assertEquals("0.0", AssignmentExplanation.fmt(0.0));
        assertEquals("0.004", AssignmentExplanation.fmt(0.004));
        assertEquals("0.500", AssignmentExplanation.fmt(0.5));
        assertEquals("-2.5", AssignmentExplanation.fmt(-2.5));
    }

    private static GroupMember member(String memberId, String instanceId, String... topics) {
        return new GroupMember(memberId, instanceId, Set.of(topics));
    }

    private static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }
}
