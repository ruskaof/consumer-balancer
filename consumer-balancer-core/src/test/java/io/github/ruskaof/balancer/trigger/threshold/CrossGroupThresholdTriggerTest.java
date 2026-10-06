package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.CrossGroupLoadService;
import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.AWAITING_HYSTERESIS;
import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.BALANCED;
import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.CROSS_GROUP_LOAD_UNAVAILABLE;
import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.FIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CrossGroupThresholdTriggerTest {

    private static final String GROUP = "orders";
    private static final TopicPartition T0 = new TopicPartition("orders", 0);
    private static final TopicPartition T1 = new TopicPartition("orders", 1);
    private static final TopicPartition BACKGROUND = new TopicPartition("payments", 0);
    private static final Set<String> INSTANCES = Set.of("i1", "i2");
    private static final List<GroupMember> MEMBERS = List.of(
            new GroupMember("m1", "i1", Set.of("orders")),
            new GroupMember("m2", "i2", Set.of("orders")));

    private final AdminClient admin = mock(AdminClient.class);
    private final MemberIdTracker tracker = mock(MemberIdTracker.class);
    private final CrossGroupLoadService otherGroups = mock(CrossGroupLoadService.class);

    @Test
    void locallyBalancedGroupTriggersWhenItsPartitionsCanImproveCombinedLoad() throws Exception {
        group(Map.of("m1", List.of(T0), "m2", List.of(T1)));
        background(Map.of("i1", 20.0));
        ThresholdTrigger trigger = trigger(RebalanceDamping.none());

        assertTrue(trigger.shouldTrigger());
        assertEquals(30.0, trigger.status().lastCurrentMaxLoad());
        assertEquals(20.0, trigger.status().lastOptimalMaxLoad());
        assertEquals(1.5, trigger.status().lastRatio());
        assertEquals(1, trigger.status().evaluations(FIRED));
    }

    @Test
    void locallySkewedGroupDoesNotTriggerWhenCombinedLoadIsBalanced() throws Exception {
        group(Map.of("m1", List.of(), "m2", List.of(T0, T1)));
        background(Map.of("i1", 20.0));
        ThresholdTrigger trigger = trigger(RebalanceDamping.none());

        assertFalse(trigger.shouldTrigger());
        assertEquals(20.0, trigger.status().lastCurrentMaxLoad());
        assertEquals(20.0, trigger.status().lastOptimalMaxLoad());
        assertEquals(1.0, trigger.status().lastRatio());
        assertEquals(1, trigger.status().evaluations(BALANCED));
    }

    @Test
    void unavailableOtherGroupSnapshotSkipsTheEvaluation() throws Exception {
        group(Map.of("m1", List.of(T0, T1), "m2", List.of()));
        when(otherGroups.snapshot(GROUP, INSTANCES)).thenReturn(null);
        ThresholdTrigger trigger = trigger(RebalanceDamping.none());

        assertFalse(trigger.shouldTrigger());
        assertEquals(1, trigger.status().evaluations(CROSS_GROUP_LOAD_UNAVAILABLE));
        assertEquals(0, trigger.status().evaluations(FIRED));
        assertEquals(0, trigger.status().violatedChecks());
        assertTrue(Double.isNaN(trigger.status().lastRatio()));
    }

    @Test
    void addsTheSameBaselineToCurrentAndOptimalLoadsOnlyOncePerInstance() throws Exception {
        group(Map.of("m1", List.of(T0), "m2", List.of(), "m3", List.of(T1)), List.of(
                MEMBERS.get(0), MEMBERS.get(1),
                new GroupMember("m3", "i1", Set.of("orders"))));
        background(Map.of("i1", 100.0, "i2", 100.0));
        ThresholdTrigger trigger = trigger(RebalanceDamping.none());

        assertFalse(trigger.shouldTrigger(), "the combined improvement is below the 1.1 threshold");
        assertEquals(120.0, trigger.status().lastCurrentMaxLoad());
        assertEquals(110.0, trigger.status().lastOptimalMaxLoad());
        assertEquals(120.0 / 110.0, trigger.status().lastRatio());
        assertEquals(3, trigger.status().lastMemberCount());
        assertEquals(2, trigger.status().lastInstanceCount());
        verify(otherGroups).snapshot(GROUP, INSTANCES);
    }

    @Test
    void doesNotTriggerWhenPinnedBackgroundLoadMakesTheCombinedSkewUnavoidable() throws Exception {
        group(Map.of("m1", List.of(), "m2", List.of(T0, T1)));
        background(Map.of("i1", 100.0));
        ThresholdTrigger trigger = trigger(RebalanceDamping.none());

        assertFalse(trigger.shouldTrigger(), "moving this group's partitions cannot reduce the max of 100");
        assertEquals(100.0, trigger.status().lastCurrentMaxLoad());
        assertEquals(100.0, trigger.status().lastOptimalMaxLoad());
        assertEquals(1.0, trigger.status().lastRatio());
        assertEquals(1, trigger.status().evaluations(BALANCED));
    }

    @Test
    void backgroundAssignmentChangeRestartsHysteresisEvenWhenItsLoadIsUnchanged() throws Exception {
        group(Map.of("m1", List.of(T0), "m2", List.of(T1)));
        var before = new CrossGroupLoadService.Snapshot(Map.of("i1", 20.0),
                Map.of("payments", Map.of("payment-member-a", Set.of(BACKGROUND))));
        var after = new CrossGroupLoadService.Snapshot(Map.of("i1", 20.0),
                Map.of("payments", Map.of("payment-member-b", Set.of(BACKGROUND))));
        when(otherGroups.snapshot(GROUP, INSTANCES)).thenReturn(before, after, after);
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger());
        assertEquals(1, trigger.status().violatedChecks());
        assertFalse(trigger.shouldTrigger(), "the changed background assignment starts a new streak");
        assertEquals(1, trigger.status().violatedChecks());
        assertTrue(trigger.shouldTrigger(), "two checks now agree on the new background assignment");
        assertEquals(2, trigger.status().evaluations(AWAITING_HYSTERESIS));
        assertEquals(1, trigger.status().evaluations(FIRED));
    }

    @Test
    void backgroundTopologyChangeRestartsHysteresisWithTheSameMemberAssignments() throws Exception {
        group(Map.of("m1", List.of(T0), "m2", List.of(T1)));
        var assignments = Map.of("payments", Map.of("payment-member", Set.of(BACKGROUND)));
        var before = new CrossGroupLoadService.Snapshot(Map.of("i1", 20.0), assignments,
                Map.of("payments", new MemberIdTracker.SnapshotToken(3, new UUID(0, 1))));
        var after = new CrossGroupLoadService.Snapshot(Map.of("i1", 20.0), assignments,
                Map.of("payments", new MemberIdTracker.SnapshotToken(4, new UUID(0, 2))));
        when(otherGroups.snapshot(GROUP, INSTANCES)).thenReturn(before, after, after);
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger());
        assertEquals(1, trigger.status().violatedChecks());
        assertFalse(trigger.shouldTrigger(), "a new topology resets the streak even if partitions did not move");
        assertEquals(1, trigger.status().violatedChecks());
        assertTrue(trigger.shouldTrigger(), "two checks now observe the same background topology");
        assertEquals(2, trigger.status().evaluations(AWAITING_HYSTERESIS));
        assertEquals(1, trigger.status().evaluations(FIRED));
    }

    private ThresholdTrigger trigger(RebalanceDamping damping) {
        return new ThresholdTrigger(admin, GROUP, tracker, partitions -> Map.of(T0, 10.0, T1, 10.0),
                1.1, new SortingRoundRobinBalanceService(), damping,
                Clock.fixed(Instant.EPOCH, ZoneOffset.UTC), otherGroups);
    }

    private void background(Map<String, Double> loads) throws Exception {
        when(otherGroups.snapshot(GROUP, INSTANCES)).thenReturn(
                new CrossGroupLoadService.Snapshot(loads,
                        Map.of("payments", Map.of("payment-member", Set.of(BACKGROUND)))));
    }

    private void group(Map<String, List<TopicPartition>> assignment) {
        group(assignment, MEMBERS);
    }

    private void group(Map<String, List<TopicPartition>> assignment, List<GroupMember> topology) {
        List<MemberDescription> members = new ArrayList<>();
        for (var entry : assignment.entrySet()) {
            MemberDescription member = mock(MemberDescription.class);
            when(member.consumerId()).thenReturn(entry.getKey());
            when(member.assignment()).thenReturn(new MemberAssignment(Set.copyOf(entry.getValue())));
            members.add(member);
        }
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.members()).thenReturn(members);
        when(description.groupState()).thenReturn(GroupState.STABLE);
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.describedGroups()).thenReturn(Map.of(GROUP, KafkaFuture.completedFuture(description)));
        when(admin.describeConsumerGroups(List.of(GROUP))).thenReturn(result);
        when(tracker.resolveMembers(GROUP, members)).thenReturn(topology);
    }
}
