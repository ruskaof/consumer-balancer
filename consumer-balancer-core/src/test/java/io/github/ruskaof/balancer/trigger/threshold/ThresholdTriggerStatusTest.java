package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * The status snapshot republishes, after every evaluation, the values the trigger otherwise
 * only logs: the imbalance ratio against the threshold, the storm-guard state, and monotonic
 * evaluation totals by outcome — including the errors {@code shouldTrigger()} swallows.
 */
class ThresholdTriggerStatusTest {

    private static final String GROUP = "g";
    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);

    private final AdminClient adminClient = mock(AdminClient.class);
    private final Clock clock = mock(Clock.class);
    private final Map<TopicPartition, Double> weights = new HashMap<>();

    private ThresholdTrigger trigger(RebalanceDamping damping) {
        at(Duration.ZERO);
        return new ThresholdTrigger(adminClient, GROUP, partitions -> weights, 1.1,
                new SortingRoundRobinBalanceService(), damping, clock);
    }

    /** Fires on the ratio alone, with hysteresis and cooldown out of the way. */
    private ThresholdTrigger eagerTrigger() {
        return trigger(RebalanceDamping.none());
    }

    private void at(Duration sinceStart) {
        when(clock.instant()).thenReturn(Instant.EPOCH.plus(sinceStart));
    }

    @Test
    void initialStatusReportsTheConfigurationAndNoEvaluations() {
        ThresholdTrigger.Status status =
                trigger(new RebalanceDamping(2, Duration.ofMinutes(10), Duration.ofHours(2))).status();

        assertEquals(1.1, status.threshold());
        assertTrue(Double.isNaN(status.lastRatio()));
        assertTrue(Double.isNaN(status.lastCurrentMaxLoad()));
        assertTrue(Double.isNaN(status.lastOptimalMaxLoad()));
        assertEquals(0, status.lastMemberCount());
        assertEquals(0, status.lastInstanceCount());
        assertEquals(0, status.lastPartitionCount());
        assertEquals(0, status.lastDefaultedWeightCount());
        assertEquals(0, status.violatedChecks());
        assertEquals(Duration.ofMinutes(10), status.effectiveCooldown());
        assertNull(status.lastFiredAt());
        assertEquals(0, status.evaluationCount());
        assertEquals(0, status.evaluationTimeNanos());
        for (EvaluationOutcome outcome : EvaluationOutcome.values()) {
            assertEquals(0, status.evaluations(outcome), outcome.name());
        }
    }

    @Test
    void balancedEvaluationRecordsRatioLoadsAndGroupObservations() {
        stubBalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = eagerTrigger();

        assertFalse(trigger.shouldTrigger());

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(1, status.evaluations(EvaluationOutcome.BALANCED));
        assertEquals(1, status.evaluationCount());
        assertEquals(1.0, status.lastRatio());
        assertEquals(10.0, status.lastCurrentMaxLoad());
        assertEquals(10.0, status.lastOptimalMaxLoad());
        assertEquals(2, status.lastMemberCount());
        assertEquals(2, status.lastInstanceCount());
        assertEquals(2, status.lastPartitionCount());
        assertEquals(0, status.lastDefaultedWeightCount());
    }

    @Test
    void hysteresisThenFireAreCountedAndTimestamped() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");
        assertEquals(1, trigger.status().evaluations(EvaluationOutcome.AWAITING_HYSTERESIS));
        assertEquals(1, trigger.status().violatedChecks());
        assertEquals(2.0, trigger.status().lastRatio());

        at(Duration.ofMinutes(1));
        assertTrue(trigger.shouldTrigger(), "2 of 2 checks");

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(1, status.evaluations(EvaluationOutcome.FIRED));
        assertEquals(Instant.EPOCH.plus(Duration.ofMinutes(1)), status.lastFiredAt());
        assertEquals(0, status.violatedChecks(), "a fire consumes the streak");
    }

    @Test
    void cooldownSuppressedChecksAreCounted() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger());
        at(Duration.ofMinutes(5));
        assertFalse(trigger.shouldTrigger(), "still in the cooldown");

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(1, status.evaluations(EvaluationOutcome.COOLDOWN_SUPPRESSED));
        assertEquals(Duration.ofMinutes(10), status.effectiveCooldown());
    }

    @Test
    void ineffectiveFireDoublesTheEffectiveCooldown() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger());
        at(Duration.ofMinutes(10));
        assertTrue(trigger.shouldTrigger(), "the imbalance survived the first rebalance");

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(2, status.evaluations(EvaluationOutcome.FIRED));
        assertEquals(Duration.ofMinutes(20), status.effectiveCooldown());
    }

    @Test
    void notStableChecksAreCountedAndKeepTheLastRatio() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = eagerTrigger();
        assertTrue(trigger.shouldTrigger());

        stubGroup(GroupState.PREPARING_REBALANCE,
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        assertFalse(trigger.shouldTrigger());

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(1, status.evaluations(EvaluationOutcome.GROUP_NOT_STABLE));
        assertEquals(2.0, status.lastRatio(), "a skipped check keeps the last computed ratio");
    }

    @Test
    void groupsWithoutMembersAreCounted() {
        stubGroup();
        ThresholdTrigger trigger = eagerTrigger();

        assertFalse(trigger.shouldTrigger());

        assertEquals(1, trigger.status().evaluations(EvaluationOutcome.NO_MEMBERS));
    }

    @Test
    void allZeroWeightsCountAsBalancedWithAnUndefinedRatio() {
        stubImbalancedGroup();
        weigh(T0, 0.0, T1, 0.0);
        ThresholdTrigger trigger = eagerTrigger();

        assertFalse(trigger.shouldTrigger());

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(1, status.evaluations(EvaluationOutcome.BALANCED));
        assertTrue(Double.isNaN(status.lastRatio()), "zero optimal load leaves the ratio undefined");
        assertEquals(0.0, status.lastOptimalMaxLoad());
    }

    @Test
    void partitionsWithDefaultedWeightsAreReported() {
        stubImbalancedGroup();
        // NaN and a missing entry both fall back to the default weight.
        weigh(T0, Double.NaN);
        ThresholdTrigger trigger = eagerTrigger();

        trigger.shouldTrigger();

        assertEquals(2, trigger.status().lastDefaultedWeightCount());
    }

    @Test
    void swallowedEvaluationErrorsAreCounted() {
        when(adminClient.describeConsumerGroups(List.of(GROUP))).thenThrow(new RuntimeException("boom"));
        ThresholdTrigger trigger = eagerTrigger();

        assertFalse(trigger.shouldTrigger());

        assertEquals(1, trigger.status().evaluations(EvaluationOutcome.ERROR));
        assertEquals(1, trigger.status().evaluationCount());
    }

    @Test
    void evaluationTotalsAccumulateAcrossOutcomes() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "awaiting hysteresis");
        assertTrue(trigger.shouldTrigger(), "fired");
        stubBalancedGroup();
        assertFalse(trigger.shouldTrigger(), "balanced");

        ThresholdTrigger.Status status = trigger.status();
        assertEquals(3, status.evaluationCount());
        long outcomeSum = 0;
        for (EvaluationOutcome outcome : EvaluationOutcome.values()) {
            outcomeSum += status.evaluations(outcome);
        }
        assertEquals(3, outcomeSum, "every evaluation ends in exactly one outcome");
        assertTrue(status.evaluationTimeNanos() >= 0);
    }

    /** h1 carries both heavy partitions while h2 idles: ratio 2.0. */
    private void stubImbalancedGroup() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
    }

    /** One heavy partition per host: ratio 1.0. */
    private void stubBalancedGroup() {
        stubGroup(
                member("m1", "h1", T0),
                member("m2", "h2", T1));
    }

    private void weighImbalanced() {
        weigh(T0, 10.0, T1, 10.0);
    }

    private void weigh(TopicPartition tp, double weight) {
        weights.clear();
        weights.put(tp, weight);
    }

    private void weigh(TopicPartition tp1, double w1, TopicPartition tp2, double w2) {
        weigh(tp1, w1);
        weights.put(tp2, w2);
    }

    private void stubGroup(MemberDescription... members) {
        stubGroup(GroupState.STABLE, members);
    }

    private void stubGroup(GroupState state, MemberDescription... members) {
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.members()).thenReturn(List.of(members));
        when(description.groupState()).thenReturn(state);
        KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        Map<String, KafkaFuture<ConsumerGroupDescription>> futures = Map.of(GROUP, future);
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.describedGroups()).thenReturn(futures);
        when(adminClient.describeConsumerGroups(List.of(GROUP))).thenReturn(result);
    }

    private static MemberDescription member(String consumerId, String host, TopicPartition... partitions) {
        MemberDescription member = mock(MemberDescription.class);
        when(member.consumerId()).thenReturn(consumerId);
        when(member.host()).thenReturn(host);
        when(member.assignment()).thenReturn(new MemberAssignment(Set.of(partitions)));
        return member;
    }
}
