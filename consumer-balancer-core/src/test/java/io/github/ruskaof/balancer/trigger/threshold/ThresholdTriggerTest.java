package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * The trigger compares instance-level loads (members grouped by broker-observed host) and
 * suppresses re-firing on an assignment it already fired on, so a grouping disagreement with
 * the assignor cannot loop rebalances.
 */
class ThresholdTriggerTest {

    private static final String GROUP = "g";
    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);
    private static final TopicPartition T2 = new TopicPartition("t", 2);
    private static final TopicPartition T3 = new TopicPartition("t", 3);

    private final AdminClient adminClient = mock(AdminClient.class);
    private final Clock clock = mock(Clock.class);

    private ThresholdTrigger trigger(Map<TopicPartition, Double> weights, Duration refireSuppression) {
        when(clock.instant()).thenReturn(Instant.EPOCH);
        return new ThresholdTrigger(adminClient, GROUP, partitions -> weights, 1.1,
                new SortingRoundRobinBalanceService(), refireSuppression, clock);
    }

    @Test
    void firesOnInstanceImbalanceInvisibleAtMemberLevel() {
        // Every member carries the load it would carry under a member-level optimum, but
        // both heavy partitions sit on host h1.
        stubGroup(
                member("m1", "h1", T0),
                member("m2", "h1", T1),
                member("m3", "h2", T2),
                member("m4", "h2", T3));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0, T2, 1.0, T3, 1.0);

        assertTrue(trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger(),
                "h1 carries 20 while the optimal instance max is 11");
    }

    @Test
    void doesNotFireWhenInstancesAreBalancedDespiteIdleMembers() {
        // More members than partitions: two members idle, but each instance carries the same load.
        stubGroup(
                member("m1", "h1", T0),
                member("m2", "h1"),
                member("m3", "h2", T1),
                member("m4", "h2"));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0);

        assertFalse(trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger());
    }

    @Test
    void membersWithBlankHostCountAsTheirOwnInstances() {
        stubGroup(
                member("m1", "", T0, T1),
                member("m2", ""));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0);

        assertTrue(trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger(),
                "with singleton instances, m1 carries 20 while the optimum is 10 per member");
    }

    @Test
    void doesNotFireWithoutMembers() {
        stubGroup();

        assertFalse(trigger(Map.of(), ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger());
    }

    @Test
    void doesNotFireWhenAllWeightsAreZero() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        Map<TopicPartition, Double> weights = Map.of(T0, 0.0, T1, 0.0);

        assertFalse(trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger());
    }

    @Test
    void suppressesRefireWhileTheFiredAssignmentStaysUnchanged() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0);
        ThresholdTrigger trigger = trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION);

        assertTrue(trigger.shouldTrigger(), "first violation fires");
        assertFalse(trigger.shouldTrigger(),
                "the assignment did not change after the rebalance we initiated: suppress");

        // The rebalance actually moved something (still violating): the same trigger fires again.
        // T2 has no weight entry and defaults to 1.0.
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2", T2));
        assertTrue(trigger.shouldTrigger(),
                "a different assignment fingerprint must not be suppressed");
    }

    @Test
    void refiresOnTheUnchangedAssignmentOnceTheSuppressionWindowPasses() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0);
        ThresholdTrigger trigger = trigger(weights, Duration.ofMinutes(10));

        assertTrue(trigger.shouldTrigger());
        assertFalse(trigger.shouldTrigger(), "within the window: suppressed");

        when(clock.instant()).thenReturn(Instant.EPOCH.plus(Duration.ofMinutes(11)));
        assertTrue(trigger.shouldTrigger(), "after the window: allowed to retry");
    }

    @Test
    void zeroSuppressionWindowDisablesSuppression() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        Map<TopicPartition, Double> weights = Map.of(T0, 10.0, T1, 10.0);
        ThresholdTrigger trigger = trigger(weights, Duration.ZERO);

        assertTrue(trigger.shouldTrigger());
        assertTrue(trigger.shouldTrigger());
    }

    @Test
    void sanitizesWeightsBeforeComparingLoads() {
        stubGroup(
                member("m1", "h1", T0, T1),
                member("m2", "h2"));
        // NaN and a missing entry must fall back to the default weight instead of
        // poisoning the load sums.
        Map<TopicPartition, Double> weights = Map.of(T0, Double.NaN);

        assertTrue(trigger(weights, ThresholdTrigger.DEFAULT_REFIRE_SUPPRESSION).shouldTrigger(),
                "with both partitions defaulted to 1.0, h1 carries 2 while the optimum is 1");
    }

    private void stubGroup(MemberDescription... members) {
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.members()).thenReturn(List.of(members));
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
