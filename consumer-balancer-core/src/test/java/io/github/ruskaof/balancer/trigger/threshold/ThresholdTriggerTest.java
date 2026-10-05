package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.LoadAwarePartitionAssignor;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * The trigger compares instance-level loads (members grouped by the instance ids the leader
 * hands back with every assignment) and is deliberately reluctant to fire: only stable groups
 * are judged, the imbalance must persist over several checks, and fires are spaced by a
 * cooldown that grows while they do not help.
 */
class ThresholdTriggerTest {

    private static final String GROUP = "g";
    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);
    private static final TopicPartition T2 = new TopicPartition("t", 2);
    private static final TopicPartition T3 = new TopicPartition("t", 3);

    private final AdminClient adminClient = mock(AdminClient.class);
    private final Clock clock = mock(Clock.class);
    private final Map<TopicPartition, Double> weights = new HashMap<>();
    private final MemberIdTracker memberIdTracker = new MemberIdTracker();
    private final Map<String, String> stubbedInstanceIds = new LinkedHashMap<>();
    private int generation;

    private ThresholdTrigger trigger(RebalanceDamping damping) {
        at(Duration.ZERO);
        return new ThresholdTrigger(adminClient, GROUP, memberIdTracker, partitions -> weights, 1.1,
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
    void firesOnInstanceImbalanceInvisibleAtMemberLevel() {
        // Every member carries the load it would carry under a member-level optimum, but
        // both heavy partitions sit in instance i1.
        stubGroup(
                member("m1", "i1", T0),
                member("m2", "i1", T1),
                member("m3", "i2", T2),
                member("m4", "i2", T3));
        weigh(T0, 10.0, T1, 10.0, T2, 1.0, T3, 1.0);

        assertTrue(eagerTrigger().shouldTrigger(),
                "i1 carries 20 while the optimal instance max is 11");
    }

    @Test
    void groupsByInstanceIdRatherThanHost() {
        // Two pods behind one address (hostNetwork, NAT) are two instances: nothing here
        // even looks at MemberDescription.host(), which the mocks deliberately never stub.
        stubGroup(
                member("m1", "i1", T0),
                member("m2", "i2", T1));
        weigh(T0, 10.0, T1, 10.0);

        assertFalse(eagerTrigger().shouldTrigger(),
                "10 each: grouping the two into one instance would see 20 against an optimum of 10 and fire");
    }

    @Test
    void doesNotFireWhenInstancesAreBalancedDespiteIdleMembers() {
        // More members than partitions: two members idle, but each instance carries the same load.
        stubGroup(
                member("m1", "i1", T0),
                member("m2", "i1"),
                member("m3", "i2", T1),
                member("m4", "i2"));
        weigh(T0, 10.0, T1, 10.0);

        assertFalse(eagerTrigger().shouldTrigger());
    }

    @Test
    void usesActualSubscriptionsRatherThanAnUnreachableCrossTopicOptimum() {
        TopicPartition other = new TopicPartition("other", 0);
        stubGroup(member("m1", "i1", T0, T1), member("m2", "i2", other));
        weigh(T0, 10.0, T1, 10.0);
        weights.put(other, 1.0);
        var topology = new MonitoringProtocol.Assignment(UUID.randomUUID(), "m:m1", "i1", Set.of("t"), 0,
                Map.of("m:m1", new MonitoringProtocol.Member("i1", Set.of("t")),
                        "m:m2", new MonitoringProtocol.Member("i2", Set.of("other"))));
        memberIdTracker.onAssignment(GROUP, ++generation, null, "m1", "m:m1", "i1", Set.of("t"),
                MonitoringProtocol.assignment(topology));

        ThresholdTrigger trigger = eagerTrigger();
        assertFalse(trigger.shouldTrigger(), "moving t partitions to m2 is impossible under its subscription");
        assertEquals(1.0, trigger.status().lastRatio());
        assertEquals(1, trigger.status().evaluations(ThresholdTrigger.EvaluationOutcome.BALANCED));
    }

    @Test
    void fiveHundredStaticMembersAcrossOneHundredInstancesCanTriggerFromTheAssignorsTopology() {
        final int count = 500;
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();
        List<MemberDescription> live = new ArrayList<>();
        List<PartitionInfo> partitionMetadata = new ArrayList<>();
        Set<TopicPartition> allPartitions = new java.util.HashSet<>();
        Node broker = new Node(0, "localhost", 9092);
        for (int i = 0; i < count; i++) {
            TopicPartition partition = new TopicPartition("t", i);
            allPartitions.add(partition);
            weights.put(partition, 1.0);
            partitionMetadata.add(new PartitionInfo("t", i, broker, new Node[]{broker}, new Node[]{broker}));
            String memberId = String.format("member-%04d", i);
            String staticId = String.format("consumer-%04d", i);
            var subscription = new ConsumerPartitionAssignor.Subscription(List.of("t"),
                    MonitoringProtocol.subscription(String.format("instance-%04d", i / 5), true, 0));
            subscription.setGroupInstanceId(Optional.of(staticId));
            subscriptions.put(memberId, subscription);
        }
        for (int i = 0; i < count; i++) {
            live.add(new MemberDescription(String.format("member-%04d", i),
                    Optional.of(String.format("consumer-%04d", i)), "client", "host",
                    new MemberAssignment(i == 0 ? allPartitions : Set.of())));
        }
        stubGroup(live.toArray(MemberDescription[]::new));
        LoadAwarePartitionAssignor assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(LoadAwarePartitionAssignor.LoadAwareAssignorConfig.WEIGHT_SERVICE,
                (WeightService) partitions -> weights));
        var assignment = assignor.assign(new Cluster("c", List.of(broker), partitionMetadata, Set.of(), Set.of()),
                new ConsumerPartitionAssignor.GroupSubscription(subscriptions));
        var delivered = assignment.groupAssignment().get("member-0000").userData();
        assertNotNull(delivered, "the required scale must not disable proactive monitoring");
        memberIdTracker.onAssignment(GROUP, ++generation, null, "member-0000", "s:consumer-0000",
                "instance-0000", Set.of("t"), delivered);

        ThresholdTrigger trigger = eagerTrigger();
        assertTrue(trigger.shouldTrigger());
        assertEquals(500, trigger.status().lastMemberCount());
        assertEquals(100, trigger.status().lastInstanceCount());
        assertEquals(500, trigger.status().lastPartitionCount());
        assertEquals(100.0, trigger.status().lastRatio());
    }

    @Test
    void skipsTheCheckWhenAMemberHasNoKnownInstance() {
        // Mid rolling upgrade: the leader is on a version that sends no mapping, so m2 is
        // unmapped. Counting it as its own instance would compare the group against an
        // optimum the assignor would never produce and fire on every check.
        stubGroup(
                member("m1", "i1", T0, T1),
                memberWithoutInstanceId("m2"));
        weighImbalanced();
        ThresholdTrigger trigger = eagerTrigger();

        assertFalse(trigger.shouldTrigger());
        assertFalse(trigger.shouldTrigger(), "and keeps skipping rather than converging on a fire");
        assertEquals(2, trigger.status().evaluations(ThresholdTrigger.EvaluationOutcome.INSTANCES_UNKNOWN));
    }

    @Test
    void resumesCheckingOnceEveryMemberIsMapped() {
        stubGroup(
                member("m1", "i1", T0, T1),
                memberWithoutInstanceId("m2"));
        weighImbalanced();
        ThresholdTrigger trigger = eagerTrigger();
        assertFalse(trigger.shouldTrigger());

        stubImbalancedGroup();
        assertTrue(trigger.shouldTrigger(), "the mapping now covers both members");
    }

    @Test
    void doesNotLetTheSkipDisturbTheViolationStreak() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");

        stubGroup(
                member("m1", "i1", T0, T1),
                memberWithoutInstanceId("m2"));
        assertFalse(trigger.shouldTrigger(), "not judged at all");

        stubImbalancedGroup();
        assertTrue(trigger.shouldTrigger(), "2 of 2 checks: an unjudged check does not reset the streak");
    }

    @Test
    void doesNotFireWithoutMembers() {
        stubGroup();

        assertFalse(eagerTrigger().shouldTrigger());
    }

    @Test
    void doesNotFireWhenAllWeightsAreZero() {
        stubImbalancedGroup();
        weigh(T0, 0.0, T1, 0.0);

        assertFalse(eagerTrigger().shouldTrigger());
    }

    @Test
    void sanitizesWeightsBeforeComparingLoads() {
        stubGroup(
                member("m1", "i1", T0, T1, T2),
                member("m2", "i2"));
        // NaN (T0) and a missing entry (T1) must fall back to the default weight instead of
        // poisoning the load sums.
        weigh(T0, Double.NaN, T2, 1.0);

        assertTrue(eagerTrigger().shouldTrigger(),
                "with T0 and T1 defaulted to 1.0, i1 carries 3 while the optimum is 2");
    }

    @Test
    void doesNotJudgeAGroupWithoutAnyUsableWeight() {
        // A freshly elected coordinator's weight store has no history for the group yet:
        // every weight defaults, and the loads would merely count partitions.
        stubImbalancedGroup();
        weights.clear();

        assertFalse(eagerTrigger().shouldTrigger(), "i1 carries 2 partitions to i2's none");
    }

    @Test
    void aCheckWithoutUsableWeightsNeitherCountsTowardsNorResetsTheStreak() {
        stubImbalancedGroup();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        weighImbalanced();
        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");
        weights.clear();
        assertFalse(trigger.shouldTrigger(), "skipped");
        weighImbalanced();

        assertTrue(trigger.shouldTrigger(), "2 of 2 checks");
    }

    @Test
    void doesNotJudgeARebalancingGroup() {
        // Mid-rebalance the admin API reports stale, partial or empty assignments; firing on
        // those would re-fire on the rebalance this trigger just caused.
        stubGroup(GroupState.PREPARING_REBALANCE,
                member("m1", "i1", T0, T1),
                member("m2", "i2"));
        weighImbalanced();

        assertFalse(eagerTrigger().shouldTrigger());
    }

    @Test
    void doesNotJudgeAGroupWhoseStateTheBrokerDidNotReport() {
        stubGroup((GroupState) null,
                member("m1", "i1", T0, T1),
                member("m2", "i2"));
        weighImbalanced();

        assertFalse(eagerTrigger().shouldTrigger());
    }

    @Test
    void needsTheImbalanceOnSeveralChecksOfOneAssignment() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(3, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 3 checks");
        assertFalse(trigger.shouldTrigger(), "2 of 3 checks");
        assertTrue(trigger.shouldTrigger(), "3 of 3 checks: the imbalance is not a single noisy sample");
    }

    @Test
    void restartsTheStreakWhenTheAssignmentChanges() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");

        // Something moved: the streak describes a different assignment now.
        stubGroup(
                member("m1", "i1", T0, T1),
                member("m2", "i2", T2));
        assertFalse(trigger.shouldTrigger(), "1 of 2 checks on the new assignment");
        assertTrue(trigger.shouldTrigger(), "2 of 2 checks on the new assignment");
    }

    @Test
    void decaysRatherThanResetsTheStreakOnACheckWithinThreshold() {
        // While the weight window still spans the load that has just been replaced, the ratio
        // drifts across the threshold. Resetting on every dip would restart the count exactly
        // when the trigger is most needed, so a balanced check only walks it back one step.
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(3, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 3 checks");
        assertFalse(trigger.shouldTrigger(), "2 of 3 checks");

        stubBalancedGroup();
        assertFalse(trigger.shouldTrigger(), "within threshold: the count decays to 1");

        stubImbalancedGroup();
        assertFalse(trigger.shouldTrigger(), "2 of 3 checks");
        assertTrue(trigger.shouldTrigger(), "3 of 3 checks");
    }

    @Test
    void windsTheStreakDownWhenTheGroupStaysBalanced() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");

        stubBalancedGroup();
        assertFalse(trigger.shouldTrigger(), "the count decays to 0");
        assertFalse(trigger.shouldTrigger(), "and stays there");

        stubImbalancedGroup();
        assertFalse(trigger.shouldTrigger(), "1 of 2 checks again");
        assertTrue(trigger.shouldTrigger(), "2 of 2 checks");
    }

    @Test
    void keepsTheStreakAcrossChecksThatFindTheGroupRebalancing() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(2, Duration.ZERO, Duration.ZERO));

        assertFalse(trigger.shouldTrigger(), "1 of 2 checks");

        stubGroup(GroupState.COMPLETING_REBALANCE,
                member("m1", "i1", T0, T1),
                member("m2", "i2"));
        assertFalse(trigger.shouldTrigger(), "not judged at all");

        stubImbalancedGroup();
        assertTrue(trigger.shouldTrigger(), "2 of 2 checks: a passing rebalance does not reset the streak");
    }

    @Test
    void spacesFiresByTheCooldown() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger(), "first violation fires");

        at(Duration.ofMinutes(9));
        assertFalse(trigger.shouldTrigger(), "still in the cooldown, whether or not the assignment changed");

        at(Duration.ofMinutes(10));
        assertTrue(trigger.shouldTrigger(), "the cooldown has passed");
    }

    @Test
    void doublesTheCooldownWhileFiresDoNotRestoreBalance() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger());

        at(Duration.ofMinutes(10));
        assertTrue(trigger.shouldTrigger(), "the imbalance survived the first rebalance");

        at(Duration.ofMinutes(25));
        assertFalse(trigger.shouldTrigger(), "the cooldown doubled to 20m, so 15m is not enough");

        at(Duration.ofMinutes(30));
        assertTrue(trigger.shouldTrigger());

        at(Duration.ofMinutes(65));
        assertFalse(trigger.shouldTrigger(), "the cooldown doubled to 40m");

        at(Duration.ofMinutes(70));
        assertTrue(trigger.shouldTrigger());
    }

    @Test
    void windsTheCooldownBackOnceTheGroupIsBalancedAgain() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger());
        at(Duration.ofMinutes(10));
        assertTrue(trigger.shouldTrigger(), "the cooldown doubles to 20m");

        stubBalancedGroup();
        at(Duration.ofMinutes(11));
        assertFalse(trigger.shouldTrigger(), "balanced: the cooldown returns to 10m");

        stubImbalancedGroup();
        at(Duration.ofMinutes(21));
        assertTrue(trigger.shouldTrigger(), "a fresh imbalance waits 10m, not the backed-off 20m");
    }

    @Test
    void neverStretchesTheCooldownBeyondItsMaximum() {
        stubImbalancedGroup();
        weighImbalanced();
        ThresholdTrigger trigger = trigger(new RebalanceDamping(1, Duration.ofHours(1), Duration.ofHours(2)));

        assertTrue(trigger.shouldTrigger());
        at(Duration.ofHours(1));
        assertTrue(trigger.shouldTrigger(), "the cooldown doubles to the 2h maximum");
        at(Duration.ofHours(3));
        assertTrue(trigger.shouldTrigger());

        at(Duration.ofMinutes(299));
        assertFalse(trigger.shouldTrigger(), "2h after the last fire has not elapsed yet");
        at(Duration.ofHours(5));
        assertTrue(trigger.shouldTrigger(), "the cooldown stays at 2h instead of doubling to 4h");
    }

    /** i1 carries both heavy partitions while i2 idles: ratio 2.0. */
    private void stubImbalancedGroup() {
        stubGroup(
                member("m1", "i1", T0, T1),
                member("m2", "i2"));
    }

    /** One heavy partition per instance: ratio 1.0. */
    private void stubBalancedGroup() {
        stubGroup(
                member("m1", "i1", T0),
                member("m2", "i2", T1));
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

    private void weigh(TopicPartition tp1, double w1, TopicPartition tp2, double w2,
            TopicPartition tp3, double w3, TopicPartition tp4, double w4) {
        weigh(tp1, w1, tp2, w2);
        weights.put(tp3, w3);
        weights.put(tp4, w4);
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
        // What the leader would have sent back with the assignment this group is now on.
        memberIdTracker.recordInstanceIds(GROUP, ++generation, stubbedInstanceIds);
        stubbedInstanceIds.clear();
    }

    /** A member of {@code instanceId}, which the next {@link #stubGroup} tells the tracker about. */
    private MemberDescription member(String consumerId, String instanceId, TopicPartition... partitions) {
        stubbedInstanceIds.put(consumerId, instanceId);
        return memberWithoutInstanceId(consumerId, partitions);
    }

    /** A member the leader's mapping does not cover — an older version, say. */
    private static MemberDescription memberWithoutInstanceId(String consumerId, TopicPartition... partitions) {
        MemberDescription member = mock(MemberDescription.class);
        when(member.consumerId()).thenReturn(consumerId);
        when(member.assignment()).thenReturn(new MemberAssignment(Set.of(partitions)));
        return member;
    }
}
