package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.BALANCED;
import static io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger.EvaluationOutcome.CROSS_GROUP_LOAD_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class CrossGroupCoordinationTest {
    private static final String FIRST = "orders";
    private static final String SECOND = "payments";
    private static final Set<String> INSTANCES = Set.of("pod-a", "pod-b");

    private final AdminClient admin = mock(AdminClient.class);
    private final MemberIdTracker tracker = new MemberIdTracker();
    private final Map<String, ConsumerGroupDescription> descriptions = new ConcurrentHashMap<>();
    private final WeightService weights = partitions -> {
        Map<TopicPartition, Double> result = new HashMap<>();
        partitions.forEach(partition -> result.put(partition, partition.partition() == 0 ? 100.0 : 1.0));
        return result;
    };
    private final CrossGroupLoadService loads = new CrossGroupLoadService(admin, tracker, weights,
            () -> List.of(FIRST, SECOND));

    @BeforeEach
    void groupsStartWithBothHeavyPartitionsOnTheSameInstance() {
        when(admin.describeConsumerGroups(anyCollection())).thenAnswer(invocation -> {
            Collection<String> groups = invocation.getArgument(0);
            Map<String, KafkaFuture<ConsumerGroupDescription>> futures = new HashMap<>();
            groups.forEach(group -> futures.put(group, KafkaFuture.completedFuture(descriptions.get(group))));
            DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
            when(result.describedGroups()).thenReturn(futures);
            return result;
        });
        assign(FIRST, false, 1);
        assign(SECOND, false, 1);
    }

    @Test
    void secondGroupWaitsForTheFirstCorrectionAndThenObservesBalancedCombinedLoad() throws Exception {
        var first = trigger(FIRST);
        var second = trigger(SECOND);

        assertTrue(first.shouldTrigger());
        assertEquals(200.0, first.status().lastCurrentMaxLoad());
        assertEquals(101.0, first.status().lastOptimalMaxLoad());
        assertFalse(second.shouldTrigger(), "both groups must not move their heavy partition together");
        assertEquals(1, second.status().evaluations(CROSS_GROUP_LOAD_UNAVAILABLE));
        assertNotNull(loads.snapshot(FIRST, INSTANCES), "the reserved group's assignor still needs background loads");

        assign(FIRST, true, 2);

        assertFalse(second.shouldTrigger(), "one group's correction balances both instances without another move");
        assertEquals(101.0, second.status().lastCurrentMaxLoad());
        assertEquals(101.0, second.status().lastOptimalMaxLoad());
        assertEquals(1, second.status().evaluations(BALANCED));
    }

    @Test
    void concurrentGroupEvaluationsReserveOnlyOneCorrection() throws Exception {
        var first = trigger(FIRST);
        var second = trigger(SECOND);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstResult = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return first.shouldTrigger();
            });
            var secondResult = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return second.shouldTrigger();
            });
            start.countDown();
            boolean firstFired = firstResult.get(10, TimeUnit.SECONDS);
            boolean secondFired = secondResult.get(10, TimeUnit.SECONDS);

            assertNotEquals(firstFired, secondFired, "exactly one of two coupled corrections may proceed");
            String changedGroup = firstFired ? FIRST : SECOND;
            ThresholdTrigger waitingTrigger = firstFired ? second : first;
            assertEquals(1, waitingTrigger.status().evaluations(CROSS_GROUP_LOAD_UNAVAILABLE));

            assign(changedGroup, true, 2);

            assertFalse(waitingTrigger.shouldTrigger());
            assertEquals(1.0, waitingTrigger.status().lastRatio());
            assertEquals(1, waitingTrigger.status().evaluations(BALANCED));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private ThresholdTrigger trigger(String group) {
        return new ThresholdTrigger(admin, group, tracker, weights, 1.1,
                new SortingRoundRobinBalanceService(), RebalanceDamping.none(), Clock.systemUTC(), loads);
    }

    private void assign(String group, boolean heavyOnB, int generation) {
        String a = group + "-a";
        String b = group + "-b";
        MemberDescription first = member(a, new TopicPartition(group, heavyOnB ? 1 : 0));
        MemberDescription second = member(b, new TopicPartition(group, heavyOnB ? 0 : 1));
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.groupState()).thenReturn(GroupState.STABLE);
        when(description.members()).thenReturn(List.of(first, second));
        descriptions.put(group, description);
        Set<String> topics = Set.of(group);
        var topology = Map.of("m:" + a, new MonitoringProtocol.Member("pod-a", topics),
                "m:" + b, new MonitoringProtocol.Member("pod-b", topics));
        var payload = new MonitoringProtocol.Assignment(UUID.randomUUID(), "m:" + a,
                "pod-a", topics, 0, topology);
        tracker.onAssignment(group, generation, null, a, "m:" + a, "pod-a", topics,
                MonitoringProtocol.assignment(payload));
    }

    private static MemberDescription member(String id, TopicPartition partition) {
        return new MemberDescription(id, Optional.empty(), "client", "host",
                new MemberAssignment(Set.of(partition)));
    }
}
