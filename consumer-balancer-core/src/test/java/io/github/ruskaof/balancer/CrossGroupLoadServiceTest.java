package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.GroupMember;
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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CrossGroupLoadServiceTest {
    private static final String CURRENT = "orders-group";
    private static final String OTHER = "payments-group";
    private static final Set<String> INSTANCES = Set.of("pod-a", "pod-b");
    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);
    private static final TopicPartition P2 = new TopicPartition("orders", 2);
    private static final TopicPartition P3 = new TopicPartition("orders", 3);

    private final AdminClient admin = mock(AdminClient.class);
    private final MemberIdTracker tracker = mock(MemberIdTracker.class);
    private final WeightService weights = mock(WeightService.class);
    private final Map<String, KafkaFuture<ConsumerGroupDescription>> descriptions = new HashMap<>();

    @BeforeEach
    void describeRegisteredGroups() {
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.describedGroups()).thenReturn(descriptions);
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(result);
    }

    @Test
    void batchesOtherGroupsAndSumsPerInstanceWithoutMultiplyingMemberOrGroupLoads() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a", "a2", "pod-a", "b1", "pod-b", "outside", "pod-c"),
                member("a1", P0), member("a2", P1), member("b1", P2), member("outside", P3));
        stableGroup("audit-group", Map.of("a3", "pod-a"), member("a3", P0));
        when(weights.computeWeights(Set.of(P0, P1, P2, P3)))
                .thenReturn(Map.of(P0, 10.0, P1, 20.0, P2, 30.0, P3, 40.0));

        CrossGroupLoadService.Snapshot snapshot = service(CURRENT, OTHER, "audit-group")
                .snapshot(CURRENT, INSTANCES);

        assertNotNull(snapshot);
        assertEquals(Map.of("pod-a", 40.0, "pod-b", 30.0), snapshot.loads(),
                "P0 is consumed by two groups and counts twice; pod-a's members each contribute their own work");
        assertEquals(Set.of(OTHER, "audit-group"), snapshot.assignments().keySet());
        assertEquals(Map.of("a1", Set.of(P0), "a2", Set.of(P1), "b1", Set.of(P2), "outside", Set.of(P3)),
                snapshot.assignments().get(OTHER));
        assertEquals(Map.of("a3", Set.of(P0)), snapshot.assignments().get("audit-group"));
        verify(admin).describeConsumerGroups(Set.of(OTHER, "audit-group"));
        verifyNoMoreInteractions(admin);
        verify(weights).computeWeights(Set.of(P0, P1, P2, P3));
        verifyNoMoreInteractions(weights);
        verify(tracker, never()).monitoringSnapshot(CURRENT);
        verify(tracker, never()).resolveMembers(eq(CURRENT), anyCollection());
    }

    @Test
    void singletonRegistryNeedsNoKafkaOrWeightObservation() throws Exception {
        CrossGroupLoadService.Snapshot snapshot = service(CURRENT).snapshot(CURRENT, INSTANCES);

        assertNotNull(snapshot);
        assertEquals(Map.of(), snapshot.loads());
        assertEquals(Map.of(), snapshot.assignments());
        verifyNoInteractions(admin, tracker, weights);
    }

    @Test
    void rejectsObservationForAGroupOutsideItsRegistryScope() throws Exception {
        assertNull(service(OTHER).snapshot(CURRENT, INSTANCES));

        verifyNoInteractions(admin, tracker, weights);
    }

    @Test
    void independentRegistryScopesOnlyCountTheirOwnGroups() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        stableGroup("separate-group", Map.of("b1", "pod-b"), member("b1", P1));
        when(weights.computeWeights(Set.of(P0))).thenReturn(Map.of(P0, 10.0));
        when(weights.computeWeights(Set.of(P1))).thenReturn(Map.of(P1, 20.0));

        CrossGroupLoadService.Snapshot first = service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES);
        CrossGroupLoadService.Snapshot second = service(CURRENT, "separate-group").snapshot(CURRENT, INSTANCES);

        assertNotNull(first);
        assertNotNull(second);
        assertEquals(Map.of("pod-a", 10.0), first.loads());
        assertEquals(Map.of("pod-b", 20.0), second.loads());
        assertEquals(Set.of(OTHER), first.assignments().keySet());
        assertEquals(Set.of("separate-group"), second.assignments().keySet());
        verify(admin).describeConsumerGroups(Set.of(OTHER));
        verify(admin).describeConsumerGroups(Set.of("separate-group"));
        verifyNoMoreInteractions(admin);
    }

    @Test
    void emptyOtherGroupsContributeNoLoadAndNeedNoTopology() throws Exception {
        describe(OTHER, GroupState.EMPTY);

        CrossGroupLoadService.Snapshot snapshot = service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES);

        assertNotNull(snapshot);
        assertEquals(Map.of(), snapshot.loads());
        assertEquals(Map.of(OTHER, Map.of()), snapshot.assignments());
        verifyNoInteractions(tracker, weights);
    }

    @Test
    void unstableOtherGroupMakesTheObservationUnavailable() throws Exception {
        describe(OTHER, GroupState.PREPARING_REBALANCE, member("a1", P0));

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));

        verifyNoInteractions(tracker, weights);
    }

    @Test
    void missingGroupDescriptionMakesTheObservationUnavailable() throws Exception {
        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));

        verifyNoInteractions(tracker, weights);
    }

    @Test
    void missingMonitoringSnapshotMakesTheObservationUnavailable() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        when(tracker.monitoringSnapshot(OTHER)).thenReturn(null);

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));

        verifyNoInteractions(weights);
    }

    @Test
    void incompleteTopologyMakesTheObservationUnavailable() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        when(tracker.resolveMembers(eq(OTHER), anyCollection())).thenReturn(null);

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));

        verifyNoInteractions(weights);
    }

    @Test
    void rebalanceDuringWeightObservationInvalidatesTheSnapshot() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        when(tracker.monitoringSnapshot(OTHER)).thenReturn(
                new MemberIdTracker.SnapshotToken(1, UUID.randomUUID()),
                new MemberIdTracker.SnapshotToken(2, UUID.randomUUID()));
        when(weights.computeWeights(Set.of(P0))).thenReturn(Map.of(P0, 10.0));

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));
    }

    @Test
    void topologyBecomingUnavailableDuringWeightObservationInvalidatesTheSnapshot() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        List<GroupMember> initial = List.of(new GroupMember("a1", "pod-a", Set.of("orders")));
        when(tracker.resolveMembers(eq(OTHER), anyCollection())).thenReturn(initial).thenReturn(null);
        when(weights.computeWeights(Set.of(P0))).thenReturn(Map.of(P0, 10.0));

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));
    }

    @Test
    void missingNullNegativeOrNonfiniteExternalWeightsMakeTheObservationUnavailable() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0));
        CrossGroupLoadService service = service(CURRENT, OTHER);
        when(weights.computeWeights(Set.of(P0))).thenReturn(Map.of());
        assertNull(service.snapshot(CURRENT, INSTANCES));

        when(weights.computeWeights(Set.of(P0))).thenReturn(null);
        assertNull(service.snapshot(CURRENT, INSTANCES));

        Map<TopicPartition, Double> nullWeight = new HashMap<>();
        nullWeight.put(P0, null);
        when(weights.computeWeights(Set.of(P0))).thenReturn(nullWeight);
        assertNull(service.snapshot(CURRENT, INSTANCES));

        for (double invalid : new double[]{-1.0, Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY}) {
            when(weights.computeWeights(Set.of(P0))).thenReturn(Map.of(P0, invalid));
            assertNull(service.snapshot(CURRENT, INSTANCES), "Invalid background weight: " + invalid);
        }
    }

    @Test
    void overflowingAggregateLoadMakesTheObservationUnavailable() throws Exception {
        stableGroup(OTHER, Map.of("a1", "pod-a"), member("a1", P0, P1));
        when(weights.computeWeights(Set.of(P0, P1)))
                .thenReturn(Map.of(P0, Double.MAX_VALUE, P1, Double.MAX_VALUE));

        assertNull(service(CURRENT, OTHER).snapshot(CURRENT, INSTANCES));
    }

    private CrossGroupLoadService service(String... groups) {
        return new CrossGroupLoadService(admin, tracker, weights, () -> List.of(groups));
    }

    private void stableGroup(String group, Map<String, String> instanceByMember, MemberDescription... members) {
        describe(group, GroupState.STABLE, members);
        when(tracker.monitoringSnapshot(group))
                .thenReturn(new MemberIdTracker.SnapshotToken(1, UUID.randomUUID()));
        List<GroupMember> topology = instanceByMember.entrySet().stream()
                .map(entry -> new GroupMember(entry.getKey(), entry.getValue(), Set.of("orders")))
                .toList();
        when(tracker.resolveMembers(eq(group), anyCollection())).thenReturn(topology);
    }

    private void describe(String group, GroupState state, MemberDescription... members) {
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.members()).thenReturn(List.of(members));
        when(description.groupState()).thenReturn(state);
        descriptions.put(group, KafkaFuture.completedFuture(description));
    }

    private static MemberDescription member(String id, TopicPartition... partitions) {
        return new MemberDescription(id, Optional.empty(), "client", "host",
                new MemberAssignment(Set.of(partitions)));
    }
}
