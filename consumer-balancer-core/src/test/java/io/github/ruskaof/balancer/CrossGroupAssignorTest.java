package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CrossGroupAssignorTest {
    private static final String GROUP = "g";
    private static final WeightService WEIGHTS = partitions -> {
        Map<TopicPartition, Double> result = new HashMap<>();
        partitions.forEach(partition -> result.put(partition, 10.0));
        return result;
    };

    @Test
    void assignsNewWorkToTheInstanceWithLessLoadFromOtherGroups() throws Exception {
        CrossGroupLoadService loads = mock(CrossGroupLoadService.class);
        when(loads.snapshot(GROUP, Set.of("pod-a", "pod-b")))
                .thenReturn(new CrossGroupLoadService.Snapshot(Map.of("pod-a", 100.0), Map.of()));
        var assignor = configured(new MemberIdTracker(), "pod-a", loads);

        var assignment = assignor.assign(Map.of("t", 4), subscriptions());

        assertTrue(assignment.get("a1").isEmpty());
        assertTrue(assignment.get("a2").isEmpty());
        assertEquals(4, assignment.get("b1").size());
        verify(loads).snapshot(GROUP, Set.of("pod-a", "pod-b"));
    }

    @Test
    void missingOrFailedCrossGroupObservationKeepsGroupLocalWeightedBalancing() throws Exception {
        for (boolean fails : List.of(false, true)) {
            CrossGroupLoadService loads = mock(CrossGroupLoadService.class);
            if (fails) when(loads.snapshot(anyString(), anySet())).thenThrow(new IllegalStateException("unavailable"));
            WeightService weights = partitions -> Map.of(
                    new TopicPartition("t", 0), 50.0,
                    new TopicPartition("t", 1), 5.0,
                    new TopicPartition("t", 2), 1.0);
            var assignor = new LoadAwarePartitionAssignor();
            assignor.configure(Map.of(
                    LoadAwareAssignorConfig.WEIGHT_SERVICE, weights,
                    LoadAwareAssignorConfig.MEMBER_ID_TRACKER, new MemberIdTracker(),
                    LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE, loads,
                    "group.id", GROUP));

            var assignment = assignor.assign(Map.of("t", 3), Map.of(
                    "a", subscription("pod-a", null, true),
                    "b", subscription("pod-b", null, true)));

            assertEquals(List.of(new TopicPartition("t", 0)), assignment.get("a"));
            assertEquals(List.of(new TopicPartition("t", 1), new TopicPartition("t", 2)), assignment.get("b"));
        }
    }

    @Test
    void sendsOneCompleteTopologyPerInstanceAndAcknowledgesOtherLocalConsumers() {
        var assignor = configured(new MemberIdTracker(), "pod-a", emptyScope());
        var assignment = assignor.assign(cluster(2), new GroupSubscription(subscriptions()));
        var owner = decoded(assignment, "a1");
        var sibling = decoded(assignment, "a2");
        var remote = decoded(assignment, "b1");

        assertEquals(Set.of("m:a1", "m:a2", "m:b1"), owner.members().keySet());
        assertEquals(owner.members(), remote.members());
        assertTrue(sibling.members().isEmpty());
        assertEquals(owner.snapshotId(), remote.snapshotId());
        assertEquals("m:a1", remote.ownerIdentity(), "replicating a topology must not create another coordinator");
        assertEquals(2, assignment.groupAssignment().values().stream()
                .filter(value -> !MonitoringProtocol.readAssignment(value.userData()).members().isEmpty()).count());
    }

    @Test
    void choosesStaticIdentityPerInstanceWithoutSelectingConsumersThatCannotMonitor() {
        var assignor = configured(new MemberIdTracker(), "pod-a", emptyScope());
        var subscriptions = Map.of(
                "z-owner", subscription("pod-a", "static-a", true),
                "a-local", subscription("pod-a", "static-b", true),
                "z-remote", subscription("pod-b", "static-c", true),
                "a-remote", subscription("pod-b", "static-d", true),
                "unsupported", subscription("pod-b", "static-0", false));
        var assignment = assignor.assign(cluster(2), new GroupSubscription(subscriptions));

        assertEquals("s:static-a", decoded(assignment, "z-owner").ownerIdentity());
        assertEquals(5, decoded(assignment, "z-owner").members().size());
        assertEquals(5, decoded(assignment, "z-remote").members().size());
        for (String member : List.of("a-local", "a-remote", "unsupported")) {
            assertTrue(decoded(assignment, member).members().isEmpty(), member);
        }
    }

    @Test
    void nonownerInstanceCanResolveTheDeliveredTopologyWithoutBecomingCoordinator() {
        var leader = configured(new MemberIdTracker(), "pod-a", emptyScope());
        var assignment = leader.assign(cluster(2), new GroupSubscription(subscriptions()));
        var remoteTracker = new MemberIdTracker();
        var remote = configured(remoteTracker, "pod-b", emptyScope());
        remote.subscriptionUserData(Set.of("t"));

        remote.onAssignment(assignment.groupAssignment().get("b1"),
                new ConsumerGroupMetadata(GROUP, 1, "b1", Optional.empty()));

        var liveMembers = List.of(live("a1"), live("a2"), live("b1"));
        var members = remoteTracker.resolveMembers(GROUP, liveMembers);
        assertNotNull(members);
        assertEquals(3, members.size());
        assertNull(remoteTracker.coordinatorMemberId(GROUP, liveMembers));
    }

    @Test
    void crossGroupMonitorSelectionPrefersTheSameInstanceDespiteDifferentMemberIdentityOrdering() {
        var assignor = configured(new MemberIdTracker(), "pod-a", emptyScope());
        var firstGroup = assignor.assign(cluster(2), new GroupSubscription(Map.of(
                "z-on-a", subscription("pod-a", "z-static", true),
                "a-on-b", subscription("pod-b", "a-static", true))));
        var secondGroup = assignor.assign(cluster(2), new GroupSubscription(Map.of(
                "a-on-a", subscription("pod-a", null, true),
                "z-on-b", subscription("pod-b", null, true))));

        assertEquals("s:z-static", decoded(firstGroup, "z-on-a").ownerIdentity());
        assertEquals("m:a-on-a", decoded(secondGroup, "a-on-a").ownerIdentity());

        var ordinary = new LoadAwarePartitionAssignor();
        ordinary.configure(Map.of(LoadAwareAssignorConfig.WEIGHT_SERVICE, WEIGHTS));
        var legacy = ordinary.assign(cluster(2), new GroupSubscription(Map.of(
                "z-on-a", subscription("pod-a", "z-static", true),
                "a-on-b", subscription("pod-b", "a-static", true))));
        assertEquals("s:a-static", decoded(legacy, "a-on-b").ownerIdentity(),
                "default monitoring election must retain its existing identity ordering");
    }

    @Test
    void replicatedTopologiesStillRespectTheWholeGroupMetadataBudget() {
        Map<String, Subscription> subscriptions = new TreeMap<>();
        for (int i = 0; i < 40; i++) {
            subscriptions.put("member-" + i + "-" + "x".repeat(400), subscription("pod-" + i, null, true));
        }
        var assignor = configured(new MemberIdTracker(), "pod-a", emptyScope());
        var assignment = assignor.assign(cluster(2), new GroupSubscription(subscriptions));

        assertEquals(40, assignment.groupAssignment().size());
        assertEquals(2, assignment.groupAssignment().values().stream().mapToInt(value -> value.partitions().size()).sum());
        assignment.groupAssignment().values().forEach(value -> assertNull(value.userData(),
                "exceeding the combined metadata budget must not deliver a partial topology"));

        var ordinary = new LoadAwarePartitionAssignor();
        ordinary.configure(Map.of(LoadAwareAssignorConfig.WEIGHT_SERVICE, WEIGHTS));
        ordinary.assign(cluster(2), new GroupSubscription(subscriptions)).groupAssignment().values()
                .forEach(value -> assertNotNull(value.userData(), "one topology still fits for the same group"));
    }

    @Test
    void crossGroupConfigurationRequiresTheSharedTracker() {
        var assignor = new LoadAwarePartitionAssignor();
        assertThrows(IllegalArgumentException.class, () -> assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, WEIGHTS,
                LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE, emptyScope())));
    }

    private static LoadAwarePartitionAssignor configured(MemberIdTracker tracker, String instance,
                                                        CrossGroupLoadService loads) {
        var assignor = new LoadAwarePartitionAssignor();
        assignor.configure(Map.of(
                LoadAwareAssignorConfig.WEIGHT_SERVICE, WEIGHTS,
                LoadAwareAssignorConfig.MEMBER_ID_TRACKER, tracker,
                LoadAwareAssignorConfig.INSTANCE_ID, instance,
                LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE, loads,
                "group.id", GROUP));
        return assignor;
    }

    private static CrossGroupLoadService emptyScope() {
        return new CrossGroupLoadService(mock(AdminClient.class), new MemberIdTracker(), WEIGHTS,
                () -> List.of(GROUP));
    }

    private static Map<String, Subscription> subscriptions() {
        return Map.of("a1", subscription("pod-a", null, true),
                "a2", subscription("pod-a", null, true), "b1", subscription("pod-b", null, true));
    }

    private static Subscription subscription(String instance, String staticId, boolean monitoring) {
        var subscription = new Subscription(List.of("t"), MonitoringProtocol.subscription(instance, monitoring, 0));
        subscription.setGroupInstanceId(Optional.ofNullable(staticId));
        return subscription;
    }

    private static MonitoringProtocol.Assignment decoded(GroupAssignment assignment, String member) {
        var result = MonitoringProtocol.readAssignment(assignment.groupAssignment().get(member).userData());
        assertNotNull(result);
        return result;
    }

    private static MemberDescription live(String memberId) {
        return new MemberDescription(memberId, Optional.empty(), "client", "host", new MemberAssignment(Set.of()));
    }

    private static Cluster cluster(int partitionCount) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new PartitionInfo("t", i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("c", List.of(node), partitions, Set.of(), Set.of());
    }
}
