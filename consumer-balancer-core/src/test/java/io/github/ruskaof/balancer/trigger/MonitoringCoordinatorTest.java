package io.github.ruskaof.balancer.trigger;

import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class MonitoringCoordinatorTest {
    private static final String GROUP = "g";
    private static final Set<String> TOPICS = Set.of("t");

    @Test
    void electionFollowsTheSnapshotOwnerRatherThanTheSmallestVolatileId() {
        MemberIdTracker tracker = new MemberIdTracker();
        var members = Map.of("s:a", new MonitoringProtocol.Member("jvm-a", TOPICS),
                "s:b", new MonitoringProtocol.Member("jvm-b", TOPICS));
        tracker.onAssignment(GROUP, 1, null, "z-owner", "s:a", "jvm-a", TOPICS,
                MonitoringProtocol.assignment(new MonitoringProtocol.Assignment(UUID.randomUUID(), "s:a",
                        "jvm-a", TOPICS, 0, members)));
        AdminClient admin = mock(AdminClient.class);
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.groupState()).thenReturn(GroupState.STABLE);
        when(description.members()).thenReturn(List.of(live("z-owner", "a"), live("a-follower", "b")));
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.describedGroups()).thenReturn(Map.of(GROUP, KafkaFuture.completedFuture(description)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(result);
        try (CoordinatorElection election = new CoordinatorElection.Builder().setGroupId(GROUP)
                .setAdminClient(admin).setMemberIdTracker(tracker).build()) {
            assertTrue(election.confirmCoordinator());
            when(description.groupState()).thenReturn(GroupState.PREPARING_REBALANCE);
            assertFalse(election.confirmCoordinator());
            when(description.groupState()).thenReturn(GroupState.STABLE);
            when(description.members()).thenReturn(List.of(live("replacement", "a"), live("a-follower", "b")));
            assertFalse(election.confirmCoordinator(), "A fenced static process must surrender ownership");
        }
    }

    @Test
    void manualElectionRequiresTheAssignorsTracker() {
        assertThrows(IllegalArgumentException.class, () -> new CoordinatorElection.Builder()
                .setGroupId(GROUP).setAdminClient(mock(AdminClient.class)).build());
    }

    @Test
    void nonmonitorRefreshesStaleTopologyAndStopsAfterAcknowledgement() throws Exception {
        MemberIdTracker tracker = staleFollower();
        CoordinatorElection election = election(tracker);
        when(election.confirmStableGroup()).thenReturn(true);
        CountDownLatch requested = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        RebalanceInitiator initiator = () -> {
            requests.incrementAndGet();
            assertEquals(1, tracker.rebalanceRevision(GROUP), "Revision must precede the Kafka rejoin");
            tracker.onAssignment(GROUP, 2, "b", "b", "s:b", "new-jvm", TOPICS,
                    MonitoringProtocol.assignment(new MonitoringProtocol.Assignment(UUID.randomUUID(), "s:a",
                            "new-jvm", TOPICS, 1, Map.of())));
            requested.countDown();
        };
        try (CoordinatorManager manager = new CoordinatorManager(election, () -> false, initiator, 20)) {
            manager.start();
            assertTrue(requested.await(3, TimeUnit.SECONDS));
            assertFalse(tracker.topologyRefreshDue(GROUP, System.nanoTime()));
            assertFalse(tracker.claimTopologyRefresh(GROUP, System.nanoTime() + TimeUnit.HOURS.toNanos(1)));
            assertEquals(1, requests.get());
        }
    }

    @Test
    void recoveryDoesNotAdvanceRevisionsDuringAnOngoingRebalance() throws Exception {
        MemberIdTracker tracker = staleFollower();
        CoordinatorElection election = election(tracker);
        CountDownLatch checked = new CountDownLatch(1);
        when(election.confirmStableGroup()).thenAnswer(call -> { checked.countDown(); return false; });
        RebalanceInitiator initiator = mock(RebalanceInitiator.class);
        try (CoordinatorManager manager = new CoordinatorManager(election, () -> false, initiator, 20)) {
            manager.start();
            assertTrue(checked.await(3, TimeUnit.SECONDS));
            verify(initiator, after(100).never()).initiateRebalance();
            assertEquals(0, tracker.rebalanceRevision(GROUP));
        }
    }

    @Test
    void ownershipLostDuringEvaluationPreventsRebalance() throws Exception {
        MemberIdTracker tracker = new MemberIdTracker();
        CoordinatorElection election = election(tracker);
        when(election.isCoordinator()).thenReturn(true);
        CountDownLatch checked = new CountDownLatch(1);
        when(election.confirmCoordinator()).thenAnswer(call -> { checked.countDown(); return false; });
        AtomicReference<CoordinatorElection.CoordinatorStatusListener> listener = new AtomicReference<>();
        doAnswer(call -> { listener.set(call.getArgument(0)); return null; }).when(election).addListener(any());
        doAnswer(call -> { listener.get().onCoordinatorStatusChanged(true); return null; }).when(election).start();
        RebalanceInitiator initiator = mock(RebalanceInitiator.class);
        try (CoordinatorManager manager = new CoordinatorManager(election, () -> true, initiator, 20)) {
            manager.start();
            assertTrue(checked.await(3, TimeUnit.SECONDS));
            verify(initiator, after(100).never()).initiateRebalance();
            assertEquals(0, tracker.rebalanceRevision(GROUP));
        }
    }

    @Test
    void completedRebalanceDuringEvaluationInvalidatesTheOldDecision() throws Exception {
        MemberIdTracker tracker = new MemberIdTracker();
        var members = Map.of("s:a", new MonitoringProtocol.Member("jvm-a", TOPICS));
        tracker.onAssignment(GROUP, 1, null, "a", "s:a", "jvm-a", TOPICS,
                MonitoringProtocol.assignment(new MonitoringProtocol.Assignment(UUID.randomUUID(), "s:a",
                        "jvm-a", TOPICS, 0, members)));
        CoordinatorElection election = election(tracker);
        when(election.isCoordinator()).thenReturn(true);
        CountDownLatch checked = new CountDownLatch(1);
        when(election.confirmCoordinator()).thenAnswer(call -> { checked.countDown(); return true; });
        AtomicReference<CoordinatorElection.CoordinatorStatusListener> listener = new AtomicReference<>();
        doAnswer(call -> { listener.set(call.getArgument(0)); return null; }).when(election).addListener(any());
        doAnswer(call -> { listener.get().onCoordinatorStatusChanged(true); return null; }).when(election).start();
        AtomicInteger generation = new AtomicInteger(1);
        RebalanceTrigger trigger = () -> {
            tracker.onAssignment(GROUP, generation.incrementAndGet(), null, "a", "s:a", "jvm-a", TOPICS,
                    MonitoringProtocol.assignment(new MonitoringProtocol.Assignment(UUID.randomUUID(), "s:a",
                            "jvm-a", TOPICS, 0, members)));
            return true;
        };
        RebalanceInitiator initiator = mock(RebalanceInitiator.class);
        try (CoordinatorManager manager = new CoordinatorManager(election, trigger, initiator, 20)) {
            manager.start();
            assertTrue(checked.await(3, TimeUnit.SECONDS));
            verify(initiator, after(100).never()).initiateRebalance();
        }
    }

    private static CoordinatorElection election(MemberIdTracker tracker) {
        CoordinatorElection election = mock(CoordinatorElection.class);
        when(election.getGroupId()).thenReturn(GROUP);
        when(election.memberIdTracker()).thenReturn(tracker);
        return election;
    }

    private static MemberIdTracker staleFollower() {
        MemberIdTracker tracker = new MemberIdTracker();
        tracker.onAssignment(GROUP, 1, null, "b", "s:b", "new-jvm", TOPICS,
                MonitoringProtocol.assignment(new MonitoringProtocol.Assignment(UUID.randomUUID(), "s:a",
                        "old-jvm", TOPICS, 0, Map.of())));
        return tracker;
    }

    private static MemberDescription live(String memberId, String staticId) {
        return new MemberDescription(memberId, Optional.of(staticId), "client", "host", new MemberAssignment(Set.of()));
    }
}
