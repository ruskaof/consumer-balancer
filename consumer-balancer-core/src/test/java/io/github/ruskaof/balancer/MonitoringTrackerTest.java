package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringTrackerTest {
    private static final String GROUP = "g";
    private static final Set<String> TOPICS = Set.of("t");
    private static final UUID SNAPSHOT = UUID.randomUUID();
    private static final Map<String, MonitoringProtocol.Member> MEMBERS = Map.of(
            "s:a", new MonitoringProtocol.Member("pod-a", TOPICS),
            "s:b", new MonitoringProtocol.Member("pod-b", TOPICS));
    private final MemberIdTracker tracker = new MemberIdTracker();

    @Test
    void resolvesStaticIdentitiesAgainstCurrentMemberIds() {
        owner(1, SNAPSHOT, "a-old", "pod-a", "pod-a", 0, MEMBERS);
        var live = List.of(live("a-old", "a"), live("b-new", "b"));
        assertEquals("a-old", tracker.coordinatorMemberId(GROUP, live));
        assertEquals(Set.of("a-old", "b-new"), tracker.resolveMembers(GROUP, live).stream()
                .map(m -> m.memberId()).collect(java.util.stream.Collectors.toSet()));
        assertNull(tracker.coordinatorMemberId(GROUP, List.of(live("a-new", "a"), live("b-new", "b"))),
                "A fenced process cannot own the replacement static consumer");
    }

    @Test
    void followerCallbackInSameJvmDoesNotEraseOwnerSnapshot() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        callback(1, SNAPSHOT, "b", "s:b", "pod-b", "pod-b", 0, Map.of());
        assertNotNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertEquals("a", tracker.coordinatorMemberId(GROUP, liveGroup()));
    }

    @Test
    void newerFollowerCallbackInvalidatesOldSnapshotUntilOwnerArrives() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        UUID next = UUID.randomUUID();
        callback(2, next, "b", "s:b", "pod-b", "pod-b", 0, Map.of());
        assertNull(tracker.resolveMembers(GROUP, liveGroup()));
        owner(2, next, "a", "pod-a", "pod-a", 0, MEMBERS);
        assertNotNull(tracker.resolveMembers(GROUP, liveGroup()));
        owner(1, SNAPSHOT, "obsolete-a", "pod-a", "pod-a", 0, MEMBERS);
        assertEquals("a", tracker.coordinatorMemberId(GROUP, liveGroup()));
        assertFalse(tracker.getCurrentMemberIds(GROUP).contains("obsolete-a"));
    }

    @Test
    void restartedAutomaticInstanceRequestsRefreshAndStopsAfterAcknowledgement() {
        owner(1, SNAPSHOT, "a", "new-jvm", "pod-a", 0, MEMBERS);
        assertNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertTrue(tracker.claimTopologyRefresh(GROUP, 1));
        assertEquals(1, tracker.rebalanceRevision(GROUP));
        assertFalse(tracker.claimTopologyRefresh(GROUP, 2));
        // Another consumer/callback observing the same stale snapshot must not bump the revision.
        owner(1, SNAPSHOT, "a", "new-jvm", "pod-a", 0, MEMBERS);
        assertEquals(1, tracker.rebalanceRevision(GROUP));
        var fresh = Map.of("s:a", new MonitoringProtocol.Member("new-jvm", TOPICS),
                "s:b", MEMBERS.get("s:b"));
        owner(2, UUID.randomUUID(), "a", "new-jvm", "new-jvm", 1, fresh);
        assertNotNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertFalse(tracker.claimTopologyRefresh(GROUP, TimeUnit.HOURS.toNanos(1)));
        assertEquals(1, tracker.rebalanceRevision(GROUP), "Acknowledgement must not reset the advertised revision");
    }

    @Test
    void staticNonownerCanRequestRecoveryWithoutHavingFullTopology() {
        callback(1, SNAPSHOT, "b", "s:b", "new-jvm", "pod-b", 0, Map.of());
        assertNull(tracker.coordinatorMemberId(GROUP, liveGroup()));
        assertTrue(tracker.claimTopologyRefresh(GROUP, 1));
        callback(2, UUID.randomUUID(), "b", "s:b", "new-jvm", "new-jvm", 1, Map.of());
        assertFalse(tracker.claimTopologyRefresh(GROUP, TimeUnit.HOURS.toNanos(1)));
    }

    @Test
    void unacknowledgedRefreshRetriesWithBackoffAndANewRevision() {
        callback(1, SNAPSHOT, "b", "s:b", "new-jvm", "pod-b", 0, Map.of());
        assertTrue(tracker.claimTopologyRefresh(GROUP, 1));
        assertFalse(tracker.claimTopologyRefresh(GROUP, TimeUnit.SECONDS.toNanos(4)));
        assertTrue(tracker.claimTopologyRefresh(GROUP, TimeUnit.SECONDS.toNanos(6)));
        assertEquals(2, tracker.rebalanceRevision(GROUP));
        assertFalse(tracker.claimTopologyRefresh(GROUP, TimeUnit.SECONDS.toNanos(7)));
    }

    @Test
    void absentOrCorruptMetadataDisablesMonitoringWithoutARefreshLoop() {
        for (ByteBuffer payload : Arrays.asList(null, ByteBuffer.wrap(new byte[]{1, 2, 3}))) {
            tracker.onAssignment(GROUP, 1, null, "a", "s:a", "pod-a", TOPICS, payload);
            assertNull(tracker.resolveMembers(GROUP, liveGroup()));
            assertFalse(tracker.claimTopologyRefresh(GROUP, 1));
        }
    }

    @Test
    void memberIdsAloneDoNotProvideAMonitoringTopology() {
        tracker.updateMemberId(GROUP, null, "a");
        tracker.updateMemberId(GROUP, null, "b");
        assertNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertNull(tracker.coordinatorMemberId(GROUP, liveGroup()));
        assertFalse(tracker.claimTopologyRefresh(GROUP, 1));
    }

    @Test
    void monitoringSnapshotsAreIsolatedByGroup() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        assertNotNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertNull(tracker.resolveMembers("other", liveGroup()));
        tracker.prepareRebalance("other");
        assertNotNull(tracker.resolveMembers(GROUP, liveGroup()));
        assertEquals(0, tracker.rebalanceRevision(GROUP));
    }

    @Test
    void rejectsIncompleteOrDuplicateLiveMembership() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        assertNull(tracker.resolveMembers(GROUP, List.of(live("a", "a"))));
        assertNull(tracker.resolveMembers(GROUP, List.of(live("a", "a"), live("c", "c"))));
        assertNull(tracker.resolveMembers(GROUP, List.of(live("a", "a"), live("a-duplicate", "a"))));
    }

    @Test
    void rejectsAssignmentOutsideTheRecordedSubscription() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        var wrong = new MemberDescription("b", Optional.of("b"), "client", "host",
                new MemberAssignment(Set.of(new TopicPartition("other", 0))));
        assertNull(tracker.resolveMembers(GROUP, List.of(live("a", "a"), wrong)));
    }

    @Test
    void proactiveRequestWaitsForAnAssignmentAcknowledgement() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        tracker.prepareRebalance(GROUP);
        assertEquals(1, tracker.rebalanceRevision(GROUP));
        assertNull(tracker.coordinatorMemberId(GROUP, liveGroup()));
        owner(2, UUID.randomUUID(), "a", "pod-a", "pod-a", 1, MEMBERS);
        assertEquals("a", tracker.coordinatorMemberId(GROUP, liveGroup()));
    }

    @Test
    void conflictingSnapshotsInOneGenerationFailClosed() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        callback(1, UUID.randomUUID(), "b", "s:b", "pod-b", "pod-b", 0, Map.of());
        assertNull(tracker.coordinatorMemberId(GROUP, liveGroup()));
    }

    @Test
    void lowerGenerationNeedsBrokerConfirmationOfGroupRecreation() {
        owner(20, SNAPSHOT, "old-a", "pod-a", "pod-a", 0, MEMBERS);
        owner(1, UUID.randomUUID(), "new-a", "pod-a", "pod-a", 0, MEMBERS);
        var oldLive = List.of(live("old-a", "a"), live("b", "b"));
        tracker.reconcileMembership(GROUP, oldLive);
        assertEquals("old-a", tracker.coordinatorMemberId(GROUP, oldLive));
        var newLive = List.of(live("new-a", "a"), live("b", "b"));
        tracker.reconcileMembership(GROUP, newLive);
        assertEquals("new-a", tracker.coordinatorMemberId(GROUP, newLive));
        assertEquals(Set.of("new-a"), tracker.getCurrentMemberIds(GROUP));
    }

    @Test
    void obsoleteDecisionCannotAdvanceTheRebalanceRevision() {
        owner(1, SNAPSHOT, "a", "pod-a", "pod-a", 0, MEMBERS);
        var observed = tracker.monitoringSnapshot(GROUP);
        owner(2, UUID.randomUUID(), "a", "pod-a", "pod-a", 0, MEMBERS);
        assertFalse(tracker.prepareRebalance(GROUP, observed));
        assertEquals(0, tracker.rebalanceRevision(GROUP));
        assertTrue(tracker.prepareRebalance(GROUP, tracker.monitoringSnapshot(GROUP)));
        assertEquals(1, tracker.rebalanceRevision(GROUP));
    }

    private void owner(int generation, UUID snapshot, String memberId, String localInstance,
                       String echoedInstance, long revision, Map<String, MonitoringProtocol.Member> members) {
        callback(generation, snapshot, memberId, "s:a", localInstance, echoedInstance, revision, members);
    }

    private void callback(int generation, UUID snapshot, String memberId, String identity,
                          String localInstance, String echoedInstance, long revision,
                          Map<String, MonitoringProtocol.Member> members) {
        var assignment = new MonitoringProtocol.Assignment(snapshot, "s:a", echoedInstance, TOPICS, revision, members);
        tracker.onAssignment(GROUP, generation, null, memberId, identity, localInstance, TOPICS,
                MonitoringProtocol.assignment(assignment));
    }

    private static List<MemberDescription> liveGroup() {
        return List.of(live("a", "a"), live("b", "b"));
    }

    private static MemberDescription live(String memberId, String staticId) {
        return new MemberDescription(memberId, Optional.of(staticId), "client", "host", new MemberAssignment(Set.of()));
    }
}
