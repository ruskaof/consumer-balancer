package io.github.ruskaof.balancer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MemberIdTrackerTest {

    @Test
    void tracksMemberIdsPerGroup() {
        MemberIdTracker tracker = new MemberIdTracker();

        tracker.updateMemberId("group-a", null, "m-1");
        tracker.updateMemberId("group-b", null, "m-2");

        assertEquals(Set.of("m-1"), tracker.getCurrentMemberIds("group-a"));
        assertEquals(Set.of("m-2"), tracker.getCurrentMemberIds("group-b"));
    }

    @Test
    void replacesPreviousMemberId() {
        MemberIdTracker tracker = new MemberIdTracker();

        tracker.updateMemberId("g", null, "m-1");
        tracker.updateMemberId("g", "m-1", "m-2");

        assertEquals(Set.of("m-2"), tracker.getCurrentMemberIds("g"));
    }

    @Test
    void reRegisteringSameIdKeepsIt() {
        MemberIdTracker tracker = new MemberIdTracker();

        tracker.updateMemberId("g", null, "m-1");
        tracker.updateMemberId("g", "m-1", "m-1");

        assertEquals(Set.of("m-1"), tracker.getCurrentMemberIds("g"));
    }

    @Test
    void unknownGroupReturnsEmptySet() {
        assertEquals(Set.of(), new MemberIdTracker().getCurrentMemberIds("unknown"));
    }

    @Test
    void snapshotIsImmutable() {
        MemberIdTracker tracker = new MemberIdTracker();
        tracker.updateMemberId("g", null, "m-1");

        Set<String> snapshot = tracker.getCurrentMemberIds("g");

        assertThrows(UnsupportedOperationException.class, () -> snapshot.add("x"));
    }

    @Test
    void tracksInstanceIdsPerGroup() {
        MemberIdTracker tracker = new MemberIdTracker();

        tracker.recordInstanceIds("group-a", 1, Map.of("m-1", "pod-a"));
        tracker.recordInstanceIds("group-b", 1, Map.of("m-2", "pod-b"));

        assertEquals(Map.of("m-1", "pod-a"), tracker.getInstanceIds("group-a"));
        assertEquals(Map.of("m-2", "pod-b"), tracker.getInstanceIds("group-b"));
    }

    @Test
    void unknownGroupHasNoInstanceIds() {
        assertEquals(Map.of(), new MemberIdTracker().getInstanceIds("unknown"));
    }

    @Test
    void newerGenerationReplacesTheInstanceIds() {
        MemberIdTracker tracker = new MemberIdTracker();

        tracker.recordInstanceIds("g", 1, Map.of("m-1", "pod-a"));
        tracker.recordInstanceIds("g", 2, Map.of("m-1", "pod-a", "m-2", "pod-b"));

        assertEquals(Map.of("m-1", "pod-a", "m-2", "pod-b"), tracker.getInstanceIds("g"));
    }

    @Test
    void olderGenerationDoesNotReplaceTheInstanceIds() {
        MemberIdTracker tracker = new MemberIdTracker();
        tracker.recordInstanceIds("g", 2, Map.of("m-1", "pod-a", "m-2", "pod-b"));

        // A consumer thread that ran late must not resurrect a stale view of the group.
        tracker.recordInstanceIds("g", 1, Map.of("m-1", "pod-a"));

        assertEquals(Map.of("m-1", "pod-a", "m-2", "pod-b"), tracker.getInstanceIds("g"));
    }

    @Test
    void sameGenerationReportedTwiceIsIdempotent() {
        MemberIdTracker tracker = new MemberIdTracker();

        // Every consumer in the JVM reports the same mapping for one generation.
        tracker.recordInstanceIds("g", 3, Map.of("m-1", "pod-a"));
        tracker.recordInstanceIds("g", 3, Map.of("m-1", "pod-a"));

        assertEquals(Map.of("m-1", "pod-a"), tracker.getInstanceIds("g"));
    }

    @Test
    void instanceIdSnapshotIsImmutableAndDetachedFromTheCaller() {
        MemberIdTracker tracker = new MemberIdTracker();
        Map<String, String> reported = new HashMap<>(Map.of("m-1", "pod-a"));
        tracker.recordInstanceIds("g", 1, reported);

        reported.put("m-2", "pod-b");
        Map<String, String> snapshot = tracker.getInstanceIds("g");

        assertEquals(Map.of("m-1", "pod-a"), snapshot);
        assertThrows(UnsupportedOperationException.class, () -> snapshot.put("m-3", "pod-c"));
    }
}
