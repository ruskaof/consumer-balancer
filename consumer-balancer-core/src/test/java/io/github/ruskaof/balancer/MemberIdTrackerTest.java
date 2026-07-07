package io.github.ruskaof.balancer;

import org.junit.jupiter.api.Test;

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
}
