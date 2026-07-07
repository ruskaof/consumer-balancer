package io.github.ruskaof.balancer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the consumer group member ids owned by this JVM, per group.
 *
 * <p>Fed by {@link LoadAwarePartitionAssignor}'s {@code onAssignment} callback when
 * registered under the {@code assignor.load-aware.member-id-tracker} consumer config.
 * {@link io.github.ruskaof.balancer.trigger.CoordinatorElection} consumes
 * {@link #getCurrentMemberIds(String)} to decide whether the group's elected member id
 * belongs to this instance.
 *
 * <p>Ids of consumers that left the group are not removed eagerly; that is harmless
 * because the election only matches tracked ids against live group members.
 */
@Slf4j
public class MemberIdTracker {

    private final Map<String, Set<String>> memberIdsByGroup = new ConcurrentHashMap<>();

    /**
     * Registers {@code currentMemberId} for {@code groupId}, replacing
     * {@code previousMemberId} ({@code null} on first report) reported earlier by the
     * same consumer.
     */
    public void updateMemberId(String groupId, String previousMemberId, String currentMemberId) {
        Set<String> memberIds = memberIdsByGroup.computeIfAbsent(groupId, g -> ConcurrentHashMap.newKeySet());
        if (previousMemberId != null && !previousMemberId.equals(currentMemberId)) {
            memberIds.remove(previousMemberId);
        }
        memberIds.add(currentMemberId);
        log.debug("Member ID registered: {} (total tracked: {}, group: {})",
                currentMemberId, memberIds.size(), groupId);
    }

    /**
     * @return immutable snapshot of the member ids tracked for {@code groupId}; empty for unknown groups
     */
    public Set<String> getCurrentMemberIds(String groupId) {
        Set<String> memberIds = memberIdsByGroup.get(groupId);
        return memberIds == null ? Set.of() : Set.copyOf(memberIds);
    }
}
