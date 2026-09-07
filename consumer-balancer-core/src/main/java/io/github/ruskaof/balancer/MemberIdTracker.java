package io.github.ruskaof.balancer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This JVM's view of the consumer groups it takes part in, per group: the member ids it
 * owns, and the instance id every member of the group runs under.
 *
 * <p>Both are fed by {@link LoadAwarePartitionAssignor}'s {@code onAssignment} callback
 * when the tracker is registered under the
 * {@code assignor.load-aware.member-id-tracker} consumer config, and both are read from
 * other threads:
 * <ul>
 *   <li>{@link io.github.ruskaof.balancer.trigger.CoordinatorElection} consumes
 *       {@link #getCurrentMemberIds(String)} to decide whether the group's elected member
 *       id belongs to this instance;</li>
 *   <li>{@link io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger} consumes
 *       {@link #getInstanceIds(String)} to group the group's members into application
 *       instances — the admin API it otherwise works from cannot see instance ids.</li>
 * </ul>
 *
 * <p>Ids of consumers that left the group are not removed eagerly; that is harmless
 * because the election only matches tracked ids against live group members.
 */
@Slf4j
public class MemberIdTracker {

    private final Map<String, Set<String>> memberIdsByGroup = new ConcurrentHashMap<>();
    private final Map<String, GroupInstances> instancesByGroup = new ConcurrentHashMap<>();

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

    /**
     * Records the {@code memberId -> instanceId} mapping the group leader computed for
     * {@code generationId}. Every consumer in this JVM receives — and reports — the same
     * mapping for a generation, so repeated calls are idempotent; a mapping from an older
     * generation never replaces a newer one, which is what keeps a consumer thread that
     * ran late from resurrecting a stale view of the group.
     */
    public void recordInstanceIds(String groupId, int generationId, Map<String, String> instanceIdByMember) {
        GroupInstances recorded = new GroupInstances(generationId, Map.copyOf(instanceIdByMember));
        instancesByGroup.merge(groupId, recorded,
                (current, incoming) -> incoming.generationId() >= current.generationId() ? incoming : current);
        log.debug("Instance ids registered for generation {} (members: {}, group: {})",
                generationId, recorded.instanceIdByMember().size(), groupId);
    }

    /**
     * @return immutable {@code memberId -> instanceId} mapping last recorded for
     *         {@code groupId}; empty for unknown groups
     */
    public Map<String, String> getInstanceIds(String groupId) {
        GroupInstances instances = instancesByGroup.get(groupId);
        return instances == null ? Map.of() : instances.instanceIdByMember();
    }

    /** One generation's mapping; immutable, so readers need no lock. */
    private record GroupInstances(int generationId, Map<String, String> instanceIdByMember) {
    }
}
