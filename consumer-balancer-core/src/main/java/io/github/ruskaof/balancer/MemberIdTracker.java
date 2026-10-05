package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.instance.MonitoringProtocol;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.MemberDescription;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This JVM's local consumer identities, monitoring snapshots and rebalance requests.
 *
 * <p>Assignments are fed by {@link LoadAwarePartitionAssignor}'s {@code onAssignment} callback
 * when the tracker is registered under the
 * {@code assignor.load-aware.member-id-tracker} consumer config. A per-group lock protects
 * state shared between consumer callbacks and the coordinator's scheduler:
 * <ul>
 *   <li>{@link io.github.ruskaof.balancer.trigger.CoordinatorElection} consumes
 *       {@link #coordinatorMemberId(String, Collection)} to validate the designated monitor;</li>
 *   <li>{@link io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger} consumes
 *       {@link #resolveMembers(String, Collection)} for current member ids, instance grouping
 *       and subscription eligibility. Only the monitor receives the full topology.</li>
 * </ul>
 *
 * <p>Ids of consumers that left the group are not removed eagerly; that is harmless
 * because the election only matches tracked ids against live group members.
 */
@Slf4j
public class MemberIdTracker {

    private final Map<String, Set<String>> memberIdsByGroup = new ConcurrentHashMap<>();
    private final Map<String, MonitoringState> monitoringByGroup = new ConcurrentHashMap<>();

    /** Identifies the assignment a trigger evaluation observes, including same-member rebalances. */
    public record SnapshotToken(int generation, UUID snapshotId) { }

    public SnapshotToken monitoringSnapshot(String groupId) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return null;
        synchronized (state) {
            return state.snapshotId == null ? null : new SnapshotToken(state.generation, state.snapshotId);
        }
    }

    /** Revision included in the next JoinGroup subscription; unchanged during ordinary polling. */
    public long rebalanceRevision(String groupId) {
        if (groupId == null) return 0;
        MonitoringState state = monitoringByGroup.computeIfAbsent(groupId, key -> new MonitoringState());
        synchronized (state) {
            return state.revision;
        }
    }

    /**
     * Make an explicit rejoin observable to Kafka even when the requester is not the Kafka
     * leader. An unchanged follower subscription can otherwise receive the old assignment.
     */
    public void prepareRebalance(String groupId) {
        MonitoringState state = monitoringByGroup.computeIfAbsent(groupId, key -> new MonitoringState());
        synchronized (state) {
            state.revision++;
            state.pendingRequest = true;
            state.nextRefreshNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        }
    }

    /** Atomically reject a decision computed for an assignment which has since changed. */
    public boolean prepareRebalance(String groupId, SnapshotToken expected) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null || expected == null) return false;
        synchronized (state) {
            if (needsRefresh(state) || !expected.equals(monitoringSnapshot(groupId))) return false;
            prepareRebalance(groupId);
            return true;
        }
    }

    /**
     * Records an assignment on the consumer thread. No Kafka calls or listener callbacks run
     * here. Recovery is performed asynchronously by the group's coordinator manager, including
     * on JVMs which do not own the monitoring role.
     */
    public void onAssignment(String groupId, int generationId, String previousMemberId,
                             String memberId, String identity, String instanceId,
                             Set<String> topics, ByteBuffer userData) {
        MonitoringProtocol.Assignment assignment = MonitoringProtocol.readAssignment(userData);
        MonitoringState state = monitoringByGroup.computeIfAbsent(groupId, key -> new MonitoringState());
        synchronized (state) {
            if (generationId < state.generation) {
                // Group deletion/expiry resets Kafka's generation. Do not accept a lower
                // one merely because a callback arrived: election must first verify that
                // the replacement consumer is live and the previous local members are gone.
                if (!getCurrentMemberIds(groupId).contains(memberId)) {
                    ByteBuffer copy = userData == null ? null : userData.duplicate();
                    byte[] bytes = copy == null || copy.remaining() > MonitoringProtocol.MAX_TOTAL_BYTES
                            ? null : new byte[copy.remaining()];
                    if (bytes != null) copy.get(bytes);
                    state.earlierAssignments.put(identity, new PendingAssignment(generationId,
                            previousMemberId, memberId, identity, instanceId, Set.copyOf(topics), bytes));
                }
                return;
            }
            updateMemberId(groupId, previousMemberId, memberId);
            if (generationId > state.generation) {
                state.generation = generationId;
                state.snapshotId = null;
                state.owner = null;
                state.members = Map.of();
                state.localAssignments.clear();
            }
            if (assignment == null) {
                // Unavailable metadata (including an exceeded budget) must not cause a storm.
                state.localAssignments.put(identity,
                        new LocalAssignment(memberId, null, false, false));
                state.members = Map.of();
                state.owner = null;
                state.pendingRequest = false;
                return;
            }
            if (state.snapshotId != null && !state.snapshotId.equals(assignment.snapshotId())) {
                // Different snapshots in one generation are not safe to combine.
                state.members = Map.of();
                state.owner = null;
                log.warn("Conflicting monitoring snapshots for group '{}' generation {}", groupId, generationId);
                return;
            }
            state.snapshotId = assignment.snapshotId();
            state.owner = assignment.ownerIdentity();
            boolean matches = instanceId.equals(assignment.instanceId())
                    && topics.equals(assignment.topics())
                    && state.revision == assignment.revision();
            state.localAssignments.put(identity,
                    new LocalAssignment(memberId, assignment.snapshotId(), matches, !matches));
            if (identity.equals(state.owner)) {
                state.members = matches ? assignment.members() : Map.of();
            }
            if (matches) {
                state.pendingRequest = false;
                if (!needsRefresh(state)) {
                    state.refreshAttempts = 0;
                    state.nextRefreshNanos = 0;
                }
            }
        }
    }

    /** Claims a coalesced refresh attempt, with exponential backoff capped at one minute. */
    public boolean claimTopologyRefresh(String groupId, long nowNanos) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return false;
        synchronized (state) {
            if (!needsRefresh(state)
                    || (state.nextRefreshNanos != 0 && nowNanos - state.nextRefreshNanos < 0)) return false;
            // Each rate-limited attempt changes the subscription. This also recovers if a
            // static member lost its member id again and Kafka cached the previous revision
            // during an UNKNOWN_MEMBER_ID rejoin without running the assignor.
            state.revision++;
            state.pendingRequest = true;
            state.refreshAttempts = Math.min(state.refreshAttempts + 1, 5);
            long delay = Math.min(60, 5L << (state.refreshAttempts - 1));
            state.nextRefreshNanos = nowNanos + TimeUnit.SECONDS.toNanos(delay);
            log.info("Refreshing monitoring topology [group={}, revision={}, retryDelaySeconds={}]",
                    groupId, state.revision, delay);
            return true;
        }
    }

    /** Cheap local check before the manager asks the broker whether recovery can proceed. */
    public boolean topologyRefreshDue(String groupId, long nowNanos) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return false;
        synchronized (state) {
            return needsRefresh(state)
                    && (state.nextRefreshNanos == 0 || nowNanos - state.nextRefreshNanos >= 0);
        }
    }

    /** Let an in-flight rebalance finish without changing the revision it will acknowledge. */
    public void deferTopologyRefresh(String groupId, long nowNanos) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return;
        synchronized (state) {
            state.nextRefreshNanos = nowNanos + TimeUnit.SECONDS.toNanos(5);
        }
    }

    private static boolean needsRefresh(MonitoringState state) {
        return state.pendingRequest || state.localAssignments.values().stream().anyMatch(LocalAssignment::refresh);
    }

    /** Current broker member id of the designated local monitor, or null while unready. */
    public String coordinatorMemberId(String groupId, Collection<MemberDescription> liveMembers) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return null;
        synchronized (state) {
            LocalAssignment local = state.localAssignments.get(state.owner);
            if (local == null || !local.valid() || needsRefresh(state)
                    || !Objects.equals(local.snapshotId(), state.snapshotId)
                    || resolve(state, liveMembers) == null) return null;
            for (MemberDescription live : liveMembers) {
                if (MonitoringProtocol.identity(live.consumerId(), live.groupInstanceId()).equals(state.owner)
                        && live.consumerId().equals(local.memberId())) return live.consumerId();
            }
            return null;
        }
    }

    /** Called only with a stable broker description, to recognize a recreated group safely. */
    public void reconcileMembership(String groupId, Collection<MemberDescription> liveMembers) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return;
        synchronized (state) {
            if (state.earlierAssignments.isEmpty()) return;
            Set<String> liveIds = new HashSet<>();
            liveMembers.forEach(member -> liveIds.add(member.consumerId()));
            if (getCurrentMemberIds(groupId).stream().anyMatch(liveIds::contains)) return;
            List<PendingAssignment> replacements = state.earlierAssignments.values().stream()
                    .filter(candidate -> liveIds.contains(candidate.memberId()))
                    .sorted(Comparator.comparingInt(PendingAssignment::generation))
                    .toList();
            if (replacements.isEmpty()) return;
            state.generation = -1;
            state.snapshotId = null;
            state.owner = null;
            state.members = Map.of();
            state.localAssignments.clear();
            state.earlierAssignments.clear();
            memberIdsByGroup.remove(groupId);
            for (PendingAssignment replacement : replacements) {
                onAssignment(groupId, replacement.generation(), replacement.previousMemberId(),
                        replacement.memberId(), replacement.identity(), replacement.instanceId(),
                        replacement.topics(), replacement.data() == null ? null : ByteBuffer.wrap(replacement.data()));
            }
            log.info("Recognized recreated consumer group '{}' from its live replacement members", groupId);
        }
    }

    /**
     * Resolves stable consumer identities to current broker member ids and validates exact
     * membership. Subscriptions come from the same snapshot the assignor used.
     */
    public List<GroupMember> resolveMembers(String groupId, Collection<MemberDescription> liveMembers) {
        MonitoringState state = monitoringByGroup.get(groupId);
        if (state == null) return null;
        synchronized (state) {
            return needsRefresh(state) ? null : resolve(state, liveMembers);
        }
    }

    private static List<GroupMember> resolve(MonitoringState state, Collection<MemberDescription> liveMembers) {
        if (state.owner == null || state.members.isEmpty() || state.members.size() != liveMembers.size()) return null;
        Set<String> identities = new HashSet<>();
        List<GroupMember> result = new ArrayList<>();
        for (MemberDescription live : liveMembers) {
            String identity = MonitoringProtocol.identity(live.consumerId(), live.groupInstanceId());
            MonitoringProtocol.Member member = state.members.get(identity);
            if (member == null || !identities.add(identity)
                    || live.assignment().topicPartitions().stream().anyMatch(tp -> !member.topics().contains(tp.topic()))) {
                return null;
            }
            result.add(new GroupMember(live.consumerId(), member.instanceId(), member.topics()));
        }
        return result;
    }

    private record LocalAssignment(String memberId, UUID snapshotId, boolean valid, boolean refresh) { }
    private record PendingAssignment(int generation, String previousMemberId, String memberId,
                                     String identity, String instanceId, Set<String> topics, byte[] data) { }

    private static final class MonitoringState {
        int generation = -1;
        UUID snapshotId;
        String owner;
        Map<String, MonitoringProtocol.Member> members = Map.of();
        final Map<String, LocalAssignment> localAssignments = new HashMap<>();
        final Map<String, PendingAssignment> earlierAssignments = new HashMap<>();
        long revision;
        boolean pendingRequest;
        int refreshAttempts;
        long nextRefreshNanos;
    }

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
