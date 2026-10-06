package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.weight.PartitionWeights;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Reads other registered groups' current assignments and weights, using the instance
 * topologies delivered to this JVM. No load measurements are cached between decisions.
 * A missing, changing or incomplete topology makes the entire observation unavailable.
 */
public final class CrossGroupLoadService {
    private static final long DESCRIBE_TIMEOUT_SECONDS = 30;

    private final AdminClient adminClient;
    private final MemberIdTracker tracker;
    private final WeightService weightService;
    private final Supplier<? extends Collection<String>> groupIds;
    private String pendingGroup;
    private MemberIdTracker.SnapshotToken pendingTopology;
    private long pendingUntilNanos;

    public CrossGroupLoadService(AdminClient adminClient, MemberIdTracker tracker,
                                 WeightService weightService,
                                 Supplier<? extends Collection<String>> groupIds) {
        this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.weightService = Objects.requireNonNull(weightService, "weightService");
        this.groupIds = Objects.requireNonNull(groupIds, "groupIds");
    }

    /**
     * Other groups' load on the given instances, or null when it cannot be safely measured.
     * The current group is always excluded. A partition consumed by two different groups
     * contributes twice, while multiple consumers in one instance never multiply its load.
     * Kafka calls have one shared timeout for the complete batch.
     */
    public Snapshot snapshot(String currentGroup, Set<String> instances) throws Exception {
        if (anotherRebalancePending(currentGroup)) return null;
        SortedSet<String> registered = new TreeSet<>(groupIds.get());
        if (!registered.remove(currentGroup)) return null;
        if (registered.isEmpty()) return new Snapshot(Map.of(), Map.of());

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DESCRIBE_TIMEOUT_SECONDS);
        var futures = adminClient.describeConsumerGroups(registered).describedGroups();
        Map<String, ConsumerGroupDescription> descriptions = new TreeMap<>();
        Map<String, List<GroupMember>> memberships = new HashMap<>();
        Map<String, MemberIdTracker.SnapshotToken> tokens = new HashMap<>();
        Set<TopicPartition> partitions = new HashSet<>();
        Map<String, Map<String, Set<TopicPartition>>> assignments = new TreeMap<>();
        for (String group : registered) {
            var future = futures.get(group);
            if (future == null) return null;
            var description = future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (description.groupState() == GroupState.EMPTY && description.members().isEmpty()) {
                assignments.put(group, Map.of());
                continue;
            }
            if (description.groupState() != GroupState.STABLE) return null;
            tracker.reconcileMembership(group, description.members());
            var token = tracker.monitoringSnapshot(group);
            var members = tracker.resolveMembers(group, description.members());
            if (token == null || members == null) return null;
            descriptions.put(group, description);
            memberships.put(group, members);
            tokens.put(group, token);
            Map<String, Set<TopicPartition>> assignment = new TreeMap<>();
            for (var member : description.members()) {
                var owned = Set.copyOf(member.assignment().topicPartitions());
                assignment.put(member.consumerId(), owned);
                partitions.addAll(owned);
            }
            assignments.put(group, Map.copyOf(assignment));
        }

        var sanitized = PartitionWeights.sanitizedCounted(partitions,
                partitions.isEmpty() ? Map.of() : weightService.computeWeights(partitions));
        // Guessing any background load can reverse which instance should receive work.
        if (sanitized.defaultedCount() > 0
                || sanitized.weights().values().stream().anyMatch(weight -> weight < 0)) return null;
        Map<String, Double> loads = new TreeMap<>();
        for (var entry : descriptions.entrySet()) {
            String group = entry.getKey();
            if (!memberships.get(group).equals(tracker.resolveMembers(group, entry.getValue().members()))
                    || !tokens.get(group).equals(tracker.monitoringSnapshot(group))) return null;
            Map<String, String> instanceByMember = new HashMap<>();
            memberships.get(group).forEach(member -> instanceByMember.put(member.memberId(), member.instanceId()));
            for (var member : entry.getValue().members()) {
                String instance = instanceByMember.get(member.consumerId());
                if (!instances.contains(instance)) continue;
                for (var partition : member.assignment().topicPartitions()) {
                    loads.merge(instance, sanitized.weights().get(partition), Double::sum);
                }
            }
        }
        if (loads.values().stream().anyMatch(load -> !Double.isFinite(load))) return null;
        return new Snapshot(loads, assignments, tokens);
    }

    /**
     * Reserves the next correction until its assignment arrives. Opt-in monitors are elected
     * on the same instance across groups, and synchronize evaluation on this shared service.
     * A bounded reservation also releases a decision the coordinator could not execute.
     */
    public synchronized void reserveRebalance(String groupId) {
        pendingGroup = groupId;
        pendingTopology = tracker.monitoringSnapshot(groupId);
        pendingUntilNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(DESCRIBE_TIMEOUT_SECONDS);
    }

    private synchronized boolean anotherRebalancePending(String groupId) {
        if (pendingGroup == null) return false;
        if (!Objects.equals(pendingTopology, tracker.monitoringSnapshot(pendingGroup))
                || System.nanoTime() - pendingUntilNanos >= 0) {
            pendingGroup = null;
            pendingTopology = null;
            return false;
        }
        return !pendingGroup.equals(groupId);
    }

    /** Loads and a group-qualified assignment fingerprint for rebalance hysteresis. */
    public record Snapshot(Map<String, Double> loads,
                           Map<String, Map<String, Set<TopicPartition>>> assignments,
                           Map<String, MemberIdTracker.SnapshotToken> topologies) {
        public Snapshot(Map<String, Double> loads,
                        Map<String, Map<String, Set<TopicPartition>>> assignments) {
            this(loads, assignments, Map.of());
        }

        public Snapshot {
            loads = Map.copyOf(loads);
            topologies = Map.copyOf(topologies);
            Map<String, Map<String, Set<TopicPartition>>> copy = new TreeMap<>();
            assignments.forEach((group, members) -> {
                Map<String, Set<TopicPartition>> memberCopy = new TreeMap<>();
                members.forEach((member, partitions) -> memberCopy.put(member, Set.copyOf(partitions)));
                copy.put(group, Map.copyOf(memberCopy));
            });
            assignments = Map.copyOf(copy);
        }
    }
}
