package io.github.ruskaof.balancer.balance;

import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Greedy least-loaded assignment in two levels: partitions are placed heaviest-first onto the
 * eligible application instance (one having a member subscribed to the partition's topic) with
 * the lowest accumulated load, then onto the least-loaded subscribed member inside that
 * instance. Traffic therefore evens out across instances first — the members of an instance
 * share its resources — and across the members of each instance second. Ties are broken by
 * instance id, then member id. Zero-weight partitions never change a load, so they are spread
 * by assigned-partition count instead, at both levels.
 *
 * <p>Instance selection deliberately ignores how many members an instance has: every instance
 * is expected to carry the same traffic, so an instance with fewer members receives the same
 * load on fewer, busier members. When every member is its own instance (e.g. members that did
 * not report an instance id), the instance level degenerates and this is plain member-level
 * least-loaded greedy.
 */
@Slf4j
public class SortingRoundRobinBalanceService implements BalanceService {

    @Override
    public Map<String, List<TopicPartition>> computeOptimalAssignment(
            Collection<GroupMember> members,
            Map<TopicPartition, Double> partitionWeights) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("No members provided for assignment");
        }

        Map<String, List<TopicPartition>> assignment = new TreeMap<>();
        Map<String, Double> memberLoads = new TreeMap<>();
        Map<String, Set<String>> topicsByMember = new HashMap<>();
        Map<String, Double> instanceLoads = new TreeMap<>();
        Map<String, Integer> instanceCounts = new TreeMap<>();
        Map<String, SortedSet<String>> membersByInstance = new TreeMap<>();
        for (GroupMember member : members) {
            if (assignment.putIfAbsent(member.memberId(), new ArrayList<>()) != null) {
                throw new IllegalArgumentException("Duplicate member id: " + member.memberId());
            }
            memberLoads.put(member.memberId(), 0.0);
            topicsByMember.put(member.memberId(), member.subscribedTopics());
            instanceLoads.putIfAbsent(member.instanceId(), 0.0);
            instanceCounts.putIfAbsent(member.instanceId(), 0);
            membersByInstance.computeIfAbsent(member.instanceId(), id -> new TreeSet<>())
                    .add(member.memberId());
        }

        if (partitionWeights == null || partitionWeights.isEmpty()) {
            return assignment;
        }
        if (membersByInstance.size() > partitionWeights.size()) {
            log.warn("More instances ({}) than partitions ({}); some instances will receive nothing",
                    membersByInstance.size(), partitionWeights.size());
        }

        List<TopicPartition> sortedPartitions = partitionWeights.keySet().stream()
                .sorted((tp1, tp2) -> Double.compare(
                        partitionWeights.get(tp2),
                        partitionWeights.get(tp1)))
                .toList();

        for (TopicPartition partition : sortedPartitions) {
            double partitionLoad = partitionWeights.getOrDefault(partition, PartitionWeightDefaults.MISSING);
            // A zero-weight partition never increases a load, so choosing by load would elect
            // the same instance and member every time; spread those by count instead.
            boolean spreadByCount = partitionLoad <= 0.0;

            String targetInstance = membersByInstance.entrySet().stream()
                    .filter(e -> e.getValue().stream()
                            .anyMatch(m -> topicsByMember.get(m).contains(partition.topic())))
                    .map(Map.Entry::getKey)
                    .min((i1, i2) -> {
                        int primaryCompare = spreadByCount
                                ? Integer.compare(instanceCounts.get(i1), instanceCounts.get(i2))
                                : Double.compare(instanceLoads.get(i1), instanceLoads.get(i2));
                        return (primaryCompare != 0) ? primaryCompare : i1.compareTo(i2);
                    })
                    .orElseThrow(() -> new IllegalStateException(
                            "No member is subscribed to topic '" + partition.topic()
                                    + "'; cannot assign partition " + partition));

            String targetMember = membersByInstance.get(targetInstance).stream()
                    .filter(m -> topicsByMember.get(m).contains(partition.topic()))
                    .min((m1, m2) -> {
                        int primaryCompare = spreadByCount
                                ? Integer.compare(assignment.get(m1).size(), assignment.get(m2).size())
                                : Double.compare(memberLoads.get(m1), memberLoads.get(m2));
                        return (primaryCompare != 0) ? primaryCompare : m1.compareTo(m2);
                    })
                    .orElseThrow(() -> new IllegalStateException(
                            "Instance '" + targetInstance + "' was selected for partition " + partition
                                    + " but has no member subscribed to its topic"));

            assignment.get(targetMember).add(partition);
            memberLoads.put(targetMember, memberLoads.get(targetMember) + partitionLoad);
            instanceLoads.put(targetInstance, instanceLoads.get(targetInstance) + partitionLoad);
            instanceCounts.put(targetInstance, instanceCounts.get(targetInstance) + 1);

            if (log.isTraceEnabled()) {
                log.trace("Assigned partition {} (load={}) to member {} of instance {} (member load={}, instance load={})",
                        partition, partitionLoad, targetMember, targetInstance,
                        memberLoads.get(targetMember), instanceLoads.get(targetInstance));
            }
        }

        log.info("Computed load-aware assignment for {} partitions across {} members in {} instances",
                sortedPartitions.size(), assignment.size(), membersByInstance.size());
        log.debug("Computed load-aware assignment: {}", assignment);
        return assignment;
    }
}
