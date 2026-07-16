package io.github.ruskaof.balancer.balance;

import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Greedy least-loaded assignment: partitions are placed heaviest-first onto the eligible
 * member (one subscribed to the partition's topic) with the lowest accumulated load,
 * breaking ties by member id. Zero-weight partitions never change a member's load, so
 * they are spread across eligible members by assigned-partition count instead.
 */
@Slf4j
public class SortingRoundRobinBalanceService implements BalanceService {

    @Override
    public Map<String, List<TopicPartition>> computeOptimalAssignment(
            Map<String, Set<String>> subscribedTopicsByMember,
            Map<TopicPartition, Double> partitionWeights) {
        if (subscribedTopicsByMember == null || subscribedTopicsByMember.isEmpty()) {
            throw new IllegalArgumentException("No members provided for assignment");
        }

        Map<String, Double> consumerLoads = new TreeMap<>();
        Map<String, List<TopicPartition>> assignment = new TreeMap<>();
        for (String member : subscribedTopicsByMember.keySet()) {
            consumerLoads.put(member, 0.0);
            assignment.put(member, new ArrayList<>());
        }

        if (partitionWeights == null || partitionWeights.isEmpty()) {
            return assignment;
        }

        List<TopicPartition> sortedPartitions = partitionWeights.keySet().stream()
                .sorted((tp1, tp2) -> Double.compare(
                        partitionWeights.get(tp2),
                        partitionWeights.get(tp1)))
                .toList();

        for (TopicPartition partition : sortedPartitions) {
            double partitionLoad = partitionWeights.getOrDefault(partition, PartitionWeightDefaults.MISSING);
            // A zero-weight partition never increases its owner's load, so choosing by load
            // would elect the same member for every such partition; spread those by count.
            boolean spreadByCount = partitionLoad <= 0.0;

            String targetMember = consumerLoads.entrySet().stream()
                    .filter(e -> {
                        Set<String> topics = subscribedTopicsByMember.get(e.getKey());
                        return topics != null && topics.contains(partition.topic());
                    })
                    .min((e1, e2) -> {
                        int primaryCompare = spreadByCount
                                ? Integer.compare(assignment.get(e1.getKey()).size(),
                                        assignment.get(e2.getKey()).size())
                                : Double.compare(e1.getValue(), e2.getValue());
                        return (primaryCompare != 0)
                                ? primaryCompare
                                : e1.getKey().compareTo(e2.getKey());
                    })
                    .orElseThrow(() -> new IllegalStateException(
                            "No member is subscribed to topic '" + partition.topic()
                                    + "'; cannot assign partition " + partition))
                    .getKey();

            assignment.get(targetMember).add(partition);
            consumerLoads.put(targetMember, consumerLoads.get(targetMember) + partitionLoad);

            if (log.isTraceEnabled()) {
                log.trace("Assigned partition {} (load={}) to member {} (new load={})",
                        partition, partitionLoad, targetMember,
                        consumerLoads.get(targetMember));
            }
        }

        log.info("Computed load-aware assignment for {} partitions across {} members",
                sortedPartitions.size(), assignment.size());
        log.debug("Computed load-aware assignment: {}", assignment);
        return assignment;
    }
}
