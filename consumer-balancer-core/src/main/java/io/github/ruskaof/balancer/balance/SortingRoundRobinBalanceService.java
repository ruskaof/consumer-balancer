package io.github.ruskaof.balancer.balance;

import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Greedy least-loaded assignment: partitions are placed heaviest-first onto the eligible
 * member (one subscribed to the partition's topic) with the lowest accumulated load,
 * breaking ties by member id.
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

            String leastLoadedMember = consumerLoads.entrySet().stream()
                    .filter(e -> {
                        Set<String> topics = subscribedTopicsByMember.get(e.getKey());
                        return topics != null && topics.contains(partition.topic());
                    })
                    .min((e1, e2) -> {
                        int loadCompare = Double.compare(e1.getValue(), e2.getValue());
                        return (loadCompare != 0)
                                ? loadCompare
                                : e1.getKey().compareTo(e2.getKey());
                    })
                    .orElseThrow(() -> new IllegalStateException(
                            "No member is subscribed to topic '" + partition.topic()
                                    + "'; cannot assign partition " + partition))
                    .getKey();

            assignment.get(leastLoadedMember).add(partition);
            consumerLoads.put(leastLoadedMember, consumerLoads.get(leastLoadedMember) + partitionLoad);

            if (log.isTraceEnabled()) {
                log.trace("Assigned partition {} (load={}) to member {} (new load={})",
                        partition, partitionLoad, leastLoadedMember,
                        consumerLoads.get(leastLoadedMember));
            }
        }

        log.info("Computed load-aware assignment for {} partitions across {} members",
                sortedPartitions.size(), assignment.size());
        log.debug("Computed load-aware assignment: {}", assignment);
        return assignment;
    }
}
