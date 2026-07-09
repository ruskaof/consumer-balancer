package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BalanceService {

    /**
     * Computes a complete assignment of {@code partitionWeights.keySet()} to the given members.
     *
     * @param subscribedTopicsByMember every group member mapped to the topics it subscribed to;
     *                                 a partition may only be assigned to a member subscribed
     *                                 to its topic
     * @param partitionWeights         every partition that must be assigned, mapped to its
     *                                 finite weight
     * @return a map with an entry for every member (possibly an empty list); together the lists
     *         contain every key of {@code partitionWeights} exactly once
     */
    Map<String, List<TopicPartition>> computeOptimalAssignment(
            Map<String, Set<String>> subscribedTopicsByMember,
            Map<TopicPartition, Double> partitionWeights);
}
