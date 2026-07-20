package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface BalanceService {

    /**
     * Computes a complete assignment of {@code partitionWeights.keySet()} to the given members.
     *
     * <p>Members sharing a {@link GroupMember#instanceId()} run in the same application
     * instance, so the primary objective is an even total load per instance; spreading load
     * across the members inside an instance is secondary.
     *
     * @param members          every group member (member ids unique across the collection)
     *                         with the instance it runs in and the topics it subscribed to;
     *                         a partition may only be assigned to a member subscribed to its
     *                         topic
     * @param partitionWeights every partition that must be assigned, mapped to its finite
     *                         weight
     * @return a map with an entry for every member (possibly an empty list); together the lists
     *         contain every key of {@code partitionWeights} exactly once
     */
    Map<String, List<TopicPartition>> computeOptimalAssignment(
            Collection<GroupMember> members,
            Map<TopicPartition, Double> partitionWeights);
}
