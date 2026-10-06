package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /**
     * Computes an assignment while accounting for load from other consumer groups on each
     * application instance. These loads must exclude the group being assigned and use the
     * same weight units as {@code partitionWeights}. Missing instances have zero other-group
     * load; instances with no member in this group do not participate in placement.
     *
     * <p>Implementations that support this overload must count each instance's other-group
     * load once, regardless of its number of members, and require finite, nonnegative loads.
     * The default implementation preserves existing implementations for an empty load map
     * and rejects nonempty maps so unsupported load accounting cannot be silently ignored.
     *
     * @param otherGroupLoadsByInstance load from other consumer groups, keyed by instance id
     * @throws UnsupportedOperationException if other-group loads are supplied to an
     *                                       implementation that does not support them
     */
    default Map<String, List<TopicPartition>> computeOptimalAssignment(
            Collection<GroupMember> members,
            Map<TopicPartition, Double> partitionWeights,
            Map<String, Double> otherGroupLoadsByInstance) {
        Objects.requireNonNull(otherGroupLoadsByInstance, "otherGroupLoadsByInstance");
        if (!otherGroupLoadsByInstance.isEmpty()) {
            throw new UnsupportedOperationException("This balance service does not support other-group loads");
        }
        return computeOptimalAssignment(members, partitionWeights);
    }
}
