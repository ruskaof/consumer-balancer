package io.github.ruskaof.balancer.balance;

import java.util.Objects;
import java.util.Set;

/**
 * A consumer-group member as seen by the {@link BalanceService}: its Kafka member id, the
 * application instance (pod/JVM) it runs in, and the topics it subscribed to. Members that
 * share an {@code instanceId} compete for the same instance's resources, so the balancer
 * evens load across instances first and only then across the members inside each instance.
 */
public record GroupMember(String memberId, String instanceId, Set<String> subscribedTopics) {

    public GroupMember {
        Objects.requireNonNull(memberId, "memberId");
        Objects.requireNonNull(instanceId, "instanceId");
        subscribedTopics = Set.copyOf(Objects.requireNonNull(subscribedTopics, "subscribedTopics"));
    }
}
