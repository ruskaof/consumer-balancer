package io.github.ruskaof.balancer.autoconfigure;

/**
 * Selects which {@link io.github.ruskaof.balancer.trigger.RebalanceTrigger}
 * the auto-configuration wires in. Bound from {@code consumer-balancer.trigger-type}.
 */
public enum TriggerType {
    /**
     * Compares current max member load against the optimal assignment's max load.
     */
    THRESHOLD,
    /**
     * Fires when the consumer group's member set changes (join/leave/crash).
     */
    MEMBERSHIP_CHANGE,
    /**
     * Kafka-native: fires on uneven consumer lag between members.
     */
    CONSUMER_LAG,
    /**
     * Fires on the coefficient of variation of per-member weighted load.
     */
    LOAD_VARIANCE
}
