package io.github.ruskaof.balancer.trigger;

/**
 * Forces a rebalance of one consumer group's members in this JVM, e.g. by calling
 * {@code enforceRebalance()} on their listener containers. Called by the elected coordinator
 * whenever its {@link RebalanceTrigger} fires.
 */
@FunctionalInterface
public interface RebalanceInitiator {

    void initiateRebalance();
}
