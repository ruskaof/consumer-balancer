package io.github.ruskaof.balancer.trigger;

@FunctionalInterface
public interface RebalanceTrigger {
    boolean shouldTrigger();
}
