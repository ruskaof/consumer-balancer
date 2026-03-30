package com.ruskaof.balancer.trigger;

@FunctionalInterface
public interface RebalanceTrigger {
    boolean shouldTrigger();
}
