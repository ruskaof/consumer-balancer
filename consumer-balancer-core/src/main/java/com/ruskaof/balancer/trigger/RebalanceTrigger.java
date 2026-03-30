package com.ruskaof.listener.trigger;

@FunctionalInterface
public interface RebalanceTrigger {
    boolean shouldTrigger();
}
