package com.ruskaof.balancer.trigger;

import java.util.List;

public class CompositeTrigger implements RebalanceTrigger {

    public enum Mode {
        ALL, ANY
    }

    private final List<RebalanceTrigger> triggers;
    private final Mode mode;

    public CompositeTrigger(List<RebalanceTrigger> triggers, Mode mode) {
        if (triggers == null || triggers.isEmpty()) {
            throw new IllegalArgumentException("At least one trigger is required");
        }
        this.triggers = List.copyOf(triggers);
        this.mode = mode;
    }

    @Override
    public boolean shouldTrigger() {
        return switch (mode) {
            case ALL -> triggers.stream().allMatch(RebalanceTrigger::shouldTrigger);
            case ANY -> triggers.stream().anyMatch(RebalanceTrigger::shouldTrigger);
        };
    }
}
