package io.github.ruskaof.balancer.trigger;

import java.util.List;

/**
 * Combines triggers with {@link Mode#ALL} (every trigger must fire) or {@link Mode#ANY}
 * (at least one must fire).
 *
 * <p>Every trigger is evaluated on every check — there is no short-circuiting — so
 * stateful triggers such as {@link PeriodicTrigger} observe each evaluation cycle.
 */
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
        boolean all = true;
        boolean any = false;
        for (RebalanceTrigger trigger : triggers) {
            boolean fired = trigger.shouldTrigger();
            all &= fired;
            any |= fired;
        }
        return switch (mode) {
            case ALL -> all;
            case ANY -> any;
        };
    }
}
