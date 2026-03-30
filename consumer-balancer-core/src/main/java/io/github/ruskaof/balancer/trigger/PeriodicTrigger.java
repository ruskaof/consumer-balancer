package io.github.ruskaof.balancer.trigger;

import java.util.concurrent.atomic.AtomicLong;

public class PeriodicTrigger implements RebalanceTrigger {
    private final long intervalMs;
    private final AtomicLong lastTriggerTime = new AtomicLong(0);

    public PeriodicTrigger(long intervalMs) {
        if (intervalMs <= 0)
            throw new IllegalArgumentException("Interval must be positive");
        this.intervalMs = intervalMs;
    }

    @Override
    public boolean shouldTrigger() {
        long now = System.currentTimeMillis();
        long last = lastTriggerTime.get();
        if (now - last >= intervalMs) {
            return lastTriggerTime.compareAndSet(last, now);
        }
        return false;
    }
}
