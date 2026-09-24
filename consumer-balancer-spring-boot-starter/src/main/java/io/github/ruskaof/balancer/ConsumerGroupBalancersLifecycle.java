package io.github.ruskaof.balancer;

import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.Objects;

/**
 * Starts every {@link ConsumerGroupBalancers} bean of the context — the auto-configured one
 * and any the application declares, e.g. for a second Kafka cluster — once the context is
 * refreshed, and closes them when it stops. Running in the default {@link SmartLifecycle}
 * phase, it stops before the listener containers do, so no proactive rebalance is fired at a
 * container that is shutting down.
 */
public class ConsumerGroupBalancersLifecycle implements SmartLifecycle {

    private final List<ConsumerGroupBalancers> balancers;
    private volatile boolean running = false;

    public ConsumerGroupBalancersLifecycle(List<ConsumerGroupBalancers> balancers) {
        this.balancers = List.copyOf(Objects.requireNonNull(balancers, "balancers"));
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        balancers.forEach(ConsumerGroupBalancers::start);
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        balancers.forEach(ConsumerGroupBalancers::close);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
