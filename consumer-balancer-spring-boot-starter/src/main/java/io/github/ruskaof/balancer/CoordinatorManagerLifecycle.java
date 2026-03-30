package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

@Slf4j
@RequiredArgsConstructor
public class CoordinatorManagerLifecycle implements SmartLifecycle {

    private final CoordinatorManager coordinatorManager;
    private volatile boolean running = false;

    @Override
    public void start() {
        if (running) {
            return;
        }
        coordinatorManager.start();
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        coordinatorManager.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
