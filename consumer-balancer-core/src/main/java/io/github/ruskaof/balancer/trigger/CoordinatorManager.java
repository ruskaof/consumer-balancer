package io.github.ruskaof.balancer.trigger;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
public class CoordinatorManager implements AutoCloseable {

    private final CoordinatorElection election;
    private final RebalanceTrigger trigger;
    private final RebalanceInitiator rebalanceInitiator;
    private final long triggerCheckIntervalMs;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean monitoring = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(true);

    @FunctionalInterface
    public interface RebalanceInitiator {
        /**
         * Called when trigger condition is met. User implements rebalance logic here.
         */
        void initiateRebalance();
    }

    public CoordinatorManager(
            CoordinatorElection election,
            RebalanceTrigger trigger,
            RebalanceInitiator rebalanceInitiator,
            long triggerCheckIntervalMs) {
        this.election = election;
        this.trigger = trigger;
        this.rebalanceInitiator = rebalanceInitiator;
        this.triggerCheckIntervalMs = triggerCheckIntervalMs;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "coordinator-trigger-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        election.start();
        election.addListener(this::onCoordinatorStatusChange);
    }

    private void onCoordinatorStatusChange(boolean isCoordinator) {
        if (isCoordinator && monitoring.compareAndSet(false, true)) {
            log.info("Became coordinator - starting trigger monitoring");
            scheduler.scheduleAtFixedRate(
                    this::evaluateTrigger,
                    0,
                    triggerCheckIntervalMs,
                    TimeUnit.MILLISECONDS);
        } else if (!isCoordinator && monitoring.compareAndSet(true, false)) {
            log.info("Lost coordinator status - stopping trigger monitoring");
            scheduler.shutdownNow();
        }
    }

    private void evaluateTrigger() {
        log.info("Evaluating trigger in Coordinator manager");
        if (!running.get() || !election.isCoordinator())
            return;

        try {
            if (trigger.shouldTrigger()) {
                log.warn("Trigger condition met! Initiating rebalance...");

                monitoring.set(false);

                rebalanceInitiator.initiateRebalance();
            }
        } catch (Exception e) {
            log.error("Error evaluating trigger", e);
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            if (monitoring.get())
                scheduler.shutdownNow();
            election.close();
        }
    }
}