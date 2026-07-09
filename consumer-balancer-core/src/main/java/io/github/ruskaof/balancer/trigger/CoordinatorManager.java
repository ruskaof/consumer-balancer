package io.github.ruskaof.balancer.trigger;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CoordinatorManager implements AutoCloseable {

    private final CoordinatorElection election;
    private final RebalanceTrigger trigger;
    private final RebalanceInitiator rebalanceInitiator;
    private final long triggerCheckIntervalMs;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean monitoring = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile ScheduledFuture<?> triggerFuture;

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
        // The listener must be registered before the first election runs; otherwise an
        // immediate election result would be notified into an empty listener list and
        // monitoring would only start on the next status change.
        election.addListener(this::onCoordinatorStatusChange);
        election.start();
    }

    private void onCoordinatorStatusChange(boolean isCoordinator) {
        if (isCoordinator && monitoring.compareAndSet(false, true)) {
            log.info("Became coordinator - starting trigger monitoring");
            triggerFuture = scheduler.scheduleWithFixedDelay(
                    this::evaluateTrigger,
                    0,
                    triggerCheckIntervalMs,
                    TimeUnit.MILLISECONDS);
        } else if (!isCoordinator && monitoring.compareAndSet(true, false)) {
            log.info("Lost coordinator status - stopping trigger monitoring");
            cancelTriggerFuture();
        }
    }

    private void evaluateTrigger() {
        log.debug("Evaluating rebalance trigger");
        if (!running.get() || !election.isCoordinator())
            return;

        try {
            if (trigger.shouldTrigger()) {
                log.warn("Trigger condition met! Initiating rebalance...");
                rebalanceInitiator.initiateRebalance();
            }
        } catch (Exception e) {
            log.error("Error evaluating trigger", e);
        }
    }

    private void cancelTriggerFuture() {
        ScheduledFuture<?> f = triggerFuture;
        if (f != null) {
            f.cancel(true);
            triggerFuture = null;
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            cancelTriggerFuture();
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            election.close();
        }
    }
}
