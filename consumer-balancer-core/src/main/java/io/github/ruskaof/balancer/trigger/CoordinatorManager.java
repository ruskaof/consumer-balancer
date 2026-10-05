package io.github.ruskaof.balancer.trigger;

import io.github.ruskaof.balancer.MemberIdTracker;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
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
    private final MemberIdTracker memberIdTracker;

    private final String groupId;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean monitoring = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile ScheduledFuture<?> triggerFuture;

    public CoordinatorManager(
            CoordinatorElection election,
            RebalanceTrigger trigger,
            RebalanceInitiator rebalanceInitiator,
            long triggerCheckIntervalMs) {
        this.election = election;
        this.trigger = trigger;
        this.rebalanceInitiator = rebalanceInitiator;
        this.triggerCheckIntervalMs = triggerCheckIntervalMs;
        this.memberIdTracker = Objects.requireNonNull(election.memberIdTracker(), "memberIdTracker");
        this.groupId = election.getGroupId();
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "coordinator-trigger-" + groupId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        // The listener must be registered before the first election runs; otherwise an
        // immediate election result would be notified into an empty listener list and
        // monitoring would only start on the next status change.
        election.addListener(this::onCoordinatorStatusChange);
        // A restarted static follower must be able to refresh a cached assignment even
        // while it is not the monitor. This timer only reads local state until needed.
        scheduler.scheduleWithFixedDelay(this::refreshTopology, 0,
                Math.min(1_000, triggerCheckIntervalMs), TimeUnit.MILLISECONDS);
        election.start();
    }

    /** Whether this JVM currently holds the coordinator role; safe to call from any thread. */
    public boolean isCoordinator() {
        return election.isCoordinator();
    }

    private void onCoordinatorStatusChange(boolean isCoordinator) {
        if (isCoordinator && monitoring.compareAndSet(false, true)) {
            log.info("Became coordinator of group '{}' - starting trigger monitoring", groupId);
            triggerFuture = scheduler.scheduleWithFixedDelay(
                    this::evaluateTrigger,
                    0,
                    triggerCheckIntervalMs,
                    TimeUnit.MILLISECONDS);
        } else if (!isCoordinator && monitoring.compareAndSet(true, false)) {
            log.info("Lost coordinator status of group '{}' - stopping trigger monitoring", groupId);
            cancelTriggerFuture();
        }
    }

    private void evaluateTrigger() {
        log.debug("Evaluating rebalance trigger of group '{}'", groupId);
        if (!running.get() || !election.isCoordinator())
            return;

        try {
            MemberIdTracker.SnapshotToken observed = memberIdTracker.monitoringSnapshot(groupId);
            if (trigger.shouldTrigger() && running.get() && !Thread.currentThread().isInterrupted()
                    && election.confirmCoordinator() && observed != null
                    && memberIdTracker.prepareRebalance(groupId, observed)) {
                log.warn("Trigger condition met for group '{}'! Initiating rebalance...", groupId);
                rebalanceInitiator.initiateRebalance();
            }
        } catch (Exception e) {
            log.error("Error evaluating the trigger of group '{}'", groupId, e);
        }
    }

    private void refreshTopology() {
        if (!running.get()) return;
        try {
            if (!memberIdTracker.topologyRefreshDue(groupId, System.nanoTime())) return;
            if (!election.confirmStableGroup()) {
                memberIdTracker.deferTopologyRefresh(groupId, System.nanoTime());
                return;
            }
            if (running.get() && memberIdTracker.claimTopologyRefresh(groupId, System.nanoTime())) {
                rebalanceInitiator.initiateRebalance();
            }
        } catch (Exception e) {
            log.warn("Could not request a topology refresh for group '{}'", groupId, e);
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
