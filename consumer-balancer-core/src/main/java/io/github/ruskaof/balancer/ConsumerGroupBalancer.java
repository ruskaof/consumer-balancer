package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.RebalanceInitiator;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One consumer group registered with {@link ConsumerGroupBalancers}: its coordinator election,
 * rebalance trigger and rebalance initiator. Created only by the registry, which also starts
 * and closes it.
 *
 * <p>A group registered while proactive rebalance is off is <em>passive</em>: it runs no
 * threads, has no trigger and never rebalances — its consumers are still balanced by the
 * assignor at every regular rebalance.
 */
public final class ConsumerGroupBalancer {

    private final String groupId;
    private final RebalanceTrigger trigger;
    private final RebalanceInitiator rebalanceInitiator;
    private final CoordinatorManager coordinatorManager;
    private final AtomicBoolean started = new AtomicBoolean();

    ConsumerGroupBalancer(
            String groupId,
            RebalanceTrigger trigger,
            RebalanceInitiator rebalanceInitiator,
            CoordinatorManager coordinatorManager) {
        this.groupId = groupId;
        this.trigger = trigger;
        this.rebalanceInitiator = rebalanceInitiator;
        this.coordinatorManager = coordinatorManager;
    }

    static ConsumerGroupBalancer passive(String groupId) {
        return new ConsumerGroupBalancer(groupId, null, null, null);
    }

    public String getGroupId() {
        return groupId;
    }

    /** Whether this group runs coordinator election and may be rebalanced proactively. */
    public boolean isProactive() {
        return coordinatorManager != null;
    }

    /** Whether this JVM currently holds the group's coordinator role; safe to call from any thread. */
    public boolean isCoordinator() {
        return coordinatorManager != null && coordinatorManager.isCoordinator();
    }

    /** The trigger the coordinator evaluates; {@code null} for a passive group. */
    public RebalanceTrigger getTrigger() {
        return trigger;
    }

    /** What the coordinator calls when the trigger fires; {@code null} for a passive group. */
    public RebalanceInitiator getRebalanceInitiator() {
        return rebalanceInitiator;
    }

    void start() {
        if (coordinatorManager != null && started.compareAndSet(false, true)) {
            coordinatorManager.start();
        }
    }

    void close() {
        if (coordinatorManager != null) {
            coordinatorManager.close();
        }
    }

    @Override
    public String toString() {
        return "ConsumerGroupBalancer[" + groupId + (isProactive() ? "" : ", passive") + "]";
    }
}
