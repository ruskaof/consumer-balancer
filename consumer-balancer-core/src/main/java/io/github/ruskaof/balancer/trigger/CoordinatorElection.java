package io.github.ruskaof.balancer.trigger;

import io.github.ruskaof.balancer.MemberIdTracker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.GroupState;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CoordinatorElection implements AutoCloseable {

    private static final long DESCRIBE_TIMEOUT_MS = 30_000L;

    private final String groupId;
    private final MemberIdTracker memberIdTracker;
    private final AdminClient adminClient;
    private final boolean closeAdminClientOnShutdown;
    private final long electionIntervalMs;
    private final AtomicBoolean isCoordinator = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<CoordinatorStatusListener> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean groupStable;

    private CoordinatorElection(Builder builder) {
        this.groupId = builder.groupId;
        this.memberIdTracker = builder.memberIdTracker;
        this.electionIntervalMs = builder.electionIntervalMs;
        if (builder.adminClient != null) {
            this.adminClient = builder.adminClient;
            this.closeAdminClientOnShutdown = false;
        } else {
            this.adminClient = AdminClient.create(builder.adminProps);
            this.closeAdminClientOnShutdown = true;
        }
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "coordinator-election-" + groupId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.get()) {
            scheduler.scheduleWithFixedDelay(this::runElection, 0, electionIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void runElection() {
        if (!running.get())
            return;

        try {
            DescribeConsumerGroupsResult result = adminClient
                    .describeConsumerGroups(Collections.singletonList(groupId));
            ConsumerGroupDescription desc = result.describedGroups().get(groupId)
                    .get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            groupStable = desc.groupState() == GroupState.STABLE;
            if (groupStable) memberIdTracker.reconcileMembership(groupId, desc.members());

            String selected = memberIdTracker.coordinatorMemberId(groupId, desc.members());
            updateStatus(groupStable && selected != null);
        } catch (InterruptedException e) {
            groupStable = false;
            updateStatus(false);
            Thread.currentThread().interrupt();
            log.warn("Interrupted during election for group '{}'", groupId);
        } catch (Exception e) {
            groupStable = false;
            updateStatus(false);
            log.warn("Election failed for group '{}'", groupId, e);
        }
    }

    private void updateStatus(boolean newStatus) {
        if (isCoordinator.getAndSet(newStatus) != newStatus) {
            log.info("Coordinator status changed [group={}]: isCoordinator={}", groupId, newStatus);
            notifyListeners(newStatus);
        }
    }

    /** Revalidate ownership after an expensive trigger evaluation and before requesting a rebalance. */
    public synchronized boolean confirmCoordinator() {
        if (!running.get()) return false;
        runElection();
        return running.get() && isCoordinator.get();
    }

    /** Recovery may run on a follower, but must not repeatedly interrupt a rebalance. */
    public synchronized boolean confirmStableGroup() {
        if (!running.get()) return false;
        runElection();
        return running.get() && groupStable;
    }

    public String getGroupId() {
        return groupId;
    }

    MemberIdTracker memberIdTracker() {
        return memberIdTracker;
    }

    /** Current coordinator status (thread-safe) */
    public boolean isCoordinator() {
        return isCoordinator.get();
    }

    /** Register status change listener */
    public void addListener(CoordinatorStatusListener listener) {
        listeners.add(listener);
    }

    public void removeListener(CoordinatorStatusListener listener) {
        listeners.remove(listener);
    }

    private void notifyListeners(boolean isCoordinator) {
        for (CoordinatorStatusListener listener : listeners) {
            try {
                listener.onCoordinatorStatusChanged(isCoordinator);
            } catch (Exception e) {
                log.error("Listener failed", e);
            }
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS))
                    scheduler.shutdownNow();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            if (closeAdminClientOnShutdown) {
                adminClient.close();
            }
            log.info("Election stopped for group '{}'", groupId);
        }
    }

    public static class Builder {
        private String groupId;
        private MemberIdTracker memberIdTracker;
        private long electionIntervalMs = 30_000;
        private Properties adminProps = new Properties();
        /**
         * When set, used instead of creating a new {@link AdminClient} from
         * {@link #adminProps}.
         */
        private AdminClient adminClient;

        public CoordinatorElection build() {
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalArgumentException("Group id is required");
            }
            if (memberIdTracker == null) {
                throw new IllegalArgumentException("The assignor's MemberIdTracker is required for monitor election");
            }
            if (adminClient == null && !adminProps.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                throw new IllegalArgumentException("Bootstrap servers are required when admin client is not provided");
            }

            return new CoordinatorElection(this);
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        /** Follow the monitor designated by the assignor's topology snapshot. */
        public Builder setMemberIdTracker(MemberIdTracker memberIdTracker) {
            this.memberIdTracker = Objects.requireNonNull(memberIdTracker, "memberIdTracker");
            return this;
        }

        public Builder setElectionIntervalMs(long electionIntervalMs) {
            this.electionIntervalMs = electionIntervalMs;
            return this;
        }

        public Builder setAdminProps(Properties adminProps) {
            this.adminProps.putAll(adminProps);
            return this;
        }

        public Builder setAdminClient(AdminClient adminClient) {
            this.adminClient = adminClient;
            return this;
        }
    }

    @FunctionalInterface
    public interface CoordinatorStatusListener {
        void onCoordinatorStatusChanged(boolean isCoordinator);
    }
}
