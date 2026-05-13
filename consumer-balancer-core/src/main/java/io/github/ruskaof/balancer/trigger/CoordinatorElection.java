package io.github.ruskaof.balancer.trigger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
public class CoordinatorElection implements AutoCloseable {

    private final String groupId;
    private final Supplier<Set<String>> memberIdsSupplier;
    private final AdminClient adminClient;
    private final boolean closeAdminClientOnShutdown;
    private final long electionIntervalMs;
    private final AtomicBoolean isCoordinator = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<CoordinatorStatusListener> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private CoordinatorElection(Builder builder) {
        this.groupId = builder.groupId;
        this.memberIdsSupplier = builder.memberIdsSupplier;
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
            scheduler.scheduleAtFixedRate(this::runElection, 0, electionIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void runElection() {
        if (!running.get())
            return;

        try {
            DescribeConsumerGroupsResult result = adminClient
                    .describeConsumerGroups(Collections.singletonList(groupId));
            ConsumerGroupDescription desc = result.describedGroups().get(groupId).get();

            List<String> sortedMembers = desc.members().stream()
                    .map(MemberDescription::consumerId)
                    .sorted()
                    .toList();

            if (sortedMembers.isEmpty()) {
                if (isCoordinator.getAndSet(false)) {
                    log.info("No members in group '{}' - resigned coordinator", groupId);
                    notifyListeners(false);
                }
                return;
            }

            Set<String> currentMemberIds = memberIdsSupplier.get();

            boolean newStatus = currentMemberIds.stream()
                    .anyMatch((memberId) -> memberId.equals(sortedMembers.getFirst()));
            log.info("Current memberIds: {}, sortedMemberIds:{}, election result: {}", currentMemberIds, sortedMembers,
                    newStatus);

            if (isCoordinator.getAndSet(newStatus) != newStatus) {
                log.info("Coordinator status changed [group={}]: isCoordinator={} (memberIds={}, smallest={})",
                        groupId, newStatus, currentMemberIds,
                        sortedMembers.isEmpty() ? "N/A" : sortedMembers.getFirst());
                notifyListeners(newStatus);
            }
        } catch (Exception e) {
            log.warn("Election failed for group '{}'", groupId, e);
        }
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
        private Supplier<Set<String>> memberIdsSupplier;
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
            if (Objects.isNull(memberIdsSupplier)) {
                throw new IllegalArgumentException("Member id supplier is required");
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

        public Builder setMemberIdsSupplier(Supplier<Set<String>> memberIdsSupplier) {
            this.memberIdsSupplier = memberIdsSupplier;
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
