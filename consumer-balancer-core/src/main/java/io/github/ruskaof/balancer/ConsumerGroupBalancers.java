package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import io.github.ruskaof.balancer.trigger.CoordinatorElection;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import io.github.ruskaof.balancer.trigger.RebalanceInitiator;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The balancer of one Kafka cluster: the collaborators every consumer group on that cluster
 * shares, plus a registry of the groups that are balanced proactively.
 *
 * <p>Two things connect a consumer group to it:
 * <ol>
 *   <li>its consumers use {@link LoadAwarePartitionAssignor} configured with
 *       {@link #assignorConfigs()} — the same weight store, balance service, instance id and
 *       {@link MemberIdTracker} for every group;</li>
 *   <li>the group is {@linkplain #register(String, RebalanceInitiator) registered}, which gives
 *       it a coordinator election and a {@link ThresholdTrigger} reading that same tracker, and
 *       names what forces the rebalance when the trigger fires.</li>
 * </ol>
 *
 * <p>Groups can be registered at any time. Groups registered before {@link #start()} begin
 * working when it is called, later ones immediately; {@link #close()} stops them all. The
 * registry never closes the admin client or the weight store it was given.
 *
 * <p>One registry serves one cluster. An application consuming from several clusters builds
 * one registry per cluster — the group ids of different clusters may collide, and the
 * tracker, admin client and weight store must not be shared between them.
 */
@Slf4j
public final class ConsumerGroupBalancers implements AutoCloseable {

    public static final double DEFAULT_IMBALANCE_THRESHOLD = 1.1d;
    public static final Duration DEFAULT_ELECTION_INTERVAL = Duration.ofSeconds(30);
    public static final Duration DEFAULT_TRIGGER_CHECK_INTERVAL = Duration.ofSeconds(30);

    private enum State { NEW, STARTED, CLOSED }

    private final AdminClient adminClient;
    private final WeightService weightService;
    private final BalanceService balanceService;
    private final MemberIdTracker memberIdTracker;
    private final String instanceId;
    private final boolean proactiveRebalance;
    private final double imbalanceThreshold;
    private final RebalanceDamping damping;
    private final Duration electionInterval;
    private final Duration triggerCheckInterval;
    private final Map<String, String> tags;

    private final Object lock = new Object();
    private final Map<String, ConsumerGroupBalancer> groups = new LinkedHashMap<>(); // guarded by lock
    private final List<Consumer<ConsumerGroupBalancer>> listeners = new ArrayList<>(); // guarded by lock
    private State state = State.NEW; // guarded by lock

    private ConsumerGroupBalancers(Builder builder) {
        this.adminClient = Objects.requireNonNull(builder.adminClient, "adminClient");
        this.weightService = Objects.requireNonNull(builder.weightService, "weightService");
        this.balanceService = builder.balanceService != null
                ? builder.balanceService
                : new SortingRoundRobinBalanceService();
        this.memberIdTracker = builder.memberIdTracker != null ? builder.memberIdTracker : new MemberIdTracker();
        this.instanceId = builder.instanceId;
        this.proactiveRebalance = builder.proactiveRebalance;
        this.imbalanceThreshold = builder.imbalanceThreshold;
        this.damping = Objects.requireNonNull(builder.damping, "damping");
        this.electionInterval = requirePositive(builder.electionInterval, "electionInterval");
        this.triggerCheckInterval = requirePositive(builder.triggerCheckInterval, "triggerCheckInterval");
        this.tags = Map.copyOf(builder.tags);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * The {@code assignor.load-aware.*} consumer configs that make a consumer's
     * {@link LoadAwarePartitionAssignor} use this registry's collaborators. Put them into the
     * config map of every consumer factory whose groups this registry balances, next to
     * {@code partition.assignment.strategy}. The {@link MemberIdTracker} is left out while
     * proactive rebalance is off: nothing would read what the assignor reports to it.
     */
    public Map<String, Object> assignorConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService);
        configs.put(LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService);
        if (proactiveRebalance) {
            configs.put(LoadAwareAssignorConfig.MEMBER_ID_TRACKER, memberIdTracker);
        }
        if (instanceId != null && !instanceId.isBlank()) {
            configs.put(LoadAwareAssignorConfig.INSTANCE_ID, instanceId);
        }
        return Map.copyOf(configs);
    }

    /**
     * Registers {@code groupId} with this registry's default trigger settings.
     *
     * @param rebalanceInitiator forces the rebalance of the group's consumers in this JVM when
     *                           the trigger fires; ignored when proactive rebalance is off
     * @throws IllegalArgumentException when the group is already registered
     * @throws IllegalStateException    when the registry is closed
     */
    public ConsumerGroupBalancer register(String groupId, RebalanceInitiator rebalanceInitiator) {
        return group(groupId).rebalanceInitiator(rebalanceInitiator).register();
    }

    /** Starts registering {@code groupId} with settings that differ from the registry's defaults. */
    public GroupBuilder group(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group id is required");
        }
        return new GroupBuilder(groupId);
    }

    public Optional<ConsumerGroupBalancer> getGroup(String groupId) {
        synchronized (lock) {
            return Optional.ofNullable(groups.get(groupId));
        }
    }

    /** Snapshot of the registered groups, in registration order. */
    public List<ConsumerGroupBalancer> getGroups() {
        synchronized (lock) {
            return List.copyOf(groups.values());
        }
    }

    /**
     * Calls {@code listener} for every group already registered and for every group registered
     * from now on, exactly once per group. Listeners run on the registering thread and must
     * not register groups themselves.
     */
    public void onRegister(Consumer<ConsumerGroupBalancer> listener) {
        Objects.requireNonNull(listener, "listener");
        List<ConsumerGroupBalancer> existing;
        synchronized (lock) {
            listeners.add(listener);
            existing = List.copyOf(groups.values());
        }
        existing.forEach(listener);
    }

    /** Starts every group registered so far; groups registered later start on registration. */
    public void start() {
        List<ConsumerGroupBalancer> toStart;
        synchronized (lock) {
            if (state == State.CLOSED) {
                throw new IllegalStateException("ConsumerGroupBalancers is closed and cannot be restarted");
            }
            if (state == State.STARTED) {
                return;
            }
            state = State.STARTED;
            toStart = List.copyOf(groups.values());
        }
        toStart.forEach(ConsumerGroupBalancer::start);
    }

    public boolean isRunning() {
        synchronized (lock) {
            return state == State.STARTED;
        }
    }

    /** Stops every group. The admin client and the weight store stay open: they are not owned here. */
    @Override
    public void close() {
        List<ConsumerGroupBalancer> toClose;
        synchronized (lock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            toClose = List.copyOf(groups.values());
        }
        toClose.forEach(ConsumerGroupBalancer::close);
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public WeightService getWeightService() {
        return weightService;
    }

    public BalanceService getBalanceService() {
        return balanceService;
    }

    public MemberIdTracker getMemberIdTracker() {
        return memberIdTracker;
    }

    public boolean isProactiveRebalance() {
        return proactiveRebalance;
    }

    /** Extra tags for this registry's meters, e.g. {@code cluster=b}; empty by default. */
    public Map<String, String> getTags() {
        return tags;
    }

    private ConsumerGroupBalancer register(GroupBuilder spec) {
        ConsumerGroupBalancer group;
        boolean startNow;
        List<Consumer<ConsumerGroupBalancer>> toNotify;
        synchronized (lock) {
            if (state == State.CLOSED) {
                throw new IllegalStateException(
                        "ConsumerGroupBalancers is closed; cannot register group '" + spec.groupId + "'");
            }
            if (groups.containsKey(spec.groupId)) {
                // Two elections for one group in one JVM would both win and double every rebalance.
                throw new IllegalArgumentException("Group '" + spec.groupId + "' is already registered");
            }
            group = proactiveRebalance ? proactiveGroup(spec) : ConsumerGroupBalancer.passive(spec.groupId);
            groups.put(spec.groupId, group);
            startNow = state == State.STARTED;
            toNotify = List.copyOf(listeners);
        }
        if (startNow) {
            group.start();
        }
        toNotify.forEach(listener -> listener.accept(group));
        log.info("Registered consumer group '{}' with the balancer ({})", spec.groupId,
                group.isProactive() ? "proactive rebalance on" : "assignor only");
        return group;
    }

    private ConsumerGroupBalancer proactiveGroup(GroupBuilder spec) {
        String groupId = spec.groupId;
        RebalanceInitiator initiator = Objects.requireNonNull(spec.rebalanceInitiator,
                () -> "A rebalance initiator is required for group '" + groupId + "' while proactive rebalance is on");
        RebalanceTrigger trigger = spec.trigger != null
                ? spec.trigger
                : new ThresholdTrigger(
                        adminClient,
                        groupId,
                        memberIdTracker,
                        weightService,
                        spec.imbalanceThreshold,
                        balanceService,
                        spec.damping,
                        Clock.systemUTC());
        CoordinatorElection election = new CoordinatorElection.Builder()
                .setGroupId(groupId)
                .setMemberIdsSupplier(() -> memberIdTracker.getCurrentMemberIds(groupId))
                .setElectionIntervalMs(spec.electionInterval.toMillis())
                .setAdminClient(adminClient)
                .build();
        CoordinatorManager manager = new CoordinatorManager(
                election, trigger, initiator, spec.triggerCheckInterval.toMillis());
        return new ConsumerGroupBalancer(groupId, trigger, initiator, manager);
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive, but was " + value);
        }
        return value;
    }

    /** One group's registration; unset settings default to the registry's. */
    public final class GroupBuilder {

        private final String groupId;
        private RebalanceInitiator rebalanceInitiator;
        private RebalanceTrigger trigger;
        private double imbalanceThreshold = ConsumerGroupBalancers.this.imbalanceThreshold;
        private RebalanceDamping damping = ConsumerGroupBalancers.this.damping;
        private Duration electionInterval = ConsumerGroupBalancers.this.electionInterval;
        private Duration triggerCheckInterval = ConsumerGroupBalancers.this.triggerCheckInterval;

        private GroupBuilder(String groupId) {
            this.groupId = groupId;
        }

        /** Required while proactive rebalance is on. */
        public GroupBuilder rebalanceInitiator(RebalanceInitiator rebalanceInitiator) {
            this.rebalanceInitiator = rebalanceInitiator;
            return this;
        }

        /**
         * Replaces the default {@link ThresholdTrigger}; the threshold and damping settings
         * then no longer apply.
         */
        public GroupBuilder trigger(RebalanceTrigger trigger) {
            this.trigger = trigger;
            return this;
        }

        public GroupBuilder imbalanceThreshold(double imbalanceThreshold) {
            this.imbalanceThreshold = imbalanceThreshold;
            return this;
        }

        public GroupBuilder damping(RebalanceDamping damping) {
            this.damping = Objects.requireNonNull(damping, "damping");
            return this;
        }

        public GroupBuilder electionInterval(Duration electionInterval) {
            this.electionInterval = requirePositive(electionInterval, "electionInterval");
            return this;
        }

        public GroupBuilder triggerCheckInterval(Duration triggerCheckInterval) {
            this.triggerCheckInterval = requirePositive(triggerCheckInterval, "triggerCheckInterval");
            return this;
        }

        /**
         * Registers the group with these settings.
         *
         * @see ConsumerGroupBalancers#register(String, RebalanceInitiator)
         */
        public ConsumerGroupBalancer register() {
            return ConsumerGroupBalancers.this.register(this);
        }
    }

    public static final class Builder {

        private AdminClient adminClient;
        private WeightService weightService;
        private BalanceService balanceService;
        private MemberIdTracker memberIdTracker;
        private String instanceId;
        private boolean proactiveRebalance = true;
        private double imbalanceThreshold = DEFAULT_IMBALANCE_THRESHOLD;
        private RebalanceDamping damping = RebalanceDamping.defaults();
        private Duration electionInterval = DEFAULT_ELECTION_INTERVAL;
        private Duration triggerCheckInterval = DEFAULT_TRIGGER_CHECK_INTERVAL;
        private Map<String, String> tags = Map.of();

        private Builder() {
        }

        /** Required. Used by coordinator election and the trigger; not closed by the registry. */
        public Builder adminClient(AdminClient adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /** Required. Shared by the assignor and the trigger of every group; not closed by the registry. */
        public Builder weightService(WeightService weightService) {
            this.weightService = weightService;
            return this;
        }

        /** Default: {@link SortingRoundRobinBalanceService}. */
        public Builder balanceService(BalanceService balanceService) {
            this.balanceService = balanceService;
            return this;
        }

        /** Default: a new tracker, shared by every group of the registry. */
        public Builder memberIdTracker(MemberIdTracker memberIdTracker) {
            this.memberIdTracker = memberIdTracker;
            return this;
        }

        /**
         * Application-instance id every consumer of the registry reports to its group leader.
         * Default: the assignor's random id, generated once per JVM.
         */
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        /**
         * When {@code false}, registered groups are passive: no election, no trigger, and
         * assignor-only balancing. Default: {@code true}.
         */
        public Builder proactiveRebalance(boolean proactiveRebalance) {
            this.proactiveRebalance = proactiveRebalance;
            return this;
        }

        /** Default for every group: {@value ConsumerGroupBalancers#DEFAULT_IMBALANCE_THRESHOLD}. */
        public Builder imbalanceThreshold(double imbalanceThreshold) {
            this.imbalanceThreshold = imbalanceThreshold;
            return this;
        }

        /** Default for every group: {@link RebalanceDamping#defaults()}. */
        public Builder damping(RebalanceDamping damping) {
            this.damping = damping;
            return this;
        }

        /** Default for every group: 30 seconds. */
        public Builder electionInterval(Duration electionInterval) {
            this.electionInterval = electionInterval;
            return this;
        }

        /** Default for every group: 30 seconds. */
        public Builder triggerCheckInterval(Duration triggerCheckInterval) {
            this.triggerCheckInterval = triggerCheckInterval;
            return this;
        }

        /**
         * Extra tags for this registry's meters, e.g. {@code cluster=b} when several registries
         * report to one meter registry. Default: none.
         */
        public Builder tags(Map<String, String> tags) {
            this.tags = Objects.requireNonNull(tags, "tags");
            return this;
        }

        public ConsumerGroupBalancers build() {
            return new ConsumerGroupBalancers(this);
        }
    }
}
