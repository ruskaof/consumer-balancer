package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.trigger.RebalanceDamping;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import io.github.ruskaof.balancer.weight.PartitionWeights;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Fires when the most loaded application instance carries more than {@code threshold} times
 * the load it would carry under the optimal assignment computed from current weights.
 *
 * <p>The trigger observes the group from the outside, through the admin API, and that view is
 * necessarily an approximation of what the assignor sees:
 * <ul>
 *   <li>the admin API does not expose the instance ids members report to the assignor, so
 *       members are grouped by {@link MemberDescription#host()} — members of one pod share the
 *       broker-observed client address, and Kubernetes pod IPs are unique. Members with a
 *       blank host count as their own instances;</li>
 *   <li>the admin API does not expose member subscriptions either, so every member is treated
 *       as eligible for every topic in the group. That matches the instance-level load this
 *       trigger compares as long as every instance runs the whole set of listeners;</li>
 *   <li>weights are measured locally, while the assignment was computed from the group
 *       leader's own — equally valid but not identical — measurements.</li>
 * </ul>
 *
 * <p>Each of those can make the computed optimum unreachable, and because the assignor is
 * deterministic an unreachable optimum would re-fire a useless rebalance on every check. Three
 * guards keep that from becoming a rebalance storm:
 * <ol>
 *   <li><b>Stable groups only.</b> While the group is rebalancing, the admin API reports
 *       partial or stale assignments; judging those would fire again on the rebalance this
 *       trigger just caused, which is a self-sustaining loop. Non-stable checks are skipped
 *       without touching any of the state below.</li>
 *   <li><b>Hysteresis.</b> The imbalance must be seen on
 *       {@link RebalanceDamping#minViolatedChecks()} consecutive checks of one unchanged
 *       assignment, so a single noisy weight sample cannot rebalance the group.</li>
 *   <li><b>Cooldown with backoff.</b> Two fires are never closer together than
 *       {@link RebalanceDamping#cooldown()}, and every fire that does not restore balance
 *       doubles that distance up to {@link RebalanceDamping#maxCooldown()}.</li>
 * </ol>
 */
@Slf4j
public class ThresholdTrigger implements RebalanceTrigger {

    private static final long DESCRIBE_TIMEOUT_MS = 30_000L;

    private static final String LIKELY_CAUSES = "Likely causes: an instance id per member that does not match client"
            + " hosts, several instances sharing one host, instances running different sets of listeners, or an"
            + " imbalance the assignor cannot improve on with its own weight measurements.";

    private final AdminClient adminClient;
    private final String groupId;
    private final WeightService weightService;
    private final double threshold;
    private final BalanceService balanceService;
    private final RebalanceDamping damping;
    private final Clock clock;

    // All mutable state is thread-confined to the coordinator's scheduler thread.
    private Map<String, Set<TopicPartition>> lastCheckedAssignment;
    private int violatedChecks;
    private int balancedChecks;
    private int cooldownDoublings;
    private boolean balancedSinceLastFire = true;
    private Instant lastFiredAt;
    private boolean warnedAboutMissingGroupState;
    private double lastRatio = Double.NaN;
    private double lastCurrentMaxLoad = Double.NaN;
    private double lastOptimalMaxLoad = Double.NaN;
    private int lastMemberCount;
    private int lastInstanceCount;
    private int lastPartitionCount;
    private int lastDefaultedWeightCount;
    private final long[] evaluationsByOutcome = new long[EvaluationOutcome.values().length];
    private long evaluationCount;
    private long evaluationTimeNanos;

    // The one exception to the confinement above: an immutable view of that state,
    // republished after every evaluation for readers on other threads (e.g. metrics).
    private volatile Status status;

    public ThresholdTrigger(
            AdminClient adminClient,
            String groupId,
            WeightService weightService,
            double threshold,
            BalanceService balanceService,
            RebalanceDamping damping,
            Clock clock) {
        this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.weightService = Objects.requireNonNull(weightService, "weightService");
        this.threshold = threshold;
        this.balanceService = Objects.requireNonNull(balanceService, "balanceService");
        this.damping = Objects.requireNonNull(damping, "damping");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.status = snapshot();
    }

    @Override
    public boolean shouldTrigger() {
        long started = System.nanoTime();
        EvaluationOutcome outcome;
        try {
            outcome = evaluate();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while running ThresholdTrigger", e);
            outcome = EvaluationOutcome.ERROR;
        } catch (Exception e) {
            log.error("Could not run ThresholdTrigger", e);
            outcome = EvaluationOutcome.ERROR;
        }
        recordEvaluation(outcome, System.nanoTime() - started);
        return outcome == EvaluationOutcome.FIRED;
    }

    private EvaluationOutcome evaluate() throws Exception {
        var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                .describedGroups()
                .get(groupId)
                .get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (!isStable(groupDescription)) {
            return EvaluationOutcome.GROUP_NOT_STABLE;
        }

        if (groupDescription.members().isEmpty()) {
            log.debug("ThresholdTrigger evaluated [group={}]: no members, shouldTrigger=false", groupId);
            violatedChecks = 0;
            return EvaluationOutcome.NO_MEMBERS;
        }

        var allPartitions = new HashSet<TopicPartition>();
        var currentAssignment = new HashMap<String, List<TopicPartition>>();
        var instanceKeyByMember = new HashMap<String, String>();

        for (var memberDescription : groupDescription.members()) {
            allPartitions.addAll(memberDescription.assignment().topicPartitions());
            currentAssignment.put(memberDescription.consumerId(),
                    memberDescription.assignment().topicPartitions().stream().toList());
            instanceKeyByMember.put(memberDescription.consumerId(), instanceKeyOf(memberDescription));
        }

        var sanitized = PartitionWeights.sanitizedCounted(
                allPartitions, weightService.computeWeights(allPartitions));
        var weights = sanitized.weights();

        lastMemberCount = groupDescription.members().size();
        lastInstanceCount = Set.copyOf(instanceKeyByMember.values()).size();
        lastPartitionCount = allPartitions.size();
        lastDefaultedWeightCount = sanitized.defaultedCount();

        // The admin API does not expose member subscriptions, so every member is
        // treated as eligible for every topic in the group.
        Set<String> allTopics = new HashSet<>();
        for (TopicPartition tp : allPartitions) {
            allTopics.add(tp.topic());
        }
        var members = new ArrayList<GroupMember>();
        instanceKeyByMember.forEach((memberId, instanceKey) ->
                members.add(new GroupMember(memberId, instanceKey, allTopics)));

        var optimalAssignment = balanceService.computeOptimalAssignment(members, weights);

        var currentMaxLoaded = maxInstanceLoad(
                groupByInstance(currentAssignment, instanceKeyByMember), weights);
        var optimalMaxLoaded = maxInstanceLoad(
                groupByInstance(optimalAssignment, instanceKeyByMember), weights);

        if (optimalMaxLoaded == null || currentMaxLoaded == null) {
            log.debug("ThresholdTrigger evaluated [group={}]: no members with assignments, shouldTrigger=false",
                    groupId);
            violatedChecks = 0;
            return EvaluationOutcome.NO_MEMBERS;
        }
        if (optimalMaxLoaded.load <= 0.0) {
            // All weights are zero; there is no imbalance to fix.
            log.debug("ThresholdTrigger evaluated [group={}]: all partition weights are zero, shouldTrigger=false",
                    groupId);
            lastCurrentMaxLoad = currentMaxLoaded.load;
            lastOptimalMaxLoad = optimalMaxLoaded.load;
            // Zero optimal load leaves the ratio undefined, so no stale value may linger.
            lastRatio = Double.NaN;
            onBalanced();
            return EvaluationOutcome.BALANCED;
        }

        double ratio = currentMaxLoaded.load / optimalMaxLoaded.load;
        lastRatio = ratio;
        lastCurrentMaxLoad = currentMaxLoaded.load;
        lastOptimalMaxLoad = optimalMaxLoaded.load;
        boolean violated = ratio > threshold;

        if (!violated) {
            log.debug("ThresholdTrigger evaluated [group={}]: currentMaxLoadedInstance={}, "
                            + "optimalMaxLoadedInstance={}, ratio={}, threshold={}, violated=false",
                    groupId, currentMaxLoaded, optimalMaxLoaded, ratio, threshold);
            onBalanced();
            return EvaluationOutcome.BALANCED;
        }

        return onViolated(currentAssignment, currentMaxLoaded, optimalMaxLoaded, ratio);
    }

    /** The state of this trigger after its most recent evaluation; safe to read from any thread. */
    public Status status() {
        return status;
    }

    private void recordEvaluation(EvaluationOutcome outcome, long elapsedNanos) {
        evaluationsByOutcome[outcome.ordinal()]++;
        evaluationCount++;
        evaluationTimeNanos += elapsedNanos;
        status = snapshot();
    }

    private Status snapshot() {
        Map<EvaluationOutcome, Long> byOutcome = new EnumMap<>(EvaluationOutcome.class);
        for (EvaluationOutcome outcome : EvaluationOutcome.values()) {
            byOutcome.put(outcome, evaluationsByOutcome[outcome.ordinal()]);
        }
        return new Status(threshold, lastRatio, lastCurrentMaxLoad, lastOptimalMaxLoad,
                lastMemberCount, lastInstanceCount, lastPartitionCount, lastDefaultedWeightCount,
                violatedChecks, cooldown(), lastFiredAt, byOutcome, evaluationCount, evaluationTimeNanos);
    }

    /**
     * Only a stable group has assignments worth judging. While it rebalances, members report
     * the assignment of the previous generation, an empty one, or a mixture of both — and
     * acting on that would keep re-firing on the very rebalance this trigger initiated.
     */
    private boolean isStable(ConsumerGroupDescription groupDescription) {
        GroupState state = groupDescription.groupState();
        if (state == GroupState.STABLE) {
            warnedAboutMissingGroupState = false;
            return true;
        }
        if (state == null || state == GroupState.UNKNOWN) {
            // Permanent for a broker that does not report the state, so say it once.
            if (!warnedAboutMissingGroupState) {
                warnedAboutMissingGroupState = true;
                log.warn("ThresholdTrigger [group={}]: the broker did not report a group state, so the group cannot be"
                        + " checked for imbalance. Proactive rebalance stays off for this group.", groupId);
            }
        } else {
            log.debug("ThresholdTrigger skipped [group={}]: group state is {}, not STABLE", groupId, state);
        }
        return false;
    }

    /**
     * The group is within threshold. Once that holds for as many checks as a violation needs
     * to fire, the imbalance counts as resolved: the cooldown backoff is wound back so the
     * next genuine imbalance is reacted to promptly.
     *
     * <p>The two counters are deliberately asymmetric. The violation streak only <em>decays</em>
     * here, because the ratio ramps through the threshold while the weight window still spans
     * the old load: resetting it on the first dip would restart the count over and over exactly
     * when the load has just shifted, which is when the trigger is most needed. The relief
     * counter, in contrast, is hard-reset by any violation — the cooldown backoff should be
     * quick to arm and slow to disarm.
     */
    private void onBalanced() {
        if (violatedChecks > 0) {
            violatedChecks--;
        }
        if (balancedChecks < damping.minViolatedChecks()) {
            balancedChecks++;
        }
        if (balancedChecks >= damping.minViolatedChecks() && !balancedSinceLastFire) {
            log.info("ThresholdTrigger [group={}]: the group is balanced again; resetting the rebalance cooldown to {}",
                    groupId, damping.cooldown());
            balancedSinceLastFire = true;
            cooldownDoublings = 0;
        }
    }

    /**
     * The group is out of threshold: count the violation against the hysteresis, then against
     * the cooldown, and only fire when both allow it.
     */
    private EvaluationOutcome onViolated(
            Map<String, List<TopicPartition>> currentAssignment,
            InstanceLoad currentMaxLoaded,
            InstanceLoad optimalMaxLoaded,
            double ratio) {
        balancedChecks = 0;

        Map<String, Set<TopicPartition>> fingerprint = fingerprintOf(currentAssignment);
        // A streak only means something while it describes one and the same assignment, so a
        // moved partition starts the count over — unlike a merely balanced check, which only
        // decays it (see onBalanced).
        if (!fingerprint.equals(lastCheckedAssignment)) {
            violatedChecks = 0;
        }
        lastCheckedAssignment = fingerprint;
        violatedChecks = Math.min(violatedChecks + 1, damping.minViolatedChecks());

        log.info("ThresholdTrigger evaluated [group={}]: currentMaxLoadedInstance={}, "
                        + "optimalMaxLoadedInstance={}, ratio={}, threshold={}, violated=true ({}/{} checks)",
                groupId, currentMaxLoaded, optimalMaxLoaded, ratio, threshold,
                violatedChecks, damping.minViolatedChecks());

        if (violatedChecks < damping.minViolatedChecks()) {
            return EvaluationOutcome.AWAITING_HYSTERESIS;
        }

        Instant now = clock.instant();
        Duration cooldown = cooldown();
        if (lastFiredAt != null && now.isBefore(lastFiredAt.plus(cooldown))) {
            log.info("ThresholdTrigger [group={}]: imbalance confirmed but the last proactive rebalance was {} ago"
                            + " and the cooldown is {}; not rebalancing yet",
                    groupId, Duration.between(lastFiredAt, now), cooldown);
            return EvaluationOutcome.COOLDOWN_SUPPRESSED;
        }

        if (!balancedSinceLastFire && lastFiredAt != null && !cooldown.isZero()) {
            // The previous rebalance did not bring the group within threshold, so repeating it
            // at the same rate would just churn the group.
            Duration sinceLastFire = Duration.between(lastFiredAt, now);
            if (cooldown.compareTo(damping.maxCooldown()) < 0) {
                cooldownDoublings++;
                log.warn("ThresholdTrigger [group={}]: the proactive rebalance {} ago did not bring the group within"
                                + " the threshold, so the cooldown grows from {} to {}. {}",
                        groupId, sinceLastFire, cooldown, cooldown(), LIKELY_CAUSES);
            } else {
                log.warn("ThresholdTrigger [group={}]: the proactive rebalance {} ago did not bring the group within"
                                + " the threshold either; the cooldown stays at its maximum {}. {}",
                        groupId, sinceLastFire, cooldown, LIKELY_CAUSES);
            }
        }

        balancedSinceLastFire = false;
        lastFiredAt = now;
        violatedChecks = 0;
        return EvaluationOutcome.FIRED;
    }

    /** The base cooldown doubled once per ineffective fire, capped at the configured maximum. */
    private Duration cooldown() {
        Duration base = damping.cooldown();
        if (base.isZero() || cooldownDoublings == 0) {
            return base;
        }
        Duration scaled = base.multipliedBy(1L << cooldownDoublings);
        return scaled.compareTo(damping.maxCooldown()) > 0 ? damping.maxCooldown() : scaled;
    }

    /** Members of one pod share the broker-observed client host; blank hosts stay singletons. */
    private static String instanceKeyOf(MemberDescription member) {
        String host = member.host();
        return (host == null || host.isBlank()) ? member.consumerId() : host;
    }

    private static Map<String, List<TopicPartition>> groupByInstance(
            Map<String, List<TopicPartition>> assignmentByMember,
            Map<String, String> instanceKeyByMember) {
        Map<String, List<TopicPartition>> byInstance = new HashMap<>();
        assignmentByMember.forEach((member, partitions) ->
                byInstance.computeIfAbsent(
                                instanceKeyByMember.getOrDefault(member, member),
                                key -> new ArrayList<>())
                        .addAll(partitions));
        return byInstance;
    }

    private static Map<String, Set<TopicPartition>> fingerprintOf(
            Map<String, List<TopicPartition>> assignment) {
        Map<String, Set<TopicPartition>> fingerprint = new HashMap<>();
        assignment.forEach((member, partitions) -> fingerprint.put(member, Set.copyOf(partitions)));
        return fingerprint;
    }

    private static InstanceLoad maxInstanceLoad(
            Map<String, List<TopicPartition>> assignmentByInstance,
            Map<TopicPartition, Double> weights) {
        InstanceLoad maxLoaded = null;

        for (var instance : assignmentByInstance.keySet()) {
            double weightSum = 0;

            for (var partition : assignmentByInstance.get(instance)) {
                weightSum += weights.getOrDefault(partition, PartitionWeightDefaults.MISSING);
            }

            if (maxLoaded == null || maxLoaded.load < weightSum) {
                maxLoaded = new InstanceLoad(instance, weightSum);
            }
        }

        return maxLoaded;
    }

    private record InstanceLoad(
            String instanceKey,
            double load) {
    }

    /** Why one {@link #shouldTrigger()} evaluation ended the way it did. */
    public enum EvaluationOutcome {
        /** Returned {@code true}: a proactive rebalance was requested. */
        FIRED,
        /** The load ratio was within the threshold, or all weights were zero. */
        BALANCED,
        /** Violated, but on fewer consecutive checks than the hysteresis requires. */
        AWAITING_HYSTERESIS,
        /** Violated and confirmed, but the cooldown since the last fire has not passed. */
        COOLDOWN_SUPPRESSED,
        /** The group was not {@code STABLE} (or reported no state), so it was not judged. */
        GROUP_NOT_STABLE,
        /** The group had no members, or none with assigned partitions. */
        NO_MEMBERS,
        /** The evaluation threw; the exception was logged and swallowed. */
        ERROR
    }

    /**
     * Immutable snapshot of the trigger's state, republished after every evaluation — the
     * values the trigger otherwise only logs, in a form a metrics binder can poll.
     *
     * <p>Before the first evaluation the ratios and loads are {@link Double#NaN}, the counts
     * are zero, {@code lastFiredAt} is {@code null} and {@code effectiveCooldown} is the base
     * cooldown. {@code lastRatio} and the loads keep the values of the last evaluation that
     * computed them; checks that skip the computation (e.g. a rebalancing group) leave them
     * untouched, and an all-zero-weight group resets the ratio to {@code NaN}.
     */
    public record Status(
            double threshold,
            double lastRatio,
            double lastCurrentMaxLoad,
            double lastOptimalMaxLoad,
            int lastMemberCount,
            int lastInstanceCount,
            int lastPartitionCount,
            int lastDefaultedWeightCount,
            int violatedChecks,
            Duration effectiveCooldown,
            Instant lastFiredAt,
            Map<EvaluationOutcome, Long> evaluationsByOutcome,
            long evaluationCount,
            long evaluationTimeNanos) {

        public Status {
            evaluationsByOutcome = Map.copyOf(evaluationsByOutcome);
        }

        /** Total evaluations that ended with this outcome; monotonic. */
        public long evaluations(EvaluationOutcome outcome) {
            return evaluationsByOutcome.getOrDefault(outcome, 0L);
        }
    }
}
