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
    }

    @Override
    public boolean shouldTrigger() {

        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!isStable(groupDescription)) {
                return false;
            }

            if (groupDescription.members().isEmpty()) {
                log.debug("ThresholdTrigger evaluated [group={}]: no members, shouldTrigger=false", groupId);
                violatedChecks = 0;
                return false;
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

            var weights = PartitionWeights.sanitized(
                    allPartitions, weightService.computeWeights(allPartitions));

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
                return false;
            }
            if (optimalMaxLoaded.load <= 0.0) {
                // All weights are zero; there is no imbalance to fix.
                log.debug("ThresholdTrigger evaluated [group={}]: all partition weights are zero, shouldTrigger=false",
                        groupId);
                onBalanced();
                return false;
            }

            double ratio = currentMaxLoaded.load / optimalMaxLoaded.load;
            boolean violated = ratio > threshold;

            if (!violated) {
                log.debug("ThresholdTrigger evaluated [group={}]: currentMaxLoadedInstance={}, "
                                + "optimalMaxLoadedInstance={}, ratio={}, threshold={}, violated=false",
                        groupId, currentMaxLoaded, optimalMaxLoaded, ratio, threshold);
                onBalanced();
                return false;
            }

            return onViolated(currentAssignment, currentMaxLoaded, optimalMaxLoaded, ratio);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while running ThresholdTrigger", e);
            return false;
        } catch (Exception e) {
            log.error("Could not run ThresholdTrigger", e);
            return false;
        }
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
     */
    private void onBalanced() {
        violatedChecks = 0;
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
    private boolean onViolated(
            Map<String, List<TopicPartition>> currentAssignment,
            InstanceLoad currentMaxLoaded,
            InstanceLoad optimalMaxLoaded,
            double ratio) {
        balancedChecks = 0;

        Map<String, Set<TopicPartition>> fingerprint = fingerprintOf(currentAssignment);
        // A streak only means something while it describes one and the same assignment.
        violatedChecks = fingerprint.equals(lastCheckedAssignment)
                ? Math.min(violatedChecks + 1, damping.minViolatedChecks())
                : 1;
        lastCheckedAssignment = fingerprint;

        log.info("ThresholdTrigger evaluated [group={}]: currentMaxLoadedInstance={}, "
                        + "optimalMaxLoadedInstance={}, ratio={}, threshold={}, violated=true ({}/{} checks)",
                groupId, currentMaxLoaded, optimalMaxLoaded, ratio, threshold,
                violatedChecks, damping.minViolatedChecks());

        if (violatedChecks < damping.minViolatedChecks()) {
            return false;
        }

        Instant now = clock.instant();
        Duration cooldown = cooldown();
        if (lastFiredAt != null && now.isBefore(lastFiredAt.plus(cooldown))) {
            log.info("ThresholdTrigger [group={}]: imbalance confirmed but the last proactive rebalance was {} ago"
                            + " and the cooldown is {}; not rebalancing yet",
                    groupId, Duration.between(lastFiredAt, now), cooldown);
            return false;
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
        return true;
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
}
