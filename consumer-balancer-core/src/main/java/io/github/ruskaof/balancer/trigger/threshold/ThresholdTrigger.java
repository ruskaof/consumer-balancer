package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.GroupMember;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import io.github.ruskaof.balancer.weight.PartitionWeights;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberDescription;
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
 * <p>The admin API does not expose the instance ids members report to the assignor, so
 * members are grouped by {@link MemberDescription#host()}: members of one pod share the
 * broker-observed client address, and Kubernetes pod IPs are unique. Members with a blank
 * host count as their own instances. This approximation can disagree with the assignor's
 * grouping (a custom instance-id scheme not aligned with hosts, pods on the host network
 * sharing a node IP, a group mid-upgrade); because the assignor is deterministic, such a
 * disagreement would re-fire a useless rebalance every check — so a violation observed on
 * the exact assignment this trigger already fired on is suppressed for
 * {@code refireSuppression} (pass {@link Duration#ZERO} to disable suppression).
 */
@Slf4j
public class ThresholdTrigger implements RebalanceTrigger {

    public static final Duration DEFAULT_REFIRE_SUPPRESSION = Duration.ofMinutes(10);

    private static final long DESCRIBE_TIMEOUT_MS = 30_000L;

    private final AdminClient adminClient;
    private final String groupId;
    private final WeightService weightService;
    private final double threshold;
    private final BalanceService balanceService;
    private final Duration refireSuppression;
    private final Clock clock;

    // Thread-confined to the coordinator's scheduler thread.
    private Map<String, Set<TopicPartition>> lastFiredAssignment;
    private Instant lastFiredAt;

    public ThresholdTrigger(
            AdminClient adminClient,
            String groupId,
            WeightService weightService,
            double threshold,
            BalanceService balanceService,
            Duration refireSuppression,
            Clock clock) {
        this.adminClient = adminClient;
        this.groupId = groupId;
        this.weightService = weightService;
        this.threshold = threshold;
        this.balanceService = balanceService;
        this.refireSuppression = refireSuppression;
        this.clock = clock;
    }

    @Override
    public boolean shouldTrigger() {

        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (groupDescription.members().isEmpty()) {
                log.info("ThresholdTrigger evaluated [group={}]: no members, shouldTrigger=false", groupId);
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
                log.info("ThresholdTrigger evaluated [group={}]: no members with assignments, shouldTrigger=false",
                        groupId);
                return false;
            }
            if (optimalMaxLoaded.load <= 0.0) {
                // All weights are zero; there is no imbalance to fix.
                log.info("ThresholdTrigger evaluated [group={}]: all partition weights are zero, shouldTrigger=false",
                        groupId);
                return false;
            }

            double ratio = currentMaxLoaded.load / optimalMaxLoaded.load;
            boolean violated = ratio > threshold;

            log.info("ThresholdTrigger evaluated [group={}]: currentMaxLoadedInstance={}, "
                            + "optimalMaxLoadedInstance={}, ratio={}, threshold={}, violated={}",
                    groupId, currentMaxLoaded, optimalMaxLoaded, ratio, threshold, violated);
            if (!violated) {
                return false;
            }

            Map<String, Set<TopicPartition>> fingerprint = fingerprintOf(currentAssignment);
            if (fingerprint.equals(lastFiredAssignment)
                    && clock.instant().isBefore(lastFiredAt.plus(refireSuppression))) {
                log.warn("ThresholdTrigger [group={}]: the assignment is unchanged since the rebalance this "
                        + "trigger initiated but still violates the threshold; suppressing to avoid a "
                        + "rebalance loop. Likely causes: a custom instance id per member not matching "
                        + "client hosts, several instances sharing one host, or a group mid-upgrade.",
                        groupId);
                return false;
            }
            lastFiredAssignment = fingerprint;
            lastFiredAt = clock.instant();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while running ThresholdTrigger", e);
            return false;
        } catch (Exception e) {
            log.error("Could not run ThresholdTrigger", e);
            return false;
        }
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
