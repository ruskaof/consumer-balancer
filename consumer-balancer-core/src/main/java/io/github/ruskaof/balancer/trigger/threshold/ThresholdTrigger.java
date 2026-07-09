package io.github.ruskaof.balancer.trigger.threshold;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Fires when the most loaded member carries more than {@code threshold} times the load it
 * would carry under the optimal assignment computed from current weights.
 */
@Slf4j
@RequiredArgsConstructor
public class ThresholdTrigger implements RebalanceTrigger {

    private static final long DESCRIBE_TIMEOUT_MS = 30_000L;

    private final AdminClient adminClient;
    private final String groupId;
    private final WeightService weightService;
    private final double threshold;
    private final BalanceService balanceService;

    @Override
    public boolean shouldTrigger() {

        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            var allPartitions = new HashSet<TopicPartition>();
            var currentAssignment = new HashMap<String, List<TopicPartition>>();

            for (var memberDescription : groupDescription.members()) {
                allPartitions.addAll(memberDescription.assignment().topicPartitions());
                currentAssignment.put(memberDescription.consumerId(),
                        memberDescription.assignment().topicPartitions().stream().toList());
            }

            var weights = weightService.computeWeights(allPartitions);

            // The admin API does not expose member subscriptions, so every member is
            // treated as eligible for every topic in the group.
            Set<String> allTopics = new HashSet<>();
            for (TopicPartition tp : allPartitions) {
                allTopics.add(tp.topic());
            }
            var subscribedTopicsByMember = new HashMap<String, Set<String>>();
            for (String member : currentAssignment.keySet()) {
                subscribedTopicsByMember.put(member, allTopics);
            }

            var optimalAssignment = balanceService.computeOptimalAssignment(subscribedTopicsByMember, weights);
            var optimalMaxLoaded = calculateMaxLoadedMember(optimalAssignment, weights);
            var currentMaxLoaded = calculateMaxLoadedMember(currentAssignment, weights);

            if (optimalMaxLoaded == null || currentMaxLoaded == null) {
                return false;
            }
            if (optimalMaxLoaded.memberLoad <= 0.0) {
                // All weights are zero; there is no imbalance to fix.
                return false;
            }

            boolean shouldTrigger = currentMaxLoaded.memberLoad / optimalMaxLoaded.memberLoad > threshold;

            log.debug("ThresholdTrigger result: optimalMaxLoaded={}, currentMaxLoaded={}, shouldTrigger={}",
                    optimalMaxLoaded, currentMaxLoaded, shouldTrigger);
            return shouldTrigger;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while running ThresholdTrigger", e);
            return false;
        } catch (Exception e) {
            log.error("Could not run ThresholdTrigger", e);
            return false;
        }
    }

    private MemberLoad calculateMaxLoadedMember(
            Map<String, List<TopicPartition>> assignment,
            Map<TopicPartition, Double> weights) {
        MemberLoad maxLoadedMember = null;

        for (var member : assignment.keySet()) {
            double weightSum = 0;

            for (var partition : assignment.get(member)) {
                weightSum += weights.getOrDefault(partition, PartitionWeightDefaults.MISSING);
            }

            if (maxLoadedMember == null || maxLoadedMember.memberLoad < weightSum) {
                maxLoadedMember = new MemberLoad(member, weightSum);
            }
        }

        return maxLoadedMember;
    }

    private record MemberLoad(
            String memberId,
            double memberLoad) {
    }
}
