package com.ruskaof.balancer.trigger.threshold;

import com.ruskaof.balancer.balance.BalanceService;
import com.ruskaof.balancer.trigger.RebalanceTrigger;
import com.ruskaof.balancer.weight.PartitionWeightDefaults;
import com.ruskaof.balancer.weight.WeightService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class ThresholdTrigger implements RebalanceTrigger {

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
                    .get();

            var allPartitions = new HashSet<TopicPartition>();
            var currentAssignment = new HashMap<String, List<TopicPartition>>();

            for (var memberDescription : groupDescription.members()) {
                allPartitions.addAll(memberDescription.assignment().topicPartitions());
                currentAssignment.put(memberDescription.consumerId(),
                        memberDescription.assignment().topicPartitions().stream().toList());
            }

            var weights = weightService.computeWeights(allPartitions);
            var optimalAssignment = balanceService.computeOptimalAssignment(currentAssignment.keySet(), weights);
            var optimalMaxLoaded = calculateMaxLoadedMember(optimalAssignment, weights);
            var currentMaxLoaded = calculateMaxLoadedMember(currentAssignment, weights);

            if (optimalMaxLoaded == null || currentMaxLoaded == null) {
                return false;
            }

            boolean shouldTrigger = currentMaxLoaded.memberLoad / optimalMaxLoaded.memberLoad > threshold;

            log.info("ThresholdTrigger result: optimalMaxLoaded={}, currentMaxLoaded={}, shouldTrigger={}",
                    optimalMaxLoaded, currentMaxLoaded, shouldTrigger);
            return shouldTrigger;
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
