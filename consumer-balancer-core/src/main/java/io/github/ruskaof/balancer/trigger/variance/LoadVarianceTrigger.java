package io.github.ruskaof.balancer.trigger.variance;

import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.weight.PartitionWeightDefaults;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Fires when load is unevenly spread across members, measured by the
 * coefficient of variation (standard deviation / mean) of per-member load.
 * <p>
 * Unlike {@code ThresholdTrigger}, this does not compute an optimal assignment;
 * it only measures dispersion of the current assignment, so it needs the
 * {@link WeightService} but not a {@code BalanceService}. It triggers when the
 * coefficient of variation exceeds {@code variationThreshold}. This captures
 * skew that is spread across several members at once, which a single
 * max-vs-optimal ratio can understate.
 */
@Slf4j
@RequiredArgsConstructor
public class LoadVarianceTrigger implements RebalanceTrigger {

    private final AdminClient adminClient;
    private final String groupId;
    private final WeightService weightService;
    private final double variationThreshold;

    @Override
    public boolean shouldTrigger() {
        try {
            var groupDescription = adminClient.describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();

            if (groupDescription.members().size() < 2) {
                return false;
            }

            var allPartitions = new HashSet<TopicPartition>();
            for (var member : groupDescription.members()) {
                allPartitions.addAll(member.assignment().topicPartitions());
            }

            if (allPartitions.isEmpty()) {
                return false;
            }

            var weights = weightService.computeWeights(allPartitions);

            List<Double> memberLoads = new ArrayList<>();
            for (var member : groupDescription.members()) {
                double load = 0;
                for (TopicPartition tp : member.assignment().topicPartitions()) {
                    load += weights.getOrDefault(tp, PartitionWeightDefaults.MISSING);
                }
                memberLoads.add(load);
            }

            double mean = memberLoads.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            if (mean <= 0) {
                return false;
            }

            double variance = memberLoads.stream()
                    .mapToDouble(load -> (load - mean) * (load - mean))
                    .average()
                    .orElse(0);
            double coefficientOfVariation = Math.sqrt(variance) / mean;

            boolean shouldTrigger = coefficientOfVariation > variationThreshold;
            log.info("LoadVarianceTrigger result: mean={}, coefficientOfVariation={}, threshold={}, shouldTrigger={}",
                    mean, coefficientOfVariation, variationThreshold, shouldTrigger);
            return shouldTrigger;
        } catch (Exception e) {
            log.error("Could not run LoadVarianceTrigger", e);
            return false;
        }
    }
}
