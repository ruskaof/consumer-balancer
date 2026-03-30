package com.ruskaof.listener.balance;

import com.ruskaof.listener.weight.PartitionWeightDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SortingRoundRobinBalanceService implements BalanceService {

    @Override
    public Map<String, List<TopicPartition>> computeOptimalAssignment(
            Set<String> members,
            Map<TopicPartition, Double> allPartitions
    ) {
        if (members == null || members.isEmpty()) {
            throw new RuntimeException("No members provided for assignment");
        }
        if (allPartitions == null || allPartitions.isEmpty()) {
            return members.stream()
                    .collect(Collectors.toMap(
                            m -> m,
                            m -> new ArrayList<>()
                    ));
        }

        List<TopicPartition> sortedPartitions = allPartitions.keySet().stream()
                .sorted((tp1, tp2) -> Double.compare(
                        allPartitions.get(tp2),
                        allPartitions.get(tp1)))
                .toList();

        Map<String, Double> consumerLoads = members.stream()
                .collect(Collectors.toMap(
                        m -> m,
                        m -> 0.0,
                        (a, b) -> a,
                        TreeMap::new
                ));

        Map<String, List<TopicPartition>> assignment = members.stream()
                .collect(Collectors.toMap(
                        m -> m,
                        m -> new ArrayList<>(),
                        (a, b) -> a,
                        TreeMap::new
                ));

        for (TopicPartition partition : sortedPartitions) {
            double partitionLoad = allPartitions.getOrDefault(partition, PartitionWeightDefaults.MISSING);

            String leastLoadedMember = consumerLoads.entrySet().stream()
                    .min((e1, e2) -> {
                        int loadCompare = Double.compare(e1.getValue(), e2.getValue());
                        return (loadCompare != 0)
                                ? loadCompare
                                : e1.getKey().compareTo(e2.getKey());
                    })
                    .orElseThrow(() -> new IllegalStateException("No consumers available"))
                    .getKey();

            assignment.get(leastLoadedMember).add(partition);
            consumerLoads.put(leastLoadedMember, consumerLoads.get(leastLoadedMember) + partitionLoad);

            if (log.isTraceEnabled()) {
                log.trace("Assigned partition {} (load={}) to member {} (new load={})",
                        partition, partitionLoad, leastLoadedMember, consumerLoads.get(leastLoadedMember));
            }
        }

        log.info("Computed load-aware assignment: {}", assignment);
        return assignment;
    }
}
