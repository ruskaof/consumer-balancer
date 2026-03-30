package io.github.ruskaof.balancer.balance;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BalanceService {
    Map<String, List<TopicPartition>> computeOptimalAssignment(
            Set<String> members,
            Map<TopicPartition, Double> allPartitions);
}
