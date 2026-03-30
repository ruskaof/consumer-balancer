package io.github.ruskaof.balancer.weight;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public interface WeightService {

    Map<TopicPartition, Double> computeWeights(
            Set<TopicPartition> allPartitions);
}
