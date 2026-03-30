package com.ruskaof.listener.weight;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public interface WeightService {

    Map<TopicPartition, Double> computeWeights(
            Set<TopicPartition> allPartitions
    );
}
