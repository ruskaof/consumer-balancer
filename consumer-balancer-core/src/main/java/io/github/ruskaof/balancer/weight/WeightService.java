package io.github.ruskaof.balancer.weight;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public interface WeightService {

    /**
     * Computes a load weight for each of the given partitions.
     *
     * <p>Implementations should return one finite weight per requested partition. Callers
     * treat the result as a lookup over {@code allPartitions}: entries for other partitions
     * are ignored, and requested partitions that are missing (or mapped to {@code null} or a
     * non-finite value) fall back to {@link PartitionWeightDefaults#MISSING}.
     */
    Map<TopicPartition, Double> computeWeights(
            Set<TopicPartition> allPartitions);
}
