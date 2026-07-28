package io.github.ruskaof.balancer.weight;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class PartitionWeights {

    private static final Logger log = LoggerFactory.getLogger(PartitionWeights.class);

    private PartitionWeights() {
    }

    /**
     * Restricts weights to the partitions being assigned so the assignment covers exactly
     * {@code allPartitions}: entries the weight service did not return (or returned as
     * {@code null}/non-finite) fall back to {@link PartitionWeightDefaults#MISSING}, and
     * entries for other partitions (e.g. stale Prometheus series) are dropped.
     */
    public static Map<TopicPartition, Double> sanitized(
            Set<TopicPartition> allPartitions,
            Map<TopicPartition, Double> rawWeights) {
        return sanitizedCounted(allPartitions, rawWeights).weights();
    }

    /**
     * Like {@link #sanitized(Set, Map)}, but also reports how many partitions fell back to
     * the default weight — a data-quality signal for callers that expose it.
     */
    public static Sanitized sanitizedCounted(
            Set<TopicPartition> allPartitions,
            Map<TopicPartition, Double> rawWeights) {
        Map<TopicPartition, Double> weights = new HashMap<>();
        int defaulted = 0;
        for (TopicPartition tp : allPartitions) {
            Double weight = rawWeights == null ? null : rawWeights.get(tp);
            if (weight == null || !Double.isFinite(weight)) {
                weight = PartitionWeightDefaults.MISSING;
                defaulted++;
            }
            weights.put(tp, weight);
        }
        if (defaulted > 0) {
            log.warn("{} of {} partitions had no usable weight; using default weight {}",
                    defaulted, allPartitions.size(), PartitionWeightDefaults.MISSING);
        }
        return new Sanitized(weights, defaulted);
    }

    /** The sanitized weights plus how many entries fell back to the default. */
    public record Sanitized(Map<TopicPartition, Double> weights, int defaultedCount) {
    }
}
