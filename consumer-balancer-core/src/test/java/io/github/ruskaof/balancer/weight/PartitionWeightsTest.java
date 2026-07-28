package io.github.ruskaof.balancer.weight;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionWeightsTest {

    private static final TopicPartition T0 = new TopicPartition("t", 0);
    private static final TopicPartition T1 = new TopicPartition("t", 1);

    @Test
    void countsEntriesThatFellBackToTheDefaultWeight() {
        // NaN and a missing entry both fall back.
        PartitionWeights.Sanitized sanitized =
                PartitionWeights.sanitizedCounted(Set.of(T0, T1), Map.of(T0, Double.NaN));

        assertEquals(2, sanitized.defaultedCount());
        assertEquals(
                Map.of(T0, PartitionWeightDefaults.MISSING, T1, PartitionWeightDefaults.MISSING),
                sanitized.weights());
    }

    @Test
    void countsNothingWhenAllWeightsAreUsable() {
        PartitionWeights.Sanitized sanitized =
                PartitionWeights.sanitizedCounted(Set.of(T0, T1), Map.of(T0, 1.0, T1, 2.0));

        assertEquals(0, sanitized.defaultedCount());
        assertEquals(Map.of(T0, 1.0, T1, 2.0), sanitized.weights());
    }

    @Test
    void treatsANullMapAsAllDefaulted() {
        assertEquals(2, PartitionWeights.sanitizedCounted(Set.of(T0, T1), null).defaultedCount());
    }

    @Test
    void dropsWeightsOfOtherPartitionsWithoutCountingThem() {
        PartitionWeights.Sanitized sanitized =
                PartitionWeights.sanitizedCounted(Set.of(T0), Map.of(T0, 1.0, T1, 5.0));

        assertEquals(0, sanitized.defaultedCount());
        assertEquals(Map.of(T0, 1.0), sanitized.weights());
    }

    @Test
    void sanitizedReturnsTheSameWeightsWithoutTheCount() {
        assertEquals(
                Map.of(T0, PartitionWeightDefaults.MISSING, T1, 2.0),
                PartitionWeights.sanitized(Set.of(T0, T1), Map.of(T1, 2.0)));
    }
}
