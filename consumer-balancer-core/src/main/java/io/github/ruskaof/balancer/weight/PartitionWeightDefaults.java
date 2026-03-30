package io.github.ruskaof.balancer.weight;

/**
 * Default weight when a partition is missing from a {@link WeightService}
 * result.
 */
public final class PartitionWeightDefaults {

    public static final double MISSING = 1.0d;

    private PartitionWeightDefaults() {
    }
}
