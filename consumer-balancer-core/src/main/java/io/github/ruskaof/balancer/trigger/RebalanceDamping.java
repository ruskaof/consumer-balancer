package io.github.ruskaof.balancer.trigger;

import java.time.Duration;
import java.util.Objects;

/**
 * How reluctant a proactive trigger is to fire. Every proactive rebalance stops the whole
 * consumer group, so a trigger that fires on every check it dislikes costs far more than the
 * imbalance it chases: with the default 30 s check interval, an imbalance the trigger cannot
 * actually fix turns into a rebalance every 30 s forever.
 *
 * <p>Two independent limits prevent that:
 * <ul>
 *   <li>{@code minViolatedChecks} — how long the imbalance must persist (in checks on one
 *       unchanged assignment) before it counts as real rather than as measurement noise. The
 *       count decays rather than resets on a check that finds the group balanced, so a ratio
 *       drifting across the threshold still converges on a decision. Keep it low: the weight
 *       store already averages over its own window, so consecutive checks are correlated
 *       samples, and every check spent confirming is a check the imbalance goes uncorrected;</li>
 *   <li>{@code cooldown} — a hard floor on the wall-clock time between two fires. It applies
 *       whether or not the previous rebalance changed anything, which is what bounds the
 *       damage when the trigger's model of the group disagrees with the assignor's. Each fire
 *       that does not restore balance doubles the cooldown, up to {@code maxCooldown}, so a
 *       trigger that keeps asking for a rebalance nobody can satisfy fades out instead of
 *       churning; the cooldown returns to its base as soon as the group is seen balanced.</li>
 * </ul>
 *
 * @param minViolatedChecks checks that must see the imbalance on one unchanged assignment
 *                          before the trigger fires; {@code 1} fires on first sight
 * @param cooldown          minimum time between two fires; {@link Duration#ZERO} disables
 *                          both the cooldown and its backoff
 * @param maxCooldown       ceiling for the doubled cooldown; must not be shorter than
 *                          {@code cooldown}
 */
public record RebalanceDamping(int minViolatedChecks, Duration cooldown, Duration maxCooldown) {

    public static final int DEFAULT_MIN_VIOLATED_CHECKS = 2;
    public static final Duration DEFAULT_COOLDOWN = Duration.ofMinutes(10);
    public static final Duration DEFAULT_MAX_COOLDOWN = Duration.ofHours(2);

    public RebalanceDamping {
        if (minViolatedChecks < 1) {
            throw new IllegalArgumentException(
                    "minViolatedChecks must be at least 1, but was " + minViolatedChecks);
        }
        Objects.requireNonNull(cooldown, "cooldown");
        Objects.requireNonNull(maxCooldown, "maxCooldown");
        if (cooldown.isNegative()) {
            throw new IllegalArgumentException("cooldown must not be negative, but was " + cooldown);
        }
        if (maxCooldown.compareTo(cooldown) < 0) {
            throw new IllegalArgumentException(
                    "maxCooldown (" + maxCooldown + ") must not be shorter than cooldown (" + cooldown + ")");
        }
    }

    /** Two violated checks, a 10 minute cooldown backing off to 2 hours. */
    public static RebalanceDamping defaults() {
        return new RebalanceDamping(DEFAULT_MIN_VIOLATED_CHECKS, DEFAULT_COOLDOWN, DEFAULT_MAX_COOLDOWN);
    }

    /**
     * No hysteresis and no cooldown: every violated check fires. Only sensible when the
     * rebalance rate is bounded elsewhere.
     */
    public static RebalanceDamping none() {
        return new RebalanceDamping(1, Duration.ZERO, Duration.ZERO);
    }
}
