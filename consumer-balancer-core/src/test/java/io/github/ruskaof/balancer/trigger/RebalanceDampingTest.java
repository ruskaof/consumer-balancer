package io.github.ruskaof.balancer.trigger;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RebalanceDampingTest {

    @Test
    void rejectsAHysteresisBelowOneCheck() {
        assertThrows(IllegalArgumentException.class,
                () -> new RebalanceDamping(0, Duration.ofMinutes(10), Duration.ofHours(2)));
    }

    @Test
    void rejectsANegativeCooldown() {
        assertThrows(IllegalArgumentException.class,
                () -> new RebalanceDamping(1, Duration.ofMinutes(-1), Duration.ofHours(2)));
    }

    @Test
    void rejectsAMaximumBelowTheCooldown() {
        assertThrows(IllegalArgumentException.class,
                () -> new RebalanceDamping(1, Duration.ofHours(2), Duration.ofMinutes(10)));
    }

    @Test
    void acceptsAMaximumEqualToTheCooldown() {
        assertEquals(Duration.ofMinutes(10),
                new RebalanceDamping(1, Duration.ofMinutes(10), Duration.ofMinutes(10)).maxCooldown());
    }

    @Test
    void noneFiresOnEveryViolatedCheck() {
        assertEquals(new RebalanceDamping(1, Duration.ZERO, Duration.ZERO), RebalanceDamping.none());
    }

    @Test
    void defaultsAreThreeChecksAndATenMinuteCooldown() {
        assertEquals(new RebalanceDamping(3, Duration.ofMinutes(10), Duration.ofHours(2)),
                RebalanceDamping.defaults());
    }
}
