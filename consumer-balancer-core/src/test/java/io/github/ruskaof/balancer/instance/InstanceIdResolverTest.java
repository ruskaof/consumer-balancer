package io.github.ruskaof.balancer.instance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InstanceIdResolverTest {

    @Test
    void configuredIdWinsAndIsTrimmed() {
        assertEquals("pod-42", InstanceIdResolver.resolve("  pod-42  "));
    }

    @Test
    void blankConfiguredIdFallsBackToAuto() {
        String resolved = InstanceIdResolver.resolve("   ");
        assertNotNull(resolved);
        assertFalse(resolved.isBlank());
        assertEquals(InstanceIdResolver.autoInstanceId(), resolved);
    }

    @Test
    void rejectsOversizedConfiguredId() {
        assertThrows(IllegalArgumentException.class,
                () -> InstanceIdResolver.resolve("x".repeat(InstanceUserData.MAX_INSTANCE_ID_BYTES + 1)));
    }

    @Test
    void autoIdIsStableAcrossCalls() {
        InstanceIdResolver.resetCacheForTesting();
        String first = InstanceIdResolver.autoInstanceId();
        assertEquals(first, InstanceIdResolver.autoInstanceId(),
                "every consumer in the JVM must report the same instance id");
    }

    @Test
    void autoIdChangesOnlyWhenTheJvmDoes() {
        InstanceIdResolver.resetCacheForTesting();
        String first = InstanceIdResolver.autoInstanceId();
        InstanceIdResolver.resetCacheForTesting();
        assertNotEquals(first, InstanceIdResolver.autoInstanceId(),
                "a fresh JVM (simulated by the reset) must not collide with the previous id");
    }
}
