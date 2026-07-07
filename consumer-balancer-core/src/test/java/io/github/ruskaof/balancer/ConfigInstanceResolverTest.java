package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ConfigInstanceResolverTest {

    private static final String KEY = "assignor.load-aware.weight-service";

    @Test
    void returnsNullWhenKeyAbsentNullOrBlank() {
        assertNull(ConfigInstanceResolver.resolveOrNull(Map.of(), KEY, WeightService.class));

        Map<String, Object> nullValue = new HashMap<>();
        nullValue.put(KEY, null);
        assertNull(ConfigInstanceResolver.resolveOrNull(nullValue, KEY, WeightService.class));

        assertNull(ConfigInstanceResolver.resolveOrNull(Map.of(KEY, "  "), KEY, WeightService.class));
    }

    @Test
    void returnsInstanceAsIsWithoutConfiguringIt() {
        ConfigurableStub instance = new ConfigurableStub();

        WeightService resolved = ConfigInstanceResolver.resolveOrNull(
                Map.of(KEY, instance), KEY, WeightService.class);

        assertSame(instance, resolved);
        assertNull(instance.receivedConfigs, "pre-built instances must not be re-configured");
    }

    @Test
    void instantiatesFromClassName() {
        WeightService resolved = ConfigInstanceResolver.resolveOrNull(
                Map.of(KEY, Stub.class.getName()), KEY, WeightService.class);

        assertInstanceOf(Stub.class, resolved);
    }

    @Test
    void instantiatesFromClassValue() {
        WeightService resolved = ConfigInstanceResolver.resolveOrNull(
                Map.of(KEY, Stub.class), KEY, WeightService.class);

        assertInstanceOf(Stub.class, resolved);
    }

    @Test
    void configuresNewInstancesImplementingConfigurable() {
        Map<String, Object> configs = Map.of(KEY, ConfigurableStub.class.getName(), "other", "value");

        WeightService resolved = ConfigInstanceResolver.resolveOrNull(configs, KEY, WeightService.class);

        assertEquals(configs, ((ConfigurableStub) resolved).receivedConfigs);
    }

    @Test
    void failsOnUnknownClassName() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigInstanceResolver.resolveOrNull(
                        Map.of(KEY, "com.example.DoesNotExist"), KEY, WeightService.class));

        assertTrue(e.getMessage().contains(KEY));
        assertTrue(e.getMessage().contains("com.example.DoesNotExist"));
    }

    @Test
    void failsWhenClassDoesNotImplementExpectedType() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigInstanceResolver.resolveOrNull(
                        Map.of(KEY, String.class.getName()), KEY, WeightService.class));

        assertTrue(e.getMessage().contains("does not implement"));
        assertTrue(e.getMessage().contains(WeightService.class.getName()));
    }

    @Test
    void failsWithoutNoArgConstructor() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigInstanceResolver.resolveOrNull(
                        Map.of(KEY, NoNoArgConstructor.class.getName()), KEY, WeightService.class));

        assertTrue(e.getMessage().contains("no-arg constructor"));
    }

    @Test
    void failsOnUnsupportedValueType() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigInstanceResolver.resolveOrNull(Map.of(KEY, 42), KEY, WeightService.class));

        assertTrue(e.getMessage().contains("must be an instance of"));
        assertTrue(e.getMessage().contains(Integer.class.getName()));
    }

    public static class Stub implements WeightService {
        @Override
        public Map<TopicPartition, Double> computeWeights(Set<TopicPartition> allPartitions) {
            return Map.of();
        }
    }

    public static class ConfigurableStub extends Stub implements Configurable {
        Map<String, ?> receivedConfigs;

        @Override
        public void configure(Map<String, ?> configs) {
            this.receivedConfigs = configs;
        }
    }

    public static class NoNoArgConstructor extends Stub {
        public NoNoArgConstructor(String unused) {
        }
    }
}
