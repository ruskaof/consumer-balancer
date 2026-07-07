package io.github.ruskaof.balancer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * Resolves a collaborator from a Kafka consumer config entry whose value may be an
 * already-built instance, a {@link Class}, or a fully-qualified class name.
 */
public final class ConfigInstanceResolver {

    private ConfigInstanceResolver() {
    }

    /**
     * Resolves the value of {@code key} in {@code configs} to an instance of {@code expectedType}:
     *
     * <ul>
     * <li>absent, {@code null} or blank {@link String}: returns {@code null} (caller applies its default)</li>
     * <li>an instance of {@code expectedType}: returned as-is; {@code configure} is <b>not</b> called on it —
     * pre-built instances (e.g. beans shared between consumers) are configured by their owner</li>
     * <li>a {@link Class} or a {@link String} class name: instantiated via the public no-arg constructor;
     * if the new object implements {@link Configurable}, {@code configure(configs)} is called on it</li>
     * <li>anything else: {@link IllegalArgumentException}</li>
     * </ul>
     */
    public static <T> T resolveOrNull(Map<String, ?> configs, String key, Class<T> expectedType) {
        Object value = configs.get(key);
        if (value == null || (value instanceof String s && s.isBlank())) {
            return null;
        }
        if (expectedType.isInstance(value)) {
            return expectedType.cast(value);
        }
        Class<?> clazz = classFrom(value, key, expectedType);
        if (!expectedType.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(
                    "Class '" + clazz.getName() + "' configured for '" + key
                            + "' does not implement " + expectedType.getName());
        }
        Object instance;
        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate '" + clazz.getName() + "' for config '" + key
                            + "' (public no-arg constructor required)", e);
        }
        if (instance instanceof Configurable configurable) {
            configurable.configure(configs);
        }
        return expectedType.cast(instance);
    }

    private static Class<?> classFrom(Object value, String key, Class<?> expectedType) {
        if (value instanceof Class<?> clazz) {
            return clazz;
        }
        if (value instanceof String className) {
            try {
                return Class.forName(className, true, Utils.getContextOrKafkaClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                        "Class not found for config '" + key + "': " + className, e);
            }
        }
        throw new IllegalArgumentException(
                "Config '" + key + "' must be an instance of " + expectedType.getName()
                        + ", a Class, or a String class name, but was: " + value.getClass().getName());
    }
}
