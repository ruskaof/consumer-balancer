package io.github.ruskaof.balancer.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Resolves the application-instance id that groups this JVM's consumers into one instance:
 * a configured id always wins; otherwise a random id generated once per JVM. The id's only
 * job is to be shared by every consumer in the JVM and unique across JVMs — a random
 * per-JVM id satisfies both with no environment assumptions (unlike e.g. the hostname,
 * which would merge JVMs sharing a machine).
 *
 * <p>Configure an explicit id when you want stable, human-readable instance labels in the
 * leader's assignment logs.
 */
public final class InstanceIdResolver {

    private static final Logger log = LoggerFactory.getLogger(InstanceIdResolver.class);

    private static volatile String cachedAutoId;

    private InstanceIdResolver() {
    }

    /**
     * Returns the configured id (trimmed) when non-blank, the memoized per-JVM random id
     * otherwise.
     *
     * @throws IllegalArgumentException when the configured id exceeds
     *                                  {@link InstanceUserData#MAX_INSTANCE_ID_BYTES} UTF-8 bytes
     */
    public static String resolve(String configuredInstanceId) {
        if (configuredInstanceId != null && !configuredInstanceId.isBlank()) {
            String instanceId = configuredInstanceId.trim();
            if (instanceId.getBytes(StandardCharsets.UTF_8).length > InstanceUserData.MAX_INSTANCE_ID_BYTES) {
                throw new IllegalArgumentException(
                        "Configured instance id exceeds " + InstanceUserData.MAX_INSTANCE_ID_BYTES
                                + " UTF-8 bytes: " + instanceId);
            }
            return instanceId;
        }
        return autoInstanceId();
    }

    /** The id generated once per JVM: every consumer in the JVM reports the same instance. */
    public static String autoInstanceId() {
        String id = cachedAutoId;
        if (id == null) {
            synchronized (InstanceIdResolver.class) {
                id = cachedAutoId;
                if (id == null) {
                    id = UUID.randomUUID().toString();
                    log.info("Generated instance id '{}' for this JVM", id);
                    cachedAutoId = id;
                }
            }
        }
        return id;
    }

    static void resetCacheForTesting() {
        cachedAutoId = null;
    }
}
