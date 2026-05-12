package io.github.ruskaof.balancer.prometheus;

import java.time.Duration;
import java.util.Objects;

/**
 * Connection settings for {@link PrometheusClient} (Prometheus HTTP API).
 */
public record PrometheusConnectionSettings(
        String scheme,
        String host,
        int port,
        Duration connectTimeout,
        Duration requestTimeout) {

    public PrometheusConnectionSettings {
        Objects.requireNonNull(scheme, "scheme");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
    }

    /**
     * Origin URL without path, e.g. {@code http://localhost:9090}.
     */
    public String baseUrl() {
        return scheme + "://" + host + ":" + port;
    }
}
