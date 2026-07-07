package io.github.ruskaof.balancer.prometheus;

import java.time.Duration;
import java.util.Objects;

/**
 * Connection settings for {@link PrometheusClient} (Prometheus HTTP API).
 *
 * <p>{@code pathPrefix} is prepended to the {@code /api/v1/query} endpoint and makes the
 * client work with any Prometheus-API-compatible backend: empty for Prometheus itself,
 * {@code /prometheus} for single-node VictoriaMetrics, or
 * {@code /select/<accountID>/prometheus} for a VictoriaMetrics cluster (vmselect).
 * The value is normalized to either an empty string or a {@code /}-prefixed path without
 * a trailing slash; {@code null} counts as empty.
 */
public record PrometheusConnectionSettings(
        String scheme,
        String host,
        int port,
        String pathPrefix,
        Duration connectTimeout,
        Duration requestTimeout) {

    public PrometheusConnectionSettings {
        Objects.requireNonNull(scheme, "scheme");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        pathPrefix = normalizePathPrefix(pathPrefix);
    }

    /**
     * Origin URL without path, e.g. {@code http://localhost:9090}.
     */
    public String baseUrl() {
        return scheme + "://" + host + ":" + port;
    }

    private static String normalizePathPrefix(String pathPrefix) {
        if (pathPrefix == null || pathPrefix.isBlank()) {
            return "";
        }
        String normalized = pathPrefix.strip();
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
