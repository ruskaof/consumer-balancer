package com.ruskaof.listener.prometheus;

import java.time.Duration;
import java.util.Objects;

/**
 * Connection settings for {@link PrometheusClient} (Prometheus HTTP API).
 */
public final class PrometheusConnectionSettings {

    private final String scheme;
    private final String host;
    private final int port;
    private final Duration connectTimeout;
    private final Duration requestTimeout;

    public PrometheusConnectionSettings(
            String scheme,
            String host,
            int port,
            Duration connectTimeout,
            Duration requestTimeout
    ) {
        this.scheme = Objects.requireNonNull(scheme, "scheme");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    }

    public String scheme() {
        return scheme;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * Origin URL without path, e.g. {@code http://localhost:9090}.
     */
    public String baseUrl() {
        return scheme + "://" + host + ":" + port;
    }
}
