package io.github.ruskaof.balancer.prometheus;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrometheusConnectionSettingsTest {

    @Test
    void normalizesPathPrefix() {
        assertEquals("", settingsWithPrefix(null).pathPrefix());
        assertEquals("", settingsWithPrefix("").pathPrefix());
        assertEquals("", settingsWithPrefix("   ").pathPrefix());
        assertEquals("", settingsWithPrefix("/").pathPrefix());
        assertEquals("/prometheus", settingsWithPrefix("prometheus").pathPrefix());
        assertEquals("/prometheus", settingsWithPrefix("/prometheus").pathPrefix());
        assertEquals("/prometheus", settingsWithPrefix("/prometheus/").pathPrefix());
        assertEquals("/select/0/prometheus", settingsWithPrefix("/select/0/prometheus/").pathPrefix());
    }

    private static PrometheusConnectionSettings settingsWithPrefix(String pathPrefix) {
        return new PrometheusConnectionSettings(
                "http",
                "localhost",
                9090,
                pathPrefix,
                Duration.ofSeconds(5),
                Duration.ofSeconds(5));
    }
}
