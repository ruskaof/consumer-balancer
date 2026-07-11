package io.github.ruskaof.balancer.prometheus;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

    @Test
    void normalizesAuthorizationHeader() {
        assertNull(settingsWithAuthorization(null).authorizationHeader());
        assertNull(settingsWithAuthorization("").authorizationHeader());
        assertNull(settingsWithAuthorization("   ").authorizationHeader());
        assertEquals("Bearer x", settingsWithAuthorization(" Bearer x ").authorizationHeader());
    }

    @Test
    void constructorWithoutAuthorizationSendsNoHeader() {
        assertNull(settingsWithPrefix("").authorizationHeader());
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

    private static PrometheusConnectionSettings settingsWithAuthorization(String authorizationHeader) {
        return new PrometheusConnectionSettings(
                "http",
                "localhost",
                9090,
                "",
                authorizationHeader,
                Duration.ofSeconds(5),
                Duration.ofSeconds(5));
    }
}
