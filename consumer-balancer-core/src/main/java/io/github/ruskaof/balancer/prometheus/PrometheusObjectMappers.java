package io.github.ruskaof.balancer.prometheus;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Shared Jackson configuration for Prometheus API JSON.
 */
public final class PrometheusObjectMappers {

    private PrometheusObjectMappers() {
    }

    public static ObjectMapper create() {
        return JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .build();
    }
}
