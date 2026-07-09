package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.prometheus.PrometheusConnectionSettings;

/**
 * Maps {@link KafkaBalancerProperties} to core
 * {@link PrometheusConnectionSettings}.
 */
public final class PrometheusConnectionSettingsFactory {

    private PrometheusConnectionSettingsFactory() {
    }

    public static PrometheusConnectionSettings from(KafkaBalancerProperties properties) {
        var p = properties.getPrometheus();
        return new PrometheusConnectionSettings(
                p.getScheme(),
                p.getHost(),
                p.getPort(),
                p.getPathPrefix(),
                p.getAuthorization(),
                p.getConnectTimeout(),
                p.getRequestTimeout());
    }
}
