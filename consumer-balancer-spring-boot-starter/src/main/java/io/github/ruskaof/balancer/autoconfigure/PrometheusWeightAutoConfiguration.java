package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import io.github.ruskaof.balancer.prometheus.TemplatedKafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Opt-in weight store ({@code consumer-balancer.weight-store=prometheus}): loads
 * per-partition weights from a Prometheus-compatible backend via the configured
 * PromQL weight query.
 */
@AutoConfiguration
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(name = "consumer-balancer.weight-store", havingValue = "prometheus")
@ConditionalOnMissingBean(WeightService.class)
public class PrometheusWeightAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(KafkaRatePromqlBuilder.class)
    public KafkaRatePromqlBuilder kafkaRatePromqlBuilder(KafkaBalancerProperties kafkaBalancerProperties) {
        String template = kafkaBalancerProperties.getPrometheus().getWeightQueryTemplate();
        if (template == null || template.isBlank()) {
            throw new IllegalStateException(
                    "consumer-balancer.prometheus.weight-query-template is required when using the "
                            + "Prometheus weight store (consumer-balancer.weight-store=prometheus). It must "
                            + "contain the placeholder "
                            + "%s"
                            + " (see README for examples). Alternatively, define a WeightService bean or use "
                            + "the default offset-rate weight store.");
        }
        return new TemplatedKafkaRatePromqlBuilder(template);
    }

    @Bean
    public PrometheusClient prometheusClient(KafkaBalancerProperties kafkaBalancerProperties) {
        // Always uses the library's own mapper: the Prometheus wire format is fixed, so
        // application-level Jackson customizations (naming strategies, strict unknown
        // properties, modules) must not affect how responses are parsed.
        return new PrometheusClient(
                PrometheusConnectionSettingsFactory.from(kafkaBalancerProperties),
                PrometheusObjectMappers.create());
    }

    @Bean
    public WeightService prometheusWeightService(
            KafkaRatePromqlBuilder kafkaRatePromqlBuilder,
            PrometheusClient prometheusClient,
            KafkaBalancerProperties kafkaBalancerProperties) {
        return new PrometheusWeightService(
                kafkaRatePromqlBuilder,
                prometheusClient,
                kafkaBalancerProperties.getPrometheus().getTopicLabel(),
                kafkaBalancerProperties.getPrometheus().getPartitionLabel());
    }
}
