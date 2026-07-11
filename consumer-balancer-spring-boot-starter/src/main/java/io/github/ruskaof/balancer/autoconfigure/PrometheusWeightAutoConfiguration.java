package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import io.github.ruskaof.balancer.weight.PrometheusWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = {
        DefaultKafkaRatePromqlBuilderAutoConfiguration.class
})
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnMissingBean(WeightService.class)
public class PrometheusWeightAutoConfiguration {

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
            PrometheusClient prometheusClient) {
        return new PrometheusWeightService(kafkaRatePromqlBuilder, prometheusClient);
    }
}
