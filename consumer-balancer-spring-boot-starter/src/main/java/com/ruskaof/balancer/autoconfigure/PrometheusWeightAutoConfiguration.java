package com.ruskaof.balancer.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import com.ruskaof.balancer.prometheus.PrometheusClient;
import com.ruskaof.balancer.prometheus.PrometheusObjectMappers;
import com.ruskaof.balancer.weight.PrometheusWeightService;
import com.ruskaof.balancer.weight.WeightService;
import org.springframework.beans.factory.ObjectProvider;
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
    public PrometheusClient prometheusClient(
            KafkaBalancerProperties kafkaBalancerProperties,
            ObjectProvider<ObjectMapper> objectMapperProvider) {
        ObjectMapper mapper = objectMapperProvider.getIfAvailable();
        if (mapper == null) {
            mapper = PrometheusObjectMappers.create();
        }
        return new PrometheusClient(
                PrometheusConnectionSettingsFactory.from(kafkaBalancerProperties),
                mapper);
    }

    @Bean
    public WeightService prometheusWeightService(
            KafkaRatePromqlBuilder kafkaRatePromqlBuilder,
            PrometheusClient prometheusClient) {
        return new PrometheusWeightService(kafkaRatePromqlBuilder, prometheusClient);
    }
}
