package com.ruskaof.balancer.autoconfigure;

import com.ruskaof.listener.prometheus.KafkaRatePromqlBuilder;
import com.ruskaof.listener.prometheus.TemplatedKafkaRatePromqlBuilder;
import com.ruskaof.listener.weight.WeightService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnMissingBean({ WeightService.class, KafkaRatePromqlBuilder.class })
public class DefaultKafkaRatePromqlBuilderAutoConfiguration {

    @Bean
    public KafkaRatePromqlBuilder kafkaRatePromqlBuilder(KafkaBalancerProperties kafkaBalancerProperties) {
        String template = kafkaBalancerProperties.getPrometheus().getWeightQueryTemplate();
        if (template == null || template.isBlank()) {
            throw new IllegalStateException(
                    "consumer-balancer.prometheus.weight-query-template is required when using the default "
                            + "Prometheus weight store. It must contain the placeholder "
                            + "%s"
                            + " (see README for examples).");
        }
        return new TemplatedKafkaRatePromqlBuilder(template);
    }
}
