package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.prometheus.KafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.prometheus.TemplatedKafkaRatePromqlBuilder;
import io.github.ruskaof.balancer.weight.WeightService;
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
                            + " (see README for examples). Alternatively, define a WeightService bean or "
                            + "disable the balancer with consumer-balancer.enabled=false.");
        }
        return new TemplatedKafkaRatePromqlBuilder(template);
    }
}
