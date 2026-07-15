package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Default weight store: measures per-partition events/sec from end-offset growth using
 * the balancer's shared {@link AdminClient} — no external metrics backend needed.
 */
@AutoConfiguration
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(name = "consumer-balancer.weight-store", havingValue = "offset-rate", matchIfMissing = true)
@ConditionalOnMissingBean(WeightService.class)
public class KafkaOffsetRateWeightAutoConfiguration {

    @Bean(destroyMethod = "close")
    public KafkaOffsetRateWeightService offsetRateWeightService(
            AdminClient kafkaBalancerAdminClient,
            KafkaBalancerProperties kafkaBalancerProperties) {
        KafkaBalancerProperties.OffsetRate offsetRate = kafkaBalancerProperties.getOffsetRate();
        return offsetRate.getSampleInterval() == null
                ? new KafkaOffsetRateWeightService(kafkaBalancerAdminClient, offsetRate.getRateInterval())
                : new KafkaOffsetRateWeightService(
                        kafkaBalancerAdminClient, offsetRate.getRateInterval(), offsetRate.getSampleInterval());
    }
}
