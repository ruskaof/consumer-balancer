package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Without any weight-store configuration the offset-rate store is the default — no
 * Prometheus beans, no required weight-query-template — and its intervals bind from
 * the consumer-balancer.offset-rate.* properties.
 */
@SpringBootTest(classes = BalancerAutoConfigurationOffsetRateDefaultTest.App.class, properties = {
        "consumer-balancer.proactive-rebalance-enabled=false",
        "consumer-balancer.offset-rate.rate-interval=2m",
        "consumer-balancer.offset-rate.sample-interval=5s"
})
class BalancerAutoConfigurationOffsetRateDefaultTest {

    @Autowired
    ApplicationContext context;

    @Test
    void offsetRateStoreIsTheDefaultAndBindsIntervals() {
        WeightService weightService = context.getBean(WeightService.class);

        assertThat(weightService).isInstanceOf(KafkaOffsetRateWeightService.class);
        KafkaOffsetRateWeightService offsetRateWeightService = (KafkaOffsetRateWeightService) weightService;
        assertThat(offsetRateWeightService.getRateInterval()).isEqualTo(Duration.ofMinutes(2));
        assertThat(offsetRateWeightService.getSampleInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(context.getBeanNamesForType(PrometheusClient.class)).isEmpty();
    }

    @SpringBootApplication(exclude = KafkaAutoConfiguration.class)
    static class App {

        @Bean
        org.springframework.boot.kafka.autoconfigure.KafkaProperties kafkaProperties() {
            org.springframework.boot.kafka.autoconfigure.KafkaProperties p = new org.springframework.boot.kafka.autoconfigure.KafkaProperties();
            p.setBootstrapServers(List.of("127.0.0.1:9092"));
            p.getConsumer().setGroupId("test-group");
            return p;
        }

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return mock(KafkaListenerEndpointRegistry.class);
        }
    }
}
