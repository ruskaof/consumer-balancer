package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.metrics.ConsumerBalancerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With a MeterRegistry in the context, the starter binds the balancer's meters
 * automatically, all tagged with the consumer group. Nothing has run against the (dead)
 * broker yet, so every meter shows its documented initial value.
 */
@SpringBootTest(classes = BalancerMetricsAutoConfigurationTest.TestApp.class, properties = {
        "spring.kafka.bootstrap-servers=127.0.0.1:9092",
        "spring.kafka.consumer.group-id=test-group"
})
class BalancerMetricsAutoConfigurationTest {

    @Autowired
    ApplicationContext context;

    @Autowired
    SimpleMeterRegistry meterRegistry;

    @Test
    void bindsTheBalancerMetersWithTheGroupTag() {
        assertThat(context.getBean(ConsumerBalancerMetrics.class)).isNotNull();

        assertThat(meterRegistry.get("consumer.balancer.trigger.imbalance.threshold")
                .tag("group", "test-group").gauge().value()).isEqualTo(1.1);
        assertThat(meterRegistry.get("consumer.balancer.trigger.imbalance.ratio")
                .tag("group", "test-group").gauge().value()).isNaN();
        assertThat(meterRegistry.get("consumer.balancer.trigger.cooldown")
                .tag("group", "test-group").timeGauge().value(TimeUnit.MINUTES)).isEqualTo(10.0);
        assertThat(meterRegistry.get("consumer.balancer.trigger.evaluations")
                .tags("group", "test-group", "outcome", "fired").functionCounter().count()).isZero();
        assertThat(meterRegistry.get("consumer.balancer.coordinator")
                .tag("group", "test-group").gauge().value()).isZero();
        assertThat(meterRegistry.get("consumer.balancer.rebalance.initiations")
                .tags("group", "test-group", "result", "no_match").functionCounter().count()).isZero();
        assertThat(meterRegistry.get("consumer.balancer.offset.rate.tracked.partitions")
                .tag("group", "test-group").gauge().value()).isZero();
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({
            KafkaAutoConfiguration.class,
            DefaultBalanceServiceAutoConfiguration.class,
            KafkaOffsetRateWeightAutoConfiguration.class,
            PrometheusWeightAutoConfiguration.class,
            BalancerAutoConfiguration.class,
            BalancerMetricsAutoConfiguration.class
    })
    static class TestApp {

        @Bean
        SimpleMeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
