package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.metrics.ConsumerBalancerMetrics;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The metrics autoconfiguration must never break a context: without a MeterRegistry (or
 * with the balancer off) it backs off entirely, and when parts of the balancer stack are
 * missing or replaced by custom implementations it just binds fewer meters.
 */
class BalancerMetricsAutoConfigurationBackoffTest {

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
    }

    @Configuration(proxyBeanMethods = false)
    static class Meters {

        @Bean
        SimpleMeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomTrigger {

        @Bean
        RebalanceTrigger customRebalanceTrigger() {
            return () -> false;
        }
    }

    @Nested
    @SpringBootTest(classes = TestApp.class, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group"
    })
    class WithoutMeterRegistry {

        @Autowired
        ApplicationContext context;

        @Test
        void backsOffEntirely() {
            assertThat(context.getBeanNamesForType(ConsumerBalancerMetrics.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest(classes = {TestApp.class, Meters.class}, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group",
            "consumer-balancer.enabled=false"
    })
    class WithTheBalancerDisabled {

        @Autowired
        ApplicationContext context;

        @Test
        void backsOffWithTheBalancer() {
            assertThat(context.getBeanNamesForType(ConsumerBalancerMetrics.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest(classes = {TestApp.class, Meters.class}, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group",
            "consumer-balancer.proactive-rebalance-enabled=false"
    })
    class WithProactiveRebalanceDisabled {

        @Autowired
        ApplicationContext context;

        @Autowired
        SimpleMeterRegistry meterRegistry;

        @Test
        void bindsOnlyTheMetersOfExistingComponents() {
            assertThat(context.getBean(ConsumerBalancerMetrics.class)).isNotNull();
            assertThat(meterRegistry.find("consumer.balancer.trigger.imbalance.ratio").gauge()).isNull();
            assertThat(meterRegistry.find("consumer.balancer.coordinator").gauge()).isNull();
            assertThat(meterRegistry.find("consumer.balancer.rebalance.initiations").functionCounter()).isNull();
            assertThat(meterRegistry.find("consumer.balancer.offset.rate.tracked.partitions").gauge()).isNotNull();
        }
    }

    @Nested
    @SpringBootTest(classes = {TestApp.class, Meters.class, CustomTrigger.class}, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group"
    })
    class WithACustomTrigger {

        @Autowired
        SimpleMeterRegistry meterRegistry;

        @Test
        void skipsTheThresholdTriggerMetersButKeepsTheRest() {
            assertThat(meterRegistry.find("consumer.balancer.trigger.imbalance.ratio").gauge()).isNull();
            assertThat(meterRegistry.find("consumer.balancer.coordinator").gauge()).isNotNull();
        }
    }
}
