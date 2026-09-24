package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.ConsumerGroupBalancers;
import io.github.ruskaof.balancer.metrics.ConsumerBalancerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Binds balancer meters when the application brings Micrometer (e.g. through
 * {@code spring-boot-starter-actuator}); without it, this configuration backs off entirely.
 *
 * <p>The ordering mirrors Boot's own {@code KafkaMetricsAutoConfiguration}: the
 * {@code afterName} strings avoid a hard dependency on {@code spring-boot-micrometer-metrics},
 * and every registry export configuration runs before
 * {@code CompositeMeterRegistryAutoConfiguration}, so the {@code @ConditionalOnBean} check
 * sees whatever registries the application ends up with.
 */
@AutoConfiguration(
        after = BalancerAutoConfiguration.class,
        afterName = {
                "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
                "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"})
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnBean(MeterRegistry.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
public class BalancerMetricsAutoConfiguration {

    /**
     * One binder for every {@link ConsumerGroupBalancers} bean — the auto-configured one and
     * any the application declares for further Kafka clusters. Groups registered with them
     * after startup get their meters on registration.
     */
    @Bean
    public ConsumerBalancerMetrics consumerBalancerMetrics(
            MeterRegistry meterRegistry,
            ObjectProvider<ConsumerGroupBalancers> consumerGroupBalancers) {
        ConsumerBalancerMetrics metrics = ConsumerBalancerMetrics.of(consumerGroupBalancers.orderedStream().toList());
        metrics.bindTo(meterRegistry);
        return metrics;
    }
}
