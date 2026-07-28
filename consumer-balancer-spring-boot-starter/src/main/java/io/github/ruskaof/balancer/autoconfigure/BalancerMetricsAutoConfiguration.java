package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.ContainerRegistryRebalanceInitiator;
import io.github.ruskaof.balancer.metrics.ConsumerBalancerMetrics;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.KafkaOffsetRateWeightService;
import io.github.ruskaof.balancer.weight.WeightService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
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
     * One binder for the auto-configured balancer stack, covering whichever of its parts
     * exist in the context. Applications running hand-wired stacks against several Kafka
     * clusters define their own {@link ConsumerBalancerMetrics} beans instead, one per
     * stack with a distinguishing tag — several candidate beans of one type make
     * {@code getIfUnique()} return {@code null} here, which only drops the ambiguous
     * component's meters.
     */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerBalancerMetrics consumerBalancerMetrics(
            MeterRegistry meterRegistry,
            KafkaProperties kafkaProperties,
            ObjectProvider<RebalanceTrigger> rebalanceTrigger,
            ObjectProvider<CoordinatorManager> coordinatorManager,
            ObjectProvider<CoordinatorManager.RebalanceInitiator> rebalanceInitiator,
            ObjectProvider<WeightService> weightService) {
        String groupId = kafkaProperties.getConsumer().getGroupId();
        ConsumerBalancerMetrics metrics = new ConsumerBalancerMetrics(
                Tags.of("group", groupId == null ? "" : groupId),
                asType(rebalanceTrigger, ThresholdTrigger.class),
                coordinatorManager.getIfUnique(),
                asType(rebalanceInitiator, ContainerRegistryRebalanceInitiator.class),
                asType(weightService, KafkaOffsetRateWeightService.class));
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /**
     * The balancer's {@code @Bean} methods declare interface return types, so the concrete
     * types the meters need cannot be matched by {@code @ConditionalOnBean}; they are
     * narrowed at runtime instead. A user-supplied implementation of a different type
     * yields {@code null} — fewer meters, never a failed context.
     */
    private static <T> T asType(ObjectProvider<?> provider, Class<T> type) {
        Object candidate = provider.getIfUnique();
        return type.isInstance(candidate) ? type.cast(candidate) : null;
    }
}
