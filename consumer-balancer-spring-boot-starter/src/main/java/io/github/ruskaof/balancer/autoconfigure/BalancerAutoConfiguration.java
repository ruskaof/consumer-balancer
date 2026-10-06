package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.BalancerConsumerFactoryCustomizer;
import io.github.ruskaof.balancer.ConsumerGroupBalancers;
import io.github.ruskaof.balancer.ConsumerGroupBalancersLifecycle;
import io.github.ruskaof.balancer.ContainerRebalanceInitiator;
import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.trigger.RebalanceInitiator;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ListenerContainerRegistry;

import java.util.List;

@Slf4j
@AutoConfiguration(after = {
        DefaultBalanceServiceAutoConfiguration.class,
        KafkaOffsetRateWeightAutoConfiguration.class,
        PrometheusWeightAutoConfiguration.class
})
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
public class BalancerAutoConfiguration {

    public static final String ADMIN_CLIENT_BEAN_NAME = "kafkaBalancerAdminClient";
    public static final String BALANCERS_BEAN_NAME = "consumerGroupBalancers";

    /**
     * Shared by the default offset-rate weight store and the coordinator election and trigger
     * of every group registered with the auto-configured {@link ConsumerGroupBalancers}.
     */
    @Bean(name = ADMIN_CLIENT_BEAN_NAME, destroyMethod = "close")
    public AdminClient kafkaBalancerAdminClient(KafkaProperties kafkaProperties) {
        // Full admin properties so security settings like SSL/SASL from
        // spring.kafka.* apply to the balancer's admin client too.
        return AdminClient.create(kafkaProperties.buildAdminProperties());
    }

    @Bean
    @ConditionalOnMissingBean(MemberIdTracker.class)
    public MemberIdTracker memberIdTracker() {
        return new MemberIdTracker();
    }

    /**
     * The balancer of the cluster {@code spring.kafka.*} points at. Applications register
     * their consumer groups with it; the group of {@code spring.kafka.consumer.group-id}, when
     * set, is registered here already.
     *
     * <p>Deliberately not {@code @ConditionalOnMissingBean}: a registry the application declares
     * for a second Kafka cluster must not replace this one. Turn it off with
     * {@code consumer-balancer.enabled=false}.
     */
    @Bean(name = BALANCERS_BEAN_NAME, destroyMethod = "close")
    public ConsumerGroupBalancers consumerGroupBalancers(
            @Qualifier(ADMIN_CLIENT_BEAN_NAME) AdminClient kafkaBalancerAdminClient,
            WeightService weightService,
            BalanceService balanceService,
            MemberIdTracker memberIdTracker,
            KafkaBalancerProperties properties,
            KafkaProperties kafkaProperties,
            ObjectProvider<RebalanceInitiator> rebalanceInitiator,
            ObjectProvider<RebalanceTrigger> rebalanceTrigger,
            ObjectProvider<KafkaListenerEndpointRegistry> endpointRegistry) {
        ConsumerGroupBalancers balancers = ConsumerGroupBalancers.builder()
                .adminClient(kafkaBalancerAdminClient)
                .weightService(weightService)
                .balanceService(balanceService)
                .memberIdTracker(memberIdTracker)
                .instanceId(properties.getInstanceId())
                .proactiveRebalance(properties.isProactiveRebalanceEnabled())
                .crossGroupBalancing(properties.isCrossGroupBalancingEnabled())
                .imbalanceThreshold(properties.getRebalanceLoadImbalanceThreshold())
                .damping(properties.toRebalanceDamping())
                .electionInterval(properties.getCoordinator().getElectionInterval())
                .triggerCheckInterval(properties.getCoordinator().getTriggerCheckInterval())
                .build();

        String groupId = kafkaProperties.getConsumer().getGroupId();
        if (groupId == null || groupId.isBlank()) {
            if (properties.isProactiveRebalanceEnabled()) {
                log.info("spring.kafka.consumer.group-id is not set, so no consumer group is balanced proactively"
                        + " out of the box; register your groups with the ConsumerGroupBalancers bean");
            }
            return balancers;
        }
        if (!properties.isProactiveRebalanceEnabled()) {
            balancers.register(groupId, null);
            return balancers;
        }
        balancers.group(groupId)
                .rebalanceInitiator(rebalanceInitiator.getIfUnique(
                        () -> defaultRebalanceInitiator(groupId, endpointRegistry.getObject(), properties)))
                .trigger(rebalanceTrigger.getIfUnique())
                .register();
        return balancers;
    }

    /**
     * Puts the auto-configured registry's assignor configs into Boot's auto-configured consumer
     * factory, where {@code LoadAwarePartitionAssignor} picks them up. Registered even when
     * proactive rebalance is disabled — the assignor path needs weights either way.
     */
    @Bean
    @ConditionalOnMissingBean(BalancerConsumerFactoryCustomizer.class)
    public BalancerConsumerFactoryCustomizer balancerConsumerFactoryCustomizer(
            @Qualifier(BALANCERS_BEAN_NAME) ConsumerGroupBalancers consumerGroupBalancers) {
        return new BalancerConsumerFactoryCustomizer(consumerGroupBalancers);
    }

    @Bean
    @ConditionalOnMissingBean(ConsumerGroupBalancersLifecycle.class)
    public ConsumerGroupBalancersLifecycle consumerGroupBalancersLifecycle(
            ObjectProvider<ConsumerGroupBalancers> consumerGroupBalancers) {
        return new ConsumerGroupBalancersLifecycle(consumerGroupBalancers.orderedStream().toList());
    }

    /**
     * Rebalances the registered listener containers of the group, restricted to
     * {@code consumer-balancer.listener-ids} when set — the group id alone does not tell two
     * Kafka clusters apart.
     */
    static RebalanceInitiator defaultRebalanceInitiator(
            String groupId,
            ListenerContainerRegistry registry,
            KafkaBalancerProperties properties) {
        ContainerRebalanceInitiator initiator = ContainerRebalanceInitiator.of(groupId, registry);
        List<String> listenerIds = properties.getListenerIds();
        return listenerIds.isEmpty() ? initiator : initiator.onlyListenerIds(listenerIds);
    }
}
