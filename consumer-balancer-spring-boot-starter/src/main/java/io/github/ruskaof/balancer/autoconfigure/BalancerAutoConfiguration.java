package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.BalancerConsumerFactoryCustomizer;
import io.github.ruskaof.balancer.ContainerRegistryRebalanceInitiator;
import io.github.ruskaof.balancer.CoordinatorManagerLifecycle;
import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.trigger.CoordinatorElection;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.Set;
import java.util.function.Supplier;

@AutoConfiguration(after = {
        DefaultKafkaRatePromqlBuilderAutoConfiguration.class,
        DefaultBalanceServiceAutoConfiguration.class,
        PrometheusWeightAutoConfiguration.class
})
@EnableConfigurationProperties(KafkaBalancerProperties.class)
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
public class BalancerAutoConfiguration {

    /**
     * Puts the context's {@link WeightService}/{@link BalanceService} (and
     * {@link MemberIdTracker} when proactive rebalance is enabled) into the
     * auto-configured consumer factory's configs, where
     * {@code LoadAwarePartitionAssignor} picks them up. Registered even when proactive
     * rebalance is disabled — the assignor path needs weights either way.
     */
    @Bean
    @ConditionalOnMissingBean(BalancerConsumerFactoryCustomizer.class)
    public BalancerConsumerFactoryCustomizer balancerConsumerFactoryCustomizer(
            WeightService weightService,
            BalanceService balanceService,
            ObjectProvider<MemberIdTracker> memberIdTracker) {
        return new BalancerConsumerFactoryCustomizer(
                weightService,
                balanceService,
                memberIdTracker.getIfAvailable());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "consumer-balancer.proactive-rebalance-enabled", havingValue = "true", matchIfMissing = true)
    static class ProactiveRebalanceConfiguration {

        @Bean
        public MemberIdTracker memberIdTracker() {
            return new MemberIdTracker();
        }

        @Bean
        public Supplier<Set<String>> coordinatorMemberIdSupplier(
                MemberIdTracker memberIdTracker,
                KafkaProperties kafkaProperties) {
            String groupId = kafkaProperties.getConsumer().getGroupId();
            return () -> memberIdTracker.getCurrentMemberIds(groupId);
        }

        @Bean(destroyMethod = "close")
        public AdminClient kafkaBalancerAdminClient(KafkaProperties kafkaProperties) {
            // Full admin properties so security settings like SSL/SASL from
            // spring.kafka.* apply to the balancer's admin client too.
            return AdminClient.create(kafkaProperties.buildAdminProperties());
        }

        @Bean
        @ConditionalOnMissingBean(RebalanceTrigger.class)
        public RebalanceTrigger rebalanceTrigger(
                AdminClient kafkaBalancerAdminClient,
                KafkaProperties kafkaProperties,
                WeightService weightService,
                BalanceService balanceService,
                KafkaBalancerProperties kafkaBalancerProperties) {
            return new ThresholdTrigger(
                    kafkaBalancerAdminClient,
                    kafkaProperties.getConsumer().getGroupId(),
                    weightService,
                    kafkaBalancerProperties.getRebalanceLoadImbalanceThreshold(),
                    balanceService);
        }

        @Bean
        public CoordinatorManager.RebalanceInitiator rebalanceInitiator(
                KafkaListenerEndpointRegistry registry) {
            return new ContainerRegistryRebalanceInitiator(registry);
        }

        @Bean
        public CoordinatorManager coordinatorManager(
                Supplier<Set<String>> memberIdsSupplier,
                RebalanceTrigger trigger,
                CoordinatorManager.RebalanceInitiator rebalanceInitiator,
                KafkaBalancerProperties properties,
                KafkaProperties kafkaProperties,
                AdminClient kafkaBalancerAdminClient) {
            String groupId = kafkaProperties.getConsumer().getGroupId();

            CoordinatorElection election = new CoordinatorElection.Builder()
                    .setGroupId(groupId)
                    .setMemberIdsSupplier(memberIdsSupplier)
                    .setElectionIntervalMs(properties.getCoordinator().getElectionInterval().toMillis())
                    .setAdminClient(kafkaBalancerAdminClient)
                    .build();

            return new CoordinatorManager(
                    election,
                    trigger,
                    rebalanceInitiator,
                    properties.getCoordinator().getTriggerCheckInterval().toMillis());
        }

        @Bean
        public CoordinatorManagerLifecycle coordinatorManagerLifecycle(CoordinatorManager coordinatorManager) {
            return new CoordinatorManagerLifecycle(coordinatorManager);
        }
    }
}
