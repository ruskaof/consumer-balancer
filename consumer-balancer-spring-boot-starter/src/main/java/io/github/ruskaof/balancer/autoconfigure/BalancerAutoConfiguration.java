package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.ContainerRegistryRebalanceInitiator;
import io.github.ruskaof.balancer.CoordinatorManagerLifecycle;
import io.github.ruskaof.balancer.MemberIdTracker;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.trigger.CoordinatorElection;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.trigger.RebalanceTrigger;
import io.github.ruskaof.balancer.trigger.lag.ConsumerLagTrigger;
import io.github.ruskaof.balancer.trigger.membership.MembershipChangeTrigger;
import io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger;
import io.github.ruskaof.balancer.trigger.variance.LoadVarianceTrigger;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.Properties;
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

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "consumer-balancer.proactive-rebalance-enabled", havingValue = "true", matchIfMissing = true)
    static class ProactiveRebalanceConfiguration {

        @Bean
        public MemberIdTracker memberIdTracker() {
            return new MemberIdTracker();
        }

        @Bean
        public Supplier<Set<String>> coordinatorMemberIdSupplier(MemberIdTracker memberIdTracker) {
            return memberIdTracker::getCurrentMemberIds;
        }

        @Bean(destroyMethod = "close")
        public AdminClient kafkaBalancerAdminClient(KafkaProperties kafkaProperties) {
            Properties adminProperties = new Properties();
            adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
            return AdminClient.create(adminProperties);
        }

        @Bean
        @ConditionalOnMissingBean(RebalanceTrigger.class)
        public RebalanceTrigger rebalanceTrigger(
                AdminClient kafkaBalancerAdminClient,
                KafkaProperties kafkaProperties,
                WeightService weightService,
                BalanceService balanceService,
                KafkaBalancerProperties kafkaBalancerProperties) {
            String groupId = kafkaProperties.getConsumer().getGroupId();
            return switch (kafkaBalancerProperties.getTriggerType()) {
                case THRESHOLD -> new ThresholdTrigger(
                        kafkaBalancerAdminClient,
                        groupId,
                        weightService,
                        kafkaBalancerProperties.getRebalanceLoadImbalanceThreshold(),
                        balanceService);
                case MEMBERSHIP_CHANGE -> new MembershipChangeTrigger(
                        kafkaBalancerAdminClient,
                        groupId);
                case CONSUMER_LAG -> new ConsumerLagTrigger(
                        kafkaBalancerAdminClient,
                        groupId,
                        kafkaBalancerProperties.getLagImbalanceThreshold(),
                        kafkaBalancerProperties.getMinTotalLag());
                case LOAD_VARIANCE -> new LoadVarianceTrigger(
                        kafkaBalancerAdminClient,
                        groupId,
                        weightService,
                        kafkaBalancerProperties.getLoadVarianceThreshold());
            };
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
