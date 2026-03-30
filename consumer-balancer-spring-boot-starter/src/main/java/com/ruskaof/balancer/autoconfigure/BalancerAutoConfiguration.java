package com.ruskaof.balancer.autoconfigure;

import com.ruskaof.balancer.ContainerRegistryRebalanceInitiator;
import com.ruskaof.balancer.CoordinatorManagerLifecycle;
import com.ruskaof.balancer.MemberIdTracker;
import com.ruskaof.listener.balance.BalanceService;
import com.ruskaof.listener.trigger.CoordinatorElection;
import com.ruskaof.listener.trigger.CoordinatorManager;
import com.ruskaof.listener.trigger.RebalanceTrigger;
import com.ruskaof.listener.trigger.threshold.ThresholdTrigger;
import com.ruskaof.listener.weight.WeightService;
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
