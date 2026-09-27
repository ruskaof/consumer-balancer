package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class BalancerConsumerFactoryCustomizerTest {

    private final WeightService weightService = partitions -> Map.of();
    private final BalanceService balanceService = (members, weights) -> Map.of();
    private final MemberIdTracker memberIdTracker = new MemberIdTracker();

    @Test
    void injectsTheRegistryCollaboratorsIntoFactoryConfigs() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(balancers(true, "pod-1")).customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LoadAwarePartitionAssignor.class.getName())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService)
                .containsEntry(LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService)
                .containsEntry(LoadAwareAssignorConfig.MEMBER_ID_TRACKER, memberIdTracker)
                .containsEntry(LoadAwareAssignorConfig.INSTANCE_ID, "pod-1");
    }

    @Test
    void keepsAStrategyTheApplicationPickedItself() {
        Map<String, Object> initial = new HashMap<>();
        initial.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(initial);

        new BalancerConsumerFactoryCustomizer(balancers(true, null)).customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "org.apache.kafka.clients.consumer.RangeAssignor");
    }

    @Test
    void doesNotOverwriteExplicitUserValues() {
        Map<String, Object> initial = new HashMap<>();
        initial.put(LoadAwareAssignorConfig.WEIGHT_SERVICE, "com.example.CustomWeightService");
        initial.put(LoadAwareAssignorConfig.INSTANCE_ID, "explicit-pod");
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(initial);

        new BalancerConsumerFactoryCustomizer(balancers(true, "pod-1")).customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, "com.example.CustomWeightService")
                .containsEntry(LoadAwareAssignorConfig.INSTANCE_ID, "explicit-pod")
                .containsEntry(LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService);
    }

    @Test
    void skipsTrackerKeyWhileProactiveRebalanceIsOff() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(balancers(false, null)).customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService)
                .doesNotContainKey(LoadAwareAssignorConfig.MEMBER_ID_TRACKER);
    }

    @Test
    void skipsInstanceIdKeyWhenNotConfigured() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(balancers(true, " ")).customize(factory);

        assertThat(factory.getConfigurationProperties())
                .doesNotContainKey(LoadAwareAssignorConfig.INSTANCE_ID);
    }

    private ConsumerGroupBalancers balancers(boolean proactiveRebalance, String instanceId) {
        return ConsumerGroupBalancers.builder()
                .adminClient(mock(AdminClient.class))
                .weightService(weightService)
                .balanceService(balanceService)
                .memberIdTracker(memberIdTracker)
                .instanceId(instanceId)
                .proactiveRebalance(proactiveRebalance)
                .build();
    }

    private static DefaultKafkaConsumerFactory<Object, Object> factory(Map<String, Object> configs) {
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return new DefaultKafkaConsumerFactory<>(configs);
    }
}
