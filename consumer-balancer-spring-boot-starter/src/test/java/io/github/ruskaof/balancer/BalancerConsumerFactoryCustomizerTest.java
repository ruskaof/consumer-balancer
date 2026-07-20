package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BalancerConsumerFactoryCustomizerTest {

    private final WeightService weightService = partitions -> Map.of();
    private final BalanceService balanceService = (members, weights) -> Map.of();
    private final MemberIdTracker memberIdTracker = new MemberIdTracker();

    @Test
    void injectsContextCollaboratorsIntoFactoryConfigs() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(weightService, balanceService, memberIdTracker, "pod-1")
                .customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService)
                .containsEntry(LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService)
                .containsEntry(LoadAwareAssignorConfig.MEMBER_ID_TRACKER, memberIdTracker)
                .containsEntry(LoadAwareAssignorConfig.INSTANCE_ID, "pod-1");
    }

    @Test
    void doesNotOverwriteExplicitUserValues() {
        Map<String, Object> initial = new HashMap<>();
        initial.put(LoadAwareAssignorConfig.WEIGHT_SERVICE, "com.example.CustomWeightService");
        initial.put(LoadAwareAssignorConfig.INSTANCE_ID, "explicit-pod");
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(initial);

        new BalancerConsumerFactoryCustomizer(weightService, balanceService, memberIdTracker, "pod-1")
                .customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, "com.example.CustomWeightService")
                .containsEntry(LoadAwareAssignorConfig.INSTANCE_ID, "explicit-pod")
                .containsEntry(LoadAwareAssignorConfig.BALANCE_SERVICE, balanceService);
    }

    @Test
    void skipsTrackerKeyWhenTrackerIsAbsent() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(weightService, balanceService, null, null)
                .customize(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(LoadAwareAssignorConfig.WEIGHT_SERVICE, weightService)
                .doesNotContainKey(LoadAwareAssignorConfig.MEMBER_ID_TRACKER);
    }

    @Test
    void skipsInstanceIdKeyWhenNotConfigured() {
        DefaultKafkaConsumerFactory<Object, Object> factory = factory(new HashMap<>());

        new BalancerConsumerFactoryCustomizer(weightService, balanceService, memberIdTracker, " ")
                .customize(factory);

        assertThat(factory.getConfigurationProperties())
                .doesNotContainKey(LoadAwareAssignorConfig.INSTANCE_ID);
    }

    private static DefaultKafkaConsumerFactory<Object, Object> factory(Map<String, Object> configs) {
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return new DefaultKafkaConsumerFactory<>(configs);
    }
}
