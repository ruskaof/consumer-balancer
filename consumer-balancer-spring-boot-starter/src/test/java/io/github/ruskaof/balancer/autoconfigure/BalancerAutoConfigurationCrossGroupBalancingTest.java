package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.ConsumerGroupBalancers;
import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.weight.WeightService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BalancerAutoConfigurationCrossGroupBalancingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    KafkaAutoConfiguration.class,
                    DefaultBalanceServiceAutoConfiguration.class,
                    BalancerAutoConfiguration.class))
            .withBean(WeightService.class, () -> partitions -> Map.of())
            .withPropertyValues("spring.kafka.bootstrap-servers=127.0.0.1:9092");

    @Test
    void crossGroupBalancingIsDisabledByDefault() {
        runner.run(context -> {
            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);
            Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                    .getConfigurationProperties();

            assertThat(context.getBean(KafkaBalancerProperties.class).isCrossGroupBalancingEnabled()).isFalse();
            assertThat(balancers.isCrossGroupBalancingEnabled()).isFalse();
            assertThat(configs).doesNotContainKey(LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE);
        });
    }

    @Test
    void optInReachesTheRegistryAndConsumerFactory() {
        runner.withPropertyValues("consumer-balancer.cross-group-balancing-enabled=true").run(context -> {
            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);
            Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                    .getConfigurationProperties();

            assertThat(context.getBean(KafkaBalancerProperties.class).isCrossGroupBalancingEnabled()).isTrue();
            assertThat(balancers.isCrossGroupBalancingEnabled()).isTrue();
            assertThat(configs.get(LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE))
                    .isNotNull()
                    .isSameAs(balancers.assignorConfigs().get(LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE));
            assertThat(configs.get(LoadAwareAssignorConfig.MEMBER_ID_TRACKER))
                    .isSameAs(balancers.getMemberIdTracker());
        });
    }

    @Test
    void optInKeepsCrossGroupAssignmentEnabledForPassiveGroups() {
        runner.withPropertyValues(
                "consumer-balancer.cross-group-balancing-enabled=true",
                "consumer-balancer.proactive-rebalance-enabled=false",
                "spring.kafka.consumer.group-id=orders").run(context -> {
            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);
            Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                    .getConfigurationProperties();

            assertThat(balancers.isCrossGroupBalancingEnabled()).isTrue();
            assertThat(balancers.isProactiveRebalance()).isFalse();
            assertThat(balancers.getGroup("orders"))
                    .hasValueSatisfying(group -> assertThat(group.isProactive()).isFalse());
            assertThat(configs.get(LoadAwareAssignorConfig.CROSS_GROUP_LOAD_SERVICE)).isNotNull();
            assertThat(configs.get(LoadAwareAssignorConfig.MEMBER_ID_TRACKER))
                    .isSameAs(balancers.getMemberIdTracker());
        });
    }
}
