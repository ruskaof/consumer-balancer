package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.BalancerConsumerFactoryCustomizer;
import io.github.ruskaof.balancer.ConsumerGroupBalancers;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Applications running several consumer groups often set no spring.kafka.consumer.group-id at
 * all. Without it there is no default group to balance, but the context must still start with
 * the shared balancer in place, ready for the application to register its own groups.
 */
class BalancerAutoConfigurationGroupIdTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    KafkaAutoConfiguration.class,
                    DefaultBalanceServiceAutoConfiguration.class,
                    KafkaOffsetRateWeightAutoConfiguration.class,
                    PrometheusWeightAutoConfiguration.class,
                    BalancerAutoConfiguration.class))
            .withPropertyValues("spring.kafka.bootstrap-servers=127.0.0.1:9092");

    @Test
    void startsWithoutADefaultGroupWhenNoGroupIdIsSet() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(BalancerConsumerFactoryCustomizer.class);

            ConsumerGroupBalancers balancers = context.getBean(ConsumerGroupBalancers.class);
            assertThat(balancers.getGroups()).isEmpty();
            assertThat(balancers.isRunning()).isTrue();
        });
    }
}
