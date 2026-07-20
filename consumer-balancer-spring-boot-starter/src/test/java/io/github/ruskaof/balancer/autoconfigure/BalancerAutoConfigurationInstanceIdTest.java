package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The consumer-balancer.instance-id property must reach the consumer factory configs
 * under the assignor's instance-id key; without it the key stays absent so the assignor
 * uses its per-JVM random id.
 */
class BalancerAutoConfigurationInstanceIdTest {

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({
            KafkaAutoConfiguration.class,
            DefaultBalanceServiceAutoConfiguration.class,
            KafkaOffsetRateWeightAutoConfiguration.class,
            PrometheusWeightAutoConfiguration.class,
            BalancerAutoConfiguration.class
    })
    static class TestApp {
    }

    @Nested
    @SpringBootTest(classes = TestApp.class, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group",
            "consumer-balancer.proactive-rebalance-enabled=false",
            "consumer-balancer.instance-id=pod-42"
    })
    class WithConfiguredInstanceId {

        @Autowired
        ApplicationContext context;

        @Test
        void instanceIdReachesConsumerFactoryConfigs() {
            Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                    .getConfigurationProperties();

            assertThat(configs).containsEntry(LoadAwareAssignorConfig.INSTANCE_ID, "pod-42");
        }
    }

    @Nested
    @SpringBootTest(classes = TestApp.class, properties = {
            "spring.kafka.bootstrap-servers=127.0.0.1:9092",
            "spring.kafka.consumer.group-id=test-group",
            "consumer-balancer.proactive-rebalance-enabled=false"
    })
    class WithoutConfiguredInstanceId {

        @Autowired
        ApplicationContext context;

        @Test
        void instanceIdKeyStaysAbsentSoTheAssignorAutoResolves() {
            Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                    .getConfigurationProperties();

            assertThat(configs).doesNotContainKey(LoadAwareAssignorConfig.INSTANCE_ID);
        }
    }
}
