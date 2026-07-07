package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.autoconfigure.BalancerAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultBalanceServiceAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultKafkaRatePromqlBuilderAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.PrometheusWeightAutoConfiguration;
import io.github.ruskaof.balancer.prometheus.PrometheusClient;
import io.github.ruskaof.balancer.weight.WeightService;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A custom WeightService bean must drive the assignor too — its instance lands in the
 * consumer factory configs, and no Prometheus configuration is required (note: no
 * weight-query-template is set here).
 */
@SpringBootTest(classes = BalancerCustomWeightServiceIntegrationTest.TestApp.class, properties = {
        "spring.kafka.bootstrap-servers=127.0.0.1:9092",
        "spring.kafka.consumer.group-id=test-group",
        "consumer-balancer.proactive-rebalance-enabled=false"
})
class BalancerCustomWeightServiceIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void customWeightServiceBeanReachesAssignorConfigs() {
        Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                .getConfigurationProperties();

        assertThat(configs.get(LoadAwareAssignorConfig.WEIGHT_SERVICE))
                .isSameAs(context.getBean(WeightService.class));
        assertThat(context.getBeanNamesForType(PrometheusClient.class)).isEmpty();
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({
            KafkaAutoConfiguration.class,
            DefaultKafkaRatePromqlBuilderAutoConfiguration.class,
            DefaultBalanceServiceAutoConfiguration.class,
            PrometheusWeightAutoConfiguration.class,
            BalancerAutoConfiguration.class
    })
    static class TestApp {

        @Bean
        WeightService customWeightService() {
            return (Set<TopicPartition> partitions) -> Map.of();
        }
    }
}
