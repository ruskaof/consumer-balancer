package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.autoconfigure.BalancerAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultBalanceServiceAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultKafkaRatePromqlBuilderAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.PrometheusWeightAutoConfiguration;
import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.weight.WeightService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wiring check: the default WeightService/BalanceService beans must land in
 * Boot's auto-configured consumer factory configs, where the assignor picks them up.
 */
@SpringBootTest(classes = BalancerConsumerFactoryCustomizerIntegrationTest.TestApp.class, properties = {
        "spring.kafka.bootstrap-servers=127.0.0.1:9092",
        "spring.kafka.consumer.group-id=test-group",
        "consumer-balancer.proactive-rebalance-enabled=false"
})
@ActiveProfiles("test")
class BalancerConsumerFactoryCustomizerIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void defaultBeansReachBootConsumerFactoryConfigs() {
        Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                .getConfigurationProperties();

        assertThat(configs.get(LoadAwareAssignorConfig.WEIGHT_SERVICE))
                .isSameAs(context.getBean(WeightService.class));
        assertThat(configs.get(LoadAwareAssignorConfig.BALANCE_SERVICE))
                .isSameAs(context.getBean(BalanceService.class));
        assertThat(configs).doesNotContainKey(LoadAwareAssignorConfig.MEMBER_ID_TRACKER);
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
    }
}
