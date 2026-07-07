package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.autoconfigure.BalancerAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultBalanceServiceAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultKafkaRatePromqlBuilderAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.PrometheusWeightAutoConfiguration;
import io.github.ruskaof.balancer.weight.WeightService;
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
 * With consumer-balancer.enabled=false the starter must behave as if absent: no beans,
 * no assignor keys in the consumer factory, and no required weight-query-template
 * (none is set here).
 */
@SpringBootTest(classes = BalancerDisabledIntegrationTest.TestApp.class, properties = {
        "spring.kafka.bootstrap-servers=127.0.0.1:9092",
        "consumer-balancer.enabled=false"
})
class BalancerDisabledIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void starterBacksOffEntirelyWhenDisabled() {
        assertThat(context.getBeanNamesForType(WeightService.class)).isEmpty();
        assertThat(context.getBeanNamesForType(BalancerConsumerFactoryCustomizer.class)).isEmpty();

        Map<String, Object> configs = context.getBean(DefaultKafkaConsumerFactory.class)
                .getConfigurationProperties();
        assertThat(configs)
                .doesNotContainKey(LoadAwareAssignorConfig.WEIGHT_SERVICE)
                .doesNotContainKey(LoadAwareAssignorConfig.BALANCE_SERVICE)
                .doesNotContainKey(LoadAwareAssignorConfig.MEMBER_ID_TRACKER);
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
