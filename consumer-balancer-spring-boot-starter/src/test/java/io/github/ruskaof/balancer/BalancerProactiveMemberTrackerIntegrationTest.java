package io.github.ruskaof.balancer;

import io.github.ruskaof.balancer.LoadAwarePartitionAssignor.LoadAwareAssignorConfig;
import io.github.ruskaof.balancer.autoconfigure.BalancerAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.DefaultBalanceServiceAutoConfiguration;
import io.github.ruskaof.balancer.autoconfigure.KafkaOffsetRateWeightAutoConfiguration;
import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * With proactive rebalance enabled, the customizer must inject the MemberIdTracker bean
 * so the assignor can report member ids to the coordinator election.
 */
@SpringBootTest(classes = BalancerProactiveMemberTrackerIntegrationTest.TestApp.class)
class BalancerProactiveMemberTrackerIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void customizerInjectsMemberIdTrackerWhenProactiveRebalanceEnabled() {
        DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory<>(
                new HashMap<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")));

        context.getBean(BalancerConsumerFactoryCustomizer.class).customize(factory);

        assertThat(factory.getConfigurationProperties().get(LoadAwareAssignorConfig.MEMBER_ID_TRACKER))
                .isSameAs(context.getBean(MemberIdTracker.class));
        assertThat(context.getBean(CoordinatorManager.class)).isNotNull();
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({
            DefaultBalanceServiceAutoConfiguration.class,
            KafkaOffsetRateWeightAutoConfiguration.class,
            BalancerAutoConfiguration.class
    })
    static class TestApp {

        @Bean
        KafkaProperties kafkaProperties() {
            KafkaProperties p = new KafkaProperties();
            p.setBootstrapServers(List.of("127.0.0.1:9092"));
            p.getConsumer().setGroupId("test-group");
            // Fail fast when the election's AdminClient polls the absent broker so
            // context shutdown does not wait for the default 60s API timeout.
            p.getAdmin().getProperties().put("default.api.timeout.ms", "1000");
            p.getAdmin().getProperties().put("request.timeout.ms", "500");
            return p;
        }

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return mock(KafkaListenerEndpointRegistry.class);
        }
    }
}
