package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.trigger.CoordinatorManager;
import io.github.ruskaof.balancer.weight.WeightService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = BalancerAutoConfigurationProactiveDisabledTest.App.class, properties = "consumer-balancer.proactive-rebalance-enabled=false")
class BalancerAutoConfigurationProactiveDisabledTest {

    @Autowired
    ApplicationContext context;

    @Test
    void loadsWeightServiceButNotCoordinatorWhenProactiveRebalanceDisabled() {
        assertThat(context.getBean(WeightService.class)).isNotNull();
        assertThat(context.getBeanNamesForType(CoordinatorManager.class)).isEmpty();
    }

    @SpringBootApplication(exclude = KafkaAutoConfiguration.class)
    static class App {

        @Bean
        org.springframework.boot.kafka.autoconfigure.KafkaProperties kafkaProperties() {
            org.springframework.boot.kafka.autoconfigure.KafkaProperties p = new org.springframework.boot.kafka.autoconfigure.KafkaProperties();
            p.setBootstrapServers(List.of("127.0.0.1:9092"));
            p.getConsumer().setGroupId("test-group");
            return p;
        }

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return mock(KafkaListenerEndpointRegistry.class);
        }
    }
}
